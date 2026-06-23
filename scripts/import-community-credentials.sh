#!/usr/bin/env bash
#
# import-community-credentials.sh
#
# Inserts a community's PDS credentials into the target appview's
# `community_credentials` table. The app password is AES-256-GCM encrypted
# with the target deployment's CREDENTIAL_ENCRYPTION_KEY before insert, so the
# appview can decrypt it at use time (see src/lib/crypto.rs).
#
# This is the manual counterpart to the `registerCredentials` XRPC endpoint.
# Note it does NOT register the DID with Tap, so if the community's records are
# not already in `record_data`, the firehose won't backfill them.
#
# Requires: docker and node (Node's built-in `crypto` does the encryption, so
# there is nothing extra to install).

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: import-community-credentials.sh [options]

Required:
  --did DID                Community DID (e.g. did:plc:xxxx)
  --pds URL                Community PDS endpoint (e.g. https://pds.example.com)
  --identifier ID          PDS login identifier (handle or DID)
  --pg-container NAME       Target Postgres container name (see: docker ps)

Encryption key — CREDENTIAL_ENCRYPTION_KEY (32 bytes, base64). Resolved from the
first source available, in this order:
  --key BASE64              the value directly
  --env-file PATH           read CREDENTIAL_ENCRYPTION_KEY= from a .env file
  $CREDENTIAL_ENCRYPTION_KEY already exported in your shell
  --app-container NAME      read it from a running app container (if you have one)

Optional:
  --password PASS           App password. If omitted, you are prompted (recommended,
                            keeps it out of shell history and the process list).
                            Encryption uses Node's built-in crypto; nothing to install.
  --managed                 Mark the credential appview-managed (DID minted on
                            the appview's own PDS). Shorthand for
                            --source appview_managed.
  --source SRC              "byo" (external PDS, default) or "appview_managed"
Connection — user / password / database come from DATABASE_URL
(postgres://user:pass@host:port/db), resolved in this order:
  --database-url URL        the value directly
  --env-file PATH           read DATABASE_URL= from the same .env
  $DATABASE_URL exported in your shell
  --app-container NAME      read it from a running app container
If no DATABASE_URL is found, falls back to POSTGRES_USER/POSTGRES_DB from the
postgres container, then "colibri".

  --pg-user USER            Override the user parsed from DATABASE_URL
  --pg-db DB                Override the database parsed from DATABASE_URL
  --yes                     Skip the confirmation prompt
  -h, --help                Show this help

Example (key from the repo .env, only a postgres container running):
  ./import-community-credentials.sh \
    --did did:plc:xxxx \
    --pds https://pds.example.com \
    --identifier my.community \
    --env-file ./.env \
    --pg-container colibri-postgres-1
EOF
}

# ---- defaults --------------------------------------------------------------
DID="" PDS="" IDENTIFIER="" PASSWORD="" SOURCE="byo"
APP_CONTAINER="" PG_CONTAINER="" KEY="" ENV_FILE="" DB_URL=""
PG_USER="" PG_DB="" PG_PASS="" ASSUME_YES=0
PASSWORD_SET=0 SOURCE_SET=0 MANAGED=0

# ---- parse args ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
	case "$1" in
		--did)           DID="$2"; shift 2 ;;
		--pds)           PDS="$2"; shift 2 ;;
		--identifier)    IDENTIFIER="$2"; shift 2 ;;
		--password)      PASSWORD="$2"; PASSWORD_SET=1; shift 2 ;;
		--source)        SOURCE="$2"; SOURCE_SET=1; shift 2 ;;
		--managed)       MANAGED=1; shift ;;
		--app-container) APP_CONTAINER="$2"; shift 2 ;;
		--pg-container)  PG_CONTAINER="$2"; shift 2 ;;
		--key)           KEY="$2"; shift 2 ;;
		--env-file)      ENV_FILE="$2"; shift 2 ;;
		--database-url)  DB_URL="$2"; shift 2 ;;
		--pg-user)       PG_USER="$2"; shift 2 ;;
		--pg-db)         PG_DB="$2"; shift 2 ;;
		--yes)           ASSUME_YES=1; shift ;;
		-h|--help)       usage; exit 0 ;;
		*) echo "error: unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

# ---- validate --------------------------------------------------------------
fail() { echo "error: $1" >&2; exit 2; }

# Read VAR= from a dotenv file: last match wins; strips surrounding quotes and
# trailing whitespace/CR.
read_env_var() { # <var-name> <file>
	grep -E "^[[:space:]]*$1=" "$2" | tail -n1 \
		| sed -E "s/^[[:space:]]*$1=//; s/^[\"']//; s/[\"']?[[:space:]]*\$//"
}

[[ -n "$DID" ]]          || fail "--did is required"
[[ -n "$PDS" ]]          || fail "--pds is required"
[[ -n "$IDENTIFIER" ]]   || fail "--identifier is required"
[[ -n "$PG_CONTAINER" ]] || fail "--pg-container is required"

# --managed is shorthand for --source appview_managed. Reject the contradiction
# of asking for managed while explicitly passing a different --source.
if [[ "$MANAGED" -eq 1 ]]; then
	if [[ "$SOURCE_SET" -eq 1 && "$SOURCE" != "appview_managed" ]]; then
		fail "--managed conflicts with --source $SOURCE (managed implies appview_managed)"
	fi
	SOURCE="appview_managed"
fi
[[ "$SOURCE" == "byo" || "$SOURCE" == "appview_managed" ]] \
	|| fail "--source must be 'byo' or 'appview_managed'"

command -v docker >/dev/null 2>&1 || fail "docker not found on PATH"
command -v node   >/dev/null 2>&1 || fail "node not found on PATH"

# ---- resolve the encryption key -------------------------------------------
# Source priority: --key > --env-file > exported env var > --app-container.
if [[ -n "$KEY" ]]; then
	KEY_SOURCE="--key"
elif [[ -n "$ENV_FILE" ]]; then
	[[ -f "$ENV_FILE" ]] || fail "--env-file not found: $ENV_FILE"
	KEY="$(read_env_var CREDENTIAL_ENCRYPTION_KEY "$ENV_FILE")"
	[[ -n "$KEY" ]] || fail "CREDENTIAL_ENCRYPTION_KEY= not found in $ENV_FILE"
	KEY_SOURCE="$ENV_FILE"
elif [[ -n "${CREDENTIAL_ENCRYPTION_KEY:-}" ]]; then
	KEY="$CREDENTIAL_ENCRYPTION_KEY"
	KEY_SOURCE="\$CREDENTIAL_ENCRYPTION_KEY"
elif [[ -n "$APP_CONTAINER" ]]; then
	KEY="$(docker exec "$APP_CONTAINER" printenv CREDENTIAL_ENCRYPTION_KEY)" \
		|| fail "could not read CREDENTIAL_ENCRYPTION_KEY from container '$APP_CONTAINER'"
	[[ -n "$KEY" ]] || fail "CREDENTIAL_ENCRYPTION_KEY is empty in '$APP_CONTAINER'"
	KEY_SOURCE="container:$APP_CONTAINER"
else
	fail "no encryption key source — pass --key, --env-file, export CREDENTIAL_ENCRYPTION_KEY, or --app-container"
fi

# ---- resolve the connection (user / password / db) ------------------------
# The authoritative source is DATABASE_URL (postgres://user:pass@host:port/db).
# Resolve it the same way as the key: --database-url > --env-file > exported env
# var > the app container (if given). Explicit --pg-user/--pg-db still override
# the parsed user/db.
if [[ -z "$DB_URL" && -n "$ENV_FILE" && -f "$ENV_FILE" ]]; then
	DB_URL="$(read_env_var DATABASE_URL "$ENV_FILE")"
fi
if [[ -z "$DB_URL" && -n "${DATABASE_URL:-}" ]]; then
	DB_URL="$DATABASE_URL"
fi
if [[ -z "$DB_URL" && -n "$APP_CONTAINER" ]]; then
	DB_URL="$(docker exec "$APP_CONTAINER" printenv DATABASE_URL 2>/dev/null || true)"
fi

if [[ -n "$DB_URL" ]]; then
	# Parse with Node's URL (handles percent-encoded user/password). Emits
	# user, password, db on three lines.
	{ read -r DU_USER; read -r DU_PASS; read -r DU_DB; } < <(
		DATABASE_URL="$DB_URL" node -e '
			const u = new URL(process.env.DATABASE_URL);
			const dec = (s) => { try { return decodeURIComponent(s); } catch { return s; } };
			process.stdout.write(
				[dec(u.username), dec(u.password), dec(u.pathname.replace(/^\//, ""))].join("\n") + "\n"
			);
		' 2>/dev/null
	) || fail "could not parse DATABASE_URL (expected postgres://user:pass@host:port/db)"
	[[ -n "$DU_USER" ]] || fail "DATABASE_URL has no user component"
	PG_USER="${PG_USER:-$DU_USER}"
	PG_DB="${PG_DB:-$DU_DB}"
	PG_PASS="$DU_PASS"
else
	# No DATABASE_URL available — fall back to container env / "colibri".
	if [[ -z "$PG_USER" ]]; then
		PG_USER="$(docker exec "$PG_CONTAINER" printenv POSTGRES_USER 2>/dev/null || true)"
		PG_USER="${PG_USER:-colibri}"
	fi
	if [[ -z "$PG_DB" ]]; then
		PG_DB="$(docker exec "$PG_CONTAINER" printenv POSTGRES_DB 2>/dev/null || true)"
		PG_DB="${PG_DB:-$PG_USER}"
	fi
fi

# ---- get the password (prompt if not passed) -------------------------------
if [[ "$PASSWORD_SET" -eq 0 ]]; then
	read -r -s -p "App password for ${IDENTIFIER}: " PASSWORD
	echo
	[[ -n "$PASSWORD" ]] || fail "no password entered"
fi

# ---- confirm ---------------------------------------------------------------
echo
echo "About to upsert credentials:"
echo "  community_did : $DID"
echo "  pds_endpoint  : $PDS"
echo "  identifier    : $IDENTIFIER"
echo "  source        : $SOURCE"
echo "  key from      : $KEY_SOURCE"
echo "  -> postgres    : container=$PG_CONTAINER user=$PG_USER db=$PG_DB password=$([[ -n "$PG_PASS" ]] && echo yes || echo '(none)')"
echo
if [[ "$ASSUME_YES" -ne 1 ]]; then
	read -r -p "Proceed? [y/N] " reply
	[[ "$reply" == "y" || "$reply" == "Y" ]] || { echo "aborted."; exit 1; }
fi

# ---- build the INSERT (encrypt with node, secrets passed via env) ----------
read -r -d '' NODE_ENCRYPT <<'JS' || true
const crypto = require("crypto");

const key = Buffer.from((process.env.CRED_KEY || "").trim(), "base64");
if (key.length !== 32) {
  console.error("CREDENTIAL_ENCRYPTION_KEY must decode to exactly 32 bytes");
  process.exit(1);
}

const did      = process.env.CRED_DID;
const pds      = process.env.CRED_PDS;
const ident    = process.env.CRED_IDENT;
const password = process.env.CRED_PASSWORD;
const source   = process.env.CRED_SOURCE;

const nonce = crypto.randomBytes(12);
const cipher = crypto.createCipheriv("aes-256-gcm", key, nonce);
const enc = Buffer.concat([cipher.update(password, "utf8"), cipher.final()]);
const ct = Buffer.concat([enc, cipher.getAuthTag()]);   // ciphertext || 16-byte tag

const ctB64 = ct.toString("base64");
const nonceB64 = nonce.toString("base64");
const created = new Date().toISOString();   // e.g. 2026-05-15T00:00:00.000Z

const q = (s) => "'" + String(s).replace(/'/g, "''") + "'";
process.stdout.write(
`INSERT INTO community_credentials
(community_did, pds_endpoint, identifier, password_ciphertext_b64, password_nonce_b64, source, created_at)
VALUES (${q(did)}, ${q(pds)}, ${q(ident)}, ${q(ctB64)}, ${q(nonceB64)}, ${q(source)}, ${q(created)})
ON CONFLICT (community_did) DO UPDATE SET
  pds_endpoint=EXCLUDED.pds_endpoint, identifier=EXCLUDED.identifier,
  password_ciphertext_b64=EXCLUDED.password_ciphertext_b64,
  password_nonce_b64=EXCLUDED.password_nonce_b64,
  source=EXCLUDED.source, created_at=EXCLUDED.created_at;\n`
);
JS

SQL="$(
	CRED_KEY="$KEY" CRED_DID="$DID" CRED_PDS="$PDS" CRED_IDENT="$IDENTIFIER" \
	CRED_PASSWORD="$PASSWORD" CRED_SOURCE="$SOURCE" \
	node -e "$NODE_ENCRYPT"
)"

# Run psql inside the container. PGPASSWORD is forwarded via -e (value stays in
# this script's env, not on the command line). Reads SQL from stdin or -c.
psql_exec() {
	PGPASSWORD="$PG_PASS" docker exec -i -e PGPASSWORD "$PG_CONTAINER" \
		psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 "$@"
}

# ---- apply -----------------------------------------------------------------
echo "$SQL" | psql_exec

# ---- verify (nonce must be 12 bytes; ct = password length + 16-byte tag) ---
echo
echo "Verifying stored row:"
psql_exec -c \
	"SELECT community_did, pds_endpoint, identifier, source,
	        length(decode(password_ciphertext_b64,'base64')) AS ct_len,
	        length(decode(password_nonce_b64,'base64'))      AS nonce_len
	 FROM community_credentials WHERE community_did=$(printf "%s" "$DID" | sed "s/'/''/g; s/^/'/; s/$/'/")" </dev/null

echo
echo "Done. If the appview still can't use these, double-check that the key matched"
echo "the SAME deployment as --pg-container (a wrong key fails only at use time)."
