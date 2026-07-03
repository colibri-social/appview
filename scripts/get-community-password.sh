#!/usr/bin/env bash
#
# get-community-password.sh
#
# Reads a community's credential row from the target appview's
# `community_credentials` table and decrypts the app password with the
# deployment's CREDENTIAL_ENCRYPTION_KEY (AES-256-GCM, see src/lib/crypto.rs).
#
# This is the read-side counterpart to import-community-credentials.sh. It only
# reads — nothing is written back.
#
# Requires: docker and node (Node's built-in `crypto` does the decryption).

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: get-community-password.sh [options]

Required:
  --did DID                Community DID (e.g. did:plc:xxxx)
  --pg-container NAME       Target Postgres container name (see: docker ps)

Encryption key — CREDENTIAL_ENCRYPTION_KEY (32 bytes, base64). Resolved from the
first source available, in this order:
  --key BASE64              the value directly
  --env-file PATH           read CREDENTIAL_ENCRYPTION_KEY= from a .env file
  $CREDENTIAL_ENCRYPTION_KEY already exported in your shell
  --app-container NAME      read it from a running app container (if you have one)

Connection — user / password / database come from DATABASE_URL
(postgres://user:pass@host:port/db), resolved in this order:
  --database-url URL        the value directly
  --env-file PATH           read DATABASE_URL= from the same .env
  $DATABASE_URL exported in your shell
  --app-container NAME      read it from a running app container
If no DATABASE_URL is found, falls back to POSTGRES_USER/POSTGRES_DB from the
postgres container, then "colibri".

Optional:
  --pg-user USER            Override the user parsed from DATABASE_URL
  --pg-db DB                Override the database parsed from DATABASE_URL
  --show                    Print the password to stdout on its own line
                            (default: password is hidden, only metadata shown)
  -h, --help                Show this help

Example (key from the repo .env, only a postgres container running):
  ./get-community-password.sh \
    --did did:plc:xxxx \
    --env-file ./.env \
    --pg-container colibri-postgres-1 \
    --show
EOF
}

# ---- defaults --------------------------------------------------------------
DID="" APP_CONTAINER="" PG_CONTAINER="" KEY="" ENV_FILE="" DB_URL=""
PG_USER="" PG_DB="" PG_PASS="" SHOW=0

# ---- parse args ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
	case "$1" in
		--did)           DID="$2"; shift 2 ;;
		--app-container) APP_CONTAINER="$2"; shift 2 ;;
		--pg-container)  PG_CONTAINER="$2"; shift 2 ;;
		--key)           KEY="$2"; shift 2 ;;
		--env-file)      ENV_FILE="$2"; shift 2 ;;
		--database-url)  DB_URL="$2"; shift 2 ;;
		--pg-user)       PG_USER="$2"; shift 2 ;;
		--pg-db)         PG_DB="$2"; shift 2 ;;
		--show)          SHOW=1; shift ;;
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
[[ -n "$PG_CONTAINER" ]] || fail "--pg-container is required"

command -v docker >/dev/null 2>&1 || fail "docker not found on PATH"
command -v node   >/dev/null 2>&1 || fail "node not found on PATH"

# ---- resolve the encryption key -------------------------------------------
# Source priority: --key > --env-file > exported env var > --app-container.
if [[ -n "$KEY" ]]; then
	:
elif [[ -n "$ENV_FILE" ]]; then
	[[ -f "$ENV_FILE" ]] || fail "--env-file not found: $ENV_FILE"
	KEY="$(read_env_var CREDENTIAL_ENCRYPTION_KEY "$ENV_FILE")"
	[[ -n "$KEY" ]] || fail "CREDENTIAL_ENCRYPTION_KEY= not found in $ENV_FILE"
elif [[ -n "${CREDENTIAL_ENCRYPTION_KEY:-}" ]]; then
	KEY="$CREDENTIAL_ENCRYPTION_KEY"
elif [[ -n "$APP_CONTAINER" ]]; then
	KEY="$(docker exec "$APP_CONTAINER" printenv CREDENTIAL_ENCRYPTION_KEY)" \
		|| fail "could not read CREDENTIAL_ENCRYPTION_KEY from container '$APP_CONTAINER'"
	[[ -n "$KEY" ]] || fail "CREDENTIAL_ENCRYPTION_KEY is empty in '$APP_CONTAINER'"
else
	fail "no encryption key source — pass --key, --env-file, export CREDENTIAL_ENCRYPTION_KEY, or --app-container"
fi

# ---- resolve the connection (user / password / db) ------------------------
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
	if [[ -z "$PG_USER" ]]; then
		PG_USER="$(docker exec "$PG_CONTAINER" printenv POSTGRES_USER 2>/dev/null || true)"
		PG_USER="${PG_USER:-colibri}"
	fi
	if [[ -z "$PG_DB" ]]; then
		PG_DB="$(docker exec "$PG_CONTAINER" printenv POSTGRES_DB 2>/dev/null || true)"
		PG_DB="${PG_DB:-$PG_USER}"
	fi
fi

# ---- fetch the row ---------------------------------------------------------
# psql inside the container. PGPASSWORD is forwarded via -e (value stays in this
# script's env, not on the command line). -tA => tuples-only, unaligned.
psql_query() { # <sql>
	PGPASSWORD="$PG_PASS" docker exec -i -e PGPASSWORD "$PG_CONTAINER" \
		psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -tA -F $'\t' -c "$1" </dev/null
}

DID_SQL="$(printf "%s" "$DID" | sed "s/'/''/g; s/^/'/; s/$/'/")"
ROW="$(psql_query \
	"SELECT pds_endpoint, identifier, source, created_at,
	        password_ciphertext_b64, password_nonce_b64
	 FROM community_credentials WHERE community_did=$DID_SQL")"

[[ -n "$ROW" ]] || fail "no credential row found for $DID"

IFS=$'\t' read -r PDS_ENDPOINT IDENTIFIER SOURCE CREATED_AT CT_B64 NONCE_B64 <<<"$ROW"

# ---- decrypt (node; secrets passed via env) --------------------------------
read -r -d '' NODE_DECRYPT <<'JS' || true
const crypto = require("crypto");

const key = Buffer.from((process.env.CRED_KEY || "").trim(), "base64");
if (key.length !== 32) {
  console.error("CREDENTIAL_ENCRYPTION_KEY must decode to exactly 32 bytes");
  process.exit(1);
}

const nonce = Buffer.from(process.env.CRED_NONCE_B64 || "", "base64");
if (nonce.length !== 12) {
  console.error("nonce must decode to exactly 12 bytes, got " + nonce.length);
  process.exit(1);
}

const blob = Buffer.from(process.env.CRED_CT_B64 || "", "base64");
if (blob.length < 16) {
  console.error("ciphertext too short to contain a 16-byte auth tag");
  process.exit(1);
}
// Rust's aes-gcm appends the 16-byte tag to the ciphertext.
const tag = blob.subarray(blob.length - 16);
const ct = blob.subarray(0, blob.length - 16);

const decipher = crypto.createDecipheriv("aes-256-gcm", key, nonce);
decipher.setAuthTag(tag);
let out;
try {
  out = Buffer.concat([decipher.update(ct), decipher.final()]);
} catch (e) {
  console.error("decryption failed (wrong key, tampering, or corrupt ciphertext)");
  process.exit(1);
}
process.stdout.write(out.toString("utf8"));
JS

PASSWORD="$(
	CRED_KEY="$KEY" CRED_CT_B64="$CT_B64" CRED_NONCE_B64="$NONCE_B64" \
	node -e "$NODE_DECRYPT"
)"

# ---- output ----------------------------------------------------------------
echo "community_did : $DID"
echo "pds_endpoint  : $PDS_ENDPOINT"
echo "identifier    : $IDENTIFIER"
echo "source        : $SOURCE"
echo "created_at    : $CREATED_AT"
if [[ "$SHOW" -eq 1 ]]; then
	echo "password      : $PASSWORD"
else
	echo "password      : (hidden — pass --show to print it)"
fi
