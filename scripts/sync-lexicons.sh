#!/usr/bin/env bash
set -euo pipefail

REPO="${LEXICON_SOURCE_REPO:-colibri-social/colibri.social}"
REF="${LEXICON_SOURCE_REF:-main}"
GENERATED="apps/website/src/utils/atproto/lexicons/generated"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/lexicons"

usage() {
	cat <<'EOF'
Usage: scripts/sync-lexicons.sh [path-to-client-checkout]

With no argument, fetches the generated lexicons from the client repository
on GitHub (requires `gh`). With a path, copies them from a local checkout,
which is what you want when iterating on a lexicon change locally.

Environment:
  LEXICON_SOURCE_REPO  defaults to colibri-social/colibri.social
  LEXICON_SOURCE_REF   defaults to main
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

mkdir -p "$DEST"
rm -f "$DEST"/*.json

if [[ -n "${1:-}" ]]; then
	SRC="$1/$GENERATED"
	if [[ ! -d "$SRC" ]]; then
		echo "No generated lexicons at $SRC" >&2
		echo "Run 'pnpm lexicons:export' in the client repo first." >&2
		exit 1
	fi
	cp "$SRC"/*.json "$DEST/"
	SOURCE_REF="local:$(cd "$1" && git rev-parse HEAD 2>/dev/null || echo unknown)"
else
	command -v gh >/dev/null || { echo "gh is required" >&2; exit 1; }

	SOURCE_REF="$(gh api "repos/$REPO/commits/$REF" --jq '.sha')"

	gh api "repos/$REPO/contents/$GENERATED?ref=$SOURCE_REF" --jq '.[].name' \
		| grep '\.json$' \
		| while read -r name; do
			gh api "repos/$REPO/contents/$GENERATED/$name?ref=$SOURCE_REF" \
				--jq '.content' | base64 --decode > "$DEST/$name"
		done
fi

printf '%s\n' "$SOURCE_REF" > "$DEST/SOURCE"

echo "Synced $(find "$DEST" -name '*.json' | wc -l | tr -d ' ') lexicons from $SOURCE_REF"
