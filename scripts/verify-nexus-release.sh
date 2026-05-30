#!/usr/bin/env bash
# HARD post-publish gate. Proves (a) the staged binary is retrievable by version
# (catches half-publish) and (b) the signing private key matches the pubkey the
# installed daemons bundle (catches a key mismatch that would silently fail every
# auto-update). Verifies the locally-built .sig against the downloaded bundle
# using the pubkey from tauri.conf.json via rsign2 (pure-Rust minisign).
#
# Args: $1 PLATFORM   $2 VERSION (vX.Y.Z; leading v stripped)
set -euo pipefail
PLATFORM="$1"
VERSION="${2#v}"
WORK="$(mktemp -d)"

# 1. Download the staged binary by exact version.
curl -fSs --retry 3 \
  "https://nexus.obsidian-inc.com/admin/api/vault-sync/releases/${PLATFORM}/download?v=${VERSION}" \
  -o "$WORK/bundle"
test -s "$WORK/bundle" || { echo "::error::downloaded bundle is empty for $PLATFORM v$VERSION"; exit 1; }

# 2. Byte-identity vs the locally-built bundle (the one we signed this run).
SIG=$(find src-tauri/target -type f -name '*.sig' -path '*/release/bundle/*' | head -1)
LOCAL_BUNDLE="${SIG%.sig}"
LOCAL_SHA=$(python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$LOCAL_BUNDLE")
DL_SHA=$(python3 -c "import hashlib,sys;print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" "$WORK/bundle")
if [ "$LOCAL_SHA" != "$DL_SHA" ]; then
  echo "::error::downloaded bundle sha256 != local ($DL_SHA != $LOCAL_SHA) — half-publish/corruption"
  exit 1
fi

# 3. minisign-verify the local .sig against the bundle using the BUNDLED pubkey.
python3 - "$WORK/pub.key" <<'PY'
import base64, json, sys
pk = json.load(open("src-tauri/tauri.conf.json"))["plugins"]["updater"]["pubkey"]
open(sys.argv[1], "wb").write(base64.b64decode(pk))
PY
command -v rsign >/dev/null 2>&1 || cargo install rsign2 --quiet
rsign verify -p "$WORK/pub.key" -x "$SIG" "$LOCAL_BUNDLE"
echo "verified $PLATFORM v$VERSION: retrievable + byte-identical + signature matches bundled pubkey"
