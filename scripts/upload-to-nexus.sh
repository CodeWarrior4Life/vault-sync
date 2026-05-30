#!/usr/bin/env bash
# Stage the Tauri updater bundle + its .sig to Nexus (is_current=FALSE on the
# server; promoted separately). Discovers the artifact by its .sig sibling so it
# adapts to whatever createUpdaterArtifacts emits (NSIS .zip / .app.tar.gz /
# .AppImage). A missing .sig is a HARD ERROR — signing failed must fail the
# release, never silently skip (that is how only linux ever published).
#
# Args: $1 PLATFORM (darwin-x86_64|darwin-aarch64|windows-x86_64|linux-x86_64)
#       $2 VERSION  (tag like v0.4.2; leading v is stripped)
set -euo pipefail
PLATFORM="$1"
VERSION="$2"
ROOT="src-tauri/target"

SIG=$(find "$ROOT" -type f -name '*.sig' -path '*/release/bundle/*' | head -1)
if [ -z "${SIG:-}" ] || [ ! -f "$SIG" ]; then
  echo "::error::no .sig updater artifact for $PLATFORM (signing failed or createUpdaterArtifacts off)"
  exit 1
fi
BUNDLE="${SIG%.sig}"
if [ ! -f "$BUNDLE" ]; then
  echo "::error::.sig $SIG has no sibling bundle $BUNDLE"
  exit 1
fi

SIG_HEX=$(python3 -c "import sys; print(open(sys.argv[1],'rb').read().hex())" "$SIG")

curl -fSs --retry 3 --retry-delay 5 -X POST \
  -H "Authorization: Bearer $NEXUS_CI_TOKEN" \
  -F "version=${VERSION#v}" \
  -F "signature=${SIG_HEX}" \
  -F "binary=@${BUNDLE}" \
  "https://nexus.obsidian-inc.com/admin/api/vault-sync/releases/${PLATFORM}/upload"
echo
echo "staged $(basename "$BUNDLE") + sig for $PLATFORM v${VERSION#v}"
