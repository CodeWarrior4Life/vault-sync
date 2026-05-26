#!/usr/bin/env bash
set -euo pipefail
PLATFORM="$1"
VERSION="$2"
BUNDLE=$(find src-tauri/target/*/release/bundle -type f \( -name '*.msi' -o -name '*.dmg' -o -name '*.deb' -o -name '*.AppImage' \) | head -1)
SIG=$(find src-tauri/target/*/release/bundle -name '*.sig' | head -1)
SIG_HEX=$(xxd -p -c 9999 "$SIG")
curl -fSs -X POST \
  -H "Authorization: Bearer $NEXUS_CI_TOKEN" \
  -F "version=${VERSION#v}" \
  -F "signature=${SIG_HEX}" \
  -F "binary=@${BUNDLE}" \
  "https://nexus.obsidian-inc.com/admin/api/vault-sync/releases/${PLATFORM}/upload"
