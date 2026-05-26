#!/usr/bin/env bash
# Upload the Tauri updater bundle (NOT the user-facing installer) + its
# .sig file to Nexus. The pair is what `tauri-plugin-updater` reads via
# the `endpoints` URL — the .sig signs the updater bundle, not the .dmg/
# .msi/.deb that users download from GitHub Releases.
#
# Per Tauri 2.x `createUpdaterArtifacts: true`:
#   - macOS  → `*.app.tar.gz` + `*.app.tar.gz.sig`
#   - Windows→ `*.msi` (NSIS .exe also possible) + `*.msi.sig`
#   - Linux  → `*.AppImage` + `*.AppImage.sig`
#
# Args:
#   $1 PLATFORM  — one of: windows-x86_64 / macos-x86_64 / macos-aarch64 / linux-x86_64
#   $2 VERSION   — tag like v0.1.0 (the leading `v` is stripped)
set -euo pipefail
PLATFORM="$1"
VERSION="$2"
ROOT="src-tauri/target"

case "$PLATFORM" in
  windows-x86_64)
    BUNDLE=$(find "$ROOT" -type f -name '*.msi' -path '*/release/bundle/msi/*' | head -1)
    ;;
  macos-x86_64|macos-aarch64)
    BUNDLE=$(find "$ROOT" -type f -name '*.app.tar.gz' -path '*/release/bundle/macos/*' | head -1)
    ;;
  linux-x86_64)
    BUNDLE=$(find "$ROOT" -type f -name '*.AppImage' -path '*/release/bundle/appimage/*' | head -1)
    ;;
  *)
    echo "::error::unknown PLATFORM '$PLATFORM'"; exit 1 ;;
esac

if [ -z "${BUNDLE:-}" ] || [ ! -f "$BUNDLE" ]; then
  echo "::warning::no updater bundle for $PLATFORM (createUpdaterArtifacts may be off); skipping Nexus upload"
  exit 0
fi

SIG="${BUNDLE}.sig"
if [ ! -f "$SIG" ]; then
  echo "::warning::no .sig next to $BUNDLE (signing may have failed silently); skipping Nexus upload"
  exit 0
fi

# Nexus upload schema requires the signature as a hex-encoded string
# (`bytes.fromhex(...)` on the server). The .sig file itself is plaintext
# (minisign untrusted-comment + base64), so we hex-encode the WHOLE file
# (newlines + comments + payload) before sending. Original implementation
# used `xxd -p` which isn't on stock GHA macOS images; python3 is on all
# four GHA runner images so we use it as the portable hex encoder.
SIG_HEX=$(python3 -c "import sys; print(open(sys.argv[1],'rb').read().hex())" "$SIG")

curl -fSs -X POST \
  -H "Authorization: Bearer $NEXUS_CI_TOKEN" \
  -F "version=${VERSION#v}" \
  -F "signature=${SIG_HEX}" \
  -F "binary=@${BUNDLE}" \
  "https://nexus.obsidian-inc.com/admin/api/vault-sync/releases/${PLATFORM}/upload"
echo
echo "uploaded $(basename "$BUNDLE") + sig for $PLATFORM v${VERSION#v}"
