#!/usr/bin/env bash
#
# Nexus Vault Sync — smooth installer for macOS / Linux.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/CodeWarrior4Life/vault-sync/main/install.sh | bash
#
# Or specify a version explicitly (defaults to the latest published tag):
#   curl -fsSL https://raw.githubusercontent.com/CodeWarrior4Life/vault-sync/main/install.sh | VERSION=v0.1.4 bash
#
# What it does:
#   1. Resolve target platform (macOS arm64 / macOS x86_64 / Linux x86_64).
#   2. Download the matching .dmg / .AppImage from the GH Release.
#   3. macOS: mount, strip com.apple.quarantine, copy .app to /Applications,
#      ad-hoc codesign, eject. Then `open -a "Nexus Vault Sync"`.
#   4. Linux: drop .AppImage in ~/Applications, chmod +x, register a .desktop
#      entry, launch.
#
# Why this exists: vault-sync .dmg / .AppImage are unsigned (we don't pay for
# an Apple Developer ID). Without this script, macOS Gatekeeper marks the .app
# "damaged and can't be opened" and the user has to do the xattr / codesign
# dance manually. This automates it.

set -euo pipefail

REPO="CodeWarrior4Life/vault-sync"
APP_DISPLAY="Nexus Vault Sync"

log()  { printf '\033[1;36m[install]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[install]\033[0m %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Darwin)
    case "$ARCH" in
      arm64)  ASSET="aarch64.dmg" ;;
      x86_64) ASSET="x64.dmg" ;;
      *)      die "unsupported macOS arch: $ARCH" ;;
    esac
    PLATFORM="macOS-$ARCH"
    ;;
  Linux)
    case "$ARCH" in
      x86_64) ASSET="amd64.AppImage" ;;
      *)      die "unsupported Linux arch: $ARCH (Lattice only ships x86_64 today)" ;;
    esac
    PLATFORM="Linux-$ARCH"
    ;;
  *)
    die "unsupported OS: $OS — for Windows use the .msi installer from the GH Release page directly"
    ;;
esac

# Resolve version: env override > latest GH release tag.
VERSION="${VERSION:-}"
if [ -z "$VERSION" ]; then
  log "resolving latest release tag…"
  VERSION=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep -E '"tag_name":' | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
  [ -z "$VERSION" ] && die "could not resolve latest version from GitHub API"
fi
log "platform=$PLATFORM, version=$VERSION, asset=$ASSET"

VERSION_NO_V="${VERSION#v}"
FILE="Nexus.Vault.Sync_${VERSION_NO_V}_${ASSET}"
URL="https://github.com/$REPO/releases/download/$VERSION/$FILE"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

log "downloading $FILE …"
curl -fSL --progress-bar -o "$TMP/$FILE" "$URL"

case "$OS" in
  Darwin)
    log "stripping quarantine + mounting dmg…"
    xattr -dr com.apple.quarantine "$TMP/$FILE" 2>/dev/null || true
    # hdiutil attach output: tab-separated columns. Last column is mount path
    # when present. Grep the line that has /Volumes/ then tail-cut to extract.
    MOUNT_LINE=$(hdiutil attach "$TMP/$FILE" -nobrowse | grep -F '/Volumes/' | tail -1)
    MOUNT="${MOUNT_LINE#*	/Volumes/}"
    MOUNT="/Volumes/${MOUNT}"
    [ -d "$MOUNT" ] || die "dmg did not mount as expected: '$MOUNT' (from line: '$MOUNT_LINE')"
    APP_SRC="$MOUNT/$APP_DISPLAY.app"
    APP_DST="/Applications/$APP_DISPLAY.app"
    log "removing prior install (if any) + copying to /Applications…"
    rm -rf "$APP_DST"
    cp -R "$APP_SRC" "$APP_DST"
    log "stripping quarantine + ad-hoc codesign on installed app…"
    xattr -dr com.apple.quarantine "$APP_DST" 2>/dev/null || true
    codesign --force --deep --sign - "$APP_DST" 2>&1 | grep -v '^$' || true
    hdiutil detach "$MOUNT" -quiet || true
    log "launching…"
    open -a "$APP_DISPLAY"
    log "done. menu-bar icon should appear within a couple seconds."
    ;;
  Linux)
    APP_DIR="$HOME/Applications"
    APP_DST="$APP_DIR/Nexus-Vault-Sync.AppImage"
    mkdir -p "$APP_DIR"
    log "installing to $APP_DST…"
    cp "$TMP/$FILE" "$APP_DST"
    chmod +x "$APP_DST"
    DESK="$HOME/.local/share/applications/nexus-vault-sync.desktop"
    mkdir -p "$(dirname "$DESK")"
    cat > "$DESK" <<EOF
[Desktop Entry]
Type=Application
Name=$APP_DISPLAY
Comment=Lattice Vault Sync daemon
Exec=$APP_DST
Terminal=false
Categories=Utility;
StartupNotify=true
EOF
    log "launching…"
    "$APP_DST" &
    log "done. tray icon should appear within a couple seconds."
    ;;
esac
