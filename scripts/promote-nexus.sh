#!/usr/bin/env bash
# Promote the staged Nexus release for PLATFORM to LIVE (is_current=TRUE) so the
# daemon updater's /admin/api/vault-sync/releases/<platform>/latest endpoint
# serves it. WITHOUT this, upload-to-nexus.sh leaves the release STAGED forever
# (the S484 staged->promote gate) and auto-update never advances past the last
# manually-promoted version — which is exactly why the fleet was stuck on v0.4.4
# (S493). Runs only after the HARD-GATE signature verify, so a broken/unsigned
# build never reaches "latest".
# Args: $1 PLATFORM (darwin-x86_64|darwin-aarch64|windows-x86_64|linux-x86_64)
#       $2 VERSION  (tag like v0.4.13; leading v is stripped)
set -euo pipefail
PLATFORM="$1"
VERSION="${2#v}"
curl -fSs --retry 3 --retry-delay 5 -X POST \
  -H "Authorization: Bearer $NEXUS_CI_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"version\":\"${VERSION}\"}" \
  "https://nexus.obsidian-inc.com/admin/api/vault-sync/releases/${PLATFORM}/promote"
echo
echo "promoted $PLATFORM v${VERSION} to live (is_current=TRUE)"
