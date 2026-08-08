#!/usr/bin/env bash
# vault-sync-memory-launch.sh — fail-closed launcher for a MANAGED vault-sync
# instance (spec §5, "Memory Parity — Sync-Owned Architecture + Tiered Guards",
# 2026-08-08 R7).
#
# The unit manager execs THIS script instead of the daemon binary. Before any
# daemon process exists it verifies, in order:
#   (a) the pinned binary's sha256 matches the approved hash in the
#       provisioning file;
#   (b) `<binary> --version` output EXACTLY equals the approved version
#       (>= is forbidden — a newer binary is NOT approved);
#   (c) NEXUS_SYNC_CONFIG_DIR is set, exists, and holds a config.toml whose
#       SINGLE [[sync_roots]] path is NOT inside any vault tree;
#   (d) only then execs the daemon with the environment passed through.
# ANY failed check exits non-zero with a loud message and NO daemon runs.
#
# Inputs:
#   VAULT_SYNC_BIN               path to the pinned daemon binary
#                                (or pass it as $1).
#   VAULT_SYNC_LAUNCH_APPROVED   path to the provisioning file (optional;
#                                default: `launch.approved` beside the binary).
#                                Format — one per line:
#                                    sha256=<hex>
#                                    version=<x.y.z>
#   NEXUS_SYNC_CONFIG_DIR        the managed instance's config dir (required).
#
# Everything after the binary argument is forwarded to the daemon verbatim.

set -euo pipefail

die() {
    echo "FATAL [vault-sync-memory-launch]: $*" >&2
    echo "FATAL [vault-sync-memory-launch]: refusing to start the daemon." >&2
    exit 1
}

# --- resolve the pinned binary -------------------------------------------------
BIN="${VAULT_SYNC_BIN:-${1:-}}"
if [ -n "${1:-}" ] && [ -z "${VAULT_SYNC_BIN:-}" ]; then
    shift
fi
[ -n "$BIN" ] || die "no binary given: set VAULT_SYNC_BIN or pass the binary path as \$1"
[ -f "$BIN" ] || die "pinned binary not found: $BIN"
[ -x "$BIN" ] || die "pinned binary is not executable: $BIN"

# --- resolve + parse the provisioning file --------------------------------------
APPROVED="${VAULT_SYNC_LAUNCH_APPROVED:-$(dirname "$BIN")/launch.approved}"
[ -f "$APPROVED" ] || die "provisioning file not found: $APPROVED (need lines sha256=<hex> and version=<x.y.z>)"

APPROVED_SHA="$(sed -n 's/^sha256=//p' "$APPROVED" | head -n1 | tr -d '[:space:]')"
APPROVED_VERSION="$(sed -n 's/^version=//p' "$APPROVED" | head -n1 | tr -d '[:space:]')"
[ -n "$APPROVED_SHA" ] || die "provisioning file $APPROVED carries no sha256= line"
[ -n "$APPROVED_VERSION" ] || die "provisioning file $APPROVED carries no version= line"

# --- (a) sha256 pin --------------------------------------------------------------
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL_SHA="$(sha256sum "$BIN" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
    ACTUAL_SHA="$(shasum -a 256 "$BIN" | awk '{print $1}')"
else
    die "no sha256sum/shasum available to verify the binary"
fi
if [ "$ACTUAL_SHA" != "$APPROVED_SHA" ]; then
    die "binary hash mismatch for $BIN: got sha256=$ACTUAL_SHA, approved sha256=$APPROVED_SHA (binary changed under the pin — re-provision deliberately)"
fi

# --- (b) exact version equality (>= is FORBIDDEN) --------------------------------
ACTUAL_VERSION="$("$BIN" --version 2>/dev/null | head -n1 | tr -d '[:space:]')" \
    || die "'$BIN --version' failed to run"
if [ "$ACTUAL_VERSION" != "$APPROVED_VERSION" ]; then
    die "version mismatch: '$BIN --version' printed '$ACTUAL_VERSION' but approved version is '$APPROVED_VERSION' (EXACT equality required; a newer binary is not an approved binary)"
fi

# --- (c) managed config dir sanity ------------------------------------------------
[ -n "${NEXUS_SYNC_CONFIG_DIR:-}" ] || die "NEXUS_SYNC_CONFIG_DIR is not set — a managed instance must not run against the default (vault) config"
[ -d "$NEXUS_SYNC_CONFIG_DIR" ] || die "NEXUS_SYNC_CONFIG_DIR does not exist: $NEXUS_SYNC_CONFIG_DIR"
CFG="$NEXUS_SYNC_CONFIG_DIR/config.toml"
[ -f "$CFG" ] || die "no config.toml in NEXUS_SYNC_CONFIG_DIR ($CFG) — pair the instance before wiring the unit"

# Exactly ONE [[sync_roots]] block.
ROOT_COUNT="$(grep -c '^\[\[sync_roots\]\]' "$CFG" || true)"
[ "$ROOT_COUNT" = "1" ] || die "config $CFG must declare exactly ONE [[sync_roots]] block (found $ROOT_COUNT)"

# Its path. First path= line after the block; strip quotes + whitespace.
MANAGED_ROOT="$(awk -F'=' '/^\[\[sync_roots\]\]/{inroot=1; next} inroot && /^path[[:space:]]*=/{gsub(/^[[:space:]]*"|"[[:space:]]*$/, "", $2); gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); gsub(/^"|"$/, "", $2); print $2; exit}' "$CFG")"
[ -n "$MANAGED_ROOT" ] || die "could not read sync_roots[0].path from $CFG"

# The managed root must NOT live inside any vault tree.
case "$MANAGED_ROOT" in
    */vaults/*|*/Vaults/*)
        die "managed sync root '$MANAGED_ROOT' is inside a vault tree (contains /vaults/) — a managed instance must never point at vault content"
        ;;
esac

# ...and must not equal / live under the DEFAULT (vault) config's first sync
# root, when that default config exists on this host.
DEFAULT_CFG="${XDG_CONFIG_HOME:-$HOME/.config}/nexus-vault-sync/config.toml"
if [ -f "$DEFAULT_CFG" ]; then
    VAULT_ROOT="$(awk -F'=' '/^\[\[sync_roots\]\]/{inroot=1; next} inroot && /^path[[:space:]]*=/{gsub(/^[[:space:]]*"|"[[:space:]]*$/, "", $2); gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); gsub(/^"|"$/, "", $2); print $2; exit}' "$DEFAULT_CFG")"
    if [ -n "$VAULT_ROOT" ]; then
        if [ "$MANAGED_ROOT" = "$VAULT_ROOT" ]; then
            die "managed sync root '$MANAGED_ROOT' EQUALS the vault instance's sync root ('$VAULT_ROOT' from $DEFAULT_CFG) — two daemons on one tree is the storm scenario"
        fi
        case "$MANAGED_ROOT" in
            "$VAULT_ROOT"/*)
                die "managed sync root '$MANAGED_ROOT' is INSIDE the vault instance's sync root ('$VAULT_ROOT' from $DEFAULT_CFG)"
                ;;
        esac
    fi
fi

# --- (d) all gates green — exec the daemon (env passes through exec) --------------
echo "[vault-sync-memory-launch] gates passed: sha256 ok, version=$ACTUAL_VERSION exact, config dir $NEXUS_SYNC_CONFIG_DIR sane (root: $MANAGED_ROOT). exec'ing daemon." >&2
exec "$BIN" --silent "$@"
