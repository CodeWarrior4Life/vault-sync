#!/usr/bin/env bash
# test-vault-sync-memory-launch.sh — proves the managed launcher removes the
# VAULT_SYNC_TEST_* fault-injection hook before it execs the daemon (B4, M1).
#
# Runs the REAL launcher against a fake pinned daemon that dumps what it
# receives. Exit 0 only if no VAULT_SYNC_TEST_* variable reaches the daemon
# and an unrelated variable still does (the strip is targeted, not a wipe).
#
# Usage: test-vault-sync-memory-launch.sh [path/to/vault-sync-memory-launch.sh]
# Negative control: a launcher copy with the unset loop deleted must FAIL.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
LAUNCH="${1:-$here/vault-sync-memory-launch.sh}"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$T/bin" "$T/cfg" "$T/xdg" "$T/root/Mainframe"

# Fake daemon: --version prints the approved version; otherwise it reports
# every VAULT_SYNC_TEST_* name it can see, and one unrelated variable.
cat > "$T/bin/vault-sync" <<'SH'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then echo "0.5.0"; exit 0; fi
echo "DAEMON_HOOK_NAMES=$(env | sed -n 's/^\(VAULT_SYNC_TEST_[A-Z_]*\)=.*/\1/p' | tr '\n' ' ')"
echo "DAEMON_KEEP=${KEEP_ME:-missing}"
SH
chmod +x "$T/bin/vault-sync"
sha="$(sha256sum "$T/bin/vault-sync" | awk '{print $1}')"
printf 'sha256=%s\nversion=0.5.0\n' "$sha" > "$T/bin/launch.approved"
printf '[[sync_roots]]\npath = "%s"\n' "$T/root/Mainframe" > "$T/cfg/config.toml"

# The leak under test: the trigger AND the test mode bound to this very
# config dir, exactly as a leaked user-wide environment would carry them.
hook=(
    "VAULT_SYNC_TEST_END_REQUIRED_TASK=drain_loop:0@5"
    "VAULT_SYNC_TEST_MODE=$T/cfg"
    "VAULT_SYNC_TEST_OTHER=1"
)

out="$(env -i PATH="$PATH" HOME="$T" XDG_CONFIG_HOME="$T/xdg" \
    VAULT_SYNC_BIN="$T/bin/vault-sync" \
    VAULT_SYNC_LAUNCH_APPROVED="$T/bin/launch.approved" \
    NEXUS_SYNC_CONFIG_DIR="$T/cfg" KEEP_ME=yes \
    "${hook[@]}" \
    bash "$LAUNCH" 2>"$T/err")" || { cat "$T/err" >&2; fail "launcher exited non-zero"; }

echo "$out"
grep -q '^DAEMON_KEEP=yes$' <<<"$out" || fail "an unrelated variable did not reach the daemon"
grep -q '^DAEMON_HOOK_NAMES= *$' <<<"$out" \
    || fail "VAULT_SYNC_TEST_* reached the managed daemon (a leaked drill would exit 70 on every relaunch)"
grep -q 'gates passed' "$T/err" || fail "launcher did not reach exec (gates not passed)"
echo "PASS: the managed launcher strips VAULT_SYNC_TEST_* before exec"
