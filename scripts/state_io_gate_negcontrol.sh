#!/usr/bin/env bash
# Unit CS-2 (TKT-23091e72) gate self-test: every planted mutation in
# src-tauri/tests/fixtures/state_io_neg/*.patch must make its gate FAIL.
#
#   gate1-*  : gate 1 (clippy disallowed-methods). `cargo clippy --lib -- -D
#              warnings` on a planted scratch copy must exit non-zero WITH a
#              "use of a disallowed method" diagnostic (a compile error does
#              not count as a catch).
#   lint-*   : CS-3 T-CS3-11 (`#![deny(clippy::await_holding_lock)]` at the
#              crate roots). `cargo clippy --lib -- -D warnings` on the planted
#              copy must exit non-zero WITH an await_holding_lock diagnostic.
#   others   : gates 2/3 and C6/C7/C8/C10 (tests/state_io_gate.rs). The gate
#              test, built once from the real tree, is run against the
#              planted scratch copy via STATE_IO_GATE_ROOT and must fail.
#
# Positive control first: the unplanted tree passes the gate test. The job
# fails if any planted mutation passes. Runs in CI on ubuntu-latest.
set -uo pipefail
cd "$(dirname "$0")/../src-tauri"
TREE=$PWD
FIX=$TREE/tests/fixtures/state_io_neg
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$TREE/target}"

if ! cargo test --test state_io_gate -- --exact gate_holds_on_this_tree; then
  echo "negcontrol: the gate fails on the UNPLANTED tree; fix that first" >&2
  exit 1
fi

fails=0
total=0
for p in "$FIX"/*.patch; do
  total=$((total + 1))
  name=$(basename "$p" .patch)
  kind=${name%%-*}
  scratch=$(mktemp -d)
  # The whole repo (tauri's build needs ../src, the frontendDist), minus
  # build output and VCS metadata.
  tar -C "$TREE/.." --exclude=./src-tauri/target --exclude=./src-tauri/gen \
    --exclude=./node_modules --exclude=./.git -cf - . | tar -C "$scratch" -xf -
  if ! (cd "$scratch/src-tauri" && git apply -p1 "$p"); then
    echo "FAIL $name: the plant does not apply" >&2
    fails=$((fails + 1))
    rm -rf "$scratch"
    continue
  fi
  if [ "$kind" = "lint" ]; then
    (cd "$scratch/src-tauri" && cargo clippy --lib -- -D warnings) >"$scratch/out" 2>&1
    rc=$?
    if ! grep -q "await_holding_lock" "$scratch/out"; then
      echo "     $name: clippy rc=$rc without an await_holding_lock diagnostic:" >&2
      tail -20 "$scratch/out" >&2
      rc=0
    fi
  elif [ "$kind" = "gate1" ]; then
    (cd "$scratch/src-tauri" && cargo clippy --lib -- -D warnings) >"$scratch/out" 2>&1
    rc=$?
    if ! grep -q "use of a disallowed method" "$scratch/out"; then
      echo "     $name: clippy rc=$rc without a disallowed-method diagnostic:" >&2
      tail -20 "$scratch/out" >&2
      rc=0
    fi
  else
    STATE_IO_GATE_ROOT="$scratch/src-tauri" \
      cargo test --test state_io_gate -- --exact gate_holds_on_this_tree >"$scratch/out" 2>&1
    rc=$?
  fi
  if [ "$rc" -eq 0 ]; then
    echo "FAIL $name: the planted mutation PASSED its gate" >&2
    fails=$((fails + 1))
  else
    why=$( (grep -m1 -e 'use of a disallowed method' -e 'await_holding_lock' "$scratch/out"; grep -A1 'state_io gate violations' "$scratch/out" | tail -1) | head -1 | head -c 220)
    echo "ok   $name: caught (rc=$rc) :: $why"
  fi
  rm -rf "$scratch"
done

echo "negcontrol: $((total - fails))/$total plants caught"
[ "$fails" -eq 0 ]
