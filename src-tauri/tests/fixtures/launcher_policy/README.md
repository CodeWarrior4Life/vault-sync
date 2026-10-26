# launcher_policy fixtures (T-CS3-24, gate K2)

`gate-lp-adv/` is a copy of the lsec gate's adversarial fixture set
(`~/.cache/lsec-9325/lp-adv-1793004436` on link, row
LSEC-9325-B6D8-CS3-PR24-PASS-W-C on TKT-b6d8c4ba). The originals are left in
place. `@FIX@` replaces the fixture-set root
(`~/.cache/lsec-9325/lp-adv-1793004436`) in the x2/x3 plists; the test stages
each fixture under a temp root and substitutes that root.

`verify-launcher-policy.sh` reads systemd's EFFECTIVE values from
`systemctl --user show`, not from unit files. Each systemd fixture therefore
carries `systemctl-show.txt`: the `systemctl --user show -p LoadState
-p UnitFileState -p Restart -p RestartPreventExitStatus -p SuccessExitStatus
-p ExecStart -p FragmentPath -p ActiveState -p MainPID -p Environment` output that unit set produces (gate L1: the stubs that pass the load checks carry `ActiveState=active` and a `MainPID`) (x1: the `/dev/null`
symlink in the first dir masks the unit). Fixture mode passes it with
`--systemd-show`, so CI needs no live units. The unit files are kept as the
record of what each stub represents. In the stubs, `/opt/x/daemon` is the
daemon's path, and the tests pass `--daemon /opt/x/daemon`.

launchd runs in the tests pass `--launchctl-print` (and optionally
`--launchctl-disabled`) stubs for the live agent state (gate L1). An empty
print stub means the agent is not loaded.
