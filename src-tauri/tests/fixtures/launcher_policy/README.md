# launcher_policy fixtures (T-CS3-24, gate K2)

`gate-lp-adv/` is a copy of the lsec gate's adversarial fixture set
(`~/.cache/lsec-9325/lp-adv-1793004436` on link, row
LSEC-9325-B6D8-CS3-PR24-PASS-W-C on TKT-b6d8c4ba). The originals are left in
place. `@FIX@` replaces the absolute fixture path in the x2/x3 plists; the test
substitutes the temp copy's path.

`verify-launcher-policy.sh` reads systemd's EFFECTIVE values from
`systemctl --user show`, not from unit files. Each systemd fixture therefore
carries `systemctl-show.txt`: the `systemctl --user show -p LoadState
-p UnitFileState -p Restart -p RestartPreventExitStatus -p SuccessExitStatus
-p ExecStart -p FragmentPath` output that unit set produces (x1: the `/dev/null`
symlink in the first dir masks the unit). Fixture mode passes it with
`--systemd-show`, so CI needs no live units. The unit files are kept as the
record of what each stub represents. In the stubs, `/opt/x/daemon` is the
daemon's path, and the tests pass `--daemon /opt/x/daemon`.
