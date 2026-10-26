//! Unit CS-3 (TKT-b6d8c4ba) T-CS3-24 (gates C5 and C10), hardened by gate K2
//! (row LSEC-9325-B6D8-CS3-PR24-PASS-W-C): the fixtures for
//! `scripts/verify-launcher-policy.sh`. Every failing fixture returns its own
//! exit code (section 4.3 item 11) and every positive fixture returns 0.
//!
//! systemd fixtures run in fixture mode: `--systemd-show` hands the script
//! the `systemctl --user show` output, so CI needs no live units. The gate's
//! adversarial set lives in `tests/fixtures/launcher_policy/gate-lp-adv`
//! (see its README). The per-host live run (link, Trinity, icarus) is a
//! fleet-release item (gate E1), never run from a test.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::process::Command;

const NAME: &str = "vs-fx";
const GATE_DAEMON: &str = "/opt/x/daemon";

fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("scripts")
        .join("verify-launcher-policy.sh")
}

fn gate_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/launcher_policy/gate-lp-adv")
}

/// Copy one gate fixture into a temp dir laid out like the gate's set
/// (`<root>/<fixture>/...`), substituting `@FIX@` with that root (x2/x3
/// plists name their start.sh by absolute path).
fn stage(fixture: &str) -> (tempfile::TempDir, PathBuf) {
    let t = tempfile::TempDir::new().unwrap();
    let dst = t.path().join(fixture);
    copy_tree(&gate_dir().join(fixture), &dst);
    for f in walk(&dst) {
        if let Ok(s) = std::fs::read_to_string(&f) {
            if s.contains("@FIX@") {
                std::fs::write(&f, s.replace("@FIX@", &t.path().display().to_string())).unwrap();
            }
        }
    }
    (t, dst)
}

fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let p = e.path();
        let to = dst.join(e.file_name());
        let ft = std::fs::symlink_metadata(&p).unwrap().file_type();
        if ft.is_dir() {
            copy_tree(&p, &to);
        } else if ft.is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(&p).unwrap(), &to).unwrap();
        } else {
            std::fs::copy(&p, &to).unwrap();
        }
    }
}

fn walk(d: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(d).unwrap() {
        let p = e.unwrap().path();
        if std::fs::symlink_metadata(&p).unwrap().is_dir() {
            v.extend(walk(&p));
        } else {
            v.push(p);
        }
    }
    v
}

enum Mode<'a> {
    /// `--systemd-show <file>`
    Systemd(&'a Path),
    /// `--agent-dir <dir>`
    Launchd(&'a Path),
}

fn run(mode: Mode, daemon: Option<&str>) -> (i32, String) {
    run_with(mode, daemon, &[])
}

/// A running launchd agent's `launchctl print` (L1): loaded, with a PID.
const LC_RUNNING: &str =
    "gui/501/vs-fx = {\n\tactive count = 1\n\tstate = running\n\tpid = 4242\n}\n";

/// `extra` is appended verbatim. A launchd run with no `--launchctl-print`
/// in `extra` gets a running-agent stub (L1), so the K2 cases keep testing
/// the plist rules.
fn run_with(mode: Mode, daemon: Option<&str>, extra: &[&str]) -> (i32, String) {
    let mut c = Command::new("bash");
    c.arg(script()).args(["--name", NAME]);
    if let Some(d) = daemon {
        c.args(["--daemon", d]);
    }
    match mode {
        Mode::Systemd(f) => {
            c.arg("--systemd-show").arg(f);
        }
        Mode::Launchd(d) => {
            c.arg("--agent-dir").arg(d);
            if !extra.contains(&"--launchctl-print") {
                let stub = d.parent().unwrap().join("lc-print-running.txt");
                std::fs::write(&stub, LC_RUNNING).unwrap();
                c.arg("--launchctl-print").arg(stub);
            }
        }
    };
    c.args(extra);
    let out = c.output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    )
}

fn expect(what: &str, got: (i32, String), want: i32) {
    assert_eq!(
        got.0, want,
        "{what}: expected exit {want}, got {}\n{}",
        got.0, got.1
    );
}

/// Run one gate fixture: systemd (its `systemctl-show.txt`) when it has
/// one, launchd (its `a/` agent dir) otherwise.
fn gate(fixture: &str, want: i32) -> String {
    let (_t, dir) = stage(fixture);
    let show = dir.join("systemctl-show.txt");
    let got = if show.exists() {
        run(Mode::Systemd(&show), Some(GATE_DAEMON))
    } else {
        run(Mode::Launchd(&dir.join("a")), Some(GATE_DAEMON))
    };
    let out = got.1.clone();
    expect(fixture, got, want);
    out
}

// ---------------------------------------------------------------------------
// The gate's adversarial fixtures (K2): each false-pass is now a fail.
// ---------------------------------------------------------------------------

/// x1: a masked unit (`/dev/null` symlink in the first unit dir, a real unit
/// in a later dir): `LoadState=masked`, so nothing supervises the daemon: 10.
/// Mutation: drop the masked check.
#[test]
fn t_cs3_24_k2_x1_masked_unit() {
    let out = gate("x1", 10);
    // The masked check itself refused (a masked unit is also not active, so
    // the L1 ActiveState check alone would give 10 too).
    assert!(out.contains("the unit is masked"), "{out}");
}

/// x2: a bash wrapper whose script runs `open` (no exec): 13.
/// Mutation: accept any interpreter script.
#[test]
fn t_cs3_24_k2_x2_bash_wrapper_open() {
    let out = gate("x2", 13);
    // The script was resolved and read: the failure is its own reason.
    assert!(out.contains("does not end by exec'ing the daemon"), "{out}");
}

/// x3: a script whose last line is `exec /usr/bin/open`: 13.
/// Mutation: accept any script whose last line is an `exec`.
#[test]
fn t_cs3_24_k2_x3_exec_open() {
    let out = gate("x3", 13);
    assert!(
        out.contains("execs /usr/bin/open, not the daemon binary"),
        "{out}"
    );
}

/// x4: `Restart=on-failure` + `SuccessExitStatus=SOFTWARE IOERR`: 70 and 74
/// are "successful" exits, never restarted: 11.
/// Mutations: drop the SuccessExitStatus check; drop the name mapping.
#[test]
fn t_cs3_24_k2_x4_success_exit_status_names() {
    let _ = gate("x4", 11);
}

/// x5: `RestartPreventExitStatus=CONFIG`. CONFIG is EX_CONFIG = 78, which
/// the contract allows: 0 (the pre-K2 script false-failed it with 12).
/// Mutation: drop the name mapping (12).
#[test]
fn t_cs3_24_k2_x5_prevent_config_is_78() {
    let _ = gate("x5", 0);
}

/// x6: `/usr/bin/env open -a X`: 13.
/// Mutation: drop the launchd program allowlist.
#[test]
fn t_cs3_24_k2_x6_env_open() {
    let _ = gate("x6", 13);
}

/// x7: launchd `Disabled=true`: launchd neither runs nor supervises it: 10.
/// Mutation: drop the Disabled check.
#[test]
fn t_cs3_24_k2_x7_launchd_disabled() {
    let _ = gate("x7", 10);
}

/// The gate's spec fixtures keep their spec codes.
#[test]
fn t_cs3_24_gate_spec_fixtures() {
    for (f, want) in [
        ("s0a", 0),
        ("s0b", 0),
        ("s10", 10),
        ("s11", 11),
        ("s11b", 11),
        ("s12", 12),
        ("s13", 13),
    ] {
        let _ = gate(f, want);
    }
    // s10 on launchd too: no agent at all.
    let (_t, dir) = stage("s10");
    expect(
        "s10 launchd",
        run(Mode::Launchd(&dir.join("a")), Some(GATE_DAEMON)),
        10,
    );
}

// ---------------------------------------------------------------------------
// Local fixtures (allowlist, numeric and symbolic statuses, positives)
// ---------------------------------------------------------------------------

const DAEMON: &str = "/opt/vs/vault-sync-daemon";

fn show(dir: &Path, restart: &str, prevent: &str, success: &str, exec_path: &str) -> PathBuf {
    let p = dir.join("show.txt");
    std::fs::write(
        &p,
        format!(
            "LoadState=loaded\nUnitFileState=enabled\nActiveState=active\nMainPID=4242\nRestart={restart}\nRestartPreventExitStatus={prevent}\nSuccessExitStatus={success}\nExecStart={{ path={exec_path} ; argv[]={exec_path} --silent ; ignore_errors=no }}\nFragmentPath=/u/vs-fx.service\n"
        ),
    )
    .unwrap();
    p
}

fn plist(dir: &Path, extra: &str, args: &[&str]) {
    let args: String = args
        .iter()
        .map(|a| format!("<string>{a}</string>"))
        .collect();
    std::fs::write(
        dir.join(format!("{NAME}.plist")),
        format!(
            "<?xml version=\"1.0\"?>\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{NAME}</string>\n<key>ProgramArguments</key><array>{args}</array>\n<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>\n{extra}\n</dict></plist>\n"
        ),
    )
    .unwrap();
}

/// systemd: numeric 70/74 in SuccessExitStatus fail under on-failure AND
/// always; a signal-only list passes; an unknown name fails closed (2); an
/// ExecStart that is a shell wrapper fails (13) even when it would exec the
/// daemon; 74 in the prevent list is 12; the daemon by its default basename
/// passes; a not-found unit is 10.
/// Mutation: drop the systemd ExecStart allowlist (the wrapper passes).
#[test]
fn t_cs3_24_k2_systemd_local() {
    let t = tempfile::TempDir::new().unwrap();
    let d = t.path();
    expect(
        "SuccessExitStatus=70",
        run(
            Mode::Systemd(&show(d, "on-failure", "", "70", DAEMON)),
            None,
        ),
        11,
    );
    expect(
        "SuccessExitStatus=74 with Restart=always",
        run(Mode::Systemd(&show(d, "always", "", "0 74", DAEMON)), None),
        11,
    );
    expect(
        "SuccessExitStatus=SIGTERM",
        run(
            Mode::Systemd(&show(d, "on-failure", "78", "SIGTERM", DAEMON)),
            None,
        ),
        0,
    );
    expect(
        "SuccessExitStatus=BOGUS",
        run(Mode::Systemd(&show(d, "always", "", "BOGUS", DAEMON)), None),
        2,
    );
    expect(
        "ExecStart via /bin/bash",
        run(Mode::Systemd(&show(d, "always", "", "", "/bin/bash")), None),
        13,
    );
    expect(
        "ExecStart via /usr/bin/env",
        run(
            Mode::Systemd(&show(d, "always", "", "", "/usr/bin/env")),
            None,
        ),
        13,
    );
    expect(
        "RestartPreventExitStatus=74",
        run(
            Mode::Systemd(&show(d, "on-failure", "74", "", DAEMON)),
            None,
        ),
        12,
    );
    expect(
        "the daemon by its default basename",
        run(Mode::Systemd(&show(d, "always", "", "", DAEMON)), None),
        0,
    );
    expect(
        "--daemon names another path",
        run(
            Mode::Systemd(&show(d, "always", "", "", DAEMON)),
            Some("/elsewhere/vault-sync-daemon"),
        ),
        13,
    );
    let nf = d.join("nf.txt");
    std::fs::write(&nf, "LoadState=not-found\n").unwrap();
    expect("not-found unit", run(Mode::Systemd(&nf), None), 10);
}

/// launchd: the daemon binary directly passes (also with an explicit
/// Disabled=false); an interpreter or shebang script whose last command
/// execs the daemon passes; a script that execs another binary fails (13);
/// no agent is 10.
#[test]
fn t_cs3_24_k2_launchd_local() {
    let t = tempfile::TempDir::new().unwrap();
    let a = t.path().join("a");
    std::fs::create_dir_all(&a).unwrap();
    plist(&a, "", &[DAEMON, "--silent"]);
    expect("direct daemon", run(Mode::Launchd(&a), None), 0);
    plist(&a, "<key>Disabled</key><false/>", &[DAEMON]);
    expect("Disabled=false", run(Mode::Launchd(&a), None), 0);

    let good = t.path().join("good.sh");
    std::fs::write(
        &good,
        format!("#!/bin/sh\n# launch\nexport X=1\nexec {DAEMON} --silent\n"),
    )
    .unwrap();
    plist(&a, "", &["/bin/sh", good.to_str().unwrap()]);
    expect(
        "interpreter script exec daemon",
        run(Mode::Launchd(&a), None),
        0,
    );
    plist(&a, "", &[good.to_str().unwrap()]);
    expect(
        "shebang script exec daemon",
        run(Mode::Launchd(&a), None),
        0,
    );

    let other = t.path().join("other.sh");
    std::fs::write(&other, "#!/bin/sh\nexec /opt/vs/helper --silent\n").unwrap();
    plist(&a, "", &[other.to_str().unwrap()]);
    expect("script exec other binary", run(Mode::Launchd(&a), None), 13);

    let empty = t.path().join("none");
    expect("no agent", run(Mode::Launchd(&empty), None), 10);
}

/// Usage errors return 2.
#[test]
fn t_cs3_24_usage_error() {
    let out = Command::new("bash")
        .arg(script())
        .arg("--bogus")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

// ---------------------------------------------------------------------------
// L1: live state (row LSEC-9325-B6D8-CS3-K2-PR25-PASS-W-C)
// ---------------------------------------------------------------------------

/// A systemd show stub with an explicit live state and ExecStart.
fn show_live(
    dir: &Path,
    file: &str,
    active: &str,
    mainpid: &str,
    exec: &str,
    env: &str,
) -> PathBuf {
    let p = dir.join(file);
    std::fs::write(
        &p,
        format!(
            "LoadState=loaded\nUnitFileState=enabled\nActiveState={active}\nMainPID={mainpid}\nRestart=always\nRestartPreventExitStatus=\nSuccessExitStatus=\nExecStart={exec}\nEnvironment={env}\nFragmentPath=/u/vs-fx.service\n"
        ),
    )
    .unwrap();
    p
}

fn exec_of(argv: &str) -> String {
    let path = argv.split_whitespace().next().unwrap();
    format!("{{ path={path} ; argv[]={argv} ; ignore_errors=no ; start_time=[n/a] ; pid=4242 ; code=(null) ; status=0/0 }}")
}

/// L1 systemd: a loaded but inactive unit is 10.
/// Mutation: drop the ActiveState check.
#[test]
fn t_cs3_24_l1_systemd_inactive() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(t.path(), "s.txt", "inactive", "0", &exec_of(DAEMON), "");
    expect("ActiveState=inactive", run(Mode::Systemd(&f), None), 10);
    let f = show_live(t.path(), "s2.txt", "failed", "4242", &exec_of(DAEMON), "");
    expect("ActiveState=failed", run(Mode::Systemd(&f), None), 10);
}

/// L1 systemd: active but MainPID=0 is 10.
/// Mutation: drop the MainPID check.
#[test]
fn t_cs3_24_l1_systemd_mainpid_zero() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(t.path(), "s.txt", "active", "0", &exec_of(DAEMON), "");
    expect("MainPID=0", run(Mode::Systemd(&f), None), 10);
}

fn lc_case(t: &Path, print: &str, disabled: Option<&str>) -> (i32, String) {
    let a = t.join("a");
    std::fs::create_dir_all(&a).unwrap();
    plist(&a, "", &[DAEMON, "--silent"]);
    let p = t.join("print.txt");
    std::fs::write(&p, print).unwrap();
    let mut extra = vec!["--launchctl-print".to_string(), p.display().to_string()];
    if let Some(d) = disabled {
        let f = t.join("disabled.txt");
        std::fs::write(&f, d).unwrap();
        extra.push("--launchctl-disabled".into());
        extra.push(f.display().to_string());
    }
    let extra: Vec<&str> = extra.iter().map(String::as_str).collect();
    run_with(Mode::Launchd(&a), None, &extra)
}

/// L1 launchd: an agent that `launchctl print` cannot find (booted out) is
/// 10, even though its plist is fine.
/// Mutation: drop the loaded check.
#[test]
fn t_cs3_24_l1_launchd_booted_out() {
    let t = tempfile::TempDir::new().unwrap();
    let (rc, out) = lc_case(t.path(), "", None);
    assert_eq!(rc, 10, "{out}");
    // The loaded check itself must be what refused (the PID check alone
    // would also give 10 for an empty print).
    assert!(out.contains("found no loaded agent"), "{out}");
}

/// L1 launchd: `launchctl print-disabled` listing the label as disabled
/// (`=> disabled` or `=> true`) is 10.
/// Mutation: drop the print-disabled check.
#[test]
fn t_cs3_24_l1_launchd_print_disabled() {
    let t = tempfile::TempDir::new().unwrap();
    for form in ["disabled", "true"] {
        let d = format!(
            "disabled services = {{\n\t\"com.other\" => enabled\n\t\"{NAME}\" => {form}\n}}\n"
        );
        expect(
            &format!("print-disabled => {form}"),
            lc_case(t.path(), LC_RUNNING, Some(&d)),
            10,
        );
    }
}

/// L1 launchd: a loaded agent with no PID (not running) is 10.
/// Mutation: drop the PID check.
#[test]
fn t_cs3_24_l1_launchd_no_pid() {
    let t = tempfile::TempDir::new().unwrap();
    let print = "gui/501/vs-fx = {\n\tactive count = 0\n\tstate = not running\n}\n";
    expect("no PID", lc_case(t.path(), print, None), 10);
}

/// L1 PASS: an active unit with a MainPID; a loaded, enabled agent with a
/// PID (print-disabled lists it as enabled).
#[test]
fn t_cs3_24_l1_active_pass() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(t.path(), "s.txt", "active", "4242", &exec_of(DAEMON), "");
    expect("active unit", run(Mode::Systemd(&f), None), 0);
    let d = format!("disabled services = {{\n\t\"{NAME}\" => enabled\n\t\"x\" => disabled\n}}\n");
    expect("running agent", lc_case(t.path(), LC_RUNNING, Some(&d)), 0);
}

// ---------------------------------------------------------------------------
// L2: pass-through wrappers only via an exact --via path
// ---------------------------------------------------------------------------

const CAGE: &str = "/usr/bin/cage";
const APPIMAGE: &str = "/home/u/Applications/Nexus-Vault-Sync.AppImage";
const MEM_APPIMAGE: &str = "/home/u/.local/libexec/nexus-vault-sync-memory.AppImage";

/// L2: link's main shape, `cage -- <AppImage>`: 0 with `--via /usr/bin/cage`
/// (and the receipt line says so), 13 without it (no default allowlist).
/// Mutation: a default wrapper allowlist (cage accepted without --via).
#[test]
fn t_cs3_24_l2_cage_appimage() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(
        t.path(),
        "s.txt",
        "active",
        "4242",
        &exec_of(&format!("{CAGE} -- {APPIMAGE}")),
        "",
    );
    let (rc, out) = run_with(Mode::Systemd(&f), None, &["--via", CAGE]);
    assert_eq!(rc, 0, "{out}");
    assert!(
        out.contains("wrapper /usr/bin/cage accepted via --via (exact path)"),
        "{out}"
    );
    expect(
        "cage -> AppImage without --via",
        run(Mode::Systemd(&f), None),
        13,
    );
}

/// L2: link's memory shape, `cage -- launch.sh` where launch.sh ends with
/// `exec "$BIN" --silent "$@"` and `BIN="${VAULT_SYNC_BIN:-${1:-}}"`, the
/// AppImage coming from the unit's Environment: 0 with --via, 13 without.
#[test]
fn t_cs3_24_l2_cage_launch_sh() {
    let t = tempfile::TempDir::new().unwrap();
    let sh = t.path().join("vault-sync-memory-launch.sh");
    std::fs::write(
        &sh,
        "#!/usr/bin/env bash\nset -euo pipefail\nBIN=\"${VAULT_SYNC_BIN:-${1:-}}\"\n# gates ...\nexec \"$BIN\" --silent \"$@\"\n",
    )
    .unwrap();
    let f = show_live(
        t.path(),
        "s.txt",
        "active",
        "4242",
        &exec_of(&format!("{CAGE} -- {}", sh.display())),
        &format!("NEXUS_SYNC_CONFIG_DIR=/home/u/.config/m VAULT_SYNC_BIN={MEM_APPIMAGE} WLR_BACKENDS=headless"),
    );
    let (rc, out) = run_with(Mode::Systemd(&f), None, &["--via", CAGE]);
    assert_eq!(rc, 0, "{out}");
    assert!(
        out.contains("accepted via --via (exact path); the daemon follows it through"),
        "{out}"
    );
    expect(
        "cage -> launch.sh without --via",
        run(Mode::Systemd(&f), None),
        13,
    );
    // The same script exec'ing something else fails even with --via.
    std::fs::write(&sh, "#!/bin/sh\nexec /usr/bin/open -a X\n").unwrap();
    expect(
        "cage -> launch.sh exec other",
        run_with(Mode::Systemd(&f), None, &["--via", CAGE]),
        13,
    );
}

/// L2: a --via path that does not equal ExecStart argv[0] fails (13); a
/// relative --via is a usage error (2).
/// Mutation: match --via by basename instead of the exact path.
#[test]
fn t_cs3_24_l2_via_mismatch() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(
        t.path(),
        "s.txt",
        "active",
        "4242",
        &exec_of(&format!("{CAGE} -- {APPIMAGE}")),
        "",
    );
    expect(
        "--via /usr/local/bin/cage vs argv[0] /usr/bin/cage",
        run_with(Mode::Systemd(&f), None, &["--via", "/usr/local/bin/cage"]),
        13,
    );
    expect(
        "relative --via",
        run_with(Mode::Systemd(&f), None, &["--via", "cage"]),
        2,
    );
}

/// L2: --via accepts the wrapper only when the daemon follows it.
/// Mutation: accept a --via wrapper without identifying the daemon.
#[test]
fn t_cs3_24_l2_via_no_daemon_after() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(
        t.path(),
        "s.txt",
        "active",
        "4242",
        &exec_of(&format!("{CAGE} -- /usr/bin/true")),
        "",
    );
    expect(
        "cage -> /usr/bin/true",
        run_with(Mode::Systemd(&f), None, &["--via", CAGE]),
        13,
    );
}

// ---------------------------------------------------------------------------
// TKT-a6e5c227: VAULT_SYNC_SUPERVISED (informational; never the exit code)
// ---------------------------------------------------------------------------

const SUP_ENV: &str =
    "<key>EnvironmentVariables</key><dict><key>OTHER</key><string>x</string><key>VAULT_SYNC_SUPERVISED</key><string>1</string></dict>";

/// launchd: the plist's EnvironmentVariables value is reported, `1` as
/// supervised, absent or anything else as not; the exit code stays 0 in
/// every case, and a nested env dict does not disturb the KeepAlive parse.
/// Mutation: drop the EnvironmentVariables parse (red: `=1` never shown).
#[test]
fn a6_launchd_supervised_env_reported_not_gated() {
    let t = tempfile::TempDir::new().unwrap();
    let a = t.path().join("a");
    std::fs::create_dir_all(&a).unwrap();
    plist(&a, SUP_ENV, &[DAEMON, "--silent"]);
    let got = run(Mode::Launchd(&a), None);
    assert!(
        got.1
            .contains("VAULT_SYNC_SUPERVISED=1 (a daemon restart exits 70"),
        "{}",
        got.1
    );
    expect("supervised plist", got, 0);

    plist(&a, "", &[DAEMON, "--silent"]);
    let got = run(Mode::Launchd(&a), None);
    assert!(
        got.1
            .contains("VAULT_SYNC_SUPERVISED=<unset> (informational"),
        "{}",
        got.1
    );
    expect("no env", got, 0);

    plist(
        &a,
        "<key>EnvironmentVariables</key><dict><key>VAULT_SYNC_SUPERVISED</key><string>yes</string></dict>",
        &[DAEMON, "--silent"],
    );
    let got = run(Mode::Launchd(&a), None);
    assert!(
        got.1.contains("VAULT_SYNC_SUPERVISED=yes (informational"),
        "{}",
        got.1
    );
    expect("garbage env", got, 0);

    // A top-level key named like the env var (outside EnvironmentVariables)
    // is not the env.
    plist(
        &a,
        "<key>VAULT_SYNC_SUPERVISED</key><string>1</string>",
        &[DAEMON, "--silent"],
    );
    let got = run(Mode::Launchd(&a), None);
    assert!(got.1.contains("VAULT_SYNC_SUPERVISED=<unset>"), "{}", got.1);
    expect("top-level key", got, 0);
}

/// systemd: the effective Environment= value is reported; exit unchanged.
#[test]
fn a6_systemd_supervised_env_reported_not_gated() {
    let t = tempfile::TempDir::new().unwrap();
    let f = show_live(
        t.path(),
        "s.txt",
        "active",
        "4242",
        &exec_of(DAEMON),
        "HOME=/h VAULT_SYNC_SUPERVISED=1",
    );
    let got = run(Mode::Systemd(&f), None);
    assert!(
        got.1.contains("VAULT_SYNC_SUPERVISED=1 (a daemon"),
        "{}",
        got.1
    );
    expect("supervised unit", got, 0);
    let f = show_live(t.path(), "s2.txt", "active", "4242", &exec_of(DAEMON), "");
    let got = run(Mode::Systemd(&f), None);
    assert!(got.1.contains("VAULT_SYNC_SUPERVISED=<unset>"), "{}", got.1);
    expect("unsupervised unit", got, 0);
}

/// The proposed Trinity agent shipped as a document
/// (`scripts/launchd/com.lattice.vaultsync-daemon.plist`) passes the
/// launcher-policy check as-is (direct binary, KeepAlive on failure, a
/// running agent stub) and carries VAULT_SYNC_SUPERVISED=1.
#[test]
fn a6_proposed_trinity_plist_passes_policy() {
    let label = "com.lattice.vaultsync-daemon";
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("scripts/launchd")
        .join(format!("{label}.plist"));
    let t = tempfile::TempDir::new().unwrap();
    let a = t.path().join("a");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::copy(&src, a.join(format!("{label}.plist"))).unwrap();
    let stub = t.path().join("lc.txt");
    std::fs::write(&stub, LC_RUNNING.replace("vs-fx", label)).unwrap();
    let out = Command::new("bash")
        .arg(script())
        .args(["--name", label, "--agent-dir"])
        .arg(&a)
        .arg("--launchctl-print")
        .arg(&stub)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("KeepAlive=failure"), "{text}");
    assert!(text.contains("VAULT_SYNC_SUPERVISED=1 "), "{text}");
    assert!(text.contains("program=/Applications/Nexus Vault Sync.app/Contents/MacOS/vault-sync-daemon arg1=--silent"), "{text}");
}

// ---------------------------------------------------------------------------
// #29 gate C3: Restart=always is warned (exit code unchanged); on-failure is not
// ---------------------------------------------------------------------------

/// `Restart=always` restarts on 70 and 74, so it still passes (0), but it also
/// restarts after a clean exit 0, so the check prints a WARNING naming the O5
/// target. `Restart=on-failure` prints no warning. Mutation: drop the warning
/// (red on the `always` leg).
#[test]
fn c3_restart_always_is_warned_not_failed() {
    let t = tempfile::TempDir::new().unwrap();
    let got = run(
        Mode::Systemd(&show(t.path(), "always", "78", "", DAEMON)),
        None,
    );
    assert!(
        got.1
            .contains("WARNING Restart=always also restarts after a clean exit 0")
            && got
                .1
                .contains("Restart=on-failure with RestartPreventExitStatus=78"),
        "{}",
        got.1
    );
    expect("Restart=always", got, 0);
    let got = run(
        Mode::Systemd(&show(t.path(), "on-failure", "78", "", DAEMON)),
        None,
    );
    assert!(!got.1.contains("WARNING Restart=always"), "{}", got.1);
    expect("Restart=on-failure", got, 0);
}
