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

/// Copy one gate fixture into a temp dir, substituting `@FIX@` with the
/// copy's own path (x2/x3 plists name their start.sh by absolute path).
fn stage(fixture: &str) -> (tempfile::TempDir, PathBuf) {
    let t = tempfile::TempDir::new().unwrap();
    let dst = t.path().join(fixture);
    copy_tree(&gate_dir().join(fixture), &dst);
    for f in walk(&dst) {
        if let Ok(s) = std::fs::read_to_string(&f) {
            if s.contains("@FIX@") {
                std::fs::write(&f, s.replace("@FIX@", &dst.display().to_string())).unwrap();
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
    let mut c = Command::new("bash");
    c.arg(script()).args(["--name", NAME]);
    if let Some(d) = daemon {
        c.args(["--daemon", d]);
    }
    match mode {
        Mode::Systemd(f) => c.arg("--systemd-show").arg(f),
        Mode::Launchd(d) => c.arg("--agent-dir").arg(d),
    };
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
fn gate(fixture: &str, want: i32) {
    let (_t, dir) = stage(fixture);
    let show = dir.join("systemctl-show.txt");
    let got = if show.exists() {
        run(Mode::Systemd(&show), Some(GATE_DAEMON))
    } else {
        run(Mode::Launchd(&dir.join("a")), Some(GATE_DAEMON))
    };
    expect(fixture, got, want);
}

// ---------------------------------------------------------------------------
// The gate's adversarial fixtures (K2): each false-pass is now a fail.
// ---------------------------------------------------------------------------

/// x1: a masked unit (`/dev/null` symlink in the first unit dir, a real unit
/// in a later dir): `LoadState=masked`, so nothing supervises the daemon: 10.
/// Mutation: drop the masked check.
#[test]
fn t_cs3_24_k2_x1_masked_unit() {
    gate("x1", 10);
}

/// x2: a bash wrapper whose script runs `open` (no exec): 13.
/// Mutation: accept any interpreter script.
#[test]
fn t_cs3_24_k2_x2_bash_wrapper_open() {
    gate("x2", 13);
}

/// x3: a script whose last line is `exec /usr/bin/open`: 13.
/// Mutation: accept any script whose last line is an `exec`.
#[test]
fn t_cs3_24_k2_x3_exec_open() {
    gate("x3", 13);
}

/// x4: `Restart=on-failure` + `SuccessExitStatus=SOFTWARE IOERR`: 70 and 74
/// are "successful" exits, never restarted: 11.
/// Mutations: drop the SuccessExitStatus check; drop the name mapping.
#[test]
fn t_cs3_24_k2_x4_success_exit_status_names() {
    gate("x4", 11);
}

/// x5: `RestartPreventExitStatus=CONFIG`. CONFIG is EX_CONFIG = 78, which
/// the contract allows: 0 (the pre-K2 script false-failed it with 12).
/// Mutation: drop the name mapping (12).
#[test]
fn t_cs3_24_k2_x5_prevent_config_is_78() {
    gate("x5", 0);
}

/// x6: `/usr/bin/env open -a X`: 13.
/// Mutation: drop the launchd program allowlist.
#[test]
fn t_cs3_24_k2_x6_env_open() {
    gate("x6", 13);
}

/// x7: launchd `Disabled=true`: launchd neither runs nor supervises it: 10.
/// Mutation: drop the Disabled check.
#[test]
fn t_cs3_24_k2_x7_launchd_disabled() {
    gate("x7", 10);
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
        gate(f, want);
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
            "LoadState=loaded\nUnitFileState=enabled\nRestart={restart}\nRestartPreventExitStatus={prevent}\nSuccessExitStatus={success}\nExecStart={{ path={exec_path} ; argv[]={exec_path} --silent ; ignore_errors=no }}\nFragmentPath=/u/vs-fx.service\n"
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
