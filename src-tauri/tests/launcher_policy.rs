//! Unit CS-3 (TKT-b6d8c4ba) T-CS3-24 (gates C5 and C10): the fixtures for
//! `scripts/verify-launcher-policy.sh`. Every failing fixture returns its own
//! exit code (section 4.3 item 11) and every positive fixture returns 0.
//! The script is read-only; here it only reads the fixture dirs. The live
//! per-host run (link, Trinity, icarus) is a fleet-release item (gate E1).
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::process::Command;

const NAME: &str = "vs-cs3-fixture";

fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("scripts")
        .join("verify-launcher-policy.sh")
}

struct Fx {
    _root: tempfile::TempDir,
    units: PathBuf,
    agents: PathBuf,
}

fn fx() -> Fx {
    let root = tempfile::TempDir::new().unwrap();
    let units = root.path().join("units");
    let agents = root.path().join("agents");
    std::fs::create_dir_all(&units).unwrap();
    std::fs::create_dir_all(&agents).unwrap();
    Fx {
        _root: root,
        units,
        agents,
    }
}

impl Fx {
    fn unit(&self, body: &str) -> &Self {
        std::fs::write(self.units.join(format!("{NAME}.service")), body).unwrap();
        self
    }
    fn dropin(&self, file: &str, body: &str) -> &Self {
        let d = self.units.join(format!("{NAME}.service.d"));
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(file), body).unwrap();
        self
    }
    fn plist(&self, keepalive: &str, program_args0: &str) -> &Self {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{NAME}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{program_args0}</string>
		<string>--silent</string>
	</array>
	{keepalive}
	<key>RunAtLoad</key>
	<true/>
</dict>
</plist>
"#
        );
        std::fs::write(self.agents.join(format!("{NAME}.plist")), body).unwrap();
        self
    }
    fn run(&self) -> (i32, String) {
        let out = Command::new("bash")
            .arg(script())
            .args(["--name", NAME, "--unit-dir"])
            .arg(&self.units)
            .arg("--agent-dir")
            .arg(&self.agents)
            .output()
            .unwrap();
        (
            out.status.code().unwrap_or(-1),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    }
}

const KA_FAILURE: &str =
    "<key>KeepAlive</key>\n\t<dict>\n\t\t<key>SuccessfulExit</key>\n\t\t<false/>\n\t</dict>";

fn assert_code(f: &Fx, want: i32, what: &str) {
    let (code, out) = f.run();
    assert_eq!(
        code, want,
        "{what}: expected exit {want}, got {code}\n{out}"
    );
}

/// T-CS3-24: the four failing fixtures (no supervisor, no restart, no
/// KeepAlive, an `open` shim), the prevent-list fixture, and the four
/// positive fixtures.
#[test]
fn t_cs3_24_launcher_policy_fixtures() {
    // 10: no unit and no agent.
    assert_code(&fx(), 10, "no unit and no agent");

    // 11: Restart=no.
    let f = fx();
    f.unit("[Unit]\nDescription=x\n\n[Service]\nExecStart=/bin/true\nRestart=no\n");
    assert_code(&f, 11, "Restart=no");
    // 11: Restart unset (systemd's default is no), and on-abnormal.
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\n");
    assert_code(&f, 11, "Restart unset");
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\nRestart=on-abnormal\n");
    assert_code(&f, 11, "Restart=on-abnormal");

    // 11: a plist without KeepAlive.
    let f = fx();
    f.plist(
        "",
        "/Applications/Vault Sync.app/Contents/MacOS/vault-sync-daemon",
    );
    assert_code(&f, 11, "plist without KeepAlive");

    // 13: ProgramArguments[0] is /usr/bin/open, even with KeepAlive
    // {SuccessfulExit:false}.
    let f = fx();
    f.plist(KA_FAILURE, "/usr/bin/open");
    assert_code(&f, 13, "open shim with KeepAlive");

    // 12: RestartPreventExitStatus=70 78.
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\nRestart=always\nRestartPreventExitStatus=70 78\n");
    assert_code(&f, 12, "RestartPreventExitStatus=70 78");
    // 12 also when the prevent list arrives through a drop-in.
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\nRestart=on-failure\n")
        .dropin(
            "20-prevent.conf",
            "[Service]\nRestartPreventExitStatus=74\n",
        );
    assert_code(&f, 12, "drop-in RestartPreventExitStatus=74");

    // 0: Restart=always with no prevent list.
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\nRestart=always\n");
    assert_code(&f, 0, "Restart=always");
    // 0: Restart=on-failure with RestartPreventExitStatus=78.
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\nRestart=on-failure\nRestartPreventExitStatus=78\n");
    assert_code(&f, 0, "Restart=on-failure + prevent 78");
    // 0: link's shape: base on-failure, drop-in always.
    let f = fx();
    f.unit("[Service]\nExecStart=/bin/true\nRestart=on-failure\n")
        .dropin(
            "10-desktop-env.conf",
            "[Service]\nRestart=always\nRestartSec=30s\n",
        );
    assert_code(&f, 0, "drop-in Restart=always");
    // 0: KeepAlive {SuccessfulExit:false} on a direct daemon binary.
    let f = fx();
    let bin = f.agents.join("vault-sync-daemon");
    std::fs::write(&bin, b"\x7fELF-not-a-script").unwrap();
    f.plist(KA_FAILURE, bin.to_str().unwrap());
    assert_code(&f, 0, "KeepAlive on a direct binary");
    // 0: KeepAlive {SuccessfulExit:false} on a launch script that execs.
    let f = fx();
    let sh = f.agents.join("launch.sh");
    std::fs::write(
        &sh,
        "#!/bin/sh\n# launch the daemon\nexport X=1\nexec /opt/vs/vault-sync-daemon --silent\n",
    )
    .unwrap();
    f.plist(KA_FAILURE, sh.to_str().unwrap());
    assert_code(&f, 0, "KeepAlive on an exec launch script");
    // 13: the same script without exec (launchd would supervise the shell).
    let f = fx();
    let sh = f.agents.join("launch.sh");
    std::fs::write(&sh, "#!/bin/sh\n/opt/vs/vault-sync-daemon --silent\n").unwrap();
    f.plist(KA_FAILURE, sh.to_str().unwrap());
    assert_code(&f, 13, "launch script without exec");
}

/// T-CS3-24: usage errors return 2.
#[test]
fn t_cs3_24_usage_error() {
    let out = Command::new("bash")
        .arg(script())
        .arg("--bogus")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
