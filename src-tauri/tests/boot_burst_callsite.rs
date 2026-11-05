//! TKT-58b50a68 backport (0.4.43): pin the BOOT call site.
//!
//! The re-latch lives in `DeleteBurstDetector::with_pause_marker`, reached through
//! `redflag::daemon_burst_detector`. Nothing else proves boot USES that constructor; if
//! lib.rs builds `DeleteBurstDetector::new(..)` directly, a restart comes back UNLATCHED
//! again. Static source pin, same as upstream main's tests/boot_burst_callsite.rs, but
//! pointed at lib.rs (0.4.x has no boot.rs).

use std::fs;
use std::path::PathBuf;

fn src(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(name);
    fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// Strip `//` line comments so a mention in prose cannot satisfy or trip the pin.
fn code_only(s: &str) -> String {
    s.lines()
        .map(|l| match l.find("//") {
            Some(i) => &l[..i],
            None => l,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn boot_builds_the_burst_detector_only_through_daemon_burst_detector() {
    let lib = code_only(&src("lib.rs"));
    let calls = lib.matches("redflag::daemon_burst_detector(").count();
    assert_eq!(
        calls, 1,
        "lib.rs must build the delete-burst detector through redflag::daemon_burst_detector \
         exactly once (found {calls}); bypassing it drops the boot re-latch (TKT-58b50a68)"
    );
    assert!(
        !lib.contains("DeleteBurstDetector::new("),
        "lib.rs constructs DeleteBurstDetector directly; use redflag::daemon_burst_detector"
    );
}

#[test]
fn daemon_burst_detector_attaches_the_pause_marker() {
    let rf = code_only(&src("redflag.rs"));
    let start = rf
        .find("pub fn daemon_burst_detector(")
        .expect("redflag::daemon_burst_detector must exist");
    let body_end = rf[start..]
        .find("\n}")
        .map(|i| start + i)
        .expect("fn body end");
    assert!(
        rf[start..body_end].contains(".with_pause_marker("),
        "daemon_burst_detector must attach the pause marker (the re-latch path)"
    );
}
