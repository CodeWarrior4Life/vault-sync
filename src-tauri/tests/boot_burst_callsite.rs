//! TKT-58b50a68 follow-up (gate LSEC-G32-PR49-MERGE-BIND, P3): pin the BOOT call site.
//!
//! The re-latch lives in `DeleteBurstDetector::with_pause_marker`, and the redflag unit tests
//! drive `redflag::daemon_burst_detector`. Nothing proved that boot actually USES that
//! constructor: gate mutant M6 (boot.rs building `DeleteBurstDetector::new(..)` without
//! `.with_pause_marker`) survived every test. If boot bypasses the constructor, a daemon
//! restart silently comes back UNLATCHED again: the exact TKT-58b50a68 defect.
//!
//! This is a static source pin, in the same style as `state_io_gate.rs`: boot.rs must build
//! the delete-burst detector through `redflag::daemon_burst_detector(..)`, exactly once, and
//! must not construct `DeleteBurstDetector` directly.

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
    let boot = code_only(&src("boot.rs"));
    let calls = boot.matches("redflag::daemon_burst_detector(").count();
    assert_eq!(
        calls, 1,
        "boot.rs must build the delete-burst detector through redflag::daemon_burst_detector \
         exactly once (found {calls}); bypassing it drops the boot re-latch (TKT-58b50a68)"
    );
    assert!(
        !boot.contains("DeleteBurstDetector::new("),
        "boot.rs constructs DeleteBurstDetector directly; use redflag::daemon_burst_detector \
         so the PAUSED marker re-latches the valve at boot (TKT-58b50a68, gate mutant M6)"
    );
}

#[test]
fn daemon_burst_detector_attaches_the_pause_marker() {
    // The constructor itself must keep attaching the marker; without it, the boot call
    // above would be pinned to a constructor that no longer re-latches.
    let rf = code_only(&src("redflag.rs"));
    let start = rf
        .find("pub fn daemon_burst_detector(")
        .expect("redflag::daemon_burst_detector must exist");
    let body_end = rf[start..]
        .find("\n}")
        .map(|i| start + i)
        .expect("fn body end");
    let body = &rf[start..body_end];
    assert!(
        body.contains(".with_pause_marker("),
        "daemon_burst_detector must attach the pause marker (the re-latch path)"
    );
}
