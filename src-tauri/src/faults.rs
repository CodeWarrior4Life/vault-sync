//! CS-3 test-only fault points (T-CS3-15..-22). Compiled only under
//! `cfg(test)`; every production site is `#[cfg(test)]`-gated too, so a
//! release build carries no fault code. Faults are process-global and are
//! armed only inside the daemon-subprocess harness (`cs3_harness`), never by
//! an in-process test, so parallel unit tests cannot see each other's faults.
#![cfg(test)]

use std::collections::HashSet;
use std::sync::Mutex;

static ARMED: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Arm `name` for the rest of the process.
pub(crate) fn arm(name: &str) {
    ARMED
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get_or_insert_with(HashSet::new)
        .insert(name.to_string());
}

/// Disarm `name`.
#[allow(dead_code)]
pub(crate) fn disarm(name: &str) {
    if let Some(s) = ARMED.lock().unwrap_or_else(|p| p.into_inner()).as_mut() {
        s.remove(name);
    }
}

/// True iff `name` is armed.
pub(crate) fn armed(name: &str) -> bool {
    ARMED
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .is_some_and(|s| s.contains(name))
}

/// Arm every fault named in the comma-separated `CS3_FAULTS` env var.
pub(crate) fn arm_from_env() {
    if let Ok(v) = std::env::var("CS3_FAULTS") {
        for f in v.split(',').map(str::trim).filter(|f| !f.is_empty()) {
            arm(f);
        }
    }
}

/// Panic with an injected-fault message if `<site>.panic` is armed.
pub(crate) fn point(site: &str) {
    if armed(&format!("{site}.panic")) {
        panic!("CS-3 injected panic at {site}");
    }
}
