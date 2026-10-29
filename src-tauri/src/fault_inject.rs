//! B4 (arranger ruling PB-2D87-VS-SCOPE): an env-guarded fault-injection hook
//! that makes ONE named Required supervised task end, so a real host can
//! exercise CS-3's supervision path ([`crate::supervise`]) in the packaged
//! app: a Required task that ends while the daemon is `Live` exits 70
//! (`EXIT_TASK_ENDED`) for the launcher to relaunch; one that ends after a
//! Terminal stop is a `cascade` (no exit, no second notice).
//!
//! The hook does NOT decide the outcome. When it fires it aborts the named
//! task and reports `TaskEnd::Returned` to [`crate::supervise::Supervisor`]
//! exactly as if the task had returned on its own; the supervisor's real
//! `task_ended` classification decides what happens.
//!
//! # Arming (environment only)
//!
//! - [`ENV_TRIGGER`] = `<task-name>[@<delay_secs>]`, e.g. `drain_loop:0@30`.
//!   The separator is `@` because task names contain `:`. The delay counts
//!   from the moment the named task is put under supervision; the default is
//!   [`DEFAULT_DELAY_SECS`], the maximum [`MAX_DELAY_SECS`].
//! - It is REFUSED (one WARN, then ignored) unless BOTH guards hold:
//!   1. the process is a MANAGED instance (`NEXUS_SYNC_CONFIG_DIR` set and
//!      non-empty, [`crate::managed_instance::is_managed_mode`]). The vault
//!      instance (the operator's real vault, default config dir) can never
//!      be faulted, even if the trigger leaks into its environment through
//!      `launchctl setenv` or `systemctl --user set-environment`, which reach
//!      every agent or unit of the user;
//!   2. [`ENV_TEST_MODE`] is exactly `1`, an explicit statement of intent, so
//!      a trigger that leaks into a production managed instance (the memory
//!      instance) is still refused.
//!
//! Nothing else is read: no config file, no network input, no UI command
//! can arm it. The environment is read once, in
//! [`crate::supervise::Supervisor::process`]. The hook fires at most once
//! per process (a relaunched successor inherits the environment and fires
//! once more; see the PR / plan notes).
//!
//! # Log lines
//!
//! `FAULT INJECTION ARMED`, `FAULT INJECTION REFUSED` and
//! `FAULT INJECTION FIRED`, all at WARN.

use std::ffi::OsStr;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::supervise::{TaskClass, TaskSpec};

/// The trigger: `<task-name>[@<delay_secs>]`.
pub const ENV_TRIGGER: &str = "VAULT_SYNC_TEST_END_REQUIRED_TASK";

/// The second guard: must be exactly `1`.
pub const ENV_TEST_MODE: &str = "VAULT_SYNC_TEST_MODE";

/// Delay when the trigger names no `@<delay_secs>`.
pub const DEFAULT_DELAY_SECS: u64 = 30;

/// The largest accepted delay (one day).
pub const MAX_DELAY_SECS: u64 = 86_400;

/// What the environment asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// No trigger (unset or empty): the hook does not exist.
    Disarmed,
    /// A trigger is present but a guard or the syntax failed: ignored.
    Refused { reason: String },
    /// Armed: end `task` once, `delay` after it is supervised.
    Armed { task: String, delay: Duration },
}

/// Pure decision over the raw environment values (no env read).
///
/// - `trigger`: the raw [`ENV_TRIGGER`] value.
/// - `test_mode`: the raw [`ENV_TEST_MODE`] value.
/// - `managed`: [`crate::managed_instance::is_managed_mode`].
pub fn decide(trigger: Option<&OsStr>, test_mode: Option<&OsStr>, managed: bool) -> Decision {
    let raw = match trigger {
        None => return Decision::Disarmed,
        Some(v) if v.is_empty() => return Decision::Disarmed,
        Some(v) => v,
    };
    if !managed {
        return Decision::Refused {
            reason: "this is the vault instance (NEXUS_SYNC_CONFIG_DIR unset); \
                     the hook runs only in a managed instance"
                .to_string(),
        };
    }
    if !test_mode.is_some_and(|v| v == "1") {
        return Decision::Refused {
            reason: format!("{ENV_TEST_MODE} is not exactly 1"),
        };
    }
    let Some(raw) = raw.to_str() else {
        return Decision::Refused {
            reason: format!("{ENV_TRIGGER} is not UTF-8"),
        };
    };
    match parse(raw) {
        Ok((task, delay)) => Decision::Armed { task, delay },
        Err(reason) => Decision::Refused { reason },
    }
}

/// `<task-name>[@<delay_secs>]`.
fn parse(raw: &str) -> Result<(String, Duration), String> {
    let (task, secs) = match raw.split_once('@') {
        Some((t, s)) => {
            let secs = s.parse::<u64>().map_err(|_| {
                format!(
                    "{ENV_TRIGGER}={raw:?}: the delay after '@' is not a whole number of seconds"
                )
            })?;
            (t, secs)
        }
        None => (raw, DEFAULT_DELAY_SECS),
    };
    if task.is_empty() || task.chars().any(char::is_whitespace) {
        return Err(format!(
            "{ENV_TRIGGER}={raw:?}: the task name is empty or contains whitespace"
        ));
    }
    if secs > MAX_DELAY_SECS {
        return Err(format!(
            "{ENV_TRIGGER}={raw:?}: the delay exceeds {MAX_DELAY_SECS} s"
        ));
    }
    Ok((task.to_string(), Duration::from_secs(secs)))
}

/// An armed injection, held by the process supervisor.
#[derive(Debug)]
pub struct FaultInject {
    task: String,
    delay: Duration,
    claimed: AtomicBool,
    fired: Arc<AtomicBool>,
}

impl FaultInject {
    pub fn new(task: impl Into<String>, delay: Duration) -> Self {
        Self {
            task: task.into(),
            delay,
            claimed: AtomicBool::new(false),
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Read the environment once (the process supervisor calls this) and
    /// log the outcome: nothing when disarmed, one WARN when refused or
    /// armed.
    pub fn from_env() -> Option<Self> {
        let decision = decide(
            std::env::var_os(ENV_TRIGGER).as_deref(),
            std::env::var_os(ENV_TEST_MODE).as_deref(),
            crate::managed_instance::is_managed_mode(),
        );
        Self::from_decision(decision)
    }

    /// Log `decision` and build the armed injection, if any.
    pub fn from_decision(decision: Decision) -> Option<Self> {
        match decision {
            Decision::Disarmed => None,
            Decision::Refused { reason } => {
                tracing::warn!(
                    "FAULT INJECTION REFUSED: {ENV_TRIGGER} is set but ignored: {reason}. \
                     No task will be ended."
                );
                None
            }
            Decision::Armed { task, delay } => {
                tracing::warn!(
                    task = %task,
                    delay_secs = delay.as_secs(),
                    supervised = crate::supervise::is_supervised(),
                    "FAULT INJECTION ARMED: TEST HOOK ({ENV_TRIGGER}, {ENV_TEST_MODE}=1, managed instance). \
                     The Required task {task:?} will be ended {}s after it is supervised; \
                     the CS-3 supervisor then decides (exit 70 while live, cascade after a stop). \
                     Unset {ENV_TRIGGER} to disarm.",
                    delay.as_secs()
                );
                Some(Self::new(task, delay))
            }
        }
    }

    pub fn task(&self) -> &str {
        &self.task
    }

    /// True once the timer has fired (the task was ended).
    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    /// The timer for `spec` iff it is the named task, it is Required, and
    /// no task has claimed the injection yet (at most once per process).
    /// The returned future logs `FAULT INJECTION FIRED` when it completes.
    pub fn timer_for(&self, spec: &TaskSpec) -> Option<impl Future<Output = ()> + Send + 'static> {
        if spec.name != self.task {
            return None;
        }
        if spec.class != TaskClass::Required {
            tracing::warn!(
                task = %spec.name,
                class = ?spec.class,
                "FAULT INJECTION REFUSED: the named task is not Required; not injected"
            );
            return None;
        }
        if self.claimed.swap(true, Ordering::SeqCst) {
            return None;
        }
        let (name, delay, fired) = (spec.name.clone(), self.delay, Arc::clone(&self.fired));
        Some(async move {
            tokio::time::sleep(delay).await;
            fired.store(true, Ordering::SeqCst);
            tracing::warn!(
                task = %name,
                delay_secs = delay.as_secs(),
                "FAULT INJECTION FIRED: TEST HOOK ending the Required task {name:?} as if it \
                 returned unexpectedly ({ENV_TRIGGER}); the supervisor decides the outcome"
            );
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cs3_tests::RecordingHost;
    use crate::state_io::ExitGate;
    use crate::supervise::{Liveness, Supervisor, TaskEnd};
    use std::sync::Arc;
    use std::time::Instant;

    fn os(s: &str) -> Option<&OsStr> {
        Some(OsStr::new(s))
    }

    async fn wait_until(mut f: impl FnMut() -> bool, max: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < max {
            if f() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        f()
    }

    fn exits(h: &RecordingHost) -> Vec<i32> {
        h.exits.lock().unwrap().clone()
    }

    /// B4 (a): no trigger, or an empty one, is Disarmed whatever the guards.
    #[test]
    fn b4_unarmed_is_disarmed() {
        for managed in [false, true] {
            for tm in [None, os("1"), os("0")] {
                assert_eq!(decide(None, tm, managed), Decision::Disarmed);
                assert_eq!(decide(os(""), tm, managed), Decision::Disarmed);
            }
        }
        assert!(FaultInject::from_decision(Decision::Disarmed).is_none());
    }

    /// B4 (b): a trigger without BOTH guards is refused. Negative control:
    /// drop either guard check in `decide` and this test fails.
    #[test]
    fn b4_armed_without_guard_is_refused() {
        // The vault instance: refused even with TEST_MODE=1.
        assert!(matches!(
            decide(os("drain_loop:0@5"), os("1"), false),
            Decision::Refused { reason } if reason.contains("vault instance")
        ));
        // Managed but no explicit test mode, or not exactly "1".
        for tm in [None, os(""), os("0"), os("true"), os(" 1"), os("1 ")] {
            assert!(
                matches!(
                    decide(os("drain_loop:0@5"), tm, true),
                    Decision::Refused { ref reason } if reason.contains(ENV_TEST_MODE)
                ),
                "{tm:?}"
            );
        }
        assert!(FaultInject::from_decision(Decision::Refused { reason: "x".into() }).is_none());
    }

    #[test]
    fn b4_armed_with_guards_parses() {
        assert_eq!(
            decide(os("drain_loop:0@5"), os("1"), true),
            Decision::Armed {
                task: "drain_loop:0".into(),
                delay: Duration::from_secs(5)
            }
        );
        assert_eq!(
            decide(os("sse_consumer"), os("1"), true),
            Decision::Armed {
                task: "sse_consumer".into(),
                delay: Duration::from_secs(DEFAULT_DELAY_SECS)
            }
        );
        for bad in [
            "@5",
            "drain_loop:0@",
            "drain_loop:0@x",
            "drain loop@1",
            "a@86401",
        ] {
            assert!(
                matches!(decide(os(bad), os("1"), true), Decision::Refused { .. }),
                "{bad}"
            );
        }
    }

    /// B4 (c, in-process): the named Required task is ended and the REAL
    /// supervisor path exits 70 once; a same-named non-Required task and a
    /// second task with the name are never injected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn b4_armed_ends_required_task_supervisor_exits_70() {
        let host = Arc::new(RecordingHost::default());
        let sup = Supervisor::with_gate_and_fault(
            host.clone(),
            ExitGate::detached(),
            Some(FaultInject::new("b4_req", Duration::from_millis(50))),
        );
        // Optional with the name: refused, never ended.
        sup.supervise(TaskSpec::new("b4_req", TaskClass::Optional), async {
            futures::future::pending::<()>().await;
            TaskEnd::Returned
        });
        sup.supervise(TaskSpec::new("b4_req", TaskClass::Required), async {
            futures::future::pending::<()>().await;
            TaskEnd::Returned
        });
        assert!(
            wait_until(|| exits(&host) == vec![70], Duration::from_secs(2)).await,
            "{:?}",
            exits(&host)
        );
        assert_eq!(
            sup.liveness(),
            Liveness::Stopped {
                reason: "task_ended:b4_req".into()
            }
        );
        assert!(sup.fault().is_some_and(|f| f.fired()));
        assert!(host.notices.lock().unwrap().is_empty(), "exit 70 notified");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(exits(&host), vec![70]);
    }

    /// B4 (a, in-process): unarmed, the same never-ending Required task is
    /// untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn b4_unarmed_supervisor_unchanged() {
        let host = Arc::new(RecordingHost::default());
        let sup = Supervisor::with_gate_and_fault(host.clone(), ExitGate::detached(), None);
        sup.supervise(TaskSpec::new("b4_req", TaskClass::Required), async {
            futures::future::pending::<()>().await;
            TaskEnd::Returned
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(exits(&host).is_empty());
        assert!(sup.is_live());
        assert!(sup.fault().is_none());
    }

    /// B4 (c, Terminal): fired after a Terminal stop, the supervisor logs a
    /// cascade: no exit and no second notice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn b4_armed_after_terminal_is_cascade() {
        let host = Arc::new(RecordingHost::default());
        let sup = Supervisor::with_gate_and_fault(
            host.clone(),
            ExitGate::detached(),
            Some(FaultInject::new("b4_req", Duration::from_millis(200))),
        );
        sup.supervise(TaskSpec::new("b4_req", TaskClass::Required), async {
            futures::future::pending::<()>().await;
            TaskEnd::Returned
        });
        sup.supervise(TaskSpec::new("b4_sse", TaskClass::Required), async {
            crate::boot::sse_auth_terminal("sub-b4", 401)
        });
        assert!(
            wait_until(
                || host.notices.lock().unwrap().len() == 1,
                Duration::from_secs(1)
            )
            .await
        );
        assert!(
            wait_until(
                || sup.fault().is_some_and(|f| f.fired()),
                Duration::from_secs(2)
            )
            .await,
            "the injection never fired (vacuous cascade)"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(exits(&host).is_empty(), "{:?}", exits(&host));
        assert_eq!(host.notices.lock().unwrap().len(), 1);
        assert_eq!(
            sup.liveness(),
            Liveness::Stopped {
                reason: "sse_auth_failed".into()
            }
        );
    }
}
