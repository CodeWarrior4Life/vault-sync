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
//!   [`DEFAULT_DELAY_SECS`], the maximum [`MAX_DELAY_SECS`]. A delay of `0`
//!   fires on the first poll, with no timer, so it is deterministic.
//! - It is REFUSED (one WARN, then ignored) unless ALL of these hold:
//!   1. the process is a MANAGED instance (`NEXUS_SYNC_CONFIG_DIR` set and
//!      non-empty). The vault instance (default config dir) can never be
//!      faulted, even if the trigger leaks into its environment through
//!      `launchctl setenv` or `systemctl --user set-environment`;
//!   2. [`ENV_TEST_MODE`] is BOUND to that config dir: it equals
//!      `NEXUS_SYNC_CONFIG_DIR` or the directory's final path component. A
//!      bare `1` is refused, because `1` is the generic value a user-wide
//!      environment carries, and a leaked `1` must not arm a production
//!      managed instance (the memory instance);
//!   3. the task name is injectable ([`check_target`]): `sse_consumer` or
//!      `drain_loop:<root key>`. Every other name is refused at arm time,
//!      including `watcher:<key>` and any Optional task.
//! - The managed launcher (`scripts/vault-sync-memory-launch.sh`) unsets every
//!   `VAULT_SYNC_TEST_*` variable before it execs the daemon, so a managed
//!   service does not receive the hook at all. A drill runs the binary
//!   directly.
//!
//! Nothing else is read: no config file, no network input, no UI command
//! can arm it. The environment is read once, in
//! [`crate::supervise::Supervisor::process`], which runs before any task is
//! spawned (`lib.rs` calls it before `boot::start_sync`). The hook fires at
//! most once per process. A supervised successor inherits the environment and
//! re-arms, so a drill ends when the operator unsets the trigger.
//!
//! # Log lines
//!
//! `FAULT INJECTION ARMED`, `FAULT INJECTION REFUSED` and
//! `FAULT INJECTION FIRED`, all at WARN.

use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::managed_instance::ENV_CONFIG_DIR;
use crate::supervise::{TaskClass, TaskSpec};

/// The trigger: `<task-name>[@<delay_secs>]`.
pub const ENV_TRIGGER: &str = "VAULT_SYNC_TEST_END_REQUIRED_TASK";

/// The second guard: must name this managed instance's config dir (or its
/// final path component). Never a bare `1`.
pub const ENV_TEST_MODE: &str = "VAULT_SYNC_TEST_MODE";

/// Prefix of the per-root drain task name (`drain_loop:<root key>`).
pub const DRAIN_LOOP_PREFIX: &str = "drain_loop:";

/// Exact task names that may be injected, besides `drain_loop:<key>`. Each
/// must be a Required task whose supervised future IS the work, so aborting
/// it stops the work (see [`check_target`]). A unit test checks each name
/// against the spawn site in `boot.rs`.
pub const INJECTABLE_TASKS: &[&str] = &["sse_consumer"];

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
/// - `config_dir`: the raw [`ENV_CONFIG_DIR`] value. `None` or empty means
///   the vault instance.
pub fn decide(
    trigger: Option<&OsStr>,
    test_mode: Option<&OsStr>,
    config_dir: Option<&OsStr>,
) -> Decision {
    let raw = match trigger {
        None => return Decision::Disarmed,
        Some(v) if v.is_empty() => return Decision::Disarmed,
        Some(v) => v,
    };
    let Some(config_dir) = config_dir.filter(|d| !d.is_empty()) else {
        return Decision::Refused {
            reason: format!(
                "this is the vault instance ({ENV_CONFIG_DIR} unset); \
                 the hook runs only in a managed instance"
            ),
        };
    };
    if let Err(reason) = check_test_mode(test_mode, config_dir) {
        return Decision::Refused { reason };
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

/// Guard 2 (M1): [`ENV_TEST_MODE`] must be bound to this instance's config
/// dir, either as the full path or as its final component.
fn check_test_mode(test_mode: Option<&OsStr>, config_dir: &OsStr) -> Result<(), String> {
    let Some(tm) = test_mode.filter(|v| !v.is_empty()) else {
        return Err(format!(
            "{ENV_TEST_MODE} is unset; it must name this instance's {ENV_CONFIG_DIR}"
        ));
    };
    if tm == "1" {
        return Err(format!(
            "{ENV_TEST_MODE}=1 is refused: a bare 1 is the generic value a leaked \
             user-wide environment carries; it must equal {ENV_CONFIG_DIR} or its \
             final path component"
        ));
    }
    let base = Path::new(config_dir).file_name();
    if tm == config_dir || base.is_some_and(|b| tm == b) {
        Ok(())
    } else {
        Err(format!(
            "{ENV_TEST_MODE} does not name this instance: it must equal \
             {ENV_CONFIG_DIR} ({config_dir:?}) or its final path component"
        ))
    }
}

/// Guard 3 (M2): the task name must be injectable. Only tasks whose
/// supervised future IS the work are injectable, so that aborting it stops
/// the work and the supervisor's exit 70 describes a task that really ended:
///
/// - `drain_loop:<key>` runs `push_client::run_loop` directly, with no
///   spawned children (`push_client.rs`, `boot.rs`).
/// - `sse_consumer` is classified by the boot task, which races
///   `consumer.run` directly (`boot.rs`, `sse.rs` has no non-test spawn).
///
/// Refused: `watcher:<key>`. Its event loop is a separate `tokio::spawn`
/// that the wrapper does not own, so aborting the wrapper detaches the real
/// watcher, which keeps writing while the daemon exits 70. Also refused:
/// `boot`, `custody_tick` and `flush:*` (their work is not verified to stop
/// with the wrapper), every Optional task, and unknown names.
pub fn check_target(name: &str) -> Result<(), String> {
    if INJECTABLE_TASKS.contains(&name) {
        return Ok(());
    }
    if let Some(key) = name.strip_prefix(DRAIN_LOOP_PREFIX) {
        if !key.is_empty() && key.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(());
        }
    }
    if name.starts_with("watcher:") {
        return Err(format!(
            "task {name:?} is not injectable: the watcher's event loop runs in its own \
             tokio task, so aborting the supervised wrapper would leave the real watcher \
             running while the daemon exits 70. Injectable: {}",
            injectable_list()
        ));
    }
    Err(format!(
        "task {name:?} is not injectable (unknown, Optional, or not verified to stop with \
         its wrapper). Injectable: {}",
        injectable_list()
    ))
}

fn injectable_list() -> String {
    let mut names: Vec<String> = INJECTABLE_TASKS.iter().map(|s| s.to_string()).collect();
    names.push(format!("{DRAIN_LOOP_PREFIX}<root key>"));
    names.join(", ")
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
    check_target(task).map_err(|e| format!("{ENV_TRIGGER}={raw:?}: {e}"))?;
    if secs > MAX_DELAY_SECS {
        return Err(format!(
            "{ENV_TRIGGER}={raw:?}: the delay exceeds {MAX_DELAY_SECS} s"
        ));
    }
    Ok((task.to_string(), Duration::from_secs(secs)))
}

/// Remove the hook's variables from a child command, so a test child never
/// inherits a trigger from the caller's environment (L3). Every
/// `VAULT_SYNC_TEST_*` name is removed, not only the two this module reads.
pub fn scrub_test_hook_env(cmd: &mut std::process::Command) {
    cmd.env_remove(ENV_TRIGGER).env_remove(ENV_TEST_MODE);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("VAULT_SYNC_TEST_") {
            cmd.env_remove(k);
        }
    }
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
            std::env::var_os(ENV_CONFIG_DIR).as_deref(),
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
                    "FAULT INJECTION ARMED: TEST HOOK ({ENV_TRIGGER}; {ENV_TEST_MODE} bound to \
                     {ENV_CONFIG_DIR}; managed instance). \
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
            // A zero delay has no timer: the future completes on its first
            // poll, so a zero-delay drill is deterministic (M3).
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
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
    use std::ffi::OsString;
    use crate::cs3_tests::RecordingHost;
    use crate::state_io::ExitGate;
    use crate::supervise::{Liveness, Supervisor, TaskEnd};
    use std::sync::Arc;
    use std::time::Instant;

    /// The managed config dir every armed case in this module binds to.
    const CFG: &str = "/home/u/.config/nexus-memory";

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
        for cfg in [None, os(""), os(CFG)] {
            for tm in [None, os("1"), os("0"), os(CFG)] {
                assert_eq!(decide(None, tm, cfg), Decision::Disarmed);
                assert_eq!(decide(os(""), tm, cfg), Decision::Disarmed);
            }
        }
        assert!(FaultInject::from_decision(Decision::Disarmed).is_none());
    }

    /// B4 (b): the vault instance (no or empty config dir) refuses the
    /// trigger, even with a test mode that names a directory.
    /// Negative control: drop the managed check in `decide` and this fails.
    #[test]
    fn b4_vault_instance_is_refused() {
        for cfg in [None, os("")] {
            assert!(
                matches!(
                    decide(os("drain_loop:0@5"), os(CFG), cfg),
                    Decision::Refused { reason } if reason.contains("vault instance")
                ),
                "{cfg:?}"
            );
        }
        assert!(FaultInject::from_decision(Decision::Refused { reason: "x".into() }).is_none());
    }

    /// M1: `VAULT_SYNC_TEST_MODE` must be BOUND to the managed config dir
    /// (the full path, or its final component). A bare `1` and any other
    /// value are refused, so a leaked generic value cannot arm a production
    /// managed instance.
    /// Negative control: relax `check_test_mode` to accept `1` (or any
    /// non-empty value) and this test fails.
    #[test]
    fn b4_test_mode_must_bind_to_config_dir() {
        for tm in [os(CFG), os("nexus-memory")] {
            assert!(
                matches!(
                    decide(os("drain_loop:0@5"), tm, os(CFG)),
                    Decision::Armed { .. }
                ),
                "{tm:?} should bind"
            );
        }
        for tm in [
            None,
            os(""),
            os("0"),
            os("true"),
            os("1"),
            os(" 1"),
            os("1 "),
            os("nexus"),
            os("/home/u/.config/nexus-memory2"),
            os("/other/dir/nexus-memory"),
        ] {
            assert!(
                matches!(
                    decide(os("drain_loop:0@5"), tm, os(CFG)),
                    Decision::Refused { ref reason } if reason.contains(ENV_TEST_MODE)
                ),
                "{tm:?} should be refused"
            );
        }
        // A config dir whose final component is `1` still refuses the bare
        // `1`: the generic value never binds.
        assert!(matches!(
            decide(os("drain_loop:0@5"), os("1"), os("/x/1")),
            Decision::Refused { .. }
        ));
    }

    #[test]
    fn b4_armed_with_guards_parses() {
        assert_eq!(
            decide(os("drain_loop:0@5"), os(CFG), os(CFG)),
            Decision::Armed {
                task: "drain_loop:0".into(),
                delay: Duration::from_secs(5)
            }
        );
        assert_eq!(
            decide(os("sse_consumer"), os("nexus-memory"), os(CFG)),
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
                matches!(decide(os(bad), os(CFG), os(CFG)), Decision::Refused { .. }),
                "{bad}"
            );
        }
    }

    /// M2: the task name is validated at arm time. `watcher:<key>` and every
    /// other name outside the injectable set are refused with a reason, not
    /// silently armed (a typo would otherwise never fire).
    /// Negative control: let `check_target` accept any name and the refused
    /// cases below become Armed, failing this test.
    #[test]
    fn b4_unknown_and_watcher_targets_are_refused_at_arm_time() {
        for ok in ["sse_consumer", "drain_loop:0", "drain_loop:12"] {
            assert!(check_target(ok).is_ok(), "{ok}");
            assert!(
                matches!(decide(os(ok), os(CFG), os(CFG)), Decision::Armed { .. }),
                "{ok}"
            );
        }
        for bad in [
            "watcher:0",
            "boot",
            "custody_tick",
            "flush:shadow",
            "config_watch",
            "heartbeat",
            "no_such_task",
            "sse_consumer2",
            "Drain_loop:0",
            "drain_loop:",
            "drain_loop:x",
            "drain_loop:-1",
        ] {
            assert!(check_target(bad).is_err(), "{bad}");
            let d = decide(os(&format!("{bad}@5")), os(CFG), os(CFG));
            assert!(
                matches!(&d, Decision::Refused { reason } if reason.contains("not injectable")),
                "{bad}: {d:?}"
            );
        }
        let d = decide(os("watcher:0@5"), os(CFG), os(CFG));
        assert!(
            matches!(&d, Decision::Refused { reason } if reason.contains("watcher")),
            "{d:?}"
        );
    }

    /// M2 drift guard: every injectable name must still be a Required spawn
    /// site in `boot.rs`. A rename there would otherwise leave the hook
    /// arming a name that never fires.
    /// Negative control: change an entry in `INJECTABLE_TASKS` to a name that
    /// has no spawn site, and this test fails.
    #[test]
    fn b4_injectable_names_are_real_required_spawn_sites() {
        let boot = include_str!("boot.rs");
        for name in INJECTABLE_TASKS {
            assert!(
                boot.contains(&format!("TaskSpec::new(\"{name}\", TaskClass::Required)")),
                "{name} has no Required spawn site in boot.rs"
            );
        }
        assert!(
            boot.contains("TaskSpec::new(format!(\"drain_loop:{key}\"), TaskClass::Required)"),
            "drain_loop:<key> has no Required spawn site in boot.rs"
        );
    }

    /// L3: a child command built by a test has the hook variables removed,
    /// so a caller's environment cannot leak a trigger into the child.
    /// Negative control: make `scrub_test_hook_env` a no-op and this fails.
    #[test]
    fn b4_test_children_scrub_the_hook_env() {
        let mut cmd = std::process::Command::new("true");
        scrub_test_hook_env(&mut cmd);
        let removed: Vec<OsString> = cmd
            .get_envs()
            .filter(|(_, v)| v.is_none())
            .map(|(k, _)| k.to_os_string())
            .collect();
        assert!(
            removed.contains(&OsString::from(ENV_TRIGGER)),
            "{removed:?}"
        );
        assert!(
            removed.contains(&OsString::from(ENV_TEST_MODE)),
            "{removed:?}"
        );
    }

    /// B4 (c, in-process): the named Required task is ended and the REAL
    /// supervisor path exits 70 once. An Optional task with the same name
    /// is refused, not injected.
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

    /// B4 (a, in-process): with the hook UNARMED, a Required task that ends
    /// on its own still takes the normal supervisor path (exit 70, no fault
    /// object, no injection). The earlier version of this test only checked
    /// a task that never ended, so it could not observe a behaviour change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn b4_unarmed_required_end_still_exits_70() {
        let host = Arc::new(RecordingHost::default());
        let sup = Supervisor::with_gate_and_fault(host.clone(), ExitGate::detached(), None);
        sup.supervise(TaskSpec::new("b4_req", TaskClass::Required), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            TaskEnd::Returned
        });
        assert!(
            wait_until(|| exits(&host) == vec![70], Duration::from_secs(2)).await,
            "{:?}",
            exits(&host)
        );
        assert!(sup.fault().is_none());
        assert_eq!(
            sup.liveness(),
            Liveness::Stopped {
                reason: "task_ended:b4_req".into()
            }
        );
    }

    /// M3: a zero delay is deterministic. The named task has ALREADY returned
    /// when the supervisor first polls, so without the biased select (or with
    /// a timer that needs a tick) the task's own return can win the race and
    /// the hook never reports `fired`. The loop makes a lost race visible.
    /// Negative control: drop `biased;` from `supervise_handle`'s select and
    /// this fails on some iterations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn b4_zero_delay_fires_deterministically() {
        for i in 0..200 {
            let host = Arc::new(RecordingHost::default());
            let sup = Supervisor::with_gate_and_fault(
                host.clone(),
                ExitGate::detached(),
                Some(FaultInject::new("b4_now", Duration::ZERO)),
            );
            sup.supervise(TaskSpec::new("b4_now", TaskClass::Required), async {
                TaskEnd::Returned
            });
            assert!(
                wait_until(|| exits(&host) == vec![70], Duration::from_secs(2)).await,
                "iteration {i}: {:?}",
                exits(&host)
            );
            assert!(
                sup.fault().is_some_and(|f| f.fired()),
                "iteration {i}: the zero-delay hook lost the race to the task's own return"
            );
        }
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
