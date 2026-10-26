//! Unit CS-3 (TKT-b6d8c4ba) §3.5: supervised tasks, process liveness and the
//! one exit path (AR-1C-017, gates C1, C4, C6, C9).
//!
//! Every long-lived task is spawned through [`Supervisor::supervise`], which
//! awaits its `JoinHandle`. How a task's end is handled depends on its
//! [`TaskClass`] and on whether the end was designed ([`TaskEnd`]):
//!
//! | Class | Unexpected end (panic, or return while `Live`) | Designed end |
//! |---|---|---|
//! | Required | `live` = Stopped, ERROR, `task_ended`, `exit_once(70)` | Terminal (no exit, one notice) or designed restart (exit 70) |
//! | RootDegrading | the root's `alive` = false, ERROR, `task_ended`; no exit | kill switch: `task_disabled` |
//! | Optional | WARN, `task_ended`; no exit | kill switch: `task_disabled` |
//!
//! Cascade rule: once `live` is Stopped, a later end is logged at INFO as
//! `cascade` and never triggers an exit.
//!
//! `exit_once` is the CS-2 latch's exit claim ([`ExitGate`]). Every
//! own-initiative leaver claims it: `ownership_lost` (74), the supervisor
//! (70), the stall watchdog's restart and the updater's restart. Each non-74
//! leaver re-checks custody and the mutation latch right before it leaves and
//! exits 74 instead if custody is gone (gate C9).

use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::watch;

use crate::state_io::{ExitGate, CLAIM_RESTART};
use crate::sync_health::{raise_state_event, StateHealthEvent};

/// EX_SOFTWARE: a Required task ended unexpectedly (CS-3 D4).
pub const EXIT_TASK_ENDED: i32 = 70;

/// A periodic task's tick. Tests in the daemon-subprocess harness shorten
/// every periodic tick with `CS3_FAST_TICK_MS`; production returns
/// `default` unchanged.
pub fn tick(default: Duration) -> Duration {
    #[cfg(test)]
    if let Some(ms) = std::env::var("CS3_FAST_TICK_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return Duration::from_millis(ms.max(1));
    }
    default
}

/// D14: the bounded pre-exit store flush budget, in total.
const PRE_EXIT_FLUSH_BUDGET: Duration = Duration::from_secs(2);

/// Process liveness (D11). `Stopped` is one-way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Liveness {
    Live,
    Stopped { reason: String },
}

/// How a supervised task's end is handled (§3.5 table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskClass {
    Required,
    RootDegrading,
    Optional,
}

/// A dedicated operator notice (No Silent Holds: what stopped, why, and the
/// one action that lifts it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notice {
    pub title: String,
    pub body: String,
}

/// How a supervised task ended (its own report; a panic is reported by the
/// `JoinHandle`).
#[derive(Debug)]
pub enum TaskEnd {
    /// The task returned. Unexpected while `Live` for every class.
    Returned,
    /// Ended by its kill switch, by design.
    Disabled,
    /// A designed stop: `live` = Stopped{reason}, no exit, at most one
    /// notice and one SyncHealth event per reason.
    Terminal {
        reason: String,
        event: Option<StateHealthEvent>,
        notice: Option<Notice>,
    },
    /// A designed restart (the storm fence): `live` = Stopped{reason},
    /// `task_ended{cause}`, then `exit_once(70)`.
    Restart { reason: String, cause: String },
    /// A designed one-shot completion with nothing left to do (the stall
    /// watchdog after its recovery fired). Logged at INFO.
    Completed,
    /// The task already classified its own end through
    /// [`Supervisor::task_ended`] (the boot task does, before it drops the
    /// watch handles, so the cascade rule sees `Stopped` first).
    Classified,
}

/// What a supervised task is and what its end affects.
#[derive(Clone)]
pub struct TaskSpec {
    pub name: String,
    pub class: TaskClass,
    /// RootDegrading: the root's `alive` flag.
    pub root_alive: Option<Arc<AtomicBool>>,
}

impl TaskSpec {
    pub fn new(name: impl Into<String>, class: TaskClass) -> Self {
        Self {
            name: name.into(),
            class,
            root_alive: None,
        }
    }

    pub fn root_degrading(name: impl Into<String>, alive: Arc<AtomicBool>) -> Self {
        Self {
            name: name.into(),
            class: TaskClass::RootDegrading,
            root_alive: Some(alive),
        }
    }
}

/// The app-side effects the supervisor and the boot task need. Production
/// wraps the Tauri `AppHandle` ([`crate::boot::TauriHost`]); tests record.
pub trait Host: Send + Sync + 'static {
    /// One OS notification.
    fn notify(&self, title: &str, body: &str);
    /// One frontend event.
    fn emit(&self, event: &str, payload: serde_json::Value);
    /// Leave the process with `code` (production never returns).
    fn exit(&self, code: i32);
    /// Relaunch the app (production never returns).
    fn restart(&self);
}

/// How a leaver intends to leave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Leave {
    Exit(i32),
    Restart,
}

type PreExitFlush = Arc<dyn Fn() + Send + Sync>;

/// The process supervisor: liveness, the shared exit gate and the leavers.
pub struct Supervisor {
    host: Arc<dyn Host>,
    gate: ExitGate,
    live: watch::Sender<Liveness>,
    pre_exit: Mutex<Vec<PreExitFlush>>,
    notices: Mutex<HashSet<String>>,
    #[cfg(test)]
    hooks: test_hooks::Hooks,
}

static PROCESS_LIVE: OnceLock<watch::Sender<Liveness>> = OnceLock::new();

/// CS-3 §3.3: `ownership_lost` sets liveness Stopped before its exit. A
/// no-op until the process supervisor exists.
pub fn stop_process_liveness(reason: &str) {
    if let Some(tx) = PROCESS_LIVE.get() {
        stop(tx, reason);
    }
}

fn stop(tx: &watch::Sender<Liveness>, reason: &str) -> bool {
    tx.send_if_modified(|l| {
        if *l == Liveness::Live {
            *l = Liveness::Stopped {
                reason: reason.to_string(),
            };
            true
        } else {
            false
        }
    })
}

impl Supervisor {
    /// The process supervisor: the process exit gate, and liveness published
    /// for `ownership_lost`. Call once per process.
    pub fn process(host: Arc<dyn Host>) -> Arc<Self> {
        let sup = Self::with_gate(host, ExitGate::process());
        if PROCESS_LIVE.set(sup.live.clone()).is_err() {
            tracing::error!("supervise: process supervisor already installed");
        }
        sup
    }

    /// A supervisor on an explicit gate (in-process tests).
    pub fn with_gate(host: Arc<dyn Host>, gate: ExitGate) -> Arc<Self> {
        let (live, _rx) = watch::channel(Liveness::Live);
        Arc::new(Self {
            host,
            gate,
            live,
            pre_exit: Mutex::new(Vec::new()),
            notices: Mutex::new(HashSet::new()),
            #[cfg(test)]
            hooks: test_hooks::Hooks::default(),
        })
    }

    pub fn host(&self) -> &Arc<dyn Host> {
        &self.host
    }

    pub fn gate(&self) -> &ExitGate {
        &self.gate
    }

    /// A clone of the liveness sender (held by `SyncHandles`).
    pub fn live_sender(&self) -> watch::Sender<Liveness> {
        self.live.clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Liveness> {
        self.live.subscribe()
    }

    pub fn liveness(&self) -> Liveness {
        self.live.borrow().clone()
    }

    pub fn is_live(&self) -> bool {
        *self.live.borrow() == Liveness::Live
    }

    /// Set `live` = Stopped{reason} (one-way). Returns true if this call
    /// made the transition.
    pub fn stop(&self, reason: &str) -> bool {
        stop(&self.live, reason)
    }

    /// D14: register a store flush for the bounded pre-exit flush (every
    /// write goes through `guard_mutation`; errors are ignored).
    pub fn register_pre_exit_flush(&self, f: impl Fn() + Send + Sync + 'static) {
        self.pre_exit
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(Arc::new(f));
    }

    /// Spawn `fut` and supervise its end.
    pub fn supervise<F>(self: &Arc<Self>, spec: TaskSpec, fut: F)
    where
        F: Future<Output = TaskEnd> + Send + 'static,
    {
        let h = tauri::async_runtime::spawn(fut);
        self.supervise_handle(spec, h);
    }

    /// Supervise an already-spawned task's `JoinHandle` (the spawners that
    /// return one). A panic surfaces as `Err` from the handle.
    pub fn supervise_handle(
        self: &Arc<Self>,
        spec: TaskSpec,
        h: tauri::async_runtime::JoinHandle<TaskEnd>,
    ) {
        let me = Arc::clone(self);
        tauri::async_runtime::spawn(async move {
            let end = h.await.map_err(|e| e.to_string());
            me.task_ended(&spec, end);
        });
    }

    /// Classify one task end (§3.5 table and cascade rule). Public so the
    /// boot task can classify the SSE consumer's end BEFORE it drops the
    /// watch handles.
    pub fn task_ended(&self, spec: &TaskSpec, end: Result<TaskEnd, String>) {
        let name = spec.name.as_str();
        if let Ok(TaskEnd::Classified) = end {
            return;
        }
        if let Ok(TaskEnd::Completed) = end {
            tracing::info!(task = name, "supervise: task completed by design");
            return;
        }
        if let Ok(TaskEnd::Disabled) = end {
            tracing::info!(task = name, "supervise: task disabled by its kill switch");
            raise_state_event(StateHealthEvent::TaskDisabled {
                name: name.to_string(),
            });
            return;
        }
        if !self.is_live() {
            tracing::info!(
                task = name,
                outcome = ?end.as_ref().map(|_| ()).map_err(|e| e.as_str()),
                "supervise: task ended after the daemon stopped (cascade); no action"
            );
            return;
        }
        match end {
            Ok(TaskEnd::Terminal {
                reason,
                event,
                notice,
            }) => self.terminal(name, &reason, event, notice),
            Ok(TaskEnd::Restart { reason, cause }) => {
                self.stop(&reason);
                tracing::error!(
                    task = name,
                    reason = %reason,
                    cause = %cause,
                    "supervise: designed restart; exiting 70 so the launcher restarts the daemon"
                );
                raise_state_event(StateHealthEvent::TaskEnded {
                    name: name.to_string(),
                    cause,
                });
                self.exit_70(name);
            }
            Ok(TaskEnd::Returned) | Err(_) => {
                let cause = if end.is_err() { "panic" } else { "returned" };
                let detail = end.err().unwrap_or_default();
                match spec.class {
                    TaskClass::Required => {
                        self.stop(&format!("task_ended:{name}"));
                        tracing::error!(
                            task = name,
                            cause,
                            detail = %detail,
                            "supervise: REQUIRED task ended unexpectedly; exiting 70 so the launcher restarts the daemon"
                        );
                        raise_state_event(StateHealthEvent::TaskEnded {
                            name: name.to_string(),
                            cause: cause.to_string(),
                        });
                        self.exit_70(name);
                    }
                    TaskClass::RootDegrading => {
                        if let Some(a) = &spec.root_alive {
                            a.store(false, Ordering::SeqCst);
                        }
                        tracing::error!(
                            task = name,
                            cause,
                            detail = %detail,
                            "supervise: root-degrading task ended; its root is no longer alive (Verify and repair refuses it); no exit"
                        );
                        raise_state_event(StateHealthEvent::TaskEnded {
                            name: name.to_string(),
                            cause: cause.to_string(),
                        });
                    }
                    TaskClass::Optional => {
                        tracing::warn!(
                            task = name,
                            cause,
                            detail = %detail,
                            "supervise: optional task ended; no exit"
                        );
                        raise_state_event(StateHealthEvent::TaskEnded {
                            name: name.to_string(),
                            cause: cause.to_string(),
                        });
                    }
                }
            }
            Ok(TaskEnd::Disabled) | Ok(TaskEnd::Classified) | Ok(TaskEnd::Completed) => {
                unreachable!()
            }
        }
    }

    /// A Terminal stop: `live` = Stopped{reason}, the event and the notice
    /// at most once per reason, no exit.
    fn terminal(
        &self,
        name: &str,
        reason: &str,
        event: Option<StateHealthEvent>,
        notice: Option<Notice>,
    ) {
        self.stop(reason);
        let first = self
            .notices
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(reason.to_string());
        tracing::warn!(
            task = name,
            reason,
            "supervise: terminal stop; the daemon stays up, degraded (no exit)"
        );
        if !first {
            return;
        }
        if let Some(e) = event {
            raise_state_event(e);
        }
        if let Some(n) = notice {
            self.host.notify(&n.title, &n.body);
        }
    }

    /// `exit_once(70)` (gate C1 rule 2): claim, the bounded store flush
    /// (D14), then the C9 re-check, then exit 70 (or 74).
    pub fn exit_70(&self, who: &str) {
        if !self.gate.try_claim(EXIT_TASK_ENDED) {
            tracing::info!(
                who,
                claimed_by = ?self.gate.claimed(),
                "supervise: exit already claimed; leaving it to the claimant"
            );
            return;
        }
        // The flush, the re-check and the log flush block for up to ~2.3 s;
        // off the runtime worker (D3), so tasks woken by the stop (a parked
        // V&R) still run.
        crate::journal_owner::offload(|| {
            self.pre_exit_flush();
            if self.custody_lost_before_leaving(who, Leave::Exit(EXIT_TASK_ENDED)) {
                return;
            }
            self.leave(Leave::Exit(EXIT_TASK_ENDED));
        });
    }

    /// The stall watchdog's PushStall/WatcherStale restart and the updater's
    /// staged-update restart: claim `exit_once`, then the C9 re-check, then
    /// `app.restart()`. Returns false (and does nothing) when an exit or a
    /// restart is already claimed.
    pub fn restart_once(&self, who: &str) -> bool {
        if !self.gate.try_claim(CLAIM_RESTART) {
            tracing::info!(
                who,
                claimed_by = ?self.gate.claimed(),
                "supervise: exit already claimed; no restart"
            );
            return false;
        }
        crate::journal_owner::offload(|| {
            if self.custody_lost_before_leaving(who, Leave::Restart) {
                return;
            }
            self.leave(Leave::Restart);
        });
        true
    }

    /// Gate C9: right before leaving, re-check the mutation latch and
    /// `custody_all()`. If custody is lost (before or after the claim, or
    /// during the flush), exit 74 after one ERROR naming both causes, and
    /// return true.
    fn custody_lost_before_leaving(&self, who: &str, intended: Leave) -> bool {
        #[cfg(test)]
        self.hooks.run_before_recheck(who);
        // Gate K1: commit BEFORE the re-check. A custody loss after this
        // point is handled by `ownership_lost` itself (it exits 74), so no
        // window remains in which the loss is missed by both sides.
        self.gate.commit();
        let latched_before = self.gate.mutation_latched();
        let custody = crate::state_io::custody_all();
        let mutation_latch = self.gate.mutation_latched();
        if !(latched_before || mutation_latch || custody.is_err()) {
            return false;
        }
        tracing::error!(
            who,
            intended = ?intended,
            mutation_latch,
            custody = ?custody.err().map(|l| l.reason.as_str()),
            "supervise: state-dir custody was lost while {who} was leaving ({intended:?}); exiting 74 instead (CS-3 gate C9)"
        );
        crate::state_io::flush_logs_bounded();
        self.host.exit(crate::state_io::EXIT_OWNERSHIP_LOST);
        true
    }

    fn leave(&self, how: Leave) {
        #[cfg(test)]
        self.hooks.run_in_leave();
        crate::state_io::flush_logs_bounded();
        match how {
            Leave::Exit(code) => self.host.exit(code),
            Leave::Restart => self.host.restart(),
        }
    }

    /// D14: run every registered store flush on a helper thread, waiting at
    /// most 2 s in total. Every write goes through `guard_mutation`, so a
    /// write attempted after the mutation latch is refused, not issued.
    fn pre_exit_flush(&self) {
        let fs: Vec<PreExitFlush> = self
            .pre_exit
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let (tx, rx) = std::sync::mpsc::channel();
        #[cfg(test)]
        let mut in_flush = self.hooks.take_in_pre_exit_flush();
        std::thread::spawn(move || {
            #[cfg(test)]
            if let Some(h) = in_flush.as_mut() {
                h();
            }
            for f in fs {
                f();
            }
            let _ = tx.send(());
        });
        if rx.recv_timeout(PRE_EXIT_FLUSH_BUDGET).is_err() {
            tracing::warn!(
                "supervise: pre-exit store flush exceeded its 2 s budget; leaving anyway"
            );
        }
    }
}

/// Test seams for the exit-precedence orders (T-CS3-19, T-CS3-22), held
/// per supervisor so parallel in-process tests never see each other's.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;

    pub(crate) type Hook = Box<dyn FnMut(&str) + Send>;
    pub(crate) type FlushHook = Box<dyn FnMut() + Send>;

    #[derive(Default)]
    pub(crate) struct Hooks {
        before_recheck: Mutex<Option<Hook>>,
        in_pre_exit_flush: Mutex<Option<FlushHook>>,
        in_leave: Mutex<Option<FlushHook>>,
    }

    impl Hooks {
        pub(super) fn run_before_recheck(&self, who: &str) {
            if let Some(h) = self
                .before_recheck
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_mut()
            {
                h(who);
            }
        }
        pub(super) fn run_in_leave(&self) {
            let h = self
                .in_leave
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            if let Some(mut h) = h {
                h();
            }
        }
        pub(super) fn take_in_pre_exit_flush(&self) -> Option<FlushHook> {
            self.in_pre_exit_flush
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
        }
    }

    impl super::Supervisor {
        /// Runs after a leaver's claim, right before its C9 re-check.
        pub(crate) fn set_before_recheck(&self, h: Option<Hook>) {
            *self
                .hooks
                .before_recheck
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = h;
        }
        /// K1 order D: runs once inside `leave()`, after the re-check and
        /// before the log flush and the exit or restart.
        pub(crate) fn set_in_leave(&self, h: Option<FlushHook>) {
            *self
                .hooks
                .in_leave
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = h;
        }
        /// Runs once inside the pre-exit flush, before the store flushes.
        pub(crate) fn set_in_pre_exit_flush(&self, h: Option<FlushHook>) {
            *self
                .hooks
                .in_pre_exit_flush
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = h;
        }
    }
}
