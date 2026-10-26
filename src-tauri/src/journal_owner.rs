//! Unit CS-3 (TKT-b6d8c4ba) §3.2: one owner per push journal file per
//! process (I-OWN-4, AR-1C-018).
//!
//! A [`JournalOwner`] wraps the one [`PushJournal`] handle a process holds on
//! a subscriber's journal. Every caller borrows it through an `Arc`: the
//! watcher, the drain loop, the reconcile backstop, the stall watchdog, the
//! anti-strip compensating push and Verify and Repair. There is no outer
//! mutex (D1): `PushJournal` already serializes every operation on one handle
//! with its 1a `local` mutex and flock, so the owner holds no guard and no
//! guard can cross an `.await`.
//!
//! Every method runs its body through [`offload`] (D3, gate C3), so a journal
//! fsync never stalls a tokio runtime worker.

use std::sync::Arc;

use crate::push_journal::{JournalCursor, JournalError, PushEvent, PushJournal};
use crate::state_io::{DisplayPath, StateDir, StateFile};

/// The one owner of a subscriber's push journal in this process.
pub struct JournalOwner {
    j: PushJournal,
    sub: String,
}

impl std::fmt::Debug for JournalOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalOwner")
            .field("sub", &self.sub)
            .finish()
    }
}

impl JournalOwner {
    /// Production constructor. Claims the `StateDir`'s `PushJournal` owner
    /// bit; a second claim on the same `StateDir` returns
    /// [`JournalError::AlreadyOwned`], even after every `Arc` of the first
    /// owner was dropped (the claim is never released, gate C8).
    ///
    /// Gate C2 (T-CS3-25): the only production call is the `SyncHandles`
    /// builder's get-or-open (`boot::JournalTable::journal_for_root`), keyed
    /// by a root's effective subscriber.
    pub fn open(sd: &Arc<StateDir>, sub: &str) -> Result<Arc<Self>, JournalError> {
        let claim = sd
            .claim(StateFile::PushJournal)
            .map_err(|e| JournalError::AlreadyOwned(e.to_string()))?;
        let j = offload(|| PushJournal::open_claimed(sd, claim))?;
        Ok(Arc::new(Self {
            j,
            sub: sub.to_string(),
        }))
    }

    /// D9 seed only (gate C10 stays exact, D12): wraps the scratch sink.
    /// A Dir-backed journal is refused.
    pub(crate) fn wrap_scratch(j: PushJournal) -> Result<Arc<Self>, JournalError> {
        if !j.is_scratch() {
            return Err(JournalError::AlreadyOwned(
                "wrap_scratch takes only the D9 scratch sink".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            j,
            sub: String::new(),
        }))
    }

    pub fn append(&self, e: PushEvent) -> Result<(), JournalError> {
        offload(|| self.j.append(e))
    }

    pub fn append_batch(&self, v: Vec<PushEvent>) -> Result<usize, JournalError> {
        offload(|| self.j.append_batch(v))
    }

    pub fn drain(&self, n: usize) -> Result<Vec<(PushEvent, JournalCursor)>, JournalError> {
        offload(|| self.j.drain(n))
    }

    pub fn ack_batch(&self, c: Vec<JournalCursor>) -> Result<(), JournalError> {
        offload(|| self.j.ack_batch(c))
    }

    pub fn len(&self) -> usize {
        offload(|| self.j.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn compact(&self) -> Result<(usize, usize), JournalError> {
        offload(|| self.j.compact())
    }

    /// The journal's operator text (delegates to the zero-argument
    /// `PushJournal::display()`, gate C7 clean).
    pub fn display(&self) -> Option<DisplayPath> {
        self.j.display()
    }

    /// The subscriber whose journal this is (empty for the scratch sink).
    pub fn sub(&self) -> &str {
        &self.sub
    }

    /// Test access to the wrapped handle (fault injection, scratch counts).
    #[cfg(test)]
    pub(crate) fn inner(&self) -> &PushJournal {
        &self.j
    }

    /// Test access to the journal's held state dir.
    #[cfg(test)]
    pub(crate) fn state_dir(&self) -> Option<Arc<StateDir>> {
        self.j.state_dir_for_test()
    }

    /// Test constructor over an unclaimed handle (unit tests that build a
    /// journal per test without a registry).
    #[cfg(test)]
    pub(crate) fn for_test(j: PushJournal, sub: &str) -> Arc<Self> {
        Arc::new(Self {
            j,
            sub: sub.to_string(),
        })
    }
}

/// How [`offload`] ran a closure (T-CS3-10 observation).
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OffloadPath {
    Inline,
    BlockInPlace,
}

#[cfg(test)]
thread_local! {
    static IN_OFFLOAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LAST_PATH: std::cell::Cell<Option<OffloadPath>> = const { std::cell::Cell::new(None) };
}

/// T-CS3-10: true while the current thread runs inside [`offload`].
#[cfg(test)]
pub(crate) fn in_offload() -> bool {
    IN_OFFLOAD.with(|c| c.get())
}

/// T-CS3-10: the path the last [`offload`] on this thread took.
#[cfg(test)]
pub(crate) fn last_offload_path() -> Option<OffloadPath> {
    LAST_PATH.with(|c| c.get())
}

/// D3 / gate C3: run a synchronous journal body without stalling a runtime
/// worker. Inline only when there is no runtime on this thread
/// (`Handle::try_current()` is `Err`) or it is a current-thread runtime
/// (where `block_in_place` would panic); every other case goes through
/// `tokio::task::block_in_place`, which hands the worker's other tasks to a
/// fresh worker while the closure blocks. From a `spawn_blocking` thread or
/// a thread inside a multi-thread runtime's `block_on`, `block_in_place`
/// runs the closure in place (T-CS3-10 measures all five contexts).
pub(crate) fn offload<R>(f: impl FnOnce() -> R) -> R {
    use tokio::runtime::{Handle, RuntimeFlavor};
    let inline = match Handle::try_current() {
        Err(_) => true,
        Ok(h) => h.runtime_flavor() == RuntimeFlavor::CurrentThread,
    };
    if inline {
        #[cfg(test)]
        LAST_PATH.with(|c| c.set(Some(OffloadPath::Inline)));
        run_marked(f)
    } else {
        #[cfg(test)]
        LAST_PATH.with(|c| c.set(Some(OffloadPath::BlockInPlace)));
        tokio::task::block_in_place(|| run_marked(f))
    }
}

#[inline]
fn run_marked<R>(f: impl FnOnce() -> R) -> R {
    #[cfg(test)]
    {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                let prev = self.0;
                IN_OFFLOAD.with(|c| c.set(prev));
            }
        }
        let _reset = Reset(IN_OFFLOAD.with(|c| c.replace(true)));
        f()
    }
    #[cfg(not(test))]
    f()
}

/// Test helper: an owner over a fresh unclaimed handle on `sd`, opened
/// through [`offload`] so it works from any test context.
#[cfg(test)]
pub(crate) fn test_owner(sd: &Arc<StateDir>) -> Arc<JournalOwner> {
    let j = offload(|| PushJournal::open(sd)).expect("open test journal");
    JournalOwner::for_test(j, "test")
}
