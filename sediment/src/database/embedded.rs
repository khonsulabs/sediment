use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{Condvar, Mutex};

use crate::format::GrainId;

#[derive(Debug)]
pub struct EmbeddedHeaderState {
    current: AtomicU64,
    pending: GrainMutex,
}

impl EmbeddedHeaderState {
    pub fn new(current: Option<GrainId>) -> Self {
        Self {
            current: AtomicU64::new(current.map_or(0, u64::from)),
            pending: GrainMutex::new(current),
        }
    }

    pub fn current(&self) -> Option<GrainId> {
        let id = self.current.load(Ordering::SeqCst);
        if id == 0 {
            None
        } else {
            Some(GrainId::from(id))
        }
    }

    pub fn lock(&self) -> GrainMutexGuard {
        self.pending.lock()
    }

    pub fn publish(&self, new_embedded_header: Option<GrainId>) {
        self.current
            .store(new_embedded_header.map_or(0, u64::from), Ordering::SeqCst);
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EmbeddedHeaderUpdate {
    None,
    Replace(Option<GrainId>),
}

impl Default for EmbeddedHeaderUpdate {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Default)]
struct GrainMutex {
    data: Arc<GrainMutexData>,
}

impl GrainMutex {
    pub fn new(current: Option<GrainId>) -> Self {
        Self {
            data: Arc::new(GrainMutexData {
                locked: AtomicBool::new(false),
                grain_id: AtomicU64::new(current.map_or(0, |g| g.as_u64())),
                parker: Mutex::default(),
                sync: Condvar::default(),
            }),
        }
    }

    pub fn lock(&self) -> GrainMutexGuard {
        // Try to set the locked flag.
        if self
            .data
            .locked
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            let mut guard = self.data.parker.lock();
            // It's possible that we could lock the mutex now, so try again.
            if self
                .data
                .locked
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                // Wait for a signal that the current lock has been released.
                self.data.sync.wait(&mut guard);

                // Once we're woken up, we need to set locked again.
                let was_locked = self.data.locked.swap(true, Ordering::SeqCst);
                assert!(!was_locked);
            }
        }

        GrainMutexGuard {
            mutex: self.clone(),
        }
    }

    fn unlock(&self) {
        let was_locked = self.data.locked.swap(false, Ordering::SeqCst);
        assert!(was_locked);

        self.data.sync.notify_one();
    }
}

#[derive(Debug, Default)]
struct GrainMutexData {
    locked: AtomicBool,
    grain_id: AtomicU64,
    parker: Mutex<()>,
    sync: Condvar,
}

#[derive(Debug)]
pub struct GrainMutexGuard {
    mutex: GrainMutex,
}

impl GrainMutexGuard {
    pub fn set(&self, grain_id: Option<GrainId>) {
        self.mutex.data.grain_id.store(
            grain_id.map_or(0, |grain_id| grain_id.as_u64()),
            Ordering::SeqCst,
        );
    }

    #[must_use]
    pub fn get(&self) -> Option<GrainId> {
        match self.mutex.data.grain_id.load(Ordering::SeqCst) {
            0 => None,
            other => Some(GrainId::from(other)),
        }
    }

    #[must_use]
    pub fn swap(&self, grain_id: Option<GrainId>) -> Option<GrainId> {
        match self.mutex.data.grain_id.swap(
            grain_id.map_or(0, |grain_id| grain_id.as_u64()),
            Ordering::SeqCst,
        ) {
            0 => None,
            other => Some(GrainId::from(other)),
        }
    }
}

impl Drop for GrainMutexGuard {
    fn drop(&mut self) {
        self.mutex.unlock();
    }
}
