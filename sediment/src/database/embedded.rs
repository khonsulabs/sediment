use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{lock_api::ArcMutexGuard, Mutex, RawMutex};

use crate::format::GrainId;

#[derive(Debug)]
pub struct EmbeddedHeaderState {
    current: AtomicU64,
    pending: Arc<Mutex<Option<GrainId>>>,
}

impl EmbeddedHeaderState {
    pub fn new(current: Option<GrainId>) -> Self {
        Self {
            current: AtomicU64::new(current.map_or(0, u64::from)),
            pending: Arc::new(Mutex::new(current)),
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

    pub fn lock(&self) -> EmbeddedHeaderGuard {
        EmbeddedHeaderGuard(self.pending.lock_arc())
    }

    pub fn publish(&self, new_embedded_header: Option<GrainId>) {
        self.current
            .store(new_embedded_header.map_or(0, u64::from), Ordering::SeqCst);
    }
}

#[must_use]
pub struct EmbeddedHeaderGuard(ArcMutexGuard<RawMutex, Option<GrainId>>);

impl Deref for EmbeddedHeaderGuard {
    type Target = Option<GrainId>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl DerefMut for EmbeddedHeaderGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl Debug for EmbeddedHeaderGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EmbeddedHeaderGuard").field(&*self).finish()
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
