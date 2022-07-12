use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;

use crate::format::BatchId;

#[derive(Default, Debug, Clone)]
pub struct CheckpointProtector {
    data: Arc<Mutex<CheckpointProtectorData>>,
}

impl CheckpointProtector {
    pub fn guard(&self, batch_id: BatchId) -> CheckpointGuard {
        let mut data = self.data.lock();
        CheckpointGuard {
            protector: self.clone(),
            guard_data: data.guard(batch_id),
        }
    }

    pub fn safe_to_checkpoint_batch_id(&self, batch_id: BatchId) -> Option<BatchId> {
        let data = self.data.lock();
        data.safe_to_checkpoint_batch_id(batch_id)
    }

    fn free(&self, index: usize) {
        let mut data = self.data.lock();
        data.free(index);
    }
}

#[derive(Default, Debug)]
struct CheckpointProtectorData {
    slots: Vec<Arc<CheckpointGuardData>>,
    free_slots: VecDeque<usize>,
    ordered_slots: Vec<usize>,
}

impl CheckpointProtectorData {
    pub fn guard(&mut self, batch_id: BatchId) -> Arc<CheckpointGuardData> {
        let guard_data = if let Some(empty_index) = self.free_slots.pop_front() {
            let data = self.slots[empty_index].clone();
            data.batch_id.store(batch_id.0, Ordering::SeqCst);
            data
        } else {
            // Allocate a new slot
            let data = Arc::new(CheckpointGuardData {
                index: self.slots.len(),
                batch_id: AtomicU64::new(batch_id.0),
            });
            self.slots.push(data.clone());
            data
        };

        let insert_at = self
            .free_slots
            .binary_search_by_key(&batch_id.0, |slot| {
                self.slots[*slot].batch_id.load(Ordering::SeqCst)
            })
            .map_or_else(|e| e, |f| f);
        self.free_slots.insert(insert_at, guard_data.index);
        guard_data
    }

    pub fn safe_to_checkpoint_batch_id(&self, batch_id: BatchId) -> Option<BatchId> {
        if let Some(first_index) = self.ordered_slots.first() {
            let slot_batch_id = self.slots[*first_index].batch_id.load(Ordering::SeqCst);
            if slot_batch_id < batch_id.0 {
                return slot_batch_id.checked_sub(1).map(BatchId);
            }
        }

        Some(batch_id)
    }

    fn free(&mut self, index: usize) {
        self.ordered_slots.retain(|slot| *slot != index);
        self.free_slots.push_front(index);
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointGuard {
    protector: CheckpointProtector,
    guard_data: Arc<CheckpointGuardData>,
}

impl Eq for CheckpointGuard {}

impl PartialEq for CheckpointGuard {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.guard_data, &other.guard_data)
    }
}

impl PartialOrd for CheckpointGuard {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if Arc::ptr_eq(&self.protector.data, &other.protector.data) {
            Some(self.batch_id().cmp(&other.batch_id()))
        } else {
            None
        }
    }
}

impl CheckpointGuard {
    #[must_use]
    pub fn batch_id(&self) -> BatchId {
        BatchId(self.guard_data.batch_id.load(Ordering::SeqCst))
    }
}

impl Drop for CheckpointGuard {
    fn drop(&mut self) {
        // The CheckpointProtector holds one strong ref, so we check for 2
        // strong counts. This signifies that this is the last CommitGuard with
        // a reference to the data.
        if Arc::strong_count(&self.guard_data) == 2 {
            self.protector.free(self.guard_data.index);
        }
    }
}

#[derive(Debug)]
struct CheckpointGuardData {
    index: usize,
    batch_id: AtomicU64,
}
