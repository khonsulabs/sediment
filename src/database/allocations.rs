use std::{cmp::Ordering, ops::Range};

use parking_lot::Mutex;

use crate::{
    format::{Allocation, PAGE_SIZE_U64},
    io,
    ranges::{Ranges, Span},
    utils::Multiples,
};

#[derive(Debug)]
pub struct FileAllocations(Mutex<Ranges<Allocation>>);

impl FileAllocations {
    pub fn new(file_size: u64) -> Self {
        Self(Mutex::new(Ranges::new(
            Allocation::Free,
            file_size.ceil_div_by(PAGE_SIZE_U64).checked_sub(1),
        )))
    }

    pub fn allocate<File: io::File>(&self, length: u64, file: &mut File) -> io::Result<u64> {
        let mut allocations = self.0.lock();
        let pages = length.ceil_div_by(PAGE_SIZE_U64);
        let mut best_allocation = None;
        for (range, _) in allocations
            .iter()
            .filter(|(_, allocation)| matches!(allocation, Allocation::Free))
        {
            let available_amount = range.end().saturating_sub(*range.start());
            let comparison = available_amount.cmp(&pages);
            if matches!(comparison, Ordering::Greater | Ordering::Equal) {
                best_allocation = Some((available_amount, range));
                if matches!(comparison, Ordering::Equal) {
                    break;
                }
            }
        }

        if let Some((_, range)) = best_allocation {
            let location = *range.start();
            allocations.set(range, Allocation::Allocated);
            return Ok(location);
        }

        let current_length = allocations.maximum().unwrap() + 1;
        let start = match allocations.last() {
            Some(Span {
                tag: Allocation::Free,
                start,
            }) => {
                // Reuse the free space already at the tail of the file
                let start = *start;
                let space_to_use = current_length - start;
                allocations.set(start.., Allocation::Allocated);
                // Allocate whatever we still need
                let new_amount = pages - space_to_use;
                allocations.extend_by(new_amount, Allocation::Allocated);
                start
            }
            _ => {
                allocations.extend_by(pages, Allocation::Allocated);
                current_length
            }
        };

        file.set_length((start + pages) * PAGE_SIZE_U64)?;

        Ok(start * PAGE_SIZE_U64)
    }

    pub fn set(&self, range: Range<u64>, allocation: Allocation) {
        let mut allocations = self.0.lock();
        assert_eq!(range.start % PAGE_SIZE_U64, 0);
        let start = range.start / PAGE_SIZE_U64;
        allocations.set(start..range.end.ceil_div_by(PAGE_SIZE_U64), allocation);
    }
}
