use std::{cmp::Ordering, ops::Range};

use parking_lot::{Condvar, Mutex};

use crate::{
    format::{Allocation, PAGE_SIZE_U64},
    io,
    ranges::{Ranges, Span},
    utils::{Multiples, RangeLength},
};

#[derive(Debug)]
pub struct FileAllocations {
    data: Mutex<Data>,
    resize_sync: Condvar,
}

#[derive(Debug)]
struct Data {
    allocations: Ranges<Allocation>,
    new_file_size: u64,
    is_resizing: bool,
}

impl FileAllocations {
    pub fn new(file_size: u64) -> Self {
        let file_size_in_pages = file_size.ceil_div_by(PAGE_SIZE_U64);
        Self {
            data: Mutex::new(Data {
                allocations: Ranges::new(Allocation::Free, file_size_in_pages.checked_sub(1)),
                new_file_size: file_size_in_pages,
                is_resizing: false,
            }),
            resize_sync: Condvar::new(),
        }
    }

    pub fn allocate<File: io::File>(&self, length: u64, file: &mut File) -> io::Result<u64> {
        let mut data = self.data.lock();
        let pages = length.ceil_div_by(PAGE_SIZE_U64);
        let mut best_allocation = None;
        for (range, _) in data
            .allocations
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
            data.allocations
                .set(location..location + pages, Allocation::Allocated);
            return Ok(location * PAGE_SIZE_U64);
        }

        let current_length = data.allocations.maximum().unwrap() + 1;
        let start = if let Some(Span {
            tag: Allocation::Free,
            start,
        }) = data.allocations.last()
        {
            // Reuse the free space already at the tail of the file
            let start = *start;
            let space_to_use = current_length - start;
            data.allocations.set(start.., Allocation::Allocated);
            // Allocate whatever we still need
            let new_amount = pages - space_to_use;
            data.allocations
                .extend_by(new_amount, Allocation::Allocated);
            start
        } else {
            data.allocations.extend_by(pages, Allocation::Allocated);
            current_length
        };

        // We need to resize the file, but setting the file's length is "slow".
        // We are going to allow multiple requests to resize to "pile up", and
        // each time a thread regains control, its only responsibilty is to
        // ensure that before returning, the file is at least as long as
        // `current_length`
        let file_size_needed = current_length + pages;
        loop {
            if file_size_needed <= data.new_file_size {
                if data.is_resizing {
                    // Another thread is currently resizing. Wait for it to
                    // finish.
                    self.resize_sync.wait(&mut data);
                } else {
                    // Become the resizing thread.
                    data.is_resizing = true;
                    let new_page_count = data.allocations.maximum().unwrap() + 1;
                    drop(data);

                    // Now that other threads are free, perform the expensive operation.
                    let result = file.set_length(new_page_count * PAGE_SIZE_U64);

                    // Re-aquire the lock
                    data = self.data.lock();
                    // We want to handle our multiprocessing guarantees before
                    // we process any errors.
                    if result.is_ok() {
                        data.new_file_size = new_page_count;
                    }
                    data.is_resizing = false;
                    drop(data);
                    self.resize_sync.notify_all();

                    // Now that we've notified and unblocked any other threads,
                    // report any error that the file operation might have
                    // caused.
                    result?;
                    break;
                }
            } else {
                // The file is now long enough.
                break;
            }
        }

        Ok(start * PAGE_SIZE_U64)
    }

    pub fn set(&self, range: Range<u64>, allocation: Allocation) {
        assert_eq!(range.start % PAGE_SIZE_U64, 0);
        let start = range.start / PAGE_SIZE_U64;
        let mut data = self.data.lock();
        data.allocations
            .set(start..range.end.ceil_div_by(PAGE_SIZE_U64), allocation);
    }

    pub fn statistics(&self) -> FileAllocationStatistics {
        let data = self.data.lock();
        data.allocations.iter().fold(
            FileAllocationStatistics::default(),
            |mut stats, (range, tag)| {
                let length = range.len() * PAGE_SIZE_U64;
                match tag {
                    Allocation::Free => {
                        stats.free_space += length;
                        stats.largest_contiguous_free_space =
                            stats.largest_contiguous_free_space.max(length);
                    }
                    Allocation::Allocated => {
                        stats.allocated_space += length;
                    }
                }
                stats
            },
        )
    }
}

#[derive(Default, Debug)]
pub struct FileAllocationStatistics {
    pub free_space: u64,
    pub allocated_space: u64,
    pub largest_contiguous_free_space: u64,
}
