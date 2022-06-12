use std::collections::VecDeque;

use crate::{
    database::allocations::FileAllocations,
    format::{Allocation, BatchId, LogEntryIndex, LogPage, PAGE_SIZE, PAGE_SIZE_U64},
    io,
};

#[derive(Debug, Default)]
pub struct CommitLog {
    pub pages: VecDeque<CommitLogPage>,
    tail_checkpoint_index: usize,
    head_insert_index: usize,
}

impl CommitLog {
    pub fn position(&self) -> Option<u64> {
        self.pages.back().and_then(|state| state.offset)
    }

    // TODO this structure needs to be able to inform file allocations
    pub fn read_from<File: io::File>(
        mut offset: u64,
        file: &mut File,
        scratch: &mut Vec<u8>,
        checkpoint: BatchId,
        file_allocations: &FileAllocations,
    ) -> io::Result<Self> {
        let mut pages = VecDeque::new();
        let mut reached_checkpoint = false;
        scratch.resize(PAGE_SIZE, 0);
        while offset != 0 && !reached_checkpoint {
            // TODO add a sanity check that offset is page-aligned
            file_allocations.set(offset..offset + PAGE_SIZE_U64, Allocation::Allocated);

            let buffer = std::mem::take(scratch);
            let (result, buffer) = file.read_exact(buffer, offset);
            *scratch = buffer;
            result?;

            let page = LogPage::deserialize_from(scratch)?;
            for entry in &page.entries {
                if entry.position > 0 {
                    file_allocations.set(
                        entry.position..entry.position + u64::from(entry.length),
                        Allocation::Allocated,
                    );
                } else {
                    break;
                }
            }
            let next_offset = page.previous_offset;
            reached_checkpoint = page.first_batch_id <= checkpoint;
            pages.push_front(CommitLogPage {
                offset: Some(offset),
                page,
            });
            offset = next_offset;
        }

        let tail_checkpoint_index = pages
            .front()
            .and_then(|commit_page| {
                if let Some(commits_after_first) =
                    checkpoint.0.checked_sub(commit_page.page.first_batch_id.0)
                {
                    assert!(commits_after_first < 170);
                    Some(usize::try_from(commits_after_first).unwrap())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let head_insert_index = pages
            .back()
            .and_then(|commit_page| {
                commit_page
                    .page
                    .entries
                    .iter()
                    .enumerate()
                    .find_map(|(index, entry)| (entry.position != 0).then(|| index))
            })
            .unwrap_or_default();

        Ok(Self {
            pages,
            tail_checkpoint_index,
            head_insert_index,
        })
    }
    pub fn write_to<File: io::File>(
        &mut self,
        allocations: &FileAllocations,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let last_index = self.pages.len() - 1;
        for (_, page_state) in self
            .pages
            .iter_mut()
            .enumerate()
            // If the page has no offset or is the last page, we need to write
            // it.
            .filter(|(index, page_state)| index == &last_index || page_state.offset.is_none())
        {
            let offset = if let Some(offset) = page_state.offset {
                offset
            } else {
                let offset = allocations.allocate(PAGE_SIZE_U64, file)?;
                page_state.offset = Some(offset);
                offset
            };

            page_state.page.serialize_into(scratch);
            let buffer = std::mem::take(scratch);
            let (result, buffer) = file.write_all(buffer, offset);
            *scratch = buffer;
            result?;
        }

        Ok(())
    }

    pub fn push(&mut self, batch: BatchId, entry: LogEntryIndex) {
        // TODO assert batch
        if self.pages.is_empty() || self.head_insert_index == 170 {
            // Create a new empty log page for this entry.
            let previous_offset = self
                .pages
                .back()
                .and_then(|page| page.offset)
                .unwrap_or_default();
            self.pages.push_back(CommitLogPage {
                page: LogPage {
                    previous_offset,
                    first_batch_id: batch,
                    ..LogPage::default()
                },
                offset: None,
            });
            self.head_insert_index = 0;
        }

        let tail = self.pages.back_mut().unwrap();
        tail.page.entries[self.head_insert_index] = entry;
        self.head_insert_index += 1;
    }

    // pub fn checkpoint(&mut self, last_committed: BatchId, allocations: &mut FileAllocations) {
    //     while let Some(page_state) = self.pages.front() {
    //         match page_state
    //             .page
    //             .entries
    //             .iter()
    //             .enumerate()
    //             .find(|(_, entry)| entry.batch > last_committed)
    //         {
    //             Some((keep_index, _)) => {
    //                 self.tail_checkpoint_index = keep_index;
    //                 break;
    //             }
    //             None => {
    //                 // No IDs found that should be kept, remove the page.
    //                 if let Some(removed_offset) =
    //                     self.pages.pop_front().and_then(|removed| removed.offset)
    //                 {
    //                     allocations.set(
    //                         removed_offset..removed_offset + PAGE_SIZE_U64,
    //                         Allocation::Free,
    //                     );
    //                 }
    //             }
    //         }
    //     }
    // }
}

#[derive(Debug, Default)]
pub struct CommitLogPage {
    pub page: LogPage,
    pub offset: Option<u64>,
}
