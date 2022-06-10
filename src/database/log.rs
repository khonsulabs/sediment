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
    ) -> io::Result<Self> {
        let mut pages = VecDeque::new();
        let mut reached_checkpoint = false;
        scratch.resize(PAGE_SIZE, 0);
        while offset != 0 && !reached_checkpoint {
            // TODO add a sanity check that offset is page-aligned

            let buffer = std::mem::take(scratch);
            let (result, buffer) = file.read_exact(buffer, offset);
            *scratch = buffer;
            result?;

            let page = LogPage::deserialize_from(scratch)?;
            let next_offset = page.previous_offset;
            reached_checkpoint = page.entries[0].batch <= checkpoint;
            pages.push_front(CommitLogPage {
                offset: Some(offset),
                page,
            });
            offset = next_offset;
        }

        let tail_checkpoint_index = pages
            .front()
            .and_then(|commit_page| {
                commit_page
                    .page
                    .entries
                    .iter()
                    .enumerate()
                    .find_map(|(index, entry)| (entry.batch > checkpoint).then(|| index))
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
                    .find_map(|(index, entry)| (entry.batch.0 == 0).then(|| index))
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

    pub fn push(&mut self, entry: LogEntryIndex) {
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

    pub fn checkpoint(&mut self, last_committed: BatchId, allocations: &mut FileAllocations) {
        while let Some(page_state) = self.pages.front() {
            match page_state
                .page
                .entries
                .iter()
                .enumerate()
                .find(|(_, entry)| entry.batch > last_committed)
            {
                Some((keep_index, _)) => {
                    self.tail_checkpoint_index = keep_index;
                    break;
                }
                None => {
                    // No IDs found that should be kept, remove the page.
                    if let Some(removed_offset) =
                        self.pages.pop_front().and_then(|removed| removed.offset)
                    {
                        allocations.set(
                            removed_offset..removed_offset + PAGE_SIZE_U64,
                            Allocation::Free,
                        );
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct CommitLogPage {
    pub page: LogPage,
    pub offset: Option<u64>,
}
