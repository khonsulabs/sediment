use std::{
    collections::VecDeque,
    ops::{Deref, RangeInclusive},
};

use crate::{
    database::{allocations::FileAllocations, Database},
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

    pub fn most_recent_page(&self) -> Option<&CommitLogPage> {
        self.pages.back()
    }

    pub fn most_recent_entry(&self) -> Option<(BatchId, &LogEntryIndex)> {
        if self.pages.is_empty() {
            None
        } else if self.head_insert_index == 0 {
            // The most recent entry is on the second-to-last page.
            if let Some(index) = self.pages.get(self.pages.len() - 1) {
                index
                    .page
                    .entries
                    .last()
                    .map(|entry| (BatchId(index.page.first_batch_id.0 + 169), entry))
            } else {
                None
            }
        } else {
            let entry_index = self.head_insert_index - 1;
            self.most_recent_page().map(|index| {
                (
                    BatchId(index.page.first_batch_id.0 + u64::try_from(entry_index).unwrap()),
                    &index.page.entries[entry_index],
                )
            })
        }
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

            let mut buffer = std::mem::take(scratch);
            buffer.resize(PAGE_SIZE, 0);
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
                if commit_page.page.entries[0].position > 0 {
                    // This page has at least one entry. If the page is full, we
                    // should return 170 so that the next push generates a new
                    // page.
                    Some(
                        commit_page.page.entries[1..]
                            .iter()
                            .enumerate()
                            .find_map(|(index, entry)| (entry.position == 0).then(|| index + 1))
                            .unwrap_or(170),
                    )
                } else {
                    None
                }
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
        let mut last_page_offset = 0;

        for (page_index, page_state) in self.pages.iter_mut().enumerate() {
            // If the page has no offset or is the last page, we need to write
            // it.
            last_page_offset = if page_index == last_index
                || page_state.offset.is_none()
                || page_state.page.previous_offset != last_page_offset
            {
                let offset = if let Some(offset) = page_state.offset {
                    offset
                } else {
                    let offset = allocations.allocate(PAGE_SIZE_U64, file)?;
                    page_state.offset = Some(offset);
                    offset
                };

                page_state.page.previous_offset = last_page_offset;
                page_state.page.serialize_into(scratch);
                let buffer = std::mem::take(scratch);
                let (result, buffer) = file.write_all(buffer, offset);
                *scratch = buffer;
                result?;

                offset
            } else {
                page_state.offset.unwrap()
            }
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

    pub fn snapshot<Manager: io::FileManager>(
        &self,
        database: &Database<Manager>,
    ) -> CommitLogSnapshot {
        CommitLogSnapshot::new(
            database.checkpoint().next()..=database.current_batch(),
            self.pages.iter().map(|page| &page.page),
        )
    }

    pub fn checkpoint(&mut self, remove_batches_to_inclusive: BatchId) -> Vec<CommitLogPage> {
        let first_batch_to_keep = remove_batches_to_inclusive.next();
        let mut checkpointed_entries = Vec::new();
        while let Some(page_state) = self.pages.front() {
            if page_state.page.first_batch_id > remove_batches_to_inclusive {
                // The first commit checked is after the one being checkpointed.
                break;
            } else if let Some(tail_checkpoint_index) =
                page_state.page.index_of_batch_id(first_batch_to_keep)
            {
                // Partial checkpoint
                let mut partial_page = CommitLogPage::default();
                let entries_to_copy =
                    &page_state.page.entries[self.tail_checkpoint_index..=tail_checkpoint_index];
                partial_page.page.entries[0..entries_to_copy.len()]
                    .copy_from_slice(entries_to_copy);
                checkpointed_entries.push(partial_page);
                self.tail_checkpoint_index = tail_checkpoint_index;
                break;
            }

            // The entire page can be removed
            self.tail_checkpoint_index = 0;
            checkpointed_entries.push(self.pages.pop_front().unwrap());
        }

        checkpointed_entries
    }
}

#[derive(Debug, Default)]
pub struct CommitLogPage {
    pub page: LogPage,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CommitLogSnapshot {
    starting_batch_id: BatchId,
    entries: Vec<LogEntryIndex>,
}

impl CommitLogSnapshot {
    fn new<'a, Pages: Iterator<Item = &'a LogPage> + ExactSizeIterator>(
        range: RangeInclusive<BatchId>,
        pages: Pages,
    ) -> Self {
        // This allocates more space than is probably needed, but it avoids
        // extra allocations.
        let mut entries = Vec::with_capacity(pages.len() * 170);
        for page in pages {
            let mut batch = page.first_batch_id;
            for entry in &page.entries {
                if entry.position > 0 {
                    if range.contains(&batch) {
                        entries.push(*entry);
                    }
                } else {
                    // Once we hit an empty entry, we can assume the rest are blank.
                    break;
                }
                batch = batch.next();
            }
        }
        Self {
            starting_batch_id: *range.start(),
            entries,
        }
    }

    #[must_use]
    pub const fn earliest_batch_id(&self) -> BatchId {
        self.starting_batch_id
    }

    #[must_use]
    pub fn latest_batch_id(&self) -> BatchId {
        BatchId(
            self.starting_batch_id.0
                + u64::try_from(self.entries.len()).expect("u64 will never be too small"),
        )
    }

    pub fn iter(&self) -> CommitLogIter<'_> {
        self.into_iter()
    }
}

impl Deref for CommitLogSnapshot {
    type Target = [LogEntryIndex];

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<'a> IntoIterator for &'a CommitLogSnapshot {
    type Item = (BatchId, LogEntryIndex);

    type IntoIter = CommitLogIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        CommitLogIter {
            snapshot: self,
            index: Some(0),
            batch: self.starting_batch_id,
        }
    }
}

#[derive(Debug)]
#[must_use]
pub struct CommitLogIter<'a> {
    snapshot: &'a CommitLogSnapshot,
    index: Option<usize>,
    batch: BatchId,
}

impl<'a> Iterator for CommitLogIter<'a> {
    type Item = (BatchId, LogEntryIndex);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(index) = self.index {
            let entry = *self.snapshot.entries.get(index)?;
            let batch = self.batch;

            self.index = index.checked_add(1);
            self.batch = self.batch.next();

            Some((batch, entry))
        } else {
            None
        }
    }
}
