use std::{
    collections::{hash_map, HashMap},
    io::{Read, Write},
    sync::{Arc, Condvar, Mutex},
};

use crate::{
    format::{ByteUtil, GrainId, Stored, TransactionId},
    util::{u32_to_usize, usize_to_u32},
    Database, Result,
};

#[derive(Debug)]
pub struct CommitLogEntry {
    pub transaction_id: TransactionId,
    pub next_entry: Option<GrainId>,
    pub new_grains: Vec<NewGrain>,
    pub archived_grains: Vec<GrainId>,
    pub freed_grains: Vec<GrainId>,

    pub embedded_header_data: Option<GrainId>,
    pub checkpoint_target: TransactionId,
    pub checkpointed_to: TransactionId,
}

impl CommitLogEntry {
    pub fn new(
        transaction_id: TransactionId,
        next_entry: Option<GrainId>,
        embedded_header_data: Option<GrainId>,
        checkpoint_target: TransactionId,
        checkpointed_to: TransactionId,
    ) -> Self {
        Self {
            transaction_id,
            next_entry,
            new_grains: Vec::new(),
            archived_grains: Vec::new(),
            freed_grains: Vec::new(),
            embedded_header_data,
            checkpoint_target,
            checkpointed_to,
        }
    }

    pub fn serialize_to(&self, bytes: &mut Vec<u8>) -> Result<()> {
        let total_size = 8 // transaction_id
            + 8 // next_entry
            + 8 // embedded_header_data
            + 8 // checkpoint_target
            + 8 // checkpointed_to
            + 4 * 3 // u32 counts of all three grain types
            + (self.new_grains.len() * NewGrain::BYTES)
            + (self.archived_grains.len() + self.freed_grains.len()) * 8;
        bytes.clear();
        bytes.reserve(total_size);

        bytes.write_all(&self.transaction_id.to_be_bytes())?;
        bytes.write_all(&self.next_entry.to_be_bytes())?;

        bytes.write_all(&self.embedded_header_data.to_be_bytes())?;
        bytes.write_all(&self.checkpoint_target.to_be_bytes())?;
        bytes.write_all(&self.checkpointed_to.to_be_bytes())?;

        bytes.write_all(&usize_to_u32(self.new_grains.len())?.to_be_bytes())?;
        bytes.write_all(&usize_to_u32(self.archived_grains.len())?.to_be_bytes())?;
        bytes.write_all(&usize_to_u32(self.freed_grains.len())?.to_be_bytes())?;

        for grain in &self.new_grains {
            bytes.write_all(&grain.id.to_bytes())?;
            bytes.write_all(&grain.crc32.to_be_bytes())?;
        }

        for grain in &self.archived_grains {
            bytes.write_all(&grain.to_bytes())?;
        }

        for grain in &self.freed_grains {
            bytes.write_all(&grain.to_bytes())?;
        }

        Ok(())
    }

    pub fn read_from<R: Read>(mut reader: R) -> Result<Self> {
        let mut eight_bytes = [0; 8];
        reader.read_exact(&mut eight_bytes)?;
        let transaction_id = TransactionId::from_be_bytes(eight_bytes);
        reader.read_exact(&mut eight_bytes)?;
        let next_entry = GrainId::from_bytes(&eight_bytes);
        reader.read_exact(&mut eight_bytes)?;
        let embedded_header_data = GrainId::from_bytes(&eight_bytes);
        reader.read_exact(&mut eight_bytes)?;
        let checkpoint_target = TransactionId::from_be_bytes(eight_bytes);
        reader.read_exact(&mut eight_bytes)?;
        let checkpointed_to = TransactionId::from_be_bytes(eight_bytes);

        let mut four_bytes = [0; 4];
        reader.read_exact(&mut four_bytes)?;
        let new_grain_count = u32::from_be_bytes(four_bytes);
        reader.read_exact(&mut four_bytes)?;
        let archived_grain_count = u32::from_be_bytes(four_bytes);
        reader.read_exact(&mut four_bytes)?;
        let freed_grain_count = u32::from_be_bytes(four_bytes);

        let mut new_grains = Vec::with_capacity(u32_to_usize(new_grain_count)?);
        for _ in 0..new_grain_count {
            reader.read_exact(&mut eight_bytes)?;
            let id = GrainId::from_bytes(&eight_bytes).unwrap(); // TODO error
            reader.read_exact(&mut four_bytes)?;
            let crc32 = u32::from_be_bytes(four_bytes);
            new_grains.push(NewGrain { id, crc32 });
        }

        let mut archived_grains = Vec::with_capacity(u32_to_usize(archived_grain_count)?);
        for _ in 0..archived_grain_count {
            reader.read_exact(&mut eight_bytes)?;
            let id = GrainId::from_bytes(&eight_bytes).unwrap(); // TODO error
            archived_grains.push(id);
        }

        let mut freed_grains = Vec::with_capacity(u32_to_usize(freed_grain_count)?);
        for _ in 0..freed_grain_count {
            reader.read_exact(&mut eight_bytes)?;
            let id = GrainId::from_bytes(&eight_bytes).unwrap(); // TODO error
            freed_grains.push(id);
        }

        Ok(Self {
            transaction_id,
            next_entry,
            new_grains,
            archived_grains,
            freed_grains,
            embedded_header_data,
            checkpoint_target,
            checkpointed_to,
        })
    }

    pub fn next_entry(&self, database: &Database) -> Result<Option<Stored<Arc<Self>>>> {
        if self.transaction_id > database.checkpointed_to()? {
            if let Some(entry_id) = self.next_entry {
                if let Some(entry) = database.read_commit_log_entry(entry_id)? {
                    return Ok(Some(Stored {
                        grain_id: entry_id,
                        stored: entry,
                    }));
                }
            }
        }

        Ok(None)
    }
}

#[derive(Debug)]
pub struct NewGrain {
    pub id: GrainId,
    pub crc32: u32,
}

impl NewGrain {
    const BYTES: usize = 12;
}

#[derive(Debug, Default)]
pub struct CommitLogs {
    // TODO this should be an LRU
    cached: Mutex<HashMap<GrainId, CommitLogCacheEntry>>,
    sync: Condvar,
}

impl CommitLogs {
    pub fn cache(&self, grain_id: GrainId, entry: Arc<CommitLogEntry>) -> Result<()> {
        let mut data = self.cached.lock()?;
        data.insert(grain_id, CommitLogCacheEntry::Cached(Some(entry)));
        Ok(())
    }

    pub fn get_or_lookup(
        &self,
        grain_id: GrainId,
        db: &Database,
    ) -> Result<Option<Arc<CommitLogEntry>>> {
        let mut data = self.cached.lock()?;
        loop {
            match data.entry(grain_id) {
                hash_map::Entry::Occupied(entry) => match entry.get() {
                    CommitLogCacheEntry::Cached(cached) => return Ok(cached.clone()),
                    CommitLogCacheEntry::Caching => {
                        // Another thread is trying to cache this entry already.
                        data = self.sync.wait(data)?;
                    }
                },
                hash_map::Entry::Vacant(miss) => {
                    miss.insert(CommitLogCacheEntry::Caching);
                    drop(data);

                    // We want to be careful to not cause another thread to
                    // block indefinitely if we receive an error.
                    let result = match Self::read_entry(grain_id, db) {
                        Ok(entry) => {
                            let entry = entry.map(Arc::new);
                            data = self.cached.lock()?;
                            data.insert(grain_id, CommitLogCacheEntry::Cached(entry.clone()));
                            Ok(entry)
                        }
                        Err(err) => {
                            // We had an error reading, clear our entry in the
                            // cache before returning it.
                            data = self.cached.lock()?;
                            data.remove(&grain_id);
                            Err(err)
                        }
                    };

                    drop(data);

                    // This is wasteful to wake up all waiting threads, but we
                    // don't have a good way to notify just a single one.
                    self.sync.notify_all();

                    return result;
                }
            }
        }
    }

    fn read_entry(grain_id: GrainId, db: &Database) -> Result<Option<CommitLogEntry>> {
        if let Some(reader) = db.read(grain_id)? {
            let data = reader.read_all_data()?;
            let entry = CommitLogEntry::read_from(&data[..])?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
enum CommitLogCacheEntry {
    Cached(Option<Arc<CommitLogEntry>>),
    Caching,
}
