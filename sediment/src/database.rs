use std::{
    collections::HashMap,
    io::ErrorKind,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, RwLock};

use crate::{
    database::{
        allocations::{FileAllocationStatistics, FileAllocations},
        atlas::Atlas,
        committer::{Committer, GrainBatchOperation},
        disk::DiskState,
        embedded::{EmbeddedHeaderGuard, EmbeddedHeaderState},
        log::CommitLog,
        page_cache::PageCache,
    },
    format::{crc, BatchId, CommitLogEntry, GrainId, LogEntryIndex},
    io::{self, ext::ToIoResult, paths::PathId, File},
};

mod allocations;
mod atlas;
mod committer;
mod config;
mod disk;
mod embedded;
mod log;
mod page_cache;
mod session;

pub use self::{
    config::{Configuration, RecoveryCallback},
    log::CommitLogSnapshot,
    session::WriteSession,
};

#[derive(Debug)]
pub struct Database<Manager> {
    path: PathId,
    manager: Manager,
    scratch: Vec<u8>,
    state: Arc<DatabaseState>,
}

impl<Manager> Database<Manager>
where
    Manager: io::FileManager,
{
    fn new<Recovered>(
        path: impl AsRef<Path>,
        manager: Manager,
        recovered_callback: Recovered,
    ) -> io::Result<Self>
    where
        Recovered: RecoveryCallback<Manager>,
    {
        let path = manager.resolve_path(path);
        let mut file = manager.write(&path)?;

        let mut scratch = Vec::new();
        let grain_map_page_cache = PageCache::default();

        let state = DiskState::recover(&mut file, &mut scratch, &grain_map_page_cache)?;
        let atlas = Atlas::from_state(&state.disk_state);
        let embedded_header = state
            .log
            .most_recent_entry()
            .and_then(|(_, index)| index.embedded_header);
        let mut database = Self {
            path,
            manager,
            scratch,
            state: Arc::new(DatabaseState {
                current_batch: AtomicU64::new(state.disk_state.header.batch.0),
                checkpoint: AtomicU64::new(state.disk_state.header.checkpoint.0),
                atlas: Mutex::new(atlas),
                log: RwLock::new(state.log),
                disk_state: Mutex::new(state.disk_state),
                embedded_header: EmbeddedHeaderState::new(embedded_header),
                committer: Committer::default(),
                grain_map_page_cache,
                file_allocations: state.file_allocations,
            }),
        };

        if let Some(error) = state.recovered_from_error {
            recovered_callback.recovered(&mut database, error)?;
        }

        Ok(database)
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Manager: Default,
    {
        Self::open_with_manager(path, Manager::default())
    }

    pub fn open_with_manager(path: impl AsRef<Path>, manager: Manager) -> io::Result<Self> {
        Self::new(path, manager, ())
    }

    pub fn embedded_header(&self) -> Option<GrainId> {
        self.state.embedded_header.current()
    }

    pub fn current_batch(&self) -> BatchId {
        // TODO does this need SeqCst?
        BatchId(self.state.current_batch.load(Ordering::SeqCst))
    }

    pub fn checkpoint(&self) -> BatchId {
        // TODO does this need SeqCst?
        BatchId(self.state.checkpoint.load(Ordering::SeqCst))
    }

    pub fn statistics(&self) -> Statistics {
        let allocations = self.state.file_allocations.statistics();
        let disk_state = self.state.disk_state.lock();
        let mut grains_by_length = HashMap::new();
        for basin in &disk_state.basins {
            for (stratum, stratum_index) in basin.strata.iter().zip(basin.header.strata.iter()) {
                for grain_map in &stratum.grain_maps {
                    let grain_stats = grains_by_length
                        .entry(stratum_index.grain_length())
                        .or_insert_with(GrainStatistics::default);

                    for allocated in &grain_map.map.allocation_state {
                        if *allocated {
                            grain_stats.allocated += 1;
                        } else {
                            grain_stats.free += 1;
                        }
                    }
                }
            }
        }

        Statistics {
            allocations,
            grains_by_length,
        }
    }

    /// Create a new session for writing data to this database.
    pub fn new_session(&self) -> WriteSession<Manager> {
        WriteSession::new(self.clone())
    }

    pub fn read(&mut self, grain: GrainId) -> io::Result<Option<GrainData>> {
        let mut atlas = self.state.atlas.lock();
        let mut file = self.manager.read(&self.path)?;
        let info = match atlas.grain_allocation_info(
            grain,
            self.current_batch(),
            &self.state.grain_map_page_cache,
            &mut file,
            &mut self.scratch,
        )? {
            Some(info) if info.allocated_length == 0 => return Ok(None),
            Some(result) => result,
            None => return Ok(None),
        };
        drop(atlas);

        let data = vec![0; usize::try_from(info.allocated_length).to_io()?];
        let (result, data) = file.read_exact(data, info.offset);
        result?;
        Ok(Some(GrainData::from_bytes(data)))
    }

    pub fn write(&self, data: &[u8]) -> io::Result<GrainRecord> {
        let mut session = self.new_session();
        let id = session.write(data)?;
        let batch = session.commit()?;
        Ok(GrainRecord { id, batch })
    }

    pub fn commit_log(&self) -> CommitLogSnapshot {
        let log = self.state.log.read();
        log.snapshot(self)
    }

    pub fn read_commit_log_entry(&mut self, entry: &LogEntryIndex) -> io::Result<CommitLogEntry> {
        let mut file = self.manager.read(&self.path)?;
        CommitLogEntry::load_from(entry, true, &mut file, &mut self.scratch)
    }

    pub fn checkpoint_to(&self, last_entry_to_remove: BatchId) -> io::Result<BatchId> {
        self.new_session()
            .commit_and_checkpoint(last_entry_to_remove)
    }

    /// Reserve space within the database. This may allocate additional disk
    /// space.
    ///
    /// The returned reservation serves as an in-memory guard for the allocated
    /// region, preventing other writers from using this space.
    ///
    /// While the data being written may be synced during another session's
    /// commit, the Stratum is not updated with the new information until the
    /// commit phase.
    fn new_grain(&self, length: u32) -> io::Result<GrainReservation> {
        let mut active_state = self.state.atlas.lock();

        let mut file = self.manager.write(&self.path)?;
        active_state.reserve_grain(length, &mut file, &self.state.file_allocations)
    }

    fn archive(&mut self, grain_id: GrainId) -> io::Result<u8> {
        let mut active_state = self.state.atlas.lock();

        let mut file = self.manager.read(&self.path)?;
        let info = active_state.grain_allocation_info(
            grain_id,
            self.current_batch(),
            &self.state.grain_map_page_cache,
            &mut file,
            &mut self.scratch,
        )?;

        if let Some(info) = info {
            Ok(info.count)
        } else {
            Err(std::io::Error::from(ErrorKind::NotFound))
        }
    }

    /// Persists all of the writes to the database. When this function returns,
    /// the data is fully flushed to disk.
    fn commit_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainBatchOperation>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: Option<EmbeddedHeaderGuard>,
    ) -> io::Result<BatchId> {
        let mut file = self.manager.write(&self.path)?;
        self.state.committer.commit(
            reservations,
            checkpoint_to,
            new_embedded_header,
            &self.state,
            &mut file,
            &mut self.scratch,
        )
    }

    fn forget_reservations(
        &self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<()> {
        let mut active_state = self.state.atlas.lock();

        active_state.forget_reservations(reservations)
    }
}

impl<Manager> Clone for Database<Manager>
where
    Manager: Clone,
{
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            manager: self.manager.clone(),
            scratch: Vec::new(),
            state: self.state.clone(),
        }
    }
}

#[derive(Debug)]
pub struct GrainReservation {
    pub grain_id: GrainId,
    pub offset: u64,
    pub length: u32,
    crc: Option<u32>,
}

#[derive(Debug)]
struct DatabaseState {
    current_batch: AtomicU64,
    checkpoint: AtomicU64,
    atlas: Mutex<Atlas>,
    disk_state: Mutex<DiskState>,
    embedded_header: EmbeddedHeaderState,
    log: RwLock<CommitLog>,
    committer: Committer,
    grain_map_page_cache: PageCache,
    file_allocations: FileAllocations,
}

#[derive(Debug, Eq)]
pub struct GrainData {
    pub crc: u32,
    length: usize,
    data: Vec<u8>,
}

impl PartialEq for GrainData {
    fn eq(&self, other: &Self) -> bool {
        self.crc == other.crc && self.as_bytes() == other.as_bytes()
    }
}

impl Deref for GrainData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data[8..8 + self.length]
    }
}

impl AsRef<[u8]> for GrainData {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}

impl GrainData {
    pub(crate) fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            crc: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            length: usize::try_from(u32::from_le_bytes(data[4..8].try_into().unwrap())).unwrap(),
            data,
        }
    }

    #[must_use]
    pub fn calculated_crc(&self) -> u32 {
        crc(&self.data[4..8 + self.length])
    }

    #[must_use]
    pub fn is_crc_valid(&self) -> bool {
        self.calculated_crc() == self.crc
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &*self
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct GrainRecord {
    pub id: GrainId,
    pub batch: BatchId,
}

#[derive(Debug)]
pub struct Statistics {
    pub allocations: FileAllocationStatistics,
    pub grains_by_length: HashMap<u32, GrainStatistics>,
}

impl Statistics {
    #[must_use]
    pub const fn file_length(&self) -> u64 {
        self.allocations.allocated_space + self.allocations.free_space
    }
}

#[derive(Default, Debug)]
pub struct GrainStatistics {
    pub free: u64,
    pub allocated: u64,
}

#[cfg(test)]
crate::io_test!(empty, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    drop(Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap());
    // Test opening it again.
    drop(Database::<Manager>::open_with_manager(&path, manager).unwrap());
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(basic_op, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
    let mut session = db.new_session();
    let grain_id = session.write(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    // Add another grain
    let mut session = db.new_session();
    let second_grain_id = session.write(b"hello again").unwrap();
    println!("Wrote to {second_grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    let second_grain_data = db.read(second_grain_id).unwrap().unwrap();
    assert_eq!(&*second_grain_data, b"hello again");

    // Add a final grain, overwriting the original first header.
    let mut session = db.new_session();
    let third_grain_id = session.write(b"bye for now").unwrap();
    println!("Wrote to {third_grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    let second_grain_data = db.read(second_grain_id).unwrap().unwrap();
    assert_eq!(&*second_grain_data, b"hello again");

    let third_grain_data = db.read(third_grain_id).unwrap().unwrap();
    assert_eq!(&*third_grain_data, b"bye for now");

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(basic_abort_reuse, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    let mut session = db.new_session();
    let first_grain_id = session.write(b"hello world").unwrap();
    drop(session);

    let mut session = db.new_session();
    let second_grain_id = session.write(b"hello world").unwrap();
    drop(session);

    assert_eq!(first_grain_id, second_grain_id);

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(multiple_strata, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    // This test makes assumptions about how the grain allocation strategy
    // works. Each grain currently has 8 additional bytes added to the data that
    // is written. This means the actual length of "test" becomes 12 bytes,
    // which means it will allocate a 16-byte strata.
    let mut session = db.new_session();
    let first_grain_id = session.write(b"test").unwrap();
    session.commit().unwrap();
    // To create a new strata, we currently need something at least 4x as large
    // as the current strata size.
    let mut session = db.new_session();
    let second_grain_id = session.write(&vec![42; 32768]).unwrap();
    session.commit().unwrap();
    assert_ne!(
        first_grain_id.stratum_index(),
        second_grain_id.stratum_index()
    );

    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

    let first_grain_data = db.read(first_grain_id).unwrap().unwrap();
    assert_eq!(&*first_grain_data, b"test");

    let second_grain_data = db.read(second_grain_id).unwrap().unwrap();
    assert_eq!(&*second_grain_data, &vec![42; 32768]);

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

// This test is aimed at testing the in-memory state tracking by issuing
// multiple commits between opening and closing the file. Many other tests use
// limited tests which close and reopen the database with only one transaction
// per session. This test supplements those tests by testing larger numbers of
// writes.
#[cfg(test)]
crate::io_test!(multi_commits, {
    let path = unique_file_path::<Manager>();
    for count in [2_u32, 3, 170, 171] {
        println!("Testing {count} commits");
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
        let manager = Manager::default();
        let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
        let mut grains = Vec::new();
        for value in 0..count {
            grains.push(db.write(&value.to_be_bytes()).unwrap());
        }
        drop(db);

        let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

        // Verify all written data exists
        for (index, grain) in grains.into_iter().enumerate() {
            let data = db.read(grain.id).unwrap().expect("grain not found");
            let value = u32::from_be_bytes(data.as_bytes().try_into().unwrap());
            assert_eq!(value, u32::try_from(index).unwrap());
        }

        // Verify writing again still succeeds.
        db.write(b"test").unwrap();

        drop(db);
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
    }
});

#[cfg(test)]
crate::io_test!(commit_log, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    // Each commit log page can hold 170 entries. Insert 170 (filling the page)
    // and then try to reload the database and insert again.
    let mut grains = Vec::new();
    for _ in 0..170 {
        let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
        grains.push(db.write(b"test").unwrap());
        drop(db);
    }

    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    let snapshot_before_write = db.commit_log();
    assert_eq!(snapshot_before_write.len(), 170);
    grains.push(db.write(b"test").unwrap());
    let snapshot_after_write = db.commit_log();
    assert_eq!(snapshot_after_write.len(), 171);

    for ((_batch, entry_index), grain) in snapshot_after_write.iter().zip(&grains) {
        let entry = db.read_commit_log_entry(&entry_index).unwrap();
        assert_eq!(entry.grain_changes.len(), 1);
        assert_eq!(entry.grain_changes[0].start, grain.id);
        assert_eq!(entry.grain_changes[0].count, 12);
        assert!(matches!(
            entry.grain_changes[0].operation,
            crate::format::GrainOperation::Allocate { .. }
        ));
    }

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(basic_rollback, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
    let mut session = db.new_session();
    let grain_id = session.write(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    // Close the database
    drop(db);

    // Break the file header for the new commit by overwriting a byte in the
    // first header.
    let mut file = manager.write(&manager.resolve_path(&path)).unwrap();
    let buffer = vec![0xFE; 1];
    let (result, _buffer) = file.write_all(buffer, 10);
    result.unwrap();

    let mut was_recovered = false;
    let mut db = Configuration::new(manager, &path)
        .when_recovered(|_db: &mut Database<Manager>, _err| {
            was_recovered = true;
            Ok(())
        })
        .open()
        .unwrap();
    assert!(was_recovered);
    assert_ne!(db.current_batch(), committed_batch);

    assert!(db.read(grain_id).unwrap().is_none());
});

#[cfg(test)]
crate::io_test!(grain_lifecycle, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
    let mut session = db.new_session();
    let grain_id = session.write(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    // Archive the grain
    let mut session = db.new_session();
    session.archive(grain_id).unwrap();
    let archiving_commit = session.commit().unwrap();

    println!("Archived at: {archiving_commit}");

    // We should still be able to read the data.
    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");
    db.checkpoint_to(archiving_commit).unwrap();

    // The data should now no longer be available.
    assert_eq!(db.read(grain_id).unwrap(), None);

    // Reopen the database and verify it's still gone.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    assert_eq!(db.read(grain_id).unwrap(), None);

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(embedded_header, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
    let mut session = db.new_session().updating_embedded_header();
    let grain_id = session.write(b"a header").unwrap();
    session.set_embedded_header(Some(grain_id)).unwrap();
    println!("Set header to {grain_id}");
    session.commit().unwrap();

    assert_eq!(db.embedded_header(), Some(grain_id));

    // Reopen the database.
    drop(db);
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    assert_eq!(db.embedded_header(), Some(grain_id));
    let new_grain = db.write(b"another").unwrap();

    // Ensure the new write didn't overwrite the header.
    assert_eq!(db.embedded_header(), Some(grain_id));

    // Reopen the database and ensure the embedded header is still present.
    drop(db);
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();
    assert_eq!(db.embedded_header(), Some(grain_id));

    // Update the header one more time
    let mut session = db.new_session().updating_embedded_header();
    session.set_embedded_header(Some(new_grain.id)).unwrap();
    println!("Updated header to {}", new_grain.id);
    let new_header_commit = session.commit().unwrap();

    // Ensure the new write didn't overwrite the header.
    assert_eq!(db.embedded_header(), Some(new_grain.id));

    // Reopen the database and ensure the embedded header is still present.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    assert_eq!(db.embedded_header(), Some(new_grain.id));

    // Checkpoint the new header change, and verify the old grain is no longer readable.
    db.checkpoint_to(new_header_commit).unwrap();

    assert_eq!(db.read(grain_id).unwrap(), None);

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});
