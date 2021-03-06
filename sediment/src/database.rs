use std::{
    borrow::Cow,
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
use rebytes::Allocator;

use crate::{
    database::{
        allocations::{FileAllocationStatistics, FileAllocations},
        atlas::Atlas,
        checkpoint_guard::CheckpointProtector,
        committer::{Committer, GrainBatchOperation},
        disk::DiskState,
        embedded::EmbeddedHeaderState,
        log::CommitLog,
        page_cache::PageCache,
    },
    format::{crc, BatchId, CommitLogEntry, GrainId, LogEntryIndex},
    io::{self, ext::ToIoResult, paths::PathId, File},
};

mod allocations;
mod atlas;
mod checkpoint_guard;
mod committer;
mod config;
mod disk;
mod embedded;
mod log;
mod page_cache;
mod session;

pub use self::{
    checkpoint_guard::CheckpointGuard,
    committer::PendingCommit,
    config::{Configuration, RecoveryCallback},
    embedded::GrainMutexGuard,
    log::CommitLogSnapshot,
    session::{HeaderUpdateSession, WriteSession},
};

#[derive(Debug)]
pub struct Database<Manager>
where
    Manager: io::FileManager,
{
    state: Arc<DatabaseState<Manager>>,
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

        let grain_map_page_cache = PageCache::default();

        let buffer_allocator = Allocator::build()
            .minimum_allocation_size(4096)
            .finish()
            .unwrap();

        let state = DiskState::recover(&mut file, &buffer_allocator, &grain_map_page_cache)?;
        drop(file);

        let atlas = Atlas::from_state(&state.disk_state);
        let embedded_header = state
            .log
            .most_recent_entry()
            .and_then(|(_, index)| index.embedded_header);
        let mut database = Self {
            state: Arc::new(DatabaseState {
                path,
                file_manager: manager,
                current_batch: AtomicU64::new(state.disk_state.header.batch.0),
                checkpoint: AtomicU64::new(state.disk_state.header.checkpoint.0),
                checkpoint_protector: CheckpointProtector::default(),
                atlas,
                log: RwLock::new(state.log),
                disk_state: Mutex::new(state.disk_state),
                embedded_header: EmbeddedHeaderState::new(embedded_header),
                committer: Committer::default(),
                grain_map_page_cache,
                file_allocations: state.file_allocations,
                buffer_allocator,
            }),
        };

        if let Some(error) = state.recovered_from_error {
            recovered_callback.recovered(&mut database, error)?;
        }

        Ok(database)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.state.path.path
    }

    #[must_use]
    pub fn path_id(&self) -> &PathId {
        &self.state.path
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

    #[must_use]
    pub fn embedded_header(&self) -> Option<GrainId> {
        self.state.embedded_header.current()
    }

    #[must_use]
    pub fn current_batch(&self) -> BatchId {
        // TODO does this need SeqCst?
        BatchId(self.state.current_batch.load(Ordering::SeqCst))
    }

    #[must_use]
    pub fn checkpoint(&self) -> BatchId {
        // TODO does this need SeqCst?
        BatchId(self.state.checkpoint.load(Ordering::SeqCst))
    }

    #[must_use]
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
    #[must_use]
    pub fn new_session(&self) -> WriteSession<Manager> {
        WriteSession::new(self.clone())
    }

    pub fn get(&mut self, grain: GrainId) -> io::Result<Option<GrainData>> {
        let mut file = self.state.file_manager.read(&self.state.path)?;
        if let Some(reservation) = self.state.committer.pending_grain_reservation(grain) {
            // This grain is pending being written.
            // TODO if this was an async write, we need to wait.
            let data = vec![0; usize::try_from(reservation.length + 8).to_io()?];
            let (result, data) = file.read_exact(data, reservation.offset);
            result?;
            return Ok(Some(GrainData::from_bytes(data)));
        }

        let mut atlas = self.state.atlas.lock();
        let info = match atlas.grain_allocation_info(
            grain,
            self.current_batch(),
            &self.state.grain_map_page_cache,
            &mut file,
            &self.state.buffer_allocator,
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

    pub fn push(&self, data: &[u8]) -> io::Result<GrainRecord> {
        let mut session = self.new_session();
        let id = session.push(data)?;
        let batch = session.commit()?;
        Ok(GrainRecord { id, batch })
    }

    #[must_use]
    pub fn commit_log(&self) -> CommitLogSnapshot {
        let log = self.state.log.read();
        log.snapshot(self)
    }

    pub fn read_commit_log_entry(&mut self, entry: &LogEntryIndex) -> io::Result<Vec<u8>> {
        let mut file = self.state.file_manager.read(&self.state.path)?;
        let buffer = vec![0; usize::try_from(entry.length).to_io()?];
        let (result, buffer) = file.read_exact(buffer, entry.position);
        result?;
        Ok(buffer)
    }

    pub fn get_commit_log_entry(&mut self, entry: &LogEntryIndex) -> io::Result<CommitLogEntry> {
        let mut file = self.state.file_manager.read(&self.state.path)?;
        CommitLogEntry::load_from(entry, true, &mut file, &self.state.buffer_allocator)
    }

    pub fn checkpoint_to(&self, last_entry_to_remove: BatchId) -> io::Result<CheckpointGuard> {
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
        let mut file = self.state.file_manager.write(&self.state.path)?;
        self.state.atlas.reserve_grain(
            length,
            &self.state.grain_map_page_cache,
            &mut file,
            &self.state.file_allocations,
        )
    }

    fn archive(&mut self, grain_id: GrainId) -> io::Result<u8> {
        if let Some(pending_reservation) = self.state.committer.pending_grain_reservation(grain_id)
        {
            return Ok(dbg!(pending_reservation.grain_count));
        }

        let mut active_state = self.state.atlas.lock();

        let mut file = self.state.file_manager.read(&self.state.path)?;
        let info = active_state.grain_allocation_info(
            grain_id,
            self.current_batch(),
            &self.state.grain_map_page_cache,
            &mut file,
            &self.state.buffer_allocator,
        )?;

        if let Some(info) = info {
            Ok(dbg!(info.count))
        } else {
            Err(std::io::Error::from(ErrorKind::NotFound))
        }
    }

    /// Persists all of the writes to the database. When this function returns,
    /// the data is fully flushed to disk.
    fn commit_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainBatchOperation>,
        async_writes: impl Iterator<Item = Manager::AsyncFile>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: Option<GrainMutexGuard>,
    ) -> io::Result<CheckpointGuard> {
        self.state.committer.commit(
            reservations,
            async_writes,
            checkpoint_to,
            new_embedded_header,
            &self.state,
        )
    }

    fn enqueue_commit_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainBatchOperation>,
        async_writes: impl Iterator<Item = Manager::AsyncFile>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: Option<GrainMutexGuard>,
    ) -> PendingCommit<Manager> {
        self.state.committer.enqueue_commit(
            reservations,
            async_writes,
            checkpoint_to,
            new_embedded_header,
            self.state.clone(),
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
    Manager: io::FileManager,
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GrainReservation {
    pub grain_id: GrainId,
    pub offset: u64,
    pub length: u32,
    crc: Option<u32>,
    grain_count: u8,
}

#[derive(Debug)]
struct DatabaseState<Manager>
where
    Manager: io::FileManager,
{
    path: PathId,
    file_manager: Manager,
    current_batch: AtomicU64,
    checkpoint: AtomicU64,
    checkpoint_protector: CheckpointProtector,
    atlas: Atlas,
    disk_state: Mutex<DiskState>,
    embedded_header: EmbeddedHeaderState,
    log: RwLock<CommitLog>,
    committer: Committer<Manager::AsyncFile>,
    grain_map_page_cache: PageCache,
    file_allocations: FileAllocations,
    buffer_allocator: Allocator,
}

#[derive(Debug, Eq)]
pub struct GrainData<'a> {
    pub crc: u32,
    length: usize,
    data: Cow<'a, [u8]>,
}

impl<'a> PartialEq for GrainData<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.crc == other.crc && self.as_bytes() == other.as_bytes()
    }
}

impl<'a> Deref for GrainData<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data[Self::HEADER_BYTES..Self::HEADER_BYTES + self.length]
    }
}

impl<'a> AsRef<[u8]> for GrainData<'a> {
    fn as_ref(&self) -> &[u8] {
        &*self
    }
}

impl GrainData<'static> {
    pub(crate) fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            crc: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            length: usize::try_from(u32::from_le_bytes(data[4..8].try_into().unwrap())).unwrap(),
            data: Cow::Owned(data),
        }
    }

    #[must_use]
    pub fn into_raw_bytes(self) -> Vec<u8> {
        self.data.into_owned()
    }
}

impl<'a> GrainData<'a> {
    pub const HEADER_BYTES: usize = 8;

    pub(crate) fn borrowed(data: &'a [u8]) -> Self {
        Self {
            crc: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            length: usize::try_from(u32::from_le_bytes(data[4..8].try_into().unwrap())).unwrap(),
            data: Cow::Borrowed(data),
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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct GrainRecord {
    pub id: GrainId,
    pub batch: CheckpointGuard,
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
    let grain_id = session.push(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap().batch_id();
    assert_eq!(db.current_batch(), committed_batch);
    println!("Batch: {committed_batch}");

    let grain_data = db.get(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let grain_data = db.get(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    // Add another grain
    let mut session = db.new_session();
    let second_grain_id = session.push(b"hello again").unwrap();
    println!("Wrote to {second_grain_id}");
    let committed_batch = session.commit().unwrap().batch_id();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let grain_data = db.get(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    let second_grain_data = db.get(second_grain_id).unwrap().unwrap();
    assert_eq!(&*second_grain_data, b"hello again");

    // Add a final grain, overwriting the original first header.
    let mut session = db.new_session();
    let third_grain_id = session.push(b"bye for now").unwrap();
    println!("Wrote to {third_grain_id}");
    let committed_batch = session.commit().unwrap().batch_id();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

    let grain_data = db.get(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    let second_grain_data = db.get(second_grain_id).unwrap().unwrap();
    assert_eq!(&*second_grain_data, b"hello again");

    let third_grain_data = db.get(third_grain_id).unwrap().unwrap();
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
    let first_grain_id = session.push(b"hello world").unwrap();
    drop(session);

    let mut session = db.new_session();
    let second_grain_id = session.push(b"hello world").unwrap();
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
    let first_grain_id = session.push(b"test").unwrap();
    session.commit().unwrap();
    // To create a new strata, we currently need something at least 4x as large
    // as the current strata size.
    let mut session = db.new_session();
    let second_grain_id = session.push(&vec![42; 32768]).unwrap();
    session.commit().unwrap();
    assert_ne!(
        first_grain_id.stratum_index(),
        second_grain_id.stratum_index()
    );

    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

    let first_grain_data = db.get(first_grain_id).unwrap().unwrap();
    assert_eq!(&*first_grain_data, b"test");

    let second_grain_data = db.get(second_grain_id).unwrap().unwrap();
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
            grains.push(db.push(&value.to_be_bytes()).unwrap());
        }
        drop(db);

        let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

        // Verify all written data exists
        for (index, grain) in grains.into_iter().enumerate() {
            let data = db.get(grain.id).unwrap().expect("grain not found");
            let value = u32::from_be_bytes(data.as_bytes().try_into().unwrap());
            assert_eq!(value, u32::try_from(index).unwrap());
        }

        // Verify writing again still succeeds.
        db.push(b"test").unwrap();

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
        grains.push(db.push(b"test").unwrap());
        drop(db);
    }

    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    let snapshot_before_write = db.commit_log();
    assert_eq!(snapshot_before_write.len(), 170);
    grains.push(db.push(b"test").unwrap());
    let snapshot_after_write = db.commit_log();
    assert_eq!(snapshot_after_write.len(), 171);

    for ((_batch, entry_index), grain) in snapshot_after_write.iter().zip(&grains) {
        let entry = db.get_commit_log_entry(&entry_index).unwrap();
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
    let grain_id = session.push(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap().batch_id();
    println!("Batch: {committed_batch}");

    // Close the database
    drop(db);

    // Break the file header for the new commit by overwriting a byte in the
    // first header.
    let mut file = manager.write(&manager.resolve_path(&path)).unwrap();
    let buffer = vec![0xFE_u8; 1];
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

    assert!(db.get(grain_id).unwrap().is_none());

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(isolation, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    let mut session = db.new_session();
    let grain_id = session.push(b"hello world").unwrap();
    println!("Wrote to {grain_id}");

    assert_eq!(db.get(grain_id).unwrap(), None);
    assert_eq!(&*session.get(grain_id).unwrap().unwrap(), b"hello world");

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
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
    let grain_id = session.push(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap().batch_id();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    // Archive the grain
    let mut session = db.new_session();
    session.archive(grain_id).unwrap();
    let archiving_commit = session.commit().unwrap().batch_id();

    println!("Archived at: {archiving_commit}");

    // We should still be able to read the data.
    let grain_data = db.get(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let grain_data = db.get(grain_id).unwrap().unwrap();
    assert_eq!(&*grain_data, b"hello world");
    db.checkpoint_to(archiving_commit).unwrap();

    // The data should now no longer be available.
    assert_eq!(db.get(grain_id).unwrap(), None);

    // Reopen the database and verify it's still gone.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    assert_eq!(db.get(grain_id).unwrap(), None);

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
    let grain_id = session.push(b"a header").unwrap();
    session.set_embedded_header(Some(grain_id)).unwrap();
    println!("Set header to {grain_id}");
    session.commit().unwrap();

    assert_eq!(db.embedded_header(), Some(grain_id));

    // Reopen the database.
    drop(db);
    let db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    assert_eq!(db.embedded_header(), Some(grain_id));
    let new_grain = db.push(b"another").unwrap();

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
    let new_header_commit = session.commit().unwrap().batch_id();

    // Ensure the new write didn't overwrite the header.
    assert_eq!(db.embedded_header(), Some(new_grain.id));

    // Reopen the database and ensure the embedded header is still present.
    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();
    assert_eq!(db.embedded_header(), Some(new_grain.id));

    // Checkpoint the new header change, and verify the old grain is no longer readable.
    db.checkpoint_to(new_header_commit).unwrap();

    assert_eq!(db.get(grain_id).unwrap(), None);

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

#[cfg(test)]
crate::io_test!(multithreaded, {
    const OPS: usize = 100;
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let manager = Manager::default();
    // Create the database.
    let mut db = Database::<Manager>::open_with_manager(&path, manager.clone()).unwrap();

    let (result_sender, result_receiver) = flume::unbounded();
    let mut threads = Vec::new();
    let starter = Arc::new(RwLock::new(()));
    let start_guard = starter.write();
    let number_of_threads = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        * 2;
    for _ in 0..number_of_threads {
        let result_sender = result_sender.clone();
        let db = db.clone();
        let starter = starter.clone();
        threads.push(std::thread::spawn(move || {
            drop(starter.read());

            for i in 0..OPS {
                let value = i.to_le_bytes().to_vec();
                let grain = db.push(&value).unwrap();
                result_sender.send((grain, value)).unwrap();
            }
        }));
    }

    // Start the threads.
    drop(start_guard);
    drop(result_sender);

    for thread in threads {
        thread.join().unwrap();
    }

    // Verify the results before closing and then after reopening.
    let mut ops_counted = 0;
    let mut results = Vec::new();
    while let Ok((grain, expected_value)) = result_receiver.recv() {
        let data = db.get(grain.id).unwrap().unwrap();
        assert_eq!(&*data, &expected_value);
        ops_counted += 1;
        results.push((grain, expected_value));
    }

    assert!(ops_counted == number_of_threads * OPS);

    drop(db);
    let mut db = Database::<Manager>::open_with_manager(&path, manager).unwrap();

    for (grain, expected_value) in results {
        let data = db.get(grain.id).unwrap().unwrap();
        assert_eq!(&*data, &expected_value);
    }

    drop(db);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
});
