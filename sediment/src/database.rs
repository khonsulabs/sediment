use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, RwLock};

use crate::{
    database::{
        allocations::FileAllocations, atlas::Atlas, committer::Committer, disk::DiskState,
        log::CommitLog, page_cache::PageCache,
    },
    format::{self, BatchId, GrainId, GrainInfo},
    io::{self, ext::ToIoResult, FileManager},
};

mod allocations;
mod atlas;
mod committer;
mod disk;
mod log;
mod page_cache;
mod session;

pub use self::session::WriteSession;

#[derive(Debug)]
pub struct Database<File> {
    file: File,
    scratch: Vec<u8>,
    state: Arc<DatabaseState>,
}

impl<File> Database<File>
where
    File: io::File,
{
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        File::Manager: Default,
    {
        Self::open_with_manager(path, &<File::Manager as Default>::default())
    }

    pub fn open_with_manager(path: impl AsRef<Path>, manager: &File::Manager) -> io::Result<Self> {
        let path_id = manager.resolve_path(path);
        let mut file = manager.write(&path_id)?;

        let mut scratch = Vec::new();

        let (disk_state, log, file_allocations) = DiskState::recover(&mut file, &mut scratch)?;
        let atlas = Atlas::from_state(&disk_state)?;

        Ok(Self {
            file,
            scratch,
            state: Arc::new(DatabaseState {
                current_batch: AtomicU64::new(disk_state.header.batch.0),
                atlas: Mutex::new(atlas),
                log: RwLock::new(log),
                disk_state: Mutex::new(disk_state),
                committer: Committer::default(),
                grain_map_page_cache: PageCache::default(),
                file_allocations,
            }),
        })
    }

    pub fn current_batch(&self) -> BatchId {
        // TODO does this need SeqCst?
        BatchId(self.state.current_batch.load(Ordering::SeqCst))
    }

    pub fn statistics(&self) -> Statistics {
        let (unallocated_bytes, file_size) = self.state.file_allocations.statistics();
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
            file_size,
            unallocated_bytes,
            grains_by_length,
        }
    }

    /// Create a new session for writing data to this database.
    pub fn new_session(&mut self) -> WriteSession<'_, File> {
        WriteSession::new(self)
    }

    pub fn read(&mut self, grain: GrainId) -> io::Result<Option<GrainData>> {
        let mut atlas = self.state.atlas.lock();
        let (offset, info) = match atlas.info_of_grain(
            grain,
            &self.state.grain_map_page_cache,
            &mut self.file,
            &mut self.scratch,
        )? {
            Some((_, info)) if info.allocated_at.is_none() => return Ok(None),
            Some(result) => result,
            None => return Ok(None),
        };
        drop(atlas);

        let allocated_at = info.allocated_at.unwrap();
        if allocated_at > self.current_batch() {
            // The grain is allocated but hasn't been committed yet.
            return Ok(None);
        }

        let data = vec![0; usize::try_from(info.length).to_io()?];
        let (result, data) = self.file.read_exact(data, offset);
        result?;
        Ok(Some(GrainData { info, data }))
    }

    pub fn write(&mut self, data: &[u8]) -> io::Result<GrainId> {
        let mut session = self.new_session();
        let grain = dbg!(session.write(data))?;
        session.commit()?;
        Ok(grain)
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
    fn new_grain(&mut self, length: u32) -> io::Result<GrainReservation> {
        let mut active_state = self.state.atlas.lock();

        active_state.reserve_grain(length, &mut self.file, &self.state.file_allocations)
    }

    /// Persists all of the writes to the database. When this function returns,
    /// the data is fully flushed to disk.
    fn commit_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<BatchId> {
        self.state
            .committer
            .commit(reservations, &self.state, &mut self.file, &mut self.scratch)
    }

    fn forget_reservations(
        &self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<()> {
        let mut active_state = self.state.atlas.lock();

        active_state.forget_reservations(reservations)
    }
}

#[derive(Debug)]
pub struct GrainReservation {
    pub grain_id: GrainId,
    pub offset: u64,
    pub length: u32,
    pub crc: u32,
}

#[derive(Debug)]
struct DatabaseState {
    current_batch: AtomicU64,
    atlas: Mutex<Atlas>,
    disk_state: Mutex<DiskState>,
    log: RwLock<CommitLog>,
    committer: Committer,
    grain_map_page_cache: PageCache,
    file_allocations: FileAllocations,
}

#[derive(Debug)]
pub struct GrainData {
    pub info: GrainInfo,
    pub data: Vec<u8>,
}

impl GrainData {
    pub fn is_crc_valid(&self) -> bool {
        format::crc(&self.data) == self.info.crc
    }
}

#[derive(Debug)]
pub struct Statistics {
    pub file_size: u64,
    pub unallocated_bytes: u64,
    pub grains_by_length: HashMap<u32, GrainStatistics>,
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
    drop(Database::<Manager::File>::open_with_manager(&path, &manager).unwrap());
    // Test opening it again.
    drop(Database::<Manager::File>::open_with_manager(&path, &manager).unwrap());
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
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();
    let mut session = db.new_session();
    let grain_id = session.write(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(grain_data.data, b"hello world");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(grain_data.data, b"hello world");

    // Add another grain
    let mut session = db.new_session();
    let second_grain_id = session.write(b"hello again").unwrap();
    println!("Wrote to {second_grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(grain_data.data, b"hello world");

    let second_grain_data = db.read(second_grain_id).unwrap().unwrap();
    assert_eq!(second_grain_data.data, b"hello again");

    // Add a final grain, overwriting the original first header.
    let mut session = db.new_session();
    let third_grain_id = session.write(b"bye for now").unwrap();
    println!("Wrote to {third_grain_id}");
    let committed_batch = session.commit().unwrap();
    println!("Batch: {committed_batch}");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(grain_data.data, b"hello world");

    let second_grain_data = db.read(second_grain_id).unwrap().unwrap();
    assert_eq!(second_grain_data.data, b"hello again");

    let third_grain_data = db.read(third_grain_id).unwrap().unwrap();
    assert_eq!(third_grain_data.data, b"bye for now");

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
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();
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
    // Create the database.
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();
    let mut session = db.new_session();
    let first_grain_id = session.write(b"test").unwrap();
    session.commit().unwrap();
    let mut session = db.new_session();
    let second_grain_id = session.write(b"at least 17 bytes").unwrap();
    session.commit().unwrap();
    assert_ne!(
        first_grain_id.stratum_index(),
        second_grain_id.stratum_index()
    );

    drop(db);
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();

    let first_grain_data = db.read(first_grain_id).unwrap().unwrap();
    assert_eq!(first_grain_data.data, b"test");

    let second_grain_data = db.read(second_grain_id).unwrap().unwrap();
    assert_eq!(second_grain_data.data, b"at least 17 bytes");

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
        let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();
        let mut grain_ids = Vec::new();
        for value in 0..count {
            grain_ids.push(db.write(&value.to_be_bytes()).unwrap());
        }
        drop(db);

        let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();

        // Verify all written data exists
        for (index, grain_id) in grain_ids.into_iter().enumerate() {
            let data = db.read(grain_id).unwrap().expect("grain not found");
            let value = u32::from_be_bytes(data.data.try_into().unwrap());
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
