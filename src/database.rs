use std::{path::Path, sync::Arc};

use parking_lot::Mutex;

use crate::{
    database::{atlas::Atlas, committer::Committer, disk::DiskState, page_cache::PageCache},
    format::{self, BatchId, GrainId, GrainInfo},
    io::{self, ext::ToIoResult, FileManager},
};

mod atlas;
mod committer;
mod disk;
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

        let disk_state = DiskState::recover(&mut file, &mut scratch)?;
        let atlas = Atlas::from_state(&disk_state, &mut file)?;

        Ok(Self {
            file,
            scratch,
            state: Arc::new(DatabaseState {
                atlas: Mutex::new(atlas),
                committer: Committer::default(),
                disk_state: Mutex::new(disk_state),
                grain_map_page_cache: PageCache::default(),
            }),
        })
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

        let data = vec![0; usize::try_from(info.length).to_io()?];
        let (result, data) = self.file.read_exact(data, offset);
        result?;
        Ok(Some(GrainData { info, data }))
    }

    /// Reserve space within the database. This may allocate additional disk
    /// space.
    ///
    /// The returned reservation serves as an in-memory guard for the allocated
    /// region, preventing other writers from using this space.
    ///
    /// While the data being written may be synced during another session's
    /// commit, the Strata is not updated with the new information until the
    /// commit phase.
    fn new_grain(&mut self, length: u32) -> io::Result<GrainReservation> {
        let mut active_state = self.state.atlas.lock();

        active_state.reserve_grain(length, &mut self.file)
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
    atlas: Mutex<Atlas>,
    disk_state: Mutex<DiskState>,
    committer: Committer,
    grain_map_page_cache: PageCache,
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
    let committed_sequence = session.commit().unwrap();
    println!("Batch sequence: {committed_sequence}");

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(grain_data.data, b"hello world");

    // Reopen the database.
    drop(db);
    let mut db = Database::<Manager::File>::open_with_manager(&path, &manager).unwrap();

    let grain_data = db.read(grain_id).unwrap().unwrap();
    assert_eq!(grain_data.data, b"hello world");

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
