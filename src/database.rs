use std::{path::Path, sync::Arc};

use lrumap::LruHashMap;
use parking_lot::Mutex;

use crate::{
    database::{atlas::Atlas, committer::Committer, disk::DiskState},
    format::{BatchId, GrainId, GrainMapPage},
    io::{self, FileManager},
};

mod atlas;
mod committer;
mod disk;
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
                grain_map_pages: Mutex::new(LruHashMap::new(2048)),
            }),
        })
    }

    /// Create a new session for writing data to this database.
    pub fn new_session(&mut self) -> WriteSession<'_, File> {
        WriteSession::new(self)
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
    grain_map_pages: Mutex<LruHashMap<u64, GrainMapPage>>,
}

#[cfg(test)]
crate::io_test!(empty, {
    let path = unique_file_path::<Manager>();
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    // Create the database.
    drop(Database::<Manager::File>::open(&path).unwrap());
    // Test opening it again.
    drop(Database::<Manager::File>::open(&path).unwrap());
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
    // Create the database.
    let mut db = Database::<Manager::File>::open(&path).unwrap();
    let mut session = db.new_session();
    let grain_id = session.write(b"hello world").unwrap();
    println!("Wrote to {grain_id}");
    let committed_sequence = session.commit().unwrap();
    println!("Batch sequence: {committed_sequence}");
    return;
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
    // Create the database.
    let mut db = Database::<Manager::File>::open(&path).unwrap();
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
