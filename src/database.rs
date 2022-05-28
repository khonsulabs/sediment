use std::{collections::HashMap, path::Path, sync::Arc};

use parking_lot::Mutex;

use crate::{
    database::atlas::Atlas,
    format::{FileHeader, GrainId, SequenceId},
    io::{self, FileManager},
};

mod atlas;
mod session;

pub use self::session::WriteSession;

#[derive(Debug)]
pub struct Database<File> {
    file: File,
    scratch: Vec<u8>,
    active_state: Arc<Mutex<DatabaseState>>,
}

#[derive(Debug)]
struct DatabaseState {
    header: FileHeader,
    atlas: Atlas,
    /// Grain reservations that are outstanding. The value is the length of the
    /// reservation.
    reserved: HashMap<GrainId, u64>,
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
        let header = if file.is_empty()? {
            // Initialize the an empty database.
            let mut header = FileHeader::default();
            header.write_to(&mut file, true, &mut scratch)?;
            file.synchronize()?;
            header
        } else {
            FileHeader::read_from(&mut file, &mut scratch)?
        };

        let atlas = Atlas::load_from(&mut file, header.current())?;

        Ok(Self {
            file,
            scratch,
            active_state: Arc::new(Mutex::new(DatabaseState {
                header,
                atlas,
                reserved: HashMap::default(),
            })),
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
    fn new_grain(&mut self, length: u64) -> io::Result<GrainReservation> {
        let mut active_state = self.active_state.lock();

        active_state.new_grain(length, &mut self.file)
    }

    /// Persists all of the writes to the database. When this function returns,
    /// the data is fully flushed to disk to the best guarantee.
    fn commit_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<Vec<GrainId>> {
        let mut active_state = self.active_state.lock();
        active_state.commit_reservations(reservations, &mut self.file)
    }

    fn forget_reservations(&self, reservations: impl Iterator<Item = GrainReservation>) {
        let mut active_state = self.active_state.lock();

        active_state.forget_reservations(reservations);
    }
}

impl DatabaseState {
    fn new_grain<File: io::File>(
        &mut self,
        length: u64,
        file: &mut File,
    ) -> io::Result<GrainReservation> {
        self.atlas.reserve_grain(length)
    }
    fn commit_reservations<File: io::File>(
        &mut self,
        reservations: impl Iterator<Item = GrainReservation>,
        file: &mut File,
    ) -> io::Result<Vec<GrainId>> {
        todo!("commit reservations")
    }

    fn forget_reservations(&mut self, reservations: impl Iterator<Item = GrainReservation>) {}
}

#[derive(Debug)]
pub struct GrainReservation {
    pub grain_id: GrainId,
    pub offset: u64,
    pub length: u64,
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
