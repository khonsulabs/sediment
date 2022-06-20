use std::ops::{Deref, DerefMut};

use crate::{
    database::{
        committer::GrainBatchOperation,
        embedded::{EmbeddedHeaderGuard, EmbeddedHeaderUpdate},
        Database,
    },
    format::{BatchId, GrainId, CRC},
    io::{self, ext::ToIoResult, iobuffer::IoBufferExt, File},
};

#[derive(Debug)]
pub struct WriteSession<Manager>
where
    Manager: io::FileManager,
{
    database: Database<Manager>,
    writes: Vec<GrainBatchOperation>,
}

impl<Manager> WriteSession<Manager>
where
    Manager: io::FileManager,
{
    pub(super) fn new(database: Database<Manager>) -> Self {
        Self {
            database,
            writes: Vec::new(),
        }
    }

    /// Locks access to the current embedded header value. Only one session per
    /// database will be able to acquire a `HeaderUpdateSession` at a time, the
    /// other sessions will block until they are able to acquire the lock.
    #[must_use]
    pub fn updating_embedded_header(self) -> HeaderUpdateSession<Manager> {
        HeaderUpdateSession {
            header_guard: self.database.state.embedded_header.lock(),
            session: self,
            new_embedded_header: EmbeddedHeaderUpdate::None,
        }
    }

    pub fn write(&mut self, mut data: &[u8]) -> io::Result<GrainId> {
        let length = u32::try_from(data.len()).to_io()?;
        let mut file = self
            .database
            .state
            .file_manager
            .write(&self.database.path)?;
        let mut reservation = self.database.new_grain(length)?;
        let mut crc = CRC.digest();
        crc.update(&length.to_le_bytes());
        crc.update(data);
        let crc = crc.finalize();
        reservation.crc = Some(crc);

        let mut scratch = std::mem::take(&mut self.database.scratch);
        scratch.resize((data.len() + 8).min(16_384), 0);
        scratch[0..4].copy_from_slice(&crc.to_le_bytes());
        scratch[4..8].copy_from_slice(&length.to_le_bytes());
        let mut write_at = reservation.offset;
        let mut header_written = false;
        while !data.is_empty() {
            let (slice, source_bytes_copied) = if header_written {
                let bytes_to_copy = data.len().min(scratch.len());
                scratch[..bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);
                (scratch.io_slice(..bytes_to_copy), bytes_to_copy)
            } else {
                header_written = true;
                let bytes_to_copy = data.len().min(scratch.len() - 8);
                scratch[8..bytes_to_copy + 8].copy_from_slice(&data[..bytes_to_copy]);
                (scratch.io_slice(..bytes_to_copy + 8), bytes_to_copy)
            };

            let total_bytes_copied = slice.len();

            let (result, buffer) = file.write_all(slice, write_at);
            result?;
            scratch = buffer;

            data = &data[source_bytes_copied..];
            write_at += u64::try_from(total_bytes_copied).to_io()?;
        }

        debug_assert!(header_written);

        let id = reservation.grain_id;
        self.writes.push(GrainBatchOperation::Allocate(reservation));

        Ok(id)
    }

    pub fn archive(&mut self, grain_id: GrainId) -> io::Result<()> {
        let count = self.database.archive(grain_id)?;

        self.writes
            .push(GrainBatchOperation::Archive { grain_id, count });

        Ok(())
    }

    pub fn commit(mut self) -> io::Result<BatchId> {
        self.database
            .commit_reservations(self.writes.drain(..), None, None)
    }

    pub fn commit_and_checkpoint(mut self, checkpoint_to: BatchId) -> io::Result<BatchId> {
        self.database
            .commit_reservations(self.writes.drain(..), Some(checkpoint_to), None)
    }
}

impl<Manager> Drop for WriteSession<Manager>
where
    Manager: io::FileManager,
{
    fn drop(&mut self) {
        if !self.writes.is_empty() {
            self.database
                .forget_reservations(self.writes.drain(..).filter_map(|op| {
                    if let GrainBatchOperation::Allocate(reservation) = op {
                        Some(reservation)
                    } else {
                        None
                    }
                }))
                .unwrap();
        }
    }
}

#[derive(Debug)]
pub struct HeaderUpdateSession<Manager>
where
    Manager: io::FileManager,
{
    session: WriteSession<Manager>,
    header_guard: EmbeddedHeaderGuard,
    new_embedded_header: EmbeddedHeaderUpdate,
}

impl<Manager> HeaderUpdateSession<Manager>
where
    Manager: io::FileManager,
{
    /// Returns the embedded header [`GrainId`], if one is present. This returns
    /// the current in-memory state of the value and may not be persisted to
    /// disk yet. To read the current header's value, use
    /// [`Database::embedded_header`].
    pub fn embedded_header(&self) -> Option<GrainId> {
        match self.new_embedded_header {
            EmbeddedHeaderUpdate::None => *self.header_guard,
            EmbeddedHeaderUpdate::Replace(new_value) => new_value,
        }
    }

    /// Updates the embedded header to the new [`GrainId`], or clears the header
    /// if None is provided. This function updates the in-memory state for the
    /// pending commit of this session, but the new value will not be available
    /// until the session is committed.
    ///
    /// If this session is not committed, the header will not be published. Once
    /// the session is dropped, another session will be able to acquire the
    /// embedded header lock, and it will have the value prior to beginning this
    /// session.
    pub fn set_embedded_header(&mut self, embedded_header: Option<GrainId>) -> io::Result<()> {
        let mut embedded_header = EmbeddedHeaderUpdate::Replace(embedded_header);
        std::mem::swap(&mut embedded_header, &mut self.new_embedded_header);
        if let EmbeddedHeaderUpdate::Replace(Some(grain_id)) = embedded_header {
            // This update already had assigned a new grain, we need to archive it.
            self.archive(grain_id)?;
        }
        Ok(())
    }

    pub fn commit(self) -> io::Result<BatchId> {
        let Self {
            mut session,
            mut header_guard,
            new_embedded_header,
        } = self;

        let header_guard = if let EmbeddedHeaderUpdate::Replace(mut header) = new_embedded_header {
            std::mem::swap(&mut header, &mut header_guard);
            if let Some(grain_id) = header {
                // We replaced an old header, archive the old grain.
                session.archive(grain_id)?;
            }
            Some(header_guard)
        } else {
            // The header never updated.
            None
        };

        session
            .database
            .commit_reservations(session.writes.drain(..), None, header_guard)
    }

    pub fn commit_and_checkpoint(self, checkpoint_to: BatchId) -> io::Result<BatchId> {
        let Self {
            mut session,
            mut header_guard,
            new_embedded_header,
        } = self;

        let header_guard = if let EmbeddedHeaderUpdate::Replace(new_header) = new_embedded_header {
            *header_guard = new_header;
            Some(header_guard)
        } else {
            // The header never updated.
            None
        };

        session.database.commit_reservations(
            session.writes.drain(..),
            Some(checkpoint_to),
            header_guard,
        )
    }
}

impl<Manager> Deref for HeaderUpdateSession<Manager>
where
    Manager: io::FileManager,
{
    type Target = WriteSession<Manager>;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

impl<Manager> DerefMut for HeaderUpdateSession<Manager>
where
    Manager: io::FileManager,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.session
    }
}
