use crate::{
    database::{Database, GrainReservation},
    format::{crc, BatchId, GrainId},
    io::{self, ext::ToIoResult, iobuffer::IoBufferExt},
};

#[derive(Debug)]
pub struct WriteSession<'a, File>
where
    File: io::File,
{
    database: &'a mut Database<File>,
    writes: Vec<GrainReservation>,
}

impl<'a, File> WriteSession<'a, File>
where
    File: io::File,
{
    pub(super) fn new(database: &'a mut Database<File>) -> Self {
        Self {
            database,
            writes: Vec::new(),
        }
    }

    pub fn write(&mut self, mut data: &[u8]) -> io::Result<GrainId> {
        let length = u32::try_from(data.len()).to_io()?;
        let reservation = self.database.new_grain(length)?;
        let crc = crc(data);

        let mut scratch = std::mem::take(&mut self.database.scratch);
        scratch.resize((data.len() + 8).min(16_384), 0);
        scratch[0..4].copy_from_slice(&crc.to_le_bytes());
        scratch[4..8].copy_from_slice(&length.to_le_bytes());
        let mut write_at = reservation.offset;
        let mut header_written = false;
        while !data.is_empty() {
            let (slice, bytes_copied) = if header_written {
                let bytes_to_copy = data.len().min(scratch.len());
                scratch[..bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);
                (scratch.io_slice(..bytes_to_copy), bytes_to_copy)
            } else {
                header_written = true;
                let bytes_to_copy = data.len().min(scratch.len() - 8);
                scratch[8..bytes_to_copy + 8].copy_from_slice(&data[..bytes_to_copy]);
                (scratch.io_slice(..bytes_to_copy + 8), bytes_to_copy)
            };

            let (result, buffer) = self.database.file.write_all(slice, write_at);
            result?;
            scratch = buffer;

            data = &data[bytes_copied..];
            write_at += u64::try_from(bytes_copied).to_io()?;
        }

        debug_assert!(header_written);

        let id = reservation.grain_id;
        self.writes.push(reservation);

        Ok(id)
    }

    pub fn commit(mut self) -> io::Result<BatchId> {
        self.database.commit_reservations(self.writes.drain(..))
    }
}

impl<'a, File> Drop for WriteSession<'a, File>
where
    File: io::File,
{
    fn drop(&mut self) {
        if !self.writes.is_empty() {
            self.database
                .forget_reservations(self.writes.drain(..))
                .unwrap();
        }
    }
}
