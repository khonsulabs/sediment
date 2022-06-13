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
        let mut reservation = self
            .database
            .new_grain(u32::try_from(data.len()).to_io()?)?;
        reservation.crc = crc(data);

        let mut scratch = std::mem::take(&mut self.database.scratch);
        scratch.resize(data.len().min(16_384), 0);
        let mut write_at = reservation.offset;
        while !data.is_empty() {
            let bytes_to_copy = data.len().min(scratch.capacity());
            scratch[..bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);

            let (result, buffer) = self
                .database
                .file
                .write_all(scratch.io_slice(..bytes_to_copy), write_at);
            result?;
            scratch = buffer;

            data = &data[bytes_to_copy..];
            write_at += u64::try_from(bytes_to_copy).to_io()?;
        }

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
