use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    io::ErrorKind,
};

use bitvec::prelude::BitVec;
use crc::{Crc, CRC_32_MPEG_2};

use crate::{
    io::{self, ext::ToIoResult, iobuffer::IoBufferExt},
    utils::RoundToMultiple,
};

/// The fundamental size and alignment of data within the file.
pub const PAGE_SIZE: usize = 4096;
/// [`PAGE_SIZE`] but in a u64.
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_MPEG_2);

pub fn crc(data: &[u8]) -> u32 {
    let mut digest = CRC.digest();
    digest.update(data);
    digest.finalize()
}

/// The file header is made up of two [`Header`]s. This structure is always
/// located at the beginning of the file.
///
/// When reading from disk, compare the stored [`SequenceId`]s. Whichever is
/// larger should be validated (verify all reachable headers have the correct
/// CRCs or sequence IDs). If it cannot be validated, the other header should be
/// validated.
///
/// If both can't be validated, the file has been corrupted. If either can be
/// validated, it will be used as the active state of the file. The next write
/// *must* overwrite the version that was not used when loading from the file.
#[derive(Default, Debug)]
pub struct FileHeader {
    pub active: usize,
    pub headers: [Header; 2],
}

impl FileHeader {
    pub fn current(&self) -> &Header {
        &self.headers[self.active]
    }

    const fn next_index(&self) -> usize {
        if self.active == 0 {
            1
        } else {
            0
        }
    }

    pub fn write_next(&mut self) -> &mut Header {
        // Copy the state from the current header to the next header
        let current = self.current().clone();
        *self.next_mut() = current;
        self.next_mut()
    }

    pub fn next(&self) -> &Header {
        &self.headers[self.next_index()]
    }

    pub fn next_mut(&mut self) -> &mut Header {
        let (_, basin) = self.next_with_offset();
        basin
    }

    pub fn move_next(&mut self) {
        self.active = self.next_index();
    }

    fn next_with_offset(&mut self) -> (u64, &mut Header) {
        let next = self.next_index();

        (
            u64::try_from(next * PAGE_SIZE).unwrap(),
            &mut self.headers[next],
        )
    }

    pub fn flush_to_file<File: io::File>(
        &mut self,
        new_sequence: BatchId,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        if self.headers[0].sequence.0 == 0 && self.headers[1].sequence.0 == 0 {
            self.next_mut().sequence = new_sequence;
            self.headers[0].write_to(0, file, true, scratch)?;
            self.headers[1].write_to(PAGE_SIZE_U64, file, true, scratch)?;
        } else {
            let (offset, basin) = self.next_with_offset();
            basin.sequence = new_sequence;
            basin.write_to(offset, file, false, scratch)?;
        }

        file.synchronize()?;

        self.move_next();

        Ok(())
    }

    pub fn read_from<File: io::File>(file: &mut File, scratch: &mut Vec<u8>) -> io::Result<Self> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(PAGE_SIZE * 2, 0);
        let (result, buffer) = file.read_exact(buffer, 0);
        *scratch = buffer;
        result?;

        let headers = [
            Header::read_from(&scratch[..PAGE_SIZE], true)?,
            Header::read_from(&scratch[PAGE_SIZE..], true)?,
        ];

        let active = if headers[0].sequence > headers[1].sequence {
            0
        } else {
            1
        };

        Ok(Self { active, headers })
    }
}

/// A header contains a sequence ID and a list of segment indexes. On-disk, this
/// structure will never be longer than [`PAGE_SIZE`].
#[derive(Default, Clone, Debug)]
pub struct Header {
    /// The [`SequenceId`] of the batch that wrote this header.
    pub sequence: BatchId,
    /// The [`SequenceId`] of the last batch checkpointed.
    pub checkpoint: BatchId,
    /// The list of basins. Cannot exceed 255.
    pub basins: Vec<BasinIndex>,
}

impl Header {
    // pub fn stripe_offset(&self, stripe: GrainId) -> Option<u64> {
    //     self.stratum
    //         .get(usize::from(stripe.segment().0))
    //         .map(|index| index.grains_offset + index.bytes_per_stripe() * stripe.index())
    // }

    pub fn write_to<File: io::File>(
        &self,
        offset: u64,
        file: &mut File,
        write_full_page: bool,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(PAGE_SIZE, 0);

        assert!(self.basins.len() <= 254, "too many basins");

        buffer[4] = self.basins.len().try_into().unwrap();
        buffer[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        buffer[16..24].copy_from_slice(&self.checkpoint.to_le_bytes());
        let mut length = 32;
        for basin in &self.basins {
            buffer[length..length + 8].copy_from_slice(&basin.sequence_id.to_le_bytes());
            length += 8;
            buffer[length..length + 8].copy_from_slice(&basin.file_offset.to_le_bytes());
            length += 8;
        }
        let crc = crc(&buffer[4..length]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());

        let (result, buffer) = file.write_all(
            if write_full_page {
                buffer.io()
            } else {
                buffer.io_slice(..length)
            },
            offset,
        );
        *scratch = buffer;
        result
    }

    pub fn read_from(bytes: &[u8], verify_crc: bool) -> io::Result<Self> {
        assert!(bytes.len() == PAGE_SIZE);

        let basin_count = usize::from(bytes[4]);

        assert!(basin_count <= 254, "too many basins");

        if verify_crc {
            let total_bytes = basin_count * 16 + 32;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(std::io::Error::new(ErrorKind::InvalidData, format!("Basin CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let sequence = BatchId::from_le_bytes_slice(&bytes[8..16]);
        let checkpoint = BatchId::from_le_bytes_slice(&bytes[16..24]);
        let mut offset = 32;
        let mut basins = Vec::with_capacity(256);
        for _ in 0..basin_count {
            let sequence_id = BatchId::from_le_bytes_slice(&bytes[offset..offset + 8]);
            offset += 8;
            let file_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            basins.push(BasinIndex {
                sequence_id,
                file_offset,
            });
        }

        Ok(Self {
            sequence,
            checkpoint,
            basins,
        })
    }
}

/// A pointer to a [`Basin`]. Stored as 16 bytes on disk.
#[derive(Clone, Debug)]
pub struct BasinIndex {
    pub sequence_id: BatchId,
    pub file_offset: u64,
}

#[derive(Default, Clone, Debug)]
pub struct BasinHeader {
    active: usize,
    basins: [Basin; 2],
}

impl BasinHeader {
    pub fn current(&self) -> &Basin {
        &self.basins[self.active]
    }

    const fn next_index(&self) -> usize {
        if self.active == 0 {
            1
        } else {
            0
        }
    }

    pub fn next(&self) -> &Basin {
        &self.basins[self.next_index()]
    }

    pub fn next_mut(&mut self) -> &mut Basin {
        let (_, basin) = self.next_with_offset();
        basin
    }

    pub fn move_next(&mut self) {
        self.active = self.next_index();
    }

    fn next_with_offset(&mut self) -> (u64, &mut Basin) {
        let next = self.next_index();

        (
            u64::try_from(next * PAGE_SIZE).unwrap(),
            &mut self.basins[next],
        )
    }

    pub fn read_from<File: io::File>(
        file: &mut File,
        offset: u64,
        scratch: &mut Vec<u8>,
        verify_crc: bool,
    ) -> io::Result<Self> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(PAGE_SIZE * 2, 0);
        let (result, buffer) = file.read_exact(buffer, offset);
        *scratch = buffer;
        result?;

        let basins = [
            Basin::read_from(&scratch[..PAGE_SIZE], verify_crc)?,
            Basin::read_from(&scratch[PAGE_SIZE..], verify_crc)?,
        ];

        let active = if basins[0].sequence > basins[1].sequence {
            0
        } else {
            1
        };

        Ok(Self { active, basins })
    }

    pub fn write_to<File: io::File>(
        &mut self,
        new_sequence: BatchId,
        file: &mut File,
        offset: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        if self.basins[0].sequence.0 == 0 && self.basins[1].sequence.0 == 0 {
            self.next_mut().sequence = new_sequence;
            self.basins[0].write_to(offset, file, true, scratch)?;
            self.basins[1].write_to(offset + PAGE_SIZE_U64, file, true, scratch)?;
        } else {
            let (basin_offset, basin) = self.next_with_offset();
            basin.write_to(offset + basin_offset, file, false, scratch)?;
        }

        self.move_next();

        Ok(())
    }
}

/// A header contains a sequence ID and a list of segment indexes. A header is
/// 4,096 bytes on-disk, and the sequence ID in little endian format is the
/// first 8 bytes. Each 8 bytes after
#[derive(Default, Clone, Debug)]
pub struct Basin {
    pub sequence: BatchId,
    pub stratum: Vec<StrataIndex>,
}

impl Basin {
    // pub fn stripe_offset(&self, stripe: GrainId) -> Option<u64> {
    //     self.stratum
    //         .get(usize::from(stripe.segment().0))
    //         .map(|index| index.grains_offset + index.bytes_per_stripe() * stripe.index())
    // }

    pub fn write_to<File: io::File>(
        &self,
        offset: u64,
        file: &mut File,
        write_full_page: bool,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(PAGE_SIZE, 0);

        assert!(self.stratum.len() <= 254, "too many stratum");
        buffer[4] = self.stratum.len().try_into().unwrap();
        buffer[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        let mut length = 32;
        for stratum in &self.stratum {
            buffer[length..length + 4].copy_from_slice(&stratum.grain_map_count.to_le_bytes());
            length += 4;
            buffer[length] = stratum.grain_count_exp;
            length += 1;
            buffer[length] = stratum.grain_length_exp;
            length += 1;
            // Padding
            length += 2;
            buffer[length..length + 8].copy_from_slice(&stratum.grain_map_location.to_le_bytes());
            length += 8;
        }
        let crc = crc(&buffer[4..length]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());

        let (result, buffer) = file.write_all(
            if write_full_page {
                buffer.io()
            } else {
                buffer.io_slice(..length)
            },
            offset,
        );
        *scratch = buffer;
        result
    }

    pub fn read_from(bytes: &[u8], verify_crc: bool) -> io::Result<Self> {
        assert!(bytes.len() == PAGE_SIZE);

        let stratum_count = usize::from(bytes[4]);
        if stratum_count == 255 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "too many stratum",
            ));
        }

        if verify_crc {
            let total_bytes = stratum_count * 16 + 16;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(std::io::Error::new(ErrorKind::InvalidData, format!("Strata CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let sequence = BatchId::from_le_bytes_slice(&bytes[8..10]);
        let mut offset = 16;
        let mut stratum = Vec::with_capacity(255);
        for _ in 0..stratum_count {
            let mut int_bytes = [0; 4];
            int_bytes.copy_from_slice(&bytes[offset..offset + 4]);
            let grain_map_count = u32::from_le_bytes(int_bytes);
            offset += 4;
            let grain_count_exp = bytes[offset];
            offset += 1;
            let grain_length_exp = bytes[offset];
            offset += 1;

            // Padding
            offset += 2;
            let grain_map_location =
                u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            stratum.push(StrataIndex {
                grain_map_count,
                grain_count_exp,
                grain_length_exp,
                grain_map_location,
            });
        }

        Ok(Self { sequence, stratum })
    }
}
// Stored on-disk as 16 bytes.
#[derive(Clone, Debug)]
pub struct StrataIndex {
    // The number of grain maps allocated.
    pub grain_map_count: u32,
    /// 2^`grain_count_exp` is the number of pages of grains each map contains.
    /// Each page of grains contains 170 grains.
    pub grain_count_exp: u8,
    /// 2^`grain_length_exp` - 32 is the number of bytes each grain has
    /// allocated. The limit is 32. In practice, this value will never be below
    /// 7 (96-byte grains), and will grow as-needed based on the file's needs.
    pub grain_length_exp: u8,
    /// If `grain_map_count` is 1, this points directly to a [`GrainMap`]. If
    /// `grain_map_count` is greater than 0, this points to a list of
    /// [`GrainMap`] offsets.
    pub grain_map_location: u64,
}

impl StrataIndex {
    pub const fn grains_per_map(&self) -> u64 {
        2_u64.pow(self.grain_count_exp as u32) * 170
    }

    pub const fn grain_length(&self) -> u32 {
        2_u32.pow(self.grain_length_exp as u32)
    }

    pub fn grain_location(&self, index: u32) -> u64 {
        self.grain_map_location
            + GrainMap::header_length_for_grain_count(self.grains_per_map()) * 2
            + u64::from(self.grain_length()) * index as u64
    }

    pub fn header_length(&self) -> u64 {
        GrainMap::header_length_for_grain_count(self.grains_per_map()) * 2
    }

    pub fn grain_map_length(&self) -> u64 {
        self.grains_per_map() * u64::from(self.grain_length())
    }
}

#[derive(Debug)]
pub struct GrainMapHeader {
    offset: u64,
    active: usize,
    headers: [GrainMap; 2],
}

impl GrainMapHeader {
    pub fn new(offset: u64, grain_count: u64) -> Self {
        Self {
            offset,
            active: 0,
            headers: [GrainMap::new(grain_count), GrainMap::new(grain_count)],
        }
    }

    pub const fn offset(&self) -> u64 {
        self.offset
    }

    pub fn current(&self) -> &GrainMap {
        &self.headers[self.active]
    }

    const fn next_index(&self) -> usize {
        if self.active == 0 {
            1
        } else {
            0
        }
    }

    pub fn next(&self) -> &GrainMap {
        &self.headers[self.next_index()]
    }

    pub fn next_mut(&mut self) -> &mut GrainMap {
        let (_, basin) = self.next_with_offset();
        basin
    }

    pub fn move_next(&mut self) {
        self.active = self.next_index();
    }

    fn next_with_offset(&mut self) -> (u64, &mut GrainMap) {
        let next = self.next_index();

        (
            u64::try_from(next * PAGE_SIZE).unwrap(),
            &mut self.headers[next],
        )
    }

    pub fn write_to<File: io::File>(
        &mut self,
        new_sequence_id: BatchId,
        grain_count: u64,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        if self.headers[0].sequence.0 == 0 && self.headers[1].sequence.0 == 0 {
            self.next_mut().sequence = new_sequence_id;
            self.headers[0].write_to(file, self.offset, scratch)?;
            self.headers[1].write_to(
                file,
                self.offset + GrainMap::header_length_for_grain_count(grain_count),
                scratch,
            )?;
        } else {
            let (offset, basin) = self.next_with_offset();
            basin.write_to(file, offset, scratch)?;
        }

        self.move_next();

        Ok(())
    }

    pub fn read_from<File: io::File>(
        file: &mut File,
        offset: u64,
        grain_count: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<Self> {
        let headers = [
            GrainMap::read_from(file, offset, grain_count, scratch, true)?,
            GrainMap::read_from(
                file,
                offset + GrainMap::header_length_for_grain_count(grain_count),
                grain_count,
                scratch,
                true,
            )?,
        ];

        let active = if headers[0].sequence > headers[1].sequence {
            0
        } else {
            1
        };

        Ok(Self {
            active,
            offset,
            headers,
        })
    }
}

/// Written in duplicate.
#[derive(Debug, Default)]
pub struct GrainMap {
    /// The sequence of the batch this grain map
    pub sequence: BatchId,
    /// Ranges of grain allocations.
    pub allocation_state: BitVec<u8>,
    // // A list of grains, where each entry has an active an inactive version.
}

impl GrainMap {
    pub fn new(grain_count: u64) -> Self {
        let mut allocation_state = BitVec::new();
        allocation_state.resize(usize::try_from(grain_count).unwrap(), false);
        Self {
            sequence: BatchId::default(),
            allocation_state,
        }
    }

    fn unaligned_header_length_for_grain_count(grain_count: u64) -> u64 {
        // 8 bits per 8 grains, plus 12 bytes of header overhead (crc + sequence ID)
        grain_count
            .round_to_multiple_of(8)
            .map(|bytes| bytes + 12)
            .expect("impossible to overflow")
    }

    pub fn header_length_for_grain_count(grain_count: u64) -> u64 {
        Self::unaligned_header_length_for_grain_count(grain_count)
            .round_to_multiple_of(PAGE_SIZE_U64)
            .expect("impossible to overflow")
    }

    pub fn read_from<File: io::File>(
        file: &mut File,
        offset: u64,
        grain_count: u64,
        scratch: &mut Vec<u8>,
        validate: bool,
    ) -> io::Result<Self> {
        let mut buffer = std::mem::take(scratch);
        buffer.resize(
            usize::try_from(Self::unaligned_header_length_for_grain_count(grain_count)).to_io()?,
            0,
        );
        let (result, buffer) = file.read_exact(buffer, offset);
        result?;

        if validate {
            let calculated_crc = crc(&buffer[4..]);
            let stored_crc = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(std::io::Error::new(ErrorKind::InvalidData, format!("Grain Map CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }
        let sequence = BatchId::from_le_bytes_slice(&buffer[4..12]);
        let allocation_state = BitVec::from_vec(buffer[12..].to_vec());
        *scratch = buffer;
        Ok(Self {
            sequence,
            allocation_state,
        })
    }

    pub fn write_to<File: io::File>(
        &self,
        file: &mut File,
        offset: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut buffer = std::mem::take(scratch);
        buffer.clear();

        buffer.extend(&[0; 4]); // Reserve space for CRC
        buffer.extend(self.sequence.to_le_bytes());
        buffer.extend_from_slice(self.allocation_state.as_raw_slice());

        // Calculate the CRC of everything after the CRC
        let crc = crc(&buffer[4..]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());

        let (result, returned_buffer) = file.write_all(buffer, offset);
        *scratch = returned_buffer;
        result?;

        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Allocation {
    Free,
    Allocated,
}

impl Default for Allocation {
    fn default() -> Self {
        Self::Free
    }
}

/// Each GrainMapPage is a collection of 256 [`GrainInfo`] structures.
#[derive(Debug)]
pub struct GrainMapPage {
    pub grains: [GrainInfo; 170],
}

impl Default for GrainMapPage {
    fn default() -> Self {
        Self {
            grains: [GrainInfo::default(); 170],
        }
    }
}

impl GrainMapPage {
    pub fn write_to<File: io::File>(
        &self,
        offset: u64,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut buffer = std::mem::take(scratch);
        buffer.resize(PAGE_SIZE, 0);

        let mut bytes_written = 0;
        for grain in self.grains {
            buffer[bytes_written..bytes_written + 8]
                .copy_from_slice(&grain.allocated_at.unwrap_or_default().to_le_bytes());
            bytes_written += 8;
            buffer[bytes_written..bytes_written + 8]
                .copy_from_slice(&grain.archived_at.unwrap_or_default().to_le_bytes());
            bytes_written += 8;
            buffer[bytes_written..bytes_written + 4].copy_from_slice(&grain.length.to_le_bytes());
            bytes_written += 4;
            buffer[bytes_written..bytes_written + 4].copy_from_slice(&grain.crc.to_le_bytes());
            bytes_written += 4;
        }

        let crc = crc(&buffer[..bytes_written]);
        buffer[bytes_written..bytes_written + 4].copy_from_slice(&crc.to_le_bytes());

        let (result, buffer) = file.write_all(buffer, offset);
        *scratch = buffer;
        result
    }
}

/// Information about a single grain. A grain's lifecycle looks like this:
///
/// | Step           | `allocated_at` | `archived_at`         | Grain State |
/// |----------------|----------------|-----------------------|-------------|
/// | initial        | None           | None                  | free        |
/// | allocate       | Some(Active)   | None                  | allocated   |
/// | archive        | Some(Active)   | Some(Active)          | archived    |
/// | free for reuse | Some(Inactive) | Some(Active)          | free        |
/// | allocate       | Some(Active)   | None                  | allocated   |
///
/// The highest bit of the SequenceId is used to store whether the sequence ID
/// information is considered active or not. This sequence of a lifecycle allows
/// reverting to the previous step at any given stage in the lifecycle without
/// losing any data.
#[derive(Debug, Default, Copy, Clone)]
pub struct GrainInfo {
    pub allocated_at: Option<BatchId>,
    pub archived_at: Option<BatchId>,
    pub length: u32,
    pub crc: u32,
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BatchId(u64);

impl BatchId {
    const ARCHIVED_MASK: u64 = 0x8000_0000_0000_0000;
    const ID_MASK: u64 = 0x7FFF_FFFF_FFFF_FFFF;

    pub const fn first() -> Self {
        Self(1)
    }

    pub const fn active(&self) -> bool {
        self.0 & Self::ARCHIVED_MASK == 0
    }

    pub const fn sequence(&self) -> u64 {
        self.0 & Self::ID_MASK
    }

    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn from_le_bytes_slice(bytes: &[u8]) -> Self {
        Self::from_le_bytes(bytes.try_into().expect("incorrect byte length"))
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    pub fn next(&self) -> Self {
        Self(self.0.checked_add(1).expect("u64 wrapped"))
    }
}

impl Ord for BatchId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.sequence().cmp(&other.sequence()) {
            Ordering::Equal => self.active().cmp(&other.active()),
            not_equal => not_equal,
        }
    }
}

impl PartialOrd for BatchId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for BatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// A grain id is a reference to a stored value. Internally, this value is
/// represented by 48 bits:
///
/// - 8 bits: Basin Index
/// - 8 bits: Strata Index
/// - 32 bits: Grain Index
///
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct GrainId(u64);

impl Debug for GrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrainId")
            .field("basin", &self.basin_index())
            .field("strata", &self.strata_index())
            .field("grain", &self.grain_index())
            .finish()
    }
}

impl Display for GrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04x}-{:x}", self.0 >> 48, self.grain_index())
    }
}

impl GrainId {
    pub const fn basin_index(&self) -> u8 {
        (self.0 >> 56) as u8
    }

    pub const fn strata_index(&self) -> u8 {
        ((self.0 >> 48) & 0xFF) as u8
    }

    pub const fn grain_index(&self) -> u64 {
        self.0 & 0xFFFF_FFFF_FFFF
    }

    pub fn from_parts(
        basin_index: usize,
        strata_index: usize,
        grain_index: u64,
    ) -> io::Result<Self> {
        match (u64::try_from(basin_index), u64::try_from(strata_index)) {
            (Ok(basin_index), Ok(strata_index))
                if basin_index < 255 && strata_index < 255 && grain_index < 0x1_0000_0000_0000 =>
            {
                Ok(Self(basin_index << 56 | strata_index << 48 | grain_index))
            }
            (_, _) => Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("argument of range ({basin_index}, {strata_index}, {grain_index}"),
            )),
        }
    }
}

#[test]
fn grain_id_format_tests() {
    let test = GrainId::from_parts(1, 2, 3).unwrap();
    assert_eq!(format!("{test}"), "0102-3");
    assert_eq!(
        format!("{test:?}"),
        "GrainId { basin: 1, strata: 2, grain: 3 }"
    );
}

pub struct Log {
    /// GrainIds of stored [`LogEntry`] values that have not been checkpointed.
    pub entries: Vec<GrainId>,
}

pub struct LogEntry {
    /// The list of all changed grains.
    ///
    /// To verify that this log entry is valid,
    /// all GrainHeaders of affected GrainIds must be inspected to ensure they
    /// all reflect the same sequence as this entry.
    ///
    /// A full validation will require checksumming the stored values of those
    /// grains.
    pub grain_changes: Vec<GrainId>,
}
