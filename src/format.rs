use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

use bitvec::prelude::BitVec;
use crc::{Crc, CRC_32_MPEG_2};

use crate::{
    io::{self, ext::ToIoResult, iobuffer::IoBufferExt},
    utils::Multiples,
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

/// A header contains a sequence ID and a list of segment indexes. On-disk, this
/// structure will never be longer than [`PAGE_SIZE`].
#[derive(Default, Clone, Debug)]
pub struct Header {
    /// The [`SequenceId`] of the batch that wrote this header.
    pub sequence: BatchId,
    /// The [`SequenceId`] of the last batch checkpointed.
    pub checkpoint: BatchId,
    /// The location on disk of the most recent [`LogPage`].
    pub log_offset: u64,
    /// The list of basins. Cannot exceed 255.
    pub basins: Vec<BasinIndex>,
}

impl Header {
    pub fn write_to<File: io::File>(
        &self,
        offset: u64,
        write_full_page: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(PAGE_SIZE, 0);

        assert!(self.basins.len() <= 254, "too many basins");

        buffer[4] = self.basins.len().try_into().unwrap();
        buffer[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        buffer[16..24].copy_from_slice(&self.checkpoint.to_le_bytes());
        buffer[24..32].copy_from_slice(&self.log_offset.to_le_bytes());
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

    pub fn deserialize_from(bytes: &[u8], verify_crc: bool) -> io::Result<Self> {
        assert!(bytes.len() == PAGE_SIZE);

        let basin_count = usize::from(bytes[4]);

        assert!(basin_count <= 254, "too many basins");

        if verify_crc {
            let total_bytes = basin_count * 16 + 32;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(io::invalid_data_error(format!("Basin CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let sequence = BatchId::from_le_bytes_slice(&bytes[8..16]);
        let checkpoint = BatchId::from_le_bytes_slice(&bytes[16..24]);
        let log_offset = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
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
            log_offset,
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

/// A header contains a sequence ID and a list of segment indexes. A header is
/// 4,096 bytes on-disk, and the sequence ID in little endian format is the
/// first 8 bytes. Each 8 bytes after
#[derive(Default, Clone, Debug)]
pub struct Basin {
    pub sequence: BatchId,
    pub strata: Vec<StratumIndex>,
}

impl Basin {
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

        assert!(self.strata.len() <= 254, "too many strata");
        buffer[4] = self.strata.len().try_into().unwrap();
        buffer[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        let mut length = 32;
        for stratum in &self.strata {
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

    pub fn deserialize_from(bytes: &[u8], verify_crc: bool) -> io::Result<Self> {
        assert!(bytes.len() == PAGE_SIZE);

        let strata_count = usize::from(bytes[4]);
        if strata_count == 255 {
            return Err(io::invalid_data_error("too many strata"));
        }

        if verify_crc {
            let total_bytes = strata_count * 16 + 32;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(io::invalid_data_error(format!("Stratum CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let sequence = BatchId::from_le_bytes_slice(&bytes[8..16]);
        let mut offset = 32;
        let mut strata = Vec::with_capacity(255);
        for _ in 0..strata_count {
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
            strata.push(StratumIndex {
                grain_map_count,
                grain_count_exp,
                grain_length_exp,
                grain_map_location,
            });
        }

        Ok(Self { sequence, strata })
    }
}
// Stored on-disk as 16 bytes.
#[derive(Clone, Debug)]
pub struct StratumIndex {
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

impl StratumIndex {
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
        self.header_length()
            + (self.grains_per_map() * u64::from(self.grain_length()))
                .round_to_multiple_of(PAGE_SIZE_U64)
                .unwrap()
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
        (grain_count + 7) / 8 + 12
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
                return Err(io::invalid_data_error(format!("Grain Map CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
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
        offset: u64,
        file: &mut File,
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
#[derive(Clone, Debug)]
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
    pub fn deserialize(buffer: &[u8], verify_crc: bool) -> io::Result<Self> {
        assert!(buffer.len() >= 170 * 24 + 4);
        let mut offset = 0;
        let mut page = GrainMapPage::default();
        for grain in 0..170 {
            page.grains[grain].allocated_at =
                BatchId::from_le_bytes_slice(&buffer[offset..offset + 8]).validate();
            offset += 8;
            page.grains[grain].archived_at =
                BatchId::from_le_bytes_slice(&buffer[offset..offset + 8]).validate();
            offset += 8;
            page.grains[grain].length =
                u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap());
            offset += 4;
            page.grains[grain].crc =
                u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap());
            offset += 4;
        }
        if verify_crc {
            let computed_crc = crc(&buffer[..offset]);
            let stored_crc = u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap());
            if stored_crc != computed_crc {
                return Err(io::invalid_data_error("grain map page crc check failed"));
            }
        }
        Ok(page)
    }

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
pub struct BatchId(pub(crate) u64);

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

    pub const fn validate(self) -> Option<Self> {
        if self.0 > 0 {
            Some(self)
        } else {
            None
        }
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

/// A grain id is a reference to a stored value.
///
/// - 8 bits: Basin Index
/// - 8 bits: Stratum Index
/// - 48 bits: Grain Index
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct GrainId(u64);

impl Debug for GrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrainId")
            .field("basin", &self.basin_index())
            .field("stratum", &self.stratum_index())
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

    pub const fn stratum_index(&self) -> u8 {
        ((self.0 >> 48) & 0xFF) as u8
    }

    pub const fn grain_index(&self) -> u64 {
        self.0 & 0xFFFF_FFFF_FFFF
    }

    pub fn from_parts(
        basin_index: usize,
        stratum_index: usize,
        grain_index: u64,
    ) -> io::Result<Self> {
        match (u64::try_from(basin_index), u64::try_from(stratum_index)) {
            (Ok(basin_index), Ok(stratum_index))
                if basin_index < 255 && stratum_index < 255 && grain_index < 0x1_0000_0000_0000 =>
            {
                Ok(Self(basin_index << 56 | stratum_index << 48 | grain_index))
            }
            (_, _) => Err(io::invalid_data_error(format!(
                "argument of range ({basin_index}, {stratum_index}, {grain_index}"
            ))),
        }
    }
}

#[test]
fn grain_id_format_tests() {
    let test = GrainId::from_parts(1, 2, 3).unwrap();
    assert_eq!(format!("{test}"), "0102-3");
    assert_eq!(
        format!("{test:?}"),
        "GrainId { basin: 1, stratum: 2, grain: 3 }"
    );
}

/// A single page of the commit log.
///
/// Each page can contain 170 entries. Once an entry is written, it stays in the
/// log page and is never removed. Entries can only be added. If this page has
/// additional entries, `previous_offset` will point to an additional page of
/// log entries. This can continue indefinitely.
///
/// Once all of the entries on a page have been checkpointed, the page will be
/// freed.
///
/// Only entries whose `BatchId`s are greater than the checkpointed ID should be
/// considered valid. Similarly, if the first entry in a page is the `BatchId`
/// after the checkpoint, the previous page will have been freed and should not
/// be loaded. This allows minimal writes to maintain the log: checkpointing
/// does not require changing any log entries, just updating the header.
#[derive(Debug)]
pub struct LogPage {
    /// The location on-disk of the previous `LogPage`. This page is only valid
    /// if the first entry of this page is greater than the current checkpoint
    /// plus 1.
    pub previous_offset: u64,
    /// The log entry indexes.
    pub entries: [LogEntryIndex; 170],
}

impl LogPage {
    pub fn serialize_into(&self, buffer: &mut Vec<u8>) {
        buffer.resize(PAGE_SIZE, 0);

        buffer[..8].copy_from_slice(&self.previous_offset.to_le_bytes());
        let mut offset = 8;
        for entry in &self.entries {
            // No need to write any more data
            if entry.position == 0 {
                break;
            }
            buffer[offset..offset + 8].copy_from_slice(&entry.position.to_le_bytes());
            offset += 8;
            buffer[offset..offset + 8].copy_from_slice(&entry.batch.to_le_bytes());
            offset += 8;
            buffer[offset..offset + 8].copy_from_slice(
                &entry
                    .embedded_header
                    .map_or(0, |grain| grain.0)
                    .to_le_bytes(),
            );
            offset += 8;
        }

        // If we exited early, we want to ensure the remaining portion of the
        // page is zeroed.
        buffer[offset..].fill(0);
    }

    pub fn deserialize_from(mut serialized: &[u8]) -> io::Result<Self> {
        if serialized.len() < 8 {
            return Err(io::invalid_data_error("page not long enough"));
        }

        let previous_offset = u64::from_le_bytes(serialized[..8].try_into().unwrap());
        serialized = &serialized[8..];
        let mut entries = [LogEntryIndex::default(); 170];

        let mut index = 0;
        while serialized.len() >= 24 {
            let data = u64::from_le_bytes(serialized[..8].try_into().unwrap());
            let batch = BatchId::from_le_bytes_slice(&serialized[8..16]);
            let embedded_header = u64::from_le_bytes(serialized[16..24].try_into().unwrap());
            entries[index] = LogEntryIndex {
                position: data,
                batch,
                embedded_header: if embedded_header == 0 {
                    None
                } else {
                    Some(GrainId(embedded_header))
                },
            };
            index += 1;
            serialized = &serialized[24..]
        }
        if !serialized.is_empty() && index < 170 {
            Err(io::invalid_data_error("extra trailing bytes"))
        } else {
            Ok(Self {
                previous_offset,
                entries,
            })
        }
    }
}

impl Default for LogPage {
    fn default() -> Self {
        Self {
            previous_offset: 0,
            entries: [LogEntryIndex::default(); 170],
        }
    }
}

/// Metadata about a single entry in the commit log.
#[derive(Debug, Default, Copy, Clone)]
pub struct LogEntryIndex {
    /// The location of the data containing a [`CommitLog`].
    pub position: u64,
    /// The batch of this entry.
    pub batch: BatchId,
    /// The embedded header at the time of this batch.
    pub embedded_header: Option<GrainId>,
}

/// A list of changes that happened during a commit.
#[derive(Debug, Default)]
pub struct CommitLogEntry {
    /// The list of all changed grains.
    ///
    /// To verify that this log entry is valid, all GrainHeaders of affected
    /// GrainIds must be inspected to ensure they all reflect the same sequence
    /// as this entry.
    ///
    /// A full validation will require checksumming the stored values of those
    /// grains.
    pub grain_changes: Vec<GrainChange>,
}

impl CommitLogEntry {
    pub fn serialize_into(&self, buffer: &mut Vec<u8>) {
        buffer.clear();
        // Reserve space for the CRC
        buffer.extend([0; 4]);
        buffer.extend(
            u32::try_from(self.grain_changes.len())
                .unwrap()
                .to_le_bytes(),
        );
        for change in &self.grain_changes {
            buffer.extend(change.start.0.to_le_bytes());
            // Top 2 bits are the operation.
            assert!(change.count < 2_u32.pow(30));
            let op_and_count = (change.operation as u32) << 30 | change.count;
            buffer.extend(op_and_count.to_le_bytes());
        }
        let crc = crc(&buffer[4..]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());

        // Pad the structure to the next page size.
        buffer.resize(buffer.len().round_to_multiple_of(PAGE_SIZE).unwrap(), 0);
    }

    pub fn read_from<File: io::File>(
        offset: u64,
        verify_crc: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<Self> {
        // This structure is always at least one page long. We'll start there to
        // get the length.
        let mut buffer = std::mem::take(scratch);
        buffer.resize(PAGE_SIZE, 0);
        let (result, mut buffer) = file.read_exact(buffer, offset);
        result?;

        let grain_count =
            usize::try_from(u32::from_le_bytes(buffer[4..8].try_into().unwrap())).unwrap();
        let total_length = grain_count
            .checked_mul(12)
            .and_then(|l| l.checked_add(8))
            .unwrap();
        if total_length > PAGE_SIZE {
            // Read the remaining amount of data
            buffer.resize(total_length, 0);
            let (result, returned_buffer) =
                file.read_exact(buffer.io_slice(PAGE_SIZE..), offset + PAGE_SIZE_U64);
            buffer = returned_buffer;
            result?;
        }
        *scratch = buffer;

        if verify_crc {
            let calculated_crc = crc(&scratch[4..]);
            let stored_crc = u32::from_le_bytes(scratch[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(io::invalid_data_error(format!("Commit Log Entry CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let mut log = Self::default();
        let mut serialized = &scratch[8..];
        while serialized.len() >= 12 {
            let start = GrainId(u64::from_le_bytes(serialized[..8].try_into().unwrap()));
            let op_and_count = u32::from_le_bytes(serialized[8..12].try_into().unwrap());
            log.grain_changes.push(GrainChange {
                start,
                operation: GrainOperation::try_from(op_and_count >> 30)?,
                count: op_and_count & 0x3FFF_FFFF,
            });
            serialized = &serialized[12..];
        }

        if !serialized.is_empty() {
            Err(io::invalid_data_error("extra trailing bytes"))
        } else {
            Ok(log)
        }
    }
}

/// A single change to a sequence of grains.
#[derive(Debug)]
pub struct GrainChange {
    /// The operation that happened to this grain.
    pub operation: GrainOperation,
    /// The starting grain ID of the operation.
    pub start: GrainId,
    /// The number of sequential grains affected by this operation.
    pub count: u32,
}

/// The operation being performed on the grain.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GrainOperation {
    /// The grain transitioned from Free to Allocated.
    Allocate = 0,
    /// The grain transitioned from Allocated to Archived.
    Archive = 1,
    /// The grain transitioned from Archived to Free,
    Free = 2,
}

impl TryFrom<u32> for GrainOperation {
    type Error = std::io::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Allocate),
            1 => Ok(Self::Archive),
            2 => Ok(Self::Free),
            _ => Err(io::invalid_data_error("invalid value for GrainOperation")),
        }
    }
}
