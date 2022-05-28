use std::{cmp::Ordering, io::ErrorKind, iter::Peekable};

use crc::{Crc, CRC_32_MPEG_2};

use crate::{
    io::{self, iobuffer::IoBufferExt},
    ranges::{self, Ranges},
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

    pub fn write_to<File: io::File>(
        &mut self,
        file: &mut File,
        initializing: bool,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        if initializing {
            self.headers[0].write_to(0, file, true, scratch)?;
            self.headers[1].write_to(PAGE_SIZE_U64, file, true, scratch)?;
        } else {
            let (offset, basin) = self.next_with_offset();
            basin.write_to(offset, file, false, scratch)?;
        }

        file.synchronize()?;

        self.move_next();

        Ok(())
    }

    pub fn read_from<File: io::File>(file: &mut File, scratch: &mut Vec<u8>) -> io::Result<Self> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(8192, 0);
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
    pub sequence: SequenceId,
    /// The [`SequenceId`] of the last batch checkpointed.
    pub checkpoint: SequenceId,
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

        assert!(self.basins.len() <= 255, "too many basins");

        buffer[4] = self.basins.len().try_into().unwrap();
        buffer[8..10].copy_from_slice(&self.sequence.to_le_bytes());
        let mut length = 32;
        for basin in &self.basins {
            buffer[length..length + 8].copy_from_slice(&basin.0.to_le_bytes());
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

        if verify_crc {
            let total_bytes = stratum_count * 16 + 32;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(std::io::Error::new(ErrorKind::InvalidData, format!("Basin CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let sequence = SequenceId::from_le_bytes_slice(&bytes[8..10]);
        let checkpoint = SequenceId::from_le_bytes_slice(&bytes[10..12]);
        let mut offset = 16;
        let mut basins = Vec::with_capacity(256);
        for _ in 0..stratum_count {
            let sequence_and_location =
                u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            basins.push(BasinIndex(sequence_and_location));
        }

        Ok(Self {
            sequence,
            checkpoint,
            basins,
        })
    }
}

/// A pointer to a [`Basin`]. Stored as 8 bytes on disk.
///
/// The pointed-to [`Basin`]s must be stored within the first 281 terabytes of a
/// file. Currently, encountering this edge case will result in an error.
#[derive(Clone, Debug)]
pub struct BasinIndex(u64);

impl BasinIndex {
    pub const fn sequence(&self) -> SequenceId {
        SequenceId((self.0 >> 48) as u16)
    }

    /// The location of the first of two [`Basin`]s. Restricted to 48 bits.
    pub const fn basin_location(&self) -> u64 {
        self.0 & 0xFFFF_FFFF_FFFF
    }
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
        buffer.resize(8192, 0);
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
        file: &mut File,
        offset: u64,
        initializing: bool,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        if initializing {
            self.basins[0].write_to(offset, file, true, scratch)?;
            self.basins[1].write_to(offset + PAGE_SIZE_U64, file, true, scratch)?;
        } else {
            let (basin_offset, basin) = self.next_with_offset();
            basin.write_to(offset + basin_offset, file, false, scratch)?;
        }

        file.synchronize()?;

        self.move_next();

        Ok(())
    }
}

/// A header contains a sequence ID and a list of segment indexes. A header is
/// 4,096 bytes on-disk, and the sequence ID in little endian format is the
/// first 8 bytes. Each 8 bytes after
#[derive(Default, Clone, Debug)]
pub struct Basin {
    pub sequence: SequenceId,
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
        buffer[8..10].copy_from_slice(&self.sequence.to_le_bytes());
        let mut length = 32;
        for stratum in &self.stratum {
            buffer[length..length + 2].copy_from_slice(&stratum.grain_map_sequence.to_le_bytes());
            length += 2;
            buffer[length] = stratum.grain_count_exp;
            length += 1;
            buffer[length] = stratum.grain_length_exp;
            length += 1;
            buffer[length..length + 4].copy_from_slice(&stratum.grain_map_count.to_le_bytes());
            length += 4;
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

        let sequence = SequenceId::from_le_bytes_slice(&bytes[8..10]);
        let mut offset = 16;
        let mut stratum = Vec::with_capacity(255);
        for _ in 0..stratum_count {
            let grain_map_sequence = SequenceId::from_le_bytes_slice(&bytes[offset..offset + 2]);
            offset += 2;
            let grain_count_exp = bytes[offset];
            offset += 1;
            let grain_length_exp = bytes[offset];
            offset += 1;

            let mut int_bytes = [0; 4];
            int_bytes.copy_from_slice(&bytes[offset..offset + 4]);
            let grain_map_count = u32::from_le_bytes(int_bytes);
            offset += 4;
            let grain_map_location =
                u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            stratum.push(StrataIndex {
                grain_map_sequence,
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
    pub grain_map_sequence: SequenceId,
    pub grain_map_count: u32,
    /// 2^`grain_count_exp` is the number of pages of grains each map contains.
    /// Each page of grains contains 256 grains.
    pub grain_count_exp: u8,
    /// 2^`grain_length_exp` - 32 is the number of bytes each grain has
    /// allocated. The limit is 32. In practice, this value will never be below
    /// 7 (96-byte grains), and will grow as-needed based on the file's needs.
    pub grain_length_exp: u8,
    /// The location of the first grain map for this strata.
    pub grain_map_location: u64,
}

impl StrataIndex {
    pub const fn grain_count(&self) -> u64 {
        2_u64.pow(self.grain_count_exp as u32)
    }

    pub const fn grain_length(&self) -> u64 {
        2_u64.pow(self.grain_length_exp as u32) - 32
    }

    pub const fn grain_location(&self, index: u32) -> u64 {
        self.grain_map_location
            + GrainMap::header_length_for_grain_count(self.grain_count())
            + self.grain_length() * index as u64
    }
}

/// Written in duplicate.
#[derive(Debug)]
pub struct GrainMap {
    pub additional_maps: Option<u64>,
    /// The sequence of the batch this grain map
    pub sequence: SequenceId,
    /// Ranges of grain allocations.
    pub allocation_state: Ranges<GrainAllocation>,
    // // A list of grains, where each entry has an active an inactive version.
}

impl GrainMap {
    pub const fn header_length_for_grain_count(grain_count: u64) -> u64 {
        // The worst case encoding results in half of the number of grains as
        // bytes. This would require every other grain to be a different state.
        // Any repeats would begin to lower the ratio.
        //
        // The additional 4 bytes is for the CRC.
        (grain_count + 1) / 2 + 4
    }

    pub fn read_from<File: io::File>(
        &self,
        file: &mut File,
        offset: u64,
        scratch: &mut Vec<u8>,
        validate: bool,
    ) -> io::Result<Self> {
        let mut buffer = std::mem::take(scratch);
        // We won't know the full length until we read from disk. We know that
        // this header will be padded to a page length, so we just read a single
        // page to begin with.
        buffer.resize(PAGE_SIZE, 0);
        let (result, mut buffer) = file.read_exact(buffer, offset);
        result?;

        let length = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        buffer.resize(usize::try_from(length).unwrap(), 0);
        if buffer.len() > PAGE_SIZE {
            let (result, returned_buffer) =
                file.read_exact(buffer.io_slice(PAGE_SIZE..), offset + PAGE_SIZE_U64);
            buffer = returned_buffer;
            result?;
        }

        if validate {
            let calculated_crc = crc(&buffer[4..]);
            let stored_crc = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            if calculated_crc != stored_crc {
                return Err(std::io::Error::new(ErrorKind::InvalidData, format!("Grain Map CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let additional_maps = match u64::from_le_bytes(buffer[8..16].try_into().unwrap()) {
            0 => None,
            maps => Some(maps),
        };
        let sequence = SequenceId::from_le_bytes_slice(&buffer[16..18]);
        let allocation_state = decode_grain_allocations(&buffer[32..])?;
        Ok(Self {
            sequence,
            allocation_state,
            additional_maps,
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

        buffer.extend(&[0; 8]); // Reserve space for CRC and Length
        buffer.extend(self.additional_maps.unwrap_or_default().to_le_bytes());
        buffer.extend(self.sequence.to_le_bytes());
        let header_length = buffer.len();
        // This makes the buffer larger, but this should never be prohibitively
        // large.
        buffer.extend(GrainRangeAllocationEncoder::new(&self.allocation_state));
        let length = buffer.len() - header_length;
        buffer[4..8].copy_from_slice(&length.to_le_bytes());

        // Calculate the CRC of everything after the CRC
        let crc = crc(&buffer[4..]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());

        let (result, returned_buffer) = file.write_all(buffer, offset);
        *scratch = returned_buffer;
        result?;

        Ok(())
    }
}

/// The lifecycle of a grain.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum GrainAllocation {
    /// The grain's data is considered unallocated.
    ///
    /// The only valid state transition is to Reserved.
    Free,

    /// **In-Memory State only.** The grain is reserved for writing to. A grain
    /// can only be reserved by one writer at a time.
    ///
    /// If the write is committed, the state transitions to Assigned. If the
    /// write is abandoned, the state transitions to Free.
    Reserved,

    /// The grain has data written to it. It can be read from in this state or
    /// marked for deletion.
    ///
    /// If the grain is mared for deletion, the state transitions to
    /// PendingArchive.
    Assigned,

    /// **In-Memory State only.** The grain map will be updated on the next
    /// commit to mark this grain as archived.
    PendingArchive,

    /// This grain previously had data stored in it. The grain will be freed
    /// once the database is checkpointed.
    ///
    /// Once the database is checkpointed beyond the sequence ID for the
    /// archival operation, the state transitions to Free.
    Archived,

    /// **In-Memory State only.** The grain map will be updated on the next
    /// commit to mark this grain as Free.
    PendingFree,
}

impl TryFrom<u8> for GrainAllocation {
    type Error = std::io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0b00 => Ok(Self::Free),
            0b01 => Ok(Self::Assigned),
            0b10 => Ok(Self::Archived),
            _ => Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "invalid allocation type",
            )),
        }
    }
}

impl GrainAllocation {
    pub const fn bits(&self) -> u8 {
        match self {
            GrainAllocation::Free | GrainAllocation::Reserved => 0b00,

            GrainAllocation::Assigned | GrainAllocation::PendingArchive => 0b01,
            GrainAllocation::Archived | GrainAllocation::PendingFree => 0b10,
        }
    }
}

/// An efficent encoding of GrainAllocations to enable persisting the
/// allocations to disk.
///
/// The most significant bit of the byte dictates whether this byte encodes one
/// allocation or two:
///
/// - `0b0` - Split encoding
/// - `0b1` - Full encoding
///
/// Both formats use the same bit patterns to encode the serialized allocation
/// states:
///
/// - `0b00` - Free
/// - `0b01` - Assigned
/// - `0b10` - Archived
///
/// ## Split encoding
///
/// The remaining 7 bits are encoded in this sequence:
///
/// - 2 bits: Allocation State
/// - 2 bits: Allocation Length
/// - 2 bits: Allocation State
/// - 1 bits: Allocation Length
///
/// ## Full encoding
///
/// The remaining 7-bites are encoded in this sequence:
///
/// - 2 bits: Allocation State
/// - 5 bits: Allocation Length
///
/// When a length of 0 is encountered, no further data is parsed.
#[derive(Debug)]
pub struct GrainRangeAllocationEncoder<'a> {
    ranges: Peekable<ranges::Iter<'a, GrainAllocation>>,
    partially_encoded_span: Option<(u64, GrainAllocation)>,
}

impl<'a> GrainRangeAllocationEncoder<'a> {
    pub fn new(ranges: &'a Ranges<GrainAllocation>) -> Self {
        Self {
            ranges: ranges.iter().peekable(),
            partially_encoded_span: None,
        }
    }

    fn encode_split(
        &mut self,
        first_span_length: u8,
        first_span_allocation: GrainAllocation,
    ) -> u8 {
        if let Some((next_span_range, next_span_allocation)) = self.ranges.next() {
            let mut encoded = (first_span_allocation.bits() << 5)
                | (first_span_length << 3)
                | (next_span_allocation.bits() << 1);
            // Subtract one right away because the precense of this half
            // counts as 1.
            let next_span_length = (*next_span_range.end() + 1)
                .checked_sub(*next_span_range.start())
                .expect("invalid range")
                .saturating_sub(1);

            // The next length only has 1 bit
            if next_span_length <= 1 {
                encoded |= next_span_length as u8;
            } else {
                encoded |= 1;
                if let Some(remaining_length) = next_span_length.checked_sub(1) {
                    self.partially_encoded_span = Some((remaining_length, *next_span_allocation));
                }
            }

            encoded
        } else {
            self.encode_full(u64::from(first_span_length), first_span_allocation)
        }
    }

    fn encode_full(
        &mut self,
        first_span_length: u64,
        first_span_allocation: GrainAllocation,
    ) -> u8 {
        0b1000_0000
            | first_span_allocation.bits() << 5
            | if first_span_length <= 0b11111 {
                first_span_length as u8
            } else {
                // The full length encoded
                let remaining_length = first_span_length - 32 + 1;
                self.partially_encoded_span = Some((remaining_length, first_span_allocation));
                0b11111
            }
    }
}

impl<'a> Iterator for GrainRangeAllocationEncoder<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.partially_encoded_span
            .take()
            .or_else(|| {
                self.ranges.next().map(|(range, allocation)| {
                    (
                        range
                            .end()
                            .saturating_add(1)
                            .checked_sub(*range.start())
                            .expect("invalid range"),
                        *allocation,
                    )
                })
            })
            .map(|(length, allocation)| match length.saturating_sub(1) {
                length if length < 3 => self.encode_split(length as u8, allocation),
                length => self.encode_full(length, allocation),
            })
    }
}

pub fn decode_grain_allocations(encoded: &[u8]) -> io::Result<Ranges<GrainAllocation>> {
    // Begin with an
    let mut ranges = Ranges::new(GrainAllocation::Free, None);
    for &byte in encoded {
        if byte & 0b1000_0000 == 0 {
            // split
            let first_allocation = GrainAllocation::try_from((byte >> 5) & 0b11)?;
            let first_length = ((byte >> 3) & 0b11) + 1;
            ranges.extend_by(u64::from(first_length), first_allocation);
            let second_allocation = GrainAllocation::try_from((byte >> 1) & 0b11)?;
            let second_length = (byte & 1) + 1;
            ranges.extend_by(u64::from(second_length), second_allocation);
        } else {
            // full
            let allocation = GrainAllocation::try_from((byte >> 5) & 0b11)?;
            let length = (byte & 0b1_1111) + 1;
            ranges.extend_by(u64::from(length), allocation);
        }
    }
    Ok(ranges)
}

#[test]
fn range_encoding_tests() {
    let mut ranges = Ranges::new(GrainAllocation::Free, None);

    // Half
    ranges.extend_by(1, GrainAllocation::Free);
    ranges.extend_by(1, GrainAllocation::Archived);

    // Half
    ranges.extend_by(1, GrainAllocation::Assigned);
    ranges.extend_by(2, GrainAllocation::Archived);

    // Half
    ranges.extend_by(2, GrainAllocation::Free);
    ranges.extend_by(1, GrainAllocation::Archived);

    // Half
    ranges.extend_by(2, GrainAllocation::Assigned);
    ranges.extend_by(2, GrainAllocation::Archived);

    // Half
    ranges.extend_by(3, GrainAllocation::Assigned);
    ranges.extend_by(1, GrainAllocation::Archived);

    // Half
    ranges.extend_by(3, GrainAllocation::Assigned);
    ranges.extend_by(2, GrainAllocation::Archived);

    // Full that doesn't resume
    ranges.extend_by(4, GrainAllocation::Assigned);

    // Full that needs multiple bytes
    ranges.extend_by(64, GrainAllocation::Free);

    // // Half, followed by something too long to fit in the second half/
    ranges.extend_by(3, GrainAllocation::Assigned);
    ranges.extend_by(42, GrainAllocation::Archived);

    // Something that could be a half, but is at the end.
    ranges.extend_by(3, GrainAllocation::Assigned);

    let encoded = GrainRangeAllocationEncoder::new(&ranges).collect::<Vec<_>>();
    println!("Encoded: {encoded:02x?}");

    let decoded = decode_grain_allocations(&encoded).unwrap();
    assert_eq!(decoded, ranges);
}

/// Each GrainMapPage is a collection of 256 [`GrainInfo`] structures.
#[derive(Debug)]
pub struct GrainMapPage {
    pub grains: [GrainInfo; 256],
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
#[derive(Debug)]
pub struct GrainInfo {
    pub allocated_at: Option<SequenceId>,
    pub archived_at: Option<SequenceId>,
    pub length: u32,
    pub crc: u32,
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SequenceId(u16);

impl SequenceId {
    const ARCHIVED_MASK: u16 = 0x8000;
    const ID_MASK: u16 = 0x3FFF;

    pub const fn active(&self) -> bool {
        self.0 & Self::ARCHIVED_MASK == 0
    }

    pub const fn sequence(&self) -> u16 {
        self.0 & Self::ID_MASK
    }

    pub fn to_le_bytes(&self) -> [u8; 2] {
        self.0.to_le_bytes()
    }

    pub fn from_le_bytes_slice(bytes: &[u8]) -> Self {
        Self(u16::from_le_bytes(
            bytes.try_into().expect("incorrect byte length"),
        ))
    }

    pub fn from_le_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_le_bytes(bytes))
    }
}

impl Ord for SequenceId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.sequence().cmp(&other.sequence()) {
            Ordering::Equal => self.active().cmp(&other.active()),
            not_equal => not_equal,
        }
    }
}

impl PartialOrd for SequenceId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A grain id is a reference to a stored value. Internally, this value is
/// represented by 64 bits:
///
/// - 16 bits: [`SequenceId`]
/// - 8 bits: Basin Index
/// - 8 bits: Strata Index
/// - 32 bits: Grain Index
///
/// If attempting to conserve space, data can be retrieved using only the lower
/// 48 bits of this value. The [`SequenceId`] is used for validation purposes
/// only.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct GrainId(u64);

impl GrainId {
    pub const fn sequence(&self) -> SequenceId {
        SequenceId((self.0 >> 48) as u16)
    }

    pub const fn basin_index(&self) -> u8 {
        ((self.0 >> 40) & 0xFF) as u8
    }

    pub const fn strata_index(&self) -> u8 {
        ((self.0 >> 32) & 0xFF) as u8
    }

    pub const fn grain_index(&self) -> u32 {
        self.0 as u32
    }
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
