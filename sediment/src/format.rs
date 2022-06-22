use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    io::{ErrorKind, Write},
    num::ParseIntError,
    str::FromStr,
};

use bitvec::prelude::BitVec;

use crate::{
    io::{self, ext::ToIoResult},
    utils::Multiples,
};

/// The fundamental size and alignment of data within the file.
pub const PAGE_SIZE: usize = 4096;
/// [`PAGE_SIZE`] but in a u64.
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;

#[must_use]
pub fn crc(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// The root header for a Sediment file. On-disk, this structure fits in one
/// page (4,096 bytes).
///
/// The header determines the last batch that was committed, the last batch that
/// was checkpointed, a pointer to the most recent [`LogPage`], and list of
/// pointers to allocated [`Basin`]s.
///
/// This structure has two versions stored sequentially at the beginning of the
/// file. When loading an existing database, the instance of the header with the
/// larger `batch` should have its most recent commit completely validated. If
/// any inconsistencies are found, the other header should be used to load the
/// database. After successfully recovering a failed commit, the failed header
/// and all copies of data touched by the failed commit need to be cleared or
/// reverted.
///
/// The current header should never be overwritten by a new commit. The
/// alternate header should always be written to and only after a successful
/// fsync should the alternate header become the current header.
#[derive(Default, Clone, Debug)]
pub struct Header {
    /// The unique id of the batch that wrote this header.
    pub batch: BatchId,
    /// The [`BatchId`] of the last commit that has been checkpointed. All
    /// grains archived on or before this [`BatchId`] will be able to be reused.
    pub checkpoint: BatchId,
    /// The location on disk of the most recent [`LogPage`].
    pub log_offset: u64,
    /// The list of basins. Cannot exceed 254.
    pub basins: Vec<BasinIndex>,
}

impl Header {
    pub fn write_to<File: io::File>(
        &self,
        offset: u64,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut buffer = Vec::new();
        std::mem::swap(&mut buffer, scratch);
        buffer.resize(PAGE_SIZE, 0);

        if self.basins.len() > 254 {
            return Err(io::invalid_data_error("too many basins"));
        }

        buffer[4] = self.basins.len().try_into().expect("asserted above");
        buffer[8..16].copy_from_slice(&self.batch.to_le_bytes());
        buffer[16..24].copy_from_slice(&self.checkpoint.to_le_bytes());
        buffer[24..32].copy_from_slice(&self.log_offset.to_le_bytes());
        let mut length = 32;
        for basin in &self.basins {
            buffer[length..length + 8].copy_from_slice(&basin.last_written_at.to_le_bytes());
            length += 8;
            buffer[length..length + 8].copy_from_slice(&basin.file_offset.to_le_bytes());
            length += 8;
        }
        let crc = crc(&buffer[4..length]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());

        let (result, buffer) = file.write_all(buffer, offset);
        *scratch = buffer;
        result
    }

    pub fn deserialize_from(bytes: &[u8], verify_crc: bool) -> io::Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(io::invalid_data_error("bytes should be 4,096 bytes"));
        }

        let basin_count = usize::from(bytes[4]);

        if basin_count > 254 {
            return Err(io::invalid_data_error("too many basins"));
        }

        if verify_crc {
            let total_bytes = basin_count * 16 + 32;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().expect("u32 is 4 bytes"));
            if calculated_crc != stored_crc {
                return Err(io::invalid_data_error(format!("Basin CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let batch = BatchId::from_le_bytes_slice(&bytes[8..16]);
        let checkpoint = BatchId::from_le_bytes_slice(&bytes[16..24]);
        let log_offset = u64::from_le_bytes(bytes[24..32].try_into().expect("u64 is 8 bytes"));
        let mut offset = 32;
        let mut basins = Vec::with_capacity(256);
        for _ in 0..basin_count {
            let last_updated_at = BatchId::from_le_bytes_slice(&bytes[offset..offset + 8]);
            offset += 8;
            let file_offset = u64::from_le_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("u64 is 8 bytes"),
            );
            offset += 8;
            basins.push(BasinIndex {
                last_written_at: last_updated_at,
                file_offset,
            });
        }

        Ok(Self {
            batch,
            checkpoint,
            log_offset,
            basins,
        })
    }
}

/// A pointer to a [`Basin`]. Stored as 16 bytes on disk.
#[derive(Clone, Debug)]
pub struct BasinIndex {
    /// The [`BatchId`] corresponding to when this basin was last modified.
    pub last_written_at: BatchId,
    /// The offset of the [`Basin`] in the file.
    pub file_offset: u64,
}

/// A Basin manages a list of [`StratumIndex`]es. Basins are purely used for a
/// level of hierarchy. On-disk, this structure will be serialized to a single
/// page.
///
/// This structure has two copies stored sequentially in the file. The copy that
/// is used is the one whose `written_at` matches the corresponding
/// [`BasinIndex::last_written_at`] value.
///
/// Writing should always be done to the copy that is not referred to by the
/// current [`Header`]'s [`BasinIndex`].
#[derive(Default, Clone, Debug)]
pub struct Basin {
    pub written_at: BatchId,
    pub strata: Vec<StratumIndex>,
}

impl Basin {
    pub fn write_to<File: io::WriteIoBuffer>(
        &self,
        offset: u64,
        file: &mut File,
    ) -> io::Result<()> {
        let mut buffer = vec![0; PAGE_SIZE];

        if self.strata.len() > 254 {
            return Err(io::invalid_data_error("too many strata"));
        }

        buffer[4] = self.strata.len().try_into().expect("checked above");
        buffer[8..16].copy_from_slice(&self.written_at.to_le_bytes());
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

        file.write_all_at(buffer, offset)
    }

    pub fn deserialize_from(bytes: &[u8], verify_crc: bool) -> io::Result<Self> {
        if bytes.len() != PAGE_SIZE {
            return Err(io::invalid_data_error("bytes should be 4,096 bytes"));
        }

        let strata_count = usize::from(bytes[4]);
        if strata_count > 254 {
            return Err(io::invalid_data_error("too many strata"));
        }

        if verify_crc {
            let total_bytes = strata_count * 16 + 32;
            let calculated_crc = crc(&bytes[4..total_bytes]);
            let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().expect("u32 is 4 bytes"));
            if calculated_crc != stored_crc {
                return Err(io::invalid_data_error(format!("Stratum CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }

        let written_at = BatchId::from_le_bytes_slice(&bytes[8..16]);
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
            let grain_map_location = u64::from_le_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("u64 is 8 bytes"),
            );
            offset += 8;
            strata.push(StratumIndex {
                grain_map_count,
                grain_count_exp,
                grain_length_exp,
                grain_map_location,
            });
        }

        Ok(Self { written_at, strata })
    }
}
/// Information about a collection of grains. Stored on-disk as 16 bytes.
///
/// A stratum is subdivided into a list of [`GrainMap`]s. Each grain is able to
/// store a fixed number of bytes, and the length is dictated by this structure.
/// To accommodate large databases, each stratum can contain more than one
/// [`GrainMap`]. Each [`GrainMap`] contains the same number of grains, dictated
/// by this structure.
#[derive(Clone, Debug)]
pub struct StratumIndex {
    // The number of grain maps allocated.
    pub grain_map_count: u32,
    /// Controls the number of grains each [`GrainMap`] contains in this
    /// stratum.
    ///
    /// The value is interpretted using  2^`grain_count_exp` as the number of
    /// pages of grains each map contains. Each page of grains contains 170
    /// grains, which means that the number of grains in each [`GrainMap`] is
    /// `2^grain_count_exp * 170`.
    ///
    /// For example, a `grain_count_exp` of `2` is 2^2 (4) pages. 4 pages of 170
    /// grains each can store 680 grains per [`GrainMap`].
    pub grain_count_exp: u8,
    /// 2^`grain_length_exp` is the number of bytes each grain has allocated.
    /// The limit is 32. The length of data stored in a single grain cannot
    /// exceed the capacity of a u32, so this field has a hard limit of 32.
    ///
    /// Practically speaking, it is unlikely to be optimal to allocate 4GB-size
    /// grains as storing such large blobs would introduce severe file space
    /// overhead.
    pub grain_length_exp: u8,
    /// If `grain_map_count` is 1, this points directly to a [`GrainMap`]. If
    /// `grain_map_count` is greater than 0, this points to a list of
    /// [`GrainMap`] offsets (to be implemented).
    pub grain_map_location: u64,
}

impl StratumIndex {
    #[must_use]
    pub const fn grain_map_pages(&self) -> u64 {
        2_u64.pow(self.grain_count_exp as u32)
    }

    #[must_use]
    pub const fn grains_per_map(&self) -> u64 {
        self.grain_map_pages() * GrainMapPage::GRAINS_U64
    }

    #[must_use]
    pub const fn grain_length(&self) -> u32 {
        2_u32.pow(self.grain_length_exp as u32)
    }

    #[must_use]
    pub fn header_length(&self) -> u64 {
        GrainMap::header_length_for_grain_count(self.grains_per_map())
    }

    #[must_use]
    pub fn total_header_length(&self) -> u64 {
        self.header_length() * 2
    }

    #[must_use]
    pub fn grain_map_length(&self) -> u64 {
        self.grain_map_data_offset()
            + (self.grains_per_map() * u64::from(self.grain_length()))
                .round_to_multiple_of(PAGE_SIZE_U64)
                .expect("too many grains")
                * 2
    }

    #[must_use]
    pub fn grain_map_data_offset(&self) -> u64 {
        self.total_header_length() + u64::from(self.grain_map_count) * PAGE_SIZE_U64 * 2
    }
}

/// A header for a map of grains. The header contains an overview of the grain
/// allocation states, allowing the database load process to not require
/// scanning every page of grains to evaluate their allocation states.
///
/// This header is dynamically sized and written in duplicate. Its length is
/// padded to the next page size, but a single page currently can represent up
/// to 32,656 grains in a single page.
///
/// The list of [`GrainMapPage`] headers follows both copies of this header.
///
/// After all of the [`GrainMapPage`] headers is a contiguous region of the file
/// for all grains contained within this map.
#[derive(Debug, Default)]
pub struct GrainMap {
    /// The batch this header was written during.
    pub written_at: BatchId,
    /// A collection of bits representing whether each grain is allocated or
    /// not.
    pub allocation_state: BitVec<u8>,
}

impl GrainMap {
    pub fn new(grain_count: u64) -> io::Result<Self> {
        let mut allocation_state = BitVec::new();
        allocation_state.resize(usize::try_from(grain_count).to_io()?, false);
        Ok(Self {
            written_at: BatchId::default(),
            allocation_state,
        })
    }

    fn unaligned_header_length_for_grain_count(grain_count: u64) -> u64 {
        // 8 bits per 8 grains, plus 12 bytes of header overhead (crc + BatchId)
        (grain_count + 7) / 8 + 12
    }

    #[must_use]
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
            let stored_crc = u32::from_le_bytes(buffer[0..4].try_into().expect("u32 is 4 bytes"));
            if calculated_crc != stored_crc {
                return Err(io::invalid_data_error(format!("Grain Map CRC Check Failed (Stored: {stored_crc:x} != Calculated: {calculated_crc:x}")));
            }
        }
        let written_at = BatchId::from_le_bytes_slice(&buffer[4..12]);
        let mut allocation_state = BitVec::from_vec(buffer[12..].to_vec());
        // Because we always encode in chunks of 8, we may have additional bits
        // after restoring from the file.
        allocation_state.truncate(usize::try_from(grain_count).to_io()?);
        *scratch = buffer;
        Ok(Self {
            written_at,
            allocation_state,
        })
    }

    pub fn write_to<File: io::WriteIoBuffer>(
        &self,
        offset: u64,
        file: &mut File,
    ) -> io::Result<()> {
        let mut buffer = Vec::with_capacity(PAGE_SIZE);

        buffer.extend(&[0; 4]); // Reserve space for CRC
        buffer.extend(self.written_at.to_le_bytes());
        buffer.extend_from_slice(self.allocation_state.as_raw_slice());

        // Calculate the CRC of everything after the CRC
        let crc = crc(&buffer[4..]);
        buffer[..4].copy_from_slice(&crc.to_le_bytes());
        buffer.resize(
            buffer
                .len()
                .round_to_multiple_of(PAGE_SIZE)
                .expect("too large"),
            0,
        );

        file.write_all_at(buffer, offset)
    }
}

/// Whether a location is considered allocated or free to use.
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

/// A header that describes 4,084 grain allocations.
///
/// This data structure is written twice. The page with the most recent
/// `written_at` is considered the current page.
#[derive(Clone, Debug)]
pub struct GrainMapPage {
    /// The batch when this page was last updated at.
    pub written_at: BatchId,
    /// Each element represents how many consecutive grains are allocated at the
    /// given index. For example, consider this list of entries:
    ///
    /// - Grain 0: `0`
    /// - Grain 1: `1`
    /// - Grain 2: `3`
    /// - Grain 3: `2`
    /// - Grain 4: `1`
    /// - Grain 5: `1`
    ///
    /// There is data stored in 5 out of the 6 grains listed above. Grain 0 is
    /// unallocated, Grain 1 is allocated spanning a single grain, Grain 2 is
    /// allocated spanning 3 grains, and Grain 5 is allocated spanning a single
    /// grain.
    ///
    /// This arrangement allows validating that a particular `GrainId` is
    /// pointing to the first grain allocated.
    pub consecutive_allocations: [u8; Self::GRAINS],
}

impl Default for GrainMapPage {
    fn default() -> Self {
        Self {
            written_at: BatchId(0),
            consecutive_allocations: [0; Self::GRAINS],
        }
    }
}

impl GrainMapPage {
    pub const GRAINS: usize = PAGE_SIZE - 12;
    pub const GRAINS_U64: u64 = Self::GRAINS as u64;

    pub fn deserialize(buffer: &[u8], verify_crc: bool) -> io::Result<Self> {
        if buffer.len() != PAGE_SIZE {
            return Err(io::invalid_data_error("GrainMapPage not correct length"));
        }

        let written_at = BatchId::from_le_bytes_slice(&buffer[4..12]);
        let mut page = GrainMapPage {
            written_at,
            ..GrainMapPage::default()
        };
        page.consecutive_allocations.copy_from_slice(&buffer[12..]);

        if verify_crc {
            let computed_crc = crc(&buffer[4..]);
            let stored_crc = u32::from_le_bytes(buffer[0..4].try_into().expect("u32 is 4 bytes"));
            if stored_crc != computed_crc {
                return Err(io::invalid_data_error("grain map page crc check failed"));
            }
        }
        Ok(page)
    }

    pub fn write_to<File: io::WriteIoBuffer>(
        &self,
        offset: u64,
        file: &mut File,
    ) -> io::Result<()> {
        let mut buffer = vec![0; PAGE_SIZE];

        buffer[4..12].copy_from_slice(&self.written_at.to_le_bytes());
        buffer[12..].copy_from_slice(&self.consecutive_allocations);

        let crc = crc(&buffer[4..]);
        buffer[0..4].copy_from_slice(&crc.to_le_bytes());

        file.write_all_at(buffer, offset)
    }
}

/// The unique id of an ACID-compliant batch write operation.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BatchId(pub(crate) u64);

impl BatchId {
    const INACTIVE_MASK: u64 = 0x8000_0000_0000_0000;
    const ID_MASK: u64 = 0x7FFF_FFFF_FFFF_FFFF;

    #[must_use]
    pub const fn first() -> Self {
        Self(1)
    }

    #[must_use]
    pub const fn active(&self) -> bool {
        self.0 & Self::INACTIVE_MASK == 0
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.0 & Self::ID_MASK
    }

    #[must_use]
    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    #[must_use]
    pub fn from_le_bytes_slice(bytes: &[u8]) -> Self {
        Self::from_le_bytes(bytes.try_into().expect("incorrect byte length"))
    }

    #[must_use]
    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    #[must_use]
    pub fn next(&self) -> Self {
        Self(self.0.checked_add(1).expect("u64 wrapped"))
    }

    #[must_use]
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
            .field("basin", &(self.basin_index() + 1))
            .field("stratum", &(self.stratum_index() + 1))
            .field("grain", &(self.grain_index() + 1))
            .finish()
    }
}

impl Display for GrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}-{:x}",
            self.basin_index() + 1,
            self.stratum_index() + 1,
            self.grain_index() + 1
        )
    }
}

impl FromStr for GrainId {
    type Err = InvalidGrainId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (basin_and_stratum, grain) = s.split_once('-').ok_or(InvalidGrainId)?;
        if basin_and_stratum.len() != 4 {
            return Err(InvalidGrainId);
        }
        let basin = u8::from_str_radix(&basin_and_stratum[..2], 16)?;
        let stratum = u8::from_str_radix(&basin_and_stratum[2..4], 16)?;
        let grain = u64::from_str_radix(grain, 16)?;
        if basin == 0 || stratum == 0 || grain == 0 {
            Err(InvalidGrainId)
        } else {
            GrainId::new(basin - 1, stratum - 1, grain - 1)
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InvalidGrainId;

impl From<ParseIntError> for InvalidGrainId {
    fn from(_: ParseIntError) -> Self {
        Self
    }
}

impl Display for InvalidGrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid grain id")
    }
}

impl std::error::Error for InvalidGrainId {}

impl GrainId {
    #[must_use]
    pub const fn basin_index(&self) -> u8 {
        (self.0 >> 56) as u8 - 1
    }

    #[must_use]
    pub const fn stratum_index(&self) -> u8 {
        ((self.0 >> 48) & 0xFF) as u8 - 1
    }

    #[must_use]
    pub const fn grain_index(&self) -> u64 {
        (self.0 & 0xFFFF_FFFF_FFFF) - 1
    }

    pub fn new(
        basin_index: u8,
        stratum_index: u8,
        grain_index: u64,
    ) -> Result<GrainId, InvalidGrainId> {
        let basin_index = u64::from(basin_index);
        let stratum_index = u64::from(stratum_index);

        if basin_index < 255 && stratum_index < 255 && grain_index < 0xFFFF_FFFF_FFFF {
            // We use 0 vs non-zero to distingush for Option<GrainId>. Since
            // each of our fields is limited, we can add 1 to each index to make
            // the GrainId be 1-based, allowing for 0 to represent no grain id.
            // Technically we only had to do this to one field, but
            // all fields are done for consistency.
            Ok(Self(
                (basin_index + 1) << 56 | (stratum_index + 1) << 48 | (grain_index + 1),
            ))
        } else {
            Err(InvalidGrainId)
        }
    }

    pub fn from_parts(
        basin_index: usize,
        stratum_index: usize,
        grain_index: u64,
    ) -> io::Result<Self> {
        match (u8::try_from(basin_index), u8::try_from(stratum_index)) {
            (Ok(basin_index), Ok(stratum_index)) => {
                Self::new(basin_index, stratum_index, grain_index).map_err(io::invalid_data_error)
            }
            (_, _) => Err(io::invalid_data_error(format!(
                "argument of range ({basin_index}, {stratum_index}, {grain_index}"
            ))),
        }
    }

    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for GrainId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<GrainId> for u64 {
    fn from(grain: GrainId) -> Self {
        grain.0
    }
}

#[test]
fn grain_id_format_tests() {
    let test = GrainId::from_parts(1, 2, 3).unwrap();
    // The visual display format for
    assert_eq!(format!("{test}"), "0203-4");
    let parsed = GrainId::from_str(&test.to_string()).unwrap();
    assert_eq!(test, parsed);
    assert_eq!(
        format!("{test:?}"),
        "GrainId { basin: 2, stratum: 3, grain: 4 }"
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
#[derive(Debug, Clone)]
pub struct LogPage {
    /// The location on-disk of the previous `LogPage`. This page is only valid
    /// if the first entry of this page is greater than the current checkpoint
    /// plus 1.
    pub previous_offset: u64,
    /// The batch id of entry 0. Each entry after is the next number. No batch
    /// id will be skipped.
    pub first_batch_id: BatchId,
    /// The log entry indexes.
    pub entries: [LogEntryIndex; 170],
}

impl LogPage {
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // value is always 1 byte
    pub const fn index_of_batch_id(&self, batch_id: BatchId) -> Option<usize> {
        match batch_id.0.checked_sub(self.first_batch_id.0) {
            Some(delta) if delta < 170 => Some(delta as usize),
            _ => None,
        }
    }

    pub fn serialize_into(&self, buffer: &mut Vec<u8>) {
        buffer.resize(PAGE_SIZE, 0);

        buffer[..8].copy_from_slice(&self.previous_offset.to_le_bytes());
        buffer[8..16].copy_from_slice(&self.first_batch_id.to_le_bytes());
        let mut offset = 16;
        for entry in &self.entries {
            // No need to write any more data
            if entry.position == 0 {
                break;
            }
            buffer[offset..offset + 8].copy_from_slice(&entry.position.to_le_bytes());
            offset += 8;
            buffer[offset..offset + 4].copy_from_slice(&entry.crc.to_le_bytes());
            offset += 4;
            buffer[offset..offset + 4].copy_from_slice(&entry.length.to_le_bytes());
            offset += 4;
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
        if serialized.len() < 16 {
            return Err(io::invalid_data_error("page not long enough"));
        }

        let previous_offset =
            u64::from_le_bytes(serialized[..8].try_into().expect("u64 is 8 bytes"));
        let first_batch_id = BatchId::from_le_bytes_slice(&serialized[8..16]);
        serialized = &serialized[16..];
        let mut entries = [LogEntryIndex::default(); 170];

        let mut index = 0;
        while serialized.len() >= 24 {
            let position = u64::from_le_bytes(serialized[..8].try_into().expect("u64 is 8 bytes"));
            let crc = u32::from_le_bytes(serialized[8..12].try_into().expect("u32 is 4 bytes"));
            let length = u32::from_le_bytes(serialized[12..16].try_into().expect("u32 is 4 bytes"));
            let embedded_header =
                u64::from_le_bytes(serialized[16..24].try_into().expect("u64 is 8 bytes"));
            entries[index] = LogEntryIndex {
                position,
                length,
                crc,
                embedded_header: if embedded_header == 0 {
                    None
                } else {
                    Some(GrainId(embedded_header))
                },
            };
            index += 1;
            serialized = &serialized[24..];
        }
        if !serialized.is_empty() && index < 170 {
            Err(io::invalid_data_error("extra trailing bytes"))
        } else {
            Ok(Self {
                previous_offset,
                first_batch_id,
                entries,
            })
        }
    }
}

impl Default for LogPage {
    fn default() -> Self {
        Self {
            previous_offset: 0,
            first_batch_id: BatchId(0),
            entries: [LogEntryIndex::default(); 170],
        }
    }
}

/// Metadata about a single entry in the commit log.
#[derive(Debug, Default, Copy, Clone)]
pub struct LogEntryIndex {
    /// The location of the data containing a [`CommitLogEntry`].
    pub position: u64,
    pub length: u32,
    pub crc: u32,
    /// The embedded header at the time of this batch.
    pub embedded_header: Option<GrainId>,
}

/// A list of changes that happened during a commit.
#[derive(Debug, Default)]
pub struct CommitLogEntry {
    /// The list of all changed grains.
    ///
    /// To verify that this log entry is valid, all GrainHeaders of affected
    /// GrainIds must be inspected to ensure they all reflect the same batch as
    /// this entry.
    ///
    /// A full validation will require checksumming the stored values of those
    /// grains.
    pub grain_changes: Vec<GrainChange>,
}

impl CommitLogEntry {
    pub fn serialize_into(&self, buffer: &mut Vec<u8>) -> io::Result<()> {
        buffer.clear();
        for change in &self.grain_changes {
            buffer.extend(change.start.0.to_le_bytes());
            buffer.push(change.count);
            change.operation.serialize_into(buffer)?;
        }
        Ok(())
    }

    pub fn load_from<File: io::File>(
        entry: &LogEntryIndex,
        verify_crc: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<Self> {
        let mut buffer = std::mem::take(scratch);
        buffer.resize(usize::try_from(entry.length).to_io()?, 0);
        let (result, buffer) = file.read_exact(buffer, entry.position);
        *scratch = buffer;
        result?;

        if verify_crc {
            let calculated_crc = crc(scratch);
            if calculated_crc != entry.crc {
                return Err(io::invalid_data_error(format!("Commit Log Entry CRC Check Failed (Stored: {:x} != Calculated: {calculated_crc:x}", entry.crc)));
            }
        }

        Self::deserialize_from(scratch)
    }

    fn deserialize_from(mut serialized: &[u8]) -> io::Result<Self> {
        let mut log = Self::default();
        while serialized.len() >= 10 {
            let start = GrainId(u64::from_le_bytes(serialized[..8].try_into().unwrap()));
            let count = serialized[8];
            serialized = &serialized[9..];
            let operation = GrainOperation::deserialize_from(&mut serialized)?;
            log.grain_changes.push(GrainChange {
                operation,
                start,
                count,
            });
        }

        if serialized.is_empty() {
            Ok(log)
        } else {
            Err(io::invalid_data_error("extra trailing bytes"))
        }
    }
}

/// A single change to a sequence of grains.
#[derive(Debug, Clone, Copy)]
pub struct GrainChange {
    /// The operation that happened to this grain.
    pub operation: GrainOperation,
    /// The starting grain ID of the operation.
    pub start: GrainId,
    /// The number of sequential grains affected by this operation.
    pub count: u8,
}

/// The operation being performed on the grain.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GrainOperation {
    /// The grain transitioned from Free to Allocated.
    Allocate { crc: u32 },
    /// The grain transitioned from Allocated to Archived.
    Archive,
    /// The grain transitioned from Archived to Free,
    Free,
}

impl GrainOperation {
    pub fn serialize_into<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            GrainOperation::Allocate { crc } => {
                let mut bytes = [0, 0, 0, 0, 0];
                bytes[1..].copy_from_slice(&crc.to_le_bytes());
                writer.write_all(&bytes)
            }
            GrainOperation::Archive => writer.write_all(&[1]),
            GrainOperation::Free => writer.write_all(&[2]),
        }
    }

    pub fn deserialize_from(bytes: &mut &[u8]) -> io::Result<Self> {
        let (result, bytes_read) = match bytes.get(0) {
            Some(0) => {
                if bytes.len() < 5 {
                    return Err(io::invalid_data_error("crc not found"));
                }
                let crc = u32::from_le_bytes(bytes[1..5].try_into().expect("u32 is 4 bytes"));
                (Self::Allocate { crc }, 5)
            }
            Some(1) => (Self::Archive, 1),
            Some(2) => (Self::Free, 1),
            Some(_) => return Err(io::invalid_data_error("invalid value for GrainOperation")),
            None => return Err(std::io::Error::from(ErrorKind::UnexpectedEof)),
        };

        *bytes = &bytes[bytes_read..];

        Ok(result)
    }
}
