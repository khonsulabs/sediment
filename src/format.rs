use std::fmt::{Display, Write as _};
use std::io::{BufWriter, Read, Seek, Write};
use std::ops::{AddAssign, Deref, DerefMut};
use std::str::FromStr;

use crc32c::crc32c;
use okaywal::EntryId;

use crate::commit_log::CommitLogEntry;
use crate::{Error, Result};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct GrainId(u64);

impl GrainId {
    pub const NONE: Self = Self(u64::MAX);

    pub const fn new(basin: BasinId, stratum: StratumId, id: LocalGrainId) -> Self {
        Self((basin.0 as u64) << 61 | stratum.0 << 20 | id.0)
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let value = u64::from_be_bytes(bytes.try_into().ok()?);
        if value != u64::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub const fn basin_id(self) -> BasinId {
        BasinId((self.0 >> 61) as u8)
    }

    pub fn local_grain_id(self) -> LocalGrainId {
        LocalGrainId(self.0 & 0xF_FFFF)
    }

    pub const fn local_grain_index(self) -> GrainIndex {
        GrainIndex(((self.0 >> 6) & 0x3FFF) as u16)
    }

    pub const fn grain_count(self) -> u8 {
        (self.0 & 0x3f) as u8
    }

    pub const fn basin_and_stratum(self) -> BasinAndStratum {
        BasinAndStratum(self.0 >> 20)
    }

    pub const fn stratum_id(self) -> StratumId {
        StratumId((self.0 >> 20) & 0x1ff_ffff_ffff)
    }

    pub(crate) const fn file_position(self) -> u64 {
        let grain_size = self.basin_id().grain_stripe_bytes() as u64;
        let index = self.local_grain_index().as_u16() as u64;
        let header_size = StratumHeader::BYTES * 2;

        header_size + index * grain_size
    }
}

impl FromStr for GrainId {
    type Err = GrainIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('-');
        let basin_and_stratum = parts.next().ok_or(GrainIdError::InvalidFormat)?;
        let index = parts.next().ok_or(GrainIdError::InvalidFormat)?;
        if parts.next().is_some() || basin_and_stratum.len() < 2 {
            return Err(GrainIdError::InvalidFormat);
        }

        let basin_and_stratum = BasinAndStratum::from_str(basin_and_stratum)?;

        let index_and_count =
            u64::from_str_radix(index, 16).map_err(|_| GrainIdError::InvalidGrainIndex)?;
        let count = (index_and_count & 0x3f) as u8;
        let index = GrainIndex::new((index_and_count >> 6) as u16)
            .ok_or(GrainIdError::InvalidGrainIndex)?;
        let id = LocalGrainId::from_parts(index, count).ok_or(GrainIdError::InvalidGrainIndex)?;

        Ok(Self::new(
            basin_and_stratum.basin(),
            basin_and_stratum.stratum(),
            id,
        ))
    }
}

impl std::fmt::Debug for GrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let basin_id = self.basin_id();
        let stratum_id = self.stratum_id();
        let local_index = self.local_grain_index();
        let count = self.grain_count();
        f.debug_struct("GrainId")
            .field("basin", &basin_id.0)
            .field("stratum", &stratum_id.0)
            .field("index", &local_index.0)
            .field("count", &count)
            .finish()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum GrainIdError {
    InvalidFormat,
    InvalidBasinId,
    InvalidStratum,
    InvalidGrainIndex,
    InvalidGrainCount,
}

impl Display for GrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let basin_and_stratum = self.basin_and_stratum();
        let local_index = self.local_grain_id();
        write!(f, "{basin_and_stratum}-{local_index}")
    }
}

#[test]
fn grain_id_strings() {
    let zero = GrainId(0);
    assert_eq!(zero.to_string(), "00-0");
    let none = GrainId::NONE;
    assert_eq!(none.to_string(), "71ffffffffff-fffff");
    assert_eq!(
        GrainId::from_str("71ffffffffff-fffff").unwrap(),
        GrainId::NONE
    );
    assert!(GrainId::from_str("72fffffffffff-fffff").is_err());
    assert!(GrainId::from_str("71fffffffffff-1fffff").is_err());
    assert!(GrainId::from_str("81fffffffffff-3fff").is_err());
    assert!(GrainId::from_str("---").is_err());
    assert!(GrainId::from_str("71ffffffffff-FFFFFFFFFFFFFFFFF").is_err());
    assert!(GrainId::from_str("0FFFFFFFFFFFFFFFFF-3fff").is_err());
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct StratumId(u64);

impl StratumId {
    pub const fn new(id: u64) -> Option<Self> {
        if id < 2_u64.pow(45) {
            Some(Self(id))
        } else {
            None
        }
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for StratumId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:0x}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Default)]
pub struct BasinId(u8);

impl BasinId {
    pub const MAX: Self = BasinId(7);
    pub const MIN: Self = BasinId(0);

    pub const fn new(id: u8) -> Option<Self> {
        if id < 8 {
            Some(Self(id))
        } else {
            None
        }
    }

    pub fn to_char(self) -> char {
        (b'0' + self.0) as char
    }

    pub fn from_char(ch: char) -> Option<Self> {
        if ('0'..='7').contains(&ch) {
            Some(Self(ch as u8 - b'0'))
        } else {
            None
        }
    }

    pub const fn index(self) -> u8 {
        self.0
    }

    pub const fn next(self) -> Option<Self> {
        Self::new(self.0 + 1)
    }
}

impl Display for BasinId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char(self.to_char())
    }
}

impl BasinId {
    pub const fn grain_stripe_bytes(self) -> u32 {
        match self.0 {
            0 => 2_u32.pow(5),
            1 => 2_u32.pow(8),
            2 => 2_u32.pow(12),
            3 => 2_u32.pow(16),
            4 => 2_u32.pow(20),
            5 => 2_u32.pow(24),
            6 => 2_u32.pow(28),
            7 => 2_u32.pow(31),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct BasinAndStratum(u64);

impl BasinAndStratum {
    pub const fn from_parts(basin: BasinId, stratum: StratumId) -> Self {
        Self((basin.0 as u64) << 41 | stratum.0)
    }

    pub fn basin(self) -> BasinId {
        BasinId((self.0 >> 41) as u8)
    }

    pub fn stratum(self) -> StratumId {
        StratumId(self.0 & 0x1ff_ffff_ffff)
    }
}

impl Display for BasinAndStratum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let basin = self.basin();
        let stratum = self.stratum();
        write!(f, "{basin}{stratum}")
    }
}

impl FromStr for BasinAndStratum {
    type Err = GrainIdError;

    fn from_str(basin_and_stratum: &str) -> Result<Self, Self::Err> {
        let (basin, stratum) = basin_and_stratum.split_at(1);
        let Some(basin) = BasinId::from_char(basin.as_bytes()[0] as char)
            else { return Err(GrainIdError::InvalidBasinId) };

        let stratum = u64::from_str_radix(stratum, 16).map_err(|_| GrainIdError::InvalidStratum)?;
        let stratum = StratumId::new(stratum).ok_or(GrainIdError::InvalidStratum)?;

        Ok(Self(u64::from(basin.0) << 41 | stratum.0))
    }
}

#[test]
fn basin_id_encoding() {
    for (ch, value) in ('0'..='7').zip(0..=7) {
        let expected = BasinId(value);
        assert_eq!(BasinId::from_char(ch), Some(expected));
        assert_eq!(expected.to_char(), ch);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GrainIndex(u16);

impl GrainIndex {
    pub const fn new(id: u16) -> Option<Self> {
        if id < 2_u16.pow(14) {
            Some(Self(id))
        } else {
            None
        }
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

impl AddAssign<u8> for GrainIndex {
    fn add_assign(&mut self, rhs: u8) {
        self.0 += u16::from(rhs);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalGrainId(u64);

impl LocalGrainId {
    pub const fn from_parts(index: GrainIndex, grain_count: u8) -> Option<Self> {
        if grain_count < 64 {
            Some(Self((index.0 as u64) << 6 | grain_count as u64))
        } else {
            None
        }
    }

    pub const fn grain_index(self) -> GrainIndex {
        GrainIndex((self.0 >> 6) as u16)
    }

    pub const fn grain_count(self) -> u8 {
        (self.0 & 0x3f) as u8
    }
}

impl Display for LocalGrainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:0x}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Default)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub const fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl From<u64> for TransactionId {
    fn from(id: u64) -> Self {
        TransactionId(id)
    }
}

impl From<TransactionId> for u64 {
    fn from(id: TransactionId) -> Self {
        id.0
    }
}

impl From<EntryId> for TransactionId {
    fn from(id: EntryId) -> Self {
        Self(id.0)
    }
}

impl From<TransactionId> for EntryId {
    fn from(tx_id: TransactionId) -> Self {
        EntryId(tx_id.0)
    }
}

impl PartialEq<u64> for TransactionId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<EntryId> for TransactionId {
    fn eq(&self, other: &EntryId) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd<EntryId> for TransactionId {
    fn partial_cmp(&self, other: &EntryId) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

pub enum FileHeader<T> {
    Both(T, T),
    First(T),
    Second(T),
}

impl<T> FileHeader<T>
where
    T: Duplicable,
{
    pub fn read_from<R: Read + Seek>(mut file: R, scratch: &mut Vec<u8>) -> Result<Self> {
        let first_header = T::read_from(&mut file, scratch);
        if first_header.is_err() {
            file.seek(std::io::SeekFrom::Start(T::BYTES))?;
        }
        let second_header = T::read_from(&mut file, scratch);
        match (first_header, second_header) {
            (Ok(first_header), Ok(second_header)) => Ok(Self::Both(first_header, second_header)),
            (Err(err), Err(_)) => Err(err),
            (Ok(first_header), Err(_)) => Ok(Self::First(first_header)),
            (Err(_), Ok(second_header)) => Ok(Self::Second(second_header)),
        }
    }

    pub fn as_options(&self) -> (Option<&T>, Option<&T>) {
        match self {
            FileHeader::Both(first, second) => (Some(first), Some(second)),
            FileHeader::First(first) => (Some(first), None),
            FileHeader::Second(second) => (None, Some(second)),
        }
    }

    pub fn into_first(self) -> T {
        match self {
            FileHeader::Both(first, _) | FileHeader::First(first) => first,
            FileHeader::Second(_) => unreachable!("did not contain a valid first"),
        }
    }

    pub fn into_second(self) -> T {
        match self {
            FileHeader::Both(_, second) | FileHeader::Second(second) => second,
            FileHeader::First(_) => unreachable!("did not contain a valid second"),
        }
    }
}

pub trait Duplicable: Sized {
    const BYTES: u64;

    fn read_from<R: Read>(reader: R, scratch: &mut Vec<u8>) -> Result<Self>;
    fn write_to<W: Write>(&mut self, writer: W) -> Result<()>;
}

/// A header inside of an "Index" file.
///
/// This data structure is serialized as:
///
/// - `transaction_id`: 8 bytes
/// - `embedded_header_data`: 8 bytes
/// - `commit_log_head`: 8 bytes
/// - `checkpoint_target`: 8 bytes
/// - `checkpointed_to`: 8 bytes
/// - `basin_strata_count`: 8 x 6 bytes (48 bytes).
/// - `crc32`: 4 bytes (checksum of previous 88 bytes)
///
/// The total header length is 36 bytes.
///
/// # About the Index file
///
/// Index files are the root of a Sediment database. The header is responsible
/// for pointing to several key pieces of data, which will be stored within the
/// other files.
///
/// The Index file is serialized in this fashion:
///
/// - Magic code + version (4 bytes)
/// - [`IndexHeader`]
/// - [`IndexHeader`]
///
/// The record with the highest transaction id should be checked upon recovery
/// to ensure that `embedded_header_data` is written with the same
/// [`TransactionId`].
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct IndexHeader {
    pub transaction_id: TransactionId,
    pub embedded_header_data: Option<GrainId>,
    pub commit_log_head: Option<GrainId>,
    pub checkpoint_target: TransactionId,
    pub checkpointed_to: TransactionId,
    pub basin_strata_count: [u64; 8],
    pub crc32: u32,
}

impl Duplicable for IndexHeader {
    const BYTES: u64 = 92;

    fn read_from<R: Read>(mut file: R, scratch: &mut Vec<u8>) -> Result<Self> {
        scratch.resize(Self::BYTES as usize, 0);
        file.read_exact(scratch)?;
        let crc32 = u32::from_be_bytes(scratch[88..].try_into().expect("u32 is 4 bytes"));
        let computed_crc = crc32c(&scratch[..88]);
        if crc32 != computed_crc {
            return Err(Error::ChecksumFailed);
        }

        let (transaction_bytes, remaining) = scratch.split_at(8);
        let transaction_id = TransactionId(u64::from_be_bytes(
            transaction_bytes.try_into().expect("u64 is 8 bytes"),
        ));
        let (embedded_header_bytes, remaining) = remaining.split_at(8);
        let embedded_header_data = GrainId::from_bytes(embedded_header_bytes);
        let (commit_log_head_bytes, remaining) = remaining.split_at(8);
        let commit_log_head = GrainId::from_bytes(commit_log_head_bytes);
        let (checkpoint_target_bytes, remaining) = remaining.split_at(8);
        let checkpoint_target = TransactionId(u64::from_be_bytes(
            checkpoint_target_bytes.try_into().expect("u64 is 8 bytes"),
        ));
        let (checkpointed_to_bytes, mut remaining) = remaining.split_at(8);
        let checkpointed_to = TransactionId(u64::from_be_bytes(
            checkpointed_to_bytes.try_into().expect("u64 is 8 bytes"),
        ));
        let mut basin_strata_count = [0; 8];
        for count in &mut basin_strata_count {
            let mut padded_bytes = [0; 8];
            padded_bytes[2..].copy_from_slice(&remaining[..6]);
            remaining = &remaining[6..];
            *count = u64::from_be_bytes(padded_bytes);
        }

        Ok(Self {
            transaction_id,
            embedded_header_data,
            commit_log_head,
            checkpoint_target,
            checkpointed_to,
            basin_strata_count,
            crc32,
        })
    }

    fn write_to<W: std::io::Write>(&mut self, writer: W) -> Result<()> {
        let mut writer = ChecksumWriter::new(writer);
        writer.write_all(&self.transaction_id.to_be_bytes())?;
        writer.write_all(
            &self
                .embedded_header_data
                .unwrap_or(GrainId::NONE)
                .0
                .to_be_bytes(),
        )?;
        writer.write_all(
            &self
                .commit_log_head
                .unwrap_or(GrainId::NONE)
                .0
                .to_be_bytes(),
        )?;
        writer.write_all(&self.checkpoint_target.to_be_bytes())?;
        writer.write_all(&self.checkpointed_to.to_be_bytes())?;
        for count in &self.basin_strata_count {
            writer.write_all(&count.to_be_bytes()[2..])?;
        }
        let (_, crc32) = writer.write_crc32_and_finish()?;
        self.crc32 = crc32;

        Ok(())
    }
}

/// Each Stratum header is 16kb, and describes the state of allocation of each
/// grain within the Stratum.
///
/// It is serialized as:
///
/// - [`TransactionId`]: 8 bytes
/// - [`GrainAllocationInfo`]: 16,372 one-byte entries
/// - CRC32: 4 bytes
///
/// The grain size is determined by the name of the file that contains the
/// header.
///
/// # About Statum files
///
/// Strata contain the data written to the Sediment database.
///
/// The header consists of two [`StratumHeader`]s serialized one after another.
/// The header with the latest [`TransactionId`] is considered the current
/// record. When updating the header, the inactive copy should be overwritten.
///
/// If an aborted write is detected and a rollback needs to happen, the rolled
/// back header should be overwritten with a second copy of the previous
/// version.
///
/// Directly after the two [`StratumHeader`]s is a tightly packed list of
/// grains. Each grain is serialized as:
///
/// - [`TransactionId`]: 8 bytes
/// - Data Length: 4 bytes
/// - Grain Data: The contiguous data stored within the grain.
/// - CRC32: The CRC of the [`TransactionId`] and the grain data.
///
/// Strata are grouped together to form a Basin. In each Basin, the grain stripe
/// size is always the same. The Basin's grain size is determined by the name of
/// the Stratum file. The first character is a single Base32 character whose
/// value is the exponent of the grain size equation: `2^(grain_exponent)`.
/// Because each piece of data must have 16 extra bytes allocated to it, the
/// smallest usable grain exponent is 5 (`F`).
///
/// To find the data associated with a grain, its local grain index must be
/// computed. Because each Stratum can contain a maximum of 16,372 grains, the
/// remaining characters in a Stratum's file name is a hexadecimal
/// representation of the top 50 bits of a `GrainId` in big endian. The
/// remaining 14 bits contain the local grain index.
///
/// The offset of a local grain index is `32kb + local_grain_index *
/// grain_size`. Because grains can be stored in stripes of up to 64 consecutive
/// grains, not every local grain index will point to the start of a grain
/// record. The [`StratumHeader`] must be used to determine if a given local
/// grain index is valid before trusting the data stored.
#[derive(Debug)]
pub struct StratumHeader {
    pub transaction_id: TransactionId,
    pub grains: [u8; 16372],
    pub crc32: u32,
}

impl StratumHeader {
    pub const fn grain_info(&self, index: usize) -> GrainAllocationInfo {
        GrainAllocationInfo(self.grains[index])
    }

    pub fn reflects_changes_from(&self, commit_log: &CommitLogEntry) -> bool {
        let new_grains = commit_log
            .new_grains
            .iter()
            .map(|new_grain| (GrainAllocationStatus::Allocated, new_grain.id));
        let archived_grains = commit_log
            .archived_grains
            .iter()
            .map(|grain| (GrainAllocationStatus::Archived, *grain));
        let freed_grains = commit_log
            .freed_grains
            .iter()
            .map(|grain| (GrainAllocationStatus::Free, *grain));
        for (expected_status, grain_id) in new_grains.chain(archived_grains).chain(freed_grains) {
            let start = usize::from(grain_id.local_grain_index().as_u16());
            let mut expected_count = grain_id.grain_count();
            for info in self
                .grains
                .iter()
                .skip(start)
                .take(usize::from(expected_count))
            {
                let info = GrainAllocationInfo(*info);

                let matches = if info.status() == Some(expected_status) {
                    if expected_status == GrainAllocationStatus::Free {
                        info.count() == 0
                    } else {
                        info.count() == expected_count
                    }
                } else {
                    false
                };

                if !matches {
                    return false;
                }

                expected_count -= 1;
            }
        }

        true
    }
}

impl Duplicable for StratumHeader {
    const BYTES: u64 = 16_384;

    fn read_from<R: Read>(mut file: R, scratch: &mut Vec<u8>) -> Result<Self> {
        scratch.resize(16_384, 0);
        file.read_exact(scratch)?;

        let mut grains = [0; 16_372];

        let crc32 = u32::from_be_bytes(scratch[16_380..].try_into().expect("u32 is 4 bytes"));
        let computed_crc = crc32c(&scratch[..16_380]);
        if crc32 != computed_crc {
            if scratch.iter().all(|b| b == &0) {
                return Ok(Self {
                    transaction_id: TransactionId::default(),
                    grains,
                    crc32: 0,
                });
            }

            return Err(Error::ChecksumFailed);
        }

        let transaction_id = TransactionId(u64::from_be_bytes(
            scratch[..8].try_into().expect("u64 is 8 bytes"),
        ));

        grains.copy_from_slice(&scratch[8..16_372 + 8]);

        Ok(Self {
            transaction_id,
            grains,
            crc32,
        })
    }

    fn write_to<W: std::io::Write>(&mut self, writer: W) -> Result<()> {
        let mut writer = ChecksumWriter::new(BufWriter::new(writer));
        writer.write_all(&self.transaction_id.to_be_bytes())?;
        writer.write_all(&self.grains)?;
        self.crc32 = writer.crc32();
        writer.write_all(&self.crc32.to_be_bytes())?;

        writer.flush()?;

        Ok(())
    }
}

impl Default for StratumHeader {
    fn default() -> Self {
        Self {
            transaction_id: Default::default(),
            grains: [0; 16372],
            crc32: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
#[repr(transparent)]
pub struct GrainAllocationInfo(pub u8);

impl GrainAllocationInfo {
    pub const fn allocated(count: u8) -> Self {
        assert!(count < 64);
        Self((1 << 6) | count)
    }

    pub const fn archived(count: u8) -> Self {
        assert!(count < 64);
        Self((2 << 6) | count)
    }

    pub fn status(self) -> Option<GrainAllocationStatus> {
        match self.0 >> 6 {
            0 => Some(GrainAllocationStatus::Free),
            1 => Some(GrainAllocationStatus::Allocated),
            2 => Some(GrainAllocationStatus::Archived),
            _ => None,
        }
    }

    pub fn count(self) -> u8 {
        self.0 & 0b0011_1111
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Copy)]
pub enum GrainAllocationStatus {
    Allocated,
    Archived,
    Free,
}

pub trait ByteUtil {
    fn to_be_bytes(&self) -> [u8; 8];
    fn from_be_bytes(bytes: [u8; 8]) -> Self;
}

macro_rules! impl_bytes_for {
    ($type:ident) => {
        impl ByteUtil for $type {
            fn to_be_bytes(&self) -> [u8; 8] {
                self.0.to_be_bytes()
            }

            fn from_be_bytes(bytes: [u8; 8]) -> Self {
                Self(u64::from_be_bytes(bytes))
            }
        }
    };
}

impl_bytes_for!(GrainId);

impl ByteUtil for Option<GrainId> {
    fn to_be_bytes(&self) -> [u8; 8] {
        self.unwrap_or(GrainId::NONE).to_be_bytes()
    }

    fn from_be_bytes(bytes: [u8; 8]) -> Self {
        let id = GrainId::from_be_bytes(bytes);
        if id != GrainId::NONE {
            Some(id)
        } else {
            None
        }
    }
}

pub struct ChecksumWriter<W> {
    writer: W,
    crc32: u32,
}

impl<W> ChecksumWriter<W>
where
    W: Write,
{
    pub fn new(writer: W) -> Self {
        Self { writer, crc32: 0 }
    }

    pub fn crc32(&self) -> u32 {
        self.crc32
    }

    pub fn write_crc32_and_finish(mut self) -> Result<(W, u32)> {
        self.write_all(&self.crc32.to_be_bytes())?;
        Ok((self.writer, self.crc32))
    }
}

impl<W> Write for ChecksumWriter<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.writer.write(buf)?;
        if bytes_written > 0 {
            self.crc32 = crc32c::crc32c_append(self.crc32, &buf[..bytes_written]);
        }
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
pub struct Stored<T> {
    pub grain_id: GrainId,
    pub stored: T,
}

impl<T> Deref for Stored<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.stored
    }
}

impl<T> DerefMut for Stored<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stored
    }
}
