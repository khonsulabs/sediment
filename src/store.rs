use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use crc32c::crc32c;

use crate::basinmap::BasinMap;
use crate::commit_log::CommitLogEntry;
use crate::format::{
    BasinAndStratum, BasinId, Duplicable, FileHeader, GrainId, IndexHeader, StratumHeader,
    StratumId, TransactionId,
};
use crate::fsync::{FSyncBatch, FSyncManager};
use crate::util::u32_to_usize;
use crate::{Error, Result};

#[derive(Debug)]
pub struct Store {
    pub directory: Arc<PathBuf>,
    disk_state: Mutex<DiskState>,
    pub syncer: FSyncManager,
}

impl Store {
    pub fn recover(path: &Path) -> Result<Self> {
        let disk_state = DiskState::recover(path)?;
        Ok(Self {
            directory: Arc::new(path.to_path_buf()),
            disk_state: Mutex::new(disk_state),
            syncer: FSyncManager::default(),
        })
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, DiskState>> {
        Ok(self.disk_state.lock()?)
    }
}

#[derive(Debug)]
pub struct DiskState {
    pub needs_directory_sync: bool,
    pub directory: File,
    pub index: Duplicated<IndexHeader>,
    pub index_writer: File,
    pub basins: BasinMap<BasinState>,
}

impl DiskState {
    pub fn recover(path: &Path) -> Result<Self> {
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let directory = OpenOptions::new().read(true).open(path)?;

        let index_path = path.join("index");

        let mut scratch = Vec::new();
        let mut discovered_strata = discover_strata(path, &mut scratch)?;

        if index_path.exists() {
            let mut index_writer = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&index_path)?;

            let file_header =
                FileHeader::<IndexHeader>::read_from(&mut index_writer, &mut scratch)?;

            // TODO After the commit log is implemented, verify the final commit
            // or roll back if a partial write is detected.

            let (mut first_is_active, mut active, older) =
                match (file_header.first, file_header.second) {
                    (Some(first), Some(second)) => {
                        if first.transaction_id > second.transaction_id {
                            (true, first, Some(second))
                        } else {
                            (false, second, Some(first))
                        }
                    }
                    (Some(first), None) => (true, first, None),
                    (None, Some(second)) => (false, second, None),
                    (None, None) => unreachable!("FileHeader will return an error"),
                };

            let mut strata_to_clean = None;
            match (BasinMap::verify(&active, &mut discovered_strata), older) {
                (Ok(_), _) => {}
                (Err(_), Some(older)) => {
                    BasinMap::verify(&older, &mut discovered_strata)?;
                    active = older;
                    first_is_active = !first_is_active;
                    let mut invalid_strata = Vec::new();
                    for (id, stratum) in &discovered_strata {
                        if stratum.validate(active.transaction_id).is_err() {
                            invalid_strata.push(*id);
                        }
                    }
                    strata_to_clean = Some(invalid_strata);
                }
                (Err(err), None) => return Err(err),
            };

            let mut basins = BasinMap::load_from(&active, discovered_strata, path)?;

            let mut index = Duplicated {
                active,
                first_is_active,
            };

            if let Some(strata_to_clean) = strata_to_clean {
                for id in strata_to_clean {
                    let basin = basins[id.basin()].as_mut().expect("just loaded");
                    let stratum = &mut basin.stratum[id.stratum().as_usize()];
                    stratum
                        .header
                        .write_to(stratum.file.as_mut().expect("just loaded"))?;
                }

                index.write_to(&mut index_writer)?;
                index_writer.sync_data()?;
            }

            Ok(Self {
                needs_directory_sync: false,
                directory,
                index,
                index_writer,
                basins,
            })
        } else {
            let mut index_writer = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&index_path)?;

            let mut empty_header = IndexHeader::default();
            empty_header.write_to(&mut index_writer)?;
            empty_header.write_to(&mut index_writer)?;

            // Ensure the file is fully persisted to disk.
            index_writer.sync_all()?;
            directory.sync_all()?;

            if discovered_strata.is_empty() {
                Ok(Self {
                    needs_directory_sync: false,
                    directory,
                    index: Duplicated {
                        active: empty_header,
                        first_is_active: false,
                    },
                    index_writer,
                    basins: BasinMap::new(),
                })
            } else {
                todo!("error: existing strata found without a valid index")
            }
        }
    }

    pub fn write_header(&mut self, transaction_id: TransactionId, sync: &FSyncBatch) -> Result<()> {
        self.index.active.transaction_id = transaction_id;
        self.index.write_to(&mut self.index_writer)?;

        sync.queue_fsync_data(self.index_writer.try_clone()?)?;

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Duplicated<T> {
    pub active: T,
    pub first_is_active: bool,
}

impl<T> Duplicated<T>
where
    T: Duplicable,
{
    pub fn write_to(&mut self, file: &mut File) -> Result<()> {
        let offset = if self.first_is_active { T::BYTES } else { 0 };

        file.seek(io::SeekFrom::Start(offset))?;
        self.active.write_to(file)?;
        self.first_is_active = !self.first_is_active;

        Ok(())
    }
}

#[derive(Debug)]
pub struct BasinState {
    pub id: BasinId,
    pub stratum: Vec<StratumState>,
}

impl BasinState {
    pub fn default_for(id: BasinId) -> Self {
        Self {
            id,
            stratum: Vec::new(),
        }
    }

    pub fn get_or_allocate_stratum(
        &mut self,
        id: StratumId,
        directory: &Path,
    ) -> &mut StratumState {
        while id.as_usize() >= self.stratum.len() {
            let new_id =
                StratumId::new(u64::try_from(self.stratum.len()).expect("too large of a database"))
                    .expect("invalid id");
            self.stratum.push(StratumState::default_for(
                directory.join(format!("{}{}", self.id, new_id)),
            ))
        }

        &mut self.stratum[id.as_usize()]
    }
}

fn discover_strata(
    path: &Path,
    scratch: &mut Vec<u8>,
) -> Result<BTreeMap<BasinAndStratum, UnverifiedStratum>> {
    let mut discovered = BTreeMap::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(basin_and_stratum) = BasinAndStratum::from_str(name) {
                discovered.insert(
                    basin_and_stratum,
                    UnverifiedStratum::read_from(&entry.path(), basin_and_stratum, scratch)?,
                );
            }
        }
    }

    Ok(discovered)
}

#[derive(Debug)]
pub struct StratumState {
    pub path: Arc<PathBuf>,
    pub header: Duplicated<StratumHeader>,
    pub file: Option<File>,
}

impl StratumState {
    fn default_for(path: PathBuf) -> Self {
        Self {
            path: Arc::new(path),
            header: Duplicated::default(),
            file: None,
        }
    }

    pub fn get_or_open_file(&mut self, needs_directory_sync: &mut bool) -> Result<&mut File> {
        if self.file.is_none() {
            // If this file doesn't exist, we need to do a directory sync to
            // ensure the file is persisted.
            *needs_directory_sync |= !self.path.exists();

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(self.path.as_ref())?;

            self.file = Some(file);
        }

        Ok(self.file.as_mut().expect("file always allocated above"))
    }

    pub fn write_header(
        &mut self,
        new_transaction_id: TransactionId,
        sync_batch: &FSyncBatch,
    ) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .expect("shouldn't ever write a file header if no data was written");

        self.header.active.transaction_id = new_transaction_id;
        self.header.write_to(file)?;

        let file_to_sync = file.try_clone()?;
        sync_batch.queue_fsync_data(file_to_sync)?;

        Ok(())
    }
}

pub struct UnverifiedStratum {
    pub id: BasinAndStratum,
    pub header: FileHeader<StratumHeader>,
    pub file: File,
}

impl UnverifiedStratum {
    pub fn read_from(path: &Path, id: BasinAndStratum, scratch: &mut Vec<u8>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let header = FileHeader::read_from(&mut file, scratch)?;
        Ok(Self { id, header, file })
    }

    pub fn validate(&self, transaction: TransactionId) -> Result<()> {
        match (&self.header.first, &self.header.second) {
            (Some(first), Some(second)) => {
                let first_is_valid = first.transaction_id <= transaction;
                let second_is_valid = second.transaction_id <= transaction;
                let first_is_newest = first_is_valid
                    && (first.transaction_id > second.transaction_id || !second_is_valid);
                let second_is_newest = second_is_valid
                    && (second.transaction_id > first.transaction_id || !first_is_valid);
                if (first_is_newest && first_is_valid) || (second_is_newest && second_is_valid) {
                    Ok(())
                } else {
                    Err(Error::VerificationFailed)
                }
            }
            (Some(active), _) | (_, Some(active)) if active.transaction_id <= transaction => Ok(()),
            _ => Err(Error::VerificationFailed),
        }
    }
}

impl BasinMap<BasinState> {
    pub fn verify(
        index: &IndexHeader,
        discovered_strata: &mut BTreeMap<BasinAndStratum, UnverifiedStratum>,
    ) -> Result<()> {
        for stratum in discovered_strata.values() {
            stratum.validate(index.transaction_id)?;
        }

        let mut scratch = Vec::new();
        if let Some(commit_log_head) = index.commit_log_head {
            if let Some(stratum) = discovered_strata.get_mut(&commit_log_head.basin_and_stratum()) {
                let mut reader = BufReader::new(&mut stratum.file);
                verify_read_grain(
                    commit_log_head,
                    &mut reader,
                    index.transaction_id,
                    None,
                    &mut scratch,
                )?;
                let commit_log_entry = CommitLogEntry::read_from(&scratch[..])?;
                for new_grain in &commit_log_entry.new_grains {
                    verify_read_grain(
                        new_grain.id,
                        &mut reader,
                        index.transaction_id,
                        Some(new_grain.crc32),
                        &mut scratch,
                    )?;
                }
            } else {
                // Couldn't find the stratum with the commit log.
                return Err(Error::VerificationFailed);
            }
        }

        Ok(())
    }

    pub fn load_from(
        index: &IndexHeader,
        discovered_strata: BTreeMap<BasinAndStratum, UnverifiedStratum>,
        directory: &Path,
    ) -> Result<Self> {
        let mut basins = Self::new();
        for stratum in discovered_strata.into_values() {
            let header = match (stratum.header.first, stratum.header.second) {
                (Some(first), Some(second)) => {
                    // Because we've already verified this, we can trust that if
                    // the first is larger and still valid, we should use it,
                    // otherwise we can use the second.
                    let first_is_valid = first.transaction_id <= index.transaction_id;
                    let second_is_valid = second.transaction_id <= index.transaction_id;
                    let first_is_newest = first.transaction_id > second.transaction_id;
                    if (first_is_newest && first_is_valid) || !second_is_valid {
                        Duplicated {
                            active: first,
                            first_is_active: true,
                        }
                    } else {
                        Duplicated {
                            active: second,
                            first_is_active: false,
                        }
                    }
                }
                (Some(active), _) if active.transaction_id <= index.transaction_id => Duplicated {
                    active,
                    first_is_active: true,
                },
                (_, Some(active)) if active.transaction_id <= index.transaction_id => Duplicated {
                    active,
                    first_is_active: false,
                },
                _ => unreachable!("verify() is called before load_from"),
            };

            let basin = basins.get_or_insert_with(stratum.id.basin(), || {
                BasinState::default_for(stratum.id.basin())
            });
            assert_eq!(
                basin.stratum.len(),
                stratum.id.stratum().as_usize(),
                "strata are non-sequential"
            );

            basin.stratum.push(StratumState {
                path: Arc::new(directory.join(stratum.id.to_string())),
                header,
                file: Some(stratum.file),
            });
        }
        Ok(basins)
    }
}

fn verify_read_grain(
    grain: GrainId,
    file: &mut BufReader<&mut File>,
    transaction_id: TransactionId,
    expected_crc: Option<u32>,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    file.seek(io::SeekFrom::Start(grain.file_position()))?;
    let mut eight_bytes = [0; 8];
    file.read_exact(&mut eight_bytes)?;
    let grain_transaction_id = TransactionId::from_be_bytes(eight_bytes);
    if grain_transaction_id != transaction_id {
        return Err(Error::VerificationFailed);
    }

    let mut four_bytes = [0; 4];
    file.read_exact(&mut four_bytes)?;
    let length = u32::from_be_bytes(four_bytes);

    let length = u32_to_usize(length)?;
    buffer.resize(length, 0);
    file.read_exact(buffer)?;

    let computed_crc = crc32c(buffer);

    file.read_exact(&mut four_bytes)?;
    let stored_crc = u32::from_be_bytes(four_bytes);

    if computed_crc == stored_crc && expected_crc.map_or(true, |expected| expected == stored_crc) {
        Ok(())
    } else {
        Err(Error::ChecksumFailed)
    }
}
