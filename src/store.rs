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

            let (mut first_is_active, mut active, older) = match file_header {
                FileHeader::Both(first, second) => {
                    if first.transaction_id > second.transaction_id {
                        (true, first, Some(second))
                    } else {
                        (false, second, Some(first))
                    }
                }
                FileHeader::First(first) => (true, first, None),
                FileHeader::Second(second) => (false, second, None),
            };

            let mut strata_to_clean = None;
            let commit_log = match (BasinMap::verify(&active, &mut discovered_strata), older) {
                (Ok(commit_log), _) => commit_log,
                (Err(_), Some(older)) => {
                    let commit_log = BasinMap::verify(&older, &mut discovered_strata)?;

                    active = older;
                    first_is_active = !first_is_active;

                    let mut invalid_strata = Vec::new();
                    for (id, stratum) in &discovered_strata {
                        if stratum.should_exist(&active)
                            && stratum.needs_cleanup(commit_log.as_ref())
                        {
                            invalid_strata.push(*id);
                        }
                    }
                    strata_to_clean = Some(invalid_strata);
                    commit_log
                }
                (Err(err), None) => return Err(err),
            };

            let mut basins =
                BasinMap::load_from(&active, commit_log.as_ref(), discovered_strata, path)?;

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
                Err(Error::verification_failed("existing strata found without an index file. If this is intentional, clean the directory being used for the database."))
            }
        }
    }

    pub fn write_header(&mut self, transaction_id: TransactionId, sync: &FSyncBatch) -> Result<()> {
        self.index.active.transaction_id = transaction_id;
        for (basin, count) in (&self.basins)
            .into_iter()
            .zip(&mut self.index.active.basin_strata_count)
        {
            *count = basin.1.stratum.len() as u64;
        }
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

    pub fn validate(&self, commit_log: &CommitLogEntry) -> Result<()> {
        let (first, second) = self.validate_headers(Some(commit_log));

        if first.is_some() || second.is_some() {
            Ok(())
        } else {
            Err(Error::verification_failed("neither header is valid"))
        }
    }

    pub fn should_exist(&self, index: &IndexHeader) -> bool {
        self.id.stratum().as_u64() < index.basin_strata_count[usize::from(self.id.basin().index())]
    }

    pub fn needs_cleanup(&self, commit_log: Option<&CommitLogEntry>) -> bool {
        !matches!(self.validate_headers(commit_log), (Some(_), Some(_)))
    }

    fn validate_headers(
        &self,
        commit_log: Option<&CommitLogEntry>,
    ) -> (Option<&StratumHeader>, Option<&StratumHeader>) {
        fn is_valid(
            header: &StratumHeader,
            commit_transaction: TransactionId,
            commit_log: Option<&CommitLogEntry>,
        ) -> bool {
            header.transaction_id < commit_transaction
                || (header.transaction_id == commit_transaction
                    && commit_log
                        .map_or(true, |commit_log| header.reflects_changes_from(commit_log)))
        }
        let commit_transaction = commit_log.map_or_else(TransactionId::default, |commit_log| {
            commit_log.transaction_id
        });
        let (first, second) = self.header.as_options();
        let first = first
            .and_then(|first| is_valid(first, commit_transaction, commit_log).then_some(first));
        let second = second
            .and_then(|second| is_valid(second, commit_transaction, commit_log).then_some(second));

        (first, second)
    }
}

impl BasinMap<BasinState> {
    pub fn verify(
        index: &IndexHeader,
        discovered_strata: &mut BTreeMap<BasinAndStratum, UnverifiedStratum>,
    ) -> Result<Option<CommitLogEntry>> {
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

                for stratum in discovered_strata.values() {
                    if stratum.should_exist(index) {
                        stratum.validate(&commit_log_entry)?;
                    }
                }
                return Ok(Some(commit_log_entry));
            } else {
                return Err(Error::verification_failed("commit log stratum not found"));
            }
        }

        Ok(None)
    }

    pub fn load_from(
        index: &IndexHeader,
        commit_log: Option<&CommitLogEntry>,
        discovered_strata: BTreeMap<BasinAndStratum, UnverifiedStratum>,
        directory: &Path,
    ) -> Result<Self> {
        let mut basins = Self::new();
        for stratum in discovered_strata.into_values() {
            if !stratum.should_exist(index) {
                std::fs::remove_file(directory.join(stratum.id.to_string()))?;
                continue;
            }

            let header = match stratum.validate_headers(commit_log) {
                (Some(first), Some(second)) => {
                    if first.transaction_id >= second.transaction_id {
                        Duplicated {
                            active: stratum.header.into_first(),
                            first_is_active: true,
                        }
                    } else {
                        Duplicated {
                            active: stratum.header.into_second(),
                            first_is_active: false,
                        }
                    }
                }
                (Some(_), _) => Duplicated {
                    active: stratum.header.into_first(),
                    first_is_active: true,
                },
                (_, Some(_)) => Duplicated {
                    active: stratum.header.into_second(),
                    first_is_active: false,
                },
                (None, None) => {
                    unreachable!("error is handled in validation phase")
                }
            };

            let basin = basins.get_or_insert_with(stratum.id.basin(), || {
                BasinState::default_for(stratum.id.basin())
            });
            if stratum.id.stratum().as_usize() != basin.stratum.len() {
                return Err(Error::verification_failed("strata are non-sequential"));
            }

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
        return Err(Error::verification_failed(
            "new grain was written in a different transaction",
        ));
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
