use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use crate::{
    basinmap::BasinMap,
    format::{
        BasinAndStratum, BasinId, IndexFile, IndexHeader, StratumFileHeader, StratumHeader,
        StratumId, TransactionId,
    },
    Result,
};

#[derive(Debug)]
pub struct Store {
    pub directory: Arc<PathBuf>,
    disk_state: Mutex<DiskState>,
}

impl Store {
    pub fn recover(path: &Path) -> Result<Self> {
        let disk_state = DiskState::recover(path)?;
        Ok(Self {
            directory: Arc::new(path.to_path_buf()),
            disk_state: Mutex::new(disk_state),
        })
    }

    pub fn lock(&self) -> MutexGuard<'_, DiskState> {
        self.disk_state
            .lock()
            .map_or_else(PoisonError::into_inner, |a| a)
    }
}

#[derive(Debug)]
pub struct DiskState {
    pub index: Duplicated<IndexHeader>,
    pub index_writer: File,
    pub basins: BasinMap<BasinState>,
}

impl DiskState {
    pub fn recover(path: &Path) -> Result<Self> {
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let index_path = path.join("index");

        let mut scratch = Vec::new();
        let discovered_strata = discover_strata(path, &mut scratch)?;

        if index_path.exists() {
            let mut index_writer = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&index_path)?;

            let file_header = IndexFile::read_from(&mut index_writer, &mut scratch)?;

            // TODO After the commit log is implemented, verify the final commit
            // or roll back if a partial write is detected.

            let index = if file_header.first_header.transaction_id
                > file_header.second_header.transaction_id
            {
                Duplicated {
                    active: file_header.first_header,
                    first_is_active: true,
                }
            } else {
                Duplicated {
                    active: file_header.second_header,
                    first_is_active: false,
                }
            };

            let mut basins = BasinMap::new();
            for stratum in discovered_strata {
                let header = if stratum.header.first_header.transaction_id
                    > stratum.header.second_header.transaction_id
                {
                    Duplicated {
                        active: stratum.header.first_header,
                        first_is_active: true,
                    }
                } else {
                    Duplicated {
                        active: stratum.header.second_header,
                        first_is_active: false,
                    }
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
                    path: Arc::new(path.join(stratum.id.to_string())),
                    header,
                    file: Some(stratum.file),
                });
            }

            Ok(Self {
                index,
                index_writer,
                basins,
            })
        } else {
            // TODO fsync directory
            let mut index_writer = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&index_path)?;

            let mut empty_header = IndexHeader::default();
            empty_header.write_to(&mut index_writer)?;
            empty_header.write_to(&mut index_writer)?;

            if discovered_strata.is_empty() {
                Ok(Self {
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

    pub fn write_header(&mut self, transaction_id: TransactionId) -> Result<()> {
        self.index.active.transaction_id = transaction_id;
        self.index.write_to(&mut self.index_writer)?;

        // TODO this should be backgrounded.
        self.index_writer.sync_data()?;

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

pub trait Duplicable: Sized {
    const BYTES: u64;

    fn write_to<W: io::Write>(&mut self, writer: &mut W) -> Result<()>;
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

fn discover_strata(path: &Path, scratch: &mut Vec<u8>) -> Result<Vec<UnverifiedStratum>> {
    let mut discovered = Vec::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            if let Ok(basin_and_stratum) = BasinAndStratum::from_str(name) {
                discovered.push(UnverifiedStratum::read_from(
                    entry.path(),
                    basin_and_stratum,
                    scratch,
                )?);
            }
        }
    }

    discovered.sort_by(|a, b| a.id.cmp(&b.id));

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

    pub fn get_or_open_file(&mut self) -> Result<&mut File> {
        if self.file.is_none() {
            // TODO fsync directory
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(self.path.as_ref())?;

            self.file = Some(file);
        }

        Ok(self.file.as_mut().expect("file always allocated above"))
    }

    pub fn write_header(&mut self, new_transaction_id: TransactionId) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .expect("shouldn't ever write a file header if no data was written");

        self.header.active.transaction_id = new_transaction_id;
        self.header.write_to(file)?;

        // TODO this should be moved to a threaded operation to allow multiple
        // strata to sync at the same time.
        file.sync_data()
    }
}

struct UnverifiedStratum {
    path: PathBuf,
    id: BasinAndStratum,
    header: StratumFileHeader,
    file: File,
}

impl UnverifiedStratum {
    pub fn read_from(path: PathBuf, id: BasinAndStratum, scratch: &mut Vec<u8>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let header = StratumFileHeader::read_from(&mut file, scratch)?;
        Ok(Self {
            id,
            path,
            header,
            file,
        })
    }
}
