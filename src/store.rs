use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Mutex, MutexGuard, PoisonError},
};

use crate::{
    basinmap::BasinMap,
    format::{BasinAndStratum, IndexFile, IndexHeader, StratumFileHeader, StratumHeader},
    Result,
};

#[derive(Debug)]
pub struct Store {
    directory: PathBuf,
    disk_state: Mutex<DiskState>,
}

impl Store {
    pub fn recover(path: &Path) -> Result<Self> {
        let disk_state = DiskState::recover(path)?;
        Ok(Self {
            directory: path.to_path_buf(),
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

                let basin = basins.get_or_insert_with(stratum.id.basin(), BasinState::default);
                assert_eq!(
                    basin.stratum.len(),
                    stratum.id.stratum().as_usize(),
                    "strata are non-sequential"
                );

                basin.stratum.push(header);
            }

            Ok(Self {
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
}

#[derive(Debug, Default)]
pub struct Duplicated<T> {
    pub active: T,
    pub first_is_active: bool,
}

#[derive(Debug, Default)]
pub struct BasinState {
    pub stratum: Vec<Duplicated<StratumHeader>>,
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
