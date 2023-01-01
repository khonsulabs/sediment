use std::path;

use crate::{Database, Result};

pub struct Config {
    pub wal: okaywal::Configuration,
}

impl Config {
    pub fn for_directory<Path>(directory: Path) -> Self
    where
        Path: AsRef<path::Path>,
    {
        Self {
            wal: okaywal::Configuration::default_for(directory),
        }
    }

    pub fn configure_wal<
        Configuration: FnOnce(okaywal::Configuration) -> okaywal::Configuration,
    >(
        mut self,
        configurator: Configuration,
    ) -> Self {
        self.wal = configurator(self.wal);
        self
    }

    pub fn recover(self) -> Result<Database> {
        Database::recover_config(self)
    }
}

impl From<okaywal::Configuration> for Config {
    fn from(wal: okaywal::Configuration) -> Self {
        Self { wal }
    }
}
