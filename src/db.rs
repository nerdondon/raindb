/*!
The database module contains the primary API for interacting with the key-value store.
*/

use std::path::PathBuf;
use std::sync::Arc;

use crate::errors::RainDBResult;
use crate::file_names::{DATA_DIR, WAL_DIR};
use crate::memtable::{MemTable, SkipListMemTable};
use crate::write_ahead_log::WALWriter;
use crate::DbOptions;

pub struct DB {
    options: DbOptions,
    memtable: Box<dyn MemTable>,
    wal: WALWriter,
}

impl DB {
    pub fn open(options: DbOptions) -> RainDBResult<DB> {
        log::info!(
            "Initializing raindb with the following options {:#?}",
            options
        );

        let fs = options.filesystem_provider();
        let db_path = options.db_path();

        // Create DB root directory
        log::info!("Creating DB root directory at {}.", &db_path);
        let root_path = PathBuf::from(&db_path);
        let mut wal_path = PathBuf::from(&db_path);
        wal_path.push(WAL_DIR);
        let mut data_path = PathBuf::from(&db_path);
        data_path.push(DATA_DIR);

        fs.create_dir_all(&root_path)?;
        fs.create_dir(&wal_path)?;
        fs.create_dir(&data_path)?;

        // Create WAL
        let wal = WALWriter::new(Arc::clone(&fs), wal_path.to_str().unwrap())?;

        // Create memtable
        let memtable = Box::new(SkipListMemTable::new());

        // Start compaction service

        Ok(DB {
            options,
            wal,
            memtable,
        })
    }

    pub fn get(&self) {
        todo!("working on it!")
    }

    pub fn put(&self) {
        todo!("working on it!")
    }

    pub fn delete(&self) {
        todo!("working on it!")
    }

    pub fn close(&self) {
        todo!("working on it!")
    }
}
