use std::{collections::HashMap, path::PathBuf, str::FromStr};

use log::error;

use crate::storage::{MemTable, SSTable, Wal};

/// In case there is a crash of the system and the MemTable is lost,
/// this will recover MemTable from the latest WAL.
pub(crate) struct Recovery {}

impl Recovery {
    pub(crate) fn recover(&self, dir: &str) -> Option<MemTable> {
        let mut memtable = MemTable::new();
        let (wal_epochs, mut wal_map) = match self.recover_files(dir, true) {
            Some(val) => val,
            None => return None,
        };
        let wal_epochs_iter = wal_epochs.into_iter();
        for epoch in wal_epochs_iter {
            if let Some((_, path)) = wal_map.remove_entry(&epoch) {
                let wal = Wal::from_path(&path).unwrap();
                for event in wal {
                    memtable.insert(event);
                }
            }
        }
        Some(memtable)
    }

    pub(crate) fn recover_sstable(&self, dir: &str) -> Option<Vec<SSTable>> {
        let mut table_vec = Vec::new();
        let (table_epochs, mut table_map) = match self.recover_files(dir, false) {
            Some(val) => val,
            None => return None,
        };
        for epoch in table_epochs.into_iter() {
            match table_map.remove_entry(&epoch) {
                Some((_, path)) => table_vec.push(SSTable::from_file(path).unwrap()),
                None => error!("failed to create sstable"),
            }
        }
        Some(table_vec)
    }

    fn recover_files(&self, dir: &str, wal: bool) -> Option<(Vec<u128>, HashMap<u128, PathBuf>)> {
        let dir = match PathBuf::from_str(dir) {
            Ok(d) => d,
            Err(e) => {
                error!("failed to get directory in recovery: {}", e);
                return None;
            }
        };
        let mut wal_epochs = Vec::new();
        let mut wal_map = HashMap::new();
        for entry in dir.read_dir().unwrap() {
            let path = entry.unwrap().path();
            if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                let ext = match wal {
                    true => "wal",
                    false => "table",
                };
                if extension == ext {
                    let filename = path.file_name().and_then(|f| f.to_str()).unwrap();
                    let epoch = filename
                        .split(|c| (c == '-') || (c == '.'))
                        .collect::<Vec<&str>>()[1]
                        .parse::<u128>()
                        .unwrap();
                    wal_epochs.push(epoch);
                    wal_map.insert(epoch, path);
                }
            }
        }
        wal_epochs.sort();
        Some((wal_epochs, wal_map))
    }
}

#[cfg(test)]
mod test {
    use std::{
        env,
        fs::{self, OpenOptions},
        path,
        time::UNIX_EPOCH,
    };

    use super::Recovery;

    #[test]
    fn recovery_test() {
        let recovery = Recovery {};
        let memtable = recovery.recover("/tmp").unwrap();
        for event in &memtable {
            println!("Event: {}", event);
        }
        println!("Recovery succesful");
    }
}
