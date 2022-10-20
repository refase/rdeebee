use std::{collections::HashMap, env, path::PathBuf};

use crate::storage::{MemTable, Wal};

/// In case there is a crash of the system and the MemTable is lost,
/// this will recover MemTable from the latest WAL.
pub(crate) struct Recovery {}

impl Recovery {
    pub(crate) fn recover(&self) -> MemTable {
        let mut memtable = MemTable::new();
        let (wal_epochs, mut wal_map) = Self::recover_wal_files();
        let wal_epochs_iter = wal_epochs.into_iter();
        for epoch in wal_epochs_iter {
            if let Some((_, path)) = wal_map.remove_entry(&epoch) {
                let wal = Wal::from_path(&path).unwrap();
                for event in wal {
                    memtable.insert(event);
                }
            }
        }
        memtable
    }

    fn recover_wal_files() -> (Vec<u128>, HashMap<u128, PathBuf>) {
        let tmp_dir = env::temp_dir();
        let mut wal_epochs = Vec::new();
        let mut wal_map = HashMap::new();
        for entry in tmp_dir.read_dir().unwrap() {
            let path = entry.unwrap().path();
            if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                if extension == "wal" {
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
        (wal_epochs, wal_map)
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
        let memtable = recovery.recover();
        for event in &memtable {
            println!("Event: {}", event);
        }
        println!("Recovery succesful");
    }
}
