use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::{error, info};
use uuid::Uuid;

use crate::{
    storage::mem::MemTable,
    storageops::{Action, Event, StorageEngineError},
};

/// SSTable moves the data stored onto a file on disk
/// when the MemTable exceeds a defined size.
pub(crate) struct SSTable {
    // We can use the SSTable to read data from a file, without there being a MemTable.
    memtable: Option<MemTable>,
    filepath: PathBuf,
    // If reading from the SSTable, a writer is not required.
    writer: Option<BufWriter<File>>,
}

impl SSTable {
    const TABLENAME: &str = "rdeebee";

    pub(crate) fn from_memtable(
        dirname: &str,
        memtable: MemTable,
    ) -> Result<Self, StorageEngineError> {
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        let dir = PathBuf::from_str(dirname)?;
        let filepath = dir.join(format!("{}-{}.table", Self::TABLENAME, epoch));
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&filepath)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            memtable: Some(memtable),
            filepath,
            writer: Some(writer),
        })
    }

    pub(crate) fn from_file(filepath: PathBuf) -> Result<Self, StorageEngineError> {
        info!("Creating new SSTable: {}", &filepath.display());
        Ok(Self {
            memtable: None,
            filepath: filepath,
            writer: None,
        })
    }

    fn iter(&self) -> Result<SSTableIterator, StorageEngineError> {
        SSTableIterator::new(self.filepath.clone())
    }

    /// Does the event exist in the SSTable.
    pub(crate) fn contains(&self, id: Uuid) -> bool {
        if let Ok(mut ssiter) = self.iter() {
            for ref event in ssiter.by_ref() {
                return event.transaction_id() == id;
            }
        }
        false
    }

    /// Find event by id.
    pub(crate) fn get(&self, id: Uuid) -> Option<Event> {
        if let Ok(mut ssiter) = self.iter() {
            for ref event in ssiter.by_ref() {
                if event.transaction_id() == id {
                    return Some(event.clone());
                }
            }
        }
        None
    }

    /// Save the SSTable to disk.
    pub(crate) fn write_to_disk(&mut self) -> Result<(), StorageEngineError> {
        let memtable = match self.memtable.as_ref() {
            Some(memtable) => memtable,
            None => return Err(StorageEngineError::InvalidMemTable),
        };

        match &mut self.writer {
            Some(writer) => {
                for event in memtable {
                    let event_ser = bincode::serialize(&event)?;
                    writer.write_all(&event_ser)?;
                    writer.write_all("|".as_bytes())?;
                }
                writer.flush()?;
                Ok(())
            }
            None => Err(StorageEngineError::InvalidSSTableWriter(
                self.filepath.clone(),
            )),
        }
    }

    /// commit is similar to write_to_disk
    /// but it consumes the SSTable.
    /// Typically used when mergin SSTables.
    pub(crate) fn commit(mut self) -> Result<(), StorageEngineError> {
        self.write_to_disk()
    }

    pub(crate) fn commit_events(mut self, events: Vec<Event>) -> Result<(), StorageEngineError> {
        match &mut self.writer {
            Some(writer) => {
                for event in events {
                    let event_ser = bincode::serialize(&event)?;
                    writer.write_all(&event_ser)?;
                    writer.write_all("|".as_bytes())?; // delimeter
                }
                writer.flush()?;
            }
            None => {}
        }
        Ok(())
    }

    fn epoch(&self) -> Result<u128, StorageEngineError> {
        let filename = match self.filepath.file_name().and_then(|f| f.to_str()) {
            Some(filename) => filename,
            None => {
                return Err(StorageEngineError::InvalidSSTableFilePath(
                    self.filepath.clone(),
                ))
            }
        };
        Ok(filename
            .split(|c| (c == '-') || (c == '.'))
            .collect::<Vec<&str>>()[1]
            .parse::<u128>()?)
    }

    /// Consumes SSTable to create new SSTable file.
    /// Returns the new SSTable with merged data.
    pub(crate) fn merge(mut self, other: SSTable) -> Result<Self, StorageEngineError> {
        let mut events = Vec::new();
        let mut deleted_events = Vec::new();

        let self_epoch = self.epoch()?;
        let other_epoch = other.epoch()?;

        let mut self_iter = self.iter()?;
        let mut other_iter = other.iter()?;

        loop {
            let mut event: Option<Event> = None;
            match (self_iter.next(), other_iter.next()) {
                (None, None) => break,
                (None, Some(other_event)) => {
                    if other_event.action() == Action::Delete {
                        deleted_events.push(other_event);
                    } else {
                        event = Some(other_event);
                    }
                }
                (Some(self_event), None) => {
                    if self_event.action() == Action::Delete {
                        deleted_events.push(self_event);
                    } else {
                        event = Some(self_event);
                    }
                }
                (Some(self_event), Some(other_event)) => match self_event
                    .transaction_id()
                    .cmp(&other_event.transaction_id())
                {
                    std::cmp::Ordering::Less => {
                        if self_event.action() == Action::Delete {
                            deleted_events.push(self_event);
                        } else {
                            event = Some(self_event);
                        }
                    }
                    std::cmp::Ordering::Equal => match self_epoch > other_epoch {
                        true => {
                            if self_event.action() == Action::Delete {
                                deleted_events.push(self_event);
                            } else {
                                event = Some(self_event);
                            }
                        }
                        false => {
                            if other_event.action() == Action::Delete {
                                deleted_events.push(other_event);
                            } else {
                                event = Some(other_event);
                            }
                        }
                    },
                    std::cmp::Ordering::Greater => {
                        if other_event.action() == Action::Delete {
                            deleted_events.push(other_event);
                        } else {
                            event = Some(other_event);
                        }
                    }
                },
            }
            if let Some(event) = event {
                if !deleted_events.contains(&event) {
                    events.push(event);
                }
            }
        }

        fs::remove_file(self.filepath.clone()).expect(&format!(
            "failed to remove file: {}",
            self.filepath.display()
        ));
        fs::remove_file(other.filepath.clone()).expect(&format!(
            "failed to remove file: {}",
            other.filepath.display()
        ));

        // This is a decision point - choosing the right epoch for the merged SSTable.
        // In this case, we choose the latest of the two tables merged.
        // However, we could choose a completely new epoch as:
        // let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        let epoch = if self_epoch > other_epoch {
            other_epoch
        } else {
            self_epoch
        };

        let dir = match self.filepath.parent() {
            Some(dir) => dir,
            None => return Err(StorageEngineError::InvalidDbDir(self.filepath)),
        }
        .to_owned();

        let filepath = dir.join(format!("{}-{}.table", Self::TABLENAME, epoch));
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&filepath)?;
        let writer = BufWriter::new(file);
        self.writer = Some(writer);
        self.commit_events(events)?;

        Ok(Self {
            memtable: None,
            filepath,
            writer: None,
        })
    }
}

pub(crate) struct SSTableIterator {
    reader: BufReader<File>,
}

impl SSTableIterator {
    fn new(filepath: PathBuf) -> Result<Self, StorageEngineError> {
        let file = OpenOptions::new().read(true).open(&filepath)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for SSTableIterator {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        let mut data_bytes = Vec::new();
        match self.reader.read_until(b'|', &mut data_bytes) {
            Ok(sz) => {
                if sz == 0 {
                    return None;
                }
                match bincode::deserialize::<Event>(&data_bytes) {
                    Ok(event) => Some(event),
                    Err(e) => {
                        error!("Error getting next event: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                error!("Error getting next event: {}", e);
                None
            }
        }
    }
}

impl IntoIterator for SSTable {
    type Item = Event;
    type IntoIter = SSTableIterator;

    fn into_iter(self) -> Self::IntoIter {
        SSTableIterator::new(self.filepath).unwrap()
    }
}
