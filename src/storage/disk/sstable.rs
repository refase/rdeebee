use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::{
    errors::StorageEngineError,
    event::{Action, Event},
    storage::MemTable,
};

pub(crate) struct SSTableIterator {
    reader: BufReader<File>,
}

impl SSTableIterator {
    fn new(filepath: PathBuf) -> Result<Self, StorageEngineError> {
        let file = OpenOptions::new().read(true).open(filepath)?;
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
                        log::error!("Error getting next event: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                log::error!("Error getting next event: {}", e);
                None
            }
        }
    }
}

/// For the sstable structure, I looked at this [code](https://github.com/DevinZ1993/NaiveKV/blob/main/src/sstable.rs)
/// and the associated [blog](https://devinz1993.medium.com/naivekv-a-log-structured-storage-engine-bc44bde596b)
pub(crate) struct SSTable {
    memtable: Option<MemTable>,
    filepath: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl SSTable {
    const TABLENAME: &str = "rdeebee";

    /// Create a table file in the directory provided
    /// Consumes the MemTable
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

    /// Does this event exist in the SSTable
    pub(crate) fn contains(&self, id: Uuid) -> bool {
        for ref event in self.iter() {
            if event.id() == id {
                return true;
            }
        }
        false
    }

    /// Find event by ID
    pub(crate) fn get(&self, id: Uuid) -> Option<Event> {
        for ref event in self.iter() {
            if event.id() == id {
                return Some(event.clone());
            }
        }
        None
    }

    /// Saves the SSTable to disk
    pub(crate) fn save_to_disk(&mut self) -> Result<(), StorageEngineError> {
        let memtable = match self.memtable.as_ref() {
            Some(memtable) => memtable,
            None => return Err(StorageEngineError::InvalidMemTable),
        };
        match &mut self.writer {
            Some(writer) => {
                for event in memtable {
                    let event_ser = bincode::serialize(&event)?;
                    writer.write_all(&event_ser)?;
                    writer.write_all("|".as_bytes())?; // delimeter
                }
                writer.flush()?;
                Ok(())
            }
            None => Err(StorageEngineError::InvalidSSTableWriter(
                self.filepath.clone(),
            )),
        }
    }

    /// Consumes the SSTable to write to file
    /// Used when merging
    pub(crate) fn write_to_file(mut self, events: Vec<Event>) -> Result<(), StorageEngineError> {
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

    fn get_epoch_from_filename(filename: &str) -> Result<u128, StorageEngineError> {
        Ok(filename
            .split(|c| (c == '-') || (c == '.'))
            .collect::<Vec<&str>>()[1]
            .parse::<u128>()?)
    }

    /// Given an existing file, return an SSTable
    pub(crate) fn from_file(filepath: PathBuf) -> io::Result<Self> {
        log::info!("Opening new segment: {}", &filepath.display());
        Ok(Self {
            memtable: None,
            filepath,
            writer: None,
        })
    }

    fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(self.filepath.clone()).unwrap()
    }

    /// Consumes the SSTables to create a new file
    /// Returns the new SSTable for the merged data
    pub(crate) fn merge(mut self, other: SSTable) -> Result<SSTable, StorageEngineError> {
        let mut events = Vec::new();
        let mut deleted_events = Vec::new();

        let self_file = match self.filepath.file_name().and_then(|f| f.to_str()) {
            Some(path) => path,
            None => {
                return Err(StorageEngineError::InvalidSSTableFilePath(
                    self.filepath.clone(),
                ))
            }
        };

        let other_file = match other.filepath.file_name().and_then(|f| f.to_str()) {
            Some(path) => path,
            None => {
                return Err(StorageEngineError::InvalidSSTableFilePath(
                    self.filepath.clone(),
                ))
            }
        };

        let epoch1 = Self::get_epoch_from_filename(self_file)?;
        let epoch2 = Self::get_epoch_from_filename(other_file)?;

        let mut iter1 = self.iter();
        let mut iter2 = other.iter();

        loop {
            let event: Option<Event>;
            match (iter1.next(), iter2.next()) {
                (Some(event1), Some(event2)) => match event1.id().cmp(&event2.id()) {
                    std::cmp::Ordering::Less => {
                        if event1.action() == &Action::Delete {
                            deleted_events.push(event1.id());
                        }
                        event = Some(event1);
                    }
                    std::cmp::Ordering::Equal => match epoch1 > epoch2 {
                        true => {
                            if event1.action() == &Action::Delete {
                                deleted_events.push(event1.id());
                            }
                            event = Some(event1);
                        }
                        false => {
                            if event2.action() == &Action::Delete {
                                deleted_events.push(event2.id());
                            }
                            event = Some(event2);
                        }
                    },
                    std::cmp::Ordering::Greater => {
                        if event2.action() == &Action::Delete {
                            deleted_events.push(event2.id());
                        }
                        event = Some(event2);
                    }
                },
                (None, Some(event2)) => {
                    if event2.action() == &Action::Delete {
                        deleted_events.push(event2.id());
                    }
                    event = Some(event2);
                }
                (Some(event1), None) => {
                    if event1.action() == &Action::Delete {
                        deleted_events.push(event1.id());
                    }
                    event = Some(event1);
                }
                (None, None) => break,
            }
            if let Some(event) = event {
                if !deleted_events.contains(&event.id()) {
                    events.push(event);
                }
            }
        }

        fs::remove_file(self.filepath.clone()).expect("failed to remove old file");
        fs::remove_file(other.filepath).expect("failed to remove old file");

        let epoch = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();

        let dir = match self.filepath.parent() {
            Some(dir) => dir,
            None => return Err(StorageEngineError::InvalidDbDir(self.filepath)),
        };

        let dir = dir.to_owned();
        let filepath = dir.join(format!("{}-{}.table", Self::TABLENAME, epoch));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filepath)?;
        let writer = BufWriter::new(file);
        self.writer = Some(writer);
        self.write_to_file(events)?;
        Ok(Self {
            memtable: None,
            filepath,
            writer: None,
        })
    }
}

impl IntoIterator for SSTable {
    type Item = Event;
    type IntoIter = SSTableIterator;

    fn into_iter(self) -> Self::IntoIter {
        SSTableIterator::new(self.filepath).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use crate::storage::{
        disk::SSTable,
        event::{Action, Event},
        mem::MemTable,
    };
    use rand::{distributions::Alphanumeric, Rng};
    use uuid::Uuid;

    fn create_events(n: usize) -> Vec<Event> {
        let mut events = Vec::new();
        for _ in 0..n {
            events.push(Event::new(Action::Read));
        }
        events
    }

    fn insert_events(memtable: &mut MemTable, events: Vec<Event>) {
        for event in events {
            memtable.insert(event);
        }
    }

    #[test]
    fn sstable_from_memtable_test() {
        let mut memtable = MemTable::new();
        for i in 0..5 {
            memtable.insert(Event::new(Action::Read));
        }
        let mut sstable = SSTable::from_memtable("/tmp", memtable);
        println!("{}", sstable.filepath.display());
        sstable.save_to_disk();
        for event in sstable {
            println!("Event: {}", event);
        }
    }

    #[test]
    fn sstable_from_file_test() {
        let mut memtable = MemTable::new();
        for i in 0..5 {
            memtable.insert(Event::new(Action::Read));
        }
        let mut sstable = SSTable::from_memtable("/tmp", memtable);
        sstable.save_to_disk();

        let othertable = SSTable::from_file(sstable.filepath).unwrap();
        for event in othertable {
            println!("Event: {}", event);
        }
    }

    #[test]
    fn sstable_merge_test() {
        let mut memtable1 = MemTable::new();
        let mut common_id1 = Uuid::from_u128(0);
        let mut common_id2 = Uuid::from_u128(1);
        for i in 0..5 {
            let mut event = Event::new(Action::Read);
            if i == 0 {
                common_id1 = event.id();
                event.set_payload_str("From epoch 1-1");
            } else if i == 2 {
                common_id2 = event.id();
                event.set_payload_str("From epoch 1-2");
            }
            memtable1.insert(event);
        }
        let mut sstable1 = SSTable::from_memtable("/tmp", memtable1);
        sstable1.save_to_disk();

        let epoch1 = sstable1.epoch;
        for event in sstable1.iter() {
            if event.payload().is_some() {
                println!("Event Set1: {}", event);
            }
        }

        thread::sleep(Duration::from_millis(10));

        let mut memtable2 = MemTable::new();
        for i in 0..5 {
            let mut event = Event::new(Action::Read);
            if i == 0 {
                event.set_id(common_id1);
                event.set_payload_str("From epoch 2-1");
            } else if i == 2 {
                event.set_id(common_id2);
                event.set_payload_str("From epoch 2-2");
            }
            memtable2.insert(event);
        }
        let mut sstable2 = SSTable::from_memtable("/tmp", memtable2);
        sstable2.save_to_disk();
        let epoch2 = sstable2.epoch;
        for event in sstable2.iter() {
            if event.payload().is_some() {
                println!("Event Set2: {}", event);
            }
        }

        let filepath = sstable1.merge(sstable2);

        let merged_sstable = SSTable::from_file(filepath).unwrap();
        for event in merged_sstable {
            if event.payload().is_some() {
                println!("Merged Event Set (Common ID): {}", event);
            }
        }
        if epoch1 > epoch2 {
            println!("Merged set contains epoch1");
        } else {
            println!("Merged set contains epoch2");
        }
    }
}
