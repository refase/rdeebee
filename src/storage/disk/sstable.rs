use std::{
    collections::HashMap,
    env,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::PathBuf,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use skiplist::OrderedSkipList;
use uuid::Uuid;

use crate::storage::{Action, Event, MemTable};

pub(crate) struct SSTableIterator {
    reader: BufReader<File>,
}

impl SSTableIterator {
    fn new(filepath: PathBuf) -> io::Result<Self> {
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
    epoch: u128,
    memtable: Option<MemTable>,
    // dirname: PathBuf,
    filepath: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl SSTable {
    // We will compact once we have reached this size.
    const MAX_SIZE_IN_BYTES: u64 = 2048;

    const TABLENAME: &str = "rdeebee";

    /// Create a table file in the directory provided
    /// Consumes the MemTable
    pub(crate) fn from_memtable(dirname: &str, memtable: MemTable) -> Self {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        // TODO: Error handling: Assumes a correct path
        let dir = PathBuf::from_str(dirname).unwrap();
        let filepath = dir.join(format!("{}-{}.table", Self::TABLENAME, epoch.to_string()));
        // TODO: Error handling: Assumes a correct permissions
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&filepath)
            .unwrap();
        let writer = BufWriter::new(file);
        Self {
            epoch,
            memtable: Some(memtable),
            filepath,
            writer: Some(writer),
        }
    }

    /// Saves the SSTable to disk
    pub(crate) fn save_to_disk(&mut self) -> io::Result<()> {
        let memtable = self.memtable.as_ref().unwrap();
        match &mut self.writer {
            Some(writer) => {
                for event in memtable {
                    let event_ser = bincode::serialize(&event).unwrap(); // TODO: convert to and return `thiserror`
                    writer.write_all(&event_ser)?;
                    writer.write("|".as_bytes())?; // delimeter
                }
                writer.flush()?;
            }
            None => {}
        }
        Ok(())
    }

    /// Consumes the SSTable to write to file
    /// Used when merging
    pub(crate) fn write_to_file(mut self, events: Vec<Event>) -> io::Result<()> {
        match &mut self.writer {
            Some(writer) => {
                for event in events {
                    let event_ser = bincode::serialize(&event).unwrap(); // TODO: convert to and return `thiserror`
                    writer.write_all(&event_ser)?;
                    writer.write("|".as_bytes())?; // delimeter
                }
                writer.flush()?;
            }
            None => {}
        }
        Ok(())
    }

    fn get_epoch_from_filename(filename: &str) -> u128 {
        filename
            .split(|c| (c == '-') || (c == '.'))
            .collect::<Vec<&str>>()[1]
            .parse::<u128>()
            .unwrap() // TODO: error handling
    }

    /// Given an existing file, return an SSTable
    pub(crate) fn from_file(filepath: PathBuf) -> io::Result<Self> {
        log::info!("Opening new segment: {}", &filepath.display());
        let file = OpenOptions::new().read(true).open(&filepath)?;
        let meta = file.metadata()?;
        let filename = filepath.file_name().and_then(|f| f.to_str()).unwrap(); // TODO: error handling
        let epoch = Self::get_epoch_from_filename(filename);

        Ok(Self {
            epoch,
            memtable: None,
            filepath,
            writer: None,
        })
    }

    fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(self.filepath.clone()).unwrap()
    }

    /// Consumes the SSTables to create a new file
    /// Returns the path where the new data is written
    pub(crate) fn merge(mut self, other: SSTable) -> PathBuf {
        let mut events = Vec::new();
        let mut deleted_events = Vec::new();
        let epoch1 = Self::get_epoch_from_filename(
            self.filepath.file_name().and_then(|f| f.to_str()).unwrap(),
        );
        let epoch2 = Self::get_epoch_from_filename(
            other.filepath.file_name().and_then(|f| f.to_str()).unwrap(),
        );

        let dir = self.filepath.parent().unwrap().to_owned();

        let mut iter1 = self.iter();
        let mut iter2 = other.iter();

        loop {
            let mut event: Option<Event> = None;
            match (iter1.next(), iter2.next()) {
                (Some(event1), Some(event2)) => {
                    if event1.id() < event2.id() {
                        if event1.action() == &Action::DELETE {
                            deleted_events.push(event1.id());
                        }
                        event = Some(event1);
                    } else if event1.id() == event2.id() {
                        if epoch1 > epoch2 {
                            if event1.action() == &Action::DELETE {
                                deleted_events.push(event1.id());
                            }
                            event = Some(event1);
                        } else {
                            if event2.action() == &Action::DELETE {
                                deleted_events.push(event2.id());
                            }
                            event = Some(event2);
                        }
                    } else {
                        if event2.action() == &Action::DELETE {
                            deleted_events.push(event2.id());
                        }
                        event = Some(event2);
                    }
                }
                (None, Some(event2)) => {
                    if event2.action() == &Action::DELETE {
                        deleted_events.push(event2.id());
                    }
                    event = Some(event2);
                }
                (Some(event1), None) => {
                    if event1.action() == &Action::DELETE {
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

        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let dir = self.filepath.parent().unwrap().to_owned();
        let filepath = dir.join(format!("{}-{}.table", Self::TABLENAME, epoch.to_string()));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filepath)
            .unwrap();
        let writer = BufWriter::new(file);
        self.writer = Some(writer);
        self.write_to_file(events).unwrap();
        filepath
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
            events.push(Event::new(Action::READ));
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
            memtable.insert(Event::new(Action::READ));
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
            memtable.insert(Event::new(Action::READ));
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
            let mut event = Event::new(Action::READ);
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
            let mut event = Event::new(Action::READ);
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