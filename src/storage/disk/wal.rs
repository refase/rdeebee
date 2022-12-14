use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::error;
use uuid::Uuid;

use crate::{Action, Event, StorageEngineError};

/// This is the Write-Ahead Log
/// This part, again, follows this [blog](https://adambcomer.com/blog/simple-database/Wal/)
pub(crate) struct WalIterator {
    reader: BufReader<File>,
}

impl WalIterator {
    /// Create a new iterator from the file path.
    pub(crate) fn new(path: PathBuf) -> io::Result<WalIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }
}

impl Iterator for WalIterator {
    type Item = Event;

    /// Get the next entry in the Wal file.
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

pub(crate) struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
}

impl Wal {
    const WAL_NAME: &str = "rdeebee";

    /// Create a new Wal.
    pub(crate) fn new(dir: &str) -> Result<Self, StorageEngineError> {
        let dir_path = PathBuf::from(dir);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
        let temp_file = dir_path.join(format!("{}-{}.wal", Self::WAL_NAME, timestamp));
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&temp_file)?;
        let file = BufWriter::new(file);
        Ok(Self {
            path: temp_file,
            file,
        })
    }

    /// Create a Wal from existing file.
    pub(crate) fn from_path(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let file = BufWriter::new(file);

        Ok(Wal {
            path: path.to_owned(),
            file,
        })
    }

    /// Get the file path to the current wal file.
    pub(crate) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Add an event to the Wal
    pub(crate) fn add_event(&mut self, event: Event) -> Result<(), StorageEngineError> {
        let event_ser = bincode::serialize(&event)?;
        self.file.write_all(&event_ser)?;
        self.file.write_all("|".as_bytes())?; // delimiter
        Ok(())
    }

    /// Append a delete operation to the Wal
    pub(crate) fn delete_event(
        &mut self,
        event_id: Uuid,
        seq: u64,
    ) -> Result<(), StorageEngineError> {
        let mut event = Event::new(Action::Delete, seq);
        event.set_id(event_id);
        self.add_event(event)?;
        self.file.flush()?;
        Ok(())
    }
}

impl IntoIterator for Wal {
    type Item = Event;
    type IntoIter = WalIterator;

    fn into_iter(self) -> Self::IntoIter {
        WalIterator::new(self.path).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::{
        env,
        fs::{self, File},
        io::{BufRead, BufReader, BufWriter, Read, Write},
        str,
    };

    use crate::storage::{Action, Event};

    use super::Wal;

    const TEST_DIR: &str = "/tmp";

    #[test]
    fn write_Wal_test() {
        let mut Wal = Wal::new(TEST_DIR).unwrap();
        // create two events
        let event1 = Event::new(Action::Read);
        let mut event2 = Event::new(Action::Read);
        // set payload on one event
        let payload2 = Some(bincode::serialize("This is second event read").unwrap());
        event2.set_payload(payload2);

        Wal.add_event(event1).unwrap();
        Wal.add_event(event2).unwrap();
        Wal.flush();
    }

    #[test]
    fn iterate_Wal_test() {
        let mut Wal = Wal::new(TEST_DIR).unwrap();
        // create two events
        let event1 = Event::new(Action::Read);
        let mut event2 = Event::new(Action::Read);
        // set payload on one event
        let payload2 = Some(bincode::serialize("This is second event read").unwrap());
        event2.set_payload(payload2);

        Wal.add_event(event1).unwrap();
        Wal.add_event(event2).unwrap();

        for event in Wal {
            println!("Event: {}", event);
            match event.payload() {
                Some(payload) => {
                    let payload_str = bincode::deserialize::<&str>(&payload).unwrap();
                    println!("Event payload: {}", payload_str);
                }
                None => {}
            }
        }
    }

    #[test]
    fn load_Wal_test() {
        let mut Wal = Wal::new(TEST_DIR).unwrap();
        // create two events
        let event1 = Event::new(Action::Read);
        let mut event2 = Event::new(Action::Read);
        // set payload on one event
        let payload2 = Some(bincode::serialize("This is second event read").unwrap());
        event2.set_payload(payload2);

        Wal.add_event(event1).unwrap();
        Wal.add_event(event2).unwrap();
        Wal.flush();

        let new_Wal = Wal::from_path(&Wal.path).unwrap();
        println!("Old Wal: {:#?}", &Wal.path);
        println!("New Wal: {:#?}", &new_Wal.path);

        for event in new_Wal {
            println!("Event: {}", event);
            match event.payload() {
                Some(payload) => {
                    let payload_str = bincode::deserialize::<&str>(&payload).unwrap();
                    println!("Event payload: {}", payload_str);
                }
                None => {}
            }
        }
    }
}
