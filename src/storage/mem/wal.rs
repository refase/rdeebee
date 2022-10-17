use std::{
    env,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::storage::{Action, Event};

/// This is the Write-Ahead Log
/// This part, again, follows this [blog](https://adambcomer.com/blog/simple-database/wal/)
pub(crate) struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    /// Create a new iterator from the file path.
    pub(crate) fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIterator { reader })
    }
}

impl Iterator for WALIterator {
    type Item = Event;

    /// Get the next entry in the WAL file.
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

pub(crate) struct WAL {
    path: PathBuf,
    file: BufWriter<File>,
}

impl WAL {
    const WALNAME: &str = "rdeebee";

    /// Create a new WAL.
    pub(crate) fn new() -> io::Result<Self> {
        let tmp_dir = env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let temp_file = tmp_dir.join(format!("{}-{}.wal", Self::WALNAME, timestamp.to_string()));
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

    /// Create a WAL from existing file.
    pub(crate) fn from_path(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL {
            path: path.to_owned(),
            file,
        })
    }

    /// Add an event to the WAL
    // TODO: convert return type to `thiserror`
    pub(crate) fn add_event(&mut self, event: Event) -> io::Result<()> {
        let event_ser = bincode::serialize(&event).unwrap(); // TODO: convert to and return `thiserror`
        self.file.write_all(&event_ser)?;
        self.file.write("|".as_bytes())?;
        Ok(())
    }

    /// Append a delete operation to the WAL
    pub(crate) fn delete_event(&mut self, event_id: Uuid) -> io::Result<()> {
        let mut event = Event::new(Action::DELETE);
        event.set_id(event_id);
        self.add_event(event)
    }

    /// Flush the WAL to disk
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl IntoIterator for WAL {
    type Item = Event;
    type IntoIter = WALIterator;

    fn into_iter(self) -> Self::IntoIter {
        WALIterator::new(self.path).unwrap()
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

    use super::WAL;

    #[test]
    fn write_wal_test() {
        let mut wal = WAL::new().unwrap();
        // create two events
        let event1 = Event::new(Action::READ);
        let mut event2 = Event::new(Action::READ);
        // set payload on one event
        let payload2 = Some(bincode::serialize("This is second event read").unwrap());
        event2.set_payload(payload2);

        wal.add_event(event1).unwrap();
        wal.add_event(event2).unwrap();
    }

    #[test]
    fn iterate_wal_test() {
        let mut wal = WAL::new().unwrap();
        // create two events
        let event1 = Event::new(Action::READ);
        let mut event2 = Event::new(Action::READ);
        // set payload on one event
        let payload2 = Some(bincode::serialize("This is second event read").unwrap());
        event2.set_payload(payload2);

        wal.add_event(event1).unwrap();
        wal.add_event(event2).unwrap();

        for event in wal {
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
    fn load_wal_test() {
        let mut wal = WAL::new().unwrap();
        // create two events
        let event1 = Event::new(Action::READ);
        let mut event2 = Event::new(Action::READ);
        // set payload on one event
        let payload2 = Some(bincode::serialize("This is second event read").unwrap());
        event2.set_payload(payload2);

        wal.add_event(event1).unwrap();
        wal.add_event(event2).unwrap();

        let new_wal = WAL::from_path(&wal.path).unwrap();
        println!("New WAL");

        for event in new_wal {
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
