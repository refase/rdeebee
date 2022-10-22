use std::{collections::HashMap, io, mem, str::FromStr};

use log::error;
use protobuf::EnumOrUnknown;
use protos::protos::operation::{Operation, Request, Response, Status};
use recovery::Recovery;
use storage::{Action, BloomFilter, Event, MemTable, SSTable, Wal};
use uuid::Uuid;
mod protos;
mod recovery;
mod storage;

pub struct RDeeBee {
    compaction_size: usize,
    deebee_dir: String,
    wal: Wal,
    memtable: MemTable,
    sstables: Vec<SSTable>,
    bloomfilter: BloomFilter,
    key_to_id_map: HashMap<String, Uuid>,
    recovery: Recovery,
}

impl RDeeBee {
    pub fn new(compaction_size: usize, dir: String) -> Option<Self> {
        let wal = match Wal::new(&dir) {
            Ok(wal) => wal,
            Err(e) => {
                error!("failed to create the wal: {}", e);
                return None;
            }
        };
        Some(Self {
            compaction_size,
            deebee_dir: dir,
            wal,
            memtable: MemTable::new(),
            sstables: Vec::new(),
            bloomfilter: BloomFilter::new(),
            key_to_id_map: HashMap::new(),
            recovery: Recovery {},
        })
    }

    /// Get the compaction size.
    pub fn get_compaction_size(&self) -> usize {
        self.compaction_size
    }

    /// Get MemTable size
    pub fn get_memtable_size(&self) -> usize {
        self.memtable.size()
    }

    /// Create a new MemTable.
    /// Save the old MemTable into an SSTable.
    pub fn compact_memtable(&mut self) {
        let memtable = mem::replace(&mut self.memtable, MemTable::new());
        let mut sstable = SSTable::from_memtable(&self.deebee_dir, memtable);
        match sstable.save_to_disk() {
            Ok(_) => {}
            Err(e) => error!("failed to write sstable to file: {}", e), // TODO: convert to retry
        }

        self.sstables.push(sstable);
    }

    /// Remove the two oldest SSTables.
    /// Merge them.
    /// Insert into the front of the vector.
    pub fn compact_sstables(&mut self) {
        let s1 = self.sstables.remove(0);
        let s2 = self.sstables.remove(1);
        self.sstables.insert(0, s1.merge(s2));
    }

    fn extract_id(&self, id: &str) -> Result<Uuid, bool> {
        let uuid = match Uuid::from_str(id) {
            Ok(id) => id,
            Err(e) => {
                error!("failed to extract id: {}", e);
                return Err(false);
            }
        };
        Ok(uuid)
    }

    fn get_key_id(&self, key: &str) -> Option<Uuid> {
        self.key_to_id_map.get(key).map(|id| id.to_owned())
    }

    pub fn add_event(&mut self, req: Request) -> Response {
        let mut response = Response::new();
        let action = match req.op.enum_value() {
            Ok(op) => match op {
                Operation::Read => Action::Read,
                Operation::Write => Action::Write,
                Operation::Delete => Action::Delete,
            },
            Err(e) => {
                error!("Invalid Op: {}", e);
                response.status = EnumOrUnknown::new(Status::Invalid_Op);
                return response;
            }
        };
        let mut event = match self.get_key_id(&req.key) {
            Some(id) => {
                self.key_to_id_map.insert(req.key.clone(), id);
                Event::with_id(id, action)
            }
            None => {
                let event = Event::new(action);
                self.bloomfilter.add(event.id());
                event
            }
        };
        if !req.payload.is_empty() {
            event.set_payload(Some(req.payload));
        }
        match self.wal.add_event(event.clone()) {
            Ok(_) => response.status = EnumOrUnknown::new(Status::Ok),
            Err(e) => {
                error!("failed to add event: {}", e);
                response.status = EnumOrUnknown::new(Status::Server_Error);
                return response;
            }
        }
        self.memtable.insert(event);
        response.key = req.key;
        response.op = req.op;
        response
    }

    /// Check if an event is in the database.
    pub fn contains_event(&self, id: &str) -> bool {
        let uuid = match self.extract_id(id) {
            Ok(value) => value,
            Err(value) => return value,
        };
        if self.bloomfilter.find(uuid) {
            return false;
        }
        if self.memtable.contains(uuid) {
            return true;
        }
        for table in &self.sstables {
            if table.contains(uuid) {
                return true;
            }
        }
        false
    }

    /// Get the latest event corresponding to the key.
    /// Return None if key doesn't exist.
    pub fn get_event_by_key(&self, key: &str) -> Option<Response> {
        let uuid = match self.get_key_id(key) {
            Some(uuid) => uuid,
            None => return None,
        };
        if self.bloomfilter.find(uuid) {
            return None;
        }
        let mut res = Response::new();
        for table in self.sstables.iter().rev() {
            let event = table.get(uuid);
            if let Some(event) = event {
                res.key = key.to_string();
                res.status = EnumOrUnknown::new(Status::Ok);
                res.op = match event.action() {
                    storage::Action::Read => EnumOrUnknown::new(Operation::Read),
                    storage::Action::Write => EnumOrUnknown::new(Operation::Write),
                    storage::Action::Delete => EnumOrUnknown::new(Operation::Delete),
                };
                if let Some(payload) = event.payload() {
                    res.payload = payload;
                }
                break;
            }
        }
        Some(res)
    }

    /// Get the entire stream of events if they exist
    /// TODO: Figure out how to do this live from a threadpool or async context, without storing and returning a vector.
    pub fn get_stream_by_key(&self, key: &str) -> Option<Vec<Response>> {
        let mut responses = Vec::new();
        let uuid = match self.get_key_id(key) {
            Some(uuid) => uuid,
            None => return None,
        };
        if self.bloomfilter.find(uuid) {
            return None;
        }
        for table in &self.sstables {
            let event = table.get(uuid);
            if let Some(event) = event {
                let mut res = Response::new();
                res.key = key.to_string();
                res.status = EnumOrUnknown::new(Status::Ok);
                res.op = match event.action() {
                    storage::Action::Read => EnumOrUnknown::new(Operation::Read),
                    storage::Action::Write => EnumOrUnknown::new(Operation::Write),
                    storage::Action::Delete => EnumOrUnknown::new(Operation::Delete),
                };
                if let Some(payload) = event.payload() {
                    res.payload = payload;
                }
                responses.push(res);
            }
        }
        Some(responses)
    }

    pub fn delete_event(&mut self, key: &str) -> Response {
        let mut response = Response::new();
        response.key = key.to_string();
        let id = match self.get_key_id(key) {
            Some(id) => id,
            None => {
                response.status = EnumOrUnknown::new(Status::Invalid_Key);
                return response;
            }
        };
        self.bloomfilter.delete(id);
        match self.wal.delete_event(id) {
            Ok(_) => {
                response.status = EnumOrUnknown::new(Status::Ok);
                response
            }
            Err(e) => {
                error!("failed to add delete event to write ahead log: {}", e);
                response.status = EnumOrUnknown::new(Status::Invalid_Op);
                response
            }
        }
    }

    pub fn recover(&mut self) -> io::Result<()> {
        self.memtable = self.recovery.recover_memtable(&self.deebee_dir)?;
        self.sstables = self.recovery.recover_sstable(&self.deebee_dir)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
