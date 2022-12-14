use std::collections::HashMap;

use skiplist::{ordered_skiplist::Iter, OrderedSkipList};
use uuid::Uuid;

use crate::Event;

pub(crate) struct MemtableIterator<'a> {
    memtable: &'a MemTable,
    index: Iter<'a, Uuid>,
}

impl<'a> MemtableIterator<'a> {
    fn new(memtable: &'a MemTable) -> Self {
        Self {
            memtable,
            index: (&memtable.identifiers).into_iter(),
        }
    }
}

/// The MemtableIterator returns events in ascending order of transaction IDs
// TODO: This can be optimized further by storing the events with the transaction IDs in the skiplist.
// This skiplist does not seem to support that so that may require custom implementation.
impl<'a> Iterator for MemtableIterator<'a> {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(id) = self.index.next() {
            self.memtable.get_event(*id)
        } else {
            None
        }
    }
}

/// This is the implementation for log-structured storage
/// We will use this [tutorial](https://adambcomer.com/blog/simple-database/memtable/) to build a MemTable
/// MemTable holds a sorted list of last written records
/// MemTables are compacted to the disk (SSTable) when it reaches a certain size
/// MemTable stores the event(-chain) identifiers in an ordered skiplist.
/// And it stores the actual event in an HashMap.
/// The Ordered SkipList is useful when merging the logs (since it is sorted).
/// The HashMap is useful for mapping each identifier to the actual data associated with that identifier.
/// This is an alternate to using Red Black Trees for memory.
pub(crate) struct MemTable {
    identifiers: OrderedSkipList<Uuid>,
    entries: HashMap<Uuid, Event>,
    size: usize,
}

impl MemTable {
    pub(crate) fn new() -> Self {
        Self {
            identifiers: OrderedSkipList::new(),
            entries: HashMap::new(),
            size: 0,
        }
    }

    /// Does this event exist in the MemTable
    pub(crate) fn contains(&self, id: Uuid) -> bool {
        self.identifiers.contains(&id)
    }

    /// Get memtable size in bytes
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// Insert an event into the database.
    pub(crate) fn insert(&mut self, event: Event) {
        let id = event.id();
        self.identifiers.insert(id);
        let sz = event.size();
        self.entries.insert(id, event);
        self.size += sz;
    }

    /// Get an event from the database.
    pub(crate) fn get_event(&self, transaction: Uuid) -> Option<Event> {
        self.entries.get(&transaction).map(|event| event.to_owned())
    }
}

impl<'a> IntoIterator for &'a MemTable {
    type Item = Event;
    type IntoIter = MemtableIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        MemtableIterator::new(self)
    }
}

#[cfg(test)]
mod test {
    use crate::storage::{
        event::{Action, Event},
        mem::memtable::MemTable,
    };
    use rand::{distributions::Alphanumeric, Rng};

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
    fn memtable_len_test() {
        let mut memtable = MemTable::new();
        let mut events = Vec::new();
        for _ in 0..100 {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect();
            let payload = bincode::serialize(&s).unwrap();
            let mut event = Event::new(Action::Read);
            event.set_payload(Some(payload));
            events.push(event);
        }

        for event in events {
            memtable.insert(event);
        }
        assert_eq!(memtable.len(), 100);
    }

    #[test]
    fn memtable_find_test() {
        let mut memtable = MemTable::new();
        let mut events = Vec::new();
        for i in 0..5 {
            events.push(Event::new(Action::Read));
        }

        let event = events.get(0).unwrap().to_owned();

        for event in events {
            memtable.insert(event);
        }

        let res = memtable.get_event(event.id());
        assert_eq!(res, Some(event));
    }

    #[test]
    fn memtable_size_test() {
        let mut memtable = MemTable::new();
        println!("Size at the beginning: {} bytes", memtable.size());
        insert_events(&mut memtable, create_events(10));
        println!("Size at the end: {} bytes", memtable.size());
    }

    #[test]
    fn memtable_iterator_test() {
        let mut memtable = MemTable::new();
        for i in 0..5 {
            memtable.insert(Event::new(Action::Read));
        }

        for event in &memtable {
            println!("{:#?}", event);
        }
    }
}
