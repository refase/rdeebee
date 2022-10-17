use std::collections::HashMap;

use skiplist::OrderedSkipList;
use uuid::Uuid;

use crate::storage::Event;

/// This is the implementation for log-structured storage
/// We will use this [tutorial](https://adambcomer.com/blog/simple-database/memtable/) to build a MemTable
/// MemTable holds a sorted list of last written records
/// MemTables are compacted to the disk (SSTable) when it reaches a certain size
/// MemTable stores the event(-chain) identifiers in an ordered skiplist.
/// And it stores the actual event in an HashMap.
/// The Ordered SkipList is useful when merging the logs (since it is sorted).
/// The HashMap is useful for mapping each identifier to the actual data associated with that identifier.
/// This is an alternate to using Red Black Trees for memory.
struct MemTable {
    identifiers: OrderedSkipList<Uuid>,
    entries: HashMap<Uuid, Event>,
    size: usize,
}

impl MemTable {
    // We will write to disk once we have reached 100 records.
    const MAX_RECORDS: u64 = 100;

    pub(crate) fn new() -> Self {
        Self {
            identifiers: OrderedSkipList::new(),
            entries: HashMap::new(),
            size: 0,
        }
    }

    /// Insert an event into the database.
    pub(crate) fn insert(&mut self, event: Event) {
        let id = event.id();
        self.identifiers.insert(id);
        self.entries.insert(id, event);
        self.size += 1;
    }

    /// Get an event from the database.
    /// TODO: Build a Bloom Filter to filter out IDs that do not exist.
    pub(crate) fn get_event(&self, transaction: Uuid) -> Option<Event> {
        match self.entries.get(&transaction) {
            Some(event) => Some(event.to_owned()),
            None => None, // NOTE: unlikely branch once the bloom filter is implemented
        }
    }
}

#[cfg(test)]
mod test {
    use skiplist::OrderedSkipList;

    #[test]
    fn skiplist_len_test() {
        let mut skiplist = OrderedSkipList::with_capacity(100);
        skiplist.extend(0..100);
        assert_eq!(skiplist.len(), 100);
    }

    #[test]
    fn skiplist_push_test() {
        let mut skiplist = OrderedSkipList::with_capacity(100);
        skiplist.insert(10);
        skiplist.insert(10);
        skiplist.insert(79);
        skiplist.insert(5);
        skiplist.insert(20);
        skiplist.insert(1);
        assert_eq!(skiplist.len(), 6);
        println!("Skiplist: {:#?}", skiplist);
        skiplist.dedup();
        assert_eq!(skiplist.len(), 5);
        println!("Skiplist: {:#?}", skiplist);
    }
}
