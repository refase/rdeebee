use std::collections::HashMap;

use skiplist::{ordered_skiplist::Iter, OrderedSkipList};
use uuid::Uuid;

use crate::storageops::{Action, Event};

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
        let id = event.transaction_id();
        self.identifiers.insert(id);
        let sz = event.size();
        self.entries.insert(id, event);
        self.size += sz;
    }

    /// Get an event from the database.
    pub(crate) fn event(&self, transaction: Uuid) -> Option<Event> {
        self.entries.get(&transaction).map(|event| event.to_owned())
    }

    /// Get all writes from the memtable.
    /// This is an expensive function that will walk through the entire memtable
    /// and copy every write event.
    pub(crate) fn writes(&self) -> Vec<Event> {
        let mut wrs = Vec::new();
        for (_, event) in self.entries.iter() {
            if event.action() == Action::Write(0) || event.action() == Action::Write(1) {
                wrs.push(event.clone());
            }
        }
        wrs
    }
}

/// MemTableIterator iterates over a MemTable reference.
pub(crate) struct MemTableIterator<'a> {
    memtable: &'a MemTable,
    index: Iter<'a, Uuid>,
}

impl<'a> MemTableIterator<'a> {
    fn new(memtable: &'a MemTable) -> Self {
        Self {
            memtable,
            index: (&memtable.identifiers).into_iter(),
        }
    }
}

/// MemTableIterator returns transaction ID of events in ascending order.
impl<'a> Iterator for MemTableIterator<'a> {
    type Item = Event;
    fn next(&mut self) -> Option<Self::Item> {
        match self.index.next() {
            Some(id) => self.memtable.event(*id),
            None => None,
        }
    }
}

/// Get an iterator from MemTable reference.
impl<'a> IntoIterator for &'a MemTable {
    type Item = Event;
    type IntoIter = MemTableIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        MemTableIterator::new(self)
    }
}
