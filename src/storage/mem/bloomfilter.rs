use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    num::Wrapping,
};

use bitvec::{bitvec, prelude::Msb0, vec::BitVec};
use fxhash::hash64;
use uuid::Uuid;

/// This is the custom bloomfilter implementation.
/// This is used to figure out if a key exists in the database before ever searching the SSTables.
/// The bloomfilter guarantees that failed membership test means the key does not exist.
pub(crate) struct BloomFilter {
    arr: BFInner,
}

impl BloomFilter {
    pub(crate) fn new() -> BloomFilter {
        Self {
            arr: BFInner::new(),
        }
    }

    pub(crate) fn find(&self, id: Uuid) -> bool {
        self.arr.find(id)
    }

    pub(crate) fn add(&mut self, id: Uuid) {
        self.arr.add(id);
    }

    pub(crate) fn delete(&mut self, id: Uuid) {
        self.arr.delete(id);
    }
}

// Accomodate 10 million entries with a false positive probability of 1e-7.
// But since we only want to check if something does not exist, there is some room for error?

// check here for the values (for 10M IDs) - https://hur.st/bloomfilter/?n=10000000&p=1.0E-7&m=&k=10
const BF_SIZE: usize = 56_166_771;
const NUM_HASHES: usize = 10;

struct BFInner {
    inner: BitVec<u8, Msb0>,
    size: usize,
}

impl BFInner {
    fn new() -> Self {
        Self {
            inner: bitvec!(u8, Msb0; 0; BF_SIZE),
            size: BF_SIZE,
        }
    }

    fn find(&self, id: Uuid) -> bool {
        for index in self.hash(id) {
            match self.inner.get(index) {
                Some(val) => {
                    if !*val {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }

    fn calculate_hash(id: Uuid) -> u64 {
        let mut s = DefaultHasher::new();
        id.hash(&mut s);
        s.finish()
    }

    fn hash_i(&self, id: Uuid, i: usize) -> usize {
        let hash1 = Wrapping(hash64(&id));
        let hash2 = Wrapping(Self::calculate_hash(id));
        // let hash1 = Wrapping(xx::hash64(id));
        // let hash2 = Wrapping(sea::hash64(id.as_bytes()));
        let hash_mod = Wrapping((i * i) as u64) * hash2;
        (hash1 + hash_mod).0 as usize % self.size
    }

    fn hash(&self, id: Uuid) -> Vec<usize> {
        let mut hashes = Vec::new();
        for i in 0..NUM_HASHES {
            hashes.push(self.hash_i(id, i));
        }
        hashes
    }

    fn add(&mut self, id: Uuid) {
        for index in self.hash(id) {
            self.inner.set(index, true);
        }
    }

    fn delete(&mut self, id: Uuid) {
        for index in self.hash(id) {
            self.inner.set(index, false);
        }
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use super::BloomFilter;

    #[test]
    fn bf_create_test() {
        let bf = BloomFilter::new();
    }

    #[test]
    fn bf_add_test() {
        let mut bf = BloomFilter::new();
        let id = Uuid::new_v4();
        bf.add(id);
    }

    #[test]
    fn bf_find_test() {
        let mut bf = BloomFilter::new();
        let id = Uuid::new_v4();
        bf.add(id);
        assert!(bf.find(id));
    }

    #[test]
    fn bf_delete_test() {
        let mut bf = BloomFilter::new();
        let id = Uuid::new_v4();
        bf.add(id);
        bf.delete(id);
    }
}
