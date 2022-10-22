use rdeebee::RDeeBee;

pub(crate) struct RDeeBeeServer {
    rdeebee: RDeeBee,
}

impl RDeeBeeServer {
    pub(crate) fn new(compaction_size: usize, dir: String) -> Option<Self> {
        let rdeebee = RDeeBee::new(compaction_size, dir)?;
        Some(Self { rdeebee })
    }

    pub(crate) fn compact_memtable(&mut self) {
        self.rdeebee.compact_memtable();
    }
}
