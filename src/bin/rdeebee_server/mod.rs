use std::{borrow::BorrowMut, sync::Arc};

use anyhow::anyhow;
use parking_lot::RwLock;
use rdeebee::{wire_format::operation, RDeeBee};
use tracing::error;

#[derive(Clone)]
pub(crate) struct RDeeBeeServer {
    rdeebee: Arc<RwLock<RDeeBee>>,
}

impl RDeeBeeServer {
    pub(crate) fn new(compaction_size: usize, dir: String) -> anyhow::Result<Self> {
        let rdeebee = RDeeBee::new(compaction_size, dir)?;
        Ok(Self {
            rdeebee: Arc::new(RwLock::new(rdeebee)),
        })
    }

    pub(crate) fn recover(&self) -> anyhow::Result<()> {
        match self.rdeebee.as_ref().borrow_mut().try_write() {
            Some(mut guard) => Ok({
                guard.recover()?;
            }),
            None => Err(anyhow!("failed to recover")),
        }
    }

    pub(crate) fn compact_memtable(&self) -> anyhow::Result<()> {
        match self.rdeebee.as_ref().try_write() {
            Some(mut guard) => match guard.try_memtable_compact() {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!("{:#?}", e)),
            },
            None => Err(anyhow!("Failed to acquire lock in compact_memtable")),
        }
    }

    pub(crate) fn get_memtable_size(&self) -> Option<usize> {
        match self.rdeebee.as_ref().try_read() {
            Some(guard) => Some(guard.get_memtable_size()),
            None => None,
        }
    }

    pub(crate) fn compact_sstables(&self) -> anyhow::Result<()> {
        match self.rdeebee.as_ref().try_write() {
            Some(mut guard) => match guard.try_sstables_compact() {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow!("{:#?}", e)),
            },
            None => Err(anyhow!("Failed to acquire lock in compact_memtable")),
        }
    }

    pub(crate) fn get_event(&self, key: &str) -> Option<operation::Response> {
        match self.rdeebee.as_ref().try_read() {
            Some(guard) => Some(guard.get_event_by_key(key)),
            None => None,
        }
    }

    pub(crate) fn add_event(&self, request: operation::Request) -> anyhow::Result<()> {
        match self.rdeebee.as_ref().try_write() {
            Some(mut guard) => {
                guard.add_event(request);
                Ok(())
            }
            None => {
                error!("Failed to acquire lock in compact_memtable");
                Err(anyhow!("Failed to acquire lock in compact_memtable"))
            }
        }
    }

    pub(crate) fn delete_event(&self, request: operation::Request) -> anyhow::Result<()> {
        match self.rdeebee.as_ref().try_write() {
            Some(mut guard) => {
                guard.delete_event(request);
                Ok(())
            }
            None => {
                error!("Failed to acquire lock in compact_memtable");
                Err(anyhow!("Failed to acquire lock in compact_memtable"))
            }
        }
    }
}
