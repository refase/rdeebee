use std::{
    borrow::{Borrow, BorrowMut},
    sync::Arc,
};

use anyhow::anyhow;
use parking_lot::RwLock;
use rdeebee::{wire_format::operation, Node, RDeeBee, ServiceNode};
use tracing::error;

#[derive(Clone)]
pub(crate) struct RDeeBeeServer {
    rdeebee: Arc<RwLock<RDeeBee>>,
    cluster_node: Arc<RwLock<Node>>,
}

impl RDeeBeeServer {
    pub(crate) async fn new(compaction_size: usize, dir: String) -> anyhow::Result<Self> {
        Ok(Self {
            rdeebee: Arc::new(RwLock::new(RDeeBee::new(compaction_size, dir)?)),
            cluster_node: Arc::new(RwLock::new(Node::new().await)),
        })
    }

    pub(crate) fn get_node(&self) -> Arc<RwLock<Node>> {
        self.cluster_node.clone()
    }

    pub(crate) fn recover(&self) -> anyhow::Result<()> {
        match self.rdeebee.as_ref().borrow_mut().try_write() {
            Some(mut guard) => Ok(guard.recover()?),
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
        self.rdeebee
            .as_ref()
            .try_read()
            .map(|guard| guard.get_memtable_size())
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
        self.rdeebee
            .as_ref()
            .try_read()
            .map(|guard| guard.get_event_by_key(key))
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

    pub(crate) async fn run_cluster(&self) -> anyhow::Result<()> {
        let node = self.cluster_node.clone();
        let mut node = node.as_ref().borrow_mut().write();
        node.run_cluster_node().await?;
        Ok(())
    }

    pub(crate) async fn get_leaders(&self) -> anyhow::Result<Vec<ServiceNode>> {
        let leaders = self
            .cluster_node
            .as_ref()
            .borrow()
            .read()
            .get_leaders()
            .await?;
        Ok(leaders)
    }

    pub(crate) fn is_leader(&self) -> bool {
        self.cluster_node.as_ref().borrow().read().is_leader()
    }
}
