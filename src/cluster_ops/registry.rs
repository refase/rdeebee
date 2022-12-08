use super::{error::ClusterNodeError, ServiceNode};

/// A registry of the nodes that belong to this group.
pub(crate) struct Registry {
    _group_id: usize,
    registry: Vec<ServiceNode>,
}

impl Registry {
    pub(crate) fn _new(_group_id: usize) -> Self {
        Self {
            _group_id,
            registry: vec![],
        }
    }

    /// Add a new service node to the group.
    pub(crate) fn add_endpoint(&mut self, endpoint: String) -> Result<(), ClusterNodeError> {
        let ep: ServiceNode = serde_json::from_str(&endpoint)?;
        self.registry.push(ep);
        Ok(())
    }

    /// Replace the entire group.
    pub(crate) fn update_registry(
        &mut self,
        endpoints: Vec<String>,
    ) -> Result<(), ClusterNodeError> {
        let mut new_registry = vec![];
        for ep in &endpoints {
            let endpoint = serde_json::from_str(ep)?;
            new_registry.push(endpoint);
        }
        self.registry = new_registry;
        Ok(())
    }

    /// Get the number of nodes in the group
    pub(crate) fn member_count(&self) -> usize {
        self.registry.len()
    }
}
