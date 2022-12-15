use std::{env, net::Ipv4Addr, str, str::FromStr, sync::Arc, time::Duration};

use etcd_client::{Client, EventType, GetOptions, LockOptions, PutOptions, WatchOptions};
use parking_lot::RwLock;
use tokio::{select, time::interval};
use tracing::{debug, error, info};

use crate::{group_add_lock, group_membership_key_gen, id_key_lock};

use super::{config::Config, error::ClusterNodeError, registry::Registry, NodeType, ServiceNode};

enum KeyType {
    Election,
    Leader,
}

#[derive(Clone)]
pub struct Node {
    /// Client for the etcd cluster.
    client: Client,
    /// The node name and IP address of this node.
    svc_node: ServiceNode,
    /// The node ID of this node.
    node_id: Option<usize>,
    /// The group key where members register themselves to the group.
    group_key: Option<String>,
    /// The cluster configuration.
    config: Config,
    /// This node's etcd cluster lease ID.
    lease: i64,
    /// Time between lease refreshes.
    refresh_interval: u64,
    /// The type of node this is - leader or only member.
    nodetype: NodeType,
    /// If the node is a leader then it has to watch its peers.
    registry: Arc<RwLock<Option<Registry>>>,
}

impl Node {
    pub async fn new() -> Self {
        let lease_ttl = env::var("LEASE_TTL")
            .expect("Lease TTL undefined")
            .parse::<i64>()
            .expect("Invalid lease ttl");
        let refresh_interval = env::var("REFRESH_INTERVAL")
            .expect("Refresh interval undefined")
            .parse::<u64>()
            .expect("Invalid refresh interval");

        if lease_ttl <= refresh_interval as i64 {
            error!(
                "Lease refresh interval({}) larger than lease ttl({})",
                refresh_interval, lease_ttl
            );
        }
        let etcd = env::var("ETCD").expect("etcd address undefined");

        let mut client = Client::connect([etcd], None)
            .await
            .expect("failed to create client");

        let node = env::var("NODE").expect("Node name undefined");

        let address = Ipv4Addr::from_str(
            env::var("ADDRESS")
                .expect("Node address undefined")
                .as_str(),
        )
        .expect("Invalid IPv4 address");

        let svc_node = ServiceNode { node, address };

        let lease = client
            .lease_grant(lease_ttl, None)
            .await
            .expect("Lease grant failed")
            .id();

        let config = Config::new();

        Self {
            client,
            svc_node,
            node_id: None,
            group_key: None,
            config,
            lease,
            refresh_interval,
            nodetype: NodeType::Member,
            registry: Arc::new(RwLock::new(None)),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.nodetype == NodeType::Leader
    }

    /// Add a new service node to the group.
    pub(crate) fn add_endpoint(&self, endpoint: String) -> Result<(), ClusterNodeError> {
        if self.nodetype == NodeType::Member {
            return Err(ClusterNodeError::InvalidFunctionAttempt(
                "add_endpoint".to_owned(),
            ));
        }

        let mut reg = self.registry.as_ref().write();
        match reg.as_mut() {
            Some(reg) => {
                reg.add_endpoint(endpoint)?;
                Ok(())
            }
            None => Err(ClusterNodeError::InvalidState(
                "Leader doesn't have registry initialized".to_owned(),
            )),
        }
    }

    /// Replace the entire group.
    pub(crate) fn update_registry(&self, endpoints: Vec<String>) -> Result<(), ClusterNodeError> {
        if self.nodetype == NodeType::Member {
            return Err(ClusterNodeError::InvalidFunctionAttempt(
                "update_registry".to_owned(),
            ));
        }
        let mut reg = self.registry.as_ref().write();
        match reg.as_mut() {
            Some(reg) => {
                reg.update_registry(endpoints)?;
                Ok(())
            }
            None => Err(ClusterNodeError::InvalidState(
                "Leader doesn't have registry initialized".to_owned(),
            )),
        }
    }

    fn group_key(&self) -> Result<String, ClusterNodeError> {
        match &self.group_key {
            Some(g) => Ok(g.clone()),
            None => Err(ClusterNodeError::InvalidState(
                "Group ID is not known".to_owned(),
            )),
        }
    }

    fn flip_nodetype(&mut self) {
        match self.nodetype {
            NodeType::Member => self.nodetype = NodeType::Leader,
            NodeType::Leader => self.nodetype = NodeType::Member,
        }
    }

    async fn register(&mut self, group_id: usize) -> Result<(), ClusterNodeError> {
        let putoptions = PutOptions::new().with_lease(self.lease);
        let svc_node = serde_json::to_string(&self.svc_node)?;
        let group_membership_key = group_membership_key_gen!(self.config.dbname(), group_id);
        let grp_key = format!("{}-{:#?}", group_membership_key, svc_node.clone());
        let resp = self
            .client
            .put(grp_key.clone(), svc_node, Some(putoptions))
            .await?;
        self.group_key = Some(grp_key);

        info!("Registration successful: {resp:#?}");
        Ok(())
    }

    async fn node_id(&mut self) -> Result<usize, ClusterNodeError> {
        let (id_key, failover_key) = self.config.id_keys();
        // First check if any of the leaders are reporting failed group memebers.
        let getoptions = GetOptions::new().with_prefix();
        debug!("Lock Options");
        let resp = self
            .client
            .get(failover_key, Some(getoptions))
            .await
            .expect("Failed to get node ID");

        debug!("Locked");
        let kvs = resp.kvs();
        if !kvs.is_empty() {
            for kv in kvs {
                let key = kv.key_str().expect("Failed to get key").to_owned();
                let group = kv
                    .value_str()
                    .expect("Failed to get node ID")
                    .parse::<usize>()
                    .expect("Failed to parse node ID");
                // Attempt to join group.
                if self.join_group(key, group).await {
                    return Ok(group);
                };
            }
        }
        debug!("Getting new ID");

        // At this point we have not been reassigned any old ID from a failed node.
        // So we will ask for a new ID.
        self.new_id(id_key).await
    }

    // Lock the group joining key and add the node to the group.
    async fn join_group(&mut self, key: String, group_id: usize) -> bool {
        let group_lock_key = group_add_lock!(group_id);
        // We expect to finish the op in 10 seconds.
        let lock_options = LockOptions::new().with_lease(self.lease);
        let _resp = match self.client.lock(group_lock_key, Some(lock_options)).await {
            Ok(resp) => resp,
            Err(e) => {
                error!("Error locking group add key: {}", e);
                return false;
            }
        };

        if let Err(e) = self.register(group_id).await {
            error!("Error registering node to {group_id}: {e}");
            return false;
        }

        // If added successfully delete the key that indicates this particular requirement.
        // so other nodes do not try to join this group.
        let _resp = match self.client.delete(key, None).await {
            Ok(resp) => resp,
            Err(e) => {
                error!("Error deleting lock key: {}", e);
                return false;
            }
        };

        true
    }

    // Get a new ID from the etcd cluster.
    async fn new_id(&mut self, id_key: String) -> Result<usize, ClusterNodeError> {
        // The node expects get the ID in 10 seconds.
        let lock_options = LockOptions::new().with_lease(self.lease);
        debug!("New ID lock");
        let _resp = self
            .client
            .lock(id_key_lock!(), Some(lock_options))
            .await
            .expect("Failed to get node ID");
        debug!("ID key: {}", id_key.clone());

        let resp = self
            .client
            .get(id_key, None)
            .await
            .expect("Failed to get node ID");
        debug!("New ID get response: {resp:#?}");
        let kv = resp.kvs();
        debug!("New id response");
        debug!("New ID kv: {kv:#?}");

        if !kv.is_empty() {
            let val = kv[0]
                .value_str()
                .expect("Failed to get the latest ID")
                .parse::<u64>()
                .expect("Failed to parse ID");
            Ok(val as usize)
        } else {
            error!("New ID kv=0 error");
            Err(ClusterNodeError::ServerCreationError(
                "Failed to read node ID".to_owned(),
            ))
        }
    }

    /// keepalive keeps the etcd lease for this member alive.
    /// This lease is used to both watch for group leaders if node is member
    /// and watch for peers.
    async fn keepalive(&mut self) -> Result<(), ClusterNodeError> {
        let (mut lease_keeper, mut lease_keepalive_stream) = self
            .client
            .lease_keep_alive(self.lease)
            .await
            .expect("failed to start keep alive channels");
        lease_keeper.keep_alive().await?;
        if let Some(msg) = lease_keepalive_stream.message().await? {
            debug!("lease {:?} keep alive, new ttl {:?}", msg.id(), msg.ttl());
        }
        Ok(())
    }

    /// Get peers.
    async fn get_peers(&self) -> Result<Vec<String>, ClusterNodeError> {
        if self.node_id.is_none() {
            return Err(ClusterNodeError::InvalidState(
                "Node ID is not known".to_owned(),
            ));
        }
        let getoptions = GetOptions::new().with_prefix();
        let mut client = self.client.clone();

        let group_key = self.group_key()?;

        let get_resp = client
            .get(group_key.clone(), Some(getoptions))
            .await
            .expect("Failed to get service");

        let kvs = get_resp.kvs();
        match kvs.len() {
            0 => Err(ClusterNodeError::InvalidState(
                "Invalid group key".to_owned(),
            )),
            _ => {
                let mut endpoints = Vec::new();
                for kv in kvs {
                    let ep = kv.value_str().expect("failed to get value");
                    info!("Key: {}, Service Node: {ep}", group_key);
                    endpoints.push(ep.to_owned());
                }
                Ok(endpoints)
            }
        }
    }

    /// Watch the peers.
    async fn watch_peers(&self) -> Result<(), ClusterNodeError> {
        let mut group_missing_node = Vec::new();
        let peers = self.get_peers().await?;
        for peer in peers {
            self.add_endpoint(peer)?;
        }

        // A leader has to count peers in multiple groups.
        // A member only checks its own group.
        let leader = serde_json::to_string(&self.svc_node)?;
        let groups = match self.config.group_ids(leader) {
            Some(groups) => groups,
            None => {
                let node_id = match self.node_id {
                    Some(nid) => nid,
                    None => {
                        return Err(ClusterNodeError::InvalidState(
                            "Node ID undefined".to_owned(),
                        ))
                    }
                };
                let group_id = match self.config.group_id(node_id) {
                    Some(gid) => gid,
                    None => {
                        return Err(ClusterNodeError::InvalidState(
                            "Group ID undefined".to_owned(),
                        ))
                    }
                };
                vec![group_id]
            }
        };

        let mut client = self.client.clone();
        let svc_node = serde_json::to_string(&self.svc_node)?;
        for group in groups {
            let group_membership_key = group_membership_key_gen!(self.config.dbname(), group);
            let grp_key = format!("{}-{:#?}", group_membership_key, svc_node.clone());

            let watchoptions = WatchOptions::new().with_prefix();
            let (mut peer_watcher, mut peer_watchstream) = client
                .watch(grp_key.clone(), Some(watchoptions.clone()))
                .await
                .expect("Peer Watch failed");
            info!("Created peer watcher");
            peer_watcher
                .request_progress()
                .await
                .expect("Peer watcher request progress failed");

            if let Some(msg) = peer_watchstream
                .message()
                .await
                .expect("Failed to watch peers")
            {
                for event in msg.events() {
                    match event.event_type() {
                        EventType::Put => {
                            if let Some(event) = event.kv() {
                                let svc_node = event.value_str()?;
                                info!("Added node: {svc_node}");
                                self.add_endpoint(svc_node.to_owned())?;
                            }
                        }
                        EventType::Delete => {
                            info!("One member has died");
                            let peers = self.get_peers().await?;
                            self.update_registry(peers)?;
                        }
                    }
                }
            }

            // The leader only can report failed nodes.
            if self.nodetype == NodeType::Leader {
                if let Some(reg) = self.registry.as_ref().read().as_ref() {
                    if reg.member_count() < self.config.reads() {
                        let (_, failover_key) = self.config.id_keys();
                        let svc_node = serde_json::to_string(&self.svc_node)?;
                        let key = format!("{}-{}", failover_key, svc_node);
                        group_missing_node.push((key, format!("{group}")));
                    }
                }
            }
        }
        if !group_missing_node.is_empty() {
            for (key, grp_str) in group_missing_node {
                let put_resp = client.put(key, grp_str, None).await?;
                info!("{put_resp:#?}");
            }
        }
        Ok(())
    }

    fn node_id_from_registry(&self) -> Result<usize, ClusterNodeError> {
        match self.node_id {
            Some(nid) => Ok(nid),
            None => Err(ClusterNodeError::InvalidState(
                "Node ID undefined".to_owned(),
            )),
        }
    }

    fn fetch_keys(&self, typ: KeyType) -> Result<(String, String), ClusterNodeError> {
        let group_id = match self.config.group_id(self.node_id_from_registry()?) {
            Some(gid) => gid,
            None => {
                return Err(ClusterNodeError::InvalidState(
                    "Group ID undefined".to_owned(),
                ))
            }
        };
        match typ {
            KeyType::Leader => match self.config.leader_key(group_id) {
                Some(keys) => Ok(keys),
                None => Err(ClusterNodeError::InvalidState(
                    "Leader keys not found".to_owned(),
                )),
            },
            KeyType::Election => match self.config.election_keys(group_id) {
                Some(keys) => Ok(keys),
                None => Err(ClusterNodeError::InvalidState(
                    "Election keys not found".to_owned(),
                )),
            },
        }
    }

    fn leader_keys(&self) -> Result<(String, String), ClusterNodeError> {
        self.fetch_keys(KeyType::Leader)
    }

    fn election_keys(&self) -> Result<(String, String), ClusterNodeError> {
        self.fetch_keys(KeyType::Election)
    }

    /// Get the leaders in the system.
    pub fn get_leaders(&self) -> Result<Vec<ServiceNode>, ClusterNodeError> {
        let mut client = self.client.clone();
        let getoptions = GetOptions::new().with_prefix();
        let leader_keys = self.leader_keys()?;
        let mut leaders = Vec::new();

        for i in 0..2 {
            let leader_key = if i == 0 {
                leader_keys.0.clone()
            } else {
                leader_keys.1.clone()
            };
            let handle = tokio::runtime::Handle::current();
            let resp = handle.block_on(client.get(leader_key, Some(getoptions.clone())))?;

            // let resp = client.get(leader_key, Some(getoptions.clone())).await?;

            let kvs = resp.kvs();
            if !kvs.is_empty() {
                let leader = kvs[0].value_str()?.to_owned();
                let service_node: ServiceNode = serde_json::from_str(&leader)?;
                leaders.push(service_node);
            }
        }
        match leaders.is_empty() {
            true => Err(ClusterNodeError::InvalidState("No Leader found".to_owned())),
            false => Ok(leaders),
        }
    }

    async fn campaign(&mut self, election_key: String) -> Result<(), ClusterNodeError> {
        let leader_keys = self.leader_keys()?;
        let election_keys = self.election_keys()?;
        // Lock with lease.
        let lock_options = LockOptions::new().with_lease(self.lease);
        let resp = self
            .client
            .lock(election_key.clone(), Some(lock_options))
            .await?;
        let key = match str::from_utf8(resp.key()) {
            Ok(key) => key,
            Err(e) => return Err(ClusterNodeError::StringifyError(e)),
        };
        info!("Locking with lease: {}", key);

        let svc_node = serde_json::to_string(&self.svc_node)?;

        let leader_key = if election_keys.0 == election_key.clone() {
            leader_keys.0.clone()
        } else {
            leader_keys.1.clone()
        };

        let put_resp = self.client.put(leader_key.clone(), svc_node, None).await?;
        info!("Put response: {:#?}", put_resp);
        Ok(())
    }

    /// Watch the group leaders continuously.
    /// If any of them fail, then return
    async fn watch_group_leaders(&self) -> Result<Option<String>, ClusterNodeError> {
        let leader_keys = self.leader_keys()?;
        let election_keys = self.election_keys()?;
        // Check if the keys already exist.
        let getoptions = GetOptions::new().with_prefix();
        let mut client = self.client.clone();

        for i in 0..2 {
            let election_key = if i == 0 {
                election_keys.0.clone()
            } else {
                election_keys.1.clone()
            };
            // Does the key already exist?
            let resp = client
                .get(election_key.clone(), Some(getoptions.clone()))
                .await
                .expect("Failed to query queue");
            let kvs = resp.kvs();
            if kvs.is_empty() {
                return Ok(Some(election_key));
            }
        }

        let watchoptions = WatchOptions::new().with_prefix();
        // Add the first key to the watcher.
        let (mut election_watcher, mut election_watchstream) = client
            .watch(election_keys.0.clone(), Some(watchoptions.clone()))
            .await
            .expect("Election watch failed");
        info!("Created election watcher");
        election_watcher
            .request_progress()
            .await
            .expect("Election watcher request progress failed");

        election_watcher
            .watch(election_keys.1.clone(), Some(watchoptions.clone()))
            .await
            .expect("Election watch failed");
        info!("Added the second election key");

        if let Some(msg) = election_watchstream
            .message()
            .await
            .expect("failed to watch the stream")
        {
            for event in msg.events() {
                if event.event_type() == EventType::Delete {
                    // These are the only two possibilities.
                    if let Some(kv) = event.kv() {
                        if kv
                            .key_str()
                            .expect("failed to get the key")
                            .contains(&leader_keys.0.clone())
                        {
                            return Ok(Some(election_keys.0.clone()));
                        }
                    }
                }
            }
        }
        // No delete type event was encountered.
        // But there are no errors either.
        Ok(None)
    }

    // // pub fn run_cluster_node(&mut self) -> BoxFuture<Result<(), ClusterNodeError>> {
    // pub async fn run_cluster_node(&mut self) -> anyhow::Result<()> {
    pub async fn run_cluster_node(&mut self) -> Result<(), ClusterNodeError> {
        // Register the node
        let node_id = self.node_id().await?;
        info!("Node ID: {node_id}");
        let group_id = match self.config.group_id(node_id) {
            Some(group_id) => group_id,
            None => {
                return Err(ClusterNodeError::InvalidState(
                    "Group ID invalid".to_owned(),
                ))
            }
        };
        info!("Group ID: {group_id}");
        self.register(group_id).await?;
        info!("Registered");

        // Start operations.
        let mut interval = interval(Duration::from_secs(self.refresh_interval));
        loop {
            match self.nodetype {
                // If this member is not a leader then:
                // Keep the lease alive.
                // Watch for new peers.
                // Watch the election. Campaign to become the leader.
                NodeType::Member => select! {
                    _ = interval.tick() => self.keepalive().await?,
                    Ok(Some(key)) = self.watch_group_leaders() => {
                        match self.campaign(key).await {
                            Ok(_) => self.flip_nodetype(),
                            Err(e) => match e {
                                ClusterNodeError::EtcdError(e) => match e {
                                    etcd_client::Error::GRpcStatus(e) => info!("Did not become leader: {}", e),
                                    _ => {
                                        error!("System error while campaigning: {}", e);
                                        return Err(ClusterNodeError::EtcdError(e));
                                        // return Err(anyhow!("{}", e));
                                    },
                                },
                                _ => {
                                    error!("Error while campaigning: {}", e);
                                    return Err(e);
                                    // return Err(anyhow!("{}", e));
                                },
                            }
                        }
                    },
                    _ = self.watch_peers() => {},
                },
                // If this node is the leader,
                // Keep the lease alive.
                // Keep track of the peers.
                NodeType::Leader => select! {
                    _ = interval.tick() => self.keepalive().await?,
                    _ = self.watch_peers() => {},
                },
            }
        }
    }
}
