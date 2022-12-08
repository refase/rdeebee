use std::{collections::HashMap, fs};

use serde::Deserialize;

use crate::{election_key_prefix_gen, leader_key_gen};

#[derive(Debug, Deserialize, Clone)]
/// PreConfig holds the config for the cluster that is user defined.
struct PreConfig {
    /// Name of the database.
    dbname: String,
    /// Number of groups in the system.
    groups: usize,
    /// The number of nodes in each group.
    group_size: usize,
    /// Number of nodes to be read for each read.
    /// This corresponds to the number of readers in the system.
    /// However, every leader also has a backup leader.
    reads: usize,
    /// Number of nodes any write has to be written to before the write can be acknowledged as committed.
    _writes: usize,
    /// If the system or a new group is starting up then the nodes need new IDs.
    /// This string to query in order to get a system's ID.
    id_key: String,
    /// However, the node may also be replacing a failed node.
    /// The key prefix that the group leaders will use to advertise failed nodes.
    failover_id_key_prefix: String,
}

impl PreConfig {
    fn new() -> Self {
        let contents = fs::read_to_string("config.yaml").expect("Trouble reading config.yaml");
        let preconf: PreConfig =
            serde_yaml::from_str(&contents).expect("Failed to read configuration");
        preconf
    }
}

#[derive(Debug, Clone)]
/// Config extends the user defined configuration figuring out the details needed to run the cluster.
pub(crate) struct Config {
    preconf: PreConfig,
    /// List of grouping of groups.
    /// A list of lists, where each list correspond to a combination of groups that have a common leader.
    groupings: Vec<Vec<usize>>,
    /// Map that informs which nodes map to which group.
    /// Each node makes an API call at startup to get their ID.
    /// This ID is then used to map them to a group.
    node_group_map: HashMap<usize, usize>,
    /// List of leaders and their corresponding groups.
    /// Maps each leader to the group combination it is a leader for.
    /// /// The key is the index for the groupings field.
    leader_group_map: HashMap<String, usize>,
    /// Leader election prefixes corresponding to each group (index of `groupings`).
    /// election_prefixes -> keys -> string rep of ServiceNode.
    /// election_prefixes -> value -> index of the group in groupings that this leader leads.
    /// Since each group has two leaders there will be two election prefixes for each group
    /// or combination thereof.
    /// The key is the index for the groupings field.
    election_prefixes: HashMap<usize, (String, String)>,
    /// While election prefixes hold the prefixes used to run the elections,
    /// leader keys hold the actual keys that the leaders update with their service node information
    /// upon winning an election.
    /// The key is the index for the groupings field.
    leader_keys: HashMap<usize, (String, String)>,
}

impl Config {
    pub(crate) fn new() -> Self {
        let preconf = PreConfig::new();
        let mut groupings = Vec::new();
        let mut start = 0usize;
        loop {
            let mut group = Vec::new();
            let end = start + preconf.reads;
            for g in start..end {
                group.push(g);
            }
            groupings.push(group);
            start = end;
            if start > preconf.groups {
                break;
            }
        }

        let mut election_prefixes = HashMap::new();
        for g in 0..preconf.group_size {
            election_prefixes.insert(
                g,
                (
                    election_key_prefix_gen!(preconf.dbname.clone(), g, 1),
                    election_key_prefix_gen!(preconf.dbname.clone(), g, 2),
                ),
            );
        }

        let mut node_group_map = HashMap::new();
        let mut start = 0usize;
        for g in 0..preconf.group_size {
            for n in 0..preconf.group_size {
                let node_id = start + n;
                node_group_map.insert(node_id, g);
            }
            start += preconf.group_size;
        }

        let mut leader_keys = HashMap::new();
        for g in 0..preconf.group_size {
            leader_keys.insert(
                g,
                (
                    leader_key_gen!(preconf.dbname.clone(), g, 1),
                    leader_key_gen!(preconf.dbname.clone(), g, 2),
                ),
            );
        }

        Self {
            preconf,
            groupings,
            node_group_map,
            leader_group_map: HashMap::new(),
            election_prefixes,
            leader_keys,
        }
    }

    pub(crate) fn dbname(&self) -> String {
        self.preconf.dbname.clone()
    }

    pub(crate) fn id_keys(&self) -> (String, String) {
        (
            self.preconf.id_key.clone(),
            self.preconf.failover_id_key_prefix.clone(),
        )
    }

    pub(crate) fn group_id(&self, node_id: usize) -> Option<usize> {
        self.node_group_map.get(&node_id).copied()
    }

    pub(crate) fn reads(&self) -> usize {
        self.preconf.reads
    }

    pub(crate) fn _writes(&self) -> usize {
        self.preconf._writes
    }

    pub(crate) fn group_ids(&self, leader: String) -> Option<Vec<usize>> {
        match self.leader_group_map.get(&leader) {
            Some(grp) => Some(
                self.groupings[self.groupings.iter().position(|gvec| gvec.contains(grp))?].clone(),
            ),
            None => None,
        }
    }

    fn groupings_index(&self, group: usize) -> Option<usize> {
        self.groupings.iter().position(|grp| grp.contains(&group))
    }

    pub(crate) fn leader_key(&self, group: usize) -> Option<(String, String)> {
        self.leader_keys.get(&self.groupings_index(group)?).cloned()
    }

    pub(crate) fn election_keys(&self, group: usize) -> Option<(String, String)> {
        self.election_prefixes
            .get(&self.groupings_index(group)?)
            .cloned()
    }
}
