use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

mod config;
mod error;
mod node;
mod registry;

pub use node::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeType {
    Member,
    Leader,
}

#[macro_export]
macro_rules! election_key_prefix_gen {
    ($dbname:expr, $group:expr, $num:expr) => {
        format!("election-{}-group-{}-leader-{}", $dbname, $group, $num)
    };
}

#[macro_export]
macro_rules! leader_key_gen {
    ($dbname:expr, $group:expr, $num:expr) => {
        format!("leader-{}-group-{}-{}", $dbname, $group, $num)
    };
}

#[macro_export]
macro_rules! group_membership_key_gen {
    ($dbname:expr, $group:expr) => {
        format!("member-{}-group-{}", $dbname, $group)
    };
}

#[macro_export]
macro_rules! group_add_lock {
    ($group_id:expr) => {
        format!("group-add-lock-{}", $group_id)
    };
}

#[macro_export]
macro_rules! id_key_lock {
    () => {
        stringify!("id-key-lock")
    };
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ServiceNode {
    node: String,
    address: Ipv4Addr,
}
