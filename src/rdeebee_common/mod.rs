mod consul;

pub use consul::*;

#[derive(Debug, Clone)]
pub enum Role {
    Leader,
    Candidate,
    Node, // default
}