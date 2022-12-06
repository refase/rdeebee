mod cluster_ops;
mod protos;
mod storage;
mod storageops;

pub use cluster_ops::*;
pub use protos::*;
pub use storageops::*;

// TODO: Init functions.
// There are set up ops that need to happen before the server starts up.
// Like setting up the different etcd keys like
// - failover_key, id_key etc ...
// TODO: Setup wait
// When starting a new cluster, the system must wait for all the nodes to start before
// registering failover keys
