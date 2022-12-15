# RDeeBee

Follow this [blog series](https://towardsdev.com/a-data-system-from-scratch-in-rust-part-1-an-idea-3911059883ec) for more details on this project.

Look up my detailed [article](https://bhattacharya-ratnadeep.medium.com/distributed-linearizability-without-consensus-e3f92b4d638f) on possibly a new way of sharding and replicating.

This system is inspired by Martin Kleppman's arguments that Event Sourcing system and Databases are rather two sides of the same coin. It's an area that fascinates me and I wanted to work on the internals of a system like this as far as possible. This desire gave birth to `rdeebee`.

The overall idea behind this project is to implement a distributed event database that also provides `change data capture`. Something that would combine the command and query (CQRS designs) side databases/message buses a bit.

The overall goal is to learn about design and design tradeoffs by making them.

## Using etcd

The etcd cluster is used for 3 purposes:

    - Elect two leaders for each group.
    - Register the node-to-group maps.
    - Get a globally unique sequence number for each write.

## Testing Natively

### Run the server:

```bash
TRACE_LEVEL=info LEASE_TTL=60 REFRESH_INTERVAL=50 ETCD=localhost:2379 NODE=Server-1 ADDRESS=192.168.10.10 cargo run --bin rdb-server
```

### Run the client:

#### Read

```bash
TRACE_LEVEL=info ETCD=128.105.146.151:2379 COUNTER_KEY=counter LOCK_KEY=lock SERVER_IP=127.0.0.1 SERVER_PORT=2048 cargo run --bin rdb-client -- -k Deep read
```

#### Write

```bash
TRACE_LEVEL=info ETCD=128.105.146.151:2379 COUNTER_KEY=counter LOCK_KEY=lock SERVER_IP=127.0.0.1 SERVER_PORT=2048 cargo run --bin rdb-client -- -k Deep -p "First write" write
```

#### Delete

```bash
TRACE_LEVEL=info ETCD=128.105.146.151:2379 COUNTER_KEY=counter LOCK_KEY=lock SERVER_IP=127.0.0.1 SERVER_PORT=2048 cargo run --bin rdb-client -- -k Deep delete
```

## Working Branches

- The `main` branch is the development branch.
- Each sub-component, like `database`, has their own branch.
- The sub-branches are not standalone. Rather they are used as a way of checkpointing.
