# RDeeBee

Follow this [blog series](https://towardsdev.com/a-data-system-from-scratch-in-rust-part-1-an-idea-3911059883ec) for more details on this project.

This system is inspired by Martin Kleppman's arguments that Event Sourcing system and Databases are rather two sides of the same coin. It's an area that fascinates me and I wanted to work on the internals of a system like this as far as possible. This desire gave birth to `rdeebee`.

The overall idea behind this project is to implement a distributed event database that also provides `change data capture`. Something that would combine the command and query (CQRS designs) side databases/message buses a bit.

The overall goal is to learn about design and design tradeoffs by making them.

## Discover nodes of the database

We use [`consul`](https://developer.hashicorp.com/consul) for this. Consul can be set up using this [guide](https://developer.hashicorp.com/consul/docs/k8s/installation/install#custom-installation). The configuration reference is [here](https://developer.hashicorp.com/consul/docs/k8s/helm#helm-chart-reference).

The main issue I faced is a permission [issue](https://github.com/hashicorp/consul/issues/10096#issuecomment-857113725).

## Sequencing the Writes

We use Redis `incr` function to atomically generate sequence numbers and expose that through a web server.

### Testing concurrent access

Forward the redis port from the container (and change the address in `main.go` to `localhost`).

```bash
kubectl port-forward redis-0 6379:6379
```

Write a `source.txt`

```txt
URL = localhost:6379
```

On two separate windows, start two clients:

```bash
while true; do curl -K source.txt >> test1.log; done
```

and

```bash
while true; do curl -K source.txt >> test2.log; done
```

Check that there are no common lines:

```bash
comm -1 -2 --nocheck-order --total test1.log test2.log
```

## Working Branches

- The `main` branch is the development branch.
- Each sub-component, like `database`, has their own branch.
- The sub-branches are not standalone. Rather they are used as a way of checkpointing.
