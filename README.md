# CapuKV

**CapuKV** is a small, strongly-consistent, fault-tolerant distributed key-value store built on my own implementation of the [Raft consensus algorithm](https://raft.github.io/). It uses [Rust](https://rust-lang.org/), [tokio](https://tokio.rs/), [gRPC](https://grpc.io/), [Protocol Buffers](https://protobuf.dev/), and [RocksDB](https://rocksdb.org/), and is currently deployed at home with [Docker](https://www.docker.com/), [Nomad](https://www.hashicorp.com/en/products/nomad), [Consul](https://www.hashicorp.com/en/products/consul), and [Traefik](https://traefik.io/traefik).

While not production-ready, it has been tested against node failures and harsh network conditions like partitions, packet loss, reordering, message duplication, and latency spikes.

![sample image](https://github.com/aethne0/capukv/tree/master/other/readmeclisample.png)

## What does it do?

CapuKV is a key-value store that is _distributed_, meaning it runs on _cluster_ of computers instead of on a single machine. It provides two main things:

1. **Fault tolerance** – stays available and correct even during node or network failures (within reason).
2. **Strong consistency** – behaves largely as if it were a single-threaded state machine (linearizability, isolation, ordering). This makes the system easier to reason about, less error prone and lets other layers relax their own consistency requirements, relying instead on this layer for coordination

## How does it work?¹

At the core of **CapuKV** is Raft, a distributed algorithm that allows a group of computers to continuously agree on some shared _log_. The _log_ is an ordered set of entries, each representing a single state change - for example, incrementing a shared counter or inserting a key-value pair. Because we can rely on Raft to ensure that each computer has the same _log_, each computer can independently apply the state changes recorded in the log sequentially to its own instance of the shared state machine. The state machine could be as simple as a single shared counter or lock, or as complex as a full-fledged database. In **CapuKV's** case it is comparable to an in-memory hashmap with a few additional features.

CapuKV has a message-bus-based [Raft core](https://github.com/aethne0/capukv/blob/master/capukv/src/raft/node.rs) that asynchronously handles, receives and sends out Raft messages to peers. The log entries, once committed by the Raft algorithm, are then applied to a state machine. Client requests are served by coordinating with the cluster, appending log entries to the log, and writing to (or reading from) the state machine. Reads can also skip the coordination step, which decreases latency but also forfeits linearizability gaurantees.

A few implementation specific details:

- **Async Rust with the Tokio runtime**: CapuKV spends most of its time either waiting for network messages or disk IO, which the async/coroutine model maps well onto.
- **gRPC and Protocol Buffers**: Raft and client communications are handled by two separate gRPC servers, allowing for the two to be handled by different network interfaces for performance or security reasons, and tuned and configured differently if need be.
- **RocksDB persistent log**: The Raft log itself is stored using RocksDB, an LSM-tree based embedded key-value store that is optimized for write-heavy workloads.
- **Deployment**: Personally I deploy CapuKV as a docker image using Nomad for orchestration. I use Consul + Traefik for service discovery and to forward gRPC client messages to the correct node (the current Raft leader) as well as retry/redirect in the event of node or network failure.

## Why did you make this?

I built **CapuKV** to learn more about distributed systems, databases, and systems programming, as well as deployment, orchestration, and a bit of larger system-design. It also serves as a metadata layer for a [personal distributed object storage project](https://github.com/aethne0/banana-store).

**CapuKV** is inspired by systems like [etcd](https://github.com/etcd-io/etcd), [TiKV](https://github.com/tikv/tikv) (both Raft based) and [HashiCorp’s Raft backend](https://github.com/hashicorp/raft), which provide a storage-backend for distributed databases like [TiDB](https://github.com/pingcap/tidb), or a consistent coordination layer for systems like [Kubernetes](https://kubernetes.io/) or various [HashiCorp](https://www.hashicorp.com/en) products ([Consul](https://www.hashicorp.com/en/products/consul), [Nomad](https://www.hashicorp.com/en/products/nomad), [Vault](https://www.hashicorp.com/en/products/vault)).

## What is it for?

Distributed systems often rely on a consistent, authoritative layer to maintain correctness and availability, because coordinating state across multiple nodes without one is often ad-hoc, messy, and error-prone.

By providing strong consistency, something like **CapuKV** allows other parts of the system to relax their own consistency requirements, reducing coordination overhead where possible. This makes the system easier to reason about and lets high-throughput components operate efficiently without sacrificing correctness.

Some other real world examples of distributed consistent systems include:

- **[Google Chubby](https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf)**, a distributed lock service that uses Paxos
- **[Apache Zookeeper](https://zookeeper.apache.org/)**, a general distributed coordination system using Zab
- **[Ceph's](https://ceph.io/en/) [MON](https://docs.ceph.com/en/latest/man/8/ceph-mon/)**, the Paxos-based metadata layer for Ceph's distributed block, file, and object storage
- **[CockroachDB](https://www.cockroachlabs.com/)**, a distributed SQL database that uses Raft

---

_1. CapuKV was originally implemented using roughly three to seven small new-world monkeys, but testing would quickly result in the client receiving fictitious gRPC status codes like `17:INSUFFICIENT_BANANAS`, even under healthy network conditions._
