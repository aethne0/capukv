# CapuKV

**CapuKV** is a small, strongly-consistent, fault-tolerant distributed key-value store built on my own Rust implementation of the [Raft consensus algorithm](https://raft.github.io/). It uses Rust, tokio, gRPC, Protocol Buffers, and RocksDB, and is currently deployed at home with Docker, Nomad, Consul, and Traefik.

While not production-ready, it has been tested against node failures and harsh network conditions like partitions, packet loss, reordering, message duplication, and latency spikes.

## What it does

1. **Fault tolerance** – stays available and correct despite node or network failures (within reason).
2. **Strong consistency** – behaves like a single-threaded state machine (linearizability, isolation, ordering). This makes the system easier to reason about and lets other layers relax consistency requirements, relying on this layer for coordination.

## Motivation

I built **CapuKV** to learn more about distributed systems, databases, and systems programming. It also serves as a metadata layer for a personal distributed object storage project.

**CapuKV** inspired by systems like etcd, TiKV, and HashiCorp’s Raft backend, which provide storage for distributed databases like TiDB, or consistent coordination layers for systems like Kubernetes or HashiCorp products (Consul, Nomad, Vault).

## Why it matters

Distributed systems often rely on a consistent, authoritative layer to maintain correctness and availability, because coordinating state across multiple nodes without one is often ad-hoc, messy, and error-prone.

By providing strong consistency, something like **CapuKV** allows other parts of the system to relax their own consistency requirements, reducing coordination overhead where possible. This makes the system easier to reason about and lets high-throughput components operate efficiently without sacrificing correctness.

Some other real world examples of distributed consistent layers include:

- **Google Chubby**, a distributed lock service that uses Paxos
- **Apache Zookeeper**, a general distributed coordination system using Zab
- **Ceph's MON**, the Paxos-based metadata layer for Ceph's distributed block, file, and object storage
- **CockroachDB**, a distributed SQL database that uses Raft
