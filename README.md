# capukv

**[Usage and docs](https://github.com/aethne0/capukv/blob/master/docs/usage-server.md)**

**[Releases and containers](https://git.monke.ca/monke/-/packages/container/capukv/dev)**

---

<p align="center">
    <img src="https://monke.ca/assets/monke-square.webp" alt="capu"/>
</p>

**capukv** is a small, strongly-consistent, fault-tolerant, distributed key-value store built using an implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf). While not production-ready, it has been tested against node failures and harsh network conditions like partitions, packet loss, reordering, message duplication, and latency spikes.

I built **capukv** to learn more about distributed systems, databases, and systems programming, as well as deployment, orchestration, and a bit of larger system-design. It also serves as a metadata layer for a [personal distributed object storage project](https://github.com/aethne0/banana-store).

**capukv** is made possible thanks to: 
- [Rust](https://rust-lang.org/)
- [tokio](https://tokio.rs/)
- [tonic](https://github.com/hyperium/tonic) (grpc) & [prost](https://github.com/tokio-rs/prost) (protocol buffers)
- [RocksDB](https://rocksdb.org/)

![sample image](https://raw.githubusercontent.com/aethne0/capukv/refs/heads/master/docs/readmeclisample.png)
