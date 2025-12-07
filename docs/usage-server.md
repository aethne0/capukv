# Usage (CapuKV Servers)

**CapuKV** is fairly simple to bootstrap and offers a few different ways to discover peers.

### 1. Known peer URIs

If you know each node's address/URI ahead of time you can simply pass them in as a list.

```bash
# Run using an explicit peer list - notice how the list can include the node itself.
# This is for convenience when writing deployment configs and is optional.

# for a cluster of size=3
capukv --dir /data/capukv \
    --api-addr 10.0.0.1:5700 \
    --raft-addr 10.0.0.1:5800 \
    --peer-uris http://10.0.0.1:5800,http://10.0.0.2:5800,http://10.0.0.3:5800
```

### 2. Dynamic discovery using DNS

If you are deploying in an environment with DHCP or using an orchestrator that dynamically assigns IPs+Ports, it can be very difficult to know ahead of time what addresses nodes/jobs will be running on. If you have an orchestrator or service-discovery layer or the like that provides DNS, CapuKV can instead use DNS A+SRV records to identify peers.

```bash
# Run DNS lookup - this will expect there to be A and SRV entries for all peers in the prospective cluster
# The node will requery DNS until it sees nodes equal to "expect".
# It is OK if the DNS entries are only made after the nodes start up, they will simply wait and requery periodically
# until they see the whole expected cluster.

# for a cluster of size=3
capukv --dir /data/capukv \
    --api-addr 0.0.0.0:5700 \
    --raft-addr 0.0.0.0:5800 \
    --dns "capukv.services.consul." \  # NOTE: These records should correspond to --raft-addr of each node
    --expect 3

# For reference, this would expect your DNS to look something like this
$ dog capukv.service.consul
A capukv.service.consul. 0s   10.2.16.27
A capukv.service.consul. 0s   10.2.16.26
A capukv.service.consul. 0s   10.2.16.30

$ dog capukv.service.consul SRV
SRV capukv.service.consul. 0s   1 1 "0a02101a.addr.monke-ca-toronto-01.consul.":30525
SRV capukv.service.consul. 0s   1 1 "0a021018.addr.monke-ca-toronto-01.consul.":27085
SRV capukv.service.consul. 0s   1 1 "0a02101b.addr.monke-ca-toronto-01.consul.":28205

```

## Flags

### `--dir`

The directory that all persistent state will be kept in by CapuKV. If you start CapuKV pointing at a directory that already has CapuKV state in it, CapuKV will simply resume from that state. If you want to "copy" a CapuKV instance from one machine to another you simply have to copy and transfer this entire directory.

### `--api-addr`

This is the socket-addr that the **client-api** gRPC server should bind to, that is, the server that will serve client requests (KV operations).

### `--raft-addr`

This is the socket-addr that the **raft** gRPC server should bind to, that is, the server that will be used to receive raft messages from the cluster.

### `--redirect-uri`

If passed in, this is the URI that will be gossiped to other nodes when/if this node becomes leader. It is used for redirects. It should ultimately "lead" to --api-addr (but will often be some externally accessable URI or the like, or a DNS name, etc).

### `--peer-uris`

List of concrete URIs of peers within the cluster. `http://1.2.3.4:5678`, `http://some-node-3.raft.local:1111`, etc.

### `--dns`

DNS name to query for peers.

### `--expect`

Used with `--dns` - number of peer records to wait to see when initially querying DNS.
