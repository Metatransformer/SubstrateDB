# Cluster replication

SubstrateDB cluster mode divides data into a fixed number of independent Raft groups. A scope is the pair `(namespace, logical partition)`. The first eight bytes of `SHA256(length(namespace) || namespace || length(partition) || partition)`, interpreted as an unsigned big-endian integer, select `hash % shard_count`. Length framing prevents ambiguous scope pairs, and the mapping is stable as long as the configured shard count does not change.

Each shard has an explicit voter set. Every configured node opens an empty local Raft group for every shard so it can later be added as a learner, but only a shard's voters and learners receive that shard's replicated data. Elections are independent, so different shards may have different leaders. A node outside a shard's group discovers the leader through the configured peer origins and forwards reads and writes. Mutations use OpenRaft `client_write`; reads first use OpenRaft's linearizable quorum barrier and then read the leader's local shard engine. A node that cannot reach a quorum returns an error and never reports a local-only write as successful.

## Persistence and recovery

Raft votes, committed indexes and log entries are persisted incrementally in immediate-durability redb transactions; membership is recorded in the Raft history. Append batches contain at most four entries so maximum-sized commands fit the 8 MiB RPC body. Heartbeats use 500 ms and elections 1.5–3 seconds; the library's 50 ms default caused large durable catch-up batches to time out during process-restart testing. The application engine transaction commits before the applied boundary is persisted. If a process stops in that narrow interval, OpenRaft reapplies the same command after restart; engine request-ID deduplication makes that replay deterministic.

Snapshots contain the engine image plus the corresponding applied log ID and membership. Snapshot construction and installation are serialized with state-machine application. OpenRaft controls snapshot creation, transfer, installation, log compaction, heartbeats, elections, and membership log entries. Importing a consensus snapshot is not an operator restore and does not pause jobs or schedules.

Persisted Raft membership is authoritative on restart. The initial `placements` configuration is used only to form pristine groups and as a discovery hint for forwarded requests; it does not overwrite an existing membership record.

## Configuration and bootstrap

All nodes must use the same nonzero shard count, shared secret, node-ID/address map, and initial placement map. Node IDs and shard count must remain stable across restarts. The shard count is fixed: membership movement changes replicas, not the number of shards or the scope-to-shard hash mapping.

The cluster file accepts an optional `placements` object whose keys are shard IDs and whose values are voter ID arrays:

```json
{
  "node_id": 1,
  "advertise": "http://10.0.0.11:7432",
  "peers": {
    "1": "http://10.0.0.11:7432",
    "2": "http://10.0.0.12:7432",
    "3": "http://10.0.0.13:7432",
    "4": "http://10.0.0.14:7432"
  },
  "data_dir": "/var/lib/substratedb",
  "shards": 2,
  "secret": "replace-with-a-long-random-shared-secret",
  "bootstrap": true,
  "placements": {
    "0": [1, 2, 3],
    "1": [2, 3, 4]
  }
}
```

Every explicit placement must contain known configured node IDs and must use an odd-sized group of at least three voters. A single voter is allowed only when exactly one cluster node is configured, for local development. The map must include exactly one nonempty entry for every shard. Omitting `placements` preserves the original behavior: every configured peer is a voter for every shard, including existing two-node test or development configurations.

Use the corresponding `node_id`, `advertise`, and `data_dir` on each node; keep the peer and placement maps identical. Set `bootstrap: true` on exactly one configured node for initial cluster formation and `false` on the others. The bootstrap node contacts a designated voter for each shard only after that node's advertised listener is reachable, with bounded background retries. This also permits the bootstrap node to be outside a shard's voter set. A pristine designated voter verifies that the requested voter set matches its own placement before initialization.

Run each node with its cluster file:

```sh
SUBSTRATE_ADMIN_TOKEN='replace-with-at-least-24-bytes' \
  substrate --listen 0.0.0.0:7432 --cluster-config /etc/substratedb/cluster.json
```

The advertised and peer addresses are HTTP(S) origins reachable by every member. Mount `Cluster::router()` alongside the public application router. Its Raft control routes exist only below `/internal/raft/...`; there are no `/v1` routes in the cluster router.

## Moving shard membership

Membership movement is an explicit operator action; there is no automatic rebalancer. The new node must already be in every node's discovery `peers` map from process startup. Send the complete desired voter set to the current leader of the shard:

```sh
curl --fail-with-body \
  -H 'content-type: application/json' \
  -H 'x-substrate-raft-secret: replace-with-a-long-random-shared-secret' \
  --data '{"voters":[2,3,4]}' \
  http://10.0.0.11:7432/internal/raft/0/membership
```

Only the current shard leader accepts the operation, and only one reconfiguration may run per shard at a time. For every new voter the leader first calls OpenRaft's blocking learner-add operation and waits for log catch-up. It then asks OpenRaft to commit the joint-consensus transition and final voter set. Removed voters are not retained as learners. Do not stop the old voter until this endpoint reports the updated membership.

A successful response reports the shard, leader, actual voters, and learners:

```json
{
  "shard": 0,
  "leader_id": 2,
  "voters": [2, 3, 4],
  "learners": []
}
```

The operation has a bounded deadline. A timeout or lost response can have an ambiguous outcome because a membership entry may already be committed. Do not immediately submit a different voter set. Inspect cluster metrics on the surviving nodes, find the current leader, and compare the reported `voters` before retrying the same desired set.

## Internal security and bounds

Every Raft RPC, forwarded command, initialization request, membership request, and snapshot trigger requires the shared secret in `x-substrate-raft-secret`. Authentication is applied before route handlers. Deployments must also restrict the internal listener at the network layer and use HTTPS or a trusted private network, because the shared header is a bearer credential.

Internal request and response bodies are limited to 8 MiB, commands to 1 MiB, and snapshot chunks to 1 MiB. HTTP calls, quorum operations, forwarding retries, bootstrap retries, and membership changes have bounded deadlines. The authenticated control endpoints are:

- `POST /internal/raft/{shard}/initialize` with `{"voters":[...]}` for pristine bootstrap
- `POST /internal/raft/{shard}/membership` with `{"voters":[...]}` for operator movement
- `POST /internal/raft/{shard}/snapshot/trigger`

Metrics report each local Raft group's actual `voters` and `learners`. An excluded, never-added node reports an empty voter set for that shard while still being able to forward application requests to its leader.

Placement and membership movement provide fixed-shard capacity sharding. They do not provide online shard-count changes, automatic balancing, arbitrary live discovery edits, or unsafe state copying between nodes.
