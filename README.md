# raft-kv — Distributed Key-Value Store with Raft Consensus

A replicated, strongly-consistent key-value store built from scratch on the
**Raft consensus algorithm** — leader election, log replication, and
snapshotting (log compaction) — with a gRPC transport, durable storage on
[bbolt](https://github.com/etcd-io/bbolt), and a client that transparently
survives rolling node failures.

> **Headline demo:** kill the leader mid-flight and *no committed data is lost* —
> a new leader is elected in ~hundreds of ms and writes keep flowing.
> Run `make demo`.

---

## Why this exists

Distributed consensus is the backbone of every serious datastore (etcd,
Consul, CockroachDB, TiKV). This project implements the algorithm end-to-end —
not a wrapper around a library — to demonstrate depth in:

- **Consensus & correctness:** terms, quorums, the log-matching property, and
  the commit rules that make Raft safe (§5.3–§5.4 of the Raft paper).
- **Concurrency:** a single-mutex core with per-peer replicators and a
  dedicated apply loop; the full suite passes under `go test -race`.
- **Systems engineering:** durable write-ahead state, log compaction via
  snapshots + `InstallSnapshot`, and a pluggable transport.

## Architecture

```
                      ┌────────────────────────────────────────────┐
   client (pkg/client)│  leader-aware, retries + redirects on hint │
     put/get/delete   └───────────────┬────────────────────────────┘
                                     │ gRPC (KVService)
                    ┌────────────────┴──────────────────┐
                    ▼                ▼                  ▼
              ┌──────────┐     ┌──────────┐       ┌──────────┐
              │  node n1 │◀───▶│  node n2 │◀────▶ │  node n3 │   gRPC (RaftService)
              ├──────────┤     ├──────────┤       ├──────────┤   RequestVote /
              │  Raft    │     │  Raft    │       │  Raft    │   AppendEntries /
              │  core    │     │  core    │       │  core    │   InstallSnapshot
              ├──────────┤     ├──────────┤       ├──────────┤
              │  KV FSM  │     │  KV FSM  │       │  KV FSM  │   (deterministic
              ├──────────┤     ├──────────┤       ├──────────┤    state machine)
              │  bbolt   │     │  bbolt   │       │  bbolt   │   (durable log +
              └──────────┘     └──────────┘       └──────────┘    snapshot)
```

### Package layout

| Path                       | Responsibility                                               |
|----------------------------|--------------------------------------------------------------|
| `internal/raft`            | The consensus core: election, replication, snapshotting.     |
| `internal/kv`              | The replicated FSM (a key-value map) implementing `raft.FSM`.|
| `internal/storage`         | `Bolt` (durable) and `Memory` (test) `raft.Storage`.         |
| `internal/transport`       | `raft.Transport`: gRPC (prod) + in-memory `Network` (tests). |
| `internal/server`          | The client-facing `KVService` gRPC handlers.                 |
| `pkg/client`               | Leader-aware Go client with failover + retries.              |
| `cmd/raftkvd`              | The node daemon.                                             |
| `cmd/raftkvctl`            | A small CLI.                                                 |
| `proto/`                   | gRPC service + message definitions.                          |

## How Raft is implemented here

- **Leader election** with randomized timeouts *and a pre-vote round*
  (Ongaro thesis §9.6) so a node isolated by a partition can't disrupt a stable
  leader by inflating its term on rejoin.
- **Log replication** with the §5.3 **fast-backup** optimization: a rejected
  `AppendEntries` returns `ConflictTerm`/`ConflictIndex` so the leader
  resynchronizes a lagging follower in O(#terms) round-trips instead of O(#entries).
- **Commit safety** (§5.4.2): a new leader commits a no-op entry in its term,
  and only entries from the current term are counted toward advancing
  `commitIndex`.
- **Persistence** (§5): `currentTerm`, `votedFor`, and the log are fsync-durable
  (bbolt write transactions) before an RPC is answered.
- **Snapshotting / log compaction** (§7): once enough entries accumulate, the
  FSM is snapshotted and the log prefix is truncated; a follower that has fallen
  behind the leader's log start is caught up with `InstallSnapshot`.
- **Linearizable reads via a leader lease:** reads are served only by a leader
  that has committed an entry in its current term *and* holds a quorum lease (a
  majority of peers acknowledged it within one election timeout), so a
  partitioned old leader cannot return stale data. Followers return a leader
  hint for redirection.
- **Config validation** rejects impossible setups at construction (unknown/
  duplicate peers, `HeartbeatInterval >= ElectionTimeoutMin`, inverted election
  bounds) instead of panicking at runtime.

## Correctness hardening (review-driven)

This implementation went through an adversarial review pass. The following
safety/liveness issues were found and fixed, each with a regression test:

- **Single-node clusters now commit.** `commitIndex` is advanced on the leader's
  own log when there are no peers (was: `Propose` blocked forever). — `TestSingleNodeProposeCommits`
- **No log corruption on a compacted `PrevLogIndex`.** `AppendEntries` rejects
  references below the snapshot boundary, and `truncateFrom` refuses to touch
  the committed prefix.
- **Persistence-before-ack is enforced.** A follower never returns `Success`,
  and a node never grants a vote, unless the log/state was durably written
  first. — `TestProposeFailsOnStorageError`
- **`InstallSnapshot` can't regress applied state.** It guards on `lastApplied`,
  persists the snapshot *before* mutating the FSM, and applies the §7
  retain-or-discard rule based on the boundary term.
- **Batched, single-fsync appends** and the corrected upward `firstIndexOfTerm`
  restore the §5.3 fast-backup optimization.
- **Per-RPC timeouts** on vote/append/snapshot calls so a black-holed peer can't
  pin a replicator or stall an election.

## Quick start

```bash
# build the binaries
make build

# run the flagship failover demo (3 nodes, kill the leader, data survives)
make demo
```

Run a 3-node cluster manually:

```bash
./bin/raftkvd --id n1 --listen 127.0.0.1:9001 \
  --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir ./data/n1 &
./bin/raftkvd --id n2 --listen 127.0.0.1:9002 \
  --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir ./data/n2 &
./bin/raftkvd --id n3 --listen 127.0.0.1:9003 \
  --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir ./data/n3 &

PEERS=n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003
./bin/raftkvctl --peers $PEERS put foo bar
./bin/raftkvctl --peers $PEERS get foo      # -> bar
./bin/raftkvctl --peers $PEERS del foo
```

## Testing

```bash
make test     # all unit + integration tests
make race     # the whole suite under the race detector
make cover    # coverage summary
```

The suite covers, on both the in-memory transport and the **real gRPC stack**:

- single-node and multi-node leader election (exactly one leader),
- log replication convergence across all replicas,
- **leader failover with no committed-data loss** (`TestLeaderFailoverNoDataLoss`,
  `TestEndToEndFailover`),
- rolling restarts while keeping quorum,
- snapshot-based catch-up for a node that fell behind (`TestSnapshotCatchUp`).

## Trade-offs & scope

- Membership is **static** (fixed peer set). Dynamic reconfiguration
  (joint consensus, §6) is a natural extension.
- Reads use a **leader lease** (assumes bounded clock drift). A full
  **ReadIndex** path (confirm with a fresh heartbeat round) is the stricter
  upgrade.
- Client writes are **not yet idempotent** across retries; adding request IDs +
  an FSM dedup table (§6.3) gives exactly-once semantics for non-idempotent ops.
- `InstallSnapshot` ships the snapshot in one message (bounded to 64 MiB;
  chunking fields exist in the proto for a streaming upgrade).
- The transport is unauthenticated (`insecure` creds) for local demos; mTLS +
  auth interceptors slot in behind the same `Transport` interface.

### Known follow-ups (tracked)

- `HandleInstallSnapshot` mutates the FSM under the node lock while the apply
  loop mutates it outside the lock; there is no data race (the KV FSM has its
  own `RWMutex`), but a superseded `Apply` could theoretically land after a
  `Restore`. The clean fix is to funnel all FSM mutation through the single
  apply goroutine (an `installing` guard / snapshot-apply message).

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (Raft), 2014.
- Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis) — pre-vote, ReadIndex.
