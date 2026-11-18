# TitanDB Architecture

TitanDB is an independent, distributed SQL database engine implemented from scratch in Rust with real paged disk storage, binary write-ahead logging (WAL), B+Tree indexing, MVCC snapshot isolation, deadlock detection, cost/index-aware query planning, and multi-node Raft consensus.

## Layered Engine Design

```
+-------------------------------------------------------------------+
| Interfaces: TCP Wire Protocol  |  HTTP Admin/SQL API  | titan-cli |
+-------------------------------------------------------------------+
| Distributed Layer: Shard Router  <->  Multi-Node Raft Consensus   |
+-------------------------------------------------------------------+
| SQL Layer: Lexer -> Recursive Parser -> Binder -> Planner/Explain |
|            -> Volcano Iterator Execution Engine                   |
+-------------------------------------------------------------------+
| Txn Layer: MVCC Snapshot Manager (xmin/xmax) + Wait-For Deadlock  |
|            + Lock Manager (Shared / Exclusive)                    |
+-------------------------------------------------------------------+
| Storage Layer:                                                    |
|   - Slotted 4096-byte Pages + CRC32 Checksums                     |
|   - Clock Buffer Pool Manager (pin/dirty/evict)                   |
|   - Binary WAL (LSN, REDO/UNDO Crash Recovery, Checkpointing)     |
|   - Persistent B+Tree Index Engine                                |
+-------------------------------------------------------------------+
```

## Storage Trade-offs (B-Tree Slotted Pages vs LSM-Tree)

TitanDB chooses **4KB Slotted Pages + WAL + B+Tree Indexes**:
- Predictable point-read and range-scan latency without write-amplification spikes from background compaction stalls.
- Clean alignment with in-place MVCC tuple version chains and strict page-level CRC32 corruption verification.
- Explicit WAL `fsync` before acknowledging `COMMIT`, guaranteeing crash recovery on restart.

## ACID & Distributed Guarantees

- **Atomicity & Durability:** Mutations append `Insert`/`Update`/`Delete` records to the binary WAL (`fsync` on `COMMIT` when `sync_on_commit=true`). Uncommitted transactions present during a crash are rolled back during startup WAL recovery (`UNDO`), while committed transactions whose dirty pages were not yet flushed are replayed (`REDO`).
- **Isolation:** Snapshot Isolation via MVCC (`xmin`/`xmax` tuple timestamps) plus strict Two-Phase Locking (`Shared`/`Exclusive` row locks) with DFS cycle detection in a Wait-For Graph to abort deadlocks deterministically.
- **Distributed Consensus:** Multi-node Raft cluster (`Leader`, `Candidate`, `Follower`) with randomized election timeouts, term persistence, log matching (`prev_log_index`, `prev_log_term`), quorum `commit_index`, and snapshot transfer over real TCP sockets.
