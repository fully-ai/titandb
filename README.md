# TitanDB

**TitanDB** is an independent, distributed SQL database engine implemented from scratch in Rust with real paged storage internals, CRC32 checksum verification, Clock buffer pool, WAL crash recovery, B+Tree indexes, MVCC snapshot isolation, deadlock detection, and multi-node Raft consensus.

> **Maturity:** Alpha (`v0.1.0-alpha.1`) — Storage engine, WAL REDO/UNDO recovery, MVCC + Wait-For deadlock detector, SQL planner/executor, 3-node TCP Raft cluster & failover, Wire/HTTP servers, and Python/Go/TS clients verified with integration tests.

## Core Internals

- **Paged Storage Engine (`src/storage/page.rs`, `disk.rs`, `buffer.rs`)**:
  - 4096-byte slotted pages verified by CRC32 checksums on every disk read.
  - Fixed-capacity Clock replacement buffer pool with pin counts, dirty flushing, and eviction counters.
- **Write-Ahead Log & Recovery (`src/storage/wal.rs`)**:
  - CRC32-framed binary WAL with monotonically increasing LSNs, `fsync` on `COMMIT`/`CHECKPOINT`, and startup REDO/UNDO recovery.
- **B+Tree Indexing (`src/storage/btree.rs`)**:
  - Configurable-order persistent B+Tree supporting leaf/internal node splits, exact point lookup, sibling-linked range scans, and deletion.
- **MVCC & Locking (`src/txn/mod.rs`)**:
  - Snapshot Isolation via `xmin`/`xmax` tuple chains + 2PL lock manager with DFS cycle-detection deadlock abortion.
- **SQL Engine (`src/sql/`, `src/engine.rs`)**:
  - Hand-written lexer & recursive-descent parser with `CREATE DATABASE`, `USE`, `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `INNER JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `COUNT`/`SUM`/`MIN`/`MAX`/`AVG`, `EXPLAIN`, `PREPARE`/`EXECUTE`, and transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).
- **Distributed Raft Consensus (`src/raft.rs`)**:
  - Real multi-node TCP Raft state machine (`RequestVote`, `AppendEntries`, `InstallSnapshot`, randomized election timeouts, leader failover, deterministic SQL log application) + consistent FNV-1a `ShardRouter`.
- **Client Ecosystem (`clients/`)**:
  - `titan-cli` interactive REPL, TCP length-prefixed Wire Protocol + HTTP JSON API, and tested Python (`clients/python/titandb_client.py`), Go (`clients/go/titandb.go`), and TypeScript (`clients/ts/titandb.ts`) clients.

## Quick Start

```bash
git clone https://github.com/fully-ai/titandb
cd titandb
./scripts/build.sh
./scripts/test.sh
./target/release/titandb --data-dir data/db --wire-addr 127.0.0.1:7440 --http-addr 127.0.0.1:7441
```

In a second terminal:
```bash
./target/release/titan-cli 127.0.0.1:7440
```

## Documentation & Security

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md)
- [SECURITY.md](SECURITY.md)
- Apache License 2.0 ([LICENSE](LICENSE))
