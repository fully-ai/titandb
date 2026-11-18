# ADR 001 — Paged Slotted Storage, WAL Recovery, MVCC, and Raft Consensus

- **Status:** Accepted
- **Date:** 2026-11-18

## Context
We need an independent database engine written in Rust that guarantees crash durability, concurrent transactional isolation, index-accelerated SQL execution, and real multi-node replication without delegating storage or execution to PostgreSQL or SQLite.

## Decision
1. Implement **4096-byte slotted pages** (`Page`) verified on every read via CRC32 checksums (`src/storage/page.rs`), managed by a fixed-capacity **Clock Buffer Pool** (`src/storage/buffer.rs`).
2. Implement a **CRC32-framed binary WAL** (`src/storage/wal.rs`) with LSN ordering, checkpoints, and REDO/UNDO recovery tests simulating abrupt crashes.
3. Use **pure Rust standard library networking (`std::net::TcpListener`/`TcpStream`)** plus `serde`/`serde_json` to keep dependency footprint minimal, compile fast, and ease `cargo audit`/license hygiene.
4. Provide explicit TCP wire protocol + HTTP admin/query endpoints + native Rust/Python/Go/TypeScript clients tested end-to-end against live servers.
