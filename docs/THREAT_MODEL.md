# TitanDB STRIDE Threat Model

## Assets & Trust Boundaries
- **Assets:** Database files (`*.tdb`), WAL (`wal.log`), backup snapshots, SQL query execution engine, Raft inter-node RPCs, admin HTTP/TCP endpoints.
- **Boundaries:** Untrusted client SQL/wire frames -> Parser/Server; Untrusted Raft peer frames -> Cluster handler; Disk pages -> Page CRC32 validator.

## STRIDE Mitigations
| Threat | Mitigation |
| :--- | :--- |
| Spoofing / Unauthorized Access | Optional bearer API token / cluster shared secret verified via constant-time compare before SQL/Admin execution |
| Data Corruption / Tampering | Every 4KB page carries CRC32 checksum verified on read; every WAL frame carries CRC32 + LSN check preventing silent partial-write corruption |
| SQL / Parser Crash DoS | Bounded frame length (16 MiB), recursion depth guard (`max_depth=64`) in recursive-descent SQL parser, fuzz-tested lexer/parser |
| Path Traversal in Backup/DB Name | Strict alphanumeric/`_-` validation on database names (`CREATE DATABASE`) and sanitized paths |
| Deadlock Starvation DoS | Wait-For Graph cycle detection immediately aborts deadlock victims and releases held locks |
