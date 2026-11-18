use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use titandb::engine::TitanEngine;
use titandb::error::TitanError;
use titandb::protocol::{WireRequest, WireResponse};
use titandb::raft::{RaftNode, RaftRole, ShardRouter};
use titandb::server::TitanServer;
use titandb::sql::parser::parse_sql;
use titandb::storage::btree::{BPlusTree, IndexKey};
use titandb::storage::buffer::BufferPool;
use titandb::storage::disk::DiskManager;
use titandb::storage::page::SlottedPage;
use titandb::storage::wal::{WalManager, WalOp};
use titandb::txn::{LockMode, TxnManager, Value};

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("titandb_{tag}_{ts}"));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn test_slotted_page_crc32_corruption_and_buffer_pool() {
    let dir = temp_dir("page_crc");
    let db_file = dir.join("test.tdb");
    let disk = Arc::new(DiskManager::open(&db_file).unwrap());
    let pool = BufferPool::new(disk.clone(), 4);

    for i in 0..8 {
        let mut pg: SlottedPage = pool.new_page().unwrap();
        let pid = pg.page_id();
        let slot = pg
            .insert_tuple(format!("row-payload-{i}").as_bytes())
            .unwrap();
        assert_eq!(slot, 0);
        pool.unpin_page(pid, true, Some(pg)).unwrap();
    }
    pool.flush_all().unwrap();
    assert!(pool.stats().evictions >= 4);

    // Corrupt 1 byte inside page 2 on disk directly and assert CRC32 catches it!
    {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_file)
            .unwrap();
        f.seek(SeekFrom::Start(4096 * 2 + 50)).unwrap();
        f.write_all(&[0xFF]).unwrap();
    }
    let err = disk
        .read_page(2)
        .err()
        .expect("expected corruption detection");
    assert!(matches!(err, TitanError::Corruption(_)), "got {err:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_btree_split_range_and_delete() {
    let mut tree = BPlusTree::new(4);
    for k in (0..120).rev() {
        tree.insert(IndexKey::Int(k), (k * 10) as u64);
    }
    assert_eq!(tree.len_keys(), 120);
    assert_eq!(tree.get(&IndexKey::Int(42)), vec![420]);

    let range = tree.range(Some(&IndexKey::Int(20)), Some(&IndexKey::Int(25)));
    assert_eq!(range, vec![200, 210, 220, 230, 240, 250]);

    assert!(tree.remove(&IndexKey::Int(22), 220));
    assert!(tree.get(&IndexKey::Int(22)).is_empty());
}

#[test]
fn test_wal_crash_recovery_redo_and_undo() {
    let dir = temp_dir("wal_crash");
    {
        let eng = TitanEngine::open(&dir).unwrap();
        eng.execute_sql(
            "CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT, balance INT)",
            &[],
            None,
        )
        .unwrap();
        eng.execute_sql("INSERT INTO accounts VALUES (1, 'alice', 100)", &[], None)
            .unwrap();
        eng.checkpoint().unwrap();

        // Now write committed row WITHOUT calling checkpoint(), simulating sudden crash before page flush!
        eng.execute_sql("INSERT INTO accounts VALUES (2, 'bob', 250)", &[], None)
            .unwrap();

        // Also append an UNCOMMITTED transaction directly to WAL to verify UNDO discards it on recovery!
        let wal = WalManager::open(dir.join("wal.log"), true).unwrap();
        wal.append(999, WalOp::Begin).unwrap();
        wal.append(
            999,
            WalOp::InsertTuple {
                table: "default.accounts".into(),
                row_id: 99,
                row_json: serde_json::to_vec(&vec![
                    Value::Int(99),
                    Value::Text("ghost".into()),
                    Value::Int(9999),
                ])
                .unwrap(),
            },
        )
        .unwrap();
    } // Engine dropped cleanly WITHOUT checkpointing row 2!

    // Reopen and verify REDO restored Bob (committed) and UNDO excluded Ghost (uncommitted)
    let recovered = TitanEngine::open(&dir).unwrap();
    let res = recovered
        .execute_sql(
            "SELECT id, owner, balance FROM accounts ORDER BY id ASC",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(res.rows.len(), 2, "rows: {:?}", res.rows);
    assert_eq!(res.rows[0][1], Value::Text("alice".into()));
    assert_eq!(res.rows[1][1], Value::Text("bob".into()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_mvcc_snapshot_isolation_and_deadlock_detection() {
    let dir = temp_dir("mvcc");
    let eng = TitanEngine::open(&dir).unwrap();
    eng.execute_sql(
        "CREATE TABLE items (id INT PRIMARY KEY, qty INT)",
        &[],
        None,
    )
    .unwrap();
    eng.execute_sql("INSERT INTO items VALUES (1, 10)", &[], None)
        .unwrap();

    // T1 starts snapshot before T2 inserts row 2
    let mut t1 = eng.begin_txn().unwrap();
    let r1 = eng
        .execute_sql("SELECT id FROM items", &[], Some(&mut t1))
        .unwrap();
    assert_eq!(r1.rows.len(), 1);

    let mut t2 = eng.begin_txn().unwrap();
    eng.execute_sql("INSERT INTO items VALUES (2, 20)", &[], Some(&mut t2))
        .unwrap();
    eng.commit_txn(&mut t2).unwrap();

    // T1 must NOT see row 2 (Snapshot Isolation)
    let r1_again = eng
        .execute_sql("SELECT id FROM items", &[], Some(&mut t1))
        .unwrap();
    assert_eq!(r1_again.rows.len(), 1);
    eng.commit_txn(&mut t1).unwrap();

    // New txn sees both
    let r3 = eng.execute_sql("SELECT id FROM items", &[], None).unwrap();
    assert_eq!(r3.rows.len(), 2);

    // Deadlock Wait-For cycle detection
    let lm = TxnManager::new();
    let a = lm.begin().txn_id;
    let b = lm.begin().txn_id;
    lm.acquire_lock(a, "row:1", LockMode::Exclusive).unwrap();
    lm.acquire_lock(b, "row:2", LockMode::Exclusive).unwrap();
    assert!(matches!(
        lm.acquire_lock(a, "row:2", LockMode::Exclusive),
        Err(TitanError::LockTimeout(_))
    ));
    assert!(matches!(
        lm.acquire_lock(b, "row:1", LockMode::Exclusive),
        Err(TitanError::Deadlock(_))
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_sql_joins_aggregates_explain_prepared_and_backup_restore() {
    let dir = temp_dir("sql_full");
    let bkp = temp_dir("sql_bkp");
    let restored = temp_dir("sql_restored");

    let eng = TitanEngine::open(&dir).unwrap();
    eng.execute_sql("CREATE DATABASE analytics", &[], None)
        .unwrap();
    eng.execute_sql("USE analytics", &[], None).unwrap();
    eng.execute_sql(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        &[],
        None,
    )
    .unwrap();
    eng.execute_sql(
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT)",
        &[],
        None,
    )
    .unwrap();
    eng.execute_sql("CREATE INDEX idx_orders_uid ON orders (user_id)", &[], None)
        .unwrap();

    eng.execute_sql(
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
        None,
    )
    .unwrap();
    eng.execute_sql(
        "INSERT INTO orders VALUES (10, 1, 50), (11, 1, 70), (12, 2, 30)",
        &[],
        None,
    )
    .unwrap();

    let agg = eng.execute_sql(
        "SELECT users.name, COUNT(*) AS cnt, SUM(orders.total) AS spend FROM users INNER JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY spend DESC LIMIT 2",
        &[],
        None,
    ).unwrap();
    assert_eq!(agg.rows.len(), 2);
    assert_eq!(agg.rows[0][0], Value::Text("Alice".into()));
    assert_eq!(agg.rows[0][2], Value::Int(120));

    let plan = eng
        .execute_sql("EXPLAIN SELECT * FROM orders WHERE user_id = 1", &[], None)
        .unwrap();
    assert!(plan.explain_plan.unwrap().contains("B+Tree IndexScan"));

    eng.execute_sql(
        "PREPARE find_user AS SELECT name FROM users WHERE id = $1",
        &[],
        None,
    )
    .unwrap();
    let prep = eng.execute_sql("EXECUTE find_user (2)", &[], None).unwrap();
    assert_eq!(prep.rows[0][0], Value::Text("Bob".into()));

    eng.backup_to(&bkp).unwrap();
    eng.execute_sql("DELETE FROM users WHERE id = 2", &[], None)
        .unwrap();

    let eng_restored = TitanEngine::restore_from(&bkp, &restored).unwrap();
    eng_restored
        .execute_sql("USE analytics", &[], None)
        .unwrap();
    let after = eng_restored
        .execute_sql("SELECT name FROM users ORDER BY id ASC", &[], None)
        .unwrap();
    assert_eq!(after.rows.len(), 2);

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&bkp);
    let _ = std::fs::remove_dir_all(&restored);
}

#[test]
fn test_distributed_raft_3node_election_replication_and_failover() {
    let d1 = temp_dir("raft_n1");
    let d2 = temp_dir("raft_n2");
    let d3 = temp_dir("raft_n3");
    let e1 = Arc::new(TitanEngine::open(&d1).unwrap());
    let e2 = Arc::new(TitanEngine::open(&d2).unwrap());
    let e3 = Arc::new(TitanEngine::open(&d3).unwrap());

    // Bind ephemeral listeners first to discover ports, or reserve consecutive ephemeral ports
    let l1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let l2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let l3 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a1 = l1.local_addr().unwrap().to_string();
    let a2 = l2.local_addr().unwrap().to_string();
    let a3 = l3.local_addr().unwrap().to_string();
    drop((l1, l2, l3));

    let n1 = RaftNode::start(
        1,
        a1.clone(),
        HashMap::from([(2, a2.clone()), (3, a3.clone())]),
        e1.clone(),
        Duration::from_millis(120),
    )
    .unwrap();
    let n2 = RaftNode::start(
        2,
        a2.clone(),
        HashMap::from([(1, a1.clone()), (3, a3.clone())]),
        e2.clone(),
        Duration::from_millis(240),
    )
    .unwrap();
    let n3 = RaftNode::start(
        3,
        a3.clone(),
        HashMap::from([(1, a1.clone()), (2, a2.clone())]),
        e3.clone(),
        Duration::from_millis(360),
    )
    .unwrap();

    // Wait for leader election
    let nodes = [n1.clone(), n2.clone(), n3.clone()];
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut leader_idx = None;
    while Instant::now() < deadline {
        for (i, n) in nodes.iter().enumerate() {
            if n.status().role == RaftRole::Leader {
                leader_idx = Some(i);
                break;
            }
        }
        if leader_idx.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    let li = leader_idx.expect("expected 3-node Raft leader election");

    nodes[li]
        .propose_sql("CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)")
        .unwrap();
    nodes[li]
        .propose_sql("INSERT INTO kv VALUES (1, 'replicated')")
        .unwrap();
    std::thread::sleep(Duration::from_millis(250));

    // Stop leader cleanly and verify remaining 2 nodes hold quorum + elect new leader
    nodes[li].stop();
    let remaining: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != li)
        .map(|(_, n)| n.clone())
        .collect();
    for r in &remaining {
        let res = r
            .engine
            .execute_sql("SELECT v FROM kv WHERE k = 1", &[], None)
            .unwrap();
        assert_eq!(res.rows[0][0], Value::Text("replicated".into()));
    }

    let deadline2 = Instant::now() + Duration::from_secs(4);
    let mut new_leader = None;
    while Instant::now() < deadline2 {
        for r in &remaining {
            if r.status().role == RaftRole::Leader {
                new_leader = Some(r.clone());
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    let nl = new_leader.expect("expected failover leader among remaining quorum");
    nl.propose_sql("INSERT INTO kv VALUES (2, 'post-failover')")
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let res2 = nl
        .engine
        .execute_sql("SELECT COUNT(*) FROM kv", &[], None)
        .unwrap();
    assert_eq!(res2.rows[0][0], Value::Int(2));

    for r in remaining {
        r.stop();
    }
    let router = ShardRouter::new(vec!["shard-a".into(), "shard-b".into(), "shard-c".into()]);
    assert!(router.shard_for_key("tenant:42").is_some());

    let _ = std::fs::remove_dir_all(&d1);
    let _ = std::fs::remove_dir_all(&d2);
    let _ = std::fs::remove_dir_all(&d3);
}

#[test]
fn test_wire_protocol_http_and_parser_fuzz_smoke() {
    let dir = temp_dir("wire");
    let eng = Arc::new(TitanEngine::open(&dir).unwrap());
    let srv = Arc::new(TitanServer::new(eng, None, Some("secret-tok".into())));
    let wire_addr = srv.clone().serve_wire_tcp("127.0.0.1:0").unwrap();
    let http_addr = srv.clone().serve_http("127.0.0.1:0").unwrap();

    // Wire protocol TCP roundtrip
    let mut sock = TcpStream::connect(&wire_addr).unwrap();
    let req = WireRequest {
        auth_token: Some("secret-tok".into()),
        op: "query".into(),
        sql: Some("CREATE TABLE t (id INT PRIMARY KEY)".into()),
        params: vec![],
    };
    let payload = serde_json::to_vec(&req).unwrap();
    sock.write_all(&(payload.len() as u32).to_be_bytes())
        .unwrap();
    sock.write_all(&payload).unwrap();
    let mut lbuf = [0u8; 4];
    sock.read_exact(&mut lbuf).unwrap();
    let rlen = u32::from_be_bytes(lbuf) as usize;
    let mut rbuf = vec![0u8; rlen];
    sock.read_exact(&mut rbuf).unwrap();
    let resp: WireResponse = serde_json::from_slice(&rbuf).unwrap();
    assert!(resp.ok);

    // Health HTTP check
    let mut hsock = TcpStream::connect(&http_addr).unwrap();
    hsock
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut hresp = String::new();
    hsock.read_to_string(&mut hresp).unwrap();
    assert!(hresp.contains("200 OK") && hresp.contains("titandb"));

    // Parser fuzz corpus smoke
    let malformed = [
        "",
        "SELECT",
        "SELECT ((((((((((",
        "INSERT INTO",
        "CREATE TABLE x (",
        "'unterminated",
        "SELECT * FROM a WHERE x = 'foo' OR (((a=1",
        "DELETE",
        "EXECUTE",
        "$0",
        ";;;;;;",
        "SELECT COUNT( FROM x",
        "UPDATE t SET",
        &"(".repeat(120),
    ];
    for s in malformed {
        let _ = parse_sql(s);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
