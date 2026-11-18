use std::time::Instant;
use titandb::engine::TitanEngine;

fn main() {
    let dir = std::env::temp_dir().join("titandb_bench_run");
    let _ = std::fs::remove_dir_all(&dir);
    let eng = TitanEngine::open(&dir).unwrap();
    eng.execute_sql(
        "CREATE TABLE bench (id INT PRIMARY KEY, grp INT, val TEXT)",
        &[],
        None,
    )
    .unwrap();
    eng.execute_sql("CREATE INDEX idx_grp ON bench (grp)", &[], None)
        .unwrap();

    // 1. Single explicit transaction batching 1,000 inserts (realistic OLTP txn batch)
    let t0 = Instant::now();
    let mut txn = eng.begin_txn().unwrap();
    for i in 1..=1000i64 {
        let sql = format!("INSERT INTO bench VALUES ({i}, {}, 'row-{i}')", i % 50);
        eng.execute_sql(&sql, &[], Some(&mut txn)).unwrap();
    }
    eng.commit_txn(&mut txn).unwrap();
    let ins_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // 2. 1,000 indexed point lookups via B+Tree IndexScan
    let t1 = Instant::now();
    for i in 1..=1000i64 {
        let sql = format!("SELECT val FROM bench WHERE id = {i}");
        let res = eng.execute_sql(&sql, &[], None).unwrap();
        assert_eq!(res.rows.len(), 1);
    }
    let read_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let metrics = eng.metrics();
    println!("TitanDB Release Benchmark (N=1000):");
    println!(
        "  1000 Inserts (1 WAL-synced Txn): {ins_ms:.2} ms ({:.0} rows/sec)",
        1000.0 / (ins_ms / 1000.0)
    );
    println!(
        "  1000 Point Lookups (B+Tree):     {read_ms:.2} ms ({:.0} qps)",
        1000.0 / (read_ms / 1000.0)
    );
    println!(
        "  Buffer Pool Stats: hits={} misses={} flushes={}",
        metrics.buffer.hits, metrics.buffer.misses, metrics.buffer.flushes
    );
    let _ = std::fs::remove_dir_all(&dir);
}
