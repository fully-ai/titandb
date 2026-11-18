use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::error::{Result, TitanError};
use crate::sql::ast::*;
use crate::sql::parser::parse_sql;
use crate::storage::btree::{BPlusTree, IndexKey};
use crate::storage::buffer::{BufferPool, BufferStats};
use crate::storage::disk::DiskManager;
use crate::storage::page::SlottedPage;
use crate::storage::wal::{WalManager, WalOp};
use crate::txn::{LockMode, Snapshot, TupleVersion, TxnId, TxnManager, Value, VersionedRow};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub name: String,
    pub table: String,
    pub column: String,
    pub distinct_keys: usize,
    pub lookups: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryEntry {
    pub sql: String,
    pub duration_us: u64,
    pub timestamp_epoch_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: usize,
    pub message: String,
    pub explain_plan: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableData {
    db: String,
    name: String,
    columns: Vec<ColumnDef>,
    next_row_id: u64,
    rows: BTreeMap<u64, VersionedRow>,
    indexes: HashMap<String, TableIndex>,
    page_ids: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableIndex {
    name: String,
    column: String,
    col_idx: usize,
    tree: BPlusTree,
    lookups: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CatalogSnapshot {
    databases: HashSet<String>,
    tables: HashMap<String, TableData>, // key: "db.table"
    #[serde(default)]
    checkpointed_max_txn: u64,
}

#[derive(Debug, Clone)]
pub struct TxnContext {
    pub snap: Snapshot,
    pub active: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EngineMetrics {
    pub queries_total: u64,
    pub txns_committed: u64,
    pub txns_aborted: u64,
    pub buffer: BufferStats,
    pub indexes: Vec<IndexStats>,
    pub slow_queries: Vec<SlowQueryEntry>,
}

/// Autonomous SQL + Storage + MVCC + B+Tree + WAL engine instance.
pub struct TitanEngine {
    dir: PathBuf,
    _disk: Arc<DiskManager>,
    pool: Arc<BufferPool>,
    wal: Arc<WalManager>,
    txn_mgr: Arc<TxnManager>,
    catalog: Mutex<CatalogSnapshot>,
    current_db: Mutex<String>,
    prepared: Mutex<HashMap<String, Statement>>,
    slow_queries: Mutex<Vec<SlowQueryEntry>>,
    queries_total: Mutex<u64>,
    txns_committed: Mutex<u64>,
    txns_aborted: Mutex<u64>,
    slow_threshold_us: u64,
}

impl TitanEngine {
    /// Opens or recovers a TitanDB storage directory, replaying committed WAL records and discarding uncommitted ones.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let disk = Arc::new(DiskManager::open(dir.join("pages.tdb"))?);
        let pool = Arc::new(BufferPool::new(disk.clone(), 256));
        let wal = Arc::new(WalManager::open(dir.join("wal.log"), true)?);
        let txn_mgr = Arc::new(TxnManager::new());

        let cat_path = dir.join("catalog.json");
        let mut catalog: CatalogSnapshot = if cat_path.exists() {
            let bytes = std::fs::read(&cat_path)?;
            serde_json::from_slice(&bytes)?
        } else {
            let mut set = HashSet::new();
            set.insert("default".to_string());
            CatalogSnapshot {
                databases: set,
                tables: HashMap::new(),
                checkpointed_max_txn: 0,
            }
        };
        if !catalog.databases.contains("default") {
            catalog.databases.insert("default".into());
        }

        // Verify all allocated disk pages against CRC32 corruption on open
        for pid in 0..disk.page_count() {
            let _ = pool.fetch_page(pid)?;
            pool.unpin_page(pid, false, None)?;
        }

        let engine = Self {
            dir,
            _disk: disk,
            pool,
            wal,
            txn_mgr,
            catalog: Mutex::new(catalog),
            current_db: Mutex::new("default".into()),
            prepared: Mutex::new(HashMap::new()),
            slow_queries: Mutex::new(Vec::new()),
            queries_total: Mutex::new(0),
            txns_committed: Mutex::new(0),
            txns_aborted: Mutex::new(0),
            slow_threshold_us: 25_000,
        };

        engine.recover_from_wal()?;
        Ok(engine)
    }

    fn recover_from_wal(&self) -> Result<()> {
        let records = self.wal.read_all()?;
        if records.is_empty() {
            return Ok(());
        }

        // Identify committed txns (REDO set) vs uncommitted txns (UNDO/skip)
        let mut committed: HashSet<TxnId> = HashSet::new();
        for rec in &records {
            if matches!(rec.op, WalOp::Commit) {
                committed.insert(rec.txn_id);
            }
        }

        let mut cat = self.catalog.lock().unwrap();
        let mut max_txn: TxnId = 1;
        for rec in &records {
            max_txn = max_txn.max(rec.txn_id + 1);
            if !committed.contains(&rec.txn_id) {
                continue;
            }
            match &rec.op {
                WalOp::InsertTuple {
                    table,
                    row_id,
                    row_json,
                } => {
                    if let Some(td) = cat.tables.get_mut(table) {
                        let vals: Vec<Value> = serde_json::from_slice(row_json)?;
                        td.next_row_id = td.next_row_id.max(*row_id + 1);
                        let mut found = false;
                        if let Some(vr) = td.rows.get_mut(row_id)
                            && let Some(last) = vr.versions.last_mut()
                        {
                            last.xmin = rec.txn_id;
                            last.xmax = None;
                            last.data = vals.clone();
                            found = true;
                        }
                        if !found {
                            td.rows.insert(
                                *row_id,
                                VersionedRow {
                                    row_id: *row_id,
                                    versions: vec![TupleVersion {
                                        xmin: rec.txn_id,
                                        xmax: None,
                                        data: vals.clone(),
                                    }],
                                },
                            );
                        }
                        reindex_row(td, *row_id, &vals);
                    }
                }
                WalOp::UpdateTuple {
                    table,
                    row_id,
                    new_json,
                    ..
                } => {
                    if let Some(td) = cat.tables.get_mut(table) {
                        let vals: Vec<Value> = serde_json::from_slice(new_json)?;
                        if let Some(vr) = td.rows.get_mut(row_id) {
                            if let Some(last) = vr.versions.last_mut()
                                && last.xmax.is_none()
                            {
                                last.xmax = Some(rec.txn_id);
                            }
                            vr.versions.push(TupleVersion {
                                xmin: rec.txn_id,
                                xmax: None,
                                data: vals.clone(),
                            });
                            reindex_row(td, *row_id, &vals);
                        }
                    }
                }
                WalOp::DeleteTuple { table, row_id, .. } => {
                    if let Some(td) = cat.tables.get_mut(table)
                        && let Some(vr) = td.rows.get_mut(row_id)
                        && let Some(last) = vr.versions.last_mut()
                    {
                        last.xmax = Some(rec.txn_id);
                    }
                }
                _ => {}
            }
        }
        drop(cat);
        // Ensure uncommitted versions left from a dirty catalog save prior to crash are rolled back
        let mut cat = self.catalog.lock().unwrap();
        let ck_max = cat.checkpointed_max_txn;
        for td in cat.tables.values_mut() {
            for vr in td.rows.values_mut() {
                vr.versions
                    .retain(|v| committed.contains(&v.xmin) || v.xmin <= ck_max);
                for v in &mut vr.versions {
                    if let Some(x) = v.xmax
                        && !committed.contains(&x)
                        && x > ck_max
                    {
                        v.xmax = None;
                    }
                }
            }
            td.rows.retain(|_, vr| !vr.versions.is_empty());
        }
        self.txn_mgr.advance_next_txn(max_txn.max(ck_max + 1));
        Ok(())
    }

    /// Flushes dirty buffer pages to DiskManager, saves catalog snapshot, and truncates WAL safely.
    pub fn checkpoint(&self) -> Result<()> {
        self.wal.append(0, WalOp::Checkpoint)?;
        self.pool.flush_all()?;
        let mut cat = self.catalog.lock().unwrap();
        cat.checkpointed_max_txn = self.txn_mgr.peek_next_txn().saturating_sub(1);
        let raw = serde_json::to_vec_pretty(&*cat)?;
        let tmp = self.dir.join("catalog.json.tmp");
        std::fs::write(&tmp, raw)?;
        std::fs::rename(tmp, self.dir.join("catalog.json"))?;
        self.wal.truncate_after_checkpoint()?;
        Ok(())
    }

    /// Saves a consistent checkpoint backup into `dest_dir`.
    pub fn backup_to<P: AsRef<Path>>(&self, dest_dir: P) -> Result<()> {
        self.checkpoint()?;
        let dest = dest_dir.as_ref();
        std::fs::create_dir_all(dest)?;
        for name in ["pages.tdb", "catalog.json", "wal.log"] {
            let src = self.dir.join(name);
            if src.exists() {
                std::fs::copy(src, dest.join(name))?;
            }
        }
        Ok(())
    }

    /// Restores database snapshot files from `src_dir` into `dest_dir` and opens the restored engine.
    pub fn restore_from<P: AsRef<Path>, Q: AsRef<Path>>(src_dir: P, dest_dir: Q) -> Result<Self> {
        let src = src_dir.as_ref();
        let dst = dest_dir.as_ref();
        std::fs::create_dir_all(dst)?;
        for name in ["pages.tdb", "catalog.json", "wal.log"] {
            let p = src.join(name);
            if p.exists() {
                std::fs::copy(p, dst.join(name))?;
            }
        }
        Self::open(dst)
    }

    pub fn begin_txn(&self) -> Result<TxnContext> {
        let snap = self.txn_mgr.begin();
        self.wal.append(snap.txn_id, WalOp::Begin)?;
        Ok(TxnContext { snap, active: true })
    }

    pub fn commit_txn(&self, ctx: &mut TxnContext) -> Result<()> {
        if !ctx.active {
            return Err(TitanError::Transaction("no active transaction".into()));
        }
        self.wal.append(ctx.snap.txn_id, WalOp::Commit)?;
        self.txn_mgr.commit(ctx.snap.txn_id);
        ctx.active = false;
        *self.txns_committed.lock().unwrap() += 1;
        Ok(())
    }

    pub fn rollback_txn(&self, ctx: &mut TxnContext) -> Result<()> {
        if !ctx.active {
            return Err(TitanError::Transaction("no active transaction".into()));
        }
        self.wal.append(ctx.snap.txn_id, WalOp::Abort)?;
        self.txn_mgr.abort(ctx.snap.txn_id);
        ctx.active = false;
        *self.txns_aborted.lock().unwrap() += 1;
        Ok(())
    }

    /// Executes a SQL statement in autocommit mode or inside an explicit `TxnContext`.
    pub fn execute_sql(
        &self,
        sql: &str,
        params: &[Value],
        txn: Option<&mut TxnContext>,
    ) -> Result<QueryResult> {
        let start = Instant::now();
        *self.queries_total.lock().unwrap() += 1;

        let stmt = parse_sql(sql)?;
        let res = match txn {
            Some(ctx) => self.exec_stmt(stmt, params, Some(ctx)),
            None => self.exec_stmt(stmt, params, None),
        };

        let elapsed_us = start.elapsed().as_micros() as u64;
        if elapsed_us >= self.slow_threshold_us {
            let mut sq = self.slow_queries.lock().unwrap();
            if sq.len() >= 100 {
                sq.remove(0);
            }
            sq.push(SlowQueryEntry {
                sql: sql.to_string(),
                duration_us: elapsed_us,
                timestamp_epoch_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            });
        }
        res
    }

    fn exec_stmt(
        &self,
        stmt: Statement,
        params: &[Value],
        txn: Option<&mut TxnContext>,
    ) -> Result<QueryResult> {
        match stmt {
            Statement::Begin => {
                if let Some(ctx) = txn {
                    if ctx.active {
                        return Err(TitanError::Transaction("transaction already active".into()));
                    }
                    *ctx = self.begin_txn()?;
                }
                Ok(msg_result("BEGIN"))
            }
            Statement::Commit => {
                if let Some(ctx) = txn {
                    self.commit_txn(ctx)?;
                }
                Ok(msg_result("COMMIT"))
            }
            Statement::Rollback => {
                if let Some(ctx) = txn {
                    self.rollback_txn(ctx)?;
                }
                Ok(msg_result("ROLLBACK"))
            }
            Statement::CreateDatabase { name } => {
                validate_ident(&name)?;
                let mut cat = self.catalog.lock().unwrap();
                cat.databases.insert(name.clone());
                drop(cat);
                self.checkpoint()?;
                Ok(msg_result(&format!("CREATED DATABASE {name}")))
            }
            Statement::UseDatabase { name } => {
                validate_ident(&name)?;
                let cat = self.catalog.lock().unwrap();
                if !cat.databases.contains(&name) {
                    return Err(TitanError::NotFound(format!("database {name}")));
                }
                *self.current_db.lock().unwrap() = name.clone();
                Ok(msg_result(&format!("USE {name}")))
            }
            Statement::CreateTable { name, columns } => {
                validate_ident(&name)?;
                let db = self.current_db.lock().unwrap().clone();
                let key = format!("{db}.{name}");
                let mut cat = self.catalog.lock().unwrap();
                if cat.tables.contains_key(&key) {
                    return Err(TitanError::Execution(format!(
                        "table {name} already exists"
                    )));
                }
                let page = self.pool.new_page()?;
                let pid = page.page_id();
                self.pool.unpin_page(pid, true, Some(page))?;

                let mut indexes = HashMap::new();
                for (i, col) in columns.iter().enumerate() {
                    if col.primary_key {
                        indexes.insert(
                            format!("pk_{}", col.name),
                            TableIndex {
                                name: format!("pk_{}", col.name),
                                column: col.name.clone(),
                                col_idx: i,
                                tree: BPlusTree::new(16),
                                lookups: 0,
                            },
                        );
                    }
                }
                cat.tables.insert(
                    key,
                    TableData {
                        db,
                        name: name.clone(),
                        columns,
                        next_row_id: 1,
                        rows: BTreeMap::new(),
                        indexes,
                        page_ids: vec![pid],
                    },
                );
                drop(cat);
                self.checkpoint()?;
                Ok(msg_result(&format!("CREATED TABLE {name}")))
            }
            Statement::DropTable { name } => {
                let db = self.current_db.lock().unwrap().clone();
                let key = format!("{db}.{name}");
                let mut cat = self.catalog.lock().unwrap();
                cat.tables
                    .remove(&key)
                    .ok_or_else(|| TitanError::NotFound(format!("table {name}")))?;
                drop(cat);
                self.checkpoint()?;
                Ok(msg_result(&format!("DROPPED TABLE {name}")))
            }
            Statement::CreateIndex {
                name,
                table,
                column,
            } => {
                let db = self.current_db.lock().unwrap().clone();
                let key = format!("{db}.{table}");
                let mut cat = self.catalog.lock().unwrap();
                let td = cat
                    .tables
                    .get_mut(&key)
                    .ok_or_else(|| TitanError::NotFound(format!("table {table}")))?;
                let col_idx = td
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&column))
                    .ok_or_else(|| TitanError::NotFound(format!("column {column}")))?;
                let mut tree = BPlusTree::new(16);
                for (&rid, vr) in &td.rows {
                    if let Some(last) = vr.versions.last()
                        && last.xmax.is_none()
                        && let Some(k) = value_to_index_key(&last.data[col_idx])
                    {
                        tree.insert(k, rid);
                    }
                }
                td.indexes.insert(
                    name.clone(),
                    TableIndex {
                        name: name.clone(),
                        column,
                        col_idx,
                        tree,
                        lookups: 0,
                    },
                );
                drop(cat);
                self.checkpoint()?;
                Ok(msg_result(&format!("CREATED INDEX {name}")))
            }
            Statement::Explain(inner) => {
                let plan = self.explain_stmt(&inner)?;
                Ok(QueryResult {
                    columns: vec!["QUERY PLAN".into()],
                    rows: plan
                        .lines()
                        .map(|l| vec![Value::Text(l.to_string())])
                        .collect(),
                    rows_affected: 0,
                    message: "EXPLAIN".into(),
                    explain_plan: Some(plan),
                })
            }
            Statement::Prepare { name, stmt } => {
                self.prepared.lock().unwrap().insert(name.clone(), *stmt);
                Ok(msg_result(&format!("PREPARED {name}")))
            }
            Statement::Execute {
                name,
                params: arg_exprs,
            } => {
                let inner = self
                    .prepared
                    .lock()
                    .unwrap()
                    .get(&name)
                    .cloned()
                    .ok_or_else(|| TitanError::NotFound(format!("prepared statement {name}")))?;
                let mut bound = Vec::with_capacity(arg_exprs.len());
                for e in &arg_exprs {
                    bound.push(eval_expr_standalone(e, params)?);
                }
                self.exec_stmt(inner, &bound, txn)
            }
            Statement::Insert {
                table,
                columns,
                rows,
            } => {
                let (snap, auto_commit) = self.ensure_txn(txn.as_deref())?;
                let res = self.exec_insert(&table, columns.as_deref(), &rows, params, &snap);
                if auto_commit {
                    if res.is_ok() {
                        self.wal.append(snap.txn_id, WalOp::Commit)?;
                        self.txn_mgr.commit(snap.txn_id);
                    } else {
                        self.wal.append(snap.txn_id, WalOp::Abort)?;
                        self.txn_mgr.abort(snap.txn_id);
                    }
                }
                res
            }
            Statement::Update {
                table,
                assignments,
                filter,
            } => {
                let (snap, auto_commit) = self.ensure_txn(txn.as_deref())?;
                let res = self.exec_update(&table, &assignments, filter.as_ref(), params, &snap);
                if auto_commit {
                    if res.is_ok() {
                        self.wal.append(snap.txn_id, WalOp::Commit)?;
                        self.txn_mgr.commit(snap.txn_id);
                    } else {
                        self.wal.append(snap.txn_id, WalOp::Abort)?;
                        self.txn_mgr.abort(snap.txn_id);
                    }
                }
                res
            }
            Statement::Delete { table, filter } => {
                let (snap, auto_commit) = self.ensure_txn(txn.as_deref())?;
                let res = self.exec_delete(&table, filter.as_ref(), params, &snap);
                if auto_commit {
                    if res.is_ok() {
                        self.wal.append(snap.txn_id, WalOp::Commit)?;
                        self.txn_mgr.commit(snap.txn_id);
                    } else {
                        self.wal.append(snap.txn_id, WalOp::Abort)?;
                        self.txn_mgr.abort(snap.txn_id);
                    }
                }
                res
            }
            Statement::Select {
                items,
                from,
                joins,
                filter,
                group_by,
                order_by,
                limit,
            } => {
                let (snap, auto_commit) = self.ensure_txn(txn.as_deref())?;
                let res = self.exec_select(
                    &items,
                    &from,
                    &joins,
                    filter.as_ref(),
                    &group_by,
                    &order_by,
                    limit,
                    params,
                    &snap,
                );
                if auto_commit {
                    self.txn_mgr.commit(snap.txn_id);
                }
                res
            }
        }
    }

    fn ensure_txn(&self, txn: Option<&TxnContext>) -> Result<(Snapshot, bool)> {
        if let Some(ctx) = txn
            && ctx.active
        {
            return Ok((ctx.snap.clone(), false));
        }
        let snap = self.txn_mgr.begin();
        self.wal.append(snap.txn_id, WalOp::Begin)?;
        Ok((snap, true))
    }

    pub fn explain_stmt(&self, stmt: &Statement) -> Result<String> {
        match stmt {
            Statement::Select {
                from,
                joins,
                filter,
                group_by,
                order_by,
                limit,
                ..
            } => {
                let db = self.current_db.lock().unwrap().clone();
                let key = format!("{db}.{from}");
                let cat = self.catalog.lock().unwrap();
                let td = cat
                    .tables
                    .get(&key)
                    .ok_or_else(|| TitanError::NotFound(from.clone()))?;
                let mut lines = Vec::new();
                if let Some(lim) = limit {
                    lines.push(format!("Limit (n={lim})"));
                }
                if !order_by.is_empty() {
                    let cols: Vec<String> = order_by
                        .iter()
                        .map(|o| format!("{} {}", o.col, if o.desc { "DESC" } else { "ASC" }))
                        .collect();
                    lines.push(format!("  Sort ({})", cols.join(", ")));
                }
                if !group_by.is_empty() {
                    lines.push(format!(
                        "    HashAggregate (group_by={})",
                        group_by.join(", ")
                    ));
                }
                if let Some((idx_name, col_name)) = find_applicable_index(td, filter.as_ref()) {
                    lines.push(format!(
                        "      -> B+Tree IndexScan on {from} using {idx_name}({col_name})"
                    ));
                } else {
                    lines.push(format!(
                        "      -> SeqScan on {from} (pages={}, est_rows={})",
                        td.page_ids.len(),
                        td.rows.len()
                    ));
                }
                for j in joins {
                    lines.push(format!("      -> InnerJoin ({})", j.table));
                }
                Ok(lines.join("\n"))
            }
            other => Ok(format!("DirectExecution ({other:?})")),
        }
    }

    fn write_tuple_to_page(&self, td: &mut TableData, payload: &[u8]) -> Result<()> {
        if let Some(&last_pid) = td.page_ids.last() {
            let mut page = self.pool.fetch_page(last_pid)?;
            if page.insert_tuple(payload).is_some() {
                self.pool.unpin_page(last_pid, true, Some(page))?;
                return Ok(());
            }
            self.pool.unpin_page(last_pid, false, None)?;
        }
        let mut new_page: SlottedPage = self.pool.new_page()?;
        let new_pid = new_page.page_id();
        let _ = new_page.insert_tuple(payload);
        self.pool.unpin_page(new_pid, true, Some(new_page))?;
        td.page_ids.push(new_pid);
        Ok(())
    }

    fn exec_insert(
        &self,
        table: &str,
        columns: Option<&[String]>,
        rows: &[Vec<Expr>],
        params: &[Value],
        snap: &Snapshot,
    ) -> Result<QueryResult> {
        let db = self.current_db.lock().unwrap().clone();
        let key = format!("{db}.{table}");

        let mut cat = self.catalog.lock().unwrap();
        let td = cat
            .tables
            .get_mut(&key)
            .ok_or_else(|| TitanError::NotFound(format!("table {table}")))?;

        let mut inserted = 0usize;
        for expr_row in rows {
            let mut vals = vec![Value::Null; td.columns.len()];
            if let Some(cols) = columns {
                if cols.len() != expr_row.len() {
                    return Err(TitanError::Execution("column count mismatch".into()));
                }
                for (cname, expr) in cols.iter().zip(expr_row.iter()) {
                    let idx = td
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(cname))
                        .ok_or_else(|| TitanError::NotFound(format!("column {cname}")))?;
                    vals[idx] = eval_expr_standalone(expr, params)?;
                }
            } else {
                if expr_row.len() != td.columns.len() {
                    return Err(TitanError::Execution("INSERT values count mismatch".into()));
                }
                for (i, expr) in expr_row.iter().enumerate() {
                    vals[i] = eval_expr_standalone(expr, params)?;
                }
            }
            for (cdef, v) in td.columns.iter().zip(vals.iter()) {
                if cdef.not_null && matches!(v, Value::Null) {
                    return Err(TitanError::Execution(format!(
                        "column {} is NOT NULL",
                        cdef.name
                    )));
                }
            }
            let rid = td.next_row_id;
            td.next_row_id += 1;
            let serialized = serde_json::to_vec(&vals)?;
            self.wal.append(
                snap.txn_id,
                WalOp::InsertTuple {
                    table: key.clone(),
                    row_id: rid,
                    row_json: serialized.clone(),
                },
            )?;
            self.write_tuple_to_page(td, &serialized)?;
            td.rows.insert(
                rid,
                VersionedRow {
                    row_id: rid,
                    versions: vec![TupleVersion {
                        xmin: snap.txn_id,
                        xmax: None,
                        data: vals.clone(),
                    }],
                },
            );
            reindex_row(td, rid, &vals);
            inserted += 1;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: inserted,
            message: format!("INSERT {inserted}"),
            explain_plan: None,
        })
    }

    fn exec_update(
        &self,
        table: &str,
        assignments: &[(String, Expr)],
        filter: Option<&Expr>,
        params: &[Value],
        snap: &Snapshot,
    ) -> Result<QueryResult> {
        let db = self.current_db.lock().unwrap().clone();
        let key = format!("{db}.{table}");
        self.txn_mgr
            .acquire_lock(snap.txn_id, &key, LockMode::Exclusive)?;

        let mut cat = self.catalog.lock().unwrap();
        let td = cat
            .tables
            .get_mut(&key)
            .ok_or_else(|| TitanError::NotFound(format!("table {table}")))?;
        let col_names: Vec<String> = td.columns.iter().map(|c| c.name.clone()).collect();
        let mut updates = Vec::new();
        for (&rid, vr) in &td.rows {
            if let Some(vis) = visible_version(&self.txn_mgr, snap, vr) {
                if let Some(f) = filter
                    && !eval_predicate(f, &col_names, &vis.data, table, params)?
                {
                    continue;
                }
                let mut next_vals = vis.data.clone();
                for (cname, expr) in assignments {
                    let idx = col_names
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(cname))
                        .ok_or_else(|| TitanError::NotFound(cname.clone()))?;
                    next_vals[idx] = eval_expr_standalone(expr, params)?;
                }
                updates.push((rid, vis.data.clone(), next_vals));
            }
        }

        let count = updates.len();
        for (rid, old_vals, new_vals) in updates {
            self.txn_mgr
                .acquire_lock(snap.txn_id, &format!("{key}#{rid}"), LockMode::Exclusive)?;
            let old_json = serde_json::to_vec(&old_vals)?;
            let new_json = serde_json::to_vec(&new_vals)?;
            self.wal.append(
                snap.txn_id,
                WalOp::UpdateTuple {
                    table: key.clone(),
                    row_id: rid,
                    old_json,
                    new_json: new_json.clone(),
                },
            )?;
            self.write_tuple_to_page(td, &new_json)?;
            if let Some(vr) = td.rows.get_mut(&rid) {
                for v in vr.versions.iter_mut().rev() {
                    if v.xmax.is_none() {
                        v.xmax = Some(snap.txn_id);
                        break;
                    }
                }
                vr.versions.push(TupleVersion {
                    xmin: snap.txn_id,
                    xmax: None,
                    data: new_vals.clone(),
                });
            }
            reindex_row(td, rid, &new_vals);
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
            message: format!("UPDATE {count}"),
            explain_plan: None,
        })
    }

    fn exec_delete(
        &self,
        table: &str,
        filter: Option<&Expr>,
        params: &[Value],
        snap: &Snapshot,
    ) -> Result<QueryResult> {
        let db = self.current_db.lock().unwrap().clone();
        let key = format!("{db}.{table}");
        self.txn_mgr
            .acquire_lock(snap.txn_id, &key, LockMode::Exclusive)?;

        let mut cat = self.catalog.lock().unwrap();
        let td = cat
            .tables
            .get_mut(&key)
            .ok_or_else(|| TitanError::NotFound(format!("table {table}")))?;
        let col_names: Vec<String> = td.columns.iter().map(|c| c.name.clone()).collect();
        let mut targets = Vec::new();
        for (&rid, vr) in &td.rows {
            if let Some(vis) = visible_version(&self.txn_mgr, snap, vr) {
                if let Some(f) = filter
                    && !eval_predicate(f, &col_names, &vis.data, table, params)?
                {
                    continue;
                }
                targets.push((rid, vis.data.clone()));
            }
        }
        let count = targets.len();
        for (rid, old_vals) in targets {
            let old_json = serde_json::to_vec(&old_vals)?;
            self.wal.append(
                snap.txn_id,
                WalOp::DeleteTuple {
                    table: key.clone(),
                    row_id: rid,
                    old_json,
                },
            )?;
            if let Some(vr) = td.rows.get_mut(&rid) {
                for v in vr.versions.iter_mut().rev() {
                    if v.xmax.is_none() {
                        v.xmax = Some(snap.txn_id);
                        break;
                    }
                }
            }
            for idx in td.indexes.values_mut() {
                if let Some(k) = value_to_index_key(&old_vals[idx.col_idx]) {
                    idx.tree.remove(&k, rid);
                }
            }
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
            message: format!("DELETE {count}"),
            explain_plan: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_select(
        &self,
        items: &[SelectItem],
        from: &str,
        joins: &[JoinClause],
        filter: Option<&Expr>,
        group_by: &[String],
        order_by: &[OrderByItem],
        limit: Option<usize>,
        params: &[Value],
        snap: &Snapshot,
    ) -> Result<QueryResult> {
        let db = self.current_db.lock().unwrap().clone();
        let base_key = format!("{db}.{from}");

        let mut cat = self.catalog.lock().unwrap();
        let td = cat
            .tables
            .get_mut(&base_key)
            .ok_or_else(|| TitanError::NotFound(format!("table {from}")))?;

        let mut schema_cols: Vec<(String, String)> = td
            .columns
            .iter()
            .map(|c| (from.to_string(), c.name.clone()))
            .collect();

        // Use B+Tree index lookup when predicate matches indexed column equality!
        let candidate_ids: Option<Vec<u64>> = try_index_lookup(td, filter, params);
        let mut combined_rows: Vec<Vec<Value>> = Vec::new();
        if let Some(ids) = candidate_ids {
            for rid in ids {
                if let Some(vr) = td.rows.get(&rid)
                    && let Some(vis) = visible_version(&self.txn_mgr, snap, vr)
                {
                    combined_rows.push(vis.data.clone());
                }
            }
        } else {
            for vr in td.rows.values() {
                if let Some(vis) = visible_version(&self.txn_mgr, snap, vr) {
                    combined_rows.push(vis.data.clone());
                }
            }
        }

        // Apply joins (Nested Loop / equi-join) cleanly preserving qualified names
        for j in joins {
            let jkey = format!("{db}.{}", j.table);
            let jtd = cat
                .tables
                .get(&jkey)
                .ok_or_else(|| TitanError::NotFound(format!("join table {}", j.table)))?;
            let jcols: Vec<(String, String)> = jtd
                .columns
                .iter()
                .map(|c| (j.table.clone(), c.name.clone()))
                .collect();
            let mut next_schema = schema_cols.clone();
            next_schema.extend(jcols);

            let mut jrows = Vec::new();
            for vr in jtd.rows.values() {
                if let Some(vis) = visible_version(&self.txn_mgr, snap, vr) {
                    jrows.push(vis.data.clone());
                }
            }
            let mut joined = Vec::new();
            for left in &combined_rows {
                for right in &jrows {
                    let mut merged = left.clone();
                    merged.extend(right.iter().cloned());
                    if eval_qualified_predicate(&j.on, &next_schema, &merged, params)? {
                        joined.push(merged);
                    }
                }
            }
            schema_cols = next_schema;
            combined_rows = joined;
        }
        drop(cat);

        // Apply WHERE filter
        if let Some(f) = filter {
            let mut filtered = Vec::new();
            for row in combined_rows {
                if eval_qualified_predicate(f, &schema_cols, &row, params)? {
                    filtered.push(row);
                }
            }
            combined_rows = filtered;
        }

        // Handle aggregates / GROUP BY
        let has_agg = items
            .iter()
            .any(|it| matches!(&it.expr, Some(Expr::Aggregate { .. })));

        let (out_cols, mut out_rows) = if has_agg || !group_by.is_empty() {
            execute_group_agg(items, group_by, &schema_cols, &combined_rows, params)?
        } else {
            project_rows(items, &schema_cols, &combined_rows, params)?
        };

        // ORDER BY
        if !order_by.is_empty() {
            out_rows.sort_by(|a, b| {
                for ob in order_by {
                    if let Some(idx) = find_col_idx(&out_cols, &ob.col) {
                        let c = a[idx].cmp_val(&b[idx]);
                        if c != std::cmp::Ordering::Equal {
                            return if ob.desc { c.reverse() } else { c };
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        if let Some(lim) = limit {
            out_rows.truncate(lim);
        }

        let cnt = out_rows.len();
        Ok(QueryResult {
            columns: out_cols,
            rows: out_rows,
            rows_affected: cnt,
            message: format!("SELECT {cnt}"),
            explain_plan: None,
        })
    }

    pub fn metrics(&self) -> EngineMetrics {
        let cat = self.catalog.lock().unwrap();
        let mut indexes = Vec::new();
        for td in cat.tables.values() {
            for ix in td.indexes.values() {
                indexes.push(IndexStats {
                    name: ix.name.clone(),
                    table: td.name.clone(),
                    column: ix.column.clone(),
                    distinct_keys: ix.tree.len_keys(),
                    lookups: ix.lookups,
                });
            }
        }
        EngineMetrics {
            queries_total: *self.queries_total.lock().unwrap(),
            txns_committed: *self.txns_committed.lock().unwrap(),
            txns_aborted: *self.txns_aborted.lock().unwrap(),
            buffer: self.pool.stats(),
            indexes,
            slow_queries: self.slow_queries.lock().unwrap().clone(),
        }
    }

    pub fn txn_manager(&self) -> &Arc<TxnManager> {
        &self.txn_mgr
    }
}

fn validate_ident(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 64 || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(TitanError::SqlSyntax(format!("invalid identifier {s:?}")));
    }
    Ok(())
}

fn msg_result(msg: &str) -> QueryResult {
    QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected: 0,
        message: msg.to_string(),
        explain_plan: None,
    }
}

fn value_to_index_key(v: &Value) -> Option<IndexKey> {
    match v {
        Value::Int(i) => Some(IndexKey::Int(*i)),
        Value::Text(s) => Some(IndexKey::Text(s.clone())),
        _ => None,
    }
}

fn reindex_row(td: &mut TableData, rid: u64, vals: &[Value]) {
    for idx in td.indexes.values_mut() {
        if let Some(val) = vals.get(idx.col_idx)
            && let Some(k) = value_to_index_key(val)
        {
            idx.tree.insert(k, rid);
        }
    }
}

fn visible_version<'a>(
    mgr: &TxnManager,
    snap: &Snapshot,
    vr: &'a VersionedRow,
) -> Option<&'a TupleVersion> {
    vr.versions.iter().rev().find(|v| mgr.is_visible(snap, v))
}

fn find_applicable_index(td: &TableData, filter: Option<&Expr>) -> Option<(String, String)> {
    let f = filter?;
    if let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        ..
    } = f
        && let Expr::Column { name, .. } = &**left
    {
        for ix in td.indexes.values() {
            if ix.column.eq_ignore_ascii_case(name) {
                return Some((ix.name.clone(), ix.column.clone()));
            }
        }
    }
    None
}

fn try_index_lookup(
    td: &mut TableData,
    filter: Option<&Expr>,
    params: &[Value],
) -> Option<Vec<u64>> {
    let f = filter?;
    if let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = f
        && let Expr::Column { name, .. } = &**left
        && let Ok(v) = eval_expr_standalone(right, params)
        && let Some(k) = value_to_index_key(&v)
    {
        for ix in td.indexes.values_mut() {
            if ix.column.eq_ignore_ascii_case(name) {
                ix.lookups += 1;
                return Some(ix.tree.get(&k));
            }
        }
    }
    None
}

fn eval_expr_standalone(expr: &Expr, params: &[Value]) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(idx) => params
            .get(idx.saturating_sub(1))
            .cloned()
            .ok_or_else(|| TitanError::Execution(format!("missing bound parameter ${idx}"))),
        other => Err(TitanError::Execution(format!(
            "cannot evaluate {other:?} standalone"
        ))),
    }
}

fn eval_predicate(
    expr: &Expr,
    cols: &[String],
    row: &[Value],
    table: &str,
    params: &[Value],
) -> Result<bool> {
    let qcols: Vec<(String, String)> = cols
        .iter()
        .map(|c| (table.to_string(), c.clone()))
        .collect();
    eval_qualified_predicate(expr, &qcols, row, params)
}

fn eval_qualified_predicate(
    expr: &Expr,
    cols: &[(String, String)],
    row: &[Value],
    params: &[Value],
) -> Result<bool> {
    let v = eval_qualified_expr(expr, cols, row, params)?;
    Ok(matches!(v, Value::Bool(true)))
}

fn eval_qualified_expr(
    expr: &Expr,
    cols: &[(String, String)],
    row: &[Value],
    params: &[Value],
) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Param(idx) => params
            .get(idx.saturating_sub(1))
            .cloned()
            .ok_or_else(|| TitanError::Execution(format!("missing parameter ${idx}"))),
        Expr::Column { table, name } => {
            for (i, (t, c)) in cols.iter().enumerate() {
                if c.eq_ignore_ascii_case(name)
                    && (table.is_none() || table.as_deref().unwrap().eq_ignore_ascii_case(t))
                {
                    return Ok(row[i].clone());
                }
            }
            Err(TitanError::NotFound(format!("column {name}")))
        }
        Expr::Not(inner) => {
            let b = matches!(
                eval_qualified_expr(inner, cols, row, params)?,
                Value::Bool(true)
            );
            Ok(Value::Bool(!b))
        }
        Expr::Binary { left, op, right } => match op {
            BinaryOp::And => {
                let l = matches!(
                    eval_qualified_expr(left, cols, row, params)?,
                    Value::Bool(true)
                );
                if !l {
                    return Ok(Value::Bool(false));
                }
                let r = matches!(
                    eval_qualified_expr(right, cols, row, params)?,
                    Value::Bool(true)
                );
                Ok(Value::Bool(r))
            }
            BinaryOp::Or => {
                let l = matches!(
                    eval_qualified_expr(left, cols, row, params)?,
                    Value::Bool(true)
                );
                if l {
                    return Ok(Value::Bool(true));
                }
                let r = matches!(
                    eval_qualified_expr(right, cols, row, params)?,
                    Value::Bool(true)
                );
                Ok(Value::Bool(r))
            }
            BinaryOp::Like => {
                let l = eval_qualified_expr(left, cols, row, params)?;
                let r = eval_qualified_expr(right, cols, row, params)?;
                match (l, r) {
                    (Value::Text(s), Value::Text(pat)) => Ok(Value::Bool(sql_like(&s, &pat))),
                    _ => Ok(Value::Bool(false)),
                }
            }
            _ => {
                let l = eval_qualified_expr(left, cols, row, params)?;
                let r = eval_qualified_expr(right, cols, row, params)?;
                let ord = l.cmp_val(&r);
                let res = match op {
                    BinaryOp::Eq => ord == std::cmp::Ordering::Equal,
                    BinaryOp::Neq => ord != std::cmp::Ordering::Equal,
                    BinaryOp::Lt => ord == std::cmp::Ordering::Less,
                    BinaryOp::Lte => ord != std::cmp::Ordering::Greater,
                    BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
                    BinaryOp::Gte => ord != std::cmp::Ordering::Less,
                    _ => false,
                };
                Ok(Value::Bool(res))
            }
        },
        Expr::Aggregate { .. } => Err(TitanError::Execution(
            "unexpected aggregate in scalar expr".into(),
        )),
    }
}

fn sql_like(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; p.len() + 1]; t.len() + 1];
    dp[0][0] = true;
    for j in 1..=p.len() {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=t.len() {
        for j in 1..=p.len() {
            if p[j - 1] == '%' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if p[j - 1] == '_' || p[j - 1] == t[i - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    dp[t.len()][p.len()]
}

fn project_rows(
    items: &[SelectItem],
    schema: &[(String, String)],
    rows: &[Vec<Value>],
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut out_cols = Vec::new();
    let mut col_indices: Vec<Option<(usize, &SelectItem)>> = Vec::new();

    for it in items {
        match &it.expr {
            None => {
                for (i, (_, cname)) in schema.iter().enumerate() {
                    out_cols.push(cname.clone());
                    col_indices.push(Some((i, it)));
                }
            }
            Some(Expr::Column { name, .. }) => {
                out_cols.push(it.alias.clone().unwrap_or_else(|| name.clone()));
                col_indices.push(None);
            }
            Some(e) => {
                out_cols.push(it.alias.clone().unwrap_or_else(|| format!("{e:?}")));
                col_indices.push(None);
            }
        }
    }

    let mut out_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut r = Vec::new();
        let mut star_cursor = 0;
        for it in items {
            match &it.expr {
                None => {
                    for v in row {
                        r.push(v.clone());
                        star_cursor += 1;
                    }
                }
                Some(e) => {
                    r.push(eval_qualified_expr(e, schema, row, params)?);
                    star_cursor += 1;
                }
            }
        }
        let _ = star_cursor;
        out_rows.push(r);
    }
    Ok((out_cols, out_rows))
}

fn execute_group_agg(
    items: &[SelectItem],
    group_by: &[String],
    schema: &[(String, String)],
    rows: &[Vec<Value>],
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut groups: BTreeMap<String, Vec<Vec<Value>>> = BTreeMap::new();
    if group_by.is_empty() {
        groups.insert("__all__".into(), rows.to_vec());
    } else {
        for r in rows {
            let mut key_parts = Vec::new();
            for g in group_by {
                let (t, c) = if let Some((a, b)) = g.split_once('.') {
                    (Some(a.to_string()), b.to_string())
                } else {
                    (None, g.clone())
                };
                let val =
                    eval_qualified_expr(&Expr::Column { table: t, name: c }, schema, r, params)?;
                key_parts.push(format!("{val:?}"));
            }
            groups
                .entry(key_parts.join("|"))
                .or_default()
                .push(r.clone());
        }
    }

    let mut out_cols = Vec::new();
    for it in items {
        let name = it.alias.clone().unwrap_or_else(|| match &it.expr {
            Some(Expr::Column { name, .. }) => name.clone(),
            Some(Expr::Aggregate { func, .. }) => func.clone(),
            _ => "col".into(),
        });
        out_cols.push(name);
    }

    let mut out_rows = Vec::new();
    for (_gk, grp_rows) in groups {
        let mut vals = Vec::new();
        for it in items {
            match &it.expr {
                Some(Expr::Column { table, name }) => {
                    if let Some(first) = grp_rows.first() {
                        vals.push(eval_qualified_expr(
                            &Expr::Column {
                                table: table.clone(),
                                name: name.clone(),
                            },
                            schema,
                            first,
                            params,
                        )?);
                    } else {
                        vals.push(Value::Null);
                    }
                }
                Some(Expr::Aggregate { func, arg }) => {
                    vals.push(compute_agg(
                        func,
                        arg.as_deref(),
                        schema,
                        &grp_rows,
                        params,
                    )?);
                }
                Some(e) => {
                    if let Some(first) = grp_rows.first() {
                        vals.push(eval_qualified_expr(e, schema, first, params)?);
                    }
                }
                None => vals.push(Value::Int(grp_rows.len() as i64)),
            }
        }
        out_rows.push(vals);
    }
    Ok((out_cols, out_rows))
}

fn compute_agg(
    func: &str,
    arg: Option<&Expr>,
    schema: &[(String, String)],
    rows: &[Vec<Value>],
    params: &[Value],
) -> Result<Value> {
    if func == "COUNT" && arg.is_none() {
        return Ok(Value::Int(rows.len() as i64));
    }
    let Some(expr) = arg else {
        return Ok(Value::Null);
    };
    let mut vals = Vec::new();
    for r in rows {
        let v = eval_qualified_expr(expr, schema, r, params)?;
        if !matches!(v, Value::Null) {
            vals.push(v);
        }
    }
    match func {
        "COUNT" => Ok(Value::Int(vals.len() as i64)),
        "MIN" => Ok(vals
            .into_iter()
            .min_by(|a, b| a.cmp_val(b))
            .unwrap_or(Value::Null)),
        "MAX" => Ok(vals
            .into_iter()
            .max_by(|a, b| a.cmp_val(b))
            .unwrap_or(Value::Null)),
        "SUM" | "AVG" => {
            if vals.is_empty() {
                return Ok(Value::Null);
            }
            let mut sum = 0.0f64;
            let mut all_int = true;
            let mut isum = 0i64;
            for v in &vals {
                match v {
                    Value::Int(i) => {
                        sum += *i as f64;
                        isum += *i;
                    }
                    Value::Float(f) => {
                        all_int = false;
                        sum += *f;
                    }
                    _ => {}
                }
            }
            if func == "AVG" {
                Ok(Value::Float(sum / vals.len() as f64))
            } else if all_int {
                Ok(Value::Int(isum))
            } else {
                Ok(Value::Float(sum))
            }
        }
        _ => Err(TitanError::Execution(format!("unknown aggregate {func}"))),
    }
}

fn find_col_idx(cols: &[String], target: &str) -> Option<usize> {
    let bare = target.split('.').next_back().unwrap_or(target);
    cols.iter()
        .position(|c| c.eq_ignore_ascii_case(bare) || c.eq_ignore_ascii_case(target))
}
