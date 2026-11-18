use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{Result, TitanError};

pub type TxnId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl Value {
    pub fn cmp_val(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Int(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Int(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (a, b) => format!("{a:?}").cmp(&format!("{b:?}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TupleVersion {
    pub xmin: TxnId,
    pub xmax: Option<TxnId>,
    pub data: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedRow {
    pub row_id: u64,
    pub versions: Vec<TupleVersion>,
}

/// Snapshot captured at transaction start for MVCC read consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub txn_id: TxnId,
    pub xmax: TxnId,
    pub active_txns: HashSet<TxnId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Default)]
struct LockEntry {
    holders: HashMap<TxnId, LockMode>,
}

/// Transaction, MVCC snapshot, and deadlock-aware 2PL lock manager.
pub struct TxnManager {
    next_txn: Mutex<TxnId>,
    states: Mutex<HashMap<TxnId, TxnState>>,
    locks: Mutex<HashMap<String, LockEntry>>,
    wait_for: Mutex<HashMap<TxnId, HashSet<TxnId>>>,
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnManager {
    pub fn new() -> Self {
        Self {
            next_txn: Mutex::new(1),
            states: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            wait_for: Mutex::new(HashMap::new()),
        }
    }

    pub fn begin(&self) -> Snapshot {
        let mut next = self.next_txn.lock().unwrap();
        let tid = *next;
        *next += 1;
        drop(next);

        let mut states = self.states.lock().unwrap();
        let active: HashSet<TxnId> = states
            .iter()
            .filter_map(|(&id, &st)| {
                if st == TxnState::Active {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        states.insert(tid, TxnState::Active);

        Snapshot {
            txn_id: tid,
            xmax: tid,
            active_txns: active,
        }
    }

    /// Advances transaction ID watermark (used during WAL recovery on restart).
    pub fn advance_next_txn(&self, min_next: TxnId) {
        let mut next = self.next_txn.lock().unwrap();
        if min_next > *next {
            *next = min_next;
        }
    }

    pub fn peek_next_txn(&self) -> TxnId {
        *self.next_txn.lock().unwrap()
    }

    pub fn commit(&self, txn_id: TxnId) {
        self.states
            .lock()
            .unwrap()
            .insert(txn_id, TxnState::Committed);
        self.release_all_locks(txn_id);
    }

    pub fn abort(&self, txn_id: TxnId) {
        self.states
            .lock()
            .unwrap()
            .insert(txn_id, TxnState::Aborted);
        self.release_all_locks(txn_id);
    }

    pub fn state(&self, txn_id: TxnId) -> TxnState {
        self.states
            .lock()
            .unwrap()
            .get(&txn_id)
            .copied()
            .unwrap_or(TxnState::Committed)
    }

    /// Evaluates MVCC tuple visibility against `snap`.
    pub fn is_visible(&self, snap: &Snapshot, v: &TupleVersion) -> bool {
        let states = self.states.lock().unwrap();
        let state_of = |tid: TxnId| states.get(&tid).copied().unwrap_or(TxnState::Committed);

        // Rule 1: Must be created either by this txn or a txn committed prior to snapshot.
        let created_visible = if v.xmin == snap.txn_id {
            state_of(v.xmin) != TxnState::Aborted
        } else {
            state_of(v.xmin) == TxnState::Committed
                && v.xmin < snap.xmax
                && !snap.active_txns.contains(&v.xmin)
        };
        if !created_visible {
            return false;
        }

        // Rule 2: Not deleted by a visible committed txn (or this txn itself).
        match v.xmax {
            None => true,
            Some(xid) if xid == snap.txn_id => false,
            Some(xid) => {
                if state_of(xid) != TxnState::Committed {
                    return true;
                }
                xid >= snap.xmax || snap.active_txns.contains(&xid)
            }
        }
    }

    /// Acquires a Shared or Exclusive lock on `resource`, aborting immediately on Wait-For cycle detection.
    pub fn acquire_lock(&self, txn_id: TxnId, resource: &str, mode: LockMode) -> Result<()> {
        let mut locks = self.locks.lock().unwrap();
        let entry = locks.entry(resource.to_string()).or_default();

        // Check existing lock held by txn_id
        if let Some(&existing) = entry.holders.get(&txn_id)
            && (existing == LockMode::Exclusive || mode == LockMode::Shared)
        {
            return Ok(());
        }

        // Determine conflicting holders
        let conflicts: Vec<TxnId> = entry
            .holders
            .iter()
            .filter_map(|(&other, &other_mode)| {
                if other == txn_id {
                    return None;
                }
                if mode == LockMode::Exclusive || other_mode == LockMode::Exclusive {
                    Some(other)
                } else {
                    None
                }
            })
            .collect();

        if conflicts.is_empty() {
            entry.holders.insert(txn_id, mode);
            self.wait_for.lock().unwrap().remove(&txn_id);
            return Ok(());
        }

        // Record Wait-For edge(s) and check for cycle via DFS
        let mut wf = self.wait_for.lock().unwrap();
        let deps = wf.entry(txn_id).or_default();
        for c in &conflicts {
            deps.insert(*c);
        }
        if has_cycle(&wf, txn_id) {
            wf.remove(&txn_id);
            return Err(TitanError::Deadlock(format!(
                "txn {txn_id} aborted due to wait-for cycle on {resource}"
            )));
        }
        Err(TitanError::LockTimeout(format!(
            "resource {resource} locked by {:?}",
            conflicts
        )))
    }

    pub fn release_all_locks(&self, txn_id: TxnId) {
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, entry| {
            entry.holders.remove(&txn_id);
            !entry.holders.is_empty()
        });
        let mut wf = self.wait_for.lock().unwrap();
        wf.remove(&txn_id);
        for set in wf.values_mut() {
            set.remove(&txn_id);
        }
    }
}

fn has_cycle(graph: &HashMap<TxnId, HashSet<TxnId>>, start: TxnId) -> bool {
    let mut visited = HashSet::new();
    let mut stack = vec![start];
    while let Some(node) = stack.pop() {
        if let Some(neighbors) = graph.get(&node) {
            for &next in neighbors {
                if next == start {
                    return true;
                }
                if visited.insert(next) {
                    stack.push(next);
                }
            }
        }
    }
    false
}
