use serde::{Deserialize, Serialize};

/// Canonical comparable index key values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IndexKey {
    Int(i64),
    Text(String),
}

/// Persistent-ready B+Tree index mapping `IndexKey` -> `Vec<u64>` (matching row_ids).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BPlusTree {
    order: usize, // max keys per node before split
    nodes: Vec<Node>,
    root: usize,
    first_leaf: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Node {
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<usize>,
    },
    Leaf {
        keys: Vec<IndexKey>,
        values: Vec<Vec<u64>>,
        next: Option<usize>,
    },
}

impl BPlusTree {
    /// Creates an empty B+Tree with node order >= 4.
    pub fn new(order: usize) -> Self {
        let ord = order.max(4);
        let root_leaf = Node::Leaf {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        };
        Self {
            order: ord,
            nodes: vec![root_leaf],
            root: 0,
            first_leaf: 0,
        }
    }

    pub fn insert(&mut self, key: IndexKey, row_id: u64) {
        if let Some((sep_key, right_id)) = self.insert_recursive(self.root, key, row_id) {
            let new_root = Node::Internal {
                keys: vec![sep_key],
                children: vec![self.root, right_id],
            };
            self.nodes.push(new_root);
            self.root = self.nodes.len() - 1;
        }
    }

    fn insert_recursive(
        &mut self,
        node_idx: usize,
        key: IndexKey,
        row_id: u64,
    ) -> Option<(IndexKey, usize)> {
        let is_leaf = matches!(self.nodes[node_idx], Node::Leaf { .. });
        if is_leaf {
            let need_split = if let Node::Leaf { keys, values, .. } = &mut self.nodes[node_idx] {
                match keys.binary_search(&key) {
                    Ok(pos) => {
                        if !values[pos].contains(&row_id) {
                            values[pos].push(row_id);
                        }
                    }
                    Err(pos) => {
                        keys.insert(pos, key);
                        values.insert(pos, vec![row_id]);
                    }
                }
                keys.len() > self.order
            } else {
                false
            };

            if !need_split {
                return None;
            }
            return Some(self.split_leaf(node_idx));
        }

        let child_idx = if let Node::Internal { keys, children } = &self.nodes[node_idx] {
            let pos = match keys.binary_search(&key) {
                Ok(p) => p + 1,
                Err(p) => p,
            };
            children[pos]
        } else {
            unreachable!()
        };

        let split = self.insert_recursive(child_idx, key, row_id)?;
        let need_split = if let Node::Internal { keys, children } = &mut self.nodes[node_idx] {
            let pos = match keys.binary_search(&split.0) {
                Ok(p) => p + 1,
                Err(p) => p,
            };
            keys.insert(pos, split.0);
            children.insert(pos + 1, split.1);
            keys.len() > self.order
        } else {
            false
        };

        if !need_split {
            return None;
        }
        Some(self.split_internal(node_idx))
    }

    fn split_leaf(&mut self, left_idx: usize) -> (IndexKey, usize) {
        let new_idx = self.nodes.len();
        let (right_keys, right_vals, old_next, sep_key) =
            if let Node::Leaf { keys, values, next } = &mut self.nodes[left_idx] {
                let mid = keys.len() / 2;
                let rk = keys.split_off(mid);
                let rv = values.split_off(mid);
                let on = *next;
                *next = Some(new_idx);
                let sep = rk[0].clone();
                (rk, rv, on, sep)
            } else {
                unreachable!()
            };

        self.nodes.push(Node::Leaf {
            keys: right_keys,
            values: right_vals,
            next: old_next,
        });
        (sep_key, new_idx)
    }

    fn split_internal(&mut self, left_idx: usize) -> (IndexKey, usize) {
        let new_idx = self.nodes.len();
        let (promote, rk, rc) = if let Node::Internal { keys, children } = &mut self.nodes[left_idx]
        {
            let mid = keys.len() / 2;
            let promote = keys[mid].clone();
            let mut rk = keys.split_off(mid);
            // remove promoted median key from right sibling
            if !rk.is_empty() {
                rk.remove(0);
            }
            let rc = children.split_off(mid + 1);
            (promote, rk, rc)
        } else {
            unreachable!()
        };

        self.nodes.push(Node::Internal {
            keys: rk,
            children: rc,
        });
        (promote, new_idx)
    }

    /// Returns row IDs matching an exact key in O(log N).
    pub fn get(&self, key: &IndexKey) -> Vec<u64> {
        let leaf_idx = self.find_leaf(key);
        if let Node::Leaf { keys, values, .. } = &self.nodes[leaf_idx]
            && let Ok(pos) = keys.binary_search(key)
        {
            return values[pos].clone();
        }
        Vec::new()
    }

    /// Returns all row IDs whose keys fall inside inclusive bounds `[min_key, max_key]`.
    pub fn range(&self, min_key: Option<&IndexKey>, max_key: Option<&IndexKey>) -> Vec<u64> {
        let mut leaf_idx = match min_key {
            Some(k) => self.find_leaf(k),
            None => self.first_leaf,
        };
        let mut out = Vec::new();
        while let Node::Leaf { keys, values, next } = &self.nodes[leaf_idx] {
            for (i, k) in keys.iter().enumerate() {
                if let Some(lo) = min_key
                    && k < lo
                {
                    continue;
                }
                if let Some(hi) = max_key
                    && k > hi
                {
                    return out;
                }
                out.extend_from_slice(&values[i]);
            }
            match *next {
                Some(n) => leaf_idx = n,
                None => break,
            }
        }
        out
    }

    /// Removes a specific `row_id` under `key` cleanly.
    pub fn remove(&mut self, key: &IndexKey, row_id: u64) -> bool {
        let leaf_idx = self.find_leaf(key);
        if let Node::Leaf { keys, values, .. } = &mut self.nodes[leaf_idx]
            && let Ok(pos) = keys.binary_search(key)
        {
            let before = values[pos].len();
            values[pos].retain(|&id| id != row_id);
            let removed = values[pos].len() < before;
            if values[pos].is_empty() {
                keys.remove(pos);
                values.remove(pos);
            }
            return removed;
        }
        false
    }

    fn find_leaf(&self, key: &IndexKey) -> usize {
        let mut cur = self.root;
        loop {
            match &self.nodes[cur] {
                Node::Leaf { .. } => return cur,
                Node::Internal { keys, children } => {
                    let pos = match keys.binary_search(key) {
                        Ok(p) => p + 1,
                        Err(p) => p,
                    };
                    cur = children[pos];
                }
            }
        }
    }

    pub fn len_keys(&self) -> usize {
        let mut cur = Some(self.first_leaf);
        let mut total = 0;
        while let Some(idx) = cur {
            if let Node::Leaf { keys, next, .. } = &self.nodes[idx] {
                total += keys.len();
                cur = *next;
            } else {
                break;
            }
        }
        total
    }
}
