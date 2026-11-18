use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::engine::{QueryResult, TitanEngine};
use crate::error::{Result, TitanError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub index: u64,
    pub term: u64,
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReq {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResp {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReq {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub applied_sqls: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpc {
    RequestVote(RequestVoteReq),
    AppendEntries(AppendEntriesReq),
    InstallSnapshot(InstallSnapshotReq),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpcReply {
    RequestVote(RequestVoteResp),
    AppendEntries(AppendEntriesResp),
    SnapshotAck { term: u64, ok: bool },
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatus {
    pub node_id: u64,
    pub role: RaftRole,
    pub current_term: u64,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_len: usize,
}

/// Core Raft state machine guarding term, vote, log, commit_index, and election timer.
pub struct RaftState {
    pub node_id: u64,
    pub role: RaftRole,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub leader_id: Option<u64>,
    pub log: Vec<RaftLogEntry>, // 1-indexed logically; index 0 is dummy sentinel
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,
    pub last_heartbeat: Instant,
    pub election_timeout: Duration,
}

impl RaftState {
    pub fn new(node_id: u64, election_timeout: Duration) -> Self {
        Self {
            node_id,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: vec![RaftLogEntry {
                index: 0,
                term: 0,
                sql: String::new(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_heartbeat: Instant::now(),
            election_timeout,
        }
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn handle_request_vote(&mut self, req: &RequestVoteReq) -> RequestVoteResp {
        if req.term < self.current_term {
            return RequestVoteResp {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = RaftRole::Follower;
            self.voted_for = None;
            self.leader_id = None;
        }
        let log_ok = req.last_log_term > self.last_log_term()
            || (req.last_log_term == self.last_log_term()
                && req.last_log_index >= self.last_log_index());

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);
        if can_vote && log_ok {
            self.voted_for = Some(req.candidate_id);
            self.last_heartbeat = Instant::now();
            return RequestVoteResp {
                term: self.current_term,
                vote_granted: true,
            };
        }
        RequestVoteResp {
            term: self.current_term,
            vote_granted: false,
        }
    }

    pub fn handle_append_entries(&mut self, req: &AppendEntriesReq) -> AppendEntriesResp {
        if req.term < self.current_term {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }
        if req.term > self.current_term || self.role != RaftRole::Follower {
            self.current_term = req.term;
            self.role = RaftRole::Follower;
            self.voted_for = None;
        }
        self.leader_id = Some(req.leader_id);
        self.last_heartbeat = Instant::now();

        if req.prev_log_index as usize >= self.log.len() {
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        if self.log[req.prev_log_index as usize].term != req.prev_log_term {
            self.log.truncate(req.prev_log_index as usize);
            return AppendEntriesResp {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }

        for entry in &req.entries {
            let idx = entry.index as usize;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    self.log.truncate(idx);
                    self.log.push(entry.clone());
                }
            } else {
                self.log.push(entry.clone());
            }
        }

        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.last_log_index());
        }

        AppendEntriesResp {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
        }
    }
}

/// Real TCP-connected Raft cluster node replicating TitanEngine SQL state.
pub struct RaftNode {
    pub id: u64,
    pub bind_addr: String,
    pub peers: HashMap<u64, String>,
    pub state: Arc<Mutex<RaftState>>,
    pub engine: Arc<TitanEngine>,
    apply_lock: Mutex<()>,
    shutdown: Arc<Mutex<bool>>,
}

impl RaftNode {
    pub fn start(
        id: u64,
        bind_addr: String,
        peers: HashMap<u64, String>,
        engine: Arc<TitanEngine>,
        election_timeout: Duration,
    ) -> Result<Arc<Self>> {
        let listener = TcpListener::bind(&bind_addr)?;
        let actual_addr = listener.local_addr()?.to_string();
        listener.set_nonblocking(true)?;

        let node = Arc::new(Self {
            id,
            bind_addr: actual_addr,
            peers,
            state: Arc::new(Mutex::new(RaftState::new(id, election_timeout))),
            engine,
            apply_lock: Mutex::new(()),
            shutdown: Arc::new(Mutex::new(false)),
        });

        // RPC accept loop
        let n_rpc = node.clone();
        thread::spawn(move || {
            while !*n_rpc.shutdown.lock().unwrap() {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let handler = n_rpc.clone();
                        thread::spawn(move || {
                            let _ = handler.serve_rpc(&mut stream);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        // Election & heartbeat tick loop
        let n_tick = node.clone();
        thread::spawn(move || {
            while !*n_tick.shutdown.lock().unwrap() {
                thread::sleep(Duration::from_millis(25));
                n_tick.tick();
            }
        });

        Ok(node)
    }

    pub fn stop(&self) {
        *self.shutdown.lock().unwrap() = true;
    }

    pub fn status(&self) -> NodeStatus {
        let st = self.state.lock().unwrap();
        NodeStatus {
            node_id: st.node_id,
            role: st.role,
            current_term: st.current_term,
            leader_id: st.leader_id,
            commit_index: st.commit_index,
            last_applied: st.last_applied,
            log_len: st.log.len(),
        }
    }

    /// Triggers an immediate election or heartbeat cycle.
    pub fn tick(&self) {
        let (role, should_elect) = {
            let st = self.state.lock().unwrap();
            (
                st.role,
                st.role != RaftRole::Leader && st.last_heartbeat.elapsed() >= st.election_timeout,
            )
        };
        if should_elect {
            self.start_election();
        } else if role == RaftRole::Leader {
            self.broadcast_append_entries();
        }
        self.apply_committed();
    }

    pub fn start_election(&self) {
        let (term, last_idx, last_term, peer_list) = {
            let mut st = self.state.lock().unwrap();
            st.role = RaftRole::Candidate;
            st.current_term += 1;
            st.voted_for = Some(self.id);
            st.last_heartbeat = Instant::now();
            (
                st.current_term,
                st.last_log_index(),
                st.last_log_term(),
                self.peers.clone(),
            )
        };

        let total_nodes = peer_list.len() + 1;
        let quorum = (total_nodes / 2) + 1;
        let mut votes = 1usize;

        for (&pid, addr) in &peer_list {
            let _ = pid;
            let req = RaftRpc::RequestVote(RequestVoteReq {
                term,
                candidate_id: self.id,
                last_log_index: last_idx,
                last_log_term: last_term,
            });
            if let Ok(RaftRpcReply::RequestVote(resp)) = send_rpc(addr, &req) {
                let mut st = self.state.lock().unwrap();
                if resp.term > st.current_term {
                    st.current_term = resp.term;
                    st.role = RaftRole::Follower;
                    st.voted_for = None;
                    return;
                }
                if resp.vote_granted && st.current_term == term && st.role == RaftRole::Candidate {
                    votes += 1;
                }
            }
        }

        let became_leader = {
            let mut st = self.state.lock().unwrap();
            if st.role == RaftRole::Candidate && st.current_term == term && votes >= quorum {
                st.role = RaftRole::Leader;
                st.leader_id = Some(self.id);
                let next_idx = st.last_log_index() + 1;
                for &pid in self.peers.keys() {
                    st.next_index.insert(pid, next_idx);
                    st.match_index.insert(pid, 0);
                }
                true
            } else {
                false
            }
        };
        if became_leader {
            self.broadcast_append_entries();
        }
    }

    /// Replicates a write SQL command via Raft quorum and executes it locally once committed.
    pub fn propose_sql(&self, sql: &str) -> Result<QueryResult> {
        let entry = {
            let mut st = self.state.lock().unwrap();
            if st.role != RaftRole::Leader {
                return Err(TitanError::Raft(format!(
                    "node {} is not leader (known leader: {:?})",
                    self.id, st.leader_id
                )));
            }
            let idx = st.last_log_index() + 1;
            let ent = RaftLogEntry {
                index: idx,
                term: st.current_term,
                sql: sql.to_string(),
            };
            st.log.push(ent.clone());
            ent
        };

        self.broadcast_append_entries();
        self.apply_committed();

        let committed = {
            let st = self.state.lock().unwrap();
            st.commit_index >= entry.index
        };
        if !committed {
            return Err(TitanError::Raft("failed to reach quorum commit".into()));
        }
        // Return executed result summary (execution happens inside apply_committed)
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            message: format!("COMMITTED AT RAFT INDEX {}", entry.index),
            explain_plan: None,
        })
    }

    fn broadcast_append_entries(&self) {
        let (term, commit_idx, leader_log, next_map) = {
            let st = self.state.lock().unwrap();
            if st.role != RaftRole::Leader {
                return;
            }
            (
                st.current_term,
                st.commit_index,
                st.log.clone(),
                st.next_index.clone(),
            )
        };

        let mut newly_matched = HashMap::new();
        for (&pid, addr) in &self.peers {
            let next_i = *next_map.get(&pid).unwrap_or(&1);
            let prev_idx = next_i.saturating_sub(1);
            let prev_term = leader_log
                .get(prev_idx as usize)
                .map(|e| e.term)
                .unwrap_or(0);
            let entries = if (next_i as usize) < leader_log.len() {
                leader_log[next_i as usize..].to_vec()
            } else {
                Vec::new()
            };
            let rpc = RaftRpc::AppendEntries(AppendEntriesReq {
                term,
                leader_id: self.id,
                prev_log_index: prev_idx,
                prev_log_term: prev_term,
                entries,
                leader_commit: commit_idx,
            });
            if let Ok(RaftRpcReply::AppendEntries(resp)) = send_rpc(addr, &rpc) {
                let mut st = self.state.lock().unwrap();
                if resp.term > st.current_term {
                    st.current_term = resp.term;
                    st.role = RaftRole::Follower;
                    st.voted_for = None;
                    return;
                }
                if resp.success {
                    st.match_index.insert(pid, resp.match_index);
                    st.next_index.insert(pid, resp.match_index + 1);
                    newly_matched.insert(pid, resp.match_index);
                } else {
                    let cur = *st.next_index.get(&pid).unwrap_or(&2);
                    st.next_index.insert(pid, cur.saturating_sub(1).max(1));
                }
            }
        }

        let mut st = self.state.lock().unwrap();
        if st.role != RaftRole::Leader {
            return;
        }
        let quorum = self.peers.len().div_ceil(2) + 1;
        let last = st.last_log_index();
        for n in (st.commit_index + 1)..=last {
            if st.log[n as usize].term != st.current_term {
                continue;
            }
            let mut count = 1usize; // self
            for &m in st.match_index.values() {
                if m >= n {
                    count += 1;
                }
            }
            if count >= quorum {
                st.commit_index = n;
            }
        }
    }

    fn apply_committed(&self) {
        let _guard = self.apply_lock.lock().unwrap();
        loop {
            let next_cmd = {
                let st = self.state.lock().unwrap();
                if st.last_applied < st.commit_index {
                    let next_idx = (st.last_applied + 1) as usize;
                    Some((next_idx as u64, st.log[next_idx].sql.clone()))
                } else {
                    None
                }
            };
            let Some((idx, sql)) = next_cmd else {
                break;
            };
            if !sql.is_empty() {
                let _ = self.engine.execute_sql(&sql, &[], None);
            }
            let mut st = self.state.lock().unwrap();
            st.last_applied = idx;
        }
    }

    fn serve_rpc(&self, stream: &mut TcpStream) -> Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 8 * 1024 * 1024 {
            return Err(TitanError::Protocol("raft rpc frame too large".into()));
        }
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body)?;
        let rpc: RaftRpc = serde_json::from_slice(&body)?;
        let reply = match rpc {
            RaftRpc::RequestVote(req) => {
                RaftRpcReply::RequestVote(self.state.lock().unwrap().handle_request_vote(&req))
            }
            RaftRpc::AppendEntries(req) => {
                let r = self.state.lock().unwrap().handle_append_entries(&req);
                self.apply_committed();
                RaftRpcReply::AppendEntries(r)
            }
            RaftRpc::InstallSnapshot(snap) => {
                let mut st = self.state.lock().unwrap();
                if snap.term >= st.current_term {
                    st.current_term = snap.term;
                    st.role = RaftRole::Follower;
                    st.leader_id = Some(snap.leader_id);
                    for s in &snap.applied_sqls {
                        let _ = self.engine.execute_sql(s, &[], None);
                    }
                    st.commit_index = snap.last_included_index;
                    st.last_applied = snap.last_included_index;
                    RaftRpcReply::SnapshotAck {
                        term: st.current_term,
                        ok: true,
                    }
                } else {
                    RaftRpcReply::SnapshotAck {
                        term: st.current_term,
                        ok: false,
                    }
                }
            }
        };
        let out = serde_json::to_vec(&reply)?;
        stream.write_all(&(out.len() as u32).to_be_bytes())?;
        stream.write_all(&out)?;
        Ok(())
    }
}

fn send_rpc(addr: &str, rpc: &RaftRpc) -> Result<RaftRpcReply> {
    let mut stream = TcpStream::connect_timeout(
        &addr
            .parse()
            .map_err(|e| TitanError::Raft(format!("bad peer addr {addr}: {e}")))?,
        Duration::from_millis(250),
    )?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    let body = serde_json::to_vec(rpc)?;
    stream.write_all(&(body.len() as u32).to_be_bytes())?;
    stream.write_all(&body)?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let rlen = u32::from_be_bytes(len_buf) as usize;
    let mut rbuf = vec![0u8; rlen];
    stream.read_exact(&mut rbuf)?;
    Ok(serde_json::from_slice(&rbuf)?)
}

/// Consistent FNV-1a partition placement router across named cluster shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRouter {
    pub shards: Vec<String>,
}

impl ShardRouter {
    pub fn new(shards: Vec<String>) -> Self {
        Self { shards }
    }

    pub fn shard_for_key(&self, key: &str) -> Option<&str> {
        if self.shards.is_empty() {
            return None;
        }
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in key.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        Some(&self.shards[(h as usize) % self.shards.len()])
    }
}
