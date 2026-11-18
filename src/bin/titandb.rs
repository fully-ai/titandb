use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use titandb::engine::TitanEngine;
use titandb::raft::RaftNode;
use titandb::server::TitanServer;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir = "data/titandb".to_string();
    let mut wire_addr = "127.0.0.1:7440".to_string();
    let mut http_addr = "127.0.0.1:7441".to_string();
    let mut raft_addr: Option<String> = None;
    let mut node_id: u64 = 1;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" if i + 1 < args.len() => {
                data_dir = args[i + 1].clone();
                i += 2;
            }
            "--wire-addr" if i + 1 < args.len() => {
                wire_addr = args[i + 1].clone();
                i += 2;
            }
            "--http-addr" if i + 1 < args.len() => {
                http_addr = args[i + 1].clone();
                i += 2;
            }
            "--raft-addr" if i + 1 < args.len() => {
                raft_addr = Some(args[i + 1].clone());
                i += 2;
            }
            "--node-id" if i + 1 < args.len() => {
                node_id = args[i + 1].parse().unwrap_or(1);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let engine = Arc::new(TitanEngine::open(&data_dir).expect("failed to open titandb engine"));
    let raft_node = raft_addr.map(|ra| {
        RaftNode::start(
            node_id,
            ra,
            HashMap::new(),
            engine.clone(),
            Duration::from_millis(200),
        )
        .expect("failed to start raft node")
    });
    let auth_token = std::env::var("TITANDB_AUTH_TOKEN").ok();
    let srv = Arc::new(TitanServer::new(engine.clone(), raft_node, auth_token));
    let actual_wire = srv.clone().serve_wire_tcp(&wire_addr).expect("bind wire");
    let actual_http = srv.clone().serve_http(&http_addr).expect("bind http");

    println!("[TITANDB] Wire TCP listening on {actual_wire}");
    println!("[TITANDB] HTTP Admin/Query listening on http://{actual_http}");
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}
