use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

use titandb::protocol::{WireRequest, WireResponse};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:7440");
    let token = std::env::var("TITANDB_AUTH_TOKEN").ok();

    let mut stream = TcpStream::connect(addr).unwrap_or_else(|e| {
        eprintln!("Failed to connect to TitanDB wire socket {addr}: {e}");
        std::process::exit(1);
    });
    println!("Connected to TitanDB at {addr}. Enter SQL statements terminated by ';', or 'exit'.");

    let stdin = io::stdin();
    for line in stdin.lock().lines().map_while(Result::ok) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("\\q") {
            break;
        }
        let req = WireRequest {
            auth_token: token.clone(),
            op: "query".into(),
            sql: Some(trimmed.to_string()),
            params: vec![],
        };
        let bytes = serde_json::to_vec(&req).unwrap();
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .unwrap();
        stream.write_all(&bytes).unwrap();

        let mut lbuf = [0u8; 4];
        stream.read_exact(&mut lbuf).unwrap();
        let len = u32::from_be_bytes(lbuf) as usize;
        let mut rbuf = vec![0u8; len];
        stream.read_exact(&mut rbuf).unwrap();
        let resp: WireResponse = serde_json::from_slice(&rbuf).unwrap();
        if !resp.ok {
            eprintln!("ERROR: {}", resp.error.unwrap_or_default());
        } else if let Some(res) = resp.result {
            if !res.columns.is_empty() {
                println!("Columns: {:?}", res.columns);
                for row in res.rows {
                    println!("  {:?}", row);
                }
            } else {
                println!("{}", res.message);
            }
        }
    }
}
