use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::engine::TitanEngine;
use crate::error::Result;
use crate::protocol::{WireRequest, WireResponse};
use crate::raft::RaftNode;
use crate::txn::Value;

/// Hosts both TitanDB's documented length-prefixed TCP wire protocol and HTTP JSON Admin/Query endpoints.
pub struct TitanServer {
    pub engine: Arc<TitanEngine>,
    pub raft: Option<Arc<RaftNode>>,
    pub auth_token: Option<String>,
}

impl TitanServer {
    pub fn new(
        engine: Arc<TitanEngine>,
        raft: Option<Arc<RaftNode>>,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            engine,
            raft,
            auth_token,
        }
    }

    fn authorized(&self, token: Option<&str>) -> bool {
        match &self.auth_token {
            None => true,
            Some(expected) => token
                .map(|t| constant_time_eq(t.as_bytes(), expected.as_bytes()))
                .unwrap_or(false),
        }
    }

    pub fn serve_wire_tcp(self: Arc<Self>, bind_addr: &str) -> Result<String> {
        let listener = TcpListener::bind(bind_addr)?;
        let addr = listener.local_addr()?.to_string();
        let srv = self.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let s = srv.clone();
                thread::spawn(move || {
                    let _ = s.handle_wire_conn(stream);
                });
            }
        });
        Ok(addr)
    }

    fn handle_wire_conn(&self, mut stream: TcpStream) -> Result<()> {
        loop {
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > 16 * 1024 * 1024 {
                break;
            }
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf)?;
            let req: WireRequest = serde_json::from_slice(&buf)?;
            let resp = if !self.authorized(req.auth_token.as_deref()) {
                WireResponse {
                    ok: false,
                    error: Some("unauthorized".into()),
                    result: None,
                    metrics: None,
                }
            } else {
                match req.op.as_str() {
                    "ping" => WireResponse {
                        ok: true,
                        error: None,
                        result: None,
                        metrics: None,
                    },
                    "metrics" => WireResponse {
                        ok: true,
                        error: None,
                        result: None,
                        metrics: Some(self.engine.metrics()),
                    },
                    "query" | "explain" => {
                        let sql = req.sql.unwrap_or_default();
                        let actual_sql = if req.op == "explain"
                            && !sql.to_ascii_uppercase().starts_with("EXPLAIN")
                        {
                            format!("EXPLAIN {sql}")
                        } else {
                            sql
                        };
                        match self.engine.execute_sql(&actual_sql, &req.params, None) {
                            Ok(r) => WireResponse {
                                ok: true,
                                error: None,
                                result: Some(r),
                                metrics: None,
                            },
                            Err(e) => WireResponse {
                                ok: false,
                                error: Some(e.to_string()),
                                result: None,
                                metrics: None,
                            },
                        }
                    }
                    other => WireResponse {
                        ok: false,
                        error: Some(format!("unknown wire op {other}")),
                        result: None,
                        metrics: None,
                    },
                }
            };
            let out = serde_json::to_vec(&resp)?;
            stream.write_all(&(out.len() as u32).to_be_bytes())?;
            stream.write_all(&out)?;
        }
        Ok(())
    }

    pub fn serve_http(self: Arc<Self>, bind_addr: &str) -> Result<String> {
        let listener = TcpListener::bind(bind_addr)?;
        let addr = listener.local_addr()?.to_string();
        let srv = self.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let s = srv.clone();
                thread::spawn(move || {
                    let _ = s.handle_http_conn(stream);
                });
            }
        });
        Ok(addr)
    }

    fn handle_http_conn(&self, mut stream: TcpStream) -> Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut req_line = String::new();
        if reader.read_line(&mut req_line)? == 0 {
            return Ok(());
        }
        let parts: Vec<&str> = req_line.split_whitespace().collect();
        if parts.len() < 2 {
            return Ok(());
        }
        let method = parts[0];
        let path = parts[1];

        let mut content_len = 0usize;
        let mut bearer: Option<String> = None;
        loop {
            let mut hline = String::new();
            reader.read_line(&mut hline)?;
            let trimmed = hline.trim();
            if trimmed.is_empty() {
                break;
            }
            if let Some((k, v)) = trimmed.split_once(':') {
                if k.eq_ignore_ascii_case("Content-Length") {
                    content_len = v.trim().parse().unwrap_or(0);
                } else if k.eq_ignore_ascii_case("Authorization")
                    && let Some(tok) = v.trim().strip_prefix("Bearer ")
                {
                    bearer = Some(tok.to_string());
                }
            }
        }
        let mut body = vec![0u8; content_len.min(4 * 1024 * 1024)];
        if !body.is_empty() {
            reader.read_exact(&mut body)?;
        }

        if path != "/health" && !self.authorized(bearer.as_deref()) {
            return write_http(&mut stream, 401, r#"{"error":"unauthorized"}"#);
        }

        match (method, path) {
            ("GET", "/health") => {
                write_http(&mut stream, 200, r#"{"status":"ok","engine":"titandb"}"#)
            }
            ("GET", "/metrics") => {
                let json = serde_json::to_string(&self.engine.metrics())?;
                write_http(&mut stream, 200, &json)
            }
            ("GET", "/cluster/status") => {
                if let Some(r) = &self.raft {
                    let json = serde_json::to_string(&r.status())?;
                    write_http(&mut stream, 200, &json)
                } else {
                    write_http(&mut stream, 200, r#"{"mode":"standalone"}"#)
                }
            }
            ("POST", "/query") => {
                #[derive(serde::Deserialize)]
                struct QReq {
                    sql: String,
                    #[serde(default)]
                    params: Vec<Value>,
                }
                let q: QReq = serde_json::from_slice(&body)?;
                match self.engine.execute_sql(&q.sql, &q.params, None) {
                    Ok(res) => write_http(&mut stream, 200, &serde_json::to_string(&res)?),
                    Err(e) => write_http(
                        &mut stream,
                        400,
                        &serde_json::to_string(&serde_json::json!({"error": e.to_string()}))?,
                    ),
                }
            }
            ("POST", "/backup") => {
                #[derive(serde::Deserialize)]
                struct BReq {
                    dest_dir: String,
                }
                let b: BReq = serde_json::from_slice(&body)?;
                match self.engine.backup_to(&b.dest_dir) {
                    Ok(_) => write_http(&mut stream, 200, r#"{"status":"backed_up"}"#),
                    Err(e) => write_http(
                        &mut stream,
                        500,
                        &serde_json::to_string(&serde_json::json!({"error": e.to_string()}))?,
                    ),
                }
            }
            _ => write_http(&mut stream, 404, r#"{"error":"not found"}"#),
        }
    }
}

fn write_http(stream: &mut TcpStream, code: u16, body: &str) -> Result<()> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "Error",
    };
    let resp = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes())?;
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
