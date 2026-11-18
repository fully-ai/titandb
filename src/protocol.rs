use serde::{Deserialize, Serialize};

use crate::engine::{EngineMetrics, QueryResult};
use crate::txn::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireRequest {
    pub auth_token: Option<String>,
    pub op: String, // "query", "explain", "ping", "metrics"
    pub sql: Option<String>,
    pub params: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub result: Option<QueryResult>,
    pub metrics: Option<EngineMetrics>,
}
