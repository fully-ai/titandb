use std::fmt;

/// Unified error type across TitanDB storage, transaction, SQL, and Raft layers.
#[derive(Debug)]
pub enum TitanError {
    Io(std::io::Error),
    Corruption(String),
    SqlSyntax(String),
    Execution(String),
    Transaction(String),
    Deadlock(String),
    LockTimeout(String),
    NotFound(String),
    Protocol(String),
    Raft(String),
}

pub type Result<T> = std::result::Result<T, TitanError>;

impl fmt::Display for TitanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TitanError::Io(e) => write!(f, "I/O error: {e}"),
            TitanError::Corruption(m) => write!(f, "Corruption detected: {m}"),
            TitanError::SqlSyntax(m) => write!(f, "SQL syntax error: {m}"),
            TitanError::Execution(m) => write!(f, "Execution error: {m}"),
            TitanError::Transaction(m) => write!(f, "Transaction error: {m}"),
            TitanError::Deadlock(m) => write!(f, "Deadlock aborted: {m}"),
            TitanError::LockTimeout(m) => write!(f, "Lock timeout: {m}"),
            TitanError::NotFound(m) => write!(f, "Not found: {m}"),
            TitanError::Protocol(m) => write!(f, "Protocol error: {m}"),
            TitanError::Raft(m) => write!(f, "Raft error: {m}"),
        }
    }
}

impl std::error::Error for TitanError {}

impl From<std::io::Error> for TitanError {
    fn from(value: std::io::Error) -> Self {
        TitanError::Io(value)
    }
}

impl From<serde_json::Error> for TitanError {
    fn from(value: serde_json::Error) -> Self {
        TitanError::Protocol(value.to_string())
    }
}
