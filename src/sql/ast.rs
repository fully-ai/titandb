use serde::{Deserialize, Serialize};

use crate::txn::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Float,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub not_null: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Like,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    Column {
        table: Option<String>,
        name: String,
    },
    Param(usize),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    Aggregate {
        func: String,
        arg: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Option<Expr>, // None means '*'
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub table: String,
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub col: String,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Begin,
    Commit,
    Rollback,
    CreateDatabase {
        name: String,
    },
    UseDatabase {
        name: String,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },
    CreateIndex {
        name: String,
        table: String,
        column: String,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Select {
        items: Vec<SelectItem>,
        from: String,
        joins: Vec<JoinClause>,
        filter: Option<Expr>,
        group_by: Vec<String>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
    },
    Explain(Box<Statement>),
    Prepare {
        name: String,
        stmt: Box<Statement>,
    },
    Execute {
        name: String,
        params: Vec<Expr>,
    },
}
