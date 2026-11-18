use crate::error::{Result, TitanError};
use crate::sql::ast::*;
use crate::txn::Value;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    Str(String),
    Param(usize),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Semi,
    Eof,
}

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
    qmark_count: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            src: input.as_bytes(),
            pos: 0,
            qmark_count: 0,
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut out = Vec::new();
        while self.pos < self.src.len() {
            let b = self.src[self.pos];
            if b.is_ascii_whitespace() {
                self.pos += 1;
                continue;
            }
            if b == b'-' && self.pos + 1 < self.src.len() && self.src[self.pos + 1] == b'-' {
                while self.pos < self.src.len() && self.src[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            match b {
                b'*' => {
                    out.push(Token::Star);
                    self.pos += 1;
                }
                b',' => {
                    out.push(Token::Comma);
                    self.pos += 1;
                }
                b'.' => {
                    out.push(Token::Dot);
                    self.pos += 1;
                }
                b'(' => {
                    out.push(Token::LParen);
                    self.pos += 1;
                }
                b')' => {
                    out.push(Token::RParen);
                    self.pos += 1;
                }
                b';' => {
                    out.push(Token::Semi);
                    self.pos += 1;
                }
                b'=' => {
                    out.push(Token::Eq);
                    self.pos += 1;
                }
                b'?' => {
                    self.qmark_count += 1;
                    out.push(Token::Param(self.qmark_count));
                    self.pos += 1;
                }
                b'$' => {
                    self.pos += 1;
                    let start = self.pos;
                    while self.pos < self.src.len() && self.src[self.pos].is_ascii_digit() {
                        self.pos += 1;
                    }
                    let idx: usize = std::str::from_utf8(&self.src[start..self.pos])
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(0);
                    if idx == 0 {
                        return Err(TitanError::SqlSyntax("invalid $ parameter index".into()));
                    }
                    out.push(Token::Param(idx));
                }
                b'!' if self.peek(1) == Some(b'=') => {
                    out.push(Token::Neq);
                    self.pos += 2;
                }
                b'<' if self.peek(1) == Some(b'>') => {
                    out.push(Token::Neq);
                    self.pos += 2;
                }
                b'<' if self.peek(1) == Some(b'=') => {
                    out.push(Token::Lte);
                    self.pos += 2;
                }
                b'<' => {
                    out.push(Token::Lt);
                    self.pos += 1;
                }
                b'>' if self.peek(1) == Some(b'=') => {
                    out.push(Token::Gte);
                    self.pos += 2;
                }
                b'>' => {
                    out.push(Token::Gt);
                    self.pos += 1;
                }
                b'\'' => {
                    self.pos += 1;
                    let mut s = String::new();
                    let mut closed = false;
                    while self.pos < self.src.len() {
                        let c = self.src[self.pos];
                        if c == b'\'' {
                            if self.peek(1) == Some(b'\'') {
                                s.push('\'');
                                self.pos += 2;
                                continue;
                            }
                            self.pos += 1;
                            closed = true;
                            break;
                        }
                        s.push(c as char);
                        self.pos += 1;
                    }
                    if !closed {
                        return Err(TitanError::SqlSyntax("unterminated string literal".into()));
                    }
                    out.push(Token::Str(s));
                }
                _ if b.is_ascii_digit()
                    || (b == b'-' && self.peek(1).is_some_and(|c| c.is_ascii_digit())) =>
                {
                    let start = self.pos;
                    if b == b'-' {
                        self.pos += 1;
                    }
                    let mut seen_dot = false;
                    while self.pos < self.src.len() {
                        let c = self.src[self.pos];
                        if c.is_ascii_digit() {
                            self.pos += 1;
                        } else if c == b'.' && !seen_dot {
                            seen_dot = true;
                            self.pos += 1;
                        } else {
                            break;
                        }
                    }
                    let num = std::str::from_utf8(&self.src[start..self.pos])
                        .unwrap_or("0")
                        .to_string();
                    out.push(Token::Number(num));
                }
                _ if b.is_ascii_alphabetic() || b == b'_' => {
                    let start = self.pos;
                    while self.pos < self.src.len()
                        && (self.src[self.pos].is_ascii_alphanumeric()
                            || self.src[self.pos] == b'_')
                    {
                        self.pos += 1;
                    }
                    let id = std::str::from_utf8(&self.src[start..self.pos])
                        .unwrap_or("")
                        .to_string();
                    out.push(Token::Ident(id));
                }
                _ => {
                    return Err(TitanError::SqlSyntax(format!(
                        "unexpected character '{}' at pos {}",
                        b as char, self.pos
                    )));
                }
            }
        }
        out.push(Token::Eof);
        Ok(out)
    }

    fn peek(&self, ahead: usize) -> Option<u8> {
        self.src.get(self.pos + ahead).copied()
    }
}

pub fn parse_sql(input: &str) -> Result<Statement> {
    if input.len() > 256 * 1024 {
        return Err(TitanError::SqlSyntax(
            "SQL input exceeds 256 KiB limit".into(),
        ));
    }
    let tokens = Lexer::new(input).tokenize()?;
    let mut p = Parser {
        tokens,
        pos: 0,
        depth: 0,
    };
    let stmt = p.parse_stmt()?;
    while p.current() == &Token::Semi {
        p.pos += 1;
    }
    if p.current() != &Token::Eof {
        return Err(TitanError::SqlSyntax(format!(
            "trailing token {:?}",
            p.current()
        )));
    }
    Ok(stmt)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
}

impl Parser {
    fn current(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if let Token::Ident(s) = self.current()
            && s.eq_ignore_ascii_case(kw)
        {
            self.pos += 1;
            return true;
        }
        false
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(TitanError::SqlSyntax(format!(
                "expected keyword {kw}, got {:?}",
                self.current()
            )))
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.current().clone() {
            Token::Ident(s) => {
                self.pos += 1;
                Ok(s)
            }
            other => Err(TitanError::SqlSyntax(format!(
                "expected identifier, got {other:?}"
            ))),
        }
    }

    fn expect_tok(&mut self, tok: Token) -> Result<()> {
        if self.current() == &tok {
            self.pos += 1;
            Ok(())
        } else {
            Err(TitanError::SqlSyntax(format!(
                "expected {tok:?}, got {:?}",
                self.current()
            )))
        }
    }

    fn parse_stmt(&mut self) -> Result<Statement> {
        self.depth += 1;
        if self.depth > 64 {
            return Err(TitanError::SqlSyntax(
                "recursion depth limit exceeded".into(),
            ));
        }
        let res = match self.current().clone() {
            Token::Ident(kw) => match kw.to_ascii_uppercase().as_str() {
                "BEGIN" => {
                    self.pos += 1;
                    Ok(Statement::Begin)
                }
                "COMMIT" => {
                    self.pos += 1;
                    Ok(Statement::Commit)
                }
                "ROLLBACK" => {
                    self.pos += 1;
                    Ok(Statement::Rollback)
                }
                "USE" => {
                    self.pos += 1;
                    Ok(Statement::UseDatabase {
                        name: self.expect_ident()?,
                    })
                }
                "CREATE" => self.parse_create(),
                "DROP" => self.parse_drop(),
                "INSERT" => self.parse_insert(),
                "UPDATE" => self.parse_update(),
                "DELETE" => self.parse_delete(),
                "SELECT" => self.parse_select(),
                "EXPLAIN" => {
                    self.pos += 1;
                    Ok(Statement::Explain(Box::new(self.parse_stmt()?)))
                }
                "PREPARE" => self.parse_prepare(),
                "EXECUTE" => self.parse_execute(),
                other => Err(TitanError::SqlSyntax(format!(
                    "unsupported SQL command: {other}"
                ))),
            },
            other => Err(TitanError::SqlSyntax(format!(
                "unexpected token at start: {other:?}"
            ))),
        };
        self.depth -= 1;
        res
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.pos += 1; // CREATE
        if self.eat_kw("DATABASE") {
            return Ok(Statement::CreateDatabase {
                name: self.expect_ident()?,
            });
        }
        if self.eat_kw("INDEX") {
            let name = self.expect_ident()?;
            self.expect_kw("ON")?;
            let table = self.expect_ident()?;
            self.expect_tok(Token::LParen)?;
            let column = self.expect_ident()?;
            self.expect_tok(Token::RParen)?;
            return Ok(Statement::CreateIndex {
                name,
                table,
                column,
            });
        }
        self.expect_kw("TABLE")?;
        let name = self.expect_ident()?;
        self.expect_tok(Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.expect_ident()?;
            let ty_str = self.expect_ident()?.to_ascii_uppercase();
            let data_type = match ty_str.as_str() {
                "INT" | "INTEGER" | "BIGINT" => DataType::Int,
                "FLOAT" | "DOUBLE" | "REAL" => DataType::Float,
                "BOOL" | "BOOLEAN" => DataType::Bool,
                _ => DataType::Text,
            };
            let mut primary_key = false;
            let mut not_null = false;
            loop {
                if self.eat_kw("PRIMARY") {
                    self.expect_kw("KEY")?;
                    primary_key = true;
                    not_null = true;
                } else if self.eat_kw("NOT") {
                    self.expect_kw("NULL")?;
                    not_null = true;
                } else {
                    break;
                }
            }
            columns.push(ColumnDef {
                name: col_name,
                data_type,
                primary_key,
                not_null,
            });
            if self.current() == &Token::Comma {
                self.pos += 1;
                continue;
            }
            break;
        }
        self.expect_tok(Token::RParen)?;
        Ok(Statement::CreateTable { name, columns })
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.pos += 1;
        self.expect_kw("TABLE")?;
        Ok(Statement::DropTable {
            name: self.expect_ident()?,
        })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.pos += 1;
        self.expect_kw("INTO")?;
        let table = self.expect_ident()?;
        let mut columns = None;
        if self.current() == &Token::LParen {
            self.pos += 1;
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if self.current() == &Token::Comma {
                    self.pos += 1;
                    continue;
                }
                break;
            }
            self.expect_tok(Token::RParen)?;
            columns = Some(cols);
        }
        self.expect_kw("VALUES")?;
        let mut rows = Vec::new();
        loop {
            self.expect_tok(Token::LParen)?;
            let mut vals = Vec::new();
            loop {
                vals.push(self.parse_expr()?);
                if self.current() == &Token::Comma {
                    self.pos += 1;
                    continue;
                }
                break;
            }
            self.expect_tok(Token::RParen)?;
            rows.push(vals);
            if self.current() == &Token::Comma {
                self.pos += 1;
                continue;
            }
            break;
        }
        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.pos += 1;
        let table = self.expect_ident()?;
        self.expect_kw("SET")?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect_tok(Token::Eq)?;
            let expr = self.parse_expr()?;
            assignments.push((col, expr));
            if self.current() == &Token::Comma {
                self.pos += 1;
                continue;
            }
            break;
        }
        let filter = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            filter,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.pos += 1;
        self.expect_kw("FROM")?;
        let table = self.expect_ident()?;
        let filter = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete { table, filter })
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.pos += 1;
        let mut items = Vec::new();
        loop {
            if self.current() == &Token::Star {
                self.pos += 1;
                items.push(SelectItem {
                    expr: None,
                    alias: None,
                });
            } else {
                let e = self.parse_expr()?;
                let alias = if self.eat_kw("AS") {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                items.push(SelectItem {
                    expr: Some(e),
                    alias,
                });
            }
            if self.current() == &Token::Comma {
                self.pos += 1;
                continue;
            }
            break;
        }
        self.expect_kw("FROM")?;
        let from = self.expect_ident()?;
        let mut joins = Vec::new();
        while self.eat_kw("INNER") || self.eat_kw("JOIN") {
            // If INNER consumed, also consume JOIN
            let _ = self.eat_kw("JOIN");
            let jtable = self.expect_ident()?;
            self.expect_kw("ON")?;
            let on = self.parse_expr()?;
            joins.push(JoinClause { table: jtable, on });
        }
        let filter = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let mut group_by = Vec::new();
        if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            loop {
                group_by.push(self.expect_col_ref()?);
                if self.current() == &Token::Comma {
                    self.pos += 1;
                    continue;
                }
                break;
            }
        }
        let mut order_by = Vec::new();
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                let col = self.expect_col_ref()?;
                let desc = if self.eat_kw("DESC") {
                    true
                } else {
                    let _ = self.eat_kw("ASC");
                    false
                };
                order_by.push(OrderByItem { col, desc });
                if self.current() == &Token::Comma {
                    self.pos += 1;
                    continue;
                }
                break;
            }
        }
        let limit = if self.eat_kw("LIMIT") {
            match self.current().clone() {
                Token::Number(n) => {
                    self.pos += 1;
                    Some(
                        n.parse::<usize>()
                            .map_err(|_| TitanError::SqlSyntax("invalid LIMIT".into()))?,
                    )
                }
                other => {
                    return Err(TitanError::SqlSyntax(format!(
                        "expected number after LIMIT, got {other:?}"
                    )));
                }
            }
        } else {
            None
        };
        Ok(Statement::Select {
            items,
            from,
            joins,
            filter,
            group_by,
            order_by,
            limit,
        })
    }

    fn parse_prepare(&mut self) -> Result<Statement> {
        self.pos += 1;
        let name = self.expect_ident()?;
        self.expect_kw("AS")?;
        let stmt = self.parse_stmt()?;
        Ok(Statement::Prepare {
            name,
            stmt: Box::new(stmt),
        })
    }

    fn parse_execute(&mut self) -> Result<Statement> {
        self.pos += 1;
        let name = self.expect_ident()?;
        let mut params = Vec::new();
        if self.current() == &Token::LParen {
            self.pos += 1;
            if self.current() != &Token::RParen {
                loop {
                    params.push(self.parse_expr()?);
                    if self.current() == &Token::Comma {
                        self.pos += 1;
                        continue;
                    }
                    break;
                }
            }
            self.expect_tok(Token::RParen)?;
        }
        Ok(Statement::Execute { name, params })
    }

    fn expect_col_ref(&mut self) -> Result<String> {
        let first = self.expect_ident()?;
        if self.current() == &Token::Dot {
            self.pos += 1;
            let second = self.expect_ident()?;
            Ok(format!("{first}.{second}"))
        } else {
            Ok(first)
        }
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.depth += 1;
        if self.depth > 64 {
            return Err(TitanError::SqlSyntax("expression recursion limit".into()));
        }
        let res = self.parse_or();
        self.depth -= 1;
        res
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_and()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_cmp()?;
        while self.eat_kw("AND") {
            let right = self.parse_cmp()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_cmp(&mut self) -> Result<Expr> {
        if self.eat_kw("NOT") {
            return Ok(Expr::Not(Box::new(self.parse_cmp()?)));
        }
        let left = self.parse_primary()?;
        let op = match self.current() {
            Token::Eq => BinaryOp::Eq,
            Token::Neq => BinaryOp::Neq,
            Token::Lt => BinaryOp::Lt,
            Token::Lte => BinaryOp::Lte,
            Token::Gt => BinaryOp::Gt,
            Token::Gte => BinaryOp::Gte,
            Token::Ident(s) if s.eq_ignore_ascii_case("LIKE") => BinaryOp::Like,
            _ => return Ok(left),
        };
        self.pos += 1;
        let right = self.parse_primary()?;
        Ok(Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.current().clone() {
            Token::LParen => {
                self.pos += 1;
                let inner = self.parse_expr()?;
                self.expect_tok(Token::RParen)?;
                Ok(inner)
            }
            Token::Param(idx) => {
                self.pos += 1;
                Ok(Expr::Param(idx))
            }
            Token::Str(s) => {
                self.pos += 1;
                Ok(Expr::Literal(Value::Text(s)))
            }
            Token::Number(n) => {
                self.pos += 1;
                if n.contains('.') {
                    let v: f64 = n
                        .parse()
                        .map_err(|_| TitanError::SqlSyntax("bad float".into()))?;
                    Ok(Expr::Literal(Value::Float(v)))
                } else {
                    let v: i64 = n
                        .parse()
                        .map_err(|_| TitanError::SqlSyntax("bad integer".into()))?;
                    Ok(Expr::Literal(Value::Int(v)))
                }
            }
            Token::Ident(s) => {
                self.pos += 1;
                let up = s.to_ascii_uppercase();
                match up.as_str() {
                    "TRUE" => return Ok(Expr::Literal(Value::Bool(true))),
                    "FALSE" => return Ok(Expr::Literal(Value::Bool(false))),
                    "NULL" => return Ok(Expr::Literal(Value::Null)),
                    "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" if self.current() == &Token::LParen => {
                        self.pos += 1;
                        let arg = if self.current() == &Token::Star {
                            self.pos += 1;
                            None
                        } else {
                            Some(Box::new(self.parse_expr()?))
                        };
                        self.expect_tok(Token::RParen)?;
                        return Ok(Expr::Aggregate { func: up, arg });
                    }
                    _ => {}
                }
                if self.current() == &Token::Dot {
                    self.pos += 1;
                    let col = self.expect_ident()?;
                    Ok(Expr::Column {
                        table: Some(s),
                        name: col,
                    })
                } else {
                    Ok(Expr::Column {
                        table: None,
                        name: s,
                    })
                }
            }
            other => Err(TitanError::SqlSyntax(format!("unexpected token {other:?}"))),
        }
    }
}
