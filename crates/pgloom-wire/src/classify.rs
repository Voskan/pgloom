//! Lightweight SQL classification for scheduling decisions.
//!
//! The classifier is a lexer, not a parser: it understands comments, string
//! literals (including `E''` and dollar quoting) and quoted identifiers well
//! enough to find statement boundaries, leading keywords, DML targets, the
//! tables in `FROM`/`JOIN` clauses and row-locking clauses. When in doubt it
//! errs on the side of treating a statement as a data write with unknown
//! targets, which only costs exploration time, never soundness.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Transaction-control statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TxControl {
    Begin,
    Commit,
    Rollback,
    Savepoint,
    ReleaseSavepoint,
    RollbackToSavepoint,
    PrepareTransaction,
}

impl TxControl {
    /// Whether the statement ends the top-level transaction.
    pub fn ends_transaction(self) -> bool {
        matches!(
            self,
            TxControl::Commit | TxControl::Rollback | TxControl::PrepareTransaction
        )
    }
}

/// Explicit lock acquired by a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RowLock {
    ForUpdate,
    ForNoKeyUpdate,
    ForShare,
    ForKeyShare,
    Advisory,
    Table,
}

impl RowLock {
    pub fn sql(self) -> &'static str {
        match self {
            RowLock::ForUpdate => "FOR UPDATE",
            RowLock::ForNoKeyUpdate => "FOR NO KEY UPDATE",
            RowLock::ForShare => "FOR SHARE",
            RowLock::ForKeyShare => "FOR KEY SHARE",
            RowLock::Advisory => "advisory lock",
            RowLock::Table => "LOCK TABLE",
        }
    }
}

/// Coarse classification of a single SQL statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatementKind {
    /// Driver chatter that never participates in interleaving.
    Noise,
    TransactionControl(TxControl),
    DataRead {
        tables: Vec<String>,
        row_lock: Option<RowLock>,
    },
    DataWrite {
        target_tables: Vec<String>,
        /// The statement may write to tables that could not be determined
        /// (functions, procedures, DDL, `DO` blocks).
        opaque: bool,
    },
}

/// Scheduling class of a whole `WireTurn`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnKind {
    Noise,
    TransactionControl,
    DataRead { has_row_lock: bool },
    DataWrite { target_tables: Vec<String> },
}

/// Aggregated classification of one or more statements.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnMeta {
    pub statements: Vec<StatementKind>,
}

impl TurnMeta {
    pub fn push(&mut self, kind: StatementKind) {
        self.statements.push(kind);
    }

    pub fn extend(&mut self, other: TurnMeta) {
        self.statements.extend(other.statements);
    }

    pub fn kind(&self) -> TurnKind {
        if self.has_write() {
            return TurnKind::DataWrite {
                target_tables: self.write_tables().into_iter().collect(),
            };
        }
        if self
            .statements
            .iter()
            .any(|s| matches!(s, StatementKind::DataRead { .. }))
        {
            return TurnKind::DataRead {
                has_row_lock: self.row_lock().is_some(),
            };
        }
        if self.tx_controls().next().is_some() {
            return TurnKind::TransactionControl;
        }
        TurnKind::Noise
    }

    pub fn is_noise(&self) -> bool {
        matches!(self.kind(), TurnKind::Noise)
    }

    pub fn has_write(&self) -> bool {
        self.statements
            .iter()
            .any(|s| matches!(s, StatementKind::DataWrite { .. }))
    }

    pub fn tx_controls(&self) -> impl Iterator<Item = TxControl> + '_ {
        self.statements.iter().filter_map(|s| match s {
            StatementKind::TransactionControl(tc) => Some(*tc),
            _ => None,
        })
    }

    /// True when the turn only opens a transaction (possibly with noise).
    pub fn is_begin_only(&self) -> bool {
        let mut saw_begin = false;
        for s in &self.statements {
            match s {
                StatementKind::Noise => {}
                StatementKind::TransactionControl(TxControl::Begin) => saw_begin = true,
                _ => return false,
            }
        }
        saw_begin
    }

    /// True when the turn only contains `COMMIT`/`ROLLBACK`-like statements.
    pub fn is_tx_end_only(&self) -> bool {
        let mut saw_end = false;
        for s in &self.statements {
            match s {
                StatementKind::Noise => {}
                StatementKind::TransactionControl(tc) if tc.ends_transaction() => saw_end = true,
                _ => return false,
            }
        }
        saw_end
    }

    pub fn write_tables(&self) -> BTreeSet<String> {
        self.statements
            .iter()
            .filter_map(|s| match s {
                StatementKind::DataWrite { target_tables, .. } => Some(target_tables),
                _ => None,
            })
            .flatten()
            .cloned()
            .collect()
    }

    pub fn read_tables(&self) -> BTreeSet<String> {
        self.statements
            .iter()
            .filter_map(|s| match s {
                StatementKind::DataRead { tables, .. } => Some(tables),
                _ => None,
            })
            .flatten()
            .cloned()
            .collect()
    }

    pub fn has_opaque_write(&self) -> bool {
        self.statements
            .iter()
            .any(|s| matches!(s, StatementKind::DataWrite { opaque: true, .. }))
    }

    pub fn row_lock(&self) -> Option<RowLock> {
        self.statements.iter().find_map(|s| match s {
            StatementKind::DataRead { row_lock, .. } => *row_lock,
            _ => None,
        })
    }
}

/// Classifies every statement in `sql` (which may contain several statements
/// separated by semicolons, as in a simple-query message).
pub fn classify_sql(sql: &str) -> TurnMeta {
    let tokens = tokenize(sql);
    let mut meta = TurnMeta::default();
    for stmt in tokens.split(|t| *t == Token::Symbol(';')) {
        if stmt.is_empty() {
            continue;
        }
        meta.push(classify_statement(stmt));
    }
    if meta.statements.is_empty() {
        meta.push(StatementKind::Noise);
    }
    meta
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// Unquoted identifier or keyword, lowercased.
    Word(String),
    /// Double-quoted identifier, case preserved.
    Quoted(String),
    Symbol(char),
    Literal,
    Param,
}

impl Token {
    fn word(&self) -> Option<&str> {
        match self {
            Token::Word(w) => Some(w),
            _ => None,
        }
    }

    fn is_word(&self, w: &str) -> bool {
        self.word() == Some(w)
    }

    fn ident(&self) -> Option<&str> {
        match self {
            Token::Word(w) | Token::Quoted(w) => Some(w),
            _ => None,
        }
    }
}

fn tokenize(sql: &str) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let n = chars.len();
    while i < n {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < n && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            let mut depth = 0usize;
            while i < n {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if c == '\'' {
            i = skip_string(&chars, i + 1, false);
            out.push(Token::Literal);
        } else if (c == 'e' || c == 'E') && chars.get(i + 1) == Some(&'\'') {
            i = skip_string(&chars, i + 2, true);
            out.push(Token::Literal);
        } else if c == '"' {
            let mut ident = String::new();
            i += 1;
            while i < n {
                if chars[i] == '"' {
                    if chars.get(i + 1) == Some(&'"') {
                        ident.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                ident.push(chars[i]);
                i += 1;
            }
            out.push(Token::Quoted(ident));
        } else if c == '$' {
            if chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()) {
                i += 1;
                while i < n && chars[i].is_ascii_digit() {
                    i += 1;
                }
                out.push(Token::Param);
            } else if let Some(end) = dollar_quote_end(&chars, i) {
                i = end;
                out.push(Token::Literal);
            } else {
                out.push(Token::Symbol('$'));
                i += 1;
            }
        } else if c.is_ascii_digit() {
            while i < n && (chars[i].is_ascii_alphanumeric() || chars[i] == '.') {
                i += 1;
            }
            out.push(Token::Literal);
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < n && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            out.push(Token::Word(word.to_lowercase()));
        } else {
            out.push(Token::Symbol(c));
            i += 1;
        }
    }
    out
}

fn skip_string(chars: &[char], mut i: usize, backslash_escapes: bool) -> usize {
    while i < chars.len() {
        match chars[i] {
            '\\' if backslash_escapes => i += 2,
            '\'' if chars.get(i + 1) == Some(&'\'') => i += 2,
            '\'' => return i + 1,
            _ => i += 1,
        }
    }
    i
}

/// Returns the index just past a `$tag$ ... $tag$` literal starting at `start`.
fn dollar_quote_end(chars: &[char], start: usize) -> Option<usize> {
    let mut j = start + 1;
    while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
        j += 1;
    }
    if chars.get(j) != Some(&'$') {
        return None;
    }
    let delim: Vec<char> = chars[start..=j].to_vec();
    let mut k = j + 1;
    while k + delim.len() <= chars.len() {
        if chars[k..k + delim.len()] == delim[..] {
            return Some(k + delim.len());
        }
        k += 1;
    }
    Some(chars.len())
}

fn classify_statement(tokens: &[Token]) -> StatementKind {
    let start = tokens
        .iter()
        .position(|t| *t != Token::Symbol('('))
        .unwrap_or(tokens.len());
    let tokens = &tokens[start..];
    let Some(first) = tokens.first().and_then(Token::word) else {
        return StatementKind::Noise;
    };
    let second = tokens.get(1).and_then(Token::word);
    match first {
        "begin" | "start" => StatementKind::TransactionControl(TxControl::Begin),
        "commit" | "end" => StatementKind::TransactionControl(TxControl::Commit),
        "rollback" | "abort" => {
            if tokens.iter().skip(1).any(|t| t.is_word("to")) {
                StatementKind::TransactionControl(TxControl::RollbackToSavepoint)
            } else {
                StatementKind::TransactionControl(TxControl::Rollback)
            }
        }
        "savepoint" => StatementKind::TransactionControl(TxControl::Savepoint),
        "release" => StatementKind::TransactionControl(TxControl::ReleaseSavepoint),
        "prepare" if second == Some("transaction") => {
            StatementKind::TransactionControl(TxControl::PrepareTransaction)
        }
        "prepare" | "set" | "show" | "reset" | "discard" | "deallocate" | "listen" | "unlisten"
        | "load" | "close" | "fetch" | "move" | "declare" => StatementKind::Noise,
        "explain" => classify_explain(tokens),
        "select" | "with" | "values" | "table" => classify_query(tokens),
        "insert" | "update" | "delete" | "merge" | "truncate" | "copy" | "notify" => {
            classify_query(tokens)
        }
        "lock" => StatementKind::DataRead {
            tables: idents_after_lock(tokens),
            row_lock: Some(RowLock::Table),
        },
        _ => StatementKind::DataWrite {
            target_tables: Vec::new(),
            opaque: true,
        },
    }
}

fn classify_explain(tokens: &[Token]) -> StatementKind {
    let analyze = tokens
        .iter()
        .take_while(|t| {
            !matches!(
                t.word(),
                Some("select" | "insert" | "update" | "delete" | "merge" | "with")
            )
        })
        .any(|t| t.is_word("analyze") || t.is_word("analyse"));
    if !analyze {
        return StatementKind::Noise;
    }
    match tokens.iter().position(|t| {
        matches!(
            t.word(),
            Some("select" | "insert" | "update" | "delete" | "merge" | "with" | "values")
        )
    }) {
        Some(p) => classify_query(&tokens[p..]),
        None => StatementKind::Noise,
    }
}

/// Reads a possibly schema-qualified name starting at `i`.
fn qualified_name(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    let first = tokens.get(i)?.ident()?;
    if is_reserved_after_from(first) && matches!(tokens.get(i), Some(Token::Word(_))) {
        return None;
    }
    let mut name = first.to_owned();
    let mut j = i + 1;
    while tokens.get(j) == Some(&Token::Symbol('.')) {
        match tokens.get(j + 1).and_then(Token::ident) {
            Some(part) => {
                name.push('.');
                name.push_str(part);
                j += 2;
            }
            None => break,
        }
    }
    Some((name, j))
}

fn is_reserved_after_from(w: &str) -> bool {
    matches!(
        w,
        "select"
            | "lateral"
            | "only"
            | "where"
            | "group"
            | "order"
            | "limit"
            | "on"
            | "using"
            | "set"
            | "values"
            | "default"
    )
}

fn skip_words(tokens: &[Token], mut i: usize, words: &[&str]) -> usize {
    while tokens
        .get(i)
        .and_then(Token::word)
        .is_some_and(|w| words.contains(&w))
    {
        i += 1;
    }
    i
}

fn idents_after_lock(tokens: &[Token]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = skip_words(tokens, 1, &["table", "only"]);
    while let Some((name, next)) = qualified_name(tokens, i) {
        if name == "in" || name == "nowait" {
            break;
        }
        out.push(name);
        i = next;
        if tokens.get(i) == Some(&Token::Symbol(',')) {
            i = skip_words(tokens, i + 1, &["only"]);
        } else {
            break;
        }
    }
    out
}

const NOISE_FUNCTIONS: &[&str] = &[
    "version",
    "current_setting",
    "set_config",
    "current_schema",
    "current_schemas",
    "current_database",
    "current_catalog",
    "current_user",
    "current_role",
    "session_user",
    "pg_backend_pid",
    "now",
    "clock_timestamp",
    "statement_timestamp",
    "transaction_timestamp",
    "timeofday",
    "pg_is_in_recovery",
    "inet_server_addr",
    "inet_server_port",
    "inet_client_addr",
    "inet_client_port",
    "pg_postmaster_start_time",
    "pg_conf_load_time",
    "to_regclass",
    "to_regtype",
    "format_type",
    "obj_description",
    "col_description",
    "pg_typeof",
    "txid_current",
    "pg_current_xact_id",
    "coalesce",
    "nullif",
    "greatest",
    "least",
    "lower",
    "upper",
    "length",
    "concat",
    "format",
    "cast",
    "gen_random_uuid",
    "random",
];

const ADVISORY_LOCK_FUNCTIONS: &[&str] = &[
    "pg_advisory_lock",
    "pg_advisory_lock_shared",
    "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_xact_lock_shared",
];

fn is_system_relation(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    let (schema, rel) = match lower.rsplit_once('.') {
        Some((s, r)) => (Some(s.to_owned()), r.to_owned()),
        None => (None, lower.clone()),
    };
    match schema.as_deref() {
        Some("pg_catalog" | "information_schema") => true,
        Some(_) => false,
        None => rel.starts_with("pg_"),
    }
}

/// Everything a single pass over a DML or query statement extracts.
#[derive(Debug, Default)]
struct QueryScan {
    writes: Vec<String>,
    reads: Vec<String>,
    row_lock: Option<RowLock>,
    /// Calls a function that is neither noise nor SQL syntax.
    user_functions: bool,
    /// Every function-call-shaped identifier, lowercase and unqualified.
    calls: Vec<String>,
    /// Common table expression names, which shadow relations.
    ctes: Vec<String>,
    /// A `FOR UPDATE`-style row lock clause is present.
    locking: bool,
    advisory: bool,
    is_copy_to: bool,
}

fn scan_query(tokens: &[Token]) -> QueryScan {
    let mut writes: Vec<String> = Vec::new();
    let mut reads: Vec<String> = Vec::new();
    let mut row_lock: Option<RowLock> = None;
    let mut user_functions = false;
    let mut is_copy_to = false;
    let mut calls: Vec<String> = Vec::new();
    let mut ctes: Vec<String> = Vec::new();
    let mut locking = false;
    let mut advisory = false;

    let first = tokens.first().and_then(Token::word).unwrap_or_default();
    if first == "copy" {
        let to = tokens.iter().any(|t| t.is_word("to"));
        let from = tokens.iter().any(|t| t.is_word("from"));
        is_copy_to = to && !from;
    }

    let mut i = 0;
    while i < tokens.len() {
        let prev = i.checked_sub(1).and_then(|p| tokens[p].word());
        let Some(w) = tokens[i].word() else {
            i += 1;
            continue;
        };
        match w {
            "insert" | "merge" if tokens.get(i + 1).is_some_and(|t| t.is_word("into")) => {
                let j = skip_words(tokens, i + 2, &["only"]);
                if let Some((name, next)) = qualified_name(tokens, j) {
                    writes.push(name);
                    i = next;
                    continue;
                }
            }
            "update" if !matches!(prev, Some("for" | "key" | "do")) => {
                let j = skip_words(tokens, i + 1, &["only"]);
                if let Some((name, next)) = qualified_name(tokens, j) {
                    writes.push(name);
                    i = next;
                    continue;
                }
            }
            "delete" if tokens.get(i + 1).is_some_and(|t| t.is_word("from")) => {
                let j = skip_words(tokens, i + 2, &["only"]);
                if let Some((name, next)) = qualified_name(tokens, j) {
                    writes.push(name);
                    i = next;
                    continue;
                }
            }
            "truncate" => {
                let mut j = skip_words(tokens, i + 1, &["table", "only"]);
                while let Some((name, next)) = qualified_name(tokens, j) {
                    if matches!(
                        name.as_str(),
                        "restart" | "continue" | "cascade" | "restrict"
                    ) {
                        break;
                    }
                    writes.push(name);
                    j = next;
                    if tokens.get(j) == Some(&Token::Symbol(',')) {
                        j = skip_words(tokens, j + 1, &["only"]);
                    } else {
                        break;
                    }
                }
                i = j;
                continue;
            }
            "copy" if i == 0 => {
                if let Some((name, next)) = qualified_name(tokens, i + 1) {
                    if is_copy_to {
                        reads.push(name);
                    } else {
                        writes.push(name);
                    }
                    i = next;
                    continue;
                }
            }
            "into" if first == "select" && !matches!(prev, Some("insert" | "merge")) => {
                let j = skip_words(
                    tokens,
                    i + 1,
                    &["temp", "temporary", "unlogged", "table", "strict"],
                );
                if let Some((name, next)) = qualified_name(tokens, j) {
                    writes.push(name);
                    i = next;
                    continue;
                }
            }
            "from" | "join" | "using" if prev != Some("is") && prev != Some("distinct") => {
                let mut j = skip_words(tokens, i + 1, &["only", "lateral"]);
                while let Some((name, next)) = qualified_name(tokens, j) {
                    if tokens.get(next) == Some(&Token::Symbol('(')) {
                        // Set-returning function in FROM.
                        let fname = name.rsplit('.').next().unwrap_or(&name).to_lowercase();
                        if !NOISE_FUNCTIONS.contains(&fname.as_str()) {
                            user_functions = true;
                        }
                        calls.push(fname);
                        j = next;
                        break;
                    }
                    reads.push(name);
                    j = next;
                    // Skip an optional alias, then continue a comma list.
                    j = skip_words(tokens, j, &["as"]);
                    if let Some(Token::Word(alias)) = tokens.get(j)
                        && !is_clause_keyword(alias)
                    {
                        j += 1;
                    }
                    if tokens.get(j) == Some(&Token::Symbol(',')) {
                        j = skip_words(tokens, j + 1, &["only", "lateral"]);
                        continue;
                    }
                    break;
                }
                i = j.max(i + 1);
                continue;
            }
            "for" => {
                let next = tokens.get(i + 1).and_then(Token::word);
                let next2 = tokens.get(i + 2).and_then(Token::word);
                let next3 = tokens.get(i + 3).and_then(Token::word);
                let lock = match (next, next2, next3) {
                    (Some("update"), _, _) => Some(RowLock::ForUpdate),
                    (Some("no"), Some("key"), Some("update")) => Some(RowLock::ForNoKeyUpdate),
                    (Some("share"), _, _) => Some(RowLock::ForShare),
                    (Some("key"), Some("share"), _) => Some(RowLock::ForKeyShare),
                    _ => None,
                };
                if lock.is_some() {
                    locking = true;
                    if row_lock.is_none() {
                        row_lock = lock;
                    }
                }
            }
            _ => {
                if is_cte_name(tokens, i) {
                    ctes.push(w.to_owned());
                } else if tokens.get(i + 1) == Some(&Token::Symbol('(')) && prev != Some("into") {
                    let qualified_noise = i >= 2
                        && tokens[i - 1] == Token::Symbol('.')
                        && tokens[i - 2].is_word("pg_catalog");
                    if !is_call_syntax(w) {
                        calls.push(w.to_owned());
                    }
                    if ADVISORY_LOCK_FUNCTIONS.contains(&w) {
                        advisory = true;
                        row_lock.get_or_insert(RowLock::Advisory);
                    } else if !qualified_noise
                        && !NOISE_FUNCTIONS.contains(&w)
                        && !is_sql_keyword_call(w)
                        && !w.starts_with("pg_get_")
                        && !w.starts_with("has_")
                    {
                        user_functions = true;
                    }
                }
            }
        }
        i += 1;
    }

    dedup(&mut writes);
    dedup(&mut reads);
    dedup(&mut calls);
    dedup(&mut ctes);
    QueryScan {
        writes,
        reads,
        row_lock,
        user_functions,
        calls,
        ctes,
        locking,
        advisory,
        is_copy_to,
    }
}

fn classify_query(tokens: &[Token]) -> StatementKind {
    let first = tokens.first().and_then(Token::word).unwrap_or_default();
    if first == "notify" {
        return StatementKind::Noise;
    }
    let QueryScan {
        writes,
        reads,
        row_lock,
        user_functions,
        is_copy_to,
        ..
    } = scan_query(tokens);

    if !writes.is_empty() {
        return StatementKind::DataWrite {
            target_tables: writes,
            opaque: false,
        };
    }
    let user_reads: Vec<String> = reads
        .iter()
        .filter(|t| !is_system_relation(t))
        .cloned()
        .collect();
    if matches!(
        first,
        "insert" | "update" | "delete" | "merge" | "truncate" | "copy"
    ) && !is_copy_to
    {
        return StatementKind::DataWrite {
            target_tables: Vec::new(),
            opaque: true,
        };
    }
    if user_reads.is_empty() && reads.is_empty() && user_functions && row_lock.is_none() {
        // `SELECT my_procedure(...)` may mutate arbitrary tables.
        return StatementKind::DataWrite {
            target_tables: Vec::new(),
            opaque: true,
        };
    }
    if user_reads.is_empty() && row_lock.is_none() {
        return StatementKind::Noise;
    }
    StatementKind::DataRead {
        tables: user_reads,
        row_lock,
    }
}

/// What one statement may touch. Two scheduling steps whose accesses do not
/// conflict commute: executing them in either order yields the same results
/// and the same database state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Access {
    pub tx: Option<TxControl>,
    /// Relations read without locks, as written in the statement.
    pub reads: Vec<String>,
    /// Relations written or locked (`FOR UPDATE`, `LOCK TABLE`).
    pub writes: Vec<String>,
    /// Functions called, lowercase and unqualified.
    pub calls: Vec<String>,
    /// Takes advisory locks.
    pub advisory: bool,
    /// Runs with a snapshot: the first such statement of a `REPEATABLE READ`
    /// or `SERIALIZABLE` transaction fixes what the whole transaction sees.
    pub snapshot: bool,
    /// The statement could not be analysed (DDL, `DO`, `CALL`, procedures,
    /// unknown commands) and may touch anything.
    pub opaque: bool,
}

impl Access {
    fn opaque() -> Self {
        Self {
            opaque: true,
            snapshot: true,
            ..Self::default()
        }
    }
}

/// Classifies every statement of `sql` by the database objects it touches.
pub fn classify_access(sql: &str) -> Vec<Access> {
    tokenize(sql)
        .split(|t| *t == Token::Symbol(';'))
        .filter(|stmt| !stmt.is_empty())
        .map(access_of)
        .collect()
}

fn access_of(tokens: &[Token]) -> Access {
    let start = tokens
        .iter()
        .position(|t| *t != Token::Symbol('('))
        .unwrap_or(tokens.len());
    let tokens = &tokens[start..];
    let first = tokens.first().and_then(Token::word).unwrap_or_default();
    match first {
        "select" | "with" | "values" | "table" | "insert" | "update" | "delete" | "merge"
        | "truncate" | "copy" => access_of_query(tokens),
        "explain" => match classify_explain(tokens) {
            StatementKind::Noise => Access::default(),
            _ => tokens
                .iter()
                .position(|t| {
                    matches!(
                        t.word(),
                        Some(
                            "select" | "insert" | "update" | "delete" | "merge" | "with" | "values"
                        )
                    )
                })
                .map_or_else(Access::opaque, |p| access_of_query(&tokens[p..])),
        },
        "lock" => Access {
            writes: idents_after_lock(tokens),
            ..Access::default()
        },
        "notify" => Access::default(),
        _ => match classify_statement(tokens) {
            StatementKind::Noise => Access::default(),
            StatementKind::TransactionControl(tc) => Access {
                tx: Some(tc),
                ..Access::default()
            },
            StatementKind::DataRead { .. } | StatementKind::DataWrite { .. } => Access::opaque(),
        },
    }
}

fn access_of_query(tokens: &[Token]) -> Access {
    let opaque = matches!(
        classify_query(tokens),
        StatementKind::DataWrite { opaque: true, .. }
    );
    let scan = scan_query(tokens);
    let shadowed = |name: &String| {
        let rel = name.rsplit('.').next().unwrap_or(name);
        !name.contains('.') && scan.ctes.iter().any(|c| c == rel)
    };
    let mut reads: Vec<String> = scan
        .reads
        .into_iter()
        .filter(|t| !is_system_relation(t) && !shadowed(t))
        .collect();
    let mut writes = scan.writes;
    if scan.locking {
        // Row-level locks apply to the rows of the tables being read.
        writes.append(&mut reads);
        dedup(&mut writes);
    }
    Access {
        tx: None,
        reads,
        writes,
        calls: scan.calls,
        advisory: scan.advisory,
        snapshot: true,
        opaque,
    }
}

/// `name AS (` directly after `WITH`, `RECURSIVE` or a comma.
fn is_cte_name(tokens: &[Token], i: usize) -> bool {
    let prev_ok = i
        .checked_sub(1)
        .and_then(|p| tokens.get(p))
        .is_some_and(|t| t.is_word("with") || t.is_word("recursive") || *t == Token::Symbol(','));
    if !prev_ok || !tokens.get(i + 1).is_some_and(|t| t.is_word("as")) {
        return false;
    }
    let j = skip_words(tokens, i + 2, &["not", "materialized"]);
    tokens.get(j) == Some(&Token::Symbol('('))
}

/// Keywords that are followed by a parenthesis without being a call.
fn is_call_syntax(w: &str) -> bool {
    matches!(
        w,
        "in" | "exists"
            | "any"
            | "all"
            | "some"
            | "values"
            | "row"
            | "array"
            | "as"
            | "on"
            | "over"
            | "filter"
            | "using"
            | "returning"
            | "conflict"
            | "and"
            | "or"
            | "not"
            | "where"
            | "set"
            | "select"
            | "from"
            | "into"
            | "by"
            | "then"
            | "else"
            | "when"
            | "interval"
            | "within"
            | "lateral"
            | "join"
            | "is"
            | "like"
            | "ilike"
            | "between"
            | "case"
            | "distinct"
            | "group"
            | "having"
            | "limit"
            | "offset"
            | "union"
            | "except"
            | "intersect"
            | "default"
            | "materialized"
    )
}

fn is_clause_keyword(w: &str) -> bool {
    matches!(
        w,
        "where"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "full"
            | "cross"
            | "natural"
            | "on"
            | "group"
            | "order"
            | "limit"
            | "offset"
            | "for"
            | "having"
            | "window"
            | "union"
            | "except"
            | "intersect"
            | "returning"
            | "set"
            | "using"
            | "fetch"
            | "into"
            | "values"
            | "select"
            | "when"
            | "do"
            | "to"
            | "with"
    )
}

/// Keywords that syntactically look like function calls.
fn is_sql_keyword_call(w: &str) -> bool {
    matches!(
        w,
        "in" | "exists"
            | "any"
            | "all"
            | "some"
            | "values"
            | "count"
            | "sum"
            | "min"
            | "max"
            | "avg"
            | "array_agg"
            | "string_agg"
            | "row"
            | "array"
            | "as"
            | "on"
            | "over"
            | "filter"
            | "using"
            | "returning"
            | "conflict"
            | "and"
            | "or"
            | "not"
            | "where"
            | "set"
            | "select"
            | "from"
            | "into"
            | "by"
            | "then"
            | "else"
            | "when"
            | "abs"
            | "round"
            | "extract"
            | "interval"
            | "nextval"
            | "currval"
            | "setval"
            | "lastval"
            | "jsonb_build_object"
            | "json_build_object"
            | "to_json"
            | "to_jsonb"
            | "unnest"
            | "generate_series"
            | "substring"
            | "trim"
            | "position"
            | "overlay"
    )
}

fn dedup(v: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    v.retain(|t| seen.insert(t.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind(sql: &str) -> TurnKind {
        classify_sql(sql).kind()
    }

    fn single(sql: &str) -> StatementKind {
        let meta = classify_sql(sql);
        assert_eq!(meta.statements.len(), 1, "{sql}");
        meta.statements.into_iter().next().unwrap()
    }

    #[test]
    fn driver_noise() {
        for sql in [
            "",
            "   ",
            ";",
            "-- just a comment",
            "/* block */",
            "SELECT 1",
            "select 1;",
            "SELECT version()",
            "SHOW transaction_isolation",
            "SET application_name = 'api'",
            "SET LOCAL lock_timeout = '1s'",
            "RESET ALL",
            "DISCARD ALL",
            "DEALLOCATE ALL",
            "SELECT current_setting('server_version_num')",
            "select pg_backend_pid()",
            "SELECT t.oid, t.typname FROM pg_type t WHERE t.typname = $1",
            "SELECT * FROM pg_catalog.pg_namespace",
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'x'",
            "EXPLAIN SELECT * FROM wallets",
            "PREPARE q AS SELECT 1",
        ] {
            assert_eq!(kind(sql), TurnKind::Noise, "{sql:?}");
        }
    }

    #[test]
    fn transaction_control() {
        let cases = [
            ("BEGIN", TxControl::Begin),
            ("begin isolation level repeatable read", TxControl::Begin),
            ("START TRANSACTION", TxControl::Begin),
            ("COMMIT", TxControl::Commit),
            ("END", TxControl::Commit),
            ("ROLLBACK", TxControl::Rollback),
            ("ABORT", TxControl::Rollback),
            ("SAVEPOINT sp1", TxControl::Savepoint),
            ("RELEASE SAVEPOINT sp1", TxControl::ReleaseSavepoint),
            ("ROLLBACK TO SAVEPOINT sp1", TxControl::RollbackToSavepoint),
            ("PREPARE TRANSACTION 'x'", TxControl::PrepareTransaction),
        ];
        for (sql, tc) in cases {
            assert_eq!(single(sql), StatementKind::TransactionControl(tc), "{sql}");
            assert_eq!(kind(sql), TurnKind::TransactionControl);
        }
        assert!(classify_sql("BEGIN").is_begin_only());
        assert!(classify_sql("COMMIT").is_tx_end_only());
        assert!(!classify_sql("BEGIN; SELECT * FROM t").is_begin_only());
    }

    #[test]
    fn plain_and_locking_reads() {
        assert_eq!(
            single("SELECT balance FROM wallets WHERE id = $1"),
            StatementKind::DataRead {
                tables: vec!["wallets".into()],
                row_lock: None
            }
        );
        let cases = [
            (
                "SELECT balance FROM wallets WHERE id = $1 FOR UPDATE",
                RowLock::ForUpdate,
            ),
            (
                "select balance from wallets where id = $1 for no key update",
                RowLock::ForNoKeyUpdate,
            ),
            ("SELECT * FROM t FOR SHARE SKIP LOCKED", RowLock::ForShare),
            ("SELECT * FROM t FOR KEY SHARE", RowLock::ForKeyShare),
            ("SELECT pg_advisory_xact_lock(42)", RowLock::Advisory),
        ];
        for (sql, lock) in cases {
            match single(sql) {
                StatementKind::DataRead { row_lock, .. } => {
                    assert_eq!(row_lock, Some(lock), "{sql}");
                }
                other => panic!("{sql}: {other:?}"),
            }
            assert_eq!(kind(sql), TurnKind::DataRead { has_row_lock: true });
        }
    }

    #[test]
    fn joins_aliases_and_qualified_names() {
        match single(
            r#"SELECT a.id FROM public.accounts a, users AS u JOIN "Ledger" l ON l.account_id = a.id WHERE true"#,
        ) {
            StatementKind::DataRead { tables, .. } => {
                assert_eq!(tables, vec!["public.accounts", "users", "Ledger"]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn writes_with_targets() {
        let cases = [
            (
                "UPDATE wallets SET balance = balance - $1 WHERE id = $2",
                "wallets",
            ),
            (
                "update only public.wallets set balance = 0",
                "public.wallets",
            ),
            ("INSERT INTO orders (id) VALUES ($1) RETURNING id", "orders"),
            ("DELETE FROM sessions WHERE expired", "sessions"),
            (
                "MERGE INTO stock s USING incoming i ON s.id = i.id WHEN MATCHED THEN UPDATE SET qty = i.qty",
                "stock",
            ),
            ("TRUNCATE TABLE audit_log", "audit_log"),
            ("COPY items FROM STDIN", "items"),
            ("SELECT * INTO TEMP snapshot FROM wallets", "snapshot"),
        ];
        for (sql, target) in cases {
            match kind(sql) {
                TurnKind::DataWrite { target_tables } => {
                    assert!(
                        target_tables.contains(&target.to_string()),
                        "{sql}: {target_tables:?}"
                    );
                }
                other => panic!("{sql}: {other:?}"),
            }
        }
    }

    #[test]
    fn upsert_and_cte_writes() {
        match kind("INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET n = t.n + 1") {
            TurnKind::DataWrite { target_tables } => assert_eq!(target_tables, vec!["t"]),
            other => panic!("{other:?}"),
        }
        match kind(
            "WITH moved AS (DELETE FROM queue WHERE id = $1 RETURNING *) INSERT INTO done SELECT * FROM moved",
        ) {
            TurnKind::DataWrite { target_tables } => {
                assert_eq!(target_tables, vec!["done", "queue"]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn opaque_writes() {
        for sql in [
            "CALL transfer(1, 2, 100)",
            "DO $$ BEGIN UPDATE t SET x = 1; END $$",
            "SELECT withdraw(1, 100)",
            "CREATE TABLE x (id int)",
        ] {
            assert_eq!(
                single(sql),
                StatementKind::DataWrite {
                    target_tables: vec![],
                    opaque: true
                },
                "{sql}"
            );
        }
    }

    #[test]
    fn literals_and_comments_do_not_confuse_the_lexer() {
        assert_eq!(
            kind("SELECT 'UPDATE wallets SET x = 1' -- DELETE FROM t"),
            TurnKind::Noise
        );
        assert_eq!(kind("SELECT $tag$ DELETE FROM t; $tag$"), TurnKind::Noise);
        assert_eq!(kind(r"SELECT E'it\'s; DELETE FROM t'"), TurnKind::Noise);
        assert_eq!(
            kind("/* UPDATE x */ SELECT balance FROM wallets"),
            TurnKind::DataRead {
                has_row_lock: false
            }
        );
    }

    #[test]
    fn multi_statement_simple_query() {
        let meta = classify_sql(
            "BEGIN; SELECT balance FROM wallets WHERE id = 1; UPDATE wallets SET balance = 0 WHERE id = 1; COMMIT;",
        );
        assert_eq!(meta.statements.len(), 4);
        assert_eq!(
            meta.kind(),
            TurnKind::DataWrite {
                target_tables: vec!["wallets".into()]
            }
        );
        assert_eq!(
            meta.read_tables().into_iter().collect::<Vec<_>>(),
            vec!["wallets"]
        );
    }

    #[test]
    fn lock_table_statement() {
        match single("LOCK TABLE ONLY accounts, ledger IN EXCLUSIVE MODE") {
            StatementKind::DataRead { tables, row_lock } => {
                assert_eq!(tables, vec!["accounts", "ledger"]);
                assert_eq!(row_lock, Some(RowLock::Table));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn aggregate_reads_are_reads() {
        assert_eq!(
            kind("SELECT COUNT(*) FROM coupons WHERE code = $1"),
            TurnKind::DataRead {
                has_row_lock: false
            }
        );
        assert_eq!(
            kind("SELECT COALESCE(MIN(balance), 0) >= 0 FROM wallets"),
            TurnKind::DataRead {
                has_row_lock: false
            }
        );
    }

    fn access(sql: &str) -> Access {
        let mut all = classify_access(sql);
        assert_eq!(all.len(), 1, "{sql}");
        all.remove(0)
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn access_separates_reads_writes_and_locks() {
        let a = access("SELECT balance FROM wallets WHERE id = $1");
        assert_eq!((a.reads, a.writes), (names(&["wallets"]), vec![]));

        let a = access("SELECT balance FROM wallets WHERE id = $1 FOR NO KEY UPDATE");
        assert_eq!((a.reads, a.writes), (vec![], names(&["wallets"])));

        let a = access(
            "UPDATE wallets SET balance = balance - $1 WHERE id IN (SELECT wallet_id FROM holds)",
        );
        assert_eq!(a.writes, names(&["wallets"]));
        assert_eq!(a.reads, names(&["holds"]));
        assert!(!a.opaque);

        let a = access("INSERT INTO ledger (wallet_id, amount) SELECT id, $1 FROM wallets");
        assert_eq!(
            (a.reads, a.writes),
            (names(&["wallets"]), names(&["ledger"]))
        );

        assert_eq!(
            access("LOCK TABLE accounts IN EXCLUSIVE MODE").writes,
            names(&["accounts"])
        );
    }

    #[test]
    fn access_ignores_cte_names_and_system_catalogs() {
        let a = access(
            "WITH debited AS (UPDATE wallets SET balance = 0 WHERE id = 1 RETURNING id) \
             SELECT d.id FROM debited d JOIN pg_class c ON true",
        );
        assert_eq!(a.writes, names(&["wallets"]));
        assert!(a.reads.is_empty(), "{:?}", a.reads);
    }

    #[test]
    fn access_records_calls_transactions_and_opaque_statements() {
        let a = access("SELECT transfer($1, $2)");
        assert!(a.opaque);
        assert_eq!(a.calls, names(&["transfer"]));

        let a = access("SELECT COALESCE(SUM(amount), 0) FROM ledger WHERE wallet_id = $1");
        assert_eq!(a.calls, names(&["coalesce", "sum"]));
        assert!(!a.opaque);

        let a = access("SELECT pg_advisory_xact_lock(42)");
        assert!(a.advisory);

        let all = classify_access("BEGIN; UPDATE t SET x = 1; COMMIT");
        assert_eq!(all[0].tx, Some(TxControl::Begin));
        assert_eq!(all[1].writes, names(&["t"]));
        assert_eq!(all[2].tx, Some(TxControl::Commit));

        assert!(access("CREATE INDEX ON t (x)").opaque);
        assert!(access("CALL settle()").opaque);
        assert_eq!(access("SET search_path = app"), Access::default());
        assert_eq!(access("EXPLAIN SELECT * FROM t"), Access::default());
        assert_eq!(
            access("EXPLAIN ANALYZE SELECT * FROM t").reads,
            names(&["t"])
        );
    }
}
