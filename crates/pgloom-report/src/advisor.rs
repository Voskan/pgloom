//! Fix Advisor: maps a failing trace to a known concurrency anti-pattern and
//! concrete remediations.

use std::collections::BTreeMap;

use pgloom_engine::{EpisodeRecord, StepAction, Violation, WorkerId};
use pgloom_wire::classify_sql;
use serde::Serialize;

use crate::style::one_line;
use crate::timeline::first_keyword;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Pattern {
    LockOrderInversion,
    PhantomInsert,
    CheckThenAct,
    LostUpdate,
    WriteSkew,
}

impl Pattern {
    pub fn title(self) -> &'static str {
        match self {
            Pattern::LockOrderInversion => "Lock-Order Inversion (deadlock)",
            Pattern::PhantomInsert => "Phantom Insert / Missing Unique Constraint",
            Pattern::CheckThenAct => "Unprotected Check-Then-Act under READ COMMITTED",
            Pattern::LostUpdate => "Lost Update (value computed in application memory)",
            Pattern::WriteSkew => "Write Skew (a check over a set of rows, then a write to one)",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Fix {
    pub title: String,
    pub sql: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Advice {
    pub pattern: Pattern,
    pub explanation: Vec<String>,
    pub fixes: Vec<Fix>,
}

/// Statements a worker executed, in order, with their bind parameters.
struct Trace<'a> {
    sql: Vec<&'a str>,
    params: Vec<&'a [String]>,
}

fn traces(record: &EpisodeRecord) -> BTreeMap<WorkerId, Trace<'_>> {
    let mut out: BTreeMap<WorkerId, Trace<'_>> = BTreeMap::new();
    for step in &record.steps {
        if matches!(
            step.action,
            StepAction::Execute | StepAction::Begin | StepAction::Park { .. }
        ) && !step.sql.is_empty()
        {
            let t = out.entry(step.worker).or_insert_with(|| Trace {
                sql: Vec::new(),
                params: Vec::new(),
            });
            t.sql.push(&step.sql);
            t.params.push(&step.params);
        }
    }
    out
}

/// A read of `table` without a row lock, followed by a write to it.
#[derive(Debug)]
struct ReadThenWrite<'a> {
    table: String,
    read: &'a str,
    write: &'a str,
    write_params: &'a [String],
    /// An earlier unlocked read of a table this transaction never writes,
    /// such as the parent row whose limit the transaction enforces.
    parent: Option<&'a str>,
}

fn read_then_write<'a>(trace: &Trace<'a>) -> Option<ReadThenWrite<'a>> {
    for (i, read) in trace.sql.iter().enumerate() {
        let meta = classify_sql(read);
        if meta.has_write() || meta.row_lock().is_some() {
            continue;
        }
        for table in meta.read_tables() {
            if let Some(j) = trace.sql[i + 1..]
                .iter()
                .position(|w| classify_sql(w).write_tables().contains(&table))
            {
                let j = i + 1 + j;
                let written: Vec<String> = trace
                    .sql
                    .iter()
                    .flat_map(|w| classify_sql(w).write_tables())
                    .collect();
                let parent = trace.sql[..j].iter().copied().find(|r| {
                    let m = classify_sql(r);
                    !m.has_write()
                        && m.row_lock().is_none()
                        && !is_aggregate_read(r)
                        && !m.read_tables().is_empty()
                        && m.read_tables().iter().all(|t| !written.contains(t))
                });
                return Some(ReadThenWrite {
                    table,
                    read,
                    write: trace.sql[j],
                    write_params: trace.params[j],
                    parent,
                });
            }
        }
    }
    None
}

/// Returns remediation advice, most specific pattern first.
pub fn advise(record: &EpisodeRecord, violations: &[Violation], names: &[String]) -> Vec<Advice> {
    let name = |w: WorkerId| {
        names
            .get(w.index())
            .cloned()
            .unwrap_or_else(|| w.to_string())
    };
    let mut out = Vec::new();

    if let Some(Violation::Deadlock {
        cycle, statements, ..
    }) = violations.iter().find(|v| v.is_deadlock())
    {
        out.push(lock_order(cycle, statements, &name));
    }

    let traces = traces(record);
    let found: Vec<(WorkerId, ReadThenWrite<'_>)> = traces
        .iter()
        .filter_map(|(w, t)| read_then_write(t).map(|rw| (*w, rw)))
        .collect();
    let Some((_, first)) = found.first() else {
        return out;
    };
    let who = found
        .iter()
        .map(|(w, _)| name(*w))
        .collect::<Vec<_>>()
        .join(" and ");
    let subject = if found.len() > 1 {
        format!("Workers {who} each")
    } else {
        format!("Worker {who}")
    };
    let read = one_line(first.read).trim_end_matches(';').to_owned();
    let write = one_line(first.write).trim_end_matches(';').to_owned();
    let table = &first.table;

    if first_keyword(first.write) == "INSERT" {
        let (insert_body, returning) = split_returning(&write);
        let on_conflict = match returning {
            Some(ret) => format!("{insert_body} ON CONFLICT DO NOTHING {ret};"),
            None => format!("{insert_body} ON CONFLICT DO NOTHING;"),
        };
        let mut fixes = Vec::new();
        if let Some(parent) = first.parent {
            let parent = one_line(parent).trim_end_matches(';').to_owned();
            let parent_table = classify_sql(&parent)
                .read_tables()
                .into_iter()
                .next()
                .unwrap_or_default();
            fixes.push(Fix {
                title: "Fix Option A (Lock the parent row first):".into(),
                sql: vec![
                    format!("{parent} FOR NO KEY UPDATE;"),
                    format!(
                        "-- checks and inserts for the same `{parent_table}` row now run one at a time."),
                ],
            });
        }
        fixes.push(Fix {
            title: format!(
                "Fix Option {} (Unique Constraint, when at most one row may exist):",
                option(fixes.len())
            ),
            sql: vec![
                format!("CREATE UNIQUE INDEX CONCURRENTLY ON {table} (<natural key columns>);"),
                on_conflict,
                "-- and treat rows_affected == 0 as \"already exists\".".into(),
            ],
        });
        if first.parent.is_none() {
            fixes.push(Fix {
                title: format!("Fix Option {} (Serialize on the key):", option(fixes.len())),
                sql: vec![
                    "SELECT pg_advisory_xact_lock(hashtext(<natural key>));".into(),
                    format!("-- then run: {read}"),
                ],
            });
        }
        out.push(Advice {
            pattern: Pattern::PhantomInsert,
            explanation: vec![
                format!("{subject} checked `{table}` for existing rows and then inserted one."),
                "Under READ COMMITTED neither transaction sees the other's uncommitted row, and \
                 SELECT ... FOR UPDATE cannot lock a row that does not exist yet."
                    .into(),
            ],
            fixes,
        });
        return out;
    }

    let skew = is_write_skew(first, &found);
    if skew {
        let lock = lock_rows_read(&read).unwrap_or_else(|| {
            format!("SELECT 1 FROM {table} WHERE <the rows you checked> FOR NO KEY UPDATE;")
        });
        out.push(Advice {
            pattern: Pattern::WriteSkew,
            explanation: vec![
                format!(
                    "{subject} checked a condition over several `{table}` rows and then changed \
                     a different row in a later statement."
                ),
                "The transactions write different rows, so their row locks never conflict; each \
                 one's check passed on a state the other was about to invalidate."
                    .into(),
            ],
            fixes: vec![
                Fix {
                    title: "Fix Option A (Lock every row the check depends on):".into(),
                    sql: vec![
                        lock,
                        format!("-- run it before the check: {read}"),
                    ],
                },
                Fix {
                    title: "Fix Option B (Serializable isolation):".into(),
                    sql: vec![
                        "BEGIN ISOLATION LEVEL SERIALIZABLE;".into(),
                        "-- and retry the whole transaction on SQLSTATE 40001 (serialization_failure)."
                            .into(),
                    ],
                },
            ],
        });
        return out;
    }

    let lost = is_absolute_assignment(first.write);
    let mut explanation = vec![format!(
        "{subject} read `{table}` without a row lock and then wrote it in a later statement."
    )];
    if lost {
        explanation.push(
            "The UPDATE writes a value computed in application memory, so the second commit \
             silently overwrites the first (both callers are told they succeeded)."
                .into(),
        );
    } else {
        explanation.push(
            "Another transaction committed between the check and the write; READ COMMITTED \
             re-reads the row for the UPDATE but the application already made its decision."
                .into(),
        );
    }
    let lock = if is_aggregate_read(&read) {
        lock_rows_read(&read)
    } else {
        Some(format!("{read} FOR NO KEY UPDATE;"))
    };
    let mut fixes = Vec::new();
    if let Some(lock) = lock {
        fixes.push(Fix {
            title: "Fix Option A (Pessimistic Lock):".into(),
            sql: vec![
                lock,
                "-- FOR NO KEY UPDATE blocks concurrent writers of the row but, unlike \
                 FOR UPDATE, not inserts of rows referencing it by foreign key."
                    .into(),
            ],
        });
    }
    match (lost, atomic_update(first.write)) {
        (true, Some((increment, compare_and_set, col))) => {
            fixes.push(Fix {
                title: format!("Fix Option {} (Atomic Update):", option(fixes.len())),
                sql: vec![
                    increment,
                    format!(
                        "-- compute the change in SQL, not in application memory; add \
                         `AND {col} + <delta> >= 0` if the value must not go negative."
                    ),
                ],
            });
            fixes.push(Fix {
                title: format!("Fix Option {} (Compare-and-Set):", option(fixes.len())),
                sql: vec![
                    compare_and_set,
                    "-- rows_affected == 0 means another transaction changed the row: re-read \
                     and retry, or report a conflict."
                        .into(),
                ],
            });
        }
        _ => fixes.push(Fix {
            title: format!(
                "Fix Option {} (Atomic Guarded Update):",
                option(fixes.len())
            ),
            sql: vec![
                guarded_update(first.write).unwrap_or_else(|| {
                    append_where_guard(&write, "<the condition you checked in application code>")
                }),
                "-- and verify rows_affected == 1 before reporting success.".into(),
            ],
        }),
    }
    out.push(Advice {
        pattern: if lost {
            Pattern::LostUpdate
        } else {
            Pattern::CheckThenAct
        },
        explanation,
        fixes,
    });
    out
}

fn option(index: usize) -> char {
    char::from(b'A' + (index.min(25) as u8))
}

/// The read cannot take a row lock as written: PostgreSQL rejects `FOR
/// UPDATE` together with aggregates, `GROUP BY`, `HAVING` or `DISTINCT`.
fn is_aggregate_read(sql: &str) -> bool {
    let flat = one_line(sql).to_ascii_lowercase();
    let Some(from) = find_top_level(&flat, " from ") else {
        return false;
    };
    let projection = &flat[..from];
    let compact: String = projection.chars().filter(|c| !c.is_whitespace()).collect();
    [
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "bool_and(",
        "bool_or(",
        "every(",
        "array_agg(",
        "string_agg(",
        "json_agg(",
        "jsonb_agg(",
    ]
    .iter()
    .any(|f| compact.contains(f))
        || projection.trim_start().starts_with("select distinct")
        || find_top_level(&flat, " group by ").is_some()
        || find_top_level(&flat, " having ").is_some()
}

/// Rewrites a read into one that locks every row it looked at:
/// `SELECT count(*) FROM t WHERE p` becomes `SELECT 1 FROM t WHERE p FOR NO
/// KEY UPDATE;`. Returns `None` for set operations, which cannot be locked.
fn lock_rows_read(sql: &str) -> Option<String> {
    let flat = one_line(sql);
    let flat = flat.trim_end_matches(';');
    let lower = flat.to_ascii_lowercase();
    if !lower.starts_with("select ")
        || [" union ", " intersect ", " except "]
            .iter()
            .any(|op| find_top_level(&lower, op).is_some())
    {
        return None;
    }
    let from = find_top_level(&lower, " from ")?;
    let end = [
        " group by ",
        " having ",
        " order by ",
        " limit ",
        " offset ",
    ]
    .iter()
    .filter_map(|k| find_top_level(&lower, k))
    .min()
    .unwrap_or(flat.len());
    Some(format!("SELECT 1{} FOR NO KEY UPDATE;", &flat[from..end]))
}

/// The workers checked a set of rows rather than the row they changed: the
/// read aggregates, or the same `UPDATE` ran with different arguments
/// (different rows) in different workers.
fn is_write_skew(first: &ReadThenWrite<'_>, found: &[(WorkerId, ReadThenWrite<'_>)]) -> bool {
    if first_keyword(first.write) == "INSERT" {
        return false;
    }
    if is_aggregate_read(first.read) {
        return true;
    }
    let same_write: Vec<&ReadThenWrite<'_>> = found
        .iter()
        .map(|(_, rw)| rw)
        .filter(|rw| rw.write == first.write && rw.read == first.read)
        .collect();
    same_write.len() > 1
        && !first.write_params.is_empty()
        && same_write
            .iter()
            .any(|rw| rw.write_params != first.write_params)
        && !where_reads_params(first.read)
}

/// The read's `WHERE` clause compares against a bind parameter, so the
/// workers may have checked different rows on purpose.
fn where_reads_params(sql: &str) -> bool {
    let flat = one_line(sql);
    find_top_level(&flat.to_ascii_lowercase(), " where ").is_some_and(|w| flat[w..].contains('$'))
}

/// For an absolute `SET c = $1 WHERE p`, proposes the in-SQL increment
/// `SET c = c + <delta> WHERE p` and the compare-and-set
/// `SET c = $1 WHERE p AND c = <value you read>`.
fn atomic_update(sql: &str) -> Option<(String, String, String)> {
    let u = split_update(sql)?;
    let assigned = assignments(&u.set_clause);
    let cols: Vec<&str> = assigned.iter().map(|(c, _)| c.as_str()).collect();
    let increment = SplitUpdate {
        head: u.head.clone(),
        set_clause: cols
            .iter()
            .map(|c| format!("{c} = {c} + <delta>"))
            .collect::<Vec<_>>()
            .join(", "),
        from_clause: u.from_clause.clone(),
        cond: u.cond.clone(),
        returning: u.returning.clone(),
    }
    .render(None);
    let expected = cols
        .iter()
        .map(|c| format!("{c} = <value you read>"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let first = cols.first()?.to_string();
    Some((increment, u.render_with_guard(&expected), first))
}

fn lock_order(
    cycle: &[WorkerId],
    statements: &[(WorkerId, String)],
    name: &dyn Fn(WorkerId) -> String,
) -> Advice {
    let mut path: Vec<String> = cycle.iter().map(|w| name(*w)).collect();
    if let Some(first) = path.first().cloned() {
        path.push(first);
    }
    let mut explanation = vec![format!(
        "Workers acquire the same row locks in opposite orders: {}.",
        path.join(" → ")
    )];
    for (w, sql) in statements {
        explanation.push(format!("{} waits in: {}", name(*w), one_line(sql)));
    }
    Advice {
        pattern: Pattern::LockOrderInversion,
        explanation,
        fixes: vec![Fix {
            title: "Fix: lock rows in one canonical order at the start of the transaction:".into(),
            sql: vec![
                "SELECT id FROM <table> WHERE id = ANY($1) ORDER BY id FOR NO KEY UPDATE;".into(),
                "-- e.g. always lock the lower id first in transfers between two accounts.".into(),
            ],
        }],
    }
}

/// Byte offsets of `s` that lie outside parentheses and single-quoted
/// literals. Parentheses and quotes themselves are never reported.
fn top_level_offsets(s: &str) -> Vec<usize> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut in_str = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if in_str {
            if b == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_str = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(i),
            _ => {}
        }
        i += 1;
    }
    out
}

/// Finds the first top-level ASCII case-insensitive occurrence of `needle`.
fn find_top_level(s: &str, needle: &str) -> Option<usize> {
    top_level_offsets(s).into_iter().find(|&i| {
        s.get(i..i + needle.len())
            .is_some_and(|w| w.eq_ignore_ascii_case(needle))
    })
}

/// Splits `s` on commas that are not nested in parentheses or literals.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut from = 0;
    for i in top_level_offsets(s) {
        if s.as_bytes()[i] == b',' {
            parts.push(&s[from..i]);
            from = i + 1;
        }
    }
    parts.push(&s[from..]);
    parts
}

/// Splits off a trailing top-level `RETURNING ...` clause so `WHERE` / `ON
/// CONFLICT` additions are placed before `RETURNING`.
fn split_returning(sql: &str) -> (&str, Option<&str>) {
    match find_top_level(sql, " RETURNING ") {
        Some(pos) => (sql[..pos].trim_end(), Some(sql[pos + 1..].trim())),
        None => (sql, None),
    }
}

struct SplitUpdate {
    head: String,
    set_clause: String,
    from_clause: Option<String>,
    cond: Option<String>,
    returning: Option<String>,
}

impl SplitUpdate {
    fn render_with_guard(&self, guard: &str) -> String {
        self.render(Some(guard))
    }

    fn render(&self, guard: Option<&str>) -> String {
        let from = self
            .from_clause
            .as_deref()
            .map_or_else(String::new, |f| format!(" {f}"));
        let where_body = match (&self.cond, guard) {
            (Some(c), Some(g)) => format!(" WHERE {c} AND {g}"),
            (Some(c), None) => format!(" WHERE {c}"),
            (None, Some(g)) => format!(" WHERE {g}"),
            (None, None) => String::new(),
        };
        let ret = self
            .returning
            .as_deref()
            .map_or_else(String::new, |r| format!(" {r}"));
        format!(
            "{} SET {}{from}{where_body}{ret};",
            self.head, self.set_clause
        )
    }
}

/// Parses an `UPDATE` into `head`, `SET`, optional `FROM`, optional `WHERE`,
/// and optional `RETURNING` clauses at top level.
fn split_update(sql: &str) -> Option<SplitUpdate> {
    let flat = one_line(sql);
    let flat = flat.trim_end_matches(';');
    if flat.len() < 7 || !flat[..7].eq_ignore_ascii_case("UPDATE ") {
        return None;
    }
    let (without_ret, returning) = split_returning(flat);
    let set_pos = find_top_level(without_ret, " SET ")?;
    let head = without_ret[..set_pos].to_owned();
    let after_set = &without_ret[set_pos + 5..];
    let (before_where, cond) = match find_top_level(after_set, " WHERE ") {
        Some(w) => (&after_set[..w], Some(after_set[w + 7..].trim().to_owned())),
        None => (after_set, None),
    };
    let (set_clause, from_clause) = match find_top_level(before_where, " FROM ") {
        Some(f) => (
            before_where[..f].trim().to_owned(),
            Some(before_where[f + 1..].trim().to_owned()),
        ),
        None => (before_where.trim().to_owned(), None),
    };
    Some(SplitUpdate {
        head,
        set_clause,
        from_clause,
        cond,
        returning: returning.map(str::to_owned),
    })
}

fn append_where_guard(sql: &str, guard: &str) -> String {
    match split_update(sql) {
        Some(u) => u.render_with_guard(guard),
        None => {
            let flat = one_line(sql);
            let (body, ret) = split_returning(flat.trim_end_matches(';'));
            let joiner = if find_top_level(body, " WHERE ").is_some() {
                "AND"
            } else {
                "WHERE"
            };
            let ret = ret.map_or_else(String::new, |r| format!(" {r}"));
            format!("{body} {joiner} {guard}{ret};")
        }
    }
}

fn assignments(set: &str) -> Vec<(String, String)> {
    split_top_level_commas(set)
        .into_iter()
        .filter_map(|a| a.split_once('='))
        .map(|(c, e)| (c.trim().to_owned(), e.trim().to_owned()))
        .collect()
}

/// Every assignment writes a literal or a bind parameter rather than an
/// expression over the current row.
fn is_absolute_assignment(sql: &str) -> bool {
    let Some(u) = split_update(sql) else {
        return false;
    };
    let a = assignments(&u.set_clause);
    !a.is_empty() && a.iter().all(|(_, e)| is_constant(e))
}

/// A bind parameter or literal, possibly cast the way ORMs emit them:
/// `$1`, `$1::INTEGER`, `CAST($1 AS numeric)`, `42`, `'text'`, `true`.
fn is_constant(expr: &str) -> bool {
    let mut e = expr.trim();
    let lower = e.to_ascii_lowercase();
    if lower.starts_with("cast(") && e.ends_with(')') {
        let inner = &e[5..e.len() - 1];
        return find_top_level(&inner.to_ascii_lowercase(), " as ")
            .is_some_and(|p| is_constant(&inner[..p]));
    }
    if let Some(p) = top_level_offsets(e)
        .into_iter()
        .find(|&i| e.get(i..i + 2) == Some("::"))
    {
        let type_name = &e[p + 2..];
        let is_type = type_name.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || matches!(c, '_' | ' ' | '.' | '"' | ':' | '[' | ']' | '(' | ')' | ',')
        });
        if !is_type {
            return false;
        }
        e = e[..p].trim();
    }
    let lower = e.to_ascii_lowercase();
    e.strip_prefix('$')
        .is_some_and(|d| !d.is_empty() && d.chars().all(|c| c.is_ascii_digit()))
        || e.parse::<f64>().is_ok()
        || (e.starts_with('\'') && top_level_offsets(e).is_empty())
        || matches!(lower.as_str(), "true" | "false" | "null")
}

/// For `SET c = c - x WHERE p [RETURNING ...]`, proposes
/// `... WHERE p AND c >= x [RETURNING ...]`.
fn guarded_update(sql: &str) -> Option<String> {
    let u = split_update(sql)?;
    let (col, delta) = assignments(&u.set_clause).into_iter().find_map(|(c, e)| {
        let (lhs, rhs) = e.split_once('-')?;
        (lhs.trim() == c).then(|| (c.clone(), rhs.trim().to_owned()))
    })?;
    Some(u.render_with_guard(&format!("{col} >= {delta}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgloom_engine::{DeadlockSource, RecordedStep};

    fn record(steps: &[(u32, &str)]) -> EpisodeRecord {
        EpisodeRecord {
            steps: steps
                .iter()
                .enumerate()
                .map(|(i, (w, sql))| {
                    let mut s = RecordedStep::new(i as u32 + 1, WorkerId(*w), StepAction::Execute);
                    s.sql = (*sql).into();
                    s
                })
                .collect(),
            ..Default::default()
        }
    }

    fn names() -> Vec<String> {
        vec!["req_a".into(), "req_b".into()]
    }

    #[test]
    fn check_then_act_proposes_lock_and_guarded_update() {
        let r = record(&[
            (0, "BEGIN"),
            (0, "SELECT balance FROM wallets WHERE id = $1"),
            (1, "SELECT balance FROM wallets WHERE id = $1"),
            (0, "UPDATE wallets SET balance = balance - $1 WHERE id = $2"),
            (1, "UPDATE wallets SET balance = balance - $1 WHERE id = $2"),
        ]);
        let advice = advise(&r, &[], &names());
        assert_eq!(advice.len(), 1);
        let a = &advice[0];
        assert_eq!(a.pattern, Pattern::CheckThenAct);
        assert!(a.explanation[0].starts_with("Workers req_a and req_b each read `wallets`"));
        assert_eq!(
            a.fixes[0].sql[0],
            "SELECT balance FROM wallets WHERE id = $1 FOR NO KEY UPDATE;"
        );
        assert_eq!(
            a.fixes[1].sql[0],
            "UPDATE wallets SET balance = balance - $1 WHERE id = $2 AND balance >= $1;"
        );
    }

    #[test]
    fn absolute_writes_are_lost_updates() {
        let r = record(&[
            (0, "SELECT balance FROM wallets WHERE id = 1"),
            (0, "UPDATE wallets SET balance = $1 WHERE id = 1"),
        ]);
        let a = &advise(&r, &[], &names())[0];
        assert_eq!(a.pattern, Pattern::LostUpdate);
        assert_eq!(
            a.fixes[1].sql[0],
            "UPDATE wallets SET balance = balance + <delta> WHERE id = 1;"
        );
    }

    #[test]
    fn locked_reads_are_not_flagged() {
        let r = record(&[
            (
                0,
                "SELECT balance FROM wallets WHERE id = 1 FOR NO KEY UPDATE",
            ),
            (0, "UPDATE wallets SET balance = balance - 1 WHERE id = 1"),
        ]);
        assert!(advise(&r, &[], &names()).is_empty());
    }

    #[test]
    fn count_then_insert_is_a_phantom() {
        let r = record(&[
            (0, "SELECT count(*) FROM bookings WHERE slot = 7"),
            (1, "SELECT count(*) FROM bookings WHERE slot = 7"),
            (0, "INSERT INTO bookings (slot) VALUES (7)"),
            (1, "INSERT INTO bookings (slot) VALUES (7)"),
        ]);
        let a = &advise(&r, &[], &names())[0];
        assert_eq!(a.pattern, Pattern::PhantomInsert);
        assert_eq!(
            a.fixes[0].sql[1],
            "INSERT INTO bookings (slot) VALUES (7) ON CONFLICT DO NOTHING;"
        );
    }

    #[test]
    fn orm_casts_are_absolute_writes() {
        assert!(is_absolute_assignment(
            "UPDATE products SET stock=$1::INTEGER WHERE products.id = $2::INTEGER"
        ));
        assert!(is_absolute_assignment(
            "UPDATE t SET a = CAST($1 AS numeric), b = 'x', c = true, d = $2::numeric(10, 2)"
        ));
        assert!(!is_absolute_assignment("UPDATE t SET a = 'x' || a"));
        assert!(!is_absolute_assignment("UPDATE t SET a = $1::int + a"));
    }

    #[test]
    fn lost_updates_propose_an_in_sql_change_and_compare_and_set() {
        let r = record(&[
            (0, "SELECT stock FROM products WHERE id = $1::INTEGER"),
            (
                0,
                "UPDATE products SET stock=$1::INTEGER WHERE products.id = $2::INTEGER",
            ),
        ]);
        let a = &advise(&r, &[], &names())[0];
        assert_eq!(a.pattern, Pattern::LostUpdate);
        assert_eq!(a.fixes[1].title, "Fix Option B (Atomic Update):");
        assert_eq!(
            a.fixes[1].sql[0],
            "UPDATE products SET stock = stock + <delta> WHERE products.id = $2::INTEGER;"
        );
        assert_eq!(a.fixes[2].title, "Fix Option C (Compare-and-Set):");
        assert_eq!(
            a.fixes[2].sql[0],
            "UPDATE products SET stock=$1::INTEGER WHERE products.id = $2::INTEGER AND stock = <value you read>;"
        );
    }

    #[test]
    fn aggregate_check_then_update_of_one_row_is_write_skew() {
        let r = record(&[
            (0, "SELECT count(*) FROM doctors WHERE on_call"),
            (1, "SELECT count(*) FROM doctors WHERE on_call"),
            (0, "UPDATE doctors SET on_call = false WHERE id = $1"),
            (1, "UPDATE doctors SET on_call = false WHERE id = $1"),
        ]);
        let a = &advise(&r, &[], &names())[0];
        assert_eq!(a.pattern, Pattern::WriteSkew);
        assert_eq!(
            a.fixes[0].sql[0],
            "SELECT 1 FROM doctors WHERE on_call FOR NO KEY UPDATE;"
        );
        assert!(a.fixes[1].sql[0].contains("SERIALIZABLE"));
        assert!(
            a.fixes
                .iter()
                .flat_map(|f| &f.sql)
                .all(|s| !s.contains("count(*) FROM doctors WHERE on_call FOR"))
        );
    }

    #[test]
    fn same_statement_on_different_rows_is_write_skew() {
        let mut r = record(&[
            (0, "SELECT id FROM doctors WHERE on_call"),
            (1, "SELECT id FROM doctors WHERE on_call"),
            (0, "UPDATE doctors SET on_call = false WHERE id = $1"),
            (1, "UPDATE doctors SET on_call = false WHERE id = $1"),
        ]);
        r.steps[2].params = vec!["1".into()];
        r.steps[3].params = vec!["2".into()];
        assert_eq!(advise(&r, &[], &names())[0].pattern, Pattern::WriteSkew);

        r.steps[3].params = vec!["1".into()];
        assert_eq!(advise(&r, &[], &names())[0].pattern, Pattern::LostUpdate);
    }

    #[test]
    fn aggregate_reads_are_never_given_a_row_lock_clause() {
        let r = record(&[
            (
                0,
                "SELECT sum(qty) FROM orders WHERE product_id = 1 GROUP BY product_id",
            ),
            (0, "INSERT INTO orders (product_id, qty) VALUES (1, 1)"),
        ]);
        for fix in &advise(&r, &[], &names())[0].fixes {
            for sql in &fix.sql {
                assert!(
                    !sql.contains(
                        "sum(qty) FROM orders WHERE product_id = 1 GROUP BY product_id FOR"
                    )
                );
            }
        }
        assert_eq!(
            lock_rows_read("SELECT count(*) FROM t WHERE a = 1 GROUP BY b ORDER BY 1"),
            Some("SELECT 1 FROM t WHERE a = 1 FOR NO KEY UPDATE;".into())
        );
        assert_eq!(
            lock_rows_read("SELECT a FROM t UNION SELECT a FROM u"),
            None
        );
        assert!(is_aggregate_read("SELECT DISTINCT a FROM t"));
        assert!(is_aggregate_read("SELECT COUNT (*) FROM t"));
        assert!(!is_aggregate_read(
            "SELECT a FROM t WHERE b IN (SELECT max(b) FROM u)"
        ));
    }

    #[test]
    fn limit_checks_lock_the_parent_row() {
        let r = record(&[
            (0, "BEGIN"),
            (0, "SELECT max_uses FROM coupons WHERE code = $1"),
            (0, "SELECT count(*) FROM redemptions WHERE code = $1"),
            (0, "INSERT INTO redemptions (code, user_id) VALUES ($1, $2)"),
        ]);
        let a = &advise(&r, &[], &names())[0];
        assert_eq!(a.pattern, Pattern::PhantomInsert);
        assert_eq!(
            a.fixes[0].title,
            "Fix Option A (Lock the parent row first):"
        );
        assert_eq!(
            a.fixes[0].sql[0],
            "SELECT max_uses FROM coupons WHERE code = $1 FOR NO KEY UPDATE;"
        );
        assert!(
            a.fixes[1]
                .title
                .starts_with("Fix Option B (Unique Constraint")
        );
        assert_eq!(a.fixes.len(), 2);
    }

    #[test]
    fn deadlocks_report_the_cycle() {
        let v = Violation::Deadlock {
            cycle: vec![WorkerId(0), WorkerId(1)],
            source: DeadlockSource::WaitForGraph,
            statements: vec![
                (
                    WorkerId(0),
                    "UPDATE accounts SET b = b - 1 WHERE id = 2".into(),
                ),
                (
                    WorkerId(1),
                    "UPDATE accounts SET b = b - 1 WHERE id = 1".into(),
                ),
            ],
        };
        let a = &advise(&EpisodeRecord::default(), &[v], &names())[0];
        assert_eq!(a.pattern, Pattern::LockOrderInversion);
        assert!(a.explanation[0].contains("req_a → req_b → req_a"));
        assert!(a.fixes[0].sql[0].contains("ORDER BY id FOR NO KEY UPDATE"));
    }

    #[test]
    fn returning_clauses_stay_at_the_end_of_proposed_sql() {
        let r = record(&[
            (0, "SELECT balance FROM wallets WHERE id = $1"),
            (
                0,
                "UPDATE wallets SET balance = balance - $1 WHERE id = $2 RETURNING balance",
            ),
        ]);
        let a = &advise(&r, &[], &names())[0];
        assert_eq!(
            a.fixes[1].sql[0],
            "UPDATE wallets SET balance = balance - $1 WHERE id = $2 AND balance >= $1 RETURNING balance;"
        );

        let lost = record(&[
            (0, "SELECT balance FROM wallets WHERE id = $1"),
            (
                0,
                "UPDATE wallets SET balance = $1 WHERE id = $2 RETURNING balance",
            ),
        ]);
        let a = &advise(&lost, &[], &names())[0];
        assert_eq!(a.pattern, Pattern::LostUpdate);
        assert_eq!(
            a.fixes[2].sql[0],
            "UPDATE wallets SET balance = $1 WHERE id = $2 AND balance = <value you read> RETURNING balance;"
        );

        let ins = record(&[
            (0, "SELECT count(*) FROM bookings WHERE slot = 7"),
            (0, "INSERT INTO bookings (slot) VALUES (7) RETURNING id"),
        ]);
        let a = &advise(&ins, &[], &names())[0];
        assert_eq!(
            a.fixes[0].sql[1],
            "INSERT INTO bookings (slot) VALUES (7) ON CONFLICT DO NOTHING RETURNING id;"
        );
    }

    #[test]
    fn set_clauses_split_only_on_top_level_commas() {
        assert_eq!(
            assignments("balance = GREATEST(balance - $1, 0), note = 'a, b', v = v + 1"),
            [
                ("balance".to_owned(), "GREATEST(balance - $1, 0)".to_owned()),
                ("note".to_owned(), "'a, b'".to_owned()),
                ("v".to_owned(), "v + 1".to_owned()),
            ]
        );
        assert!(!is_absolute_assignment(
            "UPDATE t SET a = $1, b = coalesce(b, 0) + 1 WHERE id = $2"
        ));
    }

    #[test]
    fn guards_without_a_where_clause_start_one() {
        assert_eq!(
            append_where_guard("DELETE FROM holds\nRETURNING id;", "expires_at < now()"),
            "DELETE FROM holds WHERE expires_at < now() RETURNING id;"
        );
        assert_eq!(
            append_where_guard("DELETE FROM holds WHERE id = $1", "expires_at < now()"),
            "DELETE FROM holds WHERE id = $1 AND expires_at < now();"
        );
    }
}
