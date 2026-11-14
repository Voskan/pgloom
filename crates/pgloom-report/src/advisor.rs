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
}

impl Pattern {
    pub fn title(self) -> &'static str {
        match self {
            Pattern::LockOrderInversion => "Lock-Order Inversion (deadlock)",
            Pattern::PhantomInsert => "Phantom Insert / Missing Unique Constraint",
            Pattern::CheckThenAct => "Unprotected Check-Then-Act under READ COMMITTED",
            Pattern::LostUpdate => "Lost Update (value computed in application memory)",
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

/// Statements a worker executed, in order, with their classification.
struct Trace<'a> {
    sql: Vec<&'a str>,
}

fn traces(record: &EpisodeRecord) -> BTreeMap<WorkerId, Trace<'_>> {
    let mut out: BTreeMap<WorkerId, Trace<'_>> = BTreeMap::new();
    for step in &record.steps {
        if matches!(
            step.action,
            StepAction::Execute | StepAction::Begin | StepAction::Park { .. }
        ) && !step.sql.is_empty()
        {
            out.entry(step.worker)
                .or_insert_with(|| Trace { sql: Vec::new() })
                .sql
                .push(&step.sql);
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
}

fn read_then_write<'a>(trace: &Trace<'a>) -> Option<ReadThenWrite<'a>> {
    for (i, read) in trace.sql.iter().enumerate() {
        let meta = classify_sql(read);
        if meta.has_write() || meta.row_lock().is_some() {
            continue;
        }
        for table in meta.read_tables() {
            if let Some(write) = trace.sql[i + 1..]
                .iter()
                .find(|w| classify_sql(w).write_tables().contains(&table))
            {
                return Some(ReadThenWrite { table, read, write });
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
        out.push(Advice {
            pattern: Pattern::PhantomInsert,
            explanation: vec![
                format!("{subject} checked `{table}` for an existing row and then inserted one."),
                "Under READ COMMITTED neither transaction sees the other's uncommitted row, and \
                 SELECT ... FOR UPDATE cannot lock a row that does not exist yet."
                    .into(),
            ],
            fixes: vec![
                Fix {
                    title: "Fix Option A (Unique Constraint):".into(),
                    sql: vec![
                        format!(
                            "CREATE UNIQUE INDEX CONCURRENTLY ON {table} (<natural key columns>);"
                        ),
                        on_conflict,
                        "-- and treat rows_affected == 0 as \"already exists\".".into(),
                    ],
                },
                Fix {
                    title: "Fix Option B (Serialize on the key):".into(),
                    sql: vec![
                        "SELECT pg_advisory_xact_lock(hashtext(<natural key>));".into(),
                        format!("-- then run: {read}"),
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
    let guarded = guarded_update(first.write);
    out.push(Advice {
        pattern: if lost {
            Pattern::LostUpdate
        } else {
            Pattern::CheckThenAct
        },
        explanation,
        fixes: vec![
            Fix {
                title: "Fix Option A (Pessimistic Lock):".into(),
                sql: vec![
                    format!("{read} FOR NO KEY UPDATE;"),
                    "-- FOR NO KEY UPDATE blocks concurrent writers of the row but, unlike \
                     FOR UPDATE, not inserts of rows referencing it by foreign key."
                        .into(),
                ],
            },
            Fix {
                title: "Fix Option B (Atomic Guarded Update):".into(),
                sql: vec![
                    guarded.unwrap_or_else(|| {
                        append_where_guard(
                            &write,
                            "<the condition you checked in application code>",
                        )
                    }),
                    "-- and verify rows_affected == 1 before reporting success.".into(),
                ],
            },
        ],
    });
    out
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
        let from = self
            .from_clause
            .as_deref()
            .map_or_else(String::new, |f| format!(" {f}"));
        let where_body = match &self.cond {
            Some(c) => format!("{c} AND {guard}"),
            None => guard.to_owned(),
        };
        let ret = self
            .returning
            .as_deref()
            .map_or_else(String::new, |r| format!(" {r}"));
        format!(
            "{} SET {}{from} WHERE {where_body}{ret};",
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
    !a.is_empty()
        && a.iter().all(|(_, e)| {
            e.strip_prefix('$')
                .is_some_and(|d| !d.is_empty() && d.chars().all(|c| c.is_ascii_digit()))
                || e.parse::<f64>().is_ok()
        })
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
        assert!(a.fixes[1].sql[0].contains("<the condition you checked"));
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
            a.fixes[1].sql[0],
            "UPDATE wallets SET balance = $1 WHERE id = $2 AND <the condition you checked in application code> RETURNING balance;"
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
