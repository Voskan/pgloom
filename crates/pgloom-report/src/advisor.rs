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
                        format!(
                            "{} ON CONFLICT DO NOTHING;",
                            one_line(first.write).trim_end_matches(';')
                        ),
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
                        format!("{write} AND <the condition you checked in application code>;")
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

/// `SET` clause and the remainder of an `UPDATE`, split at the top-level
/// `WHERE` (case-insensitive, ignoring string literals is unnecessary here
/// because only the shape is inspected).
fn split_update(sql: &str) -> Option<(String, String, Option<String>)> {
    let flat = one_line(sql);
    let flat = flat.trim_end_matches(';');
    let upper = flat.to_ascii_uppercase();
    if !upper.starts_with("UPDATE ") {
        return None;
    }
    let set = upper.find(" SET ")?;
    let (head, rest) = (flat[..set].to_owned(), &flat[set + 5..]);
    let rest_upper = &upper[set + 5..];
    match rest_upper.find(" WHERE ") {
        Some(w) => Some((head, rest[..w].to_owned(), Some(rest[w + 7..].to_owned()))),
        None => Some((head, rest.to_owned(), None)),
    }
}

fn assignments(set: &str) -> Vec<(String, String)> {
    set.split(',')
        .filter_map(|a| a.split_once('='))
        .map(|(c, e)| (c.trim().to_owned(), e.trim().to_owned()))
        .collect()
}

/// Every assignment writes a literal or a bind parameter rather than an
/// expression over the current row.
fn is_absolute_assignment(sql: &str) -> bool {
    let Some((_, set, _)) = split_update(sql) else {
        return false;
    };
    let a = assignments(&set);
    !a.is_empty()
        && a.iter().all(|(_, e)| {
            e.strip_prefix('$')
                .is_some_and(|d| !d.is_empty() && d.chars().all(|c| c.is_ascii_digit()))
                || e.parse::<f64>().is_ok()
        })
}

/// For `SET c = c - x WHERE p`, proposes `... WHERE p AND c >= x`.
fn guarded_update(sql: &str) -> Option<String> {
    let (head, set, cond) = split_update(sql)?;
    let (col, delta) = assignments(&set).into_iter().find_map(|(c, e)| {
        let (lhs, rhs) = e.split_once('-')?;
        (lhs.trim() == c).then(|| (c.clone(), rhs.trim().to_owned()))
    })?;
    let cond = match cond {
        Some(c) => format!("{c} AND {col} >= {delta}"),
        None => format!("{col} >= {delta}"),
    };
    Some(format!("{head} SET {set} WHERE {cond};"))
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
}
