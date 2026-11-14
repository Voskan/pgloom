//! Property-based tests for `pgloom_report::advisor::advise` and timeline
//! rendering over arbitrary Unicode / fuzzed traces and wild ORM SQL.

use pgloom_engine::{
    DeadlockSource, EpisodeRecord, RecordedStep, StepAction, Violation, WorkerId, WorkerOutcome,
};
use pgloom_report::advisor::{Pattern, advise};
use pgloom_report::timeline::{TimelineOptions, render_timeline};
use proptest::prelude::*;

fn arb_step_action() -> impl Strategy<Value = StepAction> {
    prop_oneof![
        Just(StepAction::Execute),
        Just(StepAction::Begin),
        Just(StepAction::Resume),
        prop::collection::vec(0u32..4, 0..3).prop_map(|ws| StepAction::Park {
            blocked_by: ws.into_iter().map(WorkerId).collect(),
        }),
        (any::<bool>(), "\\PC{0,24}", prop::option::of("\\PC{0,32}")).prop_map(
            |(success, label, detail)| StepAction::Done {
                outcome: WorkerOutcome {
                    success,
                    label,
                    detail,
                    ..WorkerOutcome::default()
                },
            }
        ),
    ]
}

fn arb_sql_fragment() -> impl Strategy<Value = String> {
    prop_oneof![
        "\\PC{0,80}",
        // Specifically stress short multibyte UTF-8 strings around 5..12 bytes
        // (`"UPDATE "`, `"SELECT "`, `"cast("` boundaries).
        "[アップデート残高口座🔒⚡a-zA-Z0-9_ '\"$,;=()-]{0,24}",
        Just("SELECT balance FROM wallets WHERE id = $1".to_owned()),
        Just("SELECT count(*) FROM bookings WHERE slot = $1 GROUP BY slot".to_owned()),
        Just("UPDATE wallets SET balance = $1 WHERE id = $2 RETURNING balance".to_owned()),
        Just("UPDATE wallets SET balance = balance - $1 WHERE id = $2".to_owned()),
        Just("INSERT INTO bookings (slot) VALUES ($1) RETURNING id".to_owned()),
        Just("DELETE FROM holds WHERE id = $1 RETURNING id".to_owned()),
    ]
}

fn arb_recorded_step() -> impl Strategy<Value = RecordedStep> {
    (
        1u32..32,
        0u32..4,
        arb_step_action(),
        arb_sql_fragment(),
        prop::collection::vec("\\PC{0,16}", 0..4),
        prop::option::of("\\PC{0,24}"),
        prop::option::of("[0-9A-Z]{5}"),
        any::<bool>(),
    )
        .prop_map(
            |(step, worker, action, sql, params, result, sqlstate, preempted)| {
                let mut s = RecordedStep::new(step, WorkerId(worker), action);
                s.sql = sql;
                s.params = params;
                s.result = result;
                s.sqlstate = sqlstate;
                s.preempted = preempted;
                s
            },
        )
}

fn arb_violation() -> impl Strategy<Value = Violation> {
    prop_oneof![
        ("\\PC{0,20}", "\\PC{0,32}", "\\PC{0,24}").prop_map(|(name, description, observed)| {
            Violation::InvariantFailed {
                name,
                description,
                observed,
            }
        }),
        (
            prop::collection::vec(0u32..4, 0..4),
            prop::collection::vec((0u32..4, arb_sql_fragment()), 0..4),
        )
            .prop_map(|(cycle, statements)| Violation::Deadlock {
                cycle: cycle.into_iter().map(WorkerId).collect(),
                source: DeadlockSource::WaitForGraph,
                statements: statements
                    .into_iter()
                    .map(|(w, s)| (WorkerId(w), s))
                    .collect(),
            }),
    ]
}

fn arb_orm_trace() -> impl Strategy<Value = (EpisodeRecord, bool, bool)> {
    let prefix = prop_oneof![
        Just(String::new()),
        Just("/* prisma:tx */ ".to_owned()),
        Just("/* Hibernate */ -- read-write\n".to_owned()),
        Just("-- django orm\n".to_owned()),
    ];
    let table = prop_oneof![
        Just("wallets".to_owned()),
        Just("\"Wallet\"".to_owned()),
        Just("\"public\".\"Account,Ledger\"".to_owned()),
    ];
    let col = prop_oneof![
        Just("balance".to_owned()),
        Just("\"balance\"".to_owned()),
        Just("\"balance,usd\"".to_owned()),
        Just("\"a=b\"".to_owned()),
    ];
    let agg_read = any::<bool>();
    let has_returning = any::<bool>();
    let write_kind = 0u8..3; // 0 = absolute UPDATE, 1 = relative UPDATE, 2 = INSERT

    (prefix, table, col, agg_read, has_returning, write_kind).prop_map(
        |(p, tbl, c, agg, ret, wk)| {
            let read_sql = if agg {
                format!("{p}SELECT count(*) FROM {tbl} WHERE \"id\" = $1 GROUP BY \"id\"")
            } else {
                format!("{p}SELECT {c} FROM {tbl} WHERE \"id\" = $1")
            };
            let ret_clause = if ret { " RETURNING \"id\"" } else { "" };
            let write_sql = match wk {
                0 => format!(
                    "{p}UPDATE {tbl} SET {c} = CAST($1::numeric(12, 2) AS numeric), \"note\" = E'it\\'s, FROM x' WHERE \"id\" = $2{ret_clause}"
                ),
                1 => format!("{p}UPDATE {tbl} SET {c} = {c} - $1 WHERE \"id\" = $2{ret_clause}"),
                _ => format!("{p}INSERT INTO {tbl} ({c}) VALUES ($1){ret_clause}"),
            };

            let mut s1 = RecordedStep::new(1, WorkerId(0), StepAction::Execute);
            s1.sql = read_sql;
            s1.params = vec!["1".into()];
            let mut s2 = RecordedStep::new(2, WorkerId(0), StepAction::Execute);
            s2.sql = write_sql;
            s2.params = vec!["100".into(), "1".into()];

            (
                EpisodeRecord {
                    steps: vec![s1, s2],
                    ..Default::default()
                },
                agg,
                ret,
            )
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn advise_and_timeline_never_panic_on_arbitrary_records(
        steps in prop::collection::vec(arb_recorded_step(), 0..20),
        violations in prop::collection::vec(arb_violation(), 0..4),
        names in prop::collection::vec("\\PC{0,16}", 0..4),
        col_width in 1usize..48,
        max_lines in 0usize..8,
        color in any::<bool>(),
    ) {
        let record = EpisodeRecord {
            steps,
            ..Default::default()
        };
        let _ = advise(&record, &violations, &names);
        let _ = render_timeline(
            &record,
            &names,
            TimelineOptions {
                column_width: col_width,
                max_cell_lines: max_lines,
                color,
            },
        );
    }

    #[test]
    fn advise_preserves_sql_invariants_on_wild_orm_traces(
        (record, is_agg, has_returning) in arb_orm_trace()
    ) {
        let names = vec!["req_a".to_owned(), "req_b".to_owned()];
        let advice = advise(&record, &[], &names);
        prop_assert_eq!(advice.len(), 1);

        for fix in &advice[0].fixes {
            for line in &fix.sql {
                if !line.trim_start().starts_with("-- ") {
                    prop_assert!(
                        !line.trim_start().starts_with("--"),
                        "proposed SQL accidentally commented out: {line}"
                    );
                }
                if is_agg {
                    prop_assert!(
                        !line.contains("count(*)") || line.trim_start().starts_with("--"),
                        "aggregate projection must never get FOR NO KEY UPDATE: {line}"
                    );
                }
                if has_returning
                    && (line.trim_start().starts_with("UPDATE ")
                        || line.trim_start().starts_with("INSERT ")
                        || line.trim_start().starts_with("/*"))
                    && line.contains("RETURNING")
                {
                    prop_assert!(
                        line.ends_with("RETURNING \"id\";"),
                        "RETURNING clause must stay at the end of rewritten SQL: {line}"
                    );
                }
            }
        }

        if is_agg && advice[0].pattern != Pattern::PhantomInsert {
            prop_assert_eq!(advice[0].pattern, Pattern::WriteSkew);
        }
    }
}
