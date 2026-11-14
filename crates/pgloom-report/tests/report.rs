use std::path::PathBuf;
use std::time::Duration;

use pgloom_engine::{
    DeadlockSource, Decision, EpisodeRecord, ExplorationStats, Failure, RecordedStep, RunOutcome,
    RunReport, Schedule, StepAction, Violation, WorkerId, WorkerOutcome,
};
use pgloom_report::{AUDIT_FOOTER, ReportContext, ReproFile, render_json, render_run};
use pgloom_wire::TxStatus;

fn step(worker: u32, action: StepAction, sql: &str, result: Option<&str>) -> RecordedStep {
    let mut s = RecordedStep::new(0, WorkerId(worker), action);
    s.sql = sql.into();
    s.result = result.map(str::to_owned);
    s.tx_status = Some(TxStatus::InTransaction);
    s.backend_pid = Some(4812 + worker as i32);
    s
}

fn done(worker: u32, status: i32, label: &str) -> RecordedStep {
    RecordedStep::new(
        0,
        WorkerId(worker),
        StepAction::Done {
            outcome: WorkerOutcome {
                success: (200..300).contains(&status),
                status: Some(status),
                label: label.into(),
                detail: None,
                ..WorkerOutcome::default()
            },
        },
    )
}

fn number(mut steps: Vec<RecordedStep>) -> Vec<RecordedStep> {
    for (i, s) in steps.iter_mut().enumerate() {
        s.step = i as u32 + 1;
    }
    steps
}

fn schedule(choices: &[(u32, Option<u32>)]) -> Schedule {
    Schedule {
        decisions: choices
            .iter()
            .map(|(c, cur)| Decision {
                enabled: vec![WorkerId(0), WorkerId(1)],
                current: cur.map(WorkerId),
                chosen: WorkerId(*c),
            })
            .collect(),
    }
}

fn double_spend() -> Failure {
    let mut preempted = step(
        1,
        StepAction::Execute,
        "SELECT balance FROM wallets WHERE id = $1",
        Some("returns 100"),
    );
    preempted.params = vec!["1".into()];
    let mut first_read = step(
        0,
        StepAction::Execute,
        "SELECT balance FROM wallets WHERE id = $1",
        Some("returns 100"),
    );
    first_read.params = vec!["1".into()];
    let steps = number(vec![
        step(0, StepAction::Begin, "BEGIN", Some("BEGIN")),
        first_read,
        step(1, StepAction::Begin, "BEGIN", Some("BEGIN")),
        RecordedStep {
            preempted: true,
            ..preempted
        },
        step(
            0,
            StepAction::Execute,
            "UPDATE wallets SET balance = balance - 100 WHERE id = 1",
            Some("UPDATE 1"),
        ),
        step(0, StepAction::Execute, "COMMIT", Some("COMMIT")),
        done(0, 200, "HTTP 200 OK"),
        step(
            1,
            StepAction::Execute,
            "UPDATE wallets SET balance = balance - 100 WHERE id = 1",
            Some("UPDATE 1"),
        ),
        step(1, StepAction::Execute, "COMMIT", Some("COMMIT")),
        done(1, 200, "HTTP 200 OK"),
    ]);
    Failure {
        schedule: schedule(&[
            (0, None),
            (1, Some(0)),
            (0, None),
            (0, Some(0)),
            (1, None),
            (1, Some(1)),
        ]),
        record: EpisodeRecord {
            steps,
            outcomes: vec![None, None],
            backend_pids: vec![Some(4812), Some(4813)],
            ..Default::default()
        },
        violations: vec![
            Violation::InvariantFailed {
                name: "no_negative_balance".into(),
                description: String::new(),
                observed: "id=1, balance=-100".into(),
            },
            Violation::InvariantFailed {
                name: "at_most_one_successful_withdrawal".into(),
                description: "expected <= 1 HTTP 2xx, got 2 (req_a=200, req_b=200)".into(),
                observed: String::new(),
            },
        ],
        shrunk_from: Some((3, 5)),
        repeats: 0,
    }
}

fn deadlock() -> Failure {
    let mut b_lock = step(
        1,
        StepAction::Execute,
        "UPDATE accounts SET balance = balance - 10 WHERE id = 2",
        Some("UPDATE 1"),
    );
    b_lock.preempted = true;
    let steps = number(vec![
        step(0, StepAction::Begin, "BEGIN", Some("BEGIN")),
        step(
            0,
            StepAction::Execute,
            "UPDATE accounts SET balance = balance - 10 WHERE id = 1",
            Some("UPDATE 1"),
        ),
        step(1, StepAction::Begin, "BEGIN", Some("BEGIN")),
        b_lock,
        step(
            1,
            StepAction::Park {
                blocked_by: vec![WorkerId(0)],
            },
            "UPDATE accounts SET balance = balance + 10 WHERE id = 1",
            None,
        ),
        step(
            0,
            StepAction::Park {
                blocked_by: vec![WorkerId(1)],
            },
            "UPDATE accounts SET balance = balance + 10 WHERE id = 2",
            None,
        ),
    ]);
    let v = Violation::Deadlock {
        cycle: vec![WorkerId(0), WorkerId(1)],
        source: DeadlockSource::WaitForGraph,
        statements: vec![
            (
                WorkerId(0),
                "UPDATE accounts SET balance = balance + 10 WHERE id = 2".into(),
            ),
            (
                WorkerId(1),
                "UPDATE accounts SET balance = balance + 10 WHERE id = 1".into(),
            ),
        ],
    };
    Failure {
        schedule: schedule(&[(0, None), (1, Some(0)), (1, Some(1)), (0, None)]),
        record: EpisodeRecord {
            steps,
            outcomes: vec![None, None],
            backend_pids: vec![Some(4812), Some(4813)],
            deadlock: Some(v.clone()),
            ..Default::default()
        },
        violations: vec![v],
        shrunk_from: None,
        repeats: 0,
    }
}

fn ctx(name: &str) -> ReportContext {
    let mut c = ReportContext::new(name, vec!["req_a".into(), "req_b".into()]);
    c.max_schedules = 20;
    c.strategy = "bounded".into();
    c.repro_path = Some(".pgloom-repro.json".into());
    c.descriptions = vec![(
        "no_negative_balance".into(),
        "Wallet balance must never drop below zero".into(),
    )];
    c
}

fn report(failure: Failure) -> RunReport {
    RunReport {
        outcome: RunOutcome::ViolationFound,
        stats: ExplorationStats {
            schedules: 6,
            preemption_level: Some(1),
            complete: false,
            ..ExplorationStats::default()
        },
        elapsed: Duration::from_millis(142),
        parks: 2,
        failures: vec![failure],
        ..RunReport::default()
    }
}

fn golden(name: &str, actual: &str) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {}; run with UPDATE_GOLDEN=1", path.display()));
    assert_eq!(actual, expected, "golden mismatch for {name}:\n{actual}");
}

#[test]
fn double_spend_report_is_stable() {
    let text = render_run(
        &ctx("wallet-double-spend-withdraw"),
        &report(double_spend()),
    );
    assert!(!text.contains('\x1b'));
    golden("double_spend.txt", &text);
}

#[test]
fn deadlock_report_is_stable() {
    let text = render_run(&ctx("transfer-deadlock"), &report(deadlock()));
    golden("deadlock.txt", &text);
}

#[test]
fn timeline_rows_have_uniform_display_width() {
    for f in [double_spend(), deadlock()] {
        let text = render_run(&ctx("x"), &report(f));
        let widths: Vec<usize> = text
            .lines()
            .filter(|l| l.trim_start().starts_with(['│', '┌', '├', '└']))
            .map(unicode_width::UnicodeWidthStr::width)
            .collect();
        assert!(widths.len() > 5);
        assert!(widths.iter().all(|w| *w == widths[0]), "{widths:?}\n{text}");
    }
}

#[test]
fn color_mode_emits_ansi() {
    let mut c = ctx("x");
    c.color = true;
    let text = render_run(&c, &report(double_spend()));
    assert!(text.contains("\x1b[31m✗ RACE CONDITION DETECTED\x1b[0m"));
}

#[test]
fn pass_report() {
    let r = RunReport {
        outcome: RunOutcome::Pass,
        stats: ExplorationStats {
            schedules: 14,
            preemption_level: Some(2),
            complete: true,
            ..ExplorationStats::default()
        },
        elapsed: Duration::from_millis(84),
        parks: 3,
        failures: vec![],
        ..RunReport::default()
    };
    assert_eq!(
        render_run(&ctx("wallet"), &r),
        "✓ NO ANOMALIES FOUND  [wallet]\n  Explored 14 schedules in 84ms (every interleaving covered)\n  Lock waits parked and resolved: 3\n"
    );
}

#[test]
fn reduced_pass_report_says_how_coverage_was_reached() {
    let r = RunReport {
        outcome: RunOutcome::Pass,
        stats: ExplorationStats {
            schedules: 6,
            complete: true,
            reduced: true,
            ..ExplorationStats::default()
        },
        elapsed: Duration::from_millis(29),
        parks: 0,
        failures: vec![],
        ..RunReport::default()
    };
    assert_eq!(
        render_run(&ctx("wallet"), &r),
        "✓ NO ANOMALIES FOUND  [wallet]\n  Explored 6 schedules in 29ms (every interleaving covered; equivalent orders of independent steps run once)\n"
    );
}

#[test]
fn footer_closes_violation_reports_only() {
    let mut c = ctx("wallet");
    c.footer = Some(AUDIT_FOOTER.to_owned());
    let text = render_run(&c, &report(double_spend()));
    assert!(text.ends_with(&format!("\n\n  {AUDIT_FOOTER}\n")), "{text}");
    assert_eq!(text.matches(AUDIT_FOOTER).count(), 1);

    let pass = RunReport {
        outcome: RunOutcome::Pass,
        stats: ExplorationStats::default(),
        elapsed: Duration::from_millis(1),
        parks: 0,
        failures: vec![],
        ..RunReport::default()
    };
    assert!(!render_run(&c, &pass).contains(AUDIT_FOOTER));
    assert!(!render_json(&c, &report(double_spend())).contains("voskan1989@gmail.com"));
}

#[test]
fn repeated_failures_are_counted_once() {
    let mut failure = double_spend();
    failure.repeats = 3;
    let text = render_run(&ctx("wallet"), &report(failure.clone()));
    assert_eq!(text.matches("RACE CONDITION DETECTED").count(), 1);
    assert!(
        text.contains("\n  The same invariants failed in 3 more schedules, not shown.\n"),
        "{text}"
    );
    let v: serde_json::Value =
        serde_json::from_str(&render_json(&ctx("wallet"), &report(failure))).unwrap();
    assert_eq!(v["failures"][0]["repeated_in_schedules"], 3);
    let plain = render_run(&ctx("wallet"), &report(double_spend()));
    assert!(!plain.contains("same invariants"));
}

#[test]
fn json_report_is_structured() {
    let text = render_json(&ctx("wallet"), &report(double_spend()));
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["result"], "violation");
    assert_eq!(v["reduction"], "none");
    assert_eq!(v["schedules_pruned"], 0);
    assert_eq!(v["failures"][0]["preemptions"], 1);
    assert_eq!(v["failures"][0]["shrunk_from_preemptions"], 3);
    assert_eq!(v["failures"][0]["advice"][0]["pattern"], "check_then_act");
    assert_eq!(v["failures"][0]["steps"][1]["worker"], "req_a");
    assert_eq!(
        v["failures"][0]["violations"][0]["kind"],
        "invariant_failed"
    );
}

#[test]
fn repro_file_round_trips() {
    let failure = double_spend();
    let workers = vec!["req_a".to_string(), "req_b".to_string()];
    let source = "version: 1\nname: wallet\n";
    let repro = ReproFile::from_failure("wallet", "pgloom.yaml", source, &workers, &failure);
    assert_eq!(repro.choice_sequence, vec![0, 1, 0, 0, 1, 1]);
    assert_eq!(repro.preemptions, 1);
    assert_eq!(repro.violation_kind, "InvariantFailed");
    assert_eq!(
        repro.violated_invariants,
        vec!["no_negative_balance", "at_most_one_successful_withdrawal"]
    );
    assert!(repro.matches_config(source));
    assert!(!repro.matches_config("version: 1\nname: other\n"));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".pgloom-repro.json");
    repro.save(&path).unwrap();
    let loaded = ReproFile::load(&path).unwrap();
    assert_eq!(loaded, repro);
    assert_eq!(loaded.choices(), failure.schedule.choices());

    let record = loaded.to_record("x").unwrap();
    assert_eq!(record.steps, failure.record.steps);
    assert_eq!(record.backend_pids, failure.record.backend_pids);
    assert_eq!(record.success_count(), 2);

    let json: serde_json::Value = serde_json::from_str(&repro.to_json()).unwrap();
    assert_eq!(json["recorded_steps"][1]["worker"], "req_a");
    assert_eq!(json["recorded_steps"][1]["params"][0], "1");
    assert_eq!(json["recorded_steps"][1]["tx_status"], "T");
    assert_eq!(json["recorded_steps"][1]["type"], "execute");

    let deadlock = ReproFile::from_failure("t", "p", source, &workers, &deadlock());
    assert_eq!(deadlock.violation_kind, "Deadlock");
}

#[test]
fn repro_rejects_other_versions_and_garbage() {
    let failure = double_spend();
    let mut repro =
        ReproFile::from_failure("w", "p", "", &["req_a".into(), "req_b".into()], &failure);
    repro.format_version = 99;
    let err = ReproFile::parse(&repro.to_json(), "f.json").unwrap_err();
    assert!(err.to_string().contains("format 99"), "{err}");
    assert!(ReproFile::parse("{", "f.json").is_err());
}

#[test]
fn interrupted_runs_say_exploration_is_incomplete() {
    let pass = RunReport {
        outcome: RunOutcome::Pass,
        stats: ExplorationStats {
            schedules: 9,
            ..ExplorationStats::default()
        },
        elapsed: Duration::from_millis(50),
        interrupted: true,
        ..RunReport::default()
    };
    let text = render_run(&ctx("wallet"), &pass);
    assert!(text.starts_with("■ INTERRUPTED  [wallet]\n"), "{text}");
    assert!(text.contains("exploration incomplete"), "{text}");
    assert!(!text.contains("NO ANOMALIES FOUND"), "{text}");
    let json: serde_json::Value =
        serde_json::from_str(&render_json(&ctx("wallet"), &pass)).unwrap();
    assert_eq!(json["result"], "interrupted");
    assert_eq!(json["interrupted"], true);

    let mut failed = report(double_spend());
    failed.interrupted = true;
    let text = render_run(&ctx("wallet"), &failed);
    assert!(
        text.contains("other interleavings were not explored"),
        "{text}"
    );
}

#[test]
fn quiet_reports_keep_the_verdict_and_drop_the_timeline() {
    let mut c = ctx("wallet");
    c.compact = true;
    let text = render_run(&c, &report(double_spend()));
    assert!(text.contains("RACE CONDITION DETECTED"), "{text}");
    assert!(text.contains("Violated Invariants:"), "{text}");
    assert!(text.contains("pgloom replay"), "{text}");
    assert!(!text.contains("Timeline"), "{text}");
    assert!(!text.contains("Fix Advisor"), "{text}");
}
