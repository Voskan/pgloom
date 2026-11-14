#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use pgloom_engine::{
    DeadlockSource, EpisodeRecord, RecordedStep, StepAction, Violation, WorkerId, WorkerOutcome,
};
use pgloom_report::advisor::advise;
use pgloom_report::timeline::{TimelineOptions, render_timeline};

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    steps: Vec<FuzzStep>,
    deadlock: Option<FuzzDeadlock>,
    names: Vec<String>,
    col_width: u8,
    max_lines: u8,
    color: bool,
}

#[derive(Arbitrary, Debug)]
struct FuzzDeadlock {
    cycle: Vec<u8>,
    statements: Vec<(u8, String)>,
}

#[derive(Arbitrary, Debug)]
struct FuzzStep {
    worker: u8,
    kind: u8,
    sql: String,
    params: Vec<String>,
    result: Option<String>,
    sqlstate: Option<String>,
    preempted: bool,
}

fuzz_target!(|input: FuzzInput| {
    let steps = input
        .steps
        .into_iter()
        .take(32)
        .enumerate()
        .map(|(i, s)| {
            let action = match s.kind % 5 {
                0 => StepAction::Execute,
                1 => StepAction::Begin,
                2 => StepAction::Resume,
                3 => StepAction::Park {
                    blocked_by: vec![WorkerId(u32::from(s.worker % 4))],
                },
                _ => StepAction::Done {
                    outcome: WorkerOutcome {
                        success: s.kind & 1 == 0,
                        label: s.sql.clone(),
                        detail: s.result.clone(),
                        ..WorkerOutcome::default()
                    },
                },
            };
            let mut step =
                RecordedStep::new(i as u32 + 1, WorkerId(u32::from(s.worker % 4)), action);
            step.sql = s.sql;
            step.params = s.params.into_iter().take(8).collect();
            step.result = s.result;
            step.sqlstate = s.sqlstate;
            step.preempted = s.preempted;
            step
        })
        .collect();

    let record = EpisodeRecord {
        steps,
        ..Default::default()
    };

    let mut violations = Vec::new();
    if let Some(dl) = input.deadlock {
        violations.push(Violation::Deadlock {
            cycle: dl
                .cycle
                .into_iter()
                .take(6)
                .map(|w| WorkerId(u32::from(w % 4)))
                .collect(),
            source: DeadlockSource::WaitForGraph,
            statements: dl
                .statements
                .into_iter()
                .take(6)
                .map(|(w, sql)| (WorkerId(u32::from(w % 4)), sql))
                .collect(),
        });
    }

    let names: Vec<String> = input.names.into_iter().take(4).collect();
    let _ = advise(&record, &violations, &names);
    let _ = render_timeline(
        &record,
        &names,
        TimelineOptions {
            column_width: usize::from(input.col_width).clamp(1, 60),
            max_cell_lines: usize::from(input.max_lines % 10),
            color: input.color,
        },
    );
});
