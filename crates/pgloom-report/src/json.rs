//! Machine-readable run reports (`--format json`).

use pgloom_engine::{RunOutcome, RunReport, Violation};
use serde::Serialize;

use crate::advisor::{Advice, advise};
use crate::repro::ReproStep;
use crate::text::ReportContext;

#[derive(Debug, Serialize)]
pub struct JsonReport {
    pub scenario: String,
    /// `pass`, `violation`, or `interrupted` (stopped by Ctrl+C before any
    /// violation was found).
    pub result: &'static str,
    /// Ctrl+C stopped the exploration early.
    pub interrupted: bool,
    pub strategy: String,
    pub schedules_explored: usize,
    /// Schedules abandoned by partial-order reduction as redundant.
    pub schedules_pruned: usize,
    /// `dpor` or `none`.
    pub reduction: &'static str,
    pub max_schedules: usize,
    pub complete: bool,
    pub elapsed_ms: u64,
    pub lock_parks: usize,
    pub workers: Vec<String>,
    pub failures: Vec<JsonFailure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repro_path: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JsonFailure {
    pub violations: Vec<Violation>,
    pub choice_sequence: Vec<u32>,
    pub preemptions: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shrunk_from_preemptions: Option<u32>,
    /// Other failing schedules with exactly the same violated invariants.
    pub repeated_in_schedules: usize,
    pub steps: Vec<ReproStep>,
    pub advice: Vec<Advice>,
}

pub fn json_report(ctx: &ReportContext, report: &RunReport) -> JsonReport {
    JsonReport {
        scenario: ctx.scenario.clone(),
        result: match report.outcome {
            RunOutcome::Pass if report.interrupted => "interrupted",
            RunOutcome::Pass => "pass",
            RunOutcome::ViolationFound => "violation",
        },
        interrupted: report.interrupted,
        strategy: ctx.strategy.clone(),
        schedules_explored: report.stats.schedules,
        schedules_pruned: report.stats.pruned,
        reduction: if report.stats.reduced { "dpor" } else { "none" },
        max_schedules: ctx.max_schedules,
        complete: report.stats.complete,
        elapsed_ms: u64::try_from(report.elapsed.as_millis()).unwrap_or(u64::MAX),
        lock_parks: report.parks,
        workers: ctx.workers.clone(),
        failures: report
            .failures
            .iter()
            .map(|f| JsonFailure {
                violations: f.violations.clone(),
                choice_sequence: f.schedule.choices().iter().map(|w| w.0).collect(),
                preemptions: f.schedule.preemptions(),
                shrunk_from_preemptions: f.shrunk_from.map(|(p, _)| p),
                repeated_in_schedules: f.repeats,
                steps: f
                    .record
                    .steps
                    .iter()
                    .map(|s| ReproStep::from_recorded(s, &ctx.workers))
                    .collect(),
                advice: advise(&f.record, &f.violations, &ctx.workers),
            })
            .collect(),
        repro_path: ctx.repro_path.clone(),
    }
}

pub fn render_json(ctx: &ReportContext, report: &RunReport) -> String {
    serde_json::to_string_pretty(&json_report(ctx, report)).unwrap_or_default()
}
