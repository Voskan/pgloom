//! Human-readable run reports.

use std::fmt::Write as _;
use std::time::Duration;

use pgloom_engine::{EpisodeRecord, Failure, RunOutcome, RunReport, Schedule, Violation};

use crate::advisor::{Advice, advise};
use crate::style::{Style, paint};
use crate::timeline::{TimelineOptions, render_timeline};

/// Closing line of a report that found a violation (see `--quiet`).
pub const AUDIT_FOOTER: &str =
    "Concurrency review for ledgers and payment systems: voskan1989@gmail.com";

/// Context shared by the report renderers.
#[derive(Debug, Clone)]
pub struct ReportContext {
    pub scenario: String,
    pub workers: Vec<String>,
    /// SQL invariant descriptions by name, shown next to violations.
    pub descriptions: Vec<(String, String)>,
    pub max_schedules: usize,
    pub strategy: String,
    pub repro_path: Option<String>,
    pub color: bool,
    pub timeline: TimelineOptions,
    /// Printed once below a report that found a violation.
    pub footer: Option<String>,
    /// `--quiet`: verdict, violated invariants and the replay command only,
    /// without the timeline and the Fix Advisor.
    pub compact: bool,
}

impl ReportContext {
    pub fn new(scenario: impl Into<String>, workers: Vec<String>) -> Self {
        Self {
            scenario: scenario.into(),
            workers,
            descriptions: Vec::new(),
            max_schedules: 0,
            strategy: String::new(),
            repro_path: None,
            color: false,
            timeline: TimelineOptions::default(),
            footer: None,
            compact: false,
        }
    }

    fn paint(&self, text: &str, style: Style) -> String {
        paint(text, style, self.color)
    }

    fn description_of(&self, name: &str) -> Option<&str> {
        self.descriptions
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, d)| d.as_str())
    }
}

pub fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1000.0 {
        format!("{}ms", ms.round() as u64)
    } else {
        format!("{:.2}s", ms / 1000.0)
    }
}

fn plural(n: impl Into<u64>, one: &str, many: &str) -> String {
    let n = n.into();
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Full report of a `pgloom run`.
pub fn render_run(ctx: &ReportContext, report: &RunReport) -> String {
    let mut out = String::new();
    match report.outcome {
        RunOutcome::Pass if report.interrupted => {
            let _ = writeln!(
                out,
                "{}  [{}]",
                ctx.paint("■ INTERRUPTED", Style::Bold),
                ctx.scenario
            );
            let _ = writeln!(
                out,
                "  Explored {} in {} before the interruption; no anomalies so far, exploration incomplete.",
                plural(report.stats.schedules as u64, "schedule", "schedules"),
                format_duration(report.elapsed),
            );
        }
        RunOutcome::Pass => {
            let _ = writeln!(
                out,
                "{}  [{}]",
                ctx.paint("✓ NO ANOMALIES FOUND", Style::Ok),
                ctx.scenario
            );
            let coverage = if report.stats.complete && report.stats.reduced {
                "every interleaving covered; equivalent orders of independent steps run once"
                    .to_owned()
            } else if report.stats.complete {
                "every interleaving covered".to_owned()
            } else {
                match report.stats.preemption_level {
                    Some(p) if p > 0 => format!(
                        "all schedules with up to {} explored; schedule budget of {} reached",
                        plural(p - 1, "preemption", "preemptions"),
                        ctx.max_schedules
                    ),
                    _ => format!("schedule budget of {} reached", ctx.max_schedules),
                }
            };
            let _ = writeln!(
                out,
                "  Explored {} in {} ({coverage})",
                plural(report.stats.schedules as u64, "schedule", "schedules"),
                format_duration(report.elapsed),
            );
            if report.parks > 0 {
                let _ = writeln!(out, "  Lock waits parked and resolved: {}", report.parks);
            }
        }
        RunOutcome::ViolationFound => {
            for (i, failure) in report.failures.iter().enumerate() {
                if i > 0 {
                    out.push('\n');
                }
                let explored = format!(
                    "  Explored {}/{} schedules in {}",
                    report.stats.schedules,
                    ctx.max_schedules,
                    format_duration(report.elapsed)
                );
                out.push_str(&render_failure(ctx, failure, &explored));
            }
            if report.interrupted {
                let _ = writeln!(
                    out,
                    "\n  Interrupted after {}; other interleavings were not explored.",
                    plural(report.stats.schedules as u64, "schedule", "schedules")
                );
            }
            if let Some(footer) = &ctx.footer {
                let _ = writeln!(out, "\n  {}", ctx.paint(footer, Style::Dim));
            }
        }
    }
    out
}

fn trace_note(schedule: &Schedule, shrunk_from: Option<(u32, u32)>) -> String {
    let p = schedule.preemptions();
    match shrunk_from {
        Some((from, _)) if from > p => {
            format!("shrunk from {from} to a minimal {p}-preemption trace")
        }
        Some(_) => format!("minimal {p}-preemption trace"),
        None => format!("{p}-preemption trace"),
    }
}

/// Headline, violations, timeline, Fix Advisor and the replay hint.
pub fn render_failure(ctx: &ReportContext, failure: &Failure, explored: &str) -> String {
    let mut out = String::new();
    let deadlock_only =
        !failure.violations.is_empty() && failure.violations.iter().all(Violation::is_deadlock);
    let headline = if deadlock_only {
        "✗ DEADLOCK DETECTED"
    } else {
        "✗ RACE CONDITION DETECTED"
    };
    let _ = writeln!(
        out,
        "{}  [{}]",
        ctx.paint(headline, Style::Error),
        ctx.scenario
    );
    let _ = writeln!(
        out,
        "{explored} ({})",
        trace_note(&failure.schedule, failure.shrunk_from)
    );
    if failure.repeats > 0 {
        let _ = writeln!(
            out,
            "  The same invariants failed in {} more {}, not shown.",
            failure.repeats,
            if failure.repeats == 1 {
                "schedule"
            } else {
                "schedules"
            }
        );
    }
    out.push('\n');
    out.push_str(&render_episode(ctx, &failure.record, &failure.violations));
    if let Some(path) = &ctx.repro_path {
        let _ = writeln!(out, "\n  Replay deterministically:");
        let _ = writeln!(out, "    $ pgloom replay {path}");
    }
    out
}

/// Violations, timeline and advice for one recorded episode.
pub fn render_episode(
    ctx: &ReportContext,
    record: &EpisodeRecord,
    violations: &[Violation],
) -> String {
    let mut out = String::new();
    if !violations.is_empty() {
        let _ = writeln!(out, "  {}", ctx.paint("Violated Invariants:", Style::Bold));
        for v in violations {
            out.push_str(&render_violation(ctx, v));
        }
        out.push('\n');
    }
    if ctx.compact {
        let mut out = out.trim_end().to_owned();
        out.push('\n');
        return out;
    }
    let _ = writeln!(
        out,
        "  {}",
        ctx.paint("Deterministic Execution Timeline:", Style::Bold)
    );
    let opts = TimelineOptions {
        color: ctx.color,
        ..ctx.timeline
    };
    for line in render_timeline(record, &ctx.workers, opts).lines() {
        let _ = writeln!(out, "  {line}");
    }
    let advice = advise(record, violations, &ctx.workers);
    if !advice.is_empty() {
        let _ = writeln!(
            out,
            "\n  {}",
            ctx.paint(
                "Root Cause & Recommended Remediation (Fix Advisor):",
                Style::Bold
            )
        );
        for a in &advice {
            out.push_str(&render_advice(a));
        }
    }
    out
}

fn worker_name(ctx: &ReportContext, w: pgloom_engine::WorkerId) -> String {
    ctx.workers
        .get(w.index())
        .cloned()
        .unwrap_or_else(|| w.to_string())
}

fn render_violation(ctx: &ReportContext, v: &Violation) -> String {
    let mut out = String::new();
    match v {
        Violation::InvariantFailed {
            name,
            description,
            observed,
        } => {
            let desc = if description.is_empty() {
                ctx.description_of(name).unwrap_or("")
            } else {
                description
            };
            let bullet = ctx.paint(&format!("• {name}"), Style::Error);
            if desc.is_empty() {
                let _ = writeln!(out, "    {bullet}");
            } else {
                let _ = writeln!(out, "    {bullet}: {desc}");
            }
            if !observed.is_empty() {
                let _ = writeln!(out, "      Observed DB state: {observed}");
            }
        }
        Violation::Deadlock {
            cycle,
            source,
            statements,
        } => {
            let mut path: Vec<String> = cycle.iter().map(|w| worker_name(ctx, *w)).collect();
            if let Some(f) = path.first().cloned() {
                path.push(f);
            }
            let by = match source {
                pgloom_engine::DeadlockSource::WaitForGraph => {
                    "found in the wait-for graph before PostgreSQL's deadlock_timeout"
                }
                pgloom_engine::DeadlockSource::Postgres => {
                    "PostgreSQL aborted a transaction with 40P01"
                }
            };
            let _ = writeln!(
                out,
                "    {}: {} ({by})",
                ctx.paint("• deadlock", Style::Error),
                path.join(" → ")
            );
            for (w, sql) in statements {
                let _ = writeln!(
                    out,
                    "      {} waits in: {}",
                    worker_name(ctx, *w),
                    crate::style::one_line(sql)
                );
            }
        }
    }
    out
}

fn render_advice(a: &Advice) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "    Pattern: {}", a.pattern.title());
    for para in &a.explanation {
        for line in crate::style::wrap(para, 76) {
            let _ = writeln!(out, "    {line}");
        }
    }
    for fix in &a.fixes {
        let _ = writeln!(out, "    {}", fix.title);
        for sql in &fix.sql {
            let _ = writeln!(out, "      {sql}");
        }
    }
    out
}
