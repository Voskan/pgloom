//! The deterministic execution timeline: one column per worker, one row per
//! scheduled step.

use pgloom_engine::{EpisodeRecord, RecordedStep, StepAction, WorkerId};

use crate::style::{Style, center, fit, one_line, paint, width, wrap};

const STEP_COL: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimelineOptions {
    /// Display width of a worker cell, excluding padding.
    pub column_width: usize,
    /// Cells longer than this are cut with `…`.
    pub max_cell_lines: usize,
    pub color: bool,
}

impl Default for TimelineOptions {
    fn default() -> Self {
        Self {
            column_width: 30,
            max_cell_lines: 6,
            color: false,
        }
    }
}

#[derive(Debug)]
struct Cell {
    lines: Vec<String>,
    style: Style,
}

#[derive(Debug)]
enum Row {
    Step {
        label: String,
        cells: Vec<Option<Cell>>,
    },
    Switch {
        from: usize,
        to: usize,
    },
}

/// Renders the record as a box-drawn table. `names[i]` labels worker `i`.
pub fn render_timeline(record: &EpisodeRecord, names: &[String], opts: TimelineOptions) -> String {
    let n = names.len().max(record.outcomes.len()).max(
        record
            .steps
            .iter()
            .map(|s| s.worker.index() + 1)
            .max()
            .unwrap_or(0),
    );
    let w = opts.column_width.max(12);
    let name_of = |id: WorkerId| {
        names
            .get(id.index())
            .cloned()
            .unwrap_or_else(|| id.to_string())
    };
    let rows = build_rows(record, n, w, opts.max_cell_lines.max(1), &name_of);

    let color = opts.color;
    let rule = |l: &str, m: &str, r: &str| {
        let mut s = format!("{l}{}", "─".repeat(STEP_COL));
        for _ in 0..n {
            s.push_str(m);
            s.push_str(&"─".repeat(w + 2));
        }
        s.push_str(r);
        s
    };
    let mut out = Vec::new();
    out.push(rule("┌", "┬", "┐"));
    let mut header = format!("│{}", paint(&center("Step", STEP_COL), Style::Bold, color));
    for i in 0..n {
        let id = WorkerId(i as u32);
        let title = match record.backend_pids.get(i).copied().flatten() {
            Some(pid) => format!("Worker {} (PG pid {pid})", name_of(id)),
            None => format!("Worker {}", name_of(id)),
        };
        header.push_str(&format!(
            "│ {} ",
            paint(&fit(&title, w), Style::Bold, color)
        ));
    }
    header.push('│');
    out.push(header);
    out.push(rule("├", "┼", "┤"));

    for row in &rows {
        match row {
            Row::Step { label, cells } => {
                let height = cells
                    .iter()
                    .flatten()
                    .map(|c| c.lines.len())
                    .max()
                    .unwrap_or(1);
                for line in 0..height {
                    let lab = if line == 0 { label.as_str() } else { "" };
                    let mut s = format!("│{}", center(lab, STEP_COL));
                    for cell in cells {
                        let (text, style) = match cell {
                            Some(c) => (c.lines.get(line).map_or("", String::as_str), c.style),
                            None => ("", Style::Plain),
                        };
                        s.push_str(&format!("│ {} ", paint(&fit(text, w), style, color)));
                    }
                    s.push('│');
                    out.push(s);
                }
            }
            Row::Switch { from, to } => {
                let (lo, hi) = ((*from).min(*to), (*from).max(*to));
                let mut s = format!("│{}", paint(&center("⚡", STEP_COL), Style::Switch, color));
                for _ in 0..lo {
                    s.push_str(&format!("│ {} ", " ".repeat(w)));
                }
                let span = (hi - lo + 1) * (w + 2) + (hi - lo);
                let text = " context switch (preempt) ";
                let fill = span.saturating_sub(width(text) + 1);
                let left = fill / 2;
                let right = fill - left;
                let arrow = if to > from {
                    format!("{}{text}{}►", "─".repeat(left), "─".repeat(right))
                } else {
                    format!("◄{}{text}{}", "─".repeat(left), "─".repeat(right))
                };
                s.push('│');
                s.push_str(&paint(&arrow, Style::Switch, color));
                for _ in hi + 1..n {
                    s.push_str(&format!("│ {} ", " ".repeat(w)));
                }
                s.push('│');
                out.push(s);
            }
        }
    }
    out.push(rule("└", "┴", "┘"));
    out.join("\n")
}

fn build_rows(
    record: &EpisodeRecord,
    n: usize,
    w: usize,
    max_lines: usize,
    name_of: &dyn Fn(WorkerId) -> String,
) -> Vec<Row> {
    let mut rows = Vec::new();
    let mut last: Option<usize> = None;
    // Rows of `BEGIN`s that ran unscheduled right before the worker's next
    // scheduled turn; a context switch into that worker is drawn above them.
    let mut begin_run: Option<(usize, usize)> = None;
    let mut t = 0;
    for (i, step) in record.steps.iter().enumerate() {
        let wi = step.worker.index();
        if wi >= n {
            continue;
        }
        let mut cells: Vec<Option<Cell>> = (0..n).map(|_| None).collect();
        let (mut lines, style) = describe(step, w, name_of);
        if lines.len() > max_lines {
            lines.truncate(max_lines);
            if let Some(l) = lines.last_mut() {
                *l = fit(&format!("{l} …"), w).trim_end().to_owned();
            }
        }
        cells[wi] = Some(Cell { lines, style });
        t += 1;
        let row = Row::Step {
            label: format!("t{t}"),
            cells,
        };

        if step.preempted
            && let Some(prev) = last
            && prev != wi
        {
            let at = match begin_run {
                Some((bw, start)) if bw == wi => start,
                _ => rows.len(),
            };
            rows.insert(at, Row::Switch { from: prev, to: wi });
            rows.push(row);
            if let Some(Row::Step { cells, .. }) = rows.get_mut(at + 1) {
                cells[prev] = Some(paused_cell(record, i, prev));
            }
        } else {
            rows.push(row);
        }

        match step.action {
            StepAction::Begin => {
                if begin_run.is_none_or(|(bw, _)| bw != wi) {
                    begin_run = Some((wi, rows.len() - 1));
                }
            }
            StepAction::Done { .. } => begin_run = None,
            _ => {
                begin_run = None;
                last = Some(wi);
            }
        }
    }
    rows
}

fn paused_cell(record: &EpisodeRecord, from: usize, worker: usize) -> Cell {
    let next = record.steps[from + 1..]
        .iter()
        .find(|s| s.worker.index() == worker && !matches!(s.action, StepAction::Done { .. }))
        .map(|s| first_keyword(&s.sql))
        .filter(|k| !k.is_empty());
    let text = match next {
        Some(k) => format!("⏸ [paused before {k}]"),
        None => "⏸ [paused]".to_owned(),
    };
    Cell {
        lines: vec![text],
        style: Style::Dim,
    }
}

/// Wraps `text` and attaches `──► result` to its last line when it fits.
fn with_result(text: &str, result: Option<&str>, w: usize) -> Vec<String> {
    let mut lines = wrap(text, w);
    let Some(r) = result else {
        return lines;
    };
    let tail = format!("──► {r}");
    match lines.last_mut() {
        Some(l) if width(l) + 1 + width(&tail) <= w => {
            l.push(' ');
            l.push_str(&tail);
        }
        _ => lines.extend(wrap(&tail, w)),
    }
    lines
}

fn describe(
    step: &RecordedStep,
    w: usize,
    name_of: &dyn Fn(WorkerId) -> String,
) -> (Vec<String>, Style) {
    let sql = statement_text(step);
    let failed = step.sqlstate.is_some();
    let result_style = if failed { Style::Error } else { Style::Plain };
    match &step.action {
        StepAction::Execute | StepAction::Begin => (
            with_result(&sql, shown_result(step).as_deref(), w),
            result_style,
        ),
        StepAction::Park { blocked_by } => {
            let holders = blocked_by
                .iter()
                .map(|b| name_of(*b))
                .collect::<Vec<_>>()
                .join(", ");
            let mut lines = wrap(&format!("🔒 {sql}"), w);
            lines.extend(wrap(&format!("(blocked by {holders})"), w));
            (lines, Style::Lock)
        }
        StepAction::Resume => {
            let lines = with_result("🔓 lock granted", step.result.as_deref(), w);
            (lines, if failed { Style::Error } else { Style::Lock })
        }
        StepAction::Done { outcome } => {
            let mark = if outcome.success { "✓" } else { "✗" };
            let mut lines = wrap(&format!("{mark} {}", outcome.label), w);
            if !outcome.success
                && let Some(d) = &outcome.detail
            {
                lines.extend(wrap(&one_line(d), w).into_iter().take(2));
            }
            (
                lines,
                if outcome.success {
                    Style::Ok
                } else {
                    Style::Error
                },
            )
        }
    }
}

fn statement_text(step: &RecordedStep) -> String {
    let sql = one_line(&step.sql);
    if step.params.is_empty() {
        return sql;
    }
    let params = step
        .params
        .iter()
        .enumerate()
        .map(|(i, p)| format!("${}={p}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{sql} [{params}]")
}

/// The response summary, unless it merely echoes the command (`BEGIN`,
/// `COMMIT`, ...).
fn shown_result(step: &RecordedStep) -> Option<String> {
    let r = step.result.as_deref()?.trim();
    if r.is_empty() || r.eq_ignore_ascii_case(step.sql.trim().trim_end_matches(';')) {
        return None;
    }
    let kw = first_keyword(&step.sql);
    if r.eq_ignore_ascii_case(&kw)
        || (kw == "START" && r == "BEGIN")
        || (kw == "END" && r == "COMMIT")
    {
        return None;
    }
    Some(r.to_owned())
}

pub(crate) fn first_keyword(sql: &str) -> String {
    sql.split(|c: char| c.is_whitespace() || c == ';' || c == '(')
        .find(|s| !s.is_empty())
        .unwrap_or("")
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hides_echo_results() {
        let mut s = RecordedStep::new(1, WorkerId(0), StepAction::Execute);
        s.sql = "COMMIT".into();
        s.result = Some("COMMIT".into());
        assert_eq!(shown_result(&s), None);
        s.sql = "SELECT 1".into();
        s.result = Some("returns 1".into());
        assert_eq!(shown_result(&s).as_deref(), Some("returns 1"));
        assert_eq!(first_keyword("  update t set x=1"), "UPDATE");
    }
}
