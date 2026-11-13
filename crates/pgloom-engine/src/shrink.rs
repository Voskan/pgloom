//! Counterexample minimisation.
//!
//! A failing schedule found by exhaustive search often contains incidental
//! context switches. The shrinker proposes simpler schedules and keeps a
//! candidate only when re-execution still reproduces the failure. Candidates
//! are replayed leniently, so the executed schedule (returned by the recheck)
//! is what gets compared, never the proposal itself.
//!
//! Two candidate families are tried, cheapest first:
//! 1. *Truncation*: keep the prefix up to a preemption and continue
//!    non-preemptively, which drops every later preemption at once.
//! 2. *Block moves*: in run-length form (`[(worker, steps)]`), move a block
//!    next to the neighbouring block of the same worker, merging them.

use std::future::Future;

use crate::WorkerId;
use crate::explore::Schedule;

/// Result of a shrinking pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShrinkOutcome {
    pub schedule: Schedule,
    /// Number of re-executions spent.
    pub attempts: usize,
    /// Number of accepted simplifications.
    pub improvements: usize,
}

/// Greedily simplifies `failing`.
///
/// `recheck` replays a list of choices and returns the executed schedule when
/// the failure reproduces, `None` otherwise. At most `max_attempts`
/// re-executions are made.
pub async fn shrink_schedule<F, Fut>(
    failing: Schedule,
    mut recheck: F,
    max_attempts: usize,
) -> ShrinkOutcome
where
    F: FnMut(Vec<WorkerId>) -> Fut,
    Fut: Future<Output = Option<Schedule>>,
{
    let mut best = failing;
    let mut attempts = 0;
    let mut improvements = 0;
    'outer: loop {
        if best.preemptions() == 0 {
            break;
        }
        for candidate in candidates(&best) {
            if attempts >= max_attempts {
                break 'outer;
            }
            attempts += 1;
            if let Some(executed) = recheck(candidate).await
                && executed.complexity() < best.complexity()
            {
                best = executed;
                improvements += 1;
                continue 'outer;
            }
        }
        break;
    }
    ShrinkOutcome {
        schedule: best,
        attempts,
        improvements,
    }
}

fn candidates(s: &Schedule) -> Vec<Vec<WorkerId>> {
    let choices = s.choices();
    let mut out: Vec<Vec<WorkerId>> = Vec::new();
    let mut push = |c: Vec<WorkerId>| {
        if c != choices && !out.contains(&c) {
            out.push(c);
        }
    };

    for (p, d) in s.decisions.iter().enumerate() {
        if d.is_preemption() {
            // Keep the preemption, drop everything that follows it.
            push(choices[..=p].to_vec());
            // Drop this preemption and everything that follows it.
            push(choices[..p].to_vec());
        }
    }

    let blocks = run_lengths(&choices);
    for i in 0..blocks.len() {
        let Some(j) = (i + 1..blocks.len()).find(|&j| blocks[j].0 == blocks[i].0) else {
            continue;
        };
        if j == i + 1 {
            continue;
        }
        // Pull block j forward to merge with block i.
        let mut pulled = blocks.clone();
        let moved = pulled.remove(j);
        pulled.insert(i + 1, moved);
        push(flatten(&pulled));
        // Push block i back to merge with block j.
        let mut pushed = blocks.clone();
        let moved = pushed.remove(i);
        pushed.insert(j - 1, moved);
        push(flatten(&pushed));
    }
    out
}

fn run_lengths(choices: &[WorkerId]) -> Vec<(WorkerId, usize)> {
    let mut out: Vec<(WorkerId, usize)> = Vec::new();
    for &w in choices {
        match out.last_mut() {
            Some((last, n)) if *last == w => *n += 1,
            _ => out.push((w, 1)),
        }
    }
    out
}

fn flatten(blocks: &[(WorkerId, usize)]) -> Vec<WorkerId> {
    blocks
        .iter()
        .flat_map(|(w, n)| std::iter::repeat_n(*w, *n))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::explore::tests::run_toy;
    use crate::explore::{ScheduleExplorer, Strategy};

    /// Bug: worker 1's first step lands between worker 0's first and second
    /// steps (a read-read-write-write race).
    fn has_bug(choices: &[WorkerId]) -> bool {
        let first_w0 = choices.iter().position(|w| *w == WorkerId(0));
        let second_w0 = choices
            .iter()
            .enumerate()
            .filter(|(_, w)| **w == WorkerId(0))
            .nth(1)
            .map(|(i, _)| i);
        let first_w1 = choices.iter().position(|w| *w == WorkerId(1));
        matches!((first_w0, first_w1, second_w0), (Some(a), Some(b), Some(c)) if a < b && b < c)
    }

    fn replay(choices: Vec<WorkerId>, steps: &[usize]) -> Schedule {
        let mut ex = ScheduleExplorer::new(
            Strategy::Replay {
                choices,
                lenient: true,
            },
            1,
        );
        assert!(ex.next_schedule());
        run_toy(&mut ex, steps).unwrap()
    }

    #[tokio::test]
    async fn shrinks_four_switches_to_a_single_preemption() {
        let steps = [4, 4];
        let failing = replay([0, 1, 0, 1, 1, 1, 0, 0].map(WorkerId).to_vec(), &steps);
        assert!(has_bug(&failing.choices()));
        assert_eq!(failing.context_switches(), 4);
        assert_eq!(failing.preemptions(), 3);

        let outcome = shrink_schedule(
            failing,
            |c| {
                let executed = replay(c, &steps);
                async move { has_bug(&executed.choices()).then_some(executed) }
            },
            64,
        )
        .await;
        let s = outcome.schedule;
        assert!(has_bug(&s.choices()));
        assert_eq!(s.preemptions(), 1, "{:?}", s.choices());
        assert_eq!(s.choices(), [0, 1, 1, 1, 1, 0, 0, 0].map(WorkerId).to_vec());
        assert!(outcome.improvements >= 1);
    }

    #[tokio::test]
    async fn respects_the_attempt_budget() {
        let steps = [4, 4];
        let failing = replay([0, 1, 0, 1, 0, 1, 0, 1].map(WorkerId).to_vec(), &steps);
        let outcome = shrink_schedule(failing.clone(), |_| async { None }, 3).await;
        assert_eq!(outcome.attempts, 3);
        assert_eq!(outcome.schedule, failing);
    }

    #[tokio::test]
    async fn already_minimal_schedules_are_untouched() {
        let steps = [2, 2];
        let serial = replay([0, 0, 1, 1].map(WorkerId).to_vec(), &steps);
        let outcome = shrink_schedule(serial.clone(), |_| async { None }, 10).await;
        assert_eq!(outcome.attempts, 0);
        assert_eq!(outcome.schedule, serial);
    }

    #[test]
    fn run_length_roundtrip() {
        let c = [0, 0, 1, 0, 1, 1].map(WorkerId).to_vec();
        assert_eq!(flatten(&run_lengths(&c)), c);
    }
}
