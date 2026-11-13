//! Deterministic stateless exploration of schedules.
//!
//! The explorer is driven by the system under test: at every scheduling point
//! the caller reports which workers can run (`enabled`) and which worker would
//! continue without a context switch (`current`), and the explorer answers with
//! the worker to run. Between schedules it backtracks over the recorded path
//! to the next unexplored branch, so the whole schedule tree is enumerated
//! depth-first without ever being stored in full.
//!
//! In bounded mode the tree is explored by iterative preemption bounding
//! (CHESS): first every schedule with zero preemptions, then every schedule
//! with exactly one, and so on. Branches that exceed the current bound are
//! parked on a frontier and become the roots of the next level, so no schedule
//! is executed twice and failures are found with the fewest preemptions first.
//!
//! In exhaustive mode the tree can be pruned by dynamic partial-order
//! reduction (Flanagan & Godefroid, POPL 2005) combined with sleep sets.
//! Every node starts with a single branch; after each schedule the executed
//! trace is scanned for *races* (conflicting steps of different workers that
//! are not ordered by happens-before), and a branch that reverses each race is
//! added at the node before it. Sleep sets keep the explorer from re-running a
//! branch whose effect is already covered. The result visits at least one
//! schedule of every equivalence class of schedules (schedules that differ
//! only in the order of independent steps), and therefore every reachable
//! final state and deadlock, provided the footprints over-approximate what
//! each step touches (see [`crate::por`]).
//!
//! Reduction is not combined with preemption bounding: pruning by equivalence
//! can discard the only representative of a class that fits within the bound,
//! which silently weakens the bound's guarantee.
//!
//! All collections are ordered (`Vec`, `BTreeSet`); exploration order depends
//! only on the inputs, which keeps `pgloom replay` bit-for-bit reproducible.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::WorkerId;
use crate::por::Footprint;

/// Partial-order reduction applied by the explorer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reduction {
    /// Every interleaving is a distinct schedule.
    #[default]
    None,
    /// Dynamic partial-order reduction with sleep sets (exhaustive mode only).
    Dpor,
}

/// Exploration strategy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    /// Every interleaving, depth-first.
    Exhaustive,
    /// Iterative preemption bounding up to `max_preemptions`; when
    /// `extend_beyond_bound` is set and budget remains, deeper levels follow
    /// until the tree is exhausted.
    Bounded {
        max_preemptions: u32,
        extend_beyond_bound: bool,
    },
    /// Uniformly random choices from a reproducible seed.
    Random { seed: u64 },
    /// A single schedule that follows `choices`. When `lenient`, decisions that
    /// are no longer possible (or past the end) fall back to the
    /// non-preemptive default instead of failing.
    Replay {
        choices: Vec<WorkerId>,
        lenient: bool,
    },
}

/// One scheduling decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub enabled: Vec<WorkerId>,
    pub current: Option<WorkerId>,
    pub chosen: WorkerId,
}

impl Decision {
    /// A preemption switches away from a worker that could have continued.
    pub fn is_preemption(&self) -> bool {
        is_preemption(self.current, &self.enabled, self.chosen)
    }
}

fn is_preemption(current: Option<WorkerId>, enabled: &[WorkerId], chosen: WorkerId) -> bool {
    current.is_some_and(|c| c != chosen && enabled.contains(&c))
}

/// The sequence of decisions of one executed schedule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedule {
    pub decisions: Vec<Decision>,
}

impl Schedule {
    pub fn choices(&self) -> Vec<WorkerId> {
        self.decisions.iter().map(|d| d.chosen).collect()
    }

    pub fn preemptions(&self) -> u32 {
        self.decisions.iter().filter(|d| d.is_preemption()).count() as u32
    }

    pub fn context_switches(&self) -> u32 {
        self.decisions
            .windows(2)
            .filter(|w| w[0].chosen != w[1].chosen)
            .count() as u32
    }

    /// Ordering key for "simpler" counterexamples.
    pub fn complexity(&self) -> (u32, u32, usize) {
        (
            self.preemptions(),
            self.context_switches(),
            self.decisions.len(),
        )
    }
}

/// The run diverged from a previously recorded schedule. This means the system
/// under test is not deterministic with respect to the scheduled order.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "nondeterminism at decision {depth}: expected enabled workers {expected:?}, observed {observed:?}"
)]
pub struct Divergence {
    pub depth: usize,
    pub expected: Vec<WorkerId>,
    pub observed: Vec<WorkerId>,
}

/// Summary of an exploration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplorationStats {
    pub schedules: usize,
    /// Schedules abandoned by partial-order reduction because every
    /// continuation was equivalent to one already explored.
    #[serde(default)]
    pub pruned: usize,
    /// Partial-order reduction was in effect.
    #[serde(default)]
    pub reduced: bool,
    /// Preemption level reached by iterative bounding (bounded mode only).
    pub preemption_level: Option<u32>,
    /// True when every schedule of the tree was executed.
    pub complete: bool,
}

#[derive(Debug, Clone)]
struct Node {
    enabled: Vec<WorkerId>,
    current: Option<WorkerId>,
    /// Branch order: the non-preemptive continuation first, then ascending ids.
    /// Under reduction it starts with one branch and grows as races are found.
    order: Vec<WorkerId>,
    idx: usize,
    preemptions_before: u32,
    /// Footprint of each enabled worker's next step (reduction only).
    footprints: Vec<Footprint>,
    /// Workers whose next step need not be explored from this node.
    sleep: Vec<(WorkerId, Footprint)>,
}

impl Node {
    fn new(enabled: &[WorkerId], current: Option<WorkerId>, preemptions_before: u32) -> Self {
        let mut order: Vec<WorkerId> = Vec::with_capacity(enabled.len());
        if let Some(c) = current.filter(|c| enabled.contains(c)) {
            order.push(c);
        }
        let mut rest: Vec<WorkerId> = enabled
            .iter()
            .copied()
            .filter(|w| !order.contains(w))
            .collect();
        rest.sort_unstable();
        rest.dedup();
        order.extend(rest);
        Self {
            enabled: enabled.to_vec(),
            current,
            order,
            idx: 0,
            preemptions_before,
            footprints: Vec::new(),
            sleep: Vec::new(),
        }
    }

    fn chosen(&self) -> WorkerId {
        self.order[self.idx]
    }

    /// Without a recorded footprint a step conflicts with everything.
    fn footprint_of(&self, w: WorkerId) -> Footprint {
        self.enabled
            .iter()
            .position(|e| *e == w)
            .and_then(|i| self.footprints.get(i).cloned())
            .unwrap_or_else(Footprint::everything)
    }

    fn is_asleep(&self, w: WorkerId) -> bool {
        self.sleep.iter().any(|(s, _)| *s == w)
    }

    /// The sleep set of the child reached through the current branch: every
    /// sibling explored before it, and every worker already asleep here, whose
    /// step commutes with the branch taken.
    fn child_sleep(&self) -> Vec<(WorkerId, Footprint)> {
        let taken = self.chosen();
        let taken_fp = self.footprint_of(taken);
        let mut out: Vec<(WorkerId, Footprint)> = Vec::new();
        let siblings = self.order[..self.idx]
            .iter()
            .map(|w| (*w, self.footprint_of(*w)));
        for (w, fp) in self.sleep.iter().cloned().chain(siblings) {
            if w != taken && !fp.conflicts(&taken_fp) && !out.iter().any(|(o, _)| *o == w) {
                out.push((w, fp));
            }
        }
        out.sort_by_key(|(w, _)| *w);
        out
    }

    /// Ensures one of `initials` is explored from this node. Falls back to
    /// every enabled worker if none of them can run here.
    fn add_initial(&mut self, initials: &[WorkerId]) {
        if initials.iter().any(|q| self.order.contains(q)) {
            return;
        }
        let mut runnable: Vec<WorkerId> = initials
            .iter()
            .copied()
            .filter(|q| self.enabled.contains(q))
            .collect();
        runnable.sort_unstable();
        if runnable.is_empty() {
            let mut all = self.enabled.clone();
            all.sort_unstable();
            for w in all {
                if !self.order.contains(&w) {
                    self.order.push(w);
                }
            }
            return;
        }
        // A sleeping initial is already covered; adding it records that.
        let pick = runnable
            .iter()
            .copied()
            .find(|q| !self.is_asleep(*q))
            .unwrap_or(runnable[0]);
        self.order.push(pick);
    }

    fn preemptions_after(&self) -> u32 {
        self.preemptions_before + u32::from(self.cost_of(self.idx))
    }

    fn cost_of(&self, idx: usize) -> bool {
        is_preemption(self.current, &self.enabled, self.order[idx])
    }
}

/// A pruned branch: the path prefix whose last node is set to the branch.
#[derive(Debug, Clone)]
struct FrontierItem {
    path: Vec<Node>,
}

/// Stateless depth-first schedule explorer.
#[derive(Debug, Clone)]
pub struct ScheduleExplorer {
    strategy: Strategy,
    reduction: Reduction,
    /// Next steps of the workers that were still runnable when the current
    /// schedule ended (reduction only).
    tail: Vec<(WorkerId, Footprint)>,
    pruned: usize,
    max_schedules: usize,
    path: Vec<Node>,
    pos: usize,
    /// Nodes below this depth belong to the current subtree root and are fixed.
    floor: usize,
    bound: Option<u32>,
    frontier: VecDeque<FrontierItem>,
    next_frontier: VecDeque<FrontierItem>,
    started: usize,
    exhausted: bool,
    rng: u64,
    trace: Vec<Decision>,
}

impl ScheduleExplorer {
    pub fn new(strategy: Strategy, max_schedules: usize) -> Self {
        let bound = match &strategy {
            Strategy::Bounded { .. } => Some(0),
            _ => None,
        };
        let rng = match &strategy {
            Strategy::Random { seed } => *seed ^ 0x9E37_79B9_7F4A_7C15,
            _ => 0,
        };
        Self {
            strategy,
            reduction: Reduction::None,
            tail: Vec::new(),
            pruned: 0,
            max_schedules: max_schedules.max(1),
            path: Vec::new(),
            pos: 0,
            floor: 0,
            bound,
            frontier: VecDeque::new(),
            next_frontier: VecDeque::new(),
            started: 0,
            exhausted: false,
            rng,
            trace: Vec::new(),
        }
    }

    pub fn strategy(&self) -> &Strategy {
        &self.strategy
    }

    /// Enables partial-order reduction. It only applies to
    /// [`Strategy::Exhaustive`]; other strategies ignore it.
    #[must_use]
    pub fn with_reduction(mut self, reduction: Reduction) -> Self {
        self.reduction = reduction;
        self
    }

    /// Whether choices should be accompanied by footprints.
    pub fn reduces(&self) -> bool {
        self.reduction == Reduction::Dpor && self.strategy == Strategy::Exhaustive
    }

    /// Prepares the next schedule. Returns `false` when the tree is exhausted
    /// or the schedule budget is spent.
    pub fn next_schedule(&mut self) -> bool {
        if self.exhausted || self.started >= self.max_schedules {
            return false;
        }
        if self.started > 0 {
            let more = match self.strategy {
                Strategy::Replay { .. } => false,
                Strategy::Random { .. } => true,
                Strategy::Exhaustive | Strategy::Bounded { .. } => self.backtrack(),
            };
            if !more {
                self.exhausted = true;
                return false;
            }
        }
        self.pos = 0;
        self.trace.clear();
        self.tail.clear();
        self.started += 1;
        true
    }

    /// Picks the worker to run at the current scheduling point.
    pub fn choose(
        &mut self,
        enabled: &[WorkerId],
        current: Option<WorkerId>,
    ) -> Result<WorkerId, Divergence> {
        // Without footprints every step conflicts with every other one, so
        // nothing is ever asleep and a choice always exists.
        Ok(self
            .choose_with(enabled, current, &[])?
            .unwrap_or_else(|| default_choice(enabled, current)))
    }

    /// Like [`choose`](Self::choose), with the footprint of each enabled
    /// worker's next step (aligned with `enabled`). Returns `None` when
    /// reduction proves that every continuation of this schedule is
    /// equivalent to one already explored; the caller should abandon the
    /// schedule and report its remaining steps with
    /// [`finish_schedule`](Self::finish_schedule).
    pub fn choose_with(
        &mut self,
        enabled: &[WorkerId],
        current: Option<WorkerId>,
        footprints: &[Footprint],
    ) -> Result<Option<WorkerId>, Divergence> {
        debug_assert!(!enabled.is_empty(), "choose() needs an enabled worker");
        let chosen = match &self.strategy {
            Strategy::Replay { choices, lenient } => {
                let wanted = choices.get(self.trace.len()).copied();
                match wanted {
                    Some(w) if enabled.contains(&w) => w,
                    _ if *lenient => default_choice(enabled, current),
                    _ => {
                        return Err(Divergence {
                            depth: self.trace.len(),
                            expected: wanted.into_iter().collect(),
                            observed: enabled.to_vec(),
                        });
                    }
                }
            }
            Strategy::Random { .. } => {
                let mut sorted = enabled.to_vec();
                sorted.sort_unstable();
                let i = (self.next_random() % sorted.len() as u64) as usize;
                sorted[i]
            }
            Strategy::Exhaustive | Strategy::Bounded { .. } => {
                match self.choose_tree(enabled, current, footprints)? {
                    Some(w) => w,
                    None => {
                        self.pruned += 1;
                        return Ok(None);
                    }
                }
            }
        };
        self.trace.push(Decision {
            enabled: enabled.to_vec(),
            current,
            chosen,
        });
        Ok(Some(chosen))
    }

    /// Reports the next step of every worker that could still run when the
    /// schedule ended early (a deadlock or an abandoned schedule), so races
    /// with those steps are reversed too.
    pub fn finish_schedule(&mut self, pending: Vec<(WorkerId, Footprint)>) {
        self.tail = pending;
    }

    /// Decisions taken so far in the current schedule.
    pub fn schedule(&self) -> Schedule {
        Schedule {
            decisions: self.trace.clone(),
        }
    }

    pub fn stats(&self) -> ExplorationStats {
        let tree = matches!(
            self.strategy,
            Strategy::Exhaustive | Strategy::Bounded { .. }
        );
        let complete = tree && {
            let mut probe = self.clone();
            let more = !probe.exhausted && probe.backtrack();
            !more && probe.frontier.is_empty() && probe.next_frontier.is_empty()
        };
        ExplorationStats {
            schedules: self.started,
            pruned: self.pruned,
            reduced: self.reduces(),
            preemption_level: self.bound,
            complete,
        }
    }

    fn choose_tree(
        &mut self,
        enabled: &[WorkerId],
        current: Option<WorkerId>,
        footprints: &[Footprint],
    ) -> Result<Option<WorkerId>, Divergence> {
        if self.pos < self.path.len() {
            let node = &self.path[self.pos];
            if node.enabled != enabled || node.current != current {
                return Err(Divergence {
                    depth: self.pos,
                    expected: node.enabled.clone(),
                    observed: enabled.to_vec(),
                });
            }
            self.pos += 1;
            return Ok(Some(node.chosen()));
        }
        let before = self.path.last().map_or(0, Node::preemptions_after);
        let mut node = Node::new(enabled, current, before);
        if self.reduces() {
            node.footprints = footprints.to_vec();
            node.sleep = self.path.last().map(Node::child_sleep).unwrap_or_default();
            let awake: Vec<WorkerId> = enabled
                .iter()
                .copied()
                .filter(|w| !node.is_asleep(*w))
                .collect();
            if awake.is_empty() {
                return Ok(None);
            }
            node.order = vec![default_choice(&awake, current)];
        }
        // Index 0 is the non-preemptive continuation, always within bound.
        let chosen = node.chosen();
        self.path.push(node);
        self.pos += 1;
        Ok(Some(chosen))
    }

    /// Scans the executed trace for races and schedules their reversal: the
    /// race-detection step of Source-DPOR (Abdulla, Aronis, Jonsson and
    /// Sagonas, POPL 2014), run once per schedule over the whole trace.
    ///
    /// Events `i < k` of different workers *race* when they conflict and no
    /// event between them happens after `i` and before `k`. To reverse the
    /// race, the node before `i` must explore a worker that can start `v`:
    /// the events between `i` and `k` that do not depend on `i`, followed by
    /// `k`. One such *initial* is added unless the node already has one.
    fn add_races(&mut self) {
        let tail = std::mem::take(&mut self.tail);
        let width = self
            .path
            .iter()
            .flat_map(|nd| nd.enabled.iter())
            .chain(tail.iter().map(|(w, _)| w))
            .map(|w| w.index() + 1)
            .max()
            .unwrap_or(0);
        let mut trace = Trace::new(width);
        let mut found: Vec<(usize, Vec<WorkerId>)> = Vec::new();
        for node in &self.path {
            let w = node.chosen();
            trace.push(w.index(), node.footprint_of(w));
            found.extend(trace.races_of_last());
        }
        // Steps that were still pending when the schedule ended early.
        for (w, fp) in tail {
            let saved = trace.push(w.index(), fp);
            found.extend(trace.races_of_last());
            trace.pop(saved);
        }
        for (i, initials) in found {
            self.path[i].add_initial(&initials);
        }
    }

    /// Moves to the next unexplored branch. Returns `false` when done.
    fn backtrack(&mut self) -> bool {
        // An episode may stop early (violation, early worker exit); the
        // unvisited suffix of the recorded path is not part of this schedule.
        self.path.truncate(self.pos.max(self.floor));
        if self.reduces() {
            self.add_races();
        }
        loop {
            while self.path.len() > self.floor {
                let depth = self.path.len() - 1;
                let bound = self.bound;
                let node = &mut self.path[depth];
                if node.idx + 1 < node.order.len() {
                    node.idx += 1;
                    if node.is_asleep(node.order[node.idx]) {
                        continue;
                    }
                    let cost = node.preemptions_before + u32::from(node.cost_of(node.idx));
                    if bound.is_none_or(|b| cost <= b) {
                        return true;
                    }
                    // Exceeds the current bound: defer the branch to the next
                    // level and keep scanning this node's alternatives.
                    let path = self.path[..=depth].to_vec();
                    self.next_frontier.push_back(FrontierItem { path });
                    continue;
                }
                self.path.pop();
            }
            // Current subtree exhausted: take the next frontier root.
            if let Some(item) = self.frontier.pop_front() {
                self.floor = item.path.len();
                self.path = item.path;
                return true;
            }
            if self.next_frontier.is_empty() {
                return false;
            }
            let next_bound = self.bound.map_or(0, |b| b + 1);
            if let Strategy::Bounded {
                max_preemptions,
                extend_beyond_bound,
            } = self.strategy
                && next_bound > max_preemptions
                && !extend_beyond_bound
            {
                return false;
            }
            self.bound = Some(next_bound);
            self.frontier = std::mem::take(&mut self.next_frontier);
            self.floor = 0;
            self.path.clear();
        }
    }

    fn next_random(&mut self) -> u64 {
        // splitmix64: tiny, fast and fully reproducible across platforms.
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// An executed trace with vector clocks over program order and conflicts.
struct Trace {
    procs: Vec<usize>,
    fps: Vec<Footprint>,
    clocks: Vec<Vec<u32>>,
    /// Clock of each worker's latest event.
    latest: Vec<Vec<u32>>,
}

impl Trace {
    fn new(width: usize) -> Self {
        Self {
            procs: Vec::new(),
            fps: Vec::new(),
            clocks: Vec::new(),
            latest: vec![vec![0; width]; width],
        }
    }

    /// Appends an event, returning the worker's previous clock for [`pop`].
    fn push(&mut self, p: usize, fp: Footprint) -> Vec<u32> {
        let mut clock = self.latest[p].clone();
        for (i, other) in self.fps.iter().enumerate() {
            if self.procs[i] != p && other.conflicts(&fp) {
                for (c, x) in clock.iter_mut().zip(&self.clocks[i]) {
                    *c = (*c).max(*x);
                }
            }
        }
        clock[p] += 1;
        self.procs.push(p);
        self.fps.push(fp);
        self.clocks.push(clock.clone());
        std::mem::replace(&mut self.latest[p], clock)
    }

    fn pop(&mut self, previous: Vec<u32>) {
        if let Some(p) = self.procs.pop() {
            self.fps.pop();
            self.clocks.pop();
            self.latest[p] = previous;
        }
    }

    /// Event `a` happens before event `b` (`a < b`).
    fn hb(&self, a: usize, b: usize) -> bool {
        let p = self.procs[a];
        self.clocks[b][p] >= self.clocks[a][p]
    }

    /// Races whose later event is the last one, with the initials of the
    /// sequence that reverses each.
    fn races_of_last(&self) -> Vec<(usize, Vec<WorkerId>)> {
        let Some(k) = self.procs.len().checked_sub(1) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for i in 0..k {
            if self.procs[i] == self.procs[k] || !self.fps[i].conflicts(&self.fps[k]) {
                continue;
            }
            if (i + 1..k).any(|j| self.hb(i, j) && self.hb(j, k)) {
                continue;
            }
            let v: Vec<usize> = (i + 1..k)
                .filter(|&j| !self.hb(i, j))
                .chain(std::iter::once(k))
                .collect();
            let mut seen: Vec<usize> = Vec::new();
            let mut initials: Vec<WorkerId> = Vec::new();
            for (pos, &j) in v.iter().enumerate() {
                let p = self.procs[j];
                if seen.contains(&p) {
                    continue;
                }
                seen.push(p);
                if !v[..pos].iter().any(|&x| self.hb(x, j)) {
                    initials.push(WorkerId(p as u32));
                }
            }
            out.push((i, initials));
        }
        out
    }
}

/// The non-preemptive choice: keep running `current`, else the lowest id.
pub fn default_choice(enabled: &[WorkerId], current: Option<WorkerId>) -> WorkerId {
    match current.filter(|c| enabled.contains(c)) {
        Some(c) => c,
        None => enabled.iter().copied().min().unwrap_or(WorkerId(0)),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Toy system: worker `i` performs `steps[i]` atomic steps.
    pub(crate) fn run_toy(
        explorer: &mut ScheduleExplorer,
        steps: &[usize],
    ) -> Result<Schedule, Divergence> {
        let mut remaining = steps.to_vec();
        let mut current: Option<WorkerId> = None;
        loop {
            let enabled: Vec<WorkerId> = remaining
                .iter()
                .enumerate()
                .filter(|(_, r)| **r > 0)
                .map(|(i, _)| WorkerId(i as u32))
                .collect();
            if enabled.is_empty() {
                return Ok(explorer.schedule());
            }
            let cur = current.filter(|c| enabled.contains(c));
            let w = explorer.choose(&enabled, cur)?;
            remaining[w.0 as usize] -= 1;
            current = Some(w);
        }
    }

    fn explore_all(strategy: Strategy, steps: &[usize], budget: usize) -> Vec<Schedule> {
        let mut ex = ScheduleExplorer::new(strategy, budget);
        let mut out = Vec::new();
        while ex.next_schedule() {
            out.push(run_toy(&mut ex, steps).unwrap());
        }
        out
    }

    fn binom(n: u64, k: u64) -> u64 {
        (1..=k).fold(1, |acc, i| acc * (n - k + i) / i)
    }

    #[test]
    fn exhaustive_two_by_three_yields_twenty_unique_schedules() {
        let all = explore_all(Strategy::Exhaustive, &[3, 3], 10_000);
        assert_eq!(all.len() as u64, binom(6, 3));
        let unique: BTreeSet<Vec<WorkerId>> = all.iter().map(Schedule::choices).collect();
        assert_eq!(unique.len(), 20);
    }

    #[test]
    fn exhaustive_three_workers_matches_multinomial() {
        let all = explore_all(Strategy::Exhaustive, &[2, 2, 2], 10_000);
        // 6! / (2! 2! 2!) = 90
        assert_eq!(all.len(), 90);
    }

    #[test]
    fn bounded_respects_the_preemption_bound_and_is_exact() {
        for bound in 0..=3 {
            let got = explore_all(
                Strategy::Bounded {
                    max_preemptions: bound,
                    extend_beyond_bound: false,
                },
                &[3, 3],
                10_000,
            );
            assert!(got.iter().all(|s| s.preemptions() <= bound));
            let got_set: BTreeSet<_> = got.iter().map(Schedule::choices).collect();
            assert_eq!(got_set.len(), got.len(), "no schedule may repeat");
            let expected: BTreeSet<_> = explore_all(Strategy::Exhaustive, &[3, 3], 10_000)
                .into_iter()
                .filter(|s| s.preemptions() <= bound)
                .map(|s| s.choices())
                .collect();
            assert_eq!(got_set, expected, "bound {bound}");
        }
    }

    #[test]
    fn iterative_bounding_visits_levels_in_order() {
        let got = explore_all(
            Strategy::Bounded {
                max_preemptions: 2,
                extend_beyond_bound: false,
            },
            &[3, 3, 2],
            10_000,
        );
        let levels: Vec<u32> = got.iter().map(Schedule::preemptions).collect();
        assert!(levels.windows(2).all(|w| w[0] <= w[1]), "{levels:?}");
        assert_eq!(levels.first(), Some(&0));
    }

    #[test]
    fn extending_beyond_the_bound_eventually_covers_the_tree() {
        let mut ex = ScheduleExplorer::new(
            Strategy::Bounded {
                max_preemptions: 0,
                extend_beyond_bound: true,
            },
            10_000,
        );
        let mut seen = BTreeSet::new();
        while ex.next_schedule() {
            seen.insert(run_toy(&mut ex, &[3, 3]).unwrap().choices());
        }
        assert_eq!(seen.len(), 20);
        assert!(ex.stats().complete);
    }

    #[test]
    fn budget_limits_schedules_and_reports_incomplete() {
        let mut ex = ScheduleExplorer::new(Strategy::Exhaustive, 5);
        let mut n = 0;
        while ex.next_schedule() {
            run_toy(&mut ex, &[3, 3]).unwrap();
            n += 1;
        }
        assert_eq!(n, 5);
        assert!(!ex.stats().complete);
        assert_eq!(ex.stats().schedules, 5);
    }

    #[test]
    fn exploration_is_deterministic_across_100_runs() {
        let strategies = [
            Strategy::Exhaustive,
            Strategy::Bounded {
                max_preemptions: 2,
                extend_beyond_bound: true,
            },
            Strategy::Random { seed: 7 },
        ];
        for strategy in strategies {
            let reference: Vec<Vec<WorkerId>> = explore_all(strategy.clone(), &[3, 2, 2], 60)
                .iter()
                .map(Schedule::choices)
                .collect();
            for _ in 0..100 {
                let again: Vec<Vec<WorkerId>> = explore_all(strategy.clone(), &[3, 2, 2], 60)
                    .iter()
                    .map(Schedule::choices)
                    .collect();
                assert_eq!(again, reference);
            }
        }
    }

    #[test]
    fn first_schedule_is_serial() {
        let all = explore_all(Strategy::Exhaustive, &[3, 3], 10);
        assert_eq!(all[0].choices(), [0, 0, 0, 1, 1, 1].map(WorkerId).to_vec());
        assert_eq!(all[0].preemptions(), 0);
    }

    #[test]
    fn strict_replay_reproduces_and_detects_divergence() {
        let choices = [0, 1, 1, 0, 0, 1].map(WorkerId).to_vec();
        let mut ex = ScheduleExplorer::new(
            Strategy::Replay {
                choices: choices.clone(),
                lenient: false,
            },
            10,
        );
        assert!(ex.next_schedule());
        assert_eq!(run_toy(&mut ex, &[3, 3]).unwrap().choices(), choices);
        assert!(!ex.next_schedule(), "replay runs exactly one schedule");

        // Worker 1 only has 2 steps now: decision 5 cannot be honoured.
        let mut ex = ScheduleExplorer::new(
            Strategy::Replay {
                choices,
                lenient: false,
            },
            10,
        );
        assert!(ex.next_schedule());
        assert!(run_toy(&mut ex, &[4, 2]).is_err());
    }

    #[test]
    fn lenient_replay_falls_back_to_non_preemptive_choices() {
        let mut ex = ScheduleExplorer::new(
            Strategy::Replay {
                choices: vec![WorkerId(1)],
                lenient: true,
            },
            1,
        );
        assert!(ex.next_schedule());
        let s = run_toy(&mut ex, &[2, 2]).unwrap();
        assert_eq!(s.choices(), [1, 1, 0, 0].map(WorkerId).to_vec());
    }

    #[test]
    fn divergence_from_recorded_path_is_reported() {
        let mut ex = ScheduleExplorer::new(Strategy::Exhaustive, 100);
        assert!(ex.next_schedule());
        run_toy(&mut ex, &[2, 2]).unwrap();
        assert!(ex.next_schedule());
        // The second schedule replays a prefix; feed a different enabled set.
        let err = ex.choose(&[WorkerId(0)], None).unwrap_err();
        assert_eq!(err.depth, 0);
    }

    mod reduction {
        use std::collections::BTreeSet;

        use super::super::*;
        use crate::por::{Object, Objects};

        fn obj(names: &[&str]) -> Objects {
            Objects::Only(
                names
                    .iter()
                    .map(|n| Object::Relation((*n).to_owned()))
                    .collect(),
            )
        }

        fn read(names: &[&str]) -> Footprint {
            Footprint {
                reads: obj(names),
                writes: Objects::default(),
            }
        }

        fn write(names: &[&str]) -> Footprint {
            Footprint {
                reads: Objects::default(),
                writes: obj(names),
            }
        }

        type Event = (usize, usize);

        /// Runs one schedule of a toy system where worker `w` performs
        /// `steps[w]` in order. Returns the executed events, or `None` when
        /// the explorer abandoned the schedule.
        fn run(ex: &mut ScheduleExplorer, steps: &[Vec<Footprint>]) -> Option<Vec<Event>> {
            let mut next = vec![0usize; steps.len()];
            let mut current: Option<WorkerId> = None;
            let mut events = Vec::new();
            loop {
                let enabled: Vec<WorkerId> = (0..steps.len())
                    .filter(|&w| next[w] < steps[w].len())
                    .map(|w| WorkerId(w as u32))
                    .collect();
                if enabled.is_empty() {
                    return Some(events);
                }
                let fps: Vec<Footprint> = enabled
                    .iter()
                    .map(|w| steps[w.index()][next[w.index()]].clone())
                    .collect();
                let cur = current.filter(|c| enabled.contains(c));
                match ex.choose_with(&enabled, cur, &fps).unwrap() {
                    Some(w) => {
                        events.push((w.index(), next[w.index()]));
                        next[w.index()] += 1;
                        current = Some(w);
                    }
                    None => {
                        ex.finish_schedule(enabled.into_iter().zip(fps).collect());
                        return None;
                    }
                }
            }
        }

        /// The equivalence class of a schedule: the order of every pair of
        /// conflicting events.
        fn class(events: &[Event], steps: &[Vec<Footprint>]) -> BTreeSet<(Event, Event)> {
            let mut out = BTreeSet::new();
            for (i, a) in events.iter().enumerate() {
                for b in &events[i + 1..] {
                    if a.0 != b.0 && steps[a.0][a.1].conflicts(&steps[b.0][b.1]) {
                        out.insert((*a, *b));
                    }
                }
            }
            out
        }

        struct Outcome {
            classes: BTreeSet<BTreeSet<(Event, Event)>>,
            complete: usize,
            started: usize,
        }

        fn explore(reduction: Reduction, steps: &[Vec<Footprint>]) -> Outcome {
            let mut ex =
                ScheduleExplorer::new(Strategy::Exhaustive, 100_000).with_reduction(reduction);
            let mut classes = BTreeSet::new();
            let mut complete = 0;
            while ex.next_schedule() {
                if let Some(events) = run(&mut ex, steps) {
                    complete += 1;
                    classes.insert(class(&events, steps));
                }
            }
            assert!(ex.stats().complete);
            Outcome {
                classes,
                complete,
                started: ex.stats().schedules,
            }
        }

        fn assert_sound(steps: &[Vec<Footprint>]) -> (Outcome, Outcome) {
            let full = explore(Reduction::None, steps);
            let dpor = explore(Reduction::Dpor, steps);
            assert_eq!(dpor.classes, full.classes, "every class must be covered");
            assert!(dpor.started <= full.started);
            (full, dpor)
        }

        #[test]
        fn independent_workers_need_a_single_schedule() {
            let steps = vec![
                vec![write(&["a"]), read(&["a"]), write(&["a"])],
                vec![write(&["b"]), read(&["b"]), write(&["b"])],
            ];
            let (full, dpor) = assert_sound(&steps);
            assert_eq!(full.complete, 20);
            assert_eq!((dpor.complete, dpor.started), (1, 1));
        }

        #[test]
        fn fully_dependent_workers_are_not_reduced() {
            let steps = vec![vec![write(&["a"]); 3], vec![write(&["a"]); 3]];
            let (full, dpor) = assert_sound(&steps);
            assert_eq!(dpor.complete, full.complete);
        }

        #[test]
        fn concurrent_reads_commute() {
            // read-modify-write of one row by two transactions.
            let rmw = vec![read(&["w"]), write(&["w"]), write(&["w"])];
            let steps = vec![rmw.clone(), rmw];
            let (full, dpor) = assert_sound(&steps);
            assert_eq!(full.complete, 20);
            assert_eq!(dpor.complete, full.classes.len());
            assert!(dpor.complete < full.complete);
        }

        #[test]
        fn three_workers_with_partial_overlap() {
            let steps = vec![
                vec![read(&["a"]), write(&["a"])],
                vec![read(&["a", "b"]), write(&["b"])],
                vec![write(&["c"]), read(&["b"])],
            ];
            let (full, dpor) = assert_sound(&steps);
            assert!(dpor.complete < full.complete);
        }

        #[test]
        fn randomized_systems_keep_every_class() {
            let names = ["a", "b", "c"];
            let mut seed: u64 = 0x5EED;
            let mut rnd = |n: usize| {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((seed >> 33) as usize) % n
            };
            let systems: usize = std::env::var("PGLOOM_DPOR_STRESS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(200);
            for _ in 0..systems {
                let workers = 2 + rnd(2);
                let steps: Vec<Vec<Footprint>> = (0..workers)
                    .map(|_| {
                        (0..1 + rnd(3))
                            .map(|_| {
                                let o = [names[rnd(3)]];
                                match rnd(5) {
                                    0 | 1 => read(&o),
                                    2 => write(&o),
                                    3 => Footprint {
                                        reads: obj(&o),
                                        writes: obj(&[names[rnd(3)]]),
                                    },
                                    _ => Footprint::none(),
                                }
                            })
                            .collect()
                    })
                    .collect();
                assert_sound(&steps);
            }
        }

        #[test]
        fn reduction_is_deterministic_and_ignored_outside_exhaustive() {
            let rmw = vec![read(&["w"]), write(&["w"]), write(&["w"])];
            let steps = vec![rmw.clone(), rmw.clone(), rmw];
            let trace = |r| {
                let mut ex = ScheduleExplorer::new(Strategy::Exhaustive, 10_000).with_reduction(r);
                let mut all = Vec::new();
                while ex.next_schedule() {
                    all.push(run(&mut ex, &steps));
                }
                all
            };
            let first = trace(Reduction::Dpor);
            for _ in 0..20 {
                assert_eq!(trace(Reduction::Dpor), first);
            }
            let bounded = ScheduleExplorer::new(
                Strategy::Bounded {
                    max_preemptions: 2,
                    extend_beyond_bound: false,
                },
                10,
            )
            .with_reduction(Reduction::Dpor);
            assert!(!bounded.reduces());
        }
    }
}
