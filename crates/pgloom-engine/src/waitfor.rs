//! Wait-for graph between workers parked on database locks.

use std::collections::{BTreeMap, BTreeSet};

use crate::WorkerId;

/// Directed graph `waiter -> blocker` of workers waiting on database locks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitForGraph {
    edges: BTreeMap<WorkerId, BTreeSet<WorkerId>>,
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `waiter` is blocked by every worker in `blockers`.
    pub fn park(&mut self, waiter: WorkerId, blockers: impl IntoIterator<Item = WorkerId>) {
        let set: BTreeSet<WorkerId> = blockers.into_iter().filter(|b| *b != waiter).collect();
        if set.is_empty() {
            self.edges.remove(&waiter);
        } else {
            self.edges.insert(waiter, set);
        }
    }

    /// Removes `waiter` from the graph (its lock wait ended).
    pub fn unpark(&mut self, waiter: WorkerId) -> bool {
        self.edges.remove(&waiter).is_some()
    }

    /// Drops every edge pointing at `blocker` (e.g. it committed). Returns the
    /// waiters that no longer wait on anyone, in ascending order.
    pub fn unpark_blocked_by(&mut self, blocker: WorkerId) -> Vec<WorkerId> {
        let mut freed = Vec::new();
        for (waiter, set) in &mut self.edges {
            if set.remove(&blocker) && set.is_empty() {
                freed.push(*waiter);
            }
        }
        for w in &freed {
            self.edges.remove(w);
        }
        freed
    }

    pub fn is_parked(&self, worker: WorkerId) -> bool {
        self.edges.contains_key(&worker)
    }

    pub fn blockers_of(&self, worker: WorkerId) -> Vec<WorkerId> {
        self.edges
            .get(&worker)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn parked(&self) -> Vec<WorkerId> {
        self.edges.keys().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Workers from `candidates` that are not parked, preserving order.
    pub fn runnable(&self, candidates: &[WorkerId]) -> Vec<WorkerId> {
        candidates
            .iter()
            .copied()
            .filter(|w| !self.is_parked(*w))
            .collect()
    }

    /// Finds a cycle deterministically: depth-first from the smallest worker,
    /// visiting successors in ascending order. The returned cycle starts at its
    /// smallest member, e.g. `[W0, W1]` for `W0 -> W1 -> W0`.
    pub fn detect_cycle(&self) -> Option<Vec<WorkerId>> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Mark {
            Active,
            Done,
        }
        let mut marks: BTreeMap<WorkerId, Mark> = BTreeMap::new();
        for &start in self.edges.keys() {
            if marks.contains_key(&start) {
                continue;
            }
            // Iterative DFS: (node, successors snapshot, next successor index).
            let mut stack: Vec<(WorkerId, Vec<WorkerId>, usize)> =
                vec![(start, self.blockers_of(start), 0)];
            marks.insert(start, Mark::Active);
            while let Some((node, succ, idx)) = stack.last_mut() {
                if let Some(&next) = succ.get(*idx) {
                    *idx += 1;
                    match marks.get(&next) {
                        Some(Mark::Active) => {
                            let from = stack.iter().position(|(n, _, _)| *n == next)?;
                            let mut cycle: Vec<WorkerId> =
                                stack[from..].iter().map(|(n, _, _)| *n).collect();
                            let min_at = cycle
                                .iter()
                                .enumerate()
                                .min_by_key(|(_, w)| **w)
                                .map_or(0, |(i, _)| i);
                            cycle.rotate_left(min_at);
                            return Some(cycle);
                        }
                        Some(Mark::Done) => {}
                        None => {
                            marks.insert(next, Mark::Active);
                            let succ = self.blockers_of(next);
                            stack.push((next, succ, 0));
                        }
                    }
                } else {
                    marks.insert(*node, Mark::Done);
                    stack.pop();
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const W0: WorkerId = WorkerId(0);
    const W1: WorkerId = WorkerId(1);
    const W2: WorkerId = WorkerId(2);
    const W3: WorkerId = WorkerId(3);

    #[test]
    fn linear_chain_has_no_cycle_and_leaves_the_head_runnable() {
        let mut g = WaitForGraph::new();
        g.park(W2, [W1]);
        g.park(W1, [W0]);
        assert_eq!(g.detect_cycle(), None);
        assert_eq!(g.runnable(&[W0, W1, W2]), vec![W0]);
    }

    #[test]
    fn mutual_wait_is_a_cycle() {
        let mut g = WaitForGraph::new();
        g.park(W1, [W0]);
        g.park(W0, [W1]);
        assert_eq!(g.detect_cycle(), Some(vec![W0, W1]));
    }

    #[test]
    fn three_way_cycle_is_rotated_to_smallest() {
        let mut g = WaitForGraph::new();
        g.park(W1, [W2]);
        g.park(W2, [W0]);
        g.park(W0, [W1]);
        assert_eq!(g.detect_cycle(), Some(vec![W0, W1, W2]));
        // A tail leading into the cycle does not change the answer.
        g.park(W3, [W2]);
        assert_eq!(g.detect_cycle(), Some(vec![W0, W1, W2]));
    }

    #[test]
    fn unpark_blocked_by_frees_only_fully_released_waiters() {
        let mut g = WaitForGraph::new();
        g.park(W1, [W0]);
        g.park(W2, [W0, W3]);
        assert_eq!(g.unpark_blocked_by(W0), vec![W1]);
        assert!(!g.is_parked(W1));
        assert_eq!(g.blockers_of(W2), vec![W3]);
        assert_eq!(g.unpark_blocked_by(W3), vec![W2]);
        assert!(g.is_empty());
    }

    #[test]
    fn self_edges_are_ignored() {
        let mut g = WaitForGraph::new();
        g.park(W0, [W0]);
        assert!(g.is_empty());
        assert_eq!(g.detect_cycle(), None);
    }

    #[test]
    fn cycle_detection_is_deterministic() {
        let build = || {
            let mut g = WaitForGraph::new();
            for (a, b) in [(3, 1), (1, 2), (2, 3), (0, 3)] {
                g.park(WorkerId(a), [WorkerId(b)]);
            }
            g
        };
        let first = build().detect_cycle();
        for _ in 0..100 {
            assert_eq!(build().detect_cycle(), first);
        }
        assert_eq!(first, Some(vec![WorkerId(1), WorkerId(2), WorkerId(3)]));
    }
}
