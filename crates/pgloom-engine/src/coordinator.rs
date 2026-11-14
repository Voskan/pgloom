//! Episode orchestration: one schedule, executed against the real database.
//!
//! # Execution model
//!
//! Workers are launched one at a time (*staged launch*): worker `i + 1` is
//! started only once worker `i` is held at the gate or has finished. From then
//! on at most one worker is ever *free* (running application code): every
//! other worker is either held at the gate, executing its granted turn, parked
//! on a database lock, or waiting for its held response to be released. Hence
//! every turn the proxy reports belongs to the single free worker, which makes
//! the worker-to-connection mapping deterministic even with connection pools.
//!
//! A granted turn normally completes within a millisecond. If it does not
//! complete within `lock_probe`, `pg_blocking_pids()` tells whether it waits on
//! a lock held by another worker; if so the worker is *parked* and another
//! worker is scheduled. A cycle in the wait-for graph is a proven deadlock.
//! Before each decision parked workers are re-probed and any that were
//! unblocked are awaited, so the enabled set at every decision depends only on
//! the schedule prefix.
//!
//! Under partial-order reduction every decision also reports the footprint of
//! each enabled worker's next step: its pending turn as classified against the
//! schema model and the worker's open transaction, or, for a held response,
//! the footprint of the turn it completes (the lock release that enabled it
//! conflicts with that turn, which keeps the enabling visible to the
//! explorer).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pgloom_wire::{
    Access, ConnId, FinishedTurn, Gate, GateEvent, PendingTurn, TurnInfo, TurnResponse, TxStatus,
    classify_access,
};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep_until;
use tracing::{debug, trace, warn};

use crate::explore::{
    Decision, Divergence, ExplorationStats, Schedule, ScheduleExplorer, Strategy,
};
use crate::por::{Footprint, SchemaModel, TxTrack};
use crate::record::{
    DeadlockSource, EpisodeRecord, RecordedStep, StepAction, Violation, WorkerId, WorkerOutcome,
};
use crate::shrink::shrink_schedule;
use crate::waitfor::WaitForGraph;

/// Errors that abort a run (as opposed to violations, which are findings).
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(
        "worker `{worker}` neither issued a database turn nor finished within {ms} ms; \
         if the application uses a connection pool, it must allow at least as many \
         concurrent connections as there are workers"
    )]
    WorkerStalled { worker: String, ms: u64 },

    #[error(
        "turn of worker `{worker}` did not complete within {ms} ms and is not waiting on a lock held by another worker: {sql}"
    )]
    StepTimeout {
        worker: String,
        sql: String,
        ms: u64,
    },

    #[error(
        "workers {workers:?} are blocked on locks held outside the scenario for more than {ms} ms"
    )]
    Stalled { workers: Vec<String>, ms: u64 },

    #[error("the system under test behaved nondeterministically: {0}")]
    Nondeterminism(#[from] Divergence),

    #[error("database control error: {0}")]
    Db(String),

    #[error("invariant check failed: {0}")]
    Oracle(String),
}

/// Launches the concurrent triggers of a scenario.
pub trait Workload: Send + Sync {
    fn worker_count(&self) -> usize;

    fn worker_name(&self, worker: WorkerId) -> String;

    /// Starts one worker; the future resolves when its trigger finished.
    fn launch(&self, worker: WorkerId) -> impl Future<Output = WorkerOutcome> + Send + 'static;
}

/// Database control operations needed by the coordinator.
pub trait DbControl: Send + Sync {
    /// `pid -> blocking pids` for the waiting backends among `pids`.
    fn blocking_pids(
        &self,
        pids: &[i32],
    ) -> impl Future<Output = Result<BTreeMap<i32, Vec<i32>>, EngineError>> + Send;

    fn cancel_backend(&self, pid: i32) -> impl Future<Output = Result<(), EngineError>> + Send;

    /// Terminates backends among `pids` that still hold a transaction open.
    fn cleanup_backends(
        &self,
        pids: &[i32],
    ) -> impl Future<Output = Result<Vec<i32>, EngineError>> + Send;

    /// Restores the database to the reference snapshot after an episode.
    fn reset(&self, record: &EpisodeRecord)
    -> impl Future<Output = Result<(), EngineError>> + Send;
}

/// Checks the state after an episode.
pub trait EpisodeOracle: Send + Sync {
    fn check(
        &self,
        record: &EpisodeRecord,
    ) -> impl Future<Output = Result<Vec<Violation>, EngineError>> + Send;
}

/// Timing and policy knobs.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// How long a granted turn may run before its lock waits are inspected.
    pub lock_probe: Duration,
    /// Hard limit for a single turn that is not waiting on another worker,
    /// and for a free worker to reach its next turn.
    pub step_timeout: Duration,
    /// Report deadlocks as violations (otherwise they are resolved like
    /// PostgreSQL would, by cancelling one participant).
    pub fail_on_deadlock: bool,
    /// Execute `BEGIN`-only turns without a scheduling decision.
    pub coalesce_begin: bool,
    /// How long abandoned workers may take to wind down.
    pub drain_timeout: Duration,
    pub stop_on_first_failure: bool,
    pub shrink: bool,
    /// Maximum re-executions spent on shrinking.
    pub shrink_budget: usize,
    /// Catalog model for partial-order reduction. Without it every step is
    /// assumed to conflict with every other one.
    pub schema: Option<Arc<SchemaModel>>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lock_probe: Duration::from_millis(2),
            step_timeout: Duration::from_millis(3000),
            fail_on_deadlock: true,
            coalesce_begin: true,
            drain_timeout: Duration::from_secs(5),
            stop_on_first_failure: true,
            shrink: true,
            shrink_budget: 32,
            schema: None,
        }
    }
}

/// Verdict of a run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RunOutcome {
    #[default]
    Pass,
    ViolationFound,
}

/// Live view of a run, shared between the coordinator and its caller: the
/// caller may request a stop and read progress while schedules execute.
#[derive(Debug, Default)]
pub struct RunControl {
    stop: AtomicBool,
    schedules: AtomicUsize,
    failures: AtomicUsize,
}

impl RunControl {
    /// Finishes the schedule in progress (including the database reset) and
    /// returns instead of starting another one.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn stop_requested(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Schedules executed so far in the current run.
    pub fn schedules(&self) -> usize {
        self.schedules.load(Ordering::Relaxed)
    }

    /// Distinct failures found so far in the current run.
    pub fn failures(&self) -> usize {
        self.failures.load(Ordering::Relaxed)
    }
}

/// A failing schedule with everything needed to report and replay it.
#[derive(Debug, Clone)]
pub struct Failure {
    pub schedule: Schedule,
    pub record: EpisodeRecord,
    pub violations: Vec<Violation>,
    /// `(preemptions, context switches)` before shrinking, if it shrank.
    pub shrunk_from: Option<(u32, u32)>,
    /// Later schedules that violated exactly the same invariants. They are
    /// counted, not shrunk or reported again.
    pub repeats: usize,
}

/// Result of [`Coordinator::run`].
#[derive(Debug, Clone, Default)]
pub struct RunReport {
    pub outcome: RunOutcome,
    pub stats: ExplorationStats,
    pub elapsed: Duration,
    /// Total lock parks across all schedules.
    pub parks: usize,
    /// Database turns the workers sent through the proxy, across all
    /// schedules. Zero means the service never used the proxy.
    pub turns: usize,
    /// The run stopped early on [`RunControl::request_stop`].
    pub interrupted: bool,
    pub failures: Vec<Failure>,
}

/// Result of a single episode.
#[derive(Debug, Clone)]
pub struct EpisodeResult {
    pub schedule: Schedule,
    pub record: EpisodeRecord,
    pub violations: Vec<Violation>,
    pub parks: usize,
    /// Partial-order reduction abandoned the schedule: every continuation
    /// was equivalent to one already explored, so it was not checked.
    pub pruned: bool,
}

/// Drives schedules through the proxy gate.
#[derive(Debug)]
pub struct Coordinator<W, D, O> {
    gate: Gate,
    workload: W,
    db: D,
    oracle: O,
    config: CoordinatorConfig,
    control: Arc<RunControl>,
}

impl<W: Workload, D: DbControl, O: EpisodeOracle> Coordinator<W, D, O> {
    pub fn new(gate: Gate, workload: W, db: D, oracle: O, config: CoordinatorConfig) -> Self {
        Self {
            gate,
            workload,
            db,
            oracle,
            config,
            control: Arc::default(),
        }
    }

    pub fn config(&self) -> &CoordinatorConfig {
        &self.config
    }

    /// Stop requests and progress of [`Coordinator::run`].
    pub fn control(&self) -> Arc<RunControl> {
        self.control.clone()
    }

    pub fn workload(&self) -> &W {
        &self.workload
    }

    /// Explores schedules until the explorer is exhausted, its budget is
    /// spent, or (by default) the first violation is found and shrunk.
    pub async fn run(&self, mut explorer: ScheduleExplorer) -> Result<RunReport, EngineError> {
        let started = Instant::now();
        let mut failures: Vec<Failure> = Vec::new();
        // Violated invariant names of each failure as first observed, before
        // shrinking could change them.
        let mut signatures: Vec<BTreeSet<String>> = Vec::new();
        let mut parks = 0;
        let mut turns = 0;
        let mut interrupted = false;
        self.control.schedules.store(0, Ordering::Relaxed);
        self.control.failures.store(0, Ordering::Relaxed);
        loop {
            if self.control.stop_requested() {
                interrupted = true;
                break;
            }
            if !explorer.next_schedule() {
                break;
            }
            let episode = self.run_episode(&mut explorer).await?;
            self.control.schedules.fetch_add(1, Ordering::Relaxed);
            parks += episode.parks;
            turns += episode.record.turns();
            if episode.pruned || episode.violations.is_empty() {
                continue;
            }
            let signature: BTreeSet<String> =
                episode.violations.iter().map(Violation::name).collect();
            if let Some(i) = signatures.iter().position(|s| *s == signature) {
                failures[i].repeats += 1;
                continue;
            }
            signatures.push(signature);
            self.control.failures.fetch_add(1, Ordering::Relaxed);
            let mut failure = Failure {
                schedule: episode.schedule,
                record: episode.record,
                violations: episode.violations,
                shrunk_from: None,
                repeats: 0,
            };
            if self.config.shrink {
                failure = self.shrink(failure).await?;
            }
            failures.push(failure);
            if self.config.stop_on_first_failure {
                break;
            }
        }
        Ok(RunReport {
            outcome: if failures.is_empty() {
                RunOutcome::Pass
            } else {
                RunOutcome::ViolationFound
            },
            stats: explorer.stats(),
            elapsed: started.elapsed(),
            parks,
            turns,
            interrupted,
            failures,
        })
    }

    /// Executes exactly the given choices (strictly), e.g. for `pgloom replay`.
    pub async fn replay(&self, choices: Vec<WorkerId>) -> Result<EpisodeResult, EngineError> {
        let mut explorer = ScheduleExplorer::new(
            Strategy::Replay {
                choices,
                lenient: false,
            },
            1,
        );
        explorer.next_schedule();
        self.run_episode(&mut explorer).await
    }

    async fn shrink(&self, failure: Failure) -> Result<Failure, EngineError> {
        let names: BTreeSet<String> = failure.violations.iter().map(Violation::name).collect();
        let before = (
            failure.schedule.preemptions(),
            failure.schedule.context_switches(),
        );
        let outcome = shrink_schedule(
            failure.schedule.clone(),
            |choices| {
                let names = &names;
                async move {
                    let mut ex = ScheduleExplorer::new(
                        Strategy::Replay {
                            choices,
                            lenient: true,
                        },
                        1,
                    );
                    ex.next_schedule();
                    match self.run_episode(&mut ex).await {
                        Ok(ep) if ep.violations.iter().any(|v| names.contains(&v.name())) => {
                            Some(ep.schedule)
                        }
                        Ok(_) => None,
                        Err(err) => {
                            debug!(%err, "shrink candidate failed to execute");
                            None
                        }
                    }
                }
            },
            self.config.shrink_budget,
        )
        .await;
        if outcome.improvements == 0 {
            return Ok(failure);
        }
        // Re-execute the minimal schedule strictly to capture its record and
        // to confirm it reproduces deterministically.
        let replayed = self.replay(outcome.schedule.choices()).await?;
        if replayed.violations.is_empty() {
            warn!("shrunk schedule did not reproduce on strict replay; keeping the original");
            return Ok(failure);
        }
        Ok(Failure {
            schedule: replayed.schedule,
            record: replayed.record,
            violations: replayed.violations,
            shrunk_from: Some(before),
            repeats: failure.repeats,
        })
    }

    /// Runs one schedule chosen by `explorer`.
    pub async fn run_episode(
        &self,
        explorer: &mut ScheduleExplorer,
    ) -> Result<EpisodeResult, EngineError> {
        let t0 = Instant::now();
        let mut ep = Episode::new(self);
        let result = ep.execute(explorer).await;
        if explorer.reduces() {
            explorer.finish_schedule(ep.pending_steps());
        }
        let t_exec = t0.elapsed();
        let deadlock = ep.violation.take();
        let pruned = ep.pruned;
        let pids = ep.tracked_pids();
        ep.wind_down().await;
        let t_drain = t0.elapsed();
        let parks = ep.parks;
        let mut record = std::mem::take(&mut ep.record);
        drop(ep);
        if let Err(err) = result {
            if let Err(e) = self.db.cleanup_backends(&pids).await {
                warn!(err = %e, "backend cleanup failed");
            }
            return Err(err);
        }

        // Leftover transactions are terminated while the invariants run: a
        // terminated backend only rolls back work the oracle cannot see.
        let check = !pruned && deadlock.is_none();
        let (terminated, checked) = tokio::join!(self.db.cleanup_backends(&pids), async {
            if check {
                self.oracle.check(&record).await.map(Some)
            } else {
                Ok(None)
            }
        });
        match terminated {
            Ok(t) if !t.is_empty() => {
                warn!(pids = ?t, "terminated backends left with open transactions");
            }
            Ok(_) => {}
            Err(err) => warn!(%err, "backend cleanup failed"),
        }
        let mut violations = checked?.unwrap_or_default();
        if let Some(v) = deadlock {
            record.deadlock = Some(v.clone());
            violations.push(v);
        }
        let t_check = t0.elapsed();
        self.db.reset(&record).await?;
        debug!(
            exec = ?t_exec,
            drain = ?(t_drain - t_exec),
            check = ?(t_check - t_drain),
            reset = ?(t0.elapsed() - t_check),
            pruned,
            "episode timing"
        );
        Ok(EpisodeResult {
            schedule: explorer.schedule(),
            record,
            violations,
            parks,
            pruned,
        })
    }
}

enum Slot {
    NotLaunched,
    /// Executing application code (the free worker).
    Running,
    AtGate(PendingTurn),
    Executing(TurnInfo),
    Parked {
        info: TurnInfo,
        blocked_by: Vec<WorkerId>,
    },
    /// A parked turn completed; its response is held until resumed.
    Completed {
        info: TurnInfo,
        finished: FinishedTurn,
    },
    Finished,
}

impl Slot {
    fn conn(&self) -> Option<ConnId> {
        match self {
            Slot::AtGate(p) => Some(p.info.conn),
            Slot::Executing(info) | Slot::Parked { info, .. } | Slot::Completed { info, .. } => {
                Some(info.conn)
            }
            _ => None,
        }
    }

    fn info(&self) -> Option<&TurnInfo> {
        match self {
            Slot::AtGate(p) => Some(&p.info),
            Slot::Executing(info) | Slot::Parked { info, .. } | Slot::Completed { info, .. } => {
                Some(info)
            }
            _ => None,
        }
    }
}

struct WorkerState {
    slot: Slot,
    tx_status: TxStatus,
    pid: Option<i32>,
    /// The worker's transaction as seen by the reduction model.
    tx: TxTrack,
    /// Footprint of the worker's latest granted turn.
    turn_fp: Footprint,
}

enum Event {
    Gate(GateEvent),
    Done(WorkerId, WorkerOutcome),
    Timeout,
}

struct Episode<'a, W, D, O> {
    co: &'a Coordinator<W, D, O>,
    events: mpsc::UnboundedReceiver<GateEvent>,
    done_tx: mpsc::UnboundedSender<(WorkerId, WorkerOutcome)>,
    done_rx: mpsc::UnboundedReceiver<(WorkerId, WorkerOutcome)>,
    tasks: JoinSet<()>,
    workers: Vec<WorkerState>,
    free: Option<WorkerId>,
    executing: Option<WorkerId>,
    graph: WaitForGraph,
    /// Connections running a coalesced `BEGIN` for the free worker.
    begin_in_flight: BTreeMap<ConnId, (WorkerId, TurnInfo)>,
    /// Connections whose turns could not be attributed and were let through.
    passthrough: BTreeSet<ConnId>,
    pid_owner: BTreeMap<i32, WorkerId>,
    record: EpisodeRecord,
    violation: Option<Violation>,
    next_preempted: bool,
    parks: usize,
    /// Footprints are tracked (partial-order reduction is on).
    reduces: bool,
    /// The explorer abandoned this schedule as redundant.
    pruned: bool,
}

impl<'a, W: Workload, D: DbControl, O: EpisodeOracle> Episode<'a, W, D, O> {
    fn new(co: &'a Coordinator<W, D, O>) -> Self {
        let n = co.workload.worker_count();
        let (done_tx, done_rx) = mpsc::unbounded_channel();
        let events = co.gate.open_episode();
        Self {
            co,
            events,
            done_tx,
            done_rx,
            tasks: JoinSet::new(),
            workers: (0..n)
                .map(|_| WorkerState {
                    slot: Slot::NotLaunched,
                    tx_status: TxStatus::Idle,
                    pid: None,
                    tx: TxTrack::default(),
                    turn_fp: Footprint::everything(),
                })
                .collect(),
            free: None,
            executing: None,
            graph: WaitForGraph::new(),
            begin_in_flight: BTreeMap::new(),
            passthrough: BTreeSet::new(),
            pid_owner: BTreeMap::new(),
            record: EpisodeRecord {
                outcomes: vec![None; n],
                backend_pids: vec![None; n],
                ..EpisodeRecord::default()
            },
            violation: None,
            next_preempted: false,
            parks: 0,
            reduces: false,
            pruned: false,
        }
    }

    fn accesses(info: &TurnInfo) -> Vec<Access> {
        info.statements
            .iter()
            .flat_map(|s| classify_access(&s.sql))
            .collect()
    }

    /// Footprint of `info` for a worker in transaction state `tx`, which is
    /// advanced past the turn.
    fn turn_footprint(&self, info: &TurnInfo, tx: &mut TxTrack) -> Footprint {
        match &self.cfg().schema {
            Some(model) => model.turn_footprint(&Self::accesses(info), tx),
            None => Footprint::everything(),
        }
    }

    /// Footprint of the step worker `w` would take if chosen now.
    fn next_footprint(&self, w: WorkerId) -> Footprint {
        let state = &self.workers[w.index()];
        match &state.slot {
            Slot::AtGate(pending) => self.turn_footprint(&pending.info, &mut state.tx.clone()),
            Slot::Completed { .. } => state.turn_fp.clone(),
            _ => Footprint::everything(),
        }
    }

    /// Next steps of the workers that could still run.
    fn pending_steps(&self) -> Vec<(WorkerId, Footprint)> {
        (0..self.workers.len())
            .map(|i| WorkerId(i as u32))
            .filter(|w| {
                matches!(
                    self.workers[w.index()].slot,
                    Slot::AtGate(_) | Slot::Completed { .. }
                )
            })
            .map(|w| (w, self.next_footprint(w)))
            .collect()
    }

    /// Records that worker `w` reached transaction status `status`.
    fn set_tx_status(&mut self, w: WorkerId, status: TxStatus) {
        let state = &mut self.workers[w.index()];
        state.tx_status = status;
        if self.reduces {
            state.tx.sync(status.in_transaction());
        }
    }

    fn cfg(&self) -> &CoordinatorConfig {
        &self.co.config
    }

    fn name(&self, w: WorkerId) -> String {
        self.co.workload.worker_name(w)
    }

    fn tracked_pids(&self) -> Vec<i32> {
        self.pid_owner.keys().copied().collect()
    }

    fn all_finished(&self) -> bool {
        self.workers
            .iter()
            .all(|w| matches!(w.slot, Slot::Finished))
    }

    async fn execute(&mut self, explorer: &mut ScheduleExplorer) -> Result<(), EngineError> {
        self.reduces = explorer.reduces();
        for i in 0..self.workers.len() {
            let w = WorkerId(i as u32);
            self.launch(w);
            self.settle().await?;
            if self.violation.is_some() {
                return Ok(());
            }
        }

        let mut last: Option<WorkerId> = None;
        loop {
            self.refresh_parked().await?;
            if self.violation.is_some() {
                return Ok(());
            }
            let enabled: Vec<WorkerId> = self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, s)| matches!(s.slot, Slot::AtGate(_) | Slot::Completed { .. }))
                .map(|(i, _)| WorkerId(i as u32))
                .collect();
            if enabled.is_empty() {
                if self.all_finished() {
                    return Ok(());
                }
                self.wait_for_progress().await?;
                continue;
            }
            let current = last.filter(|w| {
                enabled.contains(w) && self.workers[w.index()].tx_status.in_transaction()
            });
            let footprints: Vec<Footprint> = if self.reduces {
                enabled.iter().map(|w| self.next_footprint(*w)).collect()
            } else {
                Vec::new()
            };
            let Some(chosen) = explorer.choose_with(&enabled, current, &footprints)? else {
                debug!("schedule abandoned: every continuation was already covered");
                self.pruned = true;
                return Ok(());
            };
            let preempted = Decision {
                enabled: enabled.clone(),
                current,
                chosen,
            }
            .is_preemption();
            last = Some(chosen);
            trace!(?chosen, ?enabled, preempted, "decision");

            let slot = std::mem::replace(&mut self.workers[chosen.index()].slot, Slot::Running);
            match slot {
                Slot::AtGate(pending) => {
                    let info = pending.info;
                    if self.reduces {
                        let mut tx = std::mem::take(&mut self.workers[chosen.index()].tx);
                        let fp = self.turn_footprint(&info, &mut tx);
                        let state = &mut self.workers[chosen.index()];
                        state.tx = tx;
                        state.turn_fp = fp;
                    }
                    self.note_writes(&info);
                    pending.permit.grant();
                    self.workers[chosen.index()].slot = Slot::Executing(info);
                    self.executing = Some(chosen);
                    self.next_preempted = preempted;
                    self.await_executing().await?;
                }
                Slot::Completed { info, finished } => {
                    let mut step =
                        self.step(chosen, StepAction::Resume, &info, Some(&finished.response));
                    step.preempted = preempted;
                    self.record.steps.push(step);
                    self.set_tx_status(
                        chosen,
                        finished.response.tx_status.unwrap_or(TxStatus::Idle),
                    );
                    self.check_postgres_deadlock(chosen, &info, &finished.response);
                    finished.release.release();
                    self.free = Some(chosen);
                    if self.violation.is_none() {
                        self.settle().await?;
                    }
                }
                other => {
                    self.workers[chosen.index()].slot = other;
                    return Err(EngineError::Db(format!(
                        "internal: chose worker {chosen} that was not enabled"
                    )));
                }
            }
            if self.violation.is_some() {
                return Ok(());
            }
        }
    }

    fn launch(&mut self, w: WorkerId) {
        let fut = self.co.workload.launch(w);
        let tx = self.done_tx.clone();
        self.tasks.spawn(async move {
            let outcome = fut.await;
            let _ = tx.send((w, outcome));
        });
        self.workers[w.index()].slot = Slot::Running;
        self.free = Some(w);
        debug!(worker = %w, "worker launched");
    }

    async fn next_event(&mut self, deadline: tokio::time::Instant) -> Event {
        tokio::select! {
            biased;
            ev = self.events.recv() => match ev {
                Some(ev) => Event::Gate(ev),
                None => Event::Timeout,
            },
            done = self.done_rx.recv() => match done {
                Some((w, o)) => Event::Done(w, o),
                None => Event::Timeout,
            },
            () = sleep_until(deadline) => Event::Timeout,
        }
    }

    /// Waits until the free worker is held at the gate or has finished.
    async fn settle(&mut self) -> Result<(), EngineError> {
        let deadline = tokio::time::Instant::now() + self.cfg().step_timeout;
        while let Some(w) = self.free {
            match self.next_event(deadline).await {
                Event::Timeout => {
                    return Err(EngineError::WorkerStalled {
                        worker: self.name(w),
                        ms: self.cfg().step_timeout.as_millis() as u64,
                    });
                }
                ev => self.absorb(ev).await?,
            }
            if self.violation.is_some() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Waits for the executing turn, parking it if it blocks on a lock.
    async fn await_executing(&mut self) -> Result<(), EngineError> {
        let started = tokio::time::Instant::now();
        let deadline = started + self.cfg().step_timeout;
        let mut interval = self.cfg().lock_probe;
        let mut probe_at = started + interval;
        while let Some(w) = self.executing {
            match self.next_event(probe_at.min(deadline)).await {
                Event::Timeout => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        let sql = self.workers[w.index()]
                            .slot
                            .info()
                            .map(TurnInfo::sql_text)
                            .unwrap_or_default();
                        return Err(EngineError::StepTimeout {
                            worker: self.name(w),
                            sql,
                            ms: self.cfg().step_timeout.as_millis() as u64,
                        });
                    }
                    if self.probe_executing(w).await? {
                        break;
                    }
                    interval = (interval * 2).min(Duration::from_millis(50));
                    probe_at = now + interval;
                }
                ev => self.absorb(ev).await?,
            }
            if self.violation.is_some() {
                return Ok(());
            }
        }
        if self.free.is_some() && self.violation.is_none() {
            self.settle().await?;
        }
        Ok(())
    }

    /// Returns `true` if the executing worker was parked.
    async fn probe_executing(&mut self, w: WorkerId) -> Result<bool, EngineError> {
        let Some(pid) = self.workers[w.index()].pid else {
            return Ok(false);
        };
        let map = self.co.db.blocking_pids(&[pid]).await?;
        let blockers = self.worker_blockers(w, map.get(&pid));
        if blockers.is_empty() {
            return Ok(false);
        }
        // The turn may have finished while we were probing.
        if self.executing != Some(w) {
            return Ok(false);
        }
        let slot = std::mem::replace(&mut self.workers[w.index()].slot, Slot::Running);
        let Slot::Executing(info) = slot else {
            self.workers[w.index()].slot = slot;
            return Ok(false);
        };
        self.executing = None;
        self.parks += 1;
        let mut step = self.step(
            w,
            StepAction::Park {
                blocked_by: blockers.clone(),
            },
            &info,
            None,
        );
        step.preempted = std::mem::take(&mut self.next_preempted);
        self.record.steps.push(step);
        debug!(worker = %w, ?blockers, "parked on lock");
        self.graph.park(w, blockers.iter().copied());
        self.workers[w.index()].slot = Slot::Parked {
            info,
            blocked_by: blockers,
        };
        self.check_cycle(w).await?;
        Ok(true)
    }

    fn worker_blockers(&self, w: WorkerId, pids: Option<&Vec<i32>>) -> Vec<WorkerId> {
        let mut out: Vec<WorkerId> = pids
            .into_iter()
            .flatten()
            .filter_map(|p| self.pid_owner.get(p).copied())
            .filter(|b| *b != w)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Re-probes parked workers so the enabled set is settled before a decision.
    async fn refresh_parked(&mut self) -> Result<(), EngineError> {
        let deadline = tokio::time::Instant::now() + self.cfg().step_timeout;
        loop {
            let parked: Vec<(WorkerId, i32)> = self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, s)| matches!(s.slot, Slot::Parked { .. }))
                .filter_map(|(i, s)| s.pid.map(|p| (WorkerId(i as u32), p)))
                .collect();
            if parked.is_empty() {
                return Ok(());
            }
            let pids: Vec<i32> = parked.iter().map(|(_, p)| *p).collect();
            let map = self.co.db.blocking_pids(&pids).await?;
            let mut unblocked = false;
            for (w, pid) in parked {
                // Skip workers whose turn completed while we were probing.
                if !matches!(self.workers[w.index()].slot, Slot::Parked { .. }) {
                    continue;
                }
                let waiting = map.get(&pid).is_some_and(|b| !b.is_empty());
                let blockers = self.worker_blockers(w, map.get(&pid));
                if !waiting {
                    unblocked = true;
                    continue;
                }
                self.graph.park(w, blockers.iter().copied());
                if let Slot::Parked { blocked_by, .. } = &mut self.workers[w.index()].slot {
                    *blocked_by = blockers;
                }
            }
            if let Some(cycle_member) = self.graph.parked().first().copied() {
                self.check_cycle(cycle_member).await?;
                if self.violation.is_some() {
                    return Ok(());
                }
            }
            if !unblocked {
                return Ok(());
            }
            // A lock was granted: wait for that turn to report completion.
            let probe = tokio::time::Instant::now() + self.cfg().lock_probe;
            match self.next_event(probe.min(deadline)).await {
                Event::Timeout if tokio::time::Instant::now() >= deadline => {
                    return Err(self.stalled());
                }
                Event::Timeout => {}
                ev => self.absorb(ev).await?,
            }
        }
    }

    /// Nothing is enabled but workers remain: wait for any progress.
    async fn wait_for_progress(&mut self) -> Result<(), EngineError> {
        let deadline = tokio::time::Instant::now() + self.cfg().step_timeout;
        let probe = tokio::time::Instant::now() + self.cfg().lock_probe * 4;
        match self.next_event(probe.min(deadline)).await {
            Event::Timeout if tokio::time::Instant::now() >= deadline => Err(self.stalled()),
            Event::Timeout => Ok(()),
            ev => self.absorb(ev).await,
        }
    }

    fn stalled(&self) -> EngineError {
        EngineError::Stalled {
            workers: self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, s)| matches!(s.slot, Slot::Parked { .. }))
                .map(|(i, _)| self.name(WorkerId(i as u32)))
                .collect(),
            ms: self.cfg().step_timeout.as_millis() as u64,
        }
    }

    async fn check_cycle(&mut self, _hint: WorkerId) -> Result<(), EngineError> {
        let Some(cycle) = self.graph.detect_cycle() else {
            return Ok(());
        };
        let statements: Vec<(WorkerId, String)> = cycle
            .iter()
            .map(|w| {
                let sql = self.workers[w.index()]
                    .slot
                    .info()
                    .map(TurnInfo::sql_text)
                    .unwrap_or_default();
                (*w, sql)
            })
            .collect();
        // Break the cycle the way PostgreSQL would: abort the most recent
        // waiter, so no session sits in a lock wait until deadlock_timeout.
        let victim = self
            .record
            .steps
            .iter()
            .rev()
            .find(|s| matches!(s.action, StepAction::Park { .. }) && cycle.contains(&s.worker))
            .map(|s| s.worker)
            .unwrap_or(cycle[cycle.len() - 1]);
        if let Some(pid) = self.workers[victim.index()].pid {
            self.co.db.cancel_backend(pid).await?;
        }
        self.graph.unpark(victim);
        if self.cfg().fail_on_deadlock {
            debug!(?cycle, "deadlock proven by wait-for graph");
            self.violation = Some(Violation::Deadlock {
                cycle,
                source: DeadlockSource::WaitForGraph,
                statements,
            });
        }
        Ok(())
    }

    fn check_postgres_deadlock(&mut self, w: WorkerId, info: &TurnInfo, response: &TurnResponse) {
        if !response.is_deadlock() || !self.cfg().fail_on_deadlock || self.violation.is_some() {
            return;
        }
        let mut cycle = vec![w];
        cycle.extend(self.graph.blockers_of(w));
        cycle.sort_unstable();
        cycle.dedup();
        self.violation = Some(Violation::Deadlock {
            statements: cycle
                .iter()
                .map(|c| {
                    let sql = if *c == w {
                        info.sql_text()
                    } else {
                        self.workers[c.index()]
                            .slot
                            .info()
                            .map(TurnInfo::sql_text)
                            .unwrap_or_default()
                    };
                    (*c, sql)
                })
                .collect(),
            cycle,
            source: DeadlockSource::Postgres,
        });
    }

    fn note_writes(&mut self, info: &TurnInfo) {
        self.record.dirty_tables.extend(info.meta.write_tables());
        self.record.opaque_writes |= info.meta.has_opaque_write();
    }

    fn step(
        &self,
        w: WorkerId,
        action: StepAction,
        info: &TurnInfo,
        response: Option<&TurnResponse>,
    ) -> RecordedStep {
        let mut step = RecordedStep::new(self.record.steps.len() as u32 + 1, w, action)
            .with_statements(&info.statements);
        step.backend_pid = info.backend_pid.map(|p| p.0);
        step.meta = Some(info.meta.clone());
        if let Some(r) = response {
            step.tx_status = r.tx_status;
            step.result = Some(r.summary()).filter(|s| !s.is_empty());
            step.sqlstate = r.sqlstate().map(str::to_owned);
        }
        step
    }

    fn owner_of_conn(&self, conn: ConnId) -> Option<WorkerId> {
        self.workers
            .iter()
            .position(|s| s.slot.conn() == Some(conn))
            .map(|i| WorkerId(i as u32))
    }

    async fn absorb(&mut self, ev: Event) -> Result<(), EngineError> {
        match ev {
            Event::Timeout => {}
            Event::Done(w, outcome) => self.on_done(w, outcome),
            Event::Gate(GateEvent::TurnReady(pending)) => self.on_ready(pending),
            Event::Gate(GateEvent::TurnFinished(finished)) => self.on_finished(finished),
            Event::Gate(GateEvent::ConnectionClosed { conn, .. }) => self.on_closed(conn),
        }
        Ok(())
    }

    fn on_done(&mut self, w: WorkerId, outcome: WorkerOutcome) {
        let Some(state) = self.workers.get_mut(w.index()) else {
            return;
        };
        let previous = std::mem::replace(&mut state.slot, Slot::Finished);
        if !matches!(previous, Slot::Running) {
            warn!(worker = %w, "worker finished while holding a pending turn");
        }
        self.graph.unpark(w);
        if self.free == Some(w) {
            self.free = None;
        }
        if self.executing == Some(w) {
            self.executing = None;
        }
        let mut step = RecordedStep::new(
            self.record.steps.len() as u32 + 1,
            w,
            StepAction::Done {
                outcome: outcome.clone(),
            },
        );
        step.preempted = false;
        self.record.steps.push(step);
        self.record.outcomes[w.index()] = Some(outcome);
    }

    fn on_ready(&mut self, pending: PendingTurn) {
        let Some(w) = self.free else {
            warn!(
                conn = pending.info.conn,
                sql = %pending.info.sql_text(),
                "turn from a connection outside the scheduled workers; letting it through"
            );
            self.note_writes(&pending.info);
            self.passthrough.insert(pending.info.conn);
            pending.permit.grant();
            return;
        };
        if let Some(pid) = pending.info.backend_pid {
            self.pid_owner.insert(pid.0, w);
            self.workers[w.index()].pid = Some(pid.0);
            self.record.backend_pids[w.index()] = Some(pid.0);
        }
        if self.cfg().coalesce_begin && pending.info.meta.is_begin_only() {
            let info = pending.info.clone();
            if self.reduces {
                let mut tx = std::mem::take(&mut self.workers[w.index()].tx);
                self.turn_footprint(&info, &mut tx);
                self.workers[w.index()].tx = tx;
            }
            self.begin_in_flight.insert(info.conn, (w, info));
            pending.permit.grant();
            return;
        }
        trace!(worker = %w, sql = %pending.info.sql_text(), "turn held");
        self.workers[w.index()].slot = Slot::AtGate(pending);
        self.free = None;
    }

    fn on_finished(&mut self, finished: FinishedTurn) {
        let conn = finished.conn;
        if let Some((w, info)) = self.begin_in_flight.remove(&conn) {
            let step = self.step(w, StepAction::Begin, &info, Some(&finished.response));
            self.record.steps.push(step);
            self.set_tx_status(w, finished.response.tx_status.unwrap_or(TxStatus::Idle));
            finished.release.release();
            return;
        }
        if self.passthrough.remove(&conn) {
            finished.release.release();
            return;
        }
        let Some(w) = self.owner_of_conn(conn) else {
            warn!(conn, "finished turn for an unknown connection");
            finished.release.release();
            return;
        };
        let slot = std::mem::replace(&mut self.workers[w.index()].slot, Slot::Running);
        match slot {
            Slot::Executing(info) => {
                let mut step = self.step(w, StepAction::Execute, &info, Some(&finished.response));
                step.preempted = std::mem::take(&mut self.next_preempted);
                self.record.steps.push(step);
                self.set_tx_status(w, finished.response.tx_status.unwrap_or(TxStatus::Idle));
                self.check_postgres_deadlock(w, &info, &finished.response);
                self.executing = None;
                finished.release.release();
                self.free = Some(w);
            }
            Slot::Parked { info, .. } => {
                self.graph.unpark(w);
                self.workers[w.index()].slot = Slot::Completed { info, finished };
            }
            other => {
                warn!(worker = %w, "unexpected turn completion");
                self.workers[w.index()].slot = other;
                finished.release.release();
            }
        }
    }

    fn on_closed(&mut self, conn: ConnId) {
        self.begin_in_flight.remove(&conn);
        self.passthrough.remove(&conn);
        let Some(w) = self.owner_of_conn(conn) else {
            return;
        };
        let slot = std::mem::replace(&mut self.workers[w.index()].slot, Slot::Running);
        match slot {
            Slot::Executing(info) | Slot::Parked { info, .. } => {
                let mut step = self.step(w, StepAction::Execute, &info, None);
                step.result = Some("connection closed".into());
                self.record.steps.push(step);
                self.graph.unpark(w);
                if self.executing == Some(w) {
                    self.executing = None;
                }
                self.set_tx_status(w, TxStatus::Idle);
                if self.free.is_none() {
                    self.free = Some(w);
                }
            }
            Slot::AtGate(_) | Slot::Completed { .. } => {
                self.set_tx_status(w, TxStatus::Idle);
                if self.free.is_none() {
                    self.free = Some(w);
                }
            }
            other => self.workers[w.index()].slot = other,
        }
    }

    /// Releases everything and lets the workers finish unsupervised.
    async fn wind_down(&mut self) {
        self.co.gate.close_episode();
        self.events.close();
        while let Ok(ev) = self.events.try_recv() {
            drop(ev);
        }
        for state in &mut self.workers {
            if !matches!(state.slot, Slot::Finished | Slot::NotLaunched) {
                // Dropping permits and releases lets held turns proceed.
                state.slot = Slot::Running;
            }
        }
        self.begin_in_flight.clear();
        let deadline = tokio::time::Instant::now() + self.cfg().drain_timeout;
        while !self.tasks.is_empty() {
            tokio::select! {
                done = self.done_rx.recv() => {
                    if let Some((w, outcome)) = done
                        && self.record.outcomes[w.index()].is_none()
                    {
                        self.record.outcomes[w.index()] = Some(outcome);
                    }
                }
                joined = self.tasks.join_next() => {
                    if joined.is_none() {
                        break;
                    }
                }
                () = sleep_until(deadline) => {
                    warn!("workers did not finish after the episode was abandoned; aborting them");
                    self.tasks.abort_all();
                    break;
                }
            }
        }
        while let Ok((w, outcome)) = self.done_rx.try_recv() {
            if self.record.outcomes[w.index()].is_none() {
                self.record.outcomes[w.index()] = Some(outcome);
            }
        }
    }
}

/// [`DbControl`] backed by a real PostgreSQL control connection.
pub mod pg {
    use std::collections::BTreeMap;

    use pgloom_pgctrl::{PgControl, Snapshot};

    use super::{DbControl, EngineError};
    use crate::record::EpisodeRecord;

    /// How the database is reset between schedules.
    #[derive(Debug)]
    pub enum ResetStrategy {
        /// Restore only tables written during the episode from `_pgloom_snap`.
        Snapshot(Snapshot),
        /// Run a user-provided script.
        Sql(String),
    }

    #[derive(Debug)]
    pub struct PgDbControl {
        pub ctl: PgControl,
        pub reset: ResetStrategy,
    }

    #[allow(clippy::needless_pass_by_value)] // used as `map_err(db_err)`
    fn db_err(err: pgloom_pgctrl::PgCtrlError) -> EngineError {
        EngineError::Db(err.to_string())
    }

    impl DbControl for PgDbControl {
        async fn blocking_pids(
            &self,
            pids: &[i32],
        ) -> Result<BTreeMap<i32, Vec<i32>>, EngineError> {
            let map = self.ctl.blocking_pids(pids).await.map_err(db_err)?;
            Ok(map.into_iter().map(|(k, v)| (k, v.blocked_by)).collect())
        }

        async fn cancel_backend(&self, pid: i32) -> Result<(), EngineError> {
            self.ctl
                .cancel_backend(pid)
                .await
                .map(|_| ())
                .map_err(db_err)
        }

        async fn cleanup_backends(&self, pids: &[i32]) -> Result<Vec<i32>, EngineError> {
            self.ctl
                .terminate_open_transactions(pids)
                .await
                .map_err(db_err)
        }

        async fn reset(&self, record: &EpisodeRecord) -> Result<(), EngineError> {
            match &self.reset {
                ResetStrategy::Snapshot(snapshot) => {
                    let stats = snapshot
                        .restore(&self.ctl, &record.dirty_tables, record.opaque_writes)
                        .await
                        .map_err(db_err)?;
                    if !stats.unresolved.is_empty() {
                        tracing::debug!(tables = ?stats.unresolved, "written tables not covered by the snapshot");
                    }
                    Ok(())
                }
                ResetStrategy::Sql(sql) => self
                    .ctl
                    .batch(sql, "running reset_sql")
                    .await
                    .map_err(db_err),
            }
        }
    }
}
