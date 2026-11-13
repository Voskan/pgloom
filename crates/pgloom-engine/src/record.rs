//! Execution records shared by the coordinator, the reporter and replay files.

use std::collections::BTreeSet;

use pgloom_wire::{BoundStatement, TurnMeta, TxStatus};
use serde::{Deserialize, Serialize};

/// Index of a worker in launch order; stable across runs of a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(pub u32);

impl WorkerId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "W{}", self.0)
    }
}

/// What happened at a step of the timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepAction {
    /// A scheduled turn ran to completion.
    Execute,
    /// `BEGIN` executed without a scheduling decision (it takes no snapshot).
    Begin,
    /// The turn is waiting on a lock held by other workers.
    Park { blocked_by: Vec<WorkerId> },
    /// A parked turn completed after its blockers released their locks; the
    /// worker resumed.
    Resume,
    /// The worker finished (the HTTP handler returned, the command exited, ...).
    Done { outcome: WorkerOutcome },
}

/// One row of the execution timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedStep {
    pub step: u32,
    pub worker: WorkerId,
    #[serde(flatten)]
    pub action: StepAction,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_status: Option<TxStatus>,
    /// Response summary such as `returns 100` or `UPDATE 1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_pid: Option<i32>,
    /// This step was reached by preempting another worker mid-transaction.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub preempted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<TurnMeta>,
}

impl RecordedStep {
    pub fn new(step: u32, worker: WorkerId, action: StepAction) -> Self {
        Self {
            step,
            worker,
            action,
            sql: String::new(),
            params: Vec::new(),
            tx_status: None,
            result: None,
            sqlstate: None,
            backend_pid: None,
            preempted: false,
            meta: None,
        }
    }

    pub fn with_statements(mut self, statements: &[BoundStatement]) -> Self {
        self.sql = statements
            .iter()
            .map(|s| s.sql.trim().trim_end_matches(';').trim())
            .collect::<Vec<_>>()
            .join("; ");
        self.params = statements.iter().flat_map(|s| s.params.clone()).collect();
        self
    }
}

/// How a worker's trigger (HTTP request, SQL script, command) ended.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerOutcome {
    /// Counts towards `max_2xx_count`: HTTP 2xx, exit code 0 or an
    /// error-free SQL script.
    pub success: bool,
    /// HTTP status or process exit code, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<i32>,
    /// Short label such as `HTTP 200 OK` or `exit 1`.
    pub label: String,
    /// Response body excerpt or error message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The complete (size-capped) HTTP response body, for response
    /// invariants. Not recorded in repro files.
    #[serde(skip)]
    pub body: Option<String>,
}

impl WorkerOutcome {
    pub fn failed(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            success: false,
            status: None,
            label: label.into(),
            detail: Some(detail.into()),
            body: None,
        }
    }
}

/// Where a deadlock was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadlockSource {
    /// A cycle in PgLoom's wait-for graph, before PostgreSQL noticed.
    WaitForGraph,
    /// PostgreSQL aborted a transaction with SQLSTATE `40P01`.
    Postgres,
}

/// A detected anomaly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Violation {
    /// A SQL or response invariant failed after all workers finished.
    InvariantFailed {
        name: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        description: String,
        /// Observed state, e.g. `id=1, balance=-100`.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        observed: String,
    },
    /// Workers wait on each other's locks.
    Deadlock {
        cycle: Vec<WorkerId>,
        source: DeadlockSource,
        /// The statement each cycle member was blocked in.
        statements: Vec<(WorkerId, String)>,
    },
}

impl Violation {
    pub fn name(&self) -> String {
        match self {
            Violation::InvariantFailed { name, .. } => name.clone(),
            Violation::Deadlock { .. } => "deadlock".into(),
        }
    }

    pub fn is_deadlock(&self) -> bool {
        matches!(self, Violation::Deadlock { .. })
    }
}

/// Everything observed while executing one schedule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeRecord {
    pub steps: Vec<RecordedStep>,
    /// Outcome per worker, indexed by `WorkerId`.
    pub outcomes: Vec<Option<WorkerOutcome>>,
    /// Backend pid used by each worker's last scheduled turn.
    pub backend_pids: Vec<Option<i32>>,
    /// Tables written during the episode (as spelled in SQL).
    pub dirty_tables: BTreeSet<String>,
    /// A write whose targets could not be determined was observed.
    pub opaque_writes: bool,
    pub deadlock: Option<Violation>,
}

impl EpisodeRecord {
    pub fn success_count(&self) -> usize {
        self.outcomes.iter().flatten().filter(|o| o.success).count()
    }
}
