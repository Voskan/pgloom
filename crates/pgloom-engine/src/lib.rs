//! PgLoom's scheduling core.
//!
//! * [`explore`] enumerates schedules deterministically (exhaustive, CHESS
//!   iterative preemption bounding, seeded random, replay), optionally with
//!   dynamic partial-order reduction.
//! * [`por`] decides which scheduling steps commute.
//! * [`waitfor`] tracks workers parked on database locks and proves deadlocks.
//! * [`shrink`] minimises failing schedules.
//! * [`coordinator`] drives real episodes through the gating proxy.

#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod coordinator;
pub mod explore;
pub mod por;
pub mod record;
pub mod shrink;
pub mod waitfor;

pub use coordinator::{
    Coordinator, CoordinatorConfig, DbControl, EngineError, EpisodeOracle, EpisodeResult, Failure,
    RunOutcome, RunReport, Workload,
};
pub use explore::{
    Decision, Divergence, ExplorationStats, Reduction, Schedule, ScheduleExplorer, Strategy,
    default_choice,
};
pub use por::{Footprint, SchemaModel, TxTrack};
pub use record::{
    DeadlockSource, EpisodeRecord, RecordedStep, StepAction, Violation, WorkerId, WorkerOutcome,
};
pub use shrink::{ShrinkOutcome, shrink_schedule};
pub use waitfor::WaitForGraph;
