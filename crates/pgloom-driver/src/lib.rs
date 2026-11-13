//! Scenario configuration (`pgloom.yaml`) and the concurrent workers whose
//! database traffic PgLoom schedules: HTTP requests, SQL scripts sent through
//! the proxy, and arbitrary commands.

#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod config;
pub mod env;
pub mod invariants;
pub mod worker;

pub use config::{
    ApplyTo, Config, ConfigError, DEFAULT_PROXY_LISTEN, DatabaseConfig, ExecWorker,
    ExplorationConfig, HttpWorker, InvariantsConfig, JsonCheck, LoadedConfig, ReductionName,
    ResponseInvariant, SqlInvariantConfig, StrategyName, TargetConfig, WorkerConfig,
};
pub use invariants::check_responses;
pub use worker::{DriverError, ScenarioWorkload, WorkerSpec, proxied_env};
