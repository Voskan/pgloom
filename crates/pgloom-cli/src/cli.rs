//! Command-line interface definition.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use pgloom_driver::{ReductionName, StrategyName};

#[derive(Debug, Parser)]
#[command(
    name = "pgloom",
    version,
    about = "Deterministic interleaving model checker for PostgreSQL-backed services",
    long_about = "PgLoom sits between your service and PostgreSQL as a wire-protocol proxy, \
                  replays concurrent requests under systematically chosen interleavings and \
                  reports double spends, lost updates, invariant violations and deadlocks \
                  together with a deterministic replay file.",
    after_help = "Exit codes: 0 pass, 1 violation found, 2 configuration or environment error, 3 internal error."
)]
pub struct Cli {
    /// When to use colors in the report.
    #[arg(long, global = true, value_enum, default_value_t = ColorChoice::Auto)]
    pub color: ColorChoice,

    /// Print only the final verdict and errors.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Increase log verbosity (-v debug, -vv trace); RUST_LOG takes precedence.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Explore interleavings of a scenario and report anomalies.
    Run(RunArgs),
    /// Re-execute the schedule recorded in a repro file.
    Replay(ReplayArgs),
    /// Two-act showcase: find a double spend, then verify the fix.
    Demo(DemoArgs),
    /// Write a commented starter pgloom.yaml.
    Init(InitArgs),
    /// Check database permissions, latency and the proxy port.
    Doctor(DoctorArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ColorChoice {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Pretty,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StrategyArg {
    Exhaustive,
    Bounded,
    Random,
}

impl From<StrategyArg> for StrategyName {
    fn from(s: StrategyArg) -> Self {
        match s {
            StrategyArg::Exhaustive => StrategyName::Exhaustive,
            StrategyArg::Bounded => StrategyName::Bounded,
            StrategyArg::Random => StrategyName::Random,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReductionArg {
    None,
    Dpor,
}

impl From<ReductionArg> for ReductionName {
    fn from(r: ReductionArg) -> Self {
        match r {
            ReductionArg::None => ReductionName::None,
            ReductionArg::Dpor => ReductionName::Dpor,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct ConnectionArgs {
    /// URL of the real PostgreSQL server (overrides database.upstream).
    #[arg(long, env = "PGLOOM_UPSTREAM_URL", value_name = "POSTGRES_URL")]
    pub upstream: Option<String>,

    /// Address of the PgLoom proxy (overrides database.proxy_listen).
    #[arg(long, value_name = "HOST:PORT")]
    pub listen: Option<String>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Scenario file.
    #[arg(default_value = "pgloom.yaml")]
    pub config: PathBuf,

    #[command(flatten)]
    pub connection: ConnectionArgs,

    #[arg(long, value_enum)]
    pub strategy: Option<StrategyArg>,

    /// Preemption bound per schedule for the bounded strategy.
    #[arg(long, value_name = "N")]
    pub max_preemptions: Option<u32>,

    /// Upper limit of schedules to execute.
    #[arg(long, value_name = "N")]
    pub max_schedules: Option<usize>,

    /// Seed of the random strategy.
    #[arg(long)]
    pub seed: Option<u64>,

    /// Partial-order reduction for the exhaustive strategy (default: dpor).
    /// Use `none` if the service shares in-memory state between requests.
    #[arg(long, value_enum)]
    pub reduction: Option<ReductionArg>,

    /// Stop at the first violation (default).
    #[arg(long, conflicts_with = "find_all")]
    pub stop_on_first_failure: bool,

    /// Keep exploring after a violation and report every failing schedule.
    #[arg(long)]
    pub find_all: bool,

    /// Where to write the replay file when a violation is found.
    #[arg(long, value_name = "PATH", default_value = pgloom_report::DEFAULT_REPRO_PATH)]
    pub repro_out: PathBuf,

    #[arg(long, value_enum, default_value_t = Format::Pretty)]
    pub format: Format,

    /// Report the failing schedule as found, without minimizing it.
    #[arg(long)]
    pub no_shrink: bool,
}

#[derive(Debug, Args)]
pub struct ReplayArgs {
    /// Repro file written by `pgloom run`.
    #[arg(default_value = pgloom_report::DEFAULT_REPRO_PATH)]
    pub repro: PathBuf,

    #[command(flatten)]
    pub connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub struct DemoArgs {
    /// PostgreSQL to use; otherwise localhost:5432 or a throwaway Docker container.
    #[arg(long, value_name = "POSTGRES_URL")]
    pub upstream: Option<String>,
}

#[derive(Debug, Args)]
pub struct InitArgs {
    /// Where to write the scenario.
    #[arg(default_value = "pgloom.yaml")]
    pub path: PathBuf,

    /// Overwrite an existing file.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Scenario file whose database and proxy settings are checked.
    #[arg(default_value = "pgloom.yaml")]
    pub config: PathBuf,

    #[command(flatten)]
    pub connection: ConnectionArgs,
}
