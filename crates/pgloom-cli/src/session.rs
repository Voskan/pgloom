//! A scenario session: database preparation, the proxy, the target service
//! and the coordinator, torn down in reverse order.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use std::sync::Arc;

use pgloom_driver::{Config, ReductionName, ScenarioWorkload, StrategyName, proxied_env};
use pgloom_engine::coordinator::pg::{PgDbControl, ResetStrategy};
use pgloom_engine::{
    Coordinator, CoordinatorConfig, EpisodeResult, Reduction, RunControl, RunReport,
    ScheduleExplorer, SchemaModel, Strategy, WorkerId,
};
use pgloom_pgctrl::{PgControl, PgUrl, Snapshot, SnapshotOptions, SqlInvariant};
use pgloom_wire::{ProxyConfig, ProxyHandle, spawn_proxy};
use tracing::{debug, warn};

use crate::error::CliError;
use crate::interrupt;
use crate::oracle::ScenarioOracle;
use crate::target::{TargetProcess, answers, wait_http};

/// Command-line overrides of the scenario file.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub upstream: Option<String>,
    pub listen: Option<String>,
    pub strategy: Option<StrategyName>,
    pub max_preemptions: Option<u32>,
    pub max_schedules: Option<usize>,
    pub seed: Option<u64>,
    pub reduction: Option<ReductionName>,
}

impl Overrides {
    pub fn apply(&self, config: &mut Config) -> Result<(), CliError> {
        if let Some(u) = &self.upstream {
            config.database.upstream.clone_from(u);
        }
        if let Some(l) = &self.listen {
            config.database.proxy_listen.clone_from(l);
        }
        let e = &mut config.exploration;
        if let Some(s) = self.strategy {
            e.strategy = s;
        }
        if let Some(p) = self.max_preemptions {
            e.max_preemptions = p;
        }
        if let Some(m) = self.max_schedules {
            e.max_schedules = m;
        }
        if self.seed.is_some() {
            e.seed = self.seed;
        }
        if self.reduction.is_some() {
            e.reduction = self.reduction;
        }
        let problems = config.problems();
        if problems.is_empty() {
            Ok(())
        } else {
            Err(CliError::Config(format!(
                "invalid settings:\n{}",
                problems
                    .iter()
                    .map(|p| format!("  - {p}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            )))
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RunOptions {
    pub stop_on_first_failure: bool,
    pub shrink: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            stop_on_first_failure: true,
            shrink: true,
        }
    }
}

pub fn strategy_of(config: &Config) -> Strategy {
    let e = &config.exploration;
    match e.strategy {
        StrategyName::Exhaustive => Strategy::Exhaustive,
        StrategyName::Bounded => Strategy::Bounded {
            max_preemptions: e.max_preemptions,
            extend_beyond_bound: true,
        },
        StrategyName::Random => Strategy::Random {
            seed: e.seed.unwrap_or(0x5EED),
        },
    }
}

#[derive(Debug)]
pub struct Session {
    config: Config,
    workers: Vec<String>,
    upstream: PgUrl,
    proxy: Option<ProxyHandle>,
    proxy_url: String,
    ctl: PgControl,
    snapshot: bool,
    reduction: Reduction,
    target: std::sync::Mutex<Option<TargetProcess>>,
    coordinator: Coordinator<ScenarioWorkload, PgDbControl, ScenarioOracle>,
}

impl Session {
    /// Prepares the database, starts the proxy and the target service, then
    /// captures the reference state.
    ///
    /// The snapshot and the catalog are read only once the target is ready:
    /// services that migrate on startup create tables PgLoom must restore
    /// between schedules and reason about for partial-order reduction.
    pub async fn start(config: Config, options: RunOptions) -> Result<Self, CliError> {
        tokio::select! {
            session = Self::prepare(config, options) => session,
            () = interrupt::wait() => Err(CliError::Interrupted),
        }
    }

    async fn prepare(config: Config, options: RunOptions) -> Result<Self, CliError> {
        let upstream = PgUrl::parse(&config.database.upstream)
            .map_err(|e| CliError::Config(format!("database.upstream: {e}")))?;
        let url = config.database.upstream.as_str();
        let ctl = PgControl::connect(url)
            .await
            .map_err(|e| CliError::env("cannot connect to the upstream database", e))?;
        if let Some(setup) = &config.database.setup_sql {
            ctl.batch(setup, "running database.setup_sql")
                .await
                .map_err(|e| CliError::Config(e.to_string()))?;
        }

        let listen: SocketAddr = config.database.proxy_listen.parse().map_err(|_| {
            CliError::Config(format!(
                "database.proxy_listen: `{}` is not a socket address",
                config.database.proxy_listen
            ))
        })?;
        let proxy = spawn_proxy(ProxyConfig {
            listen,
            upstream: upstream.socket_addr(),
        })
        .await
        .map_err(|e| CliError::env(&format!("cannot listen on {listen}"), e))?;
        let local = proxy.local_addr();
        let host = if local.ip().is_unspecified() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            local.ip()
        };
        let host = match host {
            IpAddr::V4(v4) => v4.to_string(),
            IpAddr::V6(v6) => format!("[{v6}]"),
        };
        let proxy_url = upstream
            .with_host_port(&host, local.port())
            .with_ssl_disabled()
            .to_string();
        debug!(%local, "proxy listening");

        let mut target = None;
        if let Some(t) = &config.target {
            if let (Some(_), Some(ready)) = (&t.command, &t.ready_http)
                && answers(ready).await
            {
                return Err(CliError::Environment(format!(
                    "{ready} already answers before target.command was started: another \
                     instance of the service (or another program) is listening there and \
                     would receive the workers' requests instead. Stop it, or remove \
                     target.command to test the service that is already running"
                )));
            }
            if let Some(cmd) = &t.command {
                let env = proxied_env(&t.env, &proxy_url);
                target = Some(TargetProcess::spawn(cmd, &env, t.workdir.as_deref())?);
            }
            if let Some(ready) = &t.ready_http {
                let timeout = Duration::from_millis(t.startup_timeout_ms);
                match target.as_mut() {
                    Some(p) => p.wait_ready(ready, timeout).await?,
                    None => wait_http(ready, timeout).await?,
                }
            }
        }

        let workers: Vec<String> = config.workers.iter().map(|w| w.id.clone()).collect();
        let workload = ScenarioWorkload::new(&config.workers, proxy_url.clone())
            .map_err(|e| CliError::Config(e.to_string()))?;
        let connect = || async {
            PgControl::connect(url)
                .await
                .map_err(|e| CliError::env("cannot connect to the upstream database", e))
        };
        let (db_ctl, oracle_ctl) = (connect().await?, connect().await?);

        // Last fallible step: nothing after it can leave the snapshot behind.
        let (reset, snapshot, schema) = Self::capture_reference(&config, &ctl).await?;
        let reduction = if schema.is_some() {
            Reduction::Dpor
        } else {
            Reduction::None
        };
        let db = PgDbControl { ctl: db_ctl, reset };
        let oracle = ScenarioOracle {
            ctl: oracle_ctl,
            sql: config
                .invariants
                .sql
                .iter()
                .map(|i| SqlInvariant {
                    name: i.name.clone(),
                    description: i.description.clone(),
                    assert_true: i.assert_true.clone(),
                    diagnostic_query: i.diagnostic_query.clone(),
                })
                .collect(),
            responses: config.invariants.responses.clone(),
            workers: workers.clone(),
        };
        let e = &config.exploration;
        let cfg = CoordinatorConfig {
            lock_probe: Duration::from_millis(e.lock_probe_ms),
            step_timeout: Duration::from_millis(e.step_timeout_ms),
            fail_on_deadlock: config.invariants.fail_on_deadlock,
            stop_on_first_failure: options.stop_on_first_failure,
            shrink: options.shrink,
            schema,
            ..CoordinatorConfig::default()
        };
        let coordinator = Coordinator::new(proxy.gate().clone(), workload, db, oracle, cfg);
        Ok(Self {
            config,
            workers,
            upstream,
            proxy: Some(proxy),
            proxy_url,
            ctl,
            snapshot,
            reduction,
            target: std::sync::Mutex::new(target),
            coordinator,
        })
    }

    /// Runs `seed_sql`, snapshots the database (unless `reset_sql` replaces
    /// the snapshot) and reads the catalog for partial-order reduction.
    async fn capture_reference(
        config: &Config,
        ctl: &PgControl,
    ) -> Result<(ResetStrategy, bool, Option<Arc<SchemaModel>>), CliError> {
        if let Some(seed) = &config.database.seed_sql {
            ctl.batch(seed, "running database.seed_sql")
                .await
                .map_err(|e| CliError::Config(e.to_string()))?;
        }
        let (reset, snapshot) = match &config.database.reset_sql {
            Some(sql) => (ResetStrategy::Sql(sql.clone()), false),
            None => {
                let options = SnapshotOptions {
                    exclude: config.database.snapshot_exclude.clone(),
                };
                let snap = Snapshot::capture(ctl, &options)
                    .await
                    .map_err(|e| CliError::env("cannot snapshot the database", e))?;
                (ResetStrategy::Snapshot(snap), true)
            }
        };
        let schema = match config.exploration.effective_reduction() {
            ReductionName::None => None,
            ReductionName::Dpor => match ctl.schema_catalog().await {
                Ok(catalog) => Some(Arc::new(SchemaModel::from_catalog(&catalog))),
                Err(e) => {
                    warn!("partial-order reduction disabled: cannot read the catalog: {e}");
                    None
                }
            },
        };
        Ok((reset, snapshot, schema))
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn workers(&self) -> &[String] {
        &self.workers
    }

    pub fn proxy_url(&self) -> &str {
        &self.proxy_url
    }

    pub fn proxy_addr(&self) -> Option<SocketAddr> {
        self.proxy.as_ref().map(ProxyHandle::local_addr)
    }

    pub fn upstream(&self) -> String {
        self.upstream.redacted()
    }

    /// Stop requests and live progress of [`Session::run`].
    pub fn control(&self) -> Arc<RunControl> {
        self.coordinator.control()
    }

    /// Explores schedules as configured. Ctrl+C ends the exploration after
    /// the schedule in progress, with the database restored.
    pub async fn run(&self) -> Result<RunReport, CliError> {
        let explorer = ScheduleExplorer::new(
            strategy_of(&self.config),
            self.config.exploration.max_schedules,
        )
        .with_reduction(self.reduction);
        let control = self.coordinator.control();
        let watcher = tokio::spawn(async move {
            interrupt::wait().await;
            control.request_stop();
        });
        let report = self.coordinator.run(explorer).await;
        watcher.abort();
        let report = report?;
        self.check_target().await?;
        if report.turns == 0 && !report.interrupted {
            return Err(self.no_traffic());
        }
        Ok(report)
    }

    /// A target that exits mid-run leaves the workers talking to nothing,
    /// so whatever was explored cannot be trusted.
    async fn check_target(&self) -> Result<(), CliError> {
        let exited = {
            let mut guard = self
                .target
                .lock()
                .map_err(|_| CliError::Internal("target state poisoned".into()))?;
            guard
                .as_mut()
                .and_then(|t| t.exit_status().map(|s| (s, t.log_handle())))
        };
        match exited {
            Some((status, log)) => {
                let tail = log.lock().await.join("\n");
                let output = if tail.is_empty() {
                    String::new()
                } else {
                    format!("\ntarget output:\n{tail}")
                };
                Err(CliError::Environment(format!(
                    "the target service exited with {status} during the run, so the explored \
                     schedules are not reliable{output}"
                )))
            }
            None => Ok(()),
        }
    }

    /// Without a single turn through the proxy nothing was explored, and a
    /// pass would be meaningless.
    fn no_traffic(&self) -> CliError {
        let proxy = self.proxy_addr().map_or_else(
            || self.config.database.proxy_listen.clone(),
            |a| a.to_string(),
        );
        CliError::Config(format!(
            "no SQL from the workers reached the PgLoom proxy on {proxy}, so nothing was \
             explored. Point the service's DATABASE_URL at the proxy (target.env sets it for a \
             service PgLoom starts), and make sure no other instance of the service answers \
             the worker URLs"
        ))
    }

    /// Re-executes recorded choices. Decisions that are no longer possible
    /// (because the service changed, e.g. a lock was added) fall back to the
    /// default continuation instead of aborting.
    pub async fn replay(&self, choices: Vec<WorkerId>) -> Result<EpisodeResult, CliError> {
        let mut explorer = ScheduleExplorer::new(
            Strategy::Replay {
                choices,
                lenient: true,
            },
            1,
        );
        explorer.next_schedule();
        let episode = self.coordinator.run_episode(&mut explorer).await?;
        self.check_target().await?;
        if episode.record.turns() == 0 {
            return Err(self.no_traffic());
        }
        Ok(episode)
    }

    /// Stops the target and the proxy and drops the snapshot schema.
    pub async fn close(mut self) {
        let target = match self.target.get_mut() {
            Ok(t) => t.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(t) = target {
            t.stop().await;
        }
        if let Some(p) = self.proxy.take() {
            p.shutdown().await;
        }
        if self.snapshot
            && let Err(e) = Snapshot::discard(&self.ctl).await
        {
            warn!("cannot drop the snapshot schema: {e}");
        }
    }
}
