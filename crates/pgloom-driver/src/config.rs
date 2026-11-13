//! `pgloom.yaml`: schema, environment interpolation and validation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::env::interpolate;

pub const DEFAULT_PROXY_LISTEN: &str = "127.0.0.1:15432";

/// Errors in a scenario file. Always reported with exit code 2.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Env(String),

    #[error("invalid YAML in {path}:\n{message}")]
    Yaml { path: String, message: String },

    #[error("invalid scenario {path}:\n{}", .problems.iter().map(|p| format!("  - {p}")).collect::<Vec<_>>().join("\n"))]
    Invalid { path: String, problems: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub name: String,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub target: Option<TargetConfig>,
    #[serde(default)]
    pub exploration: ExplorationConfig,
    pub workers: Vec<WorkerConfig>,
    #[serde(default)]
    pub invariants: InvariantsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// URL of the real PostgreSQL server.
    pub upstream: String,
    /// Address the PgLoom proxy listens on.
    #[serde(default = "default_listen")]
    pub proxy_listen: String,
    /// Executed once before the target starts: schema and data the service
    /// needs to boot.
    #[serde(default)]
    pub setup_sql: Option<String>,
    /// Executed once after the target is ready, so after any migrations it
    /// runs on startup, and before the reference snapshot is taken.
    #[serde(default)]
    pub seed_sql: Option<String>,
    /// Replaces the automatic snapshot restore between schedules.
    #[serde(default)]
    pub reset_sql: Option<String>,
    /// Tables that are neither snapshotted nor restored.
    #[serde(default)]
    pub snapshot_exclude: Vec<String>,
}

fn default_listen() -> String {
    DEFAULT_PROXY_LISTEN.to_owned()
}

/// Optional process under test, started and stopped by PgLoom.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Polled until it answers 2xx before exploration starts.
    #[serde(default)]
    pub ready_http: Option<String>,
    #[serde(default = "default_startup_timeout")]
    pub startup_timeout_ms: u64,
}

fn default_startup_timeout() -> u64 {
    5000
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StrategyName {
    Exhaustive,
    Bounded,
    Random,
}

/// Partial-order reduction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReductionName {
    /// Every interleaving is executed.
    None,
    /// Dynamic partial-order reduction: interleavings that differ only in the
    /// order of independent database steps run once.
    Dpor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplorationConfig {
    #[serde(default = "default_strategy")]
    pub strategy: StrategyName,
    #[serde(default = "default_max_preemptions")]
    pub max_preemptions: u32,
    #[serde(default = "default_max_schedules")]
    pub max_schedules: usize,
    #[serde(default = "default_lock_probe")]
    pub lock_probe_ms: u64,
    #[serde(default = "default_step_timeout")]
    pub step_timeout_ms: u64,
    /// Seed for the `random` strategy.
    #[serde(default)]
    pub seed: Option<u64>,
    /// Partial-order reduction; defaults to `dpor` for `exhaustive` and
    /// `none` otherwise.
    #[serde(default)]
    pub reduction: Option<ReductionName>,
}

impl ExplorationConfig {
    /// The reduction in effect once defaults are applied.
    pub fn effective_reduction(&self) -> ReductionName {
        match (self.reduction, self.strategy) {
            (Some(r), _) => r,
            (None, StrategyName::Exhaustive) => ReductionName::Dpor,
            (None, _) => ReductionName::None,
        }
    }
}

fn default_strategy() -> StrategyName {
    StrategyName::Bounded
}
fn default_max_preemptions() -> u32 {
    2
}
fn default_max_schedules() -> usize {
    200
}
fn default_lock_probe() -> u64 {
    2
}
fn default_step_timeout() -> u64 {
    3000
}

impl Default for ExplorationConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            max_preemptions: default_max_preemptions(),
            max_schedules: default_max_schedules(),
            lock_probe_ms: default_lock_probe(),
            step_timeout_ms: default_step_timeout(),
            seed: None,
            reduction: None,
        }
    }
}

/// One concurrent trigger. Exactly one of `http`, `sql` or `exec` is set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub id: String,
    #[serde(default)]
    pub http: Option<HttpWorker>,
    /// Statements executed in order on one connection through the proxy.
    #[serde(default)]
    pub sql: Option<Vec<String>>,
    #[serde(default)]
    pub exec: Option<ExecWorker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpWorker {
    #[serde(default = "default_method")]
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default = "default_http_timeout")]
    pub timeout_ms: u64,
}

fn default_method() -> String {
    "GET".into()
}
fn default_http_timeout() -> u64 {
    30_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecWorker {
    /// Run with `sh -c`; `DATABASE_URL` points at the PgLoom proxy.
    pub command: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub workdir: Option<String>,
    #[serde(default = "default_http_timeout")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvariantsConfig {
    #[serde(default)]
    pub sql: Vec<SqlInvariantConfig>,
    #[serde(default)]
    pub responses: Vec<ResponseInvariant>,
    #[serde(default = "default_true")]
    pub fail_on_deadlock: bool,
}

fn default_true() -> bool {
    true
}

impl Default for InvariantsConfig {
    fn default() -> Self {
        Self {
            sql: Vec::new(),
            responses: Vec::new(),
            fail_on_deadlock: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlInvariantConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub assert_true: String,
    #[serde(default)]
    pub diagnostic_query: Option<String>,
}

/// Assertion over the collected worker outcomes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseInvariant {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// At most this many workers may succeed (HTTP 2xx, exit 0, SQL ok).
    #[serde(default)]
    pub max_2xx_count: Option<usize>,
    /// At least this many workers must succeed.
    #[serde(default)]
    pub min_2xx_count: Option<usize>,
    /// Every worker's status must be one of these.
    #[serde(default)]
    pub allowed_statuses: Option<Vec<i32>>,
    /// Restricts `json` and `distinct` to these workers (default: every
    /// HTTP worker).
    #[serde(default)]
    pub workers: Option<Vec<String>>,
    /// Which responses `json` and `distinct` look at.
    #[serde(default)]
    pub apply_to: ApplyTo,
    /// Field checks on every JSON response body in scope.
    #[serde(default)]
    pub json: Vec<JsonCheck>,
    /// A JSON pointer whose values must differ between the responses in
    /// scope, e.g. two withdrawals must not report the same new balance.
    #[serde(default)]
    pub distinct: Option<String>,
}

impl ResponseInvariant {
    /// Whether the invariant inspects response bodies.
    pub fn checks_bodies(&self) -> bool {
        !self.json.is_empty() || self.distinct.is_some()
    }
}

/// The responses that body checks apply to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplyTo {
    /// HTTP 2xx responses only.
    #[default]
    Success,
    /// Every HTTP response.
    All,
}

/// A check of one field of a JSON response body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonCheck {
    /// RFC 6901 JSON pointer such as `/balance` or `/items/0/id`.
    pub pointer: String,
    /// The field must be present (`true`) or absent (`false`).
    #[serde(default)]
    pub exists: Option<bool>,
    #[serde(default)]
    pub equals: Option<serde_json::Value>,
    #[serde(default)]
    pub one_of: Option<Vec<serde_json::Value>>,
    /// Inclusive numeric lower bound.
    #[serde(default)]
    pub min: Option<serde_json::Number>,
    /// Inclusive numeric upper bound.
    #[serde(default)]
    pub max: Option<serde_json::Number>,
}

impl JsonCheck {
    fn has_predicate(&self) -> bool {
        self.exists.is_some()
            || self.equals.is_some()
            || self.one_of.is_some()
            || self.min.is_some()
            || self.max.is_some()
    }
}

fn valid_pointer(p: &str) -> bool {
    p.is_empty() || p.starts_with('/')
}

/// A parsed scenario together with its source text.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: Config,
    pub path: PathBuf,
    /// The file exactly as written (before interpolation).
    pub source: String,
}

impl Config {
    /// Reads, interpolates, parses and validates a scenario file.
    pub fn load(path: &Path) -> Result<LoadedConfig, ConfigError> {
        let source = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let mut config = Self::parse(&source, &path.display().to_string())?;
        config.resolve_paths(path.parent().unwrap_or(Path::new("")));
        Ok(LoadedConfig {
            config,
            path: path.to_owned(),
            source,
        })
    }

    /// Makes working directories relative to `base`, the directory of the
    /// scenario file, so a scenario behaves the same from any current
    /// directory. Unset directories default to `base` itself.
    pub fn resolve_paths(&mut self, base: &Path) {
        if base.as_os_str().is_empty() {
            return;
        }
        let resolve = |dir: &mut Option<String>| {
            let joined = match dir.as_deref() {
                Some(d) => base.join(d),
                None => base.to_owned(),
            };
            *dir = Some(joined.to_string_lossy().into_owned());
        };
        if let Some(t) = &mut self.target {
            resolve(&mut t.workdir);
        }
        for w in &mut self.workers {
            if let Some(x) = &mut w.exec {
                resolve(&mut x.workdir);
            }
        }
    }

    /// Interpolates `${VAR}` / `${VAR:-default}` from the process environment,
    /// then parses and validates.
    pub fn parse(source: &str, origin: &str) -> Result<Self, ConfigError> {
        Self::parse_with_env(source, origin, |k| std::env::var(k).ok())
    }

    pub fn parse_with_env(
        source: &str,
        origin: &str,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let text = interpolate(source, lookup).map_err(ConfigError::Env)?;
        let config: Config = serde_saphyr::from_str(&text).map_err(|e| ConfigError::Yaml {
            path: origin.to_owned(),
            message: e.to_string(),
        })?;
        let problems = config.problems();
        if !problems.is_empty() {
            return Err(ConfigError::Invalid {
                path: origin.to_owned(),
                problems,
            });
        }
        Ok(config)
    }

    /// Semantic validation; returns every problem found.
    pub fn problems(&self) -> Vec<String> {
        let mut p = Vec::new();
        if self.version != 1 {
            p.push(format!(
                "version: unsupported version {} (expected 1)",
                self.version
            ));
        }
        if self.name.trim().is_empty() {
            p.push("name: must not be empty".into());
        }
        if !self.database.upstream.starts_with("postgres://")
            && !self.database.upstream.starts_with("postgresql://")
        {
            p.push("database.upstream: expected a postgres:// URL".into());
        }
        if self
            .database
            .proxy_listen
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            p.push(format!(
                "database.proxy_listen: `{}` is not a socket address such as 127.0.0.1:15432",
                self.database.proxy_listen
            ));
        }
        let e = &self.exploration;
        if e.max_schedules == 0 {
            p.push("exploration.max_schedules: must be at least 1".into());
        }
        if e.lock_probe_ms == 0 {
            p.push("exploration.lock_probe_ms: must be at least 1".into());
        }
        if e.step_timeout_ms <= e.lock_probe_ms {
            p.push("exploration.step_timeout_ms: must exceed lock_probe_ms".into());
        }
        if e.reduction == Some(ReductionName::Dpor) && e.strategy != StrategyName::Exhaustive {
            p.push(
                "exploration.reduction: `dpor` requires strategy `exhaustive` \
                 (preemption bounding and random sampling run without reduction)"
                    .into(),
            );
        }
        if self.workers.len() < 2 {
            p.push(format!(
                "workers: at least 2 concurrent workers are required (found {})",
                self.workers.len()
            ));
        }
        let mut ids = BTreeSet::new();
        for (i, w) in self.workers.iter().enumerate() {
            let at = format!("workers[{i}]");
            if w.id.trim().is_empty() {
                p.push(format!("{at}.id: must not be empty"));
            } else if !ids.insert(w.id.as_str()) {
                p.push(format!("{at}.id: duplicate worker id `{}`", w.id));
            }
            let kinds = [w.http.is_some(), w.sql.is_some(), w.exec.is_some()]
                .into_iter()
                .filter(|k| *k)
                .count();
            if kinds != 1 {
                p.push(format!(
                    "{at} (`{}`): exactly one of `http`, `sql` or `exec` must be set",
                    w.id
                ));
            }
            if let Some(h) = &w.http {
                if !h.url.starts_with("http://") {
                    p.push(format!(
                        "{at}.http.url: expected an http:// URL, got `{}`",
                        h.url
                    ));
                }
                let m = h.method.to_ascii_uppercase();
                if !matches!(
                    m.as_str(),
                    "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"
                ) {
                    p.push(format!(
                        "{at}.http.method: unsupported method `{}`",
                        h.method
                    ));
                }
            }
            if let Some(sql) = &w.sql
                && sql.iter().all(|s| s.trim().is_empty())
            {
                p.push(format!("{at}.sql: must contain at least one statement"));
            }
            if let Some(x) = &w.exec
                && x.command.trim().is_empty()
            {
                p.push(format!("{at}.exec.command: must not be empty"));
            }
        }
        if let Some(t) = &self.target {
            if t.command.as_deref().is_some_and(|c| c.trim().is_empty()) {
                p.push("target.command: must not be empty".into());
            }
            if let Some(url) = &t.ready_http
                && !url.starts_with("http://")
            {
                p.push(format!(
                    "target.ready_http: expected an http:// URL, got `{url}`"
                ));
            }
        }
        let mut names = BTreeSet::new();
        for inv in &self.invariants.sql {
            if !names.insert(inv.name.as_str()) {
                p.push(format!("invariants: duplicate name `{}`", inv.name));
            }
            if inv.assert_true.trim().is_empty() {
                p.push(format!(
                    "invariants.sql `{}`: assert_true is empty",
                    inv.name
                ));
            }
        }
        for inv in &self.invariants.responses {
            if !names.insert(inv.name.as_str()) {
                p.push(format!("invariants: duplicate name `{}`", inv.name));
            }
            let at = format!("invariants.responses `{}`", inv.name);
            if inv.max_2xx_count.is_none()
                && inv.min_2xx_count.is_none()
                && inv.allowed_statuses.is_none()
                && !inv.checks_bodies()
            {
                p.push(format!(
                    "{at}: set max_2xx_count, min_2xx_count, allowed_statuses, json or distinct"
                ));
            }
            for (i, check) in inv.json.iter().enumerate() {
                if !valid_pointer(&check.pointer) {
                    p.push(format!(
                        "{at}: json[{i}].pointer `{}` must be empty or start with `/`",
                        check.pointer
                    ));
                }
                if !check.has_predicate() {
                    p.push(format!(
                        "{at}: json[{i}] needs exists, equals, one_of, min or max"
                    ));
                }
                if let (Some(lo), Some(hi)) = (&check.min, &check.max)
                    && lo.as_f64() > hi.as_f64()
                {
                    p.push(format!("{at}: json[{i}].min exceeds max"));
                }
            }
            if let Some(ptr) = &inv.distinct
                && !valid_pointer(ptr)
            {
                p.push(format!(
                    "{at}: distinct `{ptr}` must be empty or start with `/`"
                ));
            }
            if inv.workers.is_some() && !inv.checks_bodies() {
                p.push(format!("{at}: workers only applies to json and distinct"));
            }
            if inv.checks_bodies() {
                let scope: Vec<&WorkerConfig> = match &inv.workers {
                    Some(ids) => {
                        for id in ids {
                            if !self.workers.iter().any(|w| &w.id == id) {
                                p.push(format!("{at}: unknown worker `{id}`"));
                            }
                        }
                        self.workers
                            .iter()
                            .filter(|w| ids.contains(&w.id))
                            .collect()
                    }
                    None => self.workers.iter().collect(),
                };
                if let Some(w) = scope
                    .iter()
                    .find(|w| w.http.is_none() && inv.workers.is_some())
                {
                    p.push(format!(
                        "{at}: worker `{}` has no HTTP response to check",
                        w.id
                    ));
                } else if !scope.iter().any(|w| w.http.is_some()) {
                    p.push(format!("{at}: json and distinct need an HTTP worker"));
                }
            }
        }
        if self.invariants.sql.is_empty()
            && self.invariants.responses.is_empty()
            && !self.invariants.fail_on_deadlock
        {
            p.push("invariants: nothing to check (no SQL or response invariants and fail_on_deadlock is false)".into());
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) const EXAMPLE: &str = r#"
version: 1
name: "wallet-double-spend-withdraw"
database:
  upstream: "${PGLOOM_UPSTREAM_URL:-postgres://postgres:postgres@127.0.0.1:5432/app_test}"
  proxy_listen: "127.0.0.1:15432"
  setup_sql: |
    CREATE TABLE IF NOT EXISTS wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
    TRUNCATE TABLE wallets;
    INSERT INTO wallets (id, balance) VALUES (1, 100);
  reset_sql: null
target:
  command: null
  env:
    DATABASE_URL: "postgres://postgres:postgres@127.0.0.1:15432/app_test?sslmode=disable"
  ready_http: "http://127.0.0.1:8080/healthz"
  startup_timeout_ms: 5000
exploration:
  strategy: "bounded"
  max_preemptions: 2
  max_schedules: 100
  lock_probe_ms: 8
  step_timeout_ms: 3000
workers:
  - id: "req_a"
    http:
      method: "POST"
      url: "http://127.0.0.1:8080/withdraw"
      headers:
        Content-Type: "application/json"
        Idempotency-Key: "key-a"
      body: '{"wallet_id": 1, "amount": 100}'
  - id: "req_b"
    http:
      method: "POST"
      url: "http://127.0.0.1:8080/withdraw"
      headers:
        Content-Type: "application/json"
      body: '{"wallet_id": 1, "amount": 100}'
invariants:
  sql:
    - name: "no_negative_balance"
      description: "Wallet balance must never drop below zero"
      assert_true: "SELECT COALESCE(MIN(balance), 0) >= 0 FROM wallets"
      diagnostic_query: "SELECT id, balance FROM wallets WHERE id = 1"
  responses:
    - name: "at_most_one_successful_withdrawal"
      max_2xx_count: 1
  fail_on_deadlock: true
"#;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn parses_the_reference_example() {
        let c = Config::parse_with_env(EXAMPLE, "pgloom.yaml", no_env).unwrap();
        assert_eq!(c.name, "wallet-double-spend-withdraw");
        assert_eq!(
            c.database.upstream,
            "postgres://postgres:postgres@127.0.0.1:5432/app_test"
        );
        assert!(c.database.reset_sql.is_none());
        assert_eq!(c.exploration.strategy, StrategyName::Bounded);
        assert_eq!(c.exploration.max_schedules, 100);
        assert_eq!(c.workers.len(), 2);
        let http = c.workers[0].http.as_ref().unwrap();
        assert_eq!(http.method, "POST");
        assert_eq!(http.headers["Idempotency-Key"], "key-a");
        assert_eq!(c.invariants.responses[0].max_2xx_count, Some(1));
        assert!(c.target.unwrap().command.is_none());
    }

    #[test]
    fn environment_overrides_defaults() {
        let c = Config::parse_with_env(EXAMPLE, "pgloom.yaml", |k| {
            (k == "PGLOOM_UPSTREAM_URL").then(|| "postgres://u:p@db:6000/x".to_string())
        })
        .unwrap();
        assert_eq!(c.database.upstream, "postgres://u:p@db:6000/x");
    }

    #[test]
    fn applies_defaults() {
        let src = r#"
version: 1
name: minimal
database:
  upstream: postgres://localhost/db
workers:
  - id: a
    sql: ["UPDATE t SET x = x + 1"]
  - id: b
    sql: ["UPDATE t SET x = x + 1"]
"#;
        let c = Config::parse_with_env(src, "x", no_env).unwrap();
        assert_eq!(c.database.proxy_listen, DEFAULT_PROXY_LISTEN);
        assert_eq!(c.exploration, ExplorationConfig::default());
        assert_eq!(c.exploration.max_schedules, 200);
        assert!(c.invariants.fail_on_deadlock);
    }

    #[test]
    fn reports_every_semantic_problem() {
        let src = r#"
version: 2
name: ""
database:
  upstream: mysql://x
  proxy_listen: "nope"
exploration:
  lock_probe_ms: 10
  step_timeout_ms: 5
workers:
  - id: a
    http: { url: "https://secure.example/x", method: BREW }
    sql: ["SELECT 1"]
  - id: a
    exec: { command: "  " }
invariants:
  responses:
    - name: r
"#;
        let err = Config::parse_with_env(src, "bad.yaml", no_env).unwrap_err();
        let ConfigError::Invalid { problems, .. } = &err else {
            panic!("{err}");
        };
        let all = problems.join("\n");
        for needle in [
            "unsupported version 2",
            "name: must not be empty",
            "database.upstream",
            "proxy_listen",
            "step_timeout_ms",
            "exactly one of",
            "https://secure.example/x",
            "unsupported method `BREW`",
            "duplicate worker id `a`",
            "exec.command",
            "set max_2xx_count",
        ] {
            assert!(all.contains(needle), "missing `{needle}` in:\n{all}");
        }
        assert!(err.to_string().starts_with("invalid scenario bad.yaml"));
    }

    const TWO_HTTP: &str = r#"
version: 1
name: bodies
database:
  upstream: postgres://localhost/db
workers:
  - id: a
    http: { url: "http://127.0.0.1:1/x" }
  - id: b
    http: { url: "http://127.0.0.1:1/x" }
  - id: s
    sql: ["SELECT 1"]
"#;

    #[test]
    fn parses_body_invariants_and_reduction() {
        let src = format!(
            "{TWO_HTTP}exploration:\n  strategy: exhaustive\n  reduction: dpor\n\
             invariants:\n  responses:\n    - name: r\n      workers: [a, b]\n      apply_to: all\n\
             \x20     json:\n        - {{ pointer: /balance, min: 0 }}\n\
             \x20       - {{ pointer: /status, one_of: [ok, declined] }}\n      distinct: /balance\n"
        );
        let c = Config::parse_with_env(&src, "x", no_env).unwrap();
        assert_eq!(c.exploration.effective_reduction(), ReductionName::Dpor);
        let r = &c.invariants.responses[0];
        assert_eq!(r.apply_to, ApplyTo::All);
        assert_eq!(
            r.workers.as_deref(),
            Some(&["a".to_owned(), "b".to_owned()][..])
        );
        assert_eq!(r.json[0].min, Some(0.into()));
        assert_eq!(r.json[1].one_of.as_ref().unwrap().len(), 2);
        assert_eq!(r.distinct.as_deref(), Some("/balance"));
        assert!(r.checks_bodies());
    }

    #[test]
    fn rejects_malformed_body_invariants() {
        let src = format!(
            "{TWO_HTTP}exploration:\n  strategy: bounded\n  reduction: dpor\n\
             invariants:\n  responses:\n\
             \x20   - name: p\n      json: [{{ pointer: balance, min: 0 }}, {{ pointer: /x }}, {{ pointer: /y, min: 5, max: 1 }}]\n\
             \x20   - name: d\n      distinct: nope\n      workers: [a, ghost, s]\n\
             \x20   - name: w\n      max_2xx_count: 1\n      workers: [a]\n"
        );
        let err = Config::parse_with_env(&src, "x", no_env).unwrap_err();
        let ConfigError::Invalid { problems, .. } = &err else {
            panic!("{err}");
        };
        let all = problems.join("\n");
        for needle in [
            "`dpor` requires strategy `exhaustive`",
            "json[0].pointer `balance`",
            "json[1] needs exists",
            "json[2].min exceeds max",
            "distinct `nope`",
            "unknown worker `ghost`",
            "worker `s` has no HTTP response",
            "workers only applies to json and distinct",
        ] {
            assert!(all.contains(needle), "missing `{needle}` in:\n{all}");
        }
    }

    #[test]
    fn rejects_unknown_fields_and_bad_yaml() {
        let src =
            "version: 1\nname: x\ndatabase: { upstream: postgres://h/d, typo: 1 }\nworkers: []\n";
        assert!(matches!(
            Config::parse_with_env(src, "x", no_env),
            Err(ConfigError::Yaml { .. })
        ));
        assert!(matches!(
            Config::parse_with_env("version: [1", "x", no_env),
            Err(ConfigError::Yaml { .. })
        ));
    }

    #[test]
    fn missing_variables_are_reported() {
        let src = "version: 1\nname: ${SCENARIO_NAME}\n";
        let err = Config::parse_with_env(src, "x", no_env).unwrap_err();
        assert!(err.to_string().contains("SCENARIO_NAME"), "{err}");
    }

    #[test]
    fn working_directories_follow_the_scenario_file() {
        let src = r#"
version: 1
name: dirs
database:
  upstream: postgres://localhost/db
target:
  command: "go run ."
workers:
  - id: a
    exec: { command: "true", workdir: scripts }
  - id: b
    exec: { command: "true", workdir: /abs }
"#;
        let mut c = Config::parse_with_env(src, "x", no_env).unwrap();
        c.resolve_paths(Path::new("examples/go-wallet"));
        assert_eq!(
            c.target.as_ref().unwrap().workdir.as_deref(),
            Some("examples/go-wallet")
        );
        let dirs: Vec<_> = c
            .workers
            .iter()
            .filter_map(|w| w.exec.as_ref()?.workdir.as_deref())
            .collect();
        assert_eq!(dirs, ["examples/go-wallet/scripts", "/abs"]);

        // A scenario in the current directory keeps the inherited one.
        let mut c = Config::parse_with_env(src, "x", no_env).unwrap();
        c.resolve_paths(Path::new(""));
        assert!(c.target.unwrap().workdir.is_none());
    }

    #[test]
    fn load_reads_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pgloom.yaml");
        std::fs::write(&path, EXAMPLE).unwrap();
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.source, EXAMPLE);
        assert!(matches!(
            Config::load(&dir.path().join("missing.yaml")),
            Err(ConfigError::Read { .. })
        ));
    }
}
