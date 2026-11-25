//! Workers: the concurrent triggers whose database traffic PgLoom schedules.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use pgloom_engine::{WorkerId, WorkerOutcome, Workload};
use tokio::io::AsyncReadExt;

use crate::config::{ExecWorker, HttpWorker, WorkerConfig};

const DETAIL_LIMIT: usize = 240;
/// Response bodies kept for JSON invariants.
const BODY_LIMIT: usize = 1 << 20;
/// Maximum bytes buffered from each of `stdout` / `stderr` of an `exec` worker.
const EXEC_OUTPUT_LIMIT: u64 = 64 * 1024;

/// A worker ready to be launched once per schedule.
#[derive(Debug, Clone)]
pub enum WorkerSpec {
    Http(HttpWorker),
    Sql(Vec<String>),
    Exec(ExecWorker),
}

impl WorkerSpec {
    fn from_config(w: &WorkerConfig) -> Option<Self> {
        match (&w.http, &w.sql, &w.exec) {
            (Some(h), None, None) => Some(Self::Http(h.clone())),
            (None, Some(s), None) => Some(Self::Sql(s.clone())),
            (None, None, Some(x)) => Some(Self::Exec(x.clone())),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Shared {
    ids: Vec<String>,
    specs: Vec<WorkerSpec>,
    /// Connection URL of the PgLoom proxy, handed to SQL and exec workers.
    proxy_url: String,
    /// One HTTP client per worker, isolating connection pools between workers
    /// while still permitting keep-alive reuse across schedules to avoid
    /// ephemeral port exhaustion in `TIME_WAIT`.
    http_clients: Vec<reqwest::Client>,
    /// Per-worker SQL session, kept across schedules.
    sql_sessions: Vec<tokio::sync::Mutex<Option<SqlSession>>>,
}

/// A SQL worker's connection through the proxy. Reusing it across schedules
/// saves a connection handshake (and password hashing) per worker and
/// schedule; `DISCARD ALL` after every script keeps schedules independent.
#[derive(Debug)]
struct SqlSession {
    client: tokio_postgres::Client,
    connection: tokio::task::JoinHandle<()>,
}

impl Drop for SqlSession {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

/// The scenario's workers, implementing the engine's [`Workload`].
#[derive(Debug, Clone)]
pub struct ScenarioWorkload {
    shared: Arc<Shared>,
}

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("worker `{0}` must set exactly one of `http`, `sql` or `exec`")]
    InvalidWorker(String),
    #[error("cannot build HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

impl ScenarioWorkload {
    /// `proxy_url` is the `postgres://` URL of the PgLoom proxy.
    pub fn new(
        workers: &[WorkerConfig],
        proxy_url: impl Into<String>,
    ) -> Result<Self, DriverError> {
        let specs = workers
            .iter()
            .map(|w| {
                WorkerSpec::from_config(w).ok_or_else(|| DriverError::InvalidWorker(w.id.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Each worker gets its own HTTP client with a single idle keep-alive
        // slot: workers never share sockets with each other, and long runs
        // reuse loopback sockets instead of exhausting ephemeral ports.
        let http_clients = specs
            .iter()
            .map(|_| reqwest::Client::builder().pool_max_idle_per_host(1).build())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            shared: Arc::new(Shared {
                ids: workers.iter().map(|w| w.id.clone()).collect(),
                sql_sessions: specs
                    .iter()
                    .map(|_| tokio::sync::Mutex::new(None))
                    .collect(),
                specs,
                proxy_url: proxy_url.into(),
                http_clients,
            }),
        })
    }

    pub fn ids(&self) -> &[String] {
        &self.shared.ids
    }

    /// Runs one worker to completion outside any exploration.
    pub async fn run(&self, worker: WorkerId) -> WorkerOutcome {
        let Some(spec) = self.shared.specs.get(worker.index()).cloned() else {
            return WorkerOutcome::failed("error", format!("unknown worker {worker}"));
        };
        let id = self.shared.ids[worker.index()].clone();
        match spec {
            WorkerSpec::Http(h) => run_http(&self.shared.http_clients[worker.index()], &h).await,
            WorkerSpec::Sql(statements) => {
                let mut session = self.shared.sql_sessions[worker.index()].lock().await;
                run_sql(&self.shared.proxy_url, &mut session, &statements).await
            }
            WorkerSpec::Exec(x) => run_exec(&x, &self.shared.proxy_url, &id).await,
        }
    }
}

impl Workload for ScenarioWorkload {
    fn worker_count(&self) -> usize {
        self.shared.specs.len()
    }

    fn worker_name(&self, worker: WorkerId) -> String {
        self.shared
            .ids
            .get(worker.index())
            .cloned()
            .unwrap_or_else(|| worker.to_string())
    }

    fn launch(&self, worker: WorkerId) -> impl Future<Output = WorkerOutcome> + Send + 'static {
        let this = self.clone();
        async move { this.run(worker).await }
    }
}

fn excerpt(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut out: String = text.chars().take(DETAIL_LIMIT).collect();
    if text.chars().count() > DETAIL_LIMIT {
        out.push('…');
    }
    Some(out)
}

async fn run_http(client: &reqwest::Client, h: &HttpWorker) -> WorkerOutcome {
    let method = match reqwest::Method::from_bytes(h.method.to_ascii_uppercase().as_bytes()) {
        Ok(m) => m,
        Err(_) => return WorkerOutcome::failed("error", format!("invalid method {}", h.method)),
    };
    let mut req = client
        .request(method, &h.url)
        .timeout(Duration::from_millis(h.timeout_ms));
    for (k, v) in &h.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let Some(body) = &h.body {
        req = req.body(body.clone());
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status();
            let mut body = resp.text().await.unwrap_or_default();
            if body.len() > BODY_LIMIT {
                let mut end = BODY_LIMIT;
                while !body.is_char_boundary(end) {
                    end -= 1;
                }
                body.truncate(end);
            }
            let reason = status.canonical_reason().unwrap_or("");
            WorkerOutcome {
                success: status.is_success(),
                status: Some(i32::from(status.as_u16())),
                label: format!("HTTP {} {reason}", status.as_u16())
                    .trim_end()
                    .to_owned(),
                detail: excerpt(&body),
                body: Some(body),
            }
        }
        Err(e) if e.is_timeout() => WorkerOutcome::failed(
            "timeout",
            format!("no HTTP response within {}ms", h.timeout_ms),
        ),
        Err(e) => WorkerOutcome::failed("HTTP error", http_error_chain(&e)),
    }
}

fn http_error_chain(e: &reqwest::Error) -> String {
    let mut msg = e.to_string();
    let mut src = std::error::Error::source(e);
    while let Some(s) = src {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        src = s.source();
    }
    msg
}

async fn run_sql(
    proxy_url: &str,
    cached: &mut Option<SqlSession>,
    statements: &[String],
) -> WorkerOutcome {
    let session = match cached.take().filter(|s| !s.client.is_closed()) {
        Some(s) => s,
        None => match tokio_postgres::connect(proxy_url, tokio_postgres::NoTls).await {
            Ok((client, connection)) => SqlSession {
                client,
                connection: tokio::spawn(async move {
                    let _ = connection.await;
                }),
            },
            Err(e) => {
                return WorkerOutcome::failed("connect error", pgloom_pgctrl::error::detail(&e));
            }
        },
    };
    let client = &session.client;
    let mut outcome = WorkerOutcome {
        success: true,
        label: "ok".into(),
        ..WorkerOutcome::default()
    };
    for sql in statements.iter().filter(|s| !s.trim().is_empty()) {
        if let Err(e) = client.simple_query(sql).await {
            let code = e.code().map(|c| c.code().to_owned());
            let message = e
                .as_db_error()
                .map_or_else(|| e.to_string(), |d| d.message().to_owned());
            // Harmless when the failure happened outside a transaction.
            let _ = client.simple_query("ROLLBACK").await;
            outcome = WorkerOutcome {
                success: false,
                label: code.map_or_else(|| "SQL error".into(), |c| format!("SQLSTATE {c}")),
                detail: excerpt(&message),
                ..WorkerOutcome::default()
            };
            break;
        }
    }
    // `DISCARD ALL` is rejected inside a transaction block: a script that left
    // one open loses its connection, which rolls the transaction back.
    if client.simple_query("DISCARD ALL").await.is_ok() {
        *cached = Some(session);
    }
    outcome
}

#[cfg(unix)]
fn kill_process_group(pgid: u32) {
    let _ = std::process::Command::new("kill")
        .args(["-KILL", "--", &format!("-{pgid}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Ensures an `exec` child and its entire process group are killed if the
/// worker times out or is dropped during wind-down.
struct ExecChildGuard {
    child: tokio::process::Child,
    pgid: Option<u32>,
}

impl ExecChildGuard {
    fn new(child: tokio::process::Child) -> Self {
        let pgid = child.id();
        Self { child, pgid }
    }

    fn terminate_group(&mut self) {
        if let Some(pgid) = self.pgid.take() {
            #[cfg(unix)]
            kill_process_group(pgid);
            #[cfg(not(unix))]
            let _ = pgid;
        }
    }
}

impl Drop for ExecChildGuard {
    fn drop(&mut self) {
        self.terminate_group();
    }
}

async fn read_capped<R: tokio::io::AsyncRead + Unpin>(reader: &mut Option<R>) -> String {
    let Some(r) = reader.as_mut() else {
        return String::new();
    };
    let mut buf = Vec::new();
    let _ = r.take(EXEC_OUTPUT_LIMIT).read_to_end(&mut buf).await;
    // Drain any excess bytes so the subprocess does not observe SIGPIPE.
    let _ = tokio::io::copy(r, &mut tokio::io::sink()).await;
    String::from_utf8_lossy(&buf).into_owned()
}

async fn run_exec(x: &ExecWorker, proxy_url: &str, id: &str) -> WorkerOutcome {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(&x.command)
        .env("DATABASE_URL", proxy_url)
        .env("PGLOOM_WORKER_ID", id)
        .envs(&x.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    if let Some(dir) = &x.workdir {
        cmd.current_dir(dir);
    }
    let child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return WorkerOutcome::failed("spawn error", e.to_string()),
    };
    let mut guard = ExecChildGuard::new(child);
    let mut stdout = guard.child.stdout.take();
    let mut stderr = guard.child.stderr.take();
    let collect = async {
        let (out, err) = tokio::join!(read_capped(&mut stdout), read_capped(&mut stderr));
        let status = guard.child.wait().await;
        (status, out, err)
    };
    match tokio::time::timeout(Duration::from_millis(x.timeout_ms), collect).await {
        Ok((Ok(status), out, err)) => {
            let code = status.code();
            let detail = excerpt(if err.trim().is_empty() { &out } else { &err });
            WorkerOutcome {
                success: status.success(),
                status: code,
                label: code.map_or_else(|| "killed by signal".into(), |c| format!("exit {c}")),
                detail,
                body: None,
            }
        }
        Ok((Err(e), _, _)) => WorkerOutcome::failed("wait error", e.to_string()),
        Err(_) => {
            guard.terminate_group();
            let _ = guard.child.kill().await;
            let _ = guard.child.wait().await;
            WorkerOutcome::failed(
                "timeout",
                format!("command did not exit within {}ms", x.timeout_ms),
            )
        }
    }
}

/// Environment for a process whose database traffic must go through PgLoom.
pub fn proxied_env(base: &BTreeMap<String, String>, proxy_url: &str) -> BTreeMap<String, String> {
    let mut env = base.clone();
    env.entry("DATABASE_URL".into())
        .or_insert_with(|| proxy_url.to_owned());
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excerpts_are_bounded() {
        assert_eq!(excerpt("  "), None);
        let long = "x".repeat(DETAIL_LIMIT + 10);
        let e = excerpt(&long).unwrap();
        assert_eq!(e.chars().count(), DETAIL_LIMIT + 1);
        assert!(e.ends_with('…'));
    }

    #[test]
    fn explicit_database_url_wins() {
        let mut base = BTreeMap::new();
        assert_eq!(
            proxied_env(&base, "postgres://p")["DATABASE_URL"],
            "postgres://p"
        );
        base.insert("DATABASE_URL".into(), "postgres://mine".into());
        assert_eq!(
            proxied_env(&base, "postgres://p")["DATABASE_URL"],
            "postgres://mine"
        );
    }

    #[tokio::test]
    async fn exec_worker_reports_exit_codes_and_env() {
        let cfg = |cmd: &str| WorkerConfig {
            id: "w".into(),
            http: None,
            sql: None,
            exec: Some(ExecWorker {
                command: cmd.into(),
                env: BTreeMap::new(),
                workdir: None,
                timeout_ms: 5000,
            }),
        };
        let workers = [
            cfg("test \"$PGLOOM_WORKER_ID\" = w && echo \"$DATABASE_URL\""),
            cfg("echo boom >&2; exit 3"),
        ];
        let wl = ScenarioWorkload::new(&workers, "postgres://proxy").unwrap();
        let ok = wl.run(WorkerId(0)).await;
        assert!(ok.success, "{ok:?}");
        assert_eq!(ok.status, Some(0));
        assert_eq!(ok.detail.as_deref(), Some("postgres://proxy"));
        let bad = wl.run(WorkerId(1)).await;
        assert!(!bad.success);
        assert_eq!(bad.label, "exit 3");
        assert_eq!(bad.detail.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn exec_worker_times_out() {
        let workers = [WorkerConfig {
            id: "slow".into(),
            http: None,
            sql: None,
            exec: Some(ExecWorker {
                command: "sleep 5".into(),
                env: BTreeMap::new(),
                workdir: None,
                timeout_ms: 50,
            }),
        }];
        let wl = ScenarioWorkload::new(&workers, "postgres://proxy").unwrap();
        let out = wl.run(WorkerId(0)).await;
        assert_eq!(out.label, "timeout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_worker_kills_process_group_on_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("child.pid");
        let cmd = format!("sleep 60 & echo $! > {}; wait", pidfile.display());
        let workers = [WorkerConfig {
            id: "grp".into(),
            http: None,
            sql: None,
            exec: Some(ExecWorker {
                command: cmd,
                env: BTreeMap::new(),
                workdir: None,
                timeout_ms: 200,
            }),
        }];
        let wl = ScenarioWorkload::new(&workers, "postgres://proxy").unwrap();
        let out = wl.run(WorkerId(0)).await;
        assert_eq!(out.label, "timeout");
        let pid: u32 = std::fs::read_to_string(&pidfile)
            .expect("grandchild pid should be written before timeout")
            .trim()
            .parse()
            .unwrap();
        // Give the kernel a moment to reap the SIGKILLed grandchild.
        let mut alive = true;
        for _ in 0..20 {
            let status = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            if !status.success() {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!alive, "grandchild process {pid} survived exec timeout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_worker_kills_background_grandchild_after_clean_exit() {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("bg.pid");
        let cmd = format!(
            "sleep 60 </dev/null >/dev/null 2>&1 & echo $! > {}; exit 0",
            pidfile.display()
        );
        let workers = [WorkerConfig {
            id: "bg".into(),
            http: None,
            sql: None,
            exec: Some(ExecWorker {
                command: cmd,
                env: BTreeMap::new(),
                workdir: None,
                timeout_ms: 5000,
            }),
        }];
        let wl = ScenarioWorkload::new(&workers, "postgres://proxy").unwrap();
        let out = wl.run(WorkerId(0)).await;
        assert!(out.success, "{out:?}");
        let pid: u32 = std::fs::read_to_string(&pidfile)
            .expect("background pid should be written before exit")
            .trim()
            .parse()
            .unwrap();
        let mut alive = true;
        for _ in 0..20 {
            let status = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            if !status.success() {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !alive,
            "background grandchild {pid} survived clean exec exit"
        );
    }

    #[tokio::test]
    async fn exec_worker_caps_large_output_without_failing() {
        let workers = [WorkerConfig {
            id: "noisy".into(),
            http: None,
            sql: None,
            exec: Some(ExecWorker {
                // Emit ~200 KiB of stdout (> EXEC_OUTPUT_LIMIT) and exit 0.
                command: "awk 'BEGIN { for (i = 0; i < 2000; i++) printf \"%0100d\\n\", i }'"
                    .into(),
                env: BTreeMap::new(),
                workdir: None,
                timeout_ms: 5000,
            }),
        }];
        let wl = ScenarioWorkload::new(&workers, "postgres://proxy").unwrap();
        let out = wl.run(WorkerId(0)).await;
        assert!(out.success, "{out:?}");
        assert!(out.detail.as_deref().is_some_and(|d| !d.is_empty()));
    }
}
