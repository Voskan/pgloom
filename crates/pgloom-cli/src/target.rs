//! Lifecycle of the service under test (`target.command`).

use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::error::CliError;

const LOG_TAIL: usize = 20;

/// A child process started in its own process group, so that wrappers such
/// as `go run` or `npm start` are stopped together with what they spawn.
#[derive(Debug)]
pub struct TargetProcess {
    child: Child,
    pgid: Option<u32>,
    log: std::sync::Arc<Mutex<Vec<String>>>,
}

impl TargetProcess {
    pub fn spawn(
        command: &str,
        env: &BTreeMap<String, String>,
        workdir: Option<&str>,
    ) -> Result<Self, CliError> {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(command)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        cmd.process_group(0);
        if let Some(dir) = workdir {
            cmd.current_dir(dir);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| CliError::env(&format!("cannot start target `{command}`"), e))?;
        let pgid = child.id();
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        if let Some(out) = child.stdout.take() {
            tokio::spawn(collect(BufReader::new(out), log.clone()));
        }
        if let Some(err) = child.stderr.take() {
            tokio::spawn(collect(BufReader::new(err), log.clone()));
        }
        info!(command, pid = ?pgid, "target started");
        Ok(Self { child, pgid, log })
    }

    /// Last lines the process printed, for error messages.
    pub async fn log_tail(&self) -> String {
        self.log.lock().await.join("\n")
    }

    /// Polls `url` until it answers with a non-5xx status, failing early if
    /// the process exits.
    pub async fn wait_ready(&mut self, url: &str, timeout: Duration) -> Result<(), CliError> {
        let client = http_client()?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(CliError::Environment(format!(
                    "target exited with {status} before {url} became ready{}",
                    with_output(&self.log_tail().await)
                )));
            }
            match probe(&client, url).await {
                Ok(()) => return Ok(()),
                Err(last) if Instant::now() >= deadline => {
                    return Err(CliError::Environment(format!(
                        "target service at {url} is not ready after {}ms ({last}){}",
                        timeout.as_millis(),
                        with_output(&self.log_tail().await)
                    )));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    /// SIGTERM to the process group, then SIGKILL after a grace period.
    pub async fn stop(mut self) {
        if let Some(pgid) = self.pgid {
            signal_group("TERM", pgid).await;
            if tokio::time::timeout(Duration::from_secs(3), self.child.wait())
                .await
                .is_ok()
            {
                return;
            }
            signal_group("KILL", pgid).await;
        }
        let _ = self.child.kill().await;
    }
}

impl Drop for TargetProcess {
    fn drop(&mut self) {
        if let (Some(pgid), Ok(None)) = (self.pgid, self.child.try_wait()) {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", "--", &format!("-{pgid}")])
                .stderr(Stdio::null())
                .status();
        }
    }
}

async fn signal_group(signal: &str, pgid: u32) {
    debug!(signal, pgid, "signalling target process group");
    let _ = Command::new("kill")
        .args([&format!("-{signal}"), "--", &format!("-{pgid}")])
        .stderr(Stdio::null())
        .status()
        .await;
}

async fn collect<R: tokio::io::AsyncBufRead + Unpin>(
    reader: R,
    log: std::sync::Arc<Mutex<Vec<String>>>,
) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        debug!(target: "pgloom::target", "{line}");
        let mut log = log.lock().await;
        if log.len() == LOG_TAIL {
            log.remove(0);
        }
        log.push(line);
    }
}

fn with_output(tail: &str) -> String {
    if tail.is_empty() {
        String::new()
    } else {
        format!("\ntarget output:\n{tail}")
    }
}

fn http_client() -> Result<reqwest::Client, CliError> {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .map_err(|e| CliError::Internal(e.to_string()))
}

async fn probe(client: &reqwest::Client, url: &str) -> Result<(), String> {
    match client.get(url).send().await {
        Ok(r) if !r.status().is_server_error() => Ok(()),
        Ok(r) => Err(format!("HTTP {}", r.status())),
        Err(e) => Err(e.to_string()),
    }
}

/// Polls `url` until it answers with a non-5xx status or `timeout` passes.
pub async fn wait_http(url: &str, timeout: Duration) -> Result<(), CliError> {
    let client = http_client()?;
    let deadline = Instant::now() + timeout;
    loop {
        match probe(&client, url).await {
            Ok(()) => return Ok(()),
            Err(last) if Instant::now() >= deadline => {
                return Err(CliError::Environment(format!(
                    "service at {url} is not ready after {}ms ({last})",
                    timeout.as_millis()
                )));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}
