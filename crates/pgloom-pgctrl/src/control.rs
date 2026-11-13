//! The control connection: a direct (non-proxied) session used for setup,
//! lock inspection, backend cleanup and invariant checks.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, Statement};
use tracing::{debug, warn};

use crate::error::{PgCtrlError, PgCtrlResult};
use crate::url::PgUrl;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Lock-wait state of one backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingInfo {
    pub pid: i32,
    pub wait_event_type: Option<String>,
    pub blocked_by: Vec<i32>,
}

/// A direct connection to the upstream server.
#[derive(Debug)]
pub struct PgControl {
    client: Client,
    connection: JoinHandle<()>,
    url: PgUrl,
    own_pid: i32,
    blocking_stmt: Statement,
}

impl Drop for PgControl {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

impl PgControl {
    pub async fn connect(url: &str) -> PgCtrlResult<Self> {
        let parsed = PgUrl::parse(url)?;
        let target = parsed.redacted();
        let connect = tokio_postgres::connect(url, NoTls);
        let (client, connection) = tokio::time::timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| PgCtrlError::Timeout {
                context: format!("connecting to {target}"),
                ms: CONNECT_TIMEOUT.as_millis() as u64,
            })?
            .map_err(|source| PgCtrlError::Connect {
                target: target.clone(),
                source,
            })?;
        let connection = tokio::spawn(async move {
            if let Err(err) = connection.await {
                debug!(%err, "control connection closed");
            }
        });
        client
            .batch_execute(
                "SET application_name = 'pgloom-control'; \
                 SET statement_timeout = '30s'; \
                 SET lock_timeout = '10s'; \
                 SET idle_in_transaction_session_timeout = 0",
            )
            .await
            .map_err(PgCtrlError::query("configuring control session"))?;
        let own_pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .map_err(PgCtrlError::query("reading control backend pid"))?
            .get(0);
        let blocking_stmt = client
            .prepare(
                "SELECT pid, wait_event_type, pg_blocking_pids(pid) \
                 FROM pg_stat_activity WHERE pid = ANY($1::int4[])",
            )
            .await
            .map_err(PgCtrlError::query("preparing lock inspection query"))?;
        Ok(Self {
            client,
            connection,
            url: parsed,
            own_pid,
            blocking_stmt,
        })
    }

    pub fn url(&self) -> &PgUrl {
        &self.url
    }

    pub fn own_pid(&self) -> i32 {
        self.own_pid
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Runs one or more statements in a single round trip.
    pub async fn batch(&self, sql: &str, context: &str) -> PgCtrlResult<()> {
        self.client
            .batch_execute(sql)
            .await
            .map_err(PgCtrlError::query(context))
    }

    /// Runs a query with the simple protocol, returning rows as text.
    pub async fn simple_rows(
        &self,
        sql: &str,
        context: &str,
    ) -> PgCtrlResult<Vec<Vec<(String, Option<String>)>>> {
        let messages = self
            .client
            .simple_query(sql)
            .await
            .map_err(PgCtrlError::query(context))?;
        Ok(messages
            .into_iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(row) => Some(
                    row.columns()
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (c.name().to_owned(), row.get(i).map(str::to_owned)))
                        .collect(),
                ),
                _ => None,
            })
            .collect())
    }

    pub async fn server_version(&self) -> PgCtrlResult<String> {
        let row = self
            .client
            .query_one("SHOW server_version", &[])
            .await
            .map_err(PgCtrlError::query("reading server_version"))?;
        Ok(row.get(0))
    }

    /// `pg_blocking_pids()` for every tracked backend that is waiting.
    pub async fn blocking_pids(&self, pids: &[i32]) -> PgCtrlResult<BTreeMap<i32, BlockingInfo>> {
        if pids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let pids: Vec<i32> = pids.to_vec();
        let rows = self
            .client
            .query(&self.blocking_stmt, &[&pids])
            .await
            .map_err(PgCtrlError::query("inspecting lock waits"))?;
        let mut out = BTreeMap::new();
        for row in rows {
            let pid: i32 = row.get(0);
            let mut blocked_by: Vec<i32> = row.get(2);
            blocked_by.sort_unstable();
            blocked_by.dedup();
            out.insert(
                pid,
                BlockingInfo {
                    pid,
                    wait_event_type: row.get(1),
                    blocked_by,
                },
            );
        }
        Ok(out)
    }

    /// Backends among `pids` that currently have an open transaction.
    pub async fn backends_in_transaction(&self, pids: &[i32]) -> PgCtrlResult<Vec<i32>> {
        if pids.is_empty() {
            return Ok(Vec::new());
        }
        let pids: Vec<i32> = pids.to_vec();
        let rows = self
            .client
            .query(
                // Aborted transactions report a NULL xact_start but still
                // occupy the session until ROLLBACK.
                "SELECT pid FROM pg_stat_activity \
                 WHERE pid = ANY($1::int4[]) \
                   AND (xact_start IS NOT NULL OR state LIKE 'idle in transaction%') \
                 ORDER BY pid",
                &[&pids],
            )
            .await
            .map_err(PgCtrlError::query("listing open transactions"))?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    /// `pg_cancel_backend(pid)`: aborts the backend's current statement.
    pub async fn cancel_backend(&self, pid: i32) -> PgCtrlResult<bool> {
        self.signal("pg_cancel_backend", pid).await
    }

    /// `pg_terminate_backend(pid)`: closes the session, rolling back its work.
    pub async fn terminate_backend(&self, pid: i32) -> PgCtrlResult<bool> {
        self.signal("pg_terminate_backend", pid).await
    }

    async fn signal(&self, function: &str, pid: i32) -> PgCtrlResult<bool> {
        if pid == self.own_pid {
            return Ok(false);
        }
        let sql = format!("SELECT {function}($1)");
        match self.client.query_one(&sql, &[&pid]).await {
            Ok(row) => Ok(row.get(0)),
            Err(err) => {
                warn!(%err, pid, function, "backend signal failed");
                Err(PgCtrlError::Query {
                    context: format!("{function}({pid})"),
                    source: err,
                })
            }
        }
    }

    /// Terminates every tracked backend that still holds a transaction open,
    /// returning the terminated pids. Used between schedules so that a
    /// misbehaving worker cannot leak locks into the next episode.
    pub async fn terminate_open_transactions(&self, pids: &[i32]) -> PgCtrlResult<Vec<i32>> {
        let open = self.backends_in_transaction(pids).await?;
        let mut terminated = Vec::new();
        for pid in open {
            if self.terminate_backend(pid).await? {
                terminated.push(pid);
            }
        }
        if !terminated.is_empty() {
            self.wait_until_gone(&terminated, Duration::from_secs(5))
                .await?;
        }
        Ok(terminated)
    }

    async fn wait_until_gone(&self, pids: &[i32], limit: Duration) -> PgCtrlResult<()> {
        let pids: Vec<i32> = pids.to_vec();
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let row = self
                .client
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1::int4[])",
                    &[&pids],
                )
                .await
                .map_err(PgCtrlError::query("waiting for terminated backends"))?;
            let alive: i64 = row.get(0);
            if alive == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(PgCtrlError::Timeout {
                    context: format!("waiting for backends {pids:?} to exit"),
                    ms: limit.as_millis() as u64,
                });
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}
