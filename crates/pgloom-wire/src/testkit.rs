//! In-process PostgreSQL stand-ins for tests (enabled by the `testkit` feature).
//!
//! [`MockPostgres`] speaks enough of protocol v3 (startup, simple and extended
//! query, `ReadyForQuery` transaction status) to exercise the proxy and the
//! scheduler without a real server. It models a single `wallets(id, balance)`
//! table with READ COMMITTED visibility and row locks that block writers until
//! the holder commits, exactly the behaviour PgLoom must survive.
//!
//! Supported statements (case-insensitive, `$n` parameters substituted):
//! `BEGIN`, `COMMIT`, `ROLLBACK`, `SELECT 1`,
//! `SELECT balance FROM wallets WHERE id = N [FOR UPDATE | FOR NO KEY UPDATE]`,
//! `UPDATE wallets SET balance = balance - A WHERE id = N`,
//! `UPDATE wallets SET balance = V WHERE id = N`.

#![allow(missing_docs, clippy::missing_panics_doc)]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::error::{Peer, WireResult};
use crate::frame::{SSL_REQUEST_CODE, StartupPacket, encode_startup, encode_startup_code};
use crate::io::FrameReader;
use crate::messages::{
    BindMessage, ExecuteMessage, ParseMessage, TxStatus, build, decode_query, sqlstate, tag,
};
use crate::response::{ResponseCollector, TurnResponse};

#[derive(Debug, Default)]
struct Session {
    in_tx: bool,
    failed: bool,
    pending: BTreeMap<i64, i64>,
    locks: BTreeSet<i64>,
    waiting_for: Option<i64>,
    cancel_requested: bool,
    terminated: bool,
}

#[derive(Debug, Default)]
struct State {
    committed: BTreeMap<i64, i64>,
    sessions: BTreeMap<i32, Session>,
    lock_owner: BTreeMap<i64, i32>,
    next_pid: i32,
    log: Vec<(i32, String)>,
}

#[derive(Debug, Default)]
struct Inner {
    state: Mutex<State>,
    changed: Notify,
}

/// Handle to a running mock server.
#[derive(Debug)]
pub struct MockPostgres {
    addr: SocketAddr,
    inner: Arc<Inner>,
    task: JoinHandle<()>,
}

impl Drop for MockPostgres {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Outcome {
    Rows(Vec<String>, &'static str),
    Command(String),
    Error(&'static str, String),
}

impl MockPostgres {
    pub async fn start(wallets: &[(i64, i64)]) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let inner = Arc::new(Inner::default());
        {
            let mut st = lock(&inner.state);
            st.next_pid = 4800;
            st.committed = wallets.iter().copied().collect();
        }
        let task_inner = Arc::clone(&inner);
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let inner = Arc::clone(&task_inner);
                tokio::spawn(async move {
                    let _ = serve(socket, inner).await;
                });
            }
        });
        Ok(Self { addr, inner, task })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn balance(&self, id: i64) -> Option<i64> {
        lock(&self.inner.state).committed.get(&id).copied()
    }

    pub fn set_wallets(&self, wallets: &[(i64, i64)]) {
        lock(&self.inner.state).committed = wallets.iter().copied().collect();
    }

    /// Statements in the order the server started executing them.
    pub fn log(&self) -> Vec<(i32, String)> {
        lock(&self.inner.state).log.clone()
    }

    pub fn clear_log(&self) {
        lock(&self.inner.state).log.clear();
    }

    /// Equivalent of `pg_blocking_pids(pid)`.
    pub fn blocking_pids(&self, pid: i32) -> Vec<i32> {
        let st = lock(&self.inner.state);
        st.sessions
            .get(&pid)
            .and_then(|s| s.waiting_for)
            .and_then(|row| st.lock_owner.get(&row).copied())
            .filter(|owner| *owner != pid)
            .into_iter()
            .collect()
    }

    /// Pids of sessions with an open transaction.
    pub fn sessions_in_transaction(&self) -> Vec<i32> {
        lock(&self.inner.state)
            .sessions
            .iter()
            .filter(|(_, s)| s.in_tx)
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// Equivalent of `pg_cancel_backend(pid)`: aborts a lock wait.
    pub fn cancel(&self, pid: i32) -> bool {
        let found = {
            let mut st = lock(&self.inner.state);
            match st.sessions.get_mut(&pid) {
                Some(s) if s.waiting_for.is_some() => {
                    s.cancel_requested = true;
                    true
                }
                Some(_) => false,
                None => false,
            }
        };
        self.inner.changed.notify_waiters();
        found
    }

    /// Equivalent of `pg_terminate_backend(pid)`: rolls back and disconnects.
    pub fn terminate(&self, pid: i32) -> bool {
        let found = {
            let mut st = lock(&self.inner.state);
            let found = st.sessions.contains_key(&pid);
            if let Some(s) = st.sessions.get_mut(&pid) {
                s.terminated = true;
                s.cancel_requested = true;
            }
            rollback(&mut st, pid);
            found
        };
        self.inner.changed.notify_waiters();
        found
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn rollback(st: &mut State, pid: i32) {
    let Some(s) = st.sessions.get_mut(&pid) else {
        return;
    };
    s.in_tx = false;
    s.failed = false;
    s.pending.clear();
    let rows: Vec<i64> = std::mem::take(&mut s.locks).into_iter().collect();
    for row in rows {
        st.lock_owner.remove(&row);
    }
}

fn commit(st: &mut State, pid: i32) -> &'static str {
    let Some(s) = st.sessions.get_mut(&pid) else {
        return "COMMIT";
    };
    if s.failed {
        rollback(st, pid);
        return "ROLLBACK";
    }
    let pending = std::mem::take(&mut s.pending);
    st.committed.extend(pending);
    rollback(st, pid);
    "COMMIT"
}

async fn acquire_row(inner: &Inner, pid: i32, row: i64) -> Result<(), &'static str> {
    loop {
        let notified = {
            let mut guard = lock(&inner.state);
            let st: &mut State = &mut guard;
            let owner = st.lock_owner.get(&row).copied();
            let sess = st.sessions.entry(pid).or_default();
            if sess.cancel_requested {
                sess.cancel_requested = false;
                sess.waiting_for = None;
                return Err(sqlstate::QUERY_CANCELED);
            }
            match owner {
                None => {
                    sess.waiting_for = None;
                    sess.locks.insert(row);
                    st.lock_owner.insert(row, pid);
                    return Ok(());
                }
                Some(o) if o == pid => {
                    sess.waiting_for = None;
                    return Ok(());
                }
                Some(_) => {
                    sess.waiting_for = Some(row);
                    let mut fut = Box::pin(inner.changed.notified());
                    fut.as_mut().enable();
                    fut
                }
            }
        };
        notified.await;
    }
}

fn substitute(sql: &str, params: &[String]) -> String {
    let mut out = sql.to_owned();
    for (i, p) in params.iter().enumerate().rev() {
        out = out.replace(&format!("${}", i + 1), p);
    }
    out
}

fn last_int(words: &[&str]) -> Option<i64> {
    words
        .iter()
        .rev()
        .find_map(|w| w.trim_end_matches(';').parse().ok())
}

async fn execute(inner: &Inner, pid: i32, sql: &str) -> Outcome {
    let norm = sql.trim().trim_end_matches(';').to_ascii_lowercase();
    let words: Vec<&str> = norm.split_whitespace().collect();
    lock(&inner.state).log.push((pid, sql.trim().to_owned()));

    let autocommit = {
        let mut st = lock(&inner.state);
        let s = st.sessions.entry(pid).or_default();
        if s.failed && !matches!(words.first(), Some(&"rollback" | &"commit")) {
            return Outcome::Error(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block"
                    .into(),
            );
        }
        !s.in_tx
    };

    let result = match words.as_slice() {
        [] => Outcome::Command(String::new()),
        ["begin", ..] => {
            lock(&inner.state).sessions.entry(pid).or_default().in_tx = true;
            Outcome::Command("BEGIN".into())
        }
        ["commit", ..] => {
            let tag = commit(&mut lock(&inner.state), pid);
            inner.changed.notify_waiters();
            Outcome::Command(tag.into())
        }
        ["rollback", ..] => {
            rollback(&mut lock(&inner.state), pid);
            inner.changed.notify_waiters();
            Outcome::Command("ROLLBACK".into())
        }
        ["select", "1"] => Outcome::Rows(vec!["1".into()], "SELECT 1"),
        [
            "select",
            "balance",
            "from",
            "wallets",
            "where",
            "id",
            "=",
            id,
            rest @ ..,
        ] => {
            let Ok(id) = id.parse::<i64>() else {
                return Outcome::Error("22P02", format!("invalid id {id}"));
            };
            if rest.first() == Some(&"for")
                && let Err(code) = acquire_row(inner, pid, id).await
            {
                return fail(inner, pid, code);
            }
            let st = lock(&inner.state);
            let own = st
                .sessions
                .get(&pid)
                .and_then(|s| s.pending.get(&id))
                .copied();
            match own.or_else(|| st.committed.get(&id).copied()) {
                Some(b) => Outcome::Rows(vec![b.to_string()], "SELECT 1"),
                None => Outcome::Rows(Vec::new(), "SELECT 0"),
            }
        }
        ["update", "wallets", "set", "balance", "=", ..] => {
            let Some(id) = last_int(&words) else {
                return Outcome::Error("42601", "missing id".into());
            };
            if let Err(code) = acquire_row(inner, pid, id).await {
                return fail(inner, pid, code);
            }
            let mut st = lock(&inner.state);
            let current = st
                .sessions
                .get(&pid)
                .and_then(|s| s.pending.get(&id))
                .copied()
                .or_else(|| st.committed.get(&id).copied());
            let Some(current) = current else {
                return Outcome::Command("UPDATE 0".into());
            };
            let new = match &words[5..] {
                ["balance", "-", amount, ..] => match amount.parse::<i64>() {
                    Ok(a) => current - a,
                    Err(_) => return Outcome::Error("22P02", "bad amount".into()),
                },
                [value, ..] => match value.parse::<i64>() {
                    Ok(v) => v,
                    Err(_) => return Outcome::Error("22P02", "bad value".into()),
                },
                [] => return Outcome::Error("42601", "syntax error".into()),
            };
            if let Some(s) = st.sessions.get_mut(&pid) {
                s.pending.insert(id, new);
            }
            Outcome::Command("UPDATE 1".into())
        }
        _ => Outcome::Error("42601", format!("mock server cannot execute: {sql}")),
    };

    if autocommit && !matches!(result, Outcome::Error(..)) {
        let mut st = lock(&inner.state);
        let in_tx = st.sessions.get(&pid).is_some_and(|s| s.in_tx);
        if !in_tx {
            commit(&mut st, pid);
            drop(st);
            inner.changed.notify_waiters();
        }
    }
    result
}

fn fail(inner: &Inner, pid: i32, code: &'static str) -> Outcome {
    let mut guard = lock(&inner.state);
    let in_tx = guard.sessions.get(&pid).is_some_and(|s| s.in_tx);
    // Like PostgreSQL, an aborted transaction releases its locks immediately
    // and only waits for ROLLBACK to leave the failed state.
    rollback(&mut guard, pid);
    if in_tx && let Some(s) = guard.sessions.get_mut(&pid) {
        s.in_tx = true;
        s.failed = true;
    }
    drop(guard);
    inner.changed.notify_waiters();
    let message = if code == sqlstate::QUERY_CANCELED {
        "canceling statement due to user request"
    } else {
        "error"
    };
    Outcome::Error(code, message.into())
}

fn tx_status(inner: &Inner, pid: i32) -> TxStatus {
    let st = lock(&inner.state);
    match st.sessions.get(&pid) {
        Some(s) if s.failed => TxStatus::Failed,
        Some(s) if s.in_tx => TxStatus::InTransaction,
        _ => TxStatus::Idle,
    }
}

fn encode_outcome(out: &mut BytesMut, outcome: Outcome, with_description: bool) {
    match outcome {
        Outcome::Rows(rows, tag) => {
            if with_description {
                build::row_description(&[("balance", 20)]).encode(out);
            }
            for r in &rows {
                build::data_row(&[Some(r)]).encode(out);
            }
            build::command_complete(tag).encode(out);
        }
        Outcome::Command(tag) if tag.is_empty() => {
            crate::frame::Frame::new(b'I', bytes::Bytes::new()).encode(out);
        }
        Outcome::Command(tag) => build::command_complete(&tag).encode(out),
        Outcome::Error(code, msg) => build::error_response(code, &msg).encode(out),
    }
}

async fn serve(socket: TcpStream, inner: Arc<Inner>) -> WireResult<()> {
    let (rd, mut wr) = socket.into_split();
    let mut reader = FrameReader::new(rd, Peer::Client);
    loop {
        match reader.next_startup().await? {
            None => return Ok(()),
            Some(StartupPacket::SslRequest | StartupPacket::GssEncRequest) => {
                wr.write_all(b"N").await?;
            }
            Some(StartupPacket::CancelRequest { backend_pid, .. }) => {
                let mut st = lock(&inner.state);
                if let Some(s) = st.sessions.get_mut(&backend_pid)
                    && s.waiting_for.is_some()
                {
                    s.cancel_requested = true;
                }
                drop(st);
                inner.changed.notify_waiters();
                return Ok(());
            }
            Some(StartupPacket::Startup { .. }) => break,
        }
    }
    let pid = {
        let mut st = lock(&inner.state);
        st.next_pid += 1;
        let pid = st.next_pid;
        st.sessions.insert(pid, Session::default());
        pid
    };
    let mut out = BytesMut::new();
    build::authentication_ok().encode(&mut out);
    build::parameter_status("server_version", "17.0 (pgloom mock)").encode(&mut out);
    build::backend_key_data(pid, 0x5eed).encode(&mut out);
    build::ready_for_query(TxStatus::Idle).encode(&mut out);
    wr.write_all(&out).await?;
    out.clear();

    let result = serve_queries(&mut reader, &mut wr, &inner, pid).await;
    let mut st = lock(&inner.state);
    rollback(&mut st, pid);
    st.sessions.remove(&pid);
    drop(st);
    inner.changed.notify_waiters();
    result
}

async fn serve_queries(
    reader: &mut FrameReader<OwnedReadHalf>,
    wr: &mut OwnedWriteHalf,
    inner: &Inner,
    pid: i32,
) -> WireResult<()> {
    let mut statements: BTreeMap<String, String> = BTreeMap::new();
    let mut portals: BTreeMap<String, String> = BTreeMap::new();
    let mut skip_until_sync = false;
    let mut out = BytesMut::new();
    while let Some(frame) = reader.next().await? {
        if lock(&inner.state)
            .sessions
            .get(&pid)
            .is_some_and(|s| s.terminated)
        {
            return Ok(());
        }
        match frame.tag {
            tag::QUERY => {
                let sql = decode_query(&frame)?;
                let outcome = execute(inner, pid, &sql).await;
                encode_outcome(&mut out, outcome, true);
                build::ready_for_query(tx_status(inner, pid)).encode(&mut out);
            }
            tag::PARSE if !skip_until_sync => {
                let msg = ParseMessage::decode(&frame)?;
                statements.insert(msg.statement, msg.sql);
                build::parse_complete().encode(&mut out);
            }
            tag::BIND if !skip_until_sync => {
                let msg = BindMessage::decode(&frame)?;
                let sql = statements.get(&msg.statement).cloned().unwrap_or_default();
                let params: Vec<String> = msg
                    .params
                    .iter()
                    .map(|p| {
                        p.as_ref()
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_else(|| "NULL".into())
                    })
                    .collect();
                portals.insert(msg.portal, substitute(&sql, &params));
                build::bind_complete().encode(&mut out);
            }
            tag::DESCRIBE if !skip_until_sync => build::no_data().encode(&mut out),
            tag::EXECUTE if !skip_until_sync => {
                let msg = ExecuteMessage::decode(&frame)?;
                let sql = portals.get(&msg.portal).cloned().unwrap_or_default();
                let outcome = execute(inner, pid, &sql).await;
                if matches!(outcome, Outcome::Error(..)) {
                    skip_until_sync = true;
                }
                encode_outcome(&mut out, outcome, false);
            }
            tag::SYNC => {
                skip_until_sync = false;
                build::ready_for_query(tx_status(inner, pid)).encode(&mut out);
            }
            tag::TERMINATE => return Ok(()),
            _ => {}
        }
        if !reader.has_buffered_frame() {
            wr.write_all(&out).await?;
            out.clear();
        }
    }
    Ok(())
}

/// Minimal protocol-level client for driving the proxy in tests.
#[derive(Debug)]
pub struct MockClient {
    reader: FrameReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    backend_pid: i32,
}

impl MockClient {
    /// Connects, optionally probing for TLS first like `sslmode=prefer`.
    pub async fn connect(addr: SocketAddr, ssl_probe: bool) -> WireResult<Self> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;
        let (rd, mut writer) = socket.into_split();
        let mut reader = FrameReader::new(rd, Peer::Server);
        if ssl_probe {
            writer
                .write_all(&encode_startup_code(SSL_REQUEST_CODE))
                .await?;
            if reader.read_u8().await? != Some(b'N') {
                return Err(crate::error::WireError::Malformed {
                    tag: '\0',
                    reason: "expected 'N' in response to SSLRequest",
                });
            }
        }
        writer
            .write_all(&encode_startup(&[("user", "test"), ("database", "test")]))
            .await?;
        let mut backend_pid = 0;
        while let Some(frame) = reader.next().await? {
            match frame.tag {
                b'K' => backend_pid = crate::messages::BackendKeyData::decode(&frame)?.pid.0,
                b'Z' => break,
                _ => {}
            }
        }
        Ok(Self {
            reader,
            writer,
            backend_pid,
        })
    }

    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// Runs a simple query and collects the response through `ReadyForQuery`.
    pub async fn query(&mut self, sql: &str) -> WireResult<TurnResponse> {
        self.writer.write_all(&build::query(sql).to_bytes()).await?;
        self.read_response().await
    }

    /// Runs an extended-protocol round trip; `parse` controls whether the
    /// statement is (re)parsed or only bound from the server-side cache.
    pub async fn query_extended(
        &mut self,
        statement: &str,
        sql: &str,
        params: &[&str],
        parse: bool,
    ) -> WireResult<TurnResponse> {
        let mut out = BytesMut::new();
        if parse {
            build::parse(statement, sql, &[]).encode(&mut out);
        }
        let values: Vec<Option<&[u8]>> = params.iter().map(|p| Some(p.as_bytes())).collect();
        build::bind("", statement, &values).encode(&mut out);
        build::describe_portal("").encode(&mut out);
        build::execute("", 0).encode(&mut out);
        build::sync().encode(&mut out);
        self.writer.write_all(&out).await?;
        self.read_response().await
    }

    pub async fn terminate(mut self) -> WireResult<()> {
        self.writer
            .write_all(&build::terminate().to_bytes())
            .await?;
        self.writer.shutdown().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> WireResult<TurnResponse> {
        let mut collector = ResponseCollector::new();
        while let Some(frame) = self.reader.next().await? {
            if collector.observe(&frame)? {
                return Ok(collector.finish());
            }
        }
        Err(crate::error::WireError::UnexpectedEof(Peer::Server))
    }
}
