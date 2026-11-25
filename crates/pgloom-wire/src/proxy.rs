//! Transparent PostgreSQL L7 proxy with scheduler-controlled turn gates.
//!
//! Every client connection is relayed to the upstream server byte for byte.
//! While an episode is open on the [`Gate`], each non-noise [`WireTurn`] is
//! held until the controller grants it, and the server's complete response
//! (through `ReadyForQuery`) is held until the controller releases it. These
//! two hand-offs let a scheduler decide, turn by turn, which connection makes
//! progress, without the client or server noticing anything but latency.
//!
//! Dropping a [`Permit`] or [`Release`] lets the connection continue
//! unsupervised, so a controller can always abandon an episode safely.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{BufMut, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, trace, warn};

use crate::classify::TurnMeta;
use crate::error::{Peer, WireError, WireResult};
use crate::frame::{CANCEL_REQUEST_CODE, StartupPacket};
use crate::io::FrameReader;
use crate::messages::{
    BackendKeyData, BackendPid, ColumnDesc, TxStatus, backend_tag, decode_ready_for_query,
};
use crate::response::{PortalLayout, ResponseCollector, TurnResponse};
use crate::turn::{
    BoundStatement, ClientUnit, DescribeTarget, FlushStep, RegistryRollback, ResultShape,
    Terminator, TurnAssembler, WireTurn,
};

/// Identifier of a proxied client connection, unique per proxy instance.
pub type ConnId = u64;

/// Description of a turn waiting at the gate.
#[derive(Debug, Clone)]
pub struct TurnInfo {
    pub conn: ConnId,
    /// Per-connection turn sequence number.
    pub seq: u64,
    pub backend_pid: Option<BackendPid>,
    /// Transaction status of the connection before this turn.
    pub tx_status: TxStatus,
    pub statements: Vec<BoundStatement>,
    pub meta: TurnMeta,
}

impl TurnInfo {
    pub fn sql_text(&self) -> String {
        self.statements
            .iter()
            .map(|s| s.sql.trim().trim_end_matches(';').trim())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Grants a held turn permission to reach the server.
#[derive(Debug)]
pub struct Permit(oneshot::Sender<()>);

impl Permit {
    pub fn grant(self) {
        let _ = self.0.send(());
    }
}

/// Delivers a held server response to the client.
#[derive(Debug)]
pub struct Release(oneshot::Sender<()>);

impl Release {
    pub fn release(self) {
        let _ = self.0.send(());
    }
}

/// A turn held at the gate.
#[derive(Debug)]
pub struct PendingTurn {
    pub info: TurnInfo,
    pub permit: Permit,
}

/// A gated turn whose response has fully arrived from the server.
#[derive(Debug)]
pub struct FinishedTurn {
    pub conn: ConnId,
    pub seq: u64,
    pub response: TurnResponse,
    pub release: Release,
}

/// Notifications delivered to the episode controller.
#[derive(Debug)]
pub enum GateEvent {
    TurnReady(PendingTurn),
    TurnFinished(FinishedTurn),
    ConnectionClosed {
        conn: ConnId,
        backend_pid: Option<BackendPid>,
    },
}

type EventTx = mpsc::UnboundedSender<GateEvent>;

/// Switch between transparent relaying and supervised episodes.
#[derive(Debug, Clone, Default)]
pub struct Gate {
    slot: Arc<Mutex<Option<EventTx>>>,
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts supervising turns; replaces any previous episode.
    pub fn open_episode(&self) -> mpsc::UnboundedReceiver<GateEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self.lock() = Some(tx);
        rx
    }

    /// Stops supervising new turns. Turns already held are released when the
    /// controller drops their permits.
    pub fn close_episode(&self) {
        *self.lock() = None;
    }

    pub fn is_open(&self) -> bool {
        self.lock().as_ref().is_some_and(|tx| !tx.is_closed())
    }

    fn sender(&self) -> Option<EventTx> {
        self.lock().as_ref().filter(|tx| !tx.is_closed()).cloned()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<EventTx>> {
        // The guarded value is a plain channel handle; a poisoned lock cannot
        // leave it in an inconsistent state.
        self.slot.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Upstream server address and listener settings.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub listen: SocketAddr,
    /// `host:port` of the real PostgreSQL server.
    pub upstream: String,
}

/// A running proxy. Dropping the handle stops it and all its sessions.
#[derive(Debug)]
pub struct ProxyHandle {
    local_addr: SocketAddr,
    gate: Gate,
    task: JoinHandle<()>,
}

impl ProxyHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    pub async fn shutdown(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Binds the listener and starts accepting connections.
pub async fn spawn_proxy(config: ProxyConfig) -> WireResult<ProxyHandle> {
    let listener = TcpListener::bind(config.listen).await?;
    let local_addr = listener.local_addr()?;
    let gate = Gate::new();
    let upstream: Arc<str> = Arc::from(config.upstream);
    let task_gate = gate.clone();
    let task = tokio::spawn(async move {
        let next_conn = AtomicU64::new(1);
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (socket, peer) = match accepted {
                        Ok(pair) => pair,
                        Err(err) => {
                            warn!(%err, "accept failed");
                            continue;
                        }
                    };
                    let conn = next_conn.fetch_add(1, Ordering::Relaxed);
                    let gate = task_gate.clone();
                    let upstream = Arc::clone(&upstream);
                    sessions.spawn(async move {
                        debug!(conn, %peer, "client connected");
                        match run_session(socket, &upstream, &gate, conn).await {
                            Ok(()) => debug!(conn, "session closed"),
                            Err(err) => debug!(conn, %err, "session ended with error"),
                        }
                    });
                }
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
            }
        }
    });
    Ok(ProxyHandle {
        local_addr,
        gate,
        task,
    })
}

/// Connection state shared between the client and server halves.
#[derive(Debug)]
struct Shared {
    tx_status: AtomicU8,
}

impl Shared {
    fn tx_status(&self) -> TxStatus {
        TxStatus::from_byte(self.tx_status.load(Ordering::Acquire)).unwrap_or(TxStatus::Idle)
    }

    fn set_tx_status(&self, status: TxStatus) {
        self.tx_status.store(status.as_byte(), Ordering::Release);
    }
}

/// What the server half should do with the response to a forwarded turn.
#[derive(Debug)]
struct Expectation {
    seq: u64,
    gated: Option<EventTx>,
    describes: VecDeque<DescribeTarget>,
    result_shapes: VecDeque<ResultShape>,
    flush_steps: VecDeque<FlushStep>,
    reset_statements: Vec<String>,
    reset_portals: Vec<String>,
    clear_all_statements: bool,
    /// A `Flush` or `ExecuteFlush` round trip: completes once every queued
    /// protocol step (or an error) arrives, since no `ReadyForQuery` follows.
    is_flush_pipeline: bool,
    /// An `Execute("COPY ...")`-then-`Flush` round trip: stream errors as well
    /// as `CopyInResponse` / `CopyOutResponse` so the client sends `Sync`.
    is_copy_flush: bool,
}

/// Completion report sent from the server relay back to the client relay once
/// an [`Expectation`] finishes.
#[derive(Debug, Clone, Copy)]
struct TurnDone {
    had_error: bool,
    parse_ok: usize,
    bind_ok: usize,
    close_ok: usize,
}

async fn run_session(
    client: TcpStream,
    upstream: &str,
    gate: &Gate,
    conn: ConnId,
) -> WireResult<()> {
    client.set_nodelay(true)?;
    let (crd, mut cwr) = client.into_split();
    let mut creader = FrameReader::new(crd, Peer::Client);

    let startup_raw = loop {
        match creader.next_startup().await? {
            None => return Ok(()),
            Some(StartupPacket::SslRequest | StartupPacket::GssEncRequest) => {
                // The loopback hop between the application and PgLoom is plaintext.
                cwr.write_all(b"N").await?;
            }
            Some(StartupPacket::CancelRequest {
                backend_pid,
                secret_key,
            }) => {
                forward_cancel(upstream, backend_pid, &secret_key).await;
                return Ok(());
            }
            Some(StartupPacket::Startup { raw, .. }) => break raw,
        }
    };

    let server = TcpStream::connect(upstream)
        .await
        .map_err(|e| WireError::Upstream(format!("{upstream}: {e}")))?;
    server.set_nodelay(true)?;
    let (srd, mut swr) = server.into_split();
    let mut sreader = FrameReader::new(srd, Peer::Server);
    swr.write_all(&startup_raw).await?;

    // Authentication is relayed verbatim until the first ReadyForQuery.
    let mut backend_pid = None;
    let initial_status = loop {
        tokio::select! {
            frame = sreader.next() => {
                let Some(frame) = frame? else { return Ok(()) };
                if frame.tag == backend_tag::BACKEND_KEY_DATA {
                    backend_pid = Some(BackendKeyData::decode(&frame)?.pid);
                }
                cwr.write_all(&frame.to_bytes()).await?;
                if frame.tag == backend_tag::READY_FOR_QUERY {
                    break decode_ready_for_query(&frame)?;
                }
            }
            frame = creader.next() => {
                let Some(frame) = frame? else { return Ok(()) };
                swr.write_all(&frame.to_bytes()).await?;
            }
        }
    };
    debug!(conn, ?backend_pid, "session ready");

    let shared = Arc::new(Shared {
        tx_status: AtomicU8::new(initial_status.as_byte()),
    });
    let (expect_tx, expect_rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = mpsc::unbounded_channel();
    let server_half = tokio::spawn(relay_server(
        sreader,
        cwr,
        expect_rx,
        done_tx,
        Arc::clone(&shared),
        conn,
    ));
    let client_result = relay_client(
        creader,
        swr,
        ClientCtx {
            gate,
            conn,
            backend_pid,
            shared: &shared,
            expect_tx,
            done_rx,
        },
    )
    .await;
    let server_result = server_half
        .await
        .unwrap_or_else(|e| Err(WireError::Upstream(format!("relay task failed: {e}"))));
    if let Some(tx) = gate.sender() {
        let _ = tx.send(GateEvent::ConnectionClosed { conn, backend_pid });
    }
    client_result.and(server_result)
}

async fn forward_cancel(upstream: &str, backend_pid: i32, secret_key: &[u8]) {
    let mut packet = BytesMut::with_capacity(12 + secret_key.len());
    packet.put_i32((12 + secret_key.len()) as i32);
    packet.put_i32(CANCEL_REQUEST_CODE);
    packet.put_i32(backend_pid);
    packet.put_slice(secret_key);
    match TcpStream::connect(upstream).await {
        Ok(mut s) => {
            if let Err(err) = s.write_all(&packet).await {
                warn!(%err, "failed to forward CancelRequest");
            }
        }
        Err(err) => warn!(%err, "failed to connect upstream for CancelRequest"),
    }
}

struct ClientCtx<'a> {
    gate: &'a Gate,
    conn: ConnId,
    backend_pid: Option<BackendPid>,
    shared: &'a Shared,
    expect_tx: mpsc::UnboundedSender<Expectation>,
    done_rx: mpsc::UnboundedReceiver<TurnDone>,
}

/// A turn waiting for its permit.
struct Held {
    turn: WireTurn,
    seq: u64,
    events: EventTx,
    permit: oneshot::Receiver<()>,
}

fn should_gate(turn: &WireTurn, status: TxStatus) -> bool {
    if !turn.terminator.is_schedulable() || turn.meta.is_noise() {
        return false;
    }
    // A stray COMMIT/ROLLBACK outside a transaction block (pool hygiene) is a no-op.
    !(turn.meta.is_tx_end_only() && status == TxStatus::Idle)
}

async fn relay_client(
    mut creader: FrameReader<OwnedReadHalf>,
    mut swr: OwnedWriteHalf,
    mut ctx: ClientCtx<'_>,
) -> WireResult<()> {
    let mut assembler = TurnAssembler::new();
    let mut queue: VecDeque<ClientUnit> = VecDeque::new();
    let mut held: Option<Held> = None;
    let mut in_flight: usize = 0;
    let mut in_flight_rollbacks: VecDeque<(RegistryRollback, bool)> = VecDeque::new();
    let mut pending_resets = PendingResets::default();
    let mut seq: u64 = 0;

    loop {
        while held.is_none() {
            match queue.front() {
                Some(ClientUnit::CopyFrame(_)) => {
                    let Some(ClientUnit::CopyFrame(frame)) = queue.pop_front() else {
                        unreachable!();
                    };
                    swr.write_all(&frame.to_bytes()).await?;
                }
                Some(ClientUnit::Turn(_)) if in_flight == 0 => {
                    let Some(ClientUnit::Turn(turn)) = queue.pop_front() else {
                        unreachable!();
                    };
                    let turn = *turn;
                    seq += 1;
                    let status = ctx.shared.tx_status();
                    let events = ctx.gate.sender().filter(|_| should_gate(&turn, status));
                    match events {
                        Some(events) => {
                            let (permit_tx, permit_rx) = oneshot::channel();
                            let info = TurnInfo {
                                conn: ctx.conn,
                                seq,
                                backend_pid: ctx.backend_pid,
                                tx_status: status,
                                statements: turn.statements.clone(),
                                meta: turn.meta.clone(),
                            };
                            trace!(conn = ctx.conn, seq, sql = %info.sql_text(), "turn held at gate");
                            let pending = PendingTurn {
                                info,
                                permit: Permit(permit_tx),
                            };
                            if events.send(GateEvent::TurnReady(pending)).is_ok() {
                                held = Some(Held {
                                    turn,
                                    seq,
                                    events,
                                    permit: permit_rx,
                                });
                            } else {
                                let is_sql = turn.terminator == Terminator::Query
                                    || !turn.statements.is_empty();
                                let rollback = turn.rollback.clone();
                                if forward(
                                    &mut swr,
                                    &ctx.expect_tx,
                                    &mut pending_resets,
                                    turn,
                                    seq,
                                    None,
                                )
                                .await?
                                {
                                    in_flight += 1;
                                    in_flight_rollbacks.push_back((rollback, is_sql));
                                }
                            }
                        }
                        None => {
                            let terminate = turn.terminator == Terminator::Terminate;
                            let is_sql =
                                turn.terminator == Terminator::Query || !turn.statements.is_empty();
                            let rollback = turn.rollback.clone();
                            if forward(
                                &mut swr,
                                &ctx.expect_tx,
                                &mut pending_resets,
                                turn,
                                seq,
                                None,
                            )
                            .await?
                            {
                                in_flight += 1;
                                in_flight_rollbacks.push_back((rollback, is_sql));
                            }
                            if terminate {
                                return Ok(());
                            }
                        }
                    }
                }
                _ => break,
            }
        }

        tokio::select! {
            frame = creader.next() => {
                let Some(frame) = frame? else { return Ok(()) };
                if let Some(unit) = assembler.push(frame)? {
                    queue.push_back(unit);
                }
            }
            granted = async {
                match held.as_mut() {
                    Some(h) => (&mut h.permit).await,
                    None => std::future::pending().await,
                }
            }, if held.is_some() => {
                let Some(h) = held.take() else { continue };
                // A dropped permit means the controller abandoned the episode:
                // forward the turn unsupervised.
                let events = granted.ok().map(|()| h.events);
                let is_sql =
                    h.turn.terminator == Terminator::Query || !h.turn.statements.is_empty();
                let rollback = h.turn.rollback.clone();
                if forward(
                    &mut swr,
                    &ctx.expect_tx,
                    &mut pending_resets,
                    h.turn,
                    h.seq,
                    events,
                )
                .await?
                {
                    in_flight += 1;
                    in_flight_rollbacks.push_back((rollback, is_sql));
                }
            }
            done = ctx.done_rx.recv() => {
                match done {
                    Some(done) => {
                        in_flight = in_flight.saturating_sub(1);
                        if let Some((rollback, is_sql)) = in_flight_rollbacks.pop_front()
                            && done.had_error
                            && !rollback.is_empty()
                        {
                            assembler.rollback(
                                rollback,
                                done.parse_ok,
                                done.bind_ok,
                                done.close_ok,
                                is_sql,
                            );
                        }
                    }
                    None => return Ok(()),
                }
            }
        }
    }
}

#[derive(Default)]
struct PendingResets {
    statements: Vec<String>,
    portals: Vec<String>,
    clear_all_statements: bool,
}

/// Forwards `turn` to the upstream server. Returns `true` when an
/// [`Expectation`] was registered on `expect_tx`.
async fn forward(
    swr: &mut OwnedWriteHalf,
    expect_tx: &mpsc::UnboundedSender<Expectation>,
    pending_resets: &mut PendingResets,
    turn: WireTurn,
    seq: u64,
    gated: Option<EventTx>,
) -> WireResult<bool> {
    pending_resets.statements.extend(turn.reset_statements);
    pending_resets.portals.extend(turn.reset_portals);
    pending_resets.clear_all_statements |= turn.clear_all_statements;
    let is_flush_pipeline = turn.terminator.is_flush_pipeline() && !turn.flush_steps.is_empty();
    let is_copy_flush = turn.terminator == Terminator::CopyFlush;
    let registered = turn.terminator.expects_ready_for_query() || is_flush_pipeline;
    if registered {
        // Must be registered before the bytes leave, so the response can never
        // overtake its expectation.
        let _ = expect_tx.send(Expectation {
            seq,
            gated,
            describes: turn.describes,
            result_shapes: turn.result_shapes,
            flush_steps: turn.flush_steps,
            reset_statements: std::mem::take(&mut pending_resets.statements),
            reset_portals: std::mem::take(&mut pending_resets.portals),
            clear_all_statements: std::mem::take(&mut pending_resets.clear_all_statements),
            is_flush_pipeline,
            is_copy_flush,
        });
    }
    swr.write_all(&turn.raw).await?;
    Ok(registered)
}

struct InFlight {
    expectation: Expectation,
    collector: ResponseCollector,
    held: BytesMut,
    streaming: bool,
    saved_statements: Vec<(String, Vec<ColumnDesc>)>,
    saved_all_statements: Option<HashMap<String, Vec<ColumnDesc>>>,
}

async fn relay_server(
    mut sreader: FrameReader<OwnedReadHalf>,
    mut cwr: OwnedWriteHalf,
    mut expect_rx: mpsc::UnboundedReceiver<Expectation>,
    done_tx: mpsc::UnboundedSender<TurnDone>,
    shared: Arc<Shared>,
    conn: ConnId,
) -> WireResult<()> {
    let mut pending: VecDeque<Expectation> = VecDeque::new();
    let mut current: Option<InFlight> = None;
    // Row layout of prepared statements and bound portals, learned from
    // `Describe` responses, for executions that receive `DataRow`s without a
    // `RowDescription`. Lookups only, so `HashMap` iteration order never matters.
    let mut statement_columns: HashMap<String, Vec<ColumnDesc>> = HashMap::new();
    let mut portal_columns: HashMap<String, PortalLayout> = HashMap::new();
    let mut out = BytesMut::new();

    while let Some(frame) = sreader.next().await? {
        while let Ok(e) = expect_rx.try_recv() {
            pending.push_back(e);
        }
        if current.is_none() {
            current = pending.pop_front().map(|expectation| {
                let saved_all_statements = if expectation.clear_all_statements {
                    Some(std::mem::take(&mut statement_columns))
                } else {
                    None
                };
                let mut saved_statements = Vec::new();
                for stmt in &expectation.reset_statements {
                    if let Some(prev) = statement_columns.remove(stmt) {
                        saved_statements.push((stmt.clone(), prev));
                    }
                }
                for portal in &expectation.reset_portals {
                    portal_columns.remove(portal);
                }
                let mut stmt_layouts = HashMap::new();
                let mut port_layouts = HashMap::new();
                for shape in &expectation.result_shapes {
                    if let Some(cols) = statement_columns.get(&shape.statement) {
                        stmt_layouts.insert(shape.statement.clone(), cols.clone());
                    }
                    if let Some(entry) = portal_columns.get(&shape.portal) {
                        port_layouts.insert(shape.portal.clone(), entry.clone());
                    }
                }
                let collector = ResponseCollector::new()
                    .with_layouts(stmt_layouts, port_layouts)
                    .with_describes(expectation.describes.clone())
                    .with_result_shapes(expectation.result_shapes.clone())
                    .with_flush_steps(expectation.flush_steps.clone());
                InFlight {
                    expectation,
                    collector,
                    held: BytesMut::new(),
                    streaming: false,
                    saved_statements,
                    saved_all_statements,
                }
            });
        }

        let Some(cur) = current.as_mut() else {
            // Unsolicited message (NoticeResponse, ParameterStatus, ...).
            frame.encode(&mut out);
            flush_if_drained(&sreader, &mut cwr, &mut out).await?;
            continue;
        };

        let is_ready = cur.collector.observe(&frame)?
            || (cur.expectation.is_flush_pipeline && cur.collector.flush_done());
        let supervised = cur.expectation.gated.is_some();
        if supervised && !cur.streaming {
            frame.encode(&mut cur.held);
            if matches!(
                frame.tag,
                backend_tag::COPY_IN_RESPONSE
                    | backend_tag::COPY_OUT_RESPONSE
                    | backend_tag::COPY_BOTH_RESPONSE
            ) || (cur.expectation.is_copy_flush && frame.tag == backend_tag::ERROR_RESPONSE)
            {
                // The client must see CopyInResponse / CopyOutResponse (or an
                // extended-COPY error) before sending CopyData and the closing
                // Sync; stream intermediate frames and hold back ReadyForQuery.
                out.extend_from_slice(&cur.held.split());
                cur.streaming = true;
            }
        } else if supervised && is_ready {
            frame.encode(&mut cur.held);
        } else {
            frame.encode(&mut out);
        }

        if !is_ready {
            flush_if_drained(&sreader, &mut cwr, &mut out).await?;
            continue;
        }

        let Some(done) = current.take() else { continue };
        let parse_ok = done.collector.parse_ok();
        let bind_ok = done.collector.bind_ok();
        let close_ok = done.collector.close_ok();
        for (target, cols) in done.collector.described() {
            match target {
                DescribeTarget::Statement(name) => {
                    statement_columns.insert(name.clone(), cols.clone());
                }
                DescribeTarget::Portal { portal, statement } => {
                    portal_columns.insert(portal.clone(), (statement.clone(), cols.clone()));
                }
            }
        }
        let response = done.collector.finish();
        let had_error = response.error.is_some();
        if had_error && parse_ok == 0 && close_ok == 0 {
            if let Some(prev_all) = done.saved_all_statements {
                statement_columns = prev_all;
            }
            for (stmt, cols) in done.saved_statements {
                statement_columns.insert(stmt, cols);
            }
        }
        if let Some(status) = response.tx_status {
            shared.set_tx_status(status);
            portal_columns.remove("");
            if !status.in_transaction() {
                portal_columns.clear();
            }
        }
        let report_done = TurnDone {
            had_error,
            parse_ok,
            bind_ok,
            close_ok,
        };
        if let Some(events) = done.expectation.gated {
            flush(&mut cwr, &mut out).await?;
            let (release_tx, release_rx) = oneshot::channel();
            let finished = FinishedTurn {
                conn,
                seq: done.expectation.seq,
                response,
                release: Release(release_tx),
            };
            if events.send(GateEvent::TurnFinished(finished)).is_ok() {
                // Dropped release means the episode was abandoned: deliver anyway.
                let _ = release_rx.await;
            }
            out.extend_from_slice(&done.held);
            flush(&mut cwr, &mut out).await?;
            let _ = done_tx.send(report_done);
        } else {
            flush_if_drained(&sreader, &mut cwr, &mut out).await?;
            let _ = done_tx.send(report_done);
        }
    }
    flush(&mut cwr, &mut out).await?;
    Ok(())
}

async fn flush_if_drained(
    sreader: &FrameReader<OwnedReadHalf>,
    cwr: &mut OwnedWriteHalf,
    out: &mut BytesMut,
) -> WireResult<()> {
    if !sreader.has_buffered_frame() {
        flush(cwr, out).await?;
    }
    Ok(())
}

async fn flush(cwr: &mut OwnedWriteHalf, out: &mut BytesMut) -> WireResult<()> {
    if !out.is_empty() {
        cwr.write_all(out).await?;
        out.clear();
    }
    Ok(())
}
