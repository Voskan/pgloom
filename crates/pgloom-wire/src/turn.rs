//! Grouping of client messages into scheduler-visible `WireTurn`s.
//!
//! A turn is the unit the scheduler interleaves: a complete simple query
//! (`'Q'`) or an extended-protocol pipeline ending in `Sync` (`'S'`).
//! PostgreSQL produces exactly one `ReadyForQuery` per turn, so a turn's
//! completion is observable on the backend stream.

use std::collections::{HashMap, VecDeque};

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::classify::{
    SessionSqlOp, StatementKind, TurnMeta, classify_sql, is_copy_sql, session_sql_ops,
};
use crate::error::WireResult;
use crate::frame::Frame;
use crate::messages::{
    BindMessage, ExecuteMessage, FormatCode, ObjectRef, ParseMessage, decode_query, tag,
};
use crate::values::render_value;

/// Expected backend response step for an extended-query `Flush` turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushStep {
    ParseComplete,
    BindComplete,
    CloseComplete,
    DescribeStatement,
    DescribePortal,
    Execute,
}

/// Reversible mutation applied to [`PreparedStatementRegistry`] during a turn,
/// used to roll back optimistic updates if PostgreSQL rejects the turn.
#[derive(Debug, Clone)]
pub struct RegistryRollback {
    ops: Vec<RegistryOp>,
}

impl RegistryRollback {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

#[derive(Debug, Clone)]
enum RegistryOp {
    Parse {
        index: usize,
        name: String,
        previous: Option<PreparedStatement>,
    },
    Bind {
        index: usize,
        name: String,
        previous: Option<Portal>,
    },
    CloseStatement {
        index: usize,
        name: String,
        previous: Option<PreparedStatement>,
    },
    ClosePortal {
        index: usize,
        name: String,
        previous: Option<Portal>,
    },
    SqlClearStatements {
        previous: HashMap<String, PreparedStatement>,
    },
    SqlDeallocate {
        name: String,
        previous: Option<PreparedStatement>,
    },
    SqlPrepare {
        name: String,
        previous: Option<PreparedStatement>,
    },
}

/// A statement that a turn actually executes, with rendered parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundStatement {
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
}

/// Object inspected by a `Describe` (`'D'`) message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement(String),
    Portal {
        portal: String,
        /// Backing prepared statement when the portal was bound on this
        /// connection.
        statement: Option<String>,
    },
}

/// How a turn was terminated by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminator {
    /// `'Q'`: one `ReadyForQuery` follows.
    Query,
    /// `'S'`: one `ReadyForQuery` follows.
    Sync,
    /// `'F'`: legacy function call; one `ReadyForQuery` follows.
    FunctionCall,
    /// `'H'` after executing `COPY`: the client waits for `CopyInResponse` /
    /// `CopyOutResponse` before sending `CopyData` and the closing `Sync` that
    /// produces `ReadyForQuery`.
    CopyFlush,
    /// `'H'` after one or more non-COPY `Execute` messages (e.g. cursor/portal
    /// fetches with `max_rows > 0` or pipeline mode): no `ReadyForQuery`
    /// follows, but the turn executes SQL and is gated by the scheduler.
    ExecuteFlush,
    /// `'H'` with no pending `Execute`: responses are flushed, no `ReadyForQuery`.
    Flush,
    /// `'X'`: the client is closing the connection.
    Terminate,
}

impl Terminator {
    pub fn expects_ready_for_query(self) -> bool {
        matches!(
            self,
            Terminator::Query | Terminator::Sync | Terminator::FunctionCall | Terminator::CopyFlush
        )
    }

    /// True when the turn may be held at the scheduler gate.
    pub fn is_schedulable(self) -> bool {
        self.expects_ready_for_query() || self == Terminator::ExecuteFlush
    }

    /// True when the turn ends with `Flush` rather than `ReadyForQuery`.
    pub fn is_flush_pipeline(self) -> bool {
        matches!(self, Terminator::Flush | Terminator::ExecuteFlush)
    }
}

/// A batch of client messages forwarded upstream as one atomic step.
#[derive(Debug, Clone)]
pub struct WireTurn {
    pub raw: Bytes,
    pub frame_count: usize,
    pub terminator: Terminator,
    pub statements: Vec<BoundStatement>,
    pub meta: TurnMeta,
    /// Objects whose `Describe` this turn sends, in wire order.
    pub describes: VecDeque<DescribeTarget>,
    /// Portal, statement and result formats of each `Execute` in wire order.
    pub result_shapes: VecDeque<ResultShape>,
    /// Expected backend responses for a `Flush` / `ExecuteFlush` turn.
    pub flush_steps: VecDeque<FlushStep>,
    /// Statements re-parsed or closed in this turn (invalidating cached layouts).
    pub reset_statements: Vec<String>,
    /// Portals re-bound or closed in this turn (invalidating cached layouts).
    pub reset_portals: Vec<String>,
    /// True when SQL `DISCARD ALL` / `DISCARD PLANS` / `DEALLOCATE ALL` ran.
    pub clear_all_statements: bool,
    /// Reversible registry mutations applied when assembling this turn.
    pub rollback: RegistryRollback,
}

/// How the rows of an executed portal are encoded, as requested by `Bind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultShape {
    pub portal: String,
    pub statement: String,
    pub formats: Vec<FormatCode>,
}

impl WireTurn {
    /// Human-readable SQL of the executed statements.
    pub fn sql_text(&self) -> String {
        self.statements
            .iter()
            .map(|s| s.sql.trim().trim_end_matches(';').trim())
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Output of the assembler.
#[derive(Debug, Clone)]
pub enum ClientUnit {
    Turn(Box<WireTurn>),
    /// `CopyData`/`CopyDone`/`CopyFail`, which belong to an in-flight turn.
    CopyFrame(Frame),
}

#[derive(Debug, Clone)]
struct PreparedStatement {
    sql: String,
    param_types: Vec<u32>,
}

#[derive(Debug, Clone)]
struct Portal {
    sql: String,
    params: Vec<String>,
    shape: ResultShape,
    known: bool,
}

/// Per-connection cache of prepared statements and portals.
///
/// Drivers such as `pgx` and `sqlx` send `Parse` once per connection and then
/// only `Bind("stmt") + Execute + Sync`, so SQL text must be recovered from
/// earlier turns. Lookups only; never iterated, so `HashMap` is safe here.
#[derive(Debug, Default)]
pub struct PreparedStatementRegistry {
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
}

impl PreparedStatementRegistry {
    pub fn sql_of(&self, statement: &str) -> Option<&str> {
        self.statements.get(statement).map(|s| s.sql.as_str())
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    fn on_parse(&mut self, msg: ParseMessage) -> Option<PreparedStatement> {
        self.statements.insert(
            msg.statement,
            PreparedStatement {
                sql: msg.sql,
                param_types: msg.param_types,
            },
        )
    }

    fn on_bind(&mut self, msg: &BindMessage) -> Option<Portal> {
        let (sql, types, known) = match self.statements.get(&msg.statement) {
            Some(s) => (s.sql.clone(), s.param_types.as_slice(), true),
            None => (
                format!("<unknown prepared statement \"{}\">", msg.statement),
                &[][..],
                false,
            ),
        };
        let params = msg
            .params
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let oid = types.get(i).copied().unwrap_or(0);
                render_value(v.as_deref(), msg.format_of(i), oid)
            })
            .collect();
        let shape = ResultShape {
            portal: msg.portal.clone(),
            statement: msg.statement.clone(),
            formats: msg.result_formats.clone(),
        };
        self.portals.insert(
            msg.portal.clone(),
            Portal {
                sql,
                params,
                shape,
                known,
            },
        )
    }

    fn on_close_statement(&mut self, name: &str) -> Option<PreparedStatement> {
        self.statements.remove(name)
    }

    fn on_close_portal(&mut self, name: &str) -> Option<Portal> {
        self.portals.remove(name)
    }

    fn restore_statement(&mut self, name: String, previous: Option<PreparedStatement>) {
        match previous {
            Some(prev) => {
                self.statements.insert(name, prev);
            }
            None => {
                self.statements.remove(&name);
            }
        }
    }

    fn restore_portal(&mut self, name: String, previous: Option<Portal>) {
        match previous {
            Some(prev) => {
                self.portals.insert(name, prev);
            }
            None => {
                self.portals.remove(&name);
            }
        }
    }

    fn portal(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }
}

/// Accumulates client frames and emits complete turns.
#[derive(Debug, Default)]
pub struct TurnAssembler {
    registry: PreparedStatementRegistry,
    pending: BytesMut,
    pending_frames: usize,
    statements: Vec<BoundStatement>,
    meta: TurnMeta,
    has_execute: bool,
    has_copy_execute: bool,
    last_turn_extended_copy: bool,
    awaiting_copy_sync: bool,
    describes: VecDeque<DescribeTarget>,
    result_shapes: VecDeque<ResultShape>,
    flush_steps: VecDeque<FlushStep>,
    reset_statements: Vec<String>,
    reset_portals: Vec<String>,
    clear_all_statements: bool,
    rollback_ops: Vec<RegistryOp>,
    parse_count: usize,
    bind_count: usize,
    close_count: usize,
}

impl TurnAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn registry(&self) -> &PreparedStatementRegistry {
        &self.registry
    }

    /// True when frames are buffered without a terminator yet.
    pub fn has_pending(&self) -> bool {
        self.pending_frames > 0
    }

    /// Reverts optimistic prepared-statement / portal updates for operations
    /// that did not complete before an `ErrorResponse`.
    pub fn rollback(
        &mut self,
        rollback: RegistryRollback,
        parse_ok: usize,
        bind_ok: usize,
        close_ok: usize,
        sql_failed: bool,
    ) {
        for op in rollback.ops.into_iter().rev() {
            match op {
                RegistryOp::Parse {
                    index,
                    name,
                    previous,
                } if index >= parse_ok => {
                    self.registry.restore_statement(name, previous);
                }
                RegistryOp::Bind {
                    index,
                    name,
                    previous,
                } if index >= bind_ok => {
                    self.registry.restore_portal(name, previous);
                }
                RegistryOp::CloseStatement {
                    index,
                    name,
                    previous,
                } if index >= close_ok => {
                    self.registry.restore_statement(name, previous);
                }
                RegistryOp::ClosePortal {
                    index,
                    name,
                    previous,
                } if index >= close_ok => {
                    self.registry.restore_portal(name, previous);
                }
                RegistryOp::SqlClearStatements { previous } if sql_failed => {
                    self.registry.statements = previous;
                }
                RegistryOp::SqlDeallocate { name, previous } if sql_failed => {
                    self.registry.restore_statement(name, previous);
                }
                RegistryOp::SqlPrepare { name, previous } if sql_failed => {
                    self.registry.restore_statement(name, previous);
                }
                _ => {}
            }
        }
    }

    fn apply_session_sql(&mut self, sql: &str) {
        for op in session_sql_ops(sql) {
            match op {
                SessionSqlOp::ClearStatements => {
                    let previous = std::mem::take(&mut self.registry.statements);
                    self.clear_all_statements = true;
                    self.rollback_ops
                        .push(RegistryOp::SqlClearStatements { previous });
                }
                SessionSqlOp::DeallocateStatement(name) => {
                    let previous = self.registry.on_close_statement(&name);
                    self.reset_statements.push(name.clone());
                    self.rollback_ops
                        .push(RegistryOp::SqlDeallocate { name, previous });
                }
                SessionSqlOp::PrepareStatement { name, sql } => {
                    let previous = self.registry.on_parse(ParseMessage {
                        statement: name.clone(),
                        sql,
                        param_types: Vec::new(),
                    });
                    self.reset_statements.push(name.clone());
                    self.rollback_ops
                        .push(RegistryOp::SqlPrepare { name, previous });
                }
            }
        }
    }

    /// Feeds one client frame; returns a unit once one is complete.
    pub fn push(&mut self, frame: Frame) -> WireResult<Option<ClientUnit>> {
        if matches!(frame.tag, tag::COPY_DATA | tag::COPY_DONE | tag::COPY_FAIL) {
            if matches!(frame.tag, tag::COPY_DONE | tag::COPY_FAIL) && self.last_turn_extended_copy
            {
                // Drivers such as tokio-postgres start extended COPY IN with
                // Bind + Execute + Sync (which PostgreSQL ignores inside COPY
                // mode) and send a second Sync after CopyDone / CopyFail.
                self.last_turn_extended_copy = false;
                self.awaiting_copy_sync = true;
            }
            return Ok(Some(ClientUnit::CopyFrame(frame)));
        }
        self.last_turn_extended_copy = false;
        if self.awaiting_copy_sync && self.pending_frames == 0 {
            match frame.tag {
                tag::FLUSH => return Ok(Some(ClientUnit::CopyFrame(frame))),
                tag::CLOSE => {
                    let target = ObjectRef::decode(&frame)?;
                    match target {
                        ObjectRef::Statement(name) => {
                            self.reset_statements.push(name.clone());
                            self.registry.on_close_statement(&name);
                        }
                        ObjectRef::Portal(name) => {
                            self.reset_portals.push(name.clone());
                            self.registry.on_close_portal(&name);
                        }
                    }
                    return Ok(Some(ClientUnit::CopyFrame(frame)));
                }
                tag::SYNC => {
                    self.awaiting_copy_sync = false;
                    return Ok(Some(ClientUnit::CopyFrame(frame)));
                }
                _ => self.awaiting_copy_sync = false,
            }
        }

        match frame.tag {
            tag::QUERY => {
                let sql = decode_query(&frame)?;
                self.apply_session_sql(&sql);
                self.meta.extend(classify_sql(&sql));
                self.statements.push(BoundStatement {
                    sql,
                    params: Vec::new(),
                });
                self.append(&frame);
                return Ok(Some(self.finish(Terminator::Query)));
            }
            tag::PARSE => {
                let msg = ParseMessage::decode(&frame)?;
                let name = msg.statement.clone();
                self.reset_statements.push(name.clone());
                let previous = self.registry.on_parse(msg);
                let index = self.parse_count;
                self.parse_count += 1;
                self.rollback_ops.push(RegistryOp::Parse {
                    index,
                    name,
                    previous,
                });
                self.flush_steps.push_back(FlushStep::ParseComplete);
            }
            tag::BIND => {
                let msg = BindMessage::decode(&frame)?;
                let name = msg.portal.clone();
                self.reset_portals.push(name.clone());
                let previous = self.registry.on_bind(&msg);
                let index = self.bind_count;
                self.bind_count += 1;
                self.rollback_ops.push(RegistryOp::Bind {
                    index,
                    name,
                    previous,
                });
                self.flush_steps.push_back(FlushStep::BindComplete);
            }
            tag::CLOSE => {
                let target = ObjectRef::decode(&frame)?;
                let index = self.close_count;
                self.close_count += 1;
                match target {
                    ObjectRef::Statement(name) => {
                        self.reset_statements.push(name.clone());
                        let previous = self.registry.on_close_statement(&name);
                        self.rollback_ops.push(RegistryOp::CloseStatement {
                            index,
                            name,
                            previous,
                        });
                    }
                    ObjectRef::Portal(name) => {
                        self.reset_portals.push(name.clone());
                        let previous = self.registry.on_close_portal(&name);
                        self.rollback_ops.push(RegistryOp::ClosePortal {
                            index,
                            name,
                            previous,
                        });
                    }
                }
                self.flush_steps.push_back(FlushStep::CloseComplete);
            }
            tag::DESCRIBE => match ObjectRef::decode(&frame)? {
                ObjectRef::Statement(name) => {
                    self.describes.push_back(DescribeTarget::Statement(name));
                    self.flush_steps.push_back(FlushStep::DescribeStatement);
                }
                ObjectRef::Portal(portal) => {
                    let statement = self
                        .registry
                        .portal(&portal)
                        .map(|p| p.shape.statement.clone());
                    self.describes
                        .push_back(DescribeTarget::Portal { portal, statement });
                    self.flush_steps.push_back(FlushStep::DescribePortal);
                }
            },
            tag::EXECUTE => {
                let msg = ExecuteMessage::decode(&frame)?;
                self.has_execute = true;
                self.flush_steps.push_back(FlushStep::Execute);
                match self.registry.portal(&msg.portal) {
                    Some(portal) => {
                        let sql = portal.sql.clone();
                        let params = portal.params.clone();
                        let shape = portal.shape.clone();
                        let known = portal.known;
                        self.result_shapes.push_back(shape);
                        if known {
                            if is_copy_sql(&sql) {
                                self.has_copy_execute = true;
                            }
                            self.apply_session_sql(&sql);
                            self.meta.extend(classify_sql(&sql));
                        } else {
                            self.meta.push(StatementKind::DataWrite {
                                target_tables: Vec::new(),
                                opaque: true,
                            });
                        }
                        self.statements.push(BoundStatement { sql, params });
                    }
                    None => {
                        self.meta.push(StatementKind::DataWrite {
                            target_tables: Vec::new(),
                            opaque: true,
                        });
                        self.statements.push(BoundStatement {
                            sql: format!("<unknown portal \"{}\">", msg.portal),
                            params: Vec::new(),
                        });
                    }
                }
            }
            tag::SYNC => {
                self.append(&frame);
                return Ok(Some(self.finish(Terminator::Sync)));
            }
            tag::FUNCTION_CALL => {
                self.meta.push(StatementKind::DataWrite {
                    target_tables: Vec::new(),
                    opaque: true,
                });
                self.statements.push(BoundStatement {
                    sql: "<function call>".into(),
                    params: Vec::new(),
                });
                self.append(&frame);
                return Ok(Some(self.finish(Terminator::FunctionCall)));
            }
            tag::TERMINATE => {
                self.append(&frame);
                return Ok(Some(self.finish(Terminator::Terminate)));
            }
            tag::FLUSH if self.has_copy_execute => {
                // Extended-protocol COPY sends Execute + Flush and waits for
                // CopyInResponse / CopyOutResponse before sending CopyData and
                // the closing Sync.
                self.append(&frame);
                self.awaiting_copy_sync = true;
                return Ok(Some(self.finish(Terminator::CopyFlush)));
            }
            tag::FLUSH if self.has_execute => {
                // Portal-chunked fetches (`pgjdbc` fetchSize, `asyncpg` cursors)
                // and pipeline flushes wait for DataRow / PortalSuspended before
                // sending the next Execute or Sync.
                self.append(&frame);
                return Ok(Some(self.finish(Terminator::ExecuteFlush)));
            }
            tag::FLUSH => {
                // Describe-before-Sync round trips (e.g. JDBC / asyncpg) must
                // not wait for a Sync the client will only send after reading.
                self.append(&frame);
                return Ok(Some(self.finish(Terminator::Flush)));
            }
            _ => {}
        }
        self.append(&frame);
        Ok(None)
    }

    fn append(&mut self, frame: &Frame) {
        frame.encode(&mut self.pending);
        self.pending_frames += 1;
    }

    fn finish(&mut self, terminator: Terminator) -> ClientUnit {
        let mut meta = std::mem::take(&mut self.meta);
        if meta.statements.is_empty() {
            meta.push(StatementKind::Noise);
        }
        let flush_steps = if terminator.is_flush_pipeline() {
            std::mem::take(&mut self.flush_steps)
        } else {
            self.flush_steps.clear();
            VecDeque::new()
        };
        let turn = WireTurn {
            raw: self.pending.split().freeze(),
            frame_count: std::mem::take(&mut self.pending_frames),
            terminator,
            statements: std::mem::take(&mut self.statements),
            meta,
            describes: std::mem::take(&mut self.describes),
            result_shapes: std::mem::take(&mut self.result_shapes),
            flush_steps,
            reset_statements: std::mem::take(&mut self.reset_statements),
            reset_portals: std::mem::take(&mut self.reset_portals),
            clear_all_statements: std::mem::take(&mut self.clear_all_statements),
            rollback: RegistryRollback {
                ops: std::mem::take(&mut self.rollback_ops),
            },
        };
        self.last_turn_extended_copy = terminator == Terminator::Sync && self.has_copy_execute;
        self.has_execute = false;
        self.has_copy_execute = false;
        self.parse_count = 0;
        self.bind_count = 0;
        self.close_count = 0;
        ClientUnit::Turn(Box::new(turn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::TurnKind;
    use crate::frame::decode_frame;
    use crate::messages::build;

    fn feed(asm: &mut TurnAssembler, frames: &[Frame]) -> Vec<ClientUnit> {
        let mut out = Vec::new();
        for f in frames {
            if let Some(u) = asm.push(f.clone()).unwrap() {
                out.push(u);
            }
        }
        out
    }

    fn turn(u: &ClientUnit) -> &WireTurn {
        match u {
            ClientUnit::Turn(t) => t,
            ClientUnit::CopyFrame(_) => panic!("expected a turn"),
        }
    }

    #[test]
    fn simple_query_is_one_turn() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[build::query("UPDATE wallets SET balance = 0 WHERE id = 1")],
        );
        assert_eq!(units.len(), 1);
        let t = turn(&units[0]);
        assert_eq!(t.terminator, Terminator::Query);
        assert!(t.terminator.expects_ready_for_query());
        assert_eq!(
            t.meta.kind(),
            TurnKind::DataWrite {
                target_tables: vec!["wallets".into()]
            }
        );
        assert_eq!(
            t.raw,
            build::query("UPDATE wallets SET balance = 0 WHERE id = 1").to_bytes()
        );
    }

    #[test]
    fn extended_pipeline_is_buffered_until_sync() {
        let mut asm = TurnAssembler::new();
        let frames = [
            build::parse("", "SELECT balance FROM wallets WHERE id = $1", &[23]),
            build::bind("", "", &[Some(&1i32.to_be_bytes()[..])]),
            build::describe_portal(""),
            build::execute("", 0),
        ];
        assert!(feed(&mut asm, &frames).is_empty());
        assert!(asm.has_pending());
        let units = feed(&mut asm, &[build::sync()]);
        let t = turn(&units[0]);
        assert_eq!(t.frame_count, 5);
        assert_eq!(t.terminator, Terminator::Sync);
        // Binary-format parameter would need a format code; text by default.
        assert_eq!(
            t.statements[0].sql,
            "SELECT balance FROM wallets WHERE id = $1"
        );
        assert_eq!(
            t.meta.kind(),
            TurnKind::DataRead {
                has_row_lock: false
            }
        );
        assert!(!asm.has_pending());
    }

    #[test]
    fn bind_without_parse_recovers_cached_sql_and_params() {
        let mut asm = TurnAssembler::new();
        // Turn 1: prepare + execute "s1".
        let t1 = feed(
            &mut asm,
            &[
                build::parse("s1", "SELECT * FROM accounts WHERE id = $1", &[]),
                build::bind("", "s1", &[Some(b"7")]),
                build::execute("", 0),
                build::sync(),
            ],
        );
        assert_eq!(turn(&t1[0]).statements[0].params, vec!["7"]);
        // Turn 2: only Bind + Execute + Sync against the cached statement.
        let t2 = feed(
            &mut asm,
            &[
                build::bind("", "s1", &[Some(b"42")]),
                build::execute("", 0),
                build::sync(),
            ],
        );
        let t = turn(&t2[0]);
        assert_eq!(t.statements.len(), 1);
        assert_eq!(t.statements[0].sql, "SELECT * FROM accounts WHERE id = $1");
        assert_eq!(t.statements[0].params, vec!["42"]);
        assert_eq!(
            asm.registry().sql_of("s1"),
            Some("SELECT * FROM accounts WHERE id = $1")
        );
    }

    #[test]
    fn tracks_described_statements_and_result_formats() {
        let mut asm = TurnAssembler::new();
        let t1 = feed(
            &mut asm,
            &[
                build::parse("s0", "SELECT balance FROM wallets WHERE id = $1", &[23]),
                build::describe_statement("s0"),
                build::sync(),
            ],
        );
        let t1 = turn(&t1[0]);
        assert_eq!(
            t1.describes,
            VecDeque::from([DescribeTarget::Statement("s0".into())])
        );
        assert!(t1.result_shapes.is_empty());
        let mut bind = BytesMut::from(&build::bind("", "s0", &[Some(b"1")]).body[..]);
        // Replace the trailing "0 result formats" with "1 format: binary".
        bind.truncate(bind.len() - 2);
        bind.extend_from_slice(&[0, 1, 0, 1]);
        let t2 = feed(
            &mut asm,
            &[
                Frame::new(tag::BIND, bind.freeze()),
                build::execute("", 0),
                build::sync(),
            ],
        );
        let t2 = turn(&t2[0]);
        assert!(t2.describes.is_empty());
        assert_eq!(
            t2.result_shapes,
            VecDeque::from([ResultShape {
                portal: String::new(),
                statement: "s0".into(),
                formats: vec![FormatCode::Binary],
            }])
        );
    }

    #[test]
    fn preserves_pipelined_describes_and_portal_targets() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse("s1", "SELECT balance FROM wallets WHERE id = $1", &[23]),
                build::describe_statement("s1"),
                build::parse("s2", "UPDATE wallets SET balance = 0 WHERE id = $1", &[23]),
                build::describe_statement("s2"),
                build::bind("p1", "s1", &[Some(b"1")]),
                build::describe_portal("p1"),
                build::flush(),
            ],
        );
        let t = turn(&units[0]);
        assert_eq!(t.terminator, Terminator::Flush);
        assert_eq!(
            t.describes,
            VecDeque::from([
                DescribeTarget::Statement("s1".into()),
                DescribeTarget::Statement("s2".into()),
                DescribeTarget::Portal {
                    portal: "p1".into(),
                    statement: Some("s1".into()),
                },
            ])
        );
    }

    #[test]
    fn extended_copy_flush_emits_turn_and_streams_closing_sync() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse("", "COPY wallets FROM STDIN", &[]),
                build::bind("", "", &[]),
                build::execute("", 0),
                build::flush(),
            ],
        );
        assert_eq!(units.len(), 1);
        let t = turn(&units[0]);
        assert_eq!(t.terminator, Terminator::CopyFlush);
        assert!(t.terminator.expects_ready_for_query());
        assert_eq!(
            t.meta.kind(),
            TurnKind::DataWrite {
                target_tables: vec!["wallets".into()]
            }
        );

        let tail = feed(
            &mut asm,
            &[
                Frame::new(tag::COPY_DATA, Bytes::from_static(b"1\t100\n")),
                Frame::new(tag::COPY_DONE, Bytes::new()),
                build::sync(),
            ],
        );
        assert_eq!(tail.len(), 3);
        assert!(tail.iter().all(|u| matches!(u, ClientUnit::CopyFrame(_))));

        let next = feed(&mut asm, &[build::query("SELECT 1")]);
        assert_eq!(turn(&next[0]).sql_text(), "SELECT 1");
    }

    #[test]
    fn extended_copy_sync_streams_post_copy_done_sync() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse("s0", "COPY wallets FROM STDIN", &[]),
                build::describe_statement("s0"),
                build::sync(),
            ],
        );
        assert_eq!(units.len(), 1);

        let copy_turn = feed(
            &mut asm,
            &[
                build::bind("", "s0", &[]),
                build::execute("", 0),
                build::sync(),
            ],
        );
        assert_eq!(copy_turn.len(), 1);
        assert_eq!(turn(&copy_turn[0]).terminator, Terminator::Sync);

        let tail = feed(
            &mut asm,
            &[
                Frame::new(tag::COPY_DATA, Bytes::from_static(b"1\t100\n")),
                Frame::new(tag::COPY_DONE, Bytes::new()),
                build::sync(),
            ],
        );
        assert_eq!(tail.len(), 3);
        assert!(tail.iter().all(|u| matches!(u, ClientUnit::CopyFrame(_))));

        let next = feed(&mut asm, &[build::query("SELECT 1")]);
        assert_eq!(turn(&next[0]).sql_text(), "SELECT 1");
    }

    #[test]
    fn binary_params_are_rendered_with_declared_types() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse(
                    "w",
                    "UPDATE wallets SET balance = balance - $1 WHERE id = $2",
                    &[20, 23],
                ),
                build::bind_with_formats(
                    "",
                    "w",
                    &[1],
                    &[
                        Some(&100i64.to_be_bytes()[..]),
                        Some(&1i32.to_be_bytes()[..]),
                    ],
                ),
                build::execute("", 0),
                build::sync(),
            ],
        );
        assert_eq!(turn(&units[0]).statements[0].params, vec!["100", "1"]);
    }

    #[test]
    fn close_evicts_statement() {
        let mut asm = TurnAssembler::new();
        feed(
            &mut asm,
            &[build::parse("s1", "SELECT 1", &[]), build::sync()],
        );
        assert_eq!(asm.registry().len(), 1);
        feed(&mut asm, &[build::close_statement("s1"), build::sync()]);
        assert!(asm.registry().is_empty());
        let units = feed(
            &mut asm,
            &[
                build::bind("", "s1", &[]),
                build::execute("", 0),
                build::sync(),
            ],
        );
        assert!(
            turn(&units[0]).statements[0]
                .sql
                .contains("unknown prepared statement")
        );
    }

    #[test]
    fn prepare_only_turn_is_noise() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse("s0", "UPDATE wallets SET balance = 0", &[]),
                Frame::new(b'D', bytes::Bytes::from_static(b"Ss0\0")),
                build::sync(),
            ],
        );
        assert!(turn(&units[0]).meta.is_noise());
        assert!(turn(&units[0]).statements.is_empty());
    }

    #[test]
    fn flush_without_execute_completes_a_noise_turn() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse("s1", "SELECT 1", &[]),
                Frame::new(tag::FLUSH, Bytes::new()),
            ],
        );
        let t = turn(&units[0]);
        assert_eq!(t.terminator, Terminator::Flush);
        assert!(!t.terminator.expects_ready_for_query());
    }

    #[test]
    fn flush_after_execute_emits_schedulable_execute_flush_turn() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[
                build::parse("", "UPDATE t SET x = 1", &[]),
                build::bind("", "", &[]),
                build::execute("", 0),
                Frame::new(tag::FLUSH, Bytes::new()),
            ],
        );
        assert_eq!(units.len(), 1);
        let t = turn(&units[0]);
        assert_eq!(t.terminator, Terminator::ExecuteFlush);
        assert!(t.terminator.is_schedulable());
        assert!(!t.terminator.expects_ready_for_query());
        assert_eq!(t.frame_count, 4);
        assert_eq!(
            t.flush_steps,
            VecDeque::from([
                FlushStep::ParseComplete,
                FlushStep::BindComplete,
                FlushStep::Execute,
            ])
        );
        let sync = feed(&mut asm, &[build::sync()]);
        assert_eq!(turn(&sync[0]).terminator, Terminator::Sync);
        assert!(turn(&sync[0]).meta.is_noise());
    }

    #[test]
    fn copy_frames_pass_through() {
        let mut asm = TurnAssembler::new();
        let units = feed(
            &mut asm,
            &[Frame::new(tag::COPY_DATA, Bytes::from_static(b"1\t2\n"))],
        );
        assert!(matches!(units[0], ClientUnit::CopyFrame(_)));
    }

    #[test]
    fn assembler_survives_fragmented_stream() {
        let frames = [
            build::parse("s1", "SELECT * FROM t WHERE id = $1", &[]),
            build::bind("", "s1", &[Some(b"5")]),
            build::execute("", 0),
            build::sync(),
            build::query("COMMIT"),
        ];
        let mut bytes = Vec::new();
        for f in &frames {
            bytes.extend_from_slice(&f.to_bytes());
        }
        for chunk in [1usize, 3, 7, 4096] {
            let mut asm = TurnAssembler::new();
            let mut buf = BytesMut::new();
            let mut turns = Vec::new();
            for piece in bytes.chunks(chunk) {
                buf.extend_from_slice(piece);
                while let Some(f) = decode_frame(&mut buf).unwrap() {
                    if let Some(ClientUnit::Turn(t)) = asm.push(f).unwrap() {
                        turns.push(t);
                    }
                }
            }
            assert_eq!(turns.len(), 2, "chunk {chunk}");
            assert_eq!(turns[0].statements[0].params, vec!["5"]);
            assert_eq!(turns[1].sql_text(), "COMMIT");
        }
    }
}
