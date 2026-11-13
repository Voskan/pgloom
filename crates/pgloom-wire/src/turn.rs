//! Grouping of client messages into scheduler-visible `WireTurn`s.
//!
//! A turn is the unit the scheduler interleaves: a complete simple query
//! (`'Q'`) or an extended-protocol pipeline ending in `Sync` (`'S'`).
//! PostgreSQL produces exactly one `ReadyForQuery` per turn, so a turn's
//! completion is observable on the backend stream.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::classify::{StatementKind, TurnMeta, classify_sql};
use crate::error::WireResult;
use crate::frame::Frame;
use crate::messages::{
    BindMessage, ExecuteMessage, FormatCode, ObjectRef, ParseMessage, decode_query, tag,
};
use crate::values::render_value;

/// A statement that a turn actually executes, with rendered parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundStatement {
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
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
    /// `'H'` with no pending `Execute`: responses are flushed, no `ReadyForQuery`.
    Flush,
    /// `'X'`: the client is closing the connection.
    Terminate,
}

impl Terminator {
    pub fn expects_ready_for_query(self) -> bool {
        matches!(
            self,
            Terminator::Query | Terminator::Sync | Terminator::FunctionCall
        )
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
    /// Prepared statement whose `Describe` this turn sends; its
    /// `RowDescription` tells how later executions encode their rows.
    pub described_statement: Option<String>,
    /// Statement and result formats of the last `Execute` in the turn.
    pub result_shape: Option<ResultShape>,
}

/// How the rows of an executed portal are encoded, as requested by `Bind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultShape {
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
    Turn(WireTurn),
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

    fn on_parse(&mut self, msg: ParseMessage) {
        self.statements.insert(
            msg.statement,
            PreparedStatement {
                sql: msg.sql,
                param_types: msg.param_types,
            },
        );
    }

    fn on_bind(&mut self, msg: &BindMessage) {
        let (sql, types) = match self.statements.get(&msg.statement) {
            Some(s) => (s.sql.clone(), s.param_types.as_slice()),
            None => (
                format!("<unknown prepared statement \"{}\">", msg.statement),
                &[][..],
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
            statement: msg.statement.clone(),
            formats: msg.result_formats.clone(),
        };
        self.portals
            .insert(msg.portal.clone(), Portal { sql, params, shape });
    }

    fn on_close(&mut self, target: ObjectRef) {
        match target {
            ObjectRef::Statement(name) => {
                self.statements.remove(&name);
            }
            ObjectRef::Portal(name) => {
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
    described_statement: Option<String>,
    result_shape: Option<ResultShape>,
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

    /// Feeds one client frame; returns a unit once one is complete.
    pub fn push(&mut self, frame: Frame) -> WireResult<Option<ClientUnit>> {
        match frame.tag {
            tag::COPY_DATA | tag::COPY_DONE | tag::COPY_FAIL => {
                return Ok(Some(ClientUnit::CopyFrame(frame)));
            }
            tag::QUERY => {
                let sql = decode_query(&frame)?;
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
                self.registry.on_parse(msg);
            }
            tag::BIND => {
                let msg = BindMessage::decode(&frame)?;
                self.registry.on_bind(&msg);
            }
            tag::CLOSE => {
                let target = ObjectRef::decode(&frame)?;
                self.registry.on_close(target);
            }
            tag::DESCRIBE => {
                if let ObjectRef::Statement(name) = ObjectRef::decode(&frame)? {
                    self.described_statement = Some(name);
                }
            }
            tag::EXECUTE => {
                let msg = ExecuteMessage::decode(&frame)?;
                self.has_execute = true;
                match self.registry.portal(&msg.portal) {
                    Some(portal) => {
                        self.result_shape = Some(portal.shape.clone());
                        self.meta.extend(classify_sql(&portal.sql));
                        self.statements.push(BoundStatement {
                            sql: portal.sql.clone(),
                            params: portal.params.clone(),
                        });
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
            tag::FLUSH if !self.has_execute => {
                // Describe-before-Sync round trips (e.g. JDBC) must not wait for
                // a Sync the client will only send after seeing the response.
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
        let turn = WireTurn {
            raw: self.pending.split().freeze(),
            frame_count: std::mem::take(&mut self.pending_frames),
            terminator,
            statements: std::mem::take(&mut self.statements),
            meta,
            described_statement: self.described_statement.take(),
            result_shape: self.result_shape.take(),
        };
        self.has_execute = false;
        ClientUnit::Turn(turn)
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
        assert_eq!(t1.described_statement.as_deref(), Some("s0"));
        assert!(t1.result_shape.is_none());
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
        assert_eq!(t2.described_statement, None);
        assert_eq!(
            t2.result_shape,
            Some(ResultShape {
                statement: "s0".into(),
                formats: vec![FormatCode::Binary],
            })
        );
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
    fn flush_after_execute_keeps_buffering_until_sync() {
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
        assert!(units.is_empty());
        let units = feed(&mut asm, &[build::sync()]);
        assert_eq!(turn(&units[0]).frame_count, 5);
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
