//! Typed views over the message payloads PgLoom needs to understand.
//!
//! Only the messages that influence scheduling, reporting or lock tracking are
//! decoded; everything else is forwarded as opaque bytes.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::error::{WireError, WireResult};
use crate::frame::Frame;

/// Frontend (client to server) message tags.
pub mod tag {
    pub const QUERY: u8 = b'Q';
    pub const PARSE: u8 = b'P';
    pub const BIND: u8 = b'B';
    pub const DESCRIBE: u8 = b'D';
    pub const EXECUTE: u8 = b'E';
    pub const CLOSE: u8 = b'C';
    pub const SYNC: u8 = b'S';
    pub const FLUSH: u8 = b'H';
    pub const TERMINATE: u8 = b'X';
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_FAIL: u8 = b'f';
    pub const FUNCTION_CALL: u8 = b'F';
    pub const PASSWORD: u8 = b'p';
}

/// Backend (server to client) message tags.
pub mod backend_tag {
    pub const AUTHENTICATION: u8 = b'R';
    pub const BACKEND_KEY_DATA: u8 = b'K';
    pub const PARAMETER_STATUS: u8 = b'S';
    pub const READY_FOR_QUERY: u8 = b'Z';
    pub const ERROR_RESPONSE: u8 = b'E';
    pub const NOTICE_RESPONSE: u8 = b'N';
    pub const COMMAND_COMPLETE: u8 = b'C';
    pub const ROW_DESCRIPTION: u8 = b'T';
    pub const DATA_ROW: u8 = b'D';
    pub const EMPTY_QUERY: u8 = b'I';
    pub const COPY_IN_RESPONSE: u8 = b'G';
    pub const COPY_OUT_RESPONSE: u8 = b'H';
    pub const COPY_BOTH_RESPONSE: u8 = b'W';
    pub const PARSE_COMPLETE: u8 = b'1';
    pub const BIND_COMPLETE: u8 = b'2';
    pub const NO_DATA: u8 = b'n';
}

/// Transaction status reported by `ReadyForQuery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TxStatus {
    /// `'I'`: not inside a transaction block.
    #[serde(rename = "I")]
    Idle,
    /// `'T'`: inside an open transaction block.
    #[serde(rename = "T")]
    InTransaction,
    /// `'E'`: inside a failed transaction block awaiting `ROLLBACK`.
    #[serde(rename = "E")]
    Failed,
}

impl TxStatus {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'I' => Some(TxStatus::Idle),
            b'T' => Some(TxStatus::InTransaction),
            b'E' => Some(TxStatus::Failed),
            _ => None,
        }
    }

    pub fn as_byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }

    pub fn in_transaction(self) -> bool {
        !matches!(self, TxStatus::Idle)
    }
}

impl std::fmt::Display for TxStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TxStatus::Idle => "idle",
            TxStatus::InTransaction => "in transaction",
            TxStatus::Failed => "failed transaction",
        })
    }
}

/// Process id of a PostgreSQL backend, as announced by `BackendKeyData`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BackendPid(pub i32);

impl std::fmt::Display for BackendPid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Bounds-checked cursor over a message body.
#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    tag: char,
}

impl<'a> Reader<'a> {
    pub fn new(frame: &'a Frame) -> Self {
        Self {
            buf: &frame.body,
            tag: frame.tag_char(),
        }
    }

    fn malformed(&self, reason: &'static str) -> WireError {
        WireError::Malformed {
            tag: self.tag,
            reason,
        }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    pub fn u8(&mut self) -> WireResult<u8> {
        let (&b, rest) = self
            .buf
            .split_first()
            .ok_or_else(|| self.malformed("truncated byte"))?;
        self.buf = rest;
        Ok(b)
    }

    pub fn i16(&mut self) -> WireResult<i16> {
        let bytes = self
            .take(2)
            .map_err(|_| self.malformed("truncated int16"))?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn i32(&mut self) -> WireResult<i32> {
        let bytes = self
            .take(4)
            .map_err(|_| self.malformed("truncated int32"))?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn take(&mut self, n: usize) -> WireResult<&'a [u8]> {
        if self.buf.len() < n {
            return Err(self.malformed("truncated payload"));
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    pub fn cstr(&mut self) -> WireResult<String> {
        let nul = self
            .buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| self.malformed("unterminated string"))?;
        let s = String::from_utf8_lossy(&self.buf[..nul]).into_owned();
        self.buf = &self.buf[nul + 1..];
        Ok(s)
    }

    fn count(&mut self) -> WireResult<usize> {
        let n = self.i16()?;
        usize::try_from(n).map_err(|_| self.malformed("negative element count"))
    }
}

/// Wire format code of a parameter or result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCode {
    Text,
    Binary,
}

impl FormatCode {
    fn from_i16(code: i16) -> Self {
        if code == 1 {
            FormatCode::Binary
        } else {
            FormatCode::Text
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseMessage {
    pub statement: String,
    pub sql: String,
    pub param_types: Vec<u32>,
}

impl ParseMessage {
    pub fn decode(frame: &Frame) -> WireResult<Self> {
        let mut r = Reader::new(frame);
        let statement = r.cstr()?;
        let sql = r.cstr()?;
        let n = r.count()?;
        let mut param_types = Vec::with_capacity(n);
        for _ in 0..n {
            param_types.push(r.i32()? as u32);
        }
        Ok(Self {
            statement,
            sql,
            param_types,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindMessage {
    pub portal: String,
    pub statement: String,
    pub param_formats: Vec<FormatCode>,
    /// `None` encodes SQL `NULL`.
    pub params: Vec<Option<Bytes>>,
    /// Requested formats of the result columns (broadcast like parameters).
    pub result_formats: Vec<FormatCode>,
}

impl BindMessage {
    pub fn decode(frame: &Frame) -> WireResult<Self> {
        let mut r = Reader::new(frame);
        let portal = r.cstr()?;
        let statement = r.cstr()?;
        let nf = r.count()?;
        let mut param_formats = Vec::with_capacity(nf);
        for _ in 0..nf {
            param_formats.push(FormatCode::from_i16(r.i16()?));
        }
        let np = r.count()?;
        let mut params = Vec::with_capacity(np);
        for _ in 0..np {
            let len = r.i32()?;
            if len < 0 {
                params.push(None);
            } else {
                let bytes = r.take(len as usize)?;
                params.push(Some(Bytes::copy_from_slice(bytes)));
            }
        }
        let nr = r.count()?;
        let mut result_formats = Vec::with_capacity(nr);
        for _ in 0..nr {
            result_formats.push(FormatCode::from_i16(r.i16()?));
        }
        Ok(Self {
            portal,
            statement,
            param_formats,
            params,
            result_formats,
        })
    }

    /// Format of result column `i`, applying the protocol's broadcast rules.
    pub fn result_format_of(&self, i: usize) -> FormatCode {
        broadcast(&self.result_formats, i)
    }

    /// Format of parameter `i`, applying the protocol's broadcast rules.
    pub fn format_of(&self, i: usize) -> FormatCode {
        broadcast(&self.param_formats, i)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteMessage {
    pub portal: String,
    pub max_rows: i32,
}

impl ExecuteMessage {
    pub fn decode(frame: &Frame) -> WireResult<Self> {
        let mut r = Reader::new(frame);
        Ok(Self {
            portal: r.cstr()?,
            max_rows: r.i32()?,
        })
    }
}

/// Target of `Close` / `Describe`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectRef {
    Statement(String),
    Portal(String),
}

impl ObjectRef {
    pub fn decode(frame: &Frame) -> WireResult<Self> {
        let mut r = Reader::new(frame);
        let kind = r.u8()?;
        let name = r.cstr()?;
        match kind {
            b'S' => Ok(ObjectRef::Statement(name)),
            b'P' => Ok(ObjectRef::Portal(name)),
            _ => Err(WireError::Malformed {
                tag: frame.tag_char(),
                reason: "object kind must be 'S' or 'P'",
            }),
        }
    }
}

pub fn decode_query(frame: &Frame) -> WireResult<String> {
    Reader::new(frame).cstr()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendKeyData {
    pub pid: BackendPid,
    pub secret_key: i32,
}

impl BackendKeyData {
    pub fn decode(frame: &Frame) -> WireResult<Self> {
        let mut r = Reader::new(frame);
        let pid = BackendPid(r.i32()?);
        // Protocol 3.2 permits variable-length keys; keep the leading word only.
        let secret_key = if r.remaining() >= 4 { r.i32()? } else { 0 };
        Ok(Self { pid, secret_key })
    }
}

pub fn decode_ready_for_query(frame: &Frame) -> WireResult<TxStatus> {
    let b = Reader::new(frame).u8()?;
    TxStatus::from_byte(b).ok_or(WireError::Malformed {
        tag: 'Z',
        reason: "unknown transaction status",
    })
}

/// Decoded `ErrorResponse` / `NoticeResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorFields {
    pub severity: String,
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl ErrorFields {
    pub fn decode(frame: &Frame) -> WireResult<Self> {
        let mut r = Reader::new(frame);
        let mut out = ErrorFields::default();
        loop {
            let field = r.u8()?;
            if field == 0 {
                return Ok(out);
            }
            let value = r.cstr()?;
            match field {
                b'V' => out.severity = value,
                b'S' if out.severity.is_empty() => out.severity = value,
                b'C' => out.code = value,
                b'M' => out.message = value,
                b'D' => out.detail = Some(value),
                _ => {}
            }
        }
    }

    pub fn is_deadlock(&self) -> bool {
        self.code == sqlstate::DEADLOCK_DETECTED
    }

    pub fn is_serialization_failure(&self) -> bool {
        self.code == sqlstate::SERIALIZATION_FAILURE
    }
}

/// SQLSTATE codes with special meaning to the scheduler.
pub mod sqlstate {
    pub const DEADLOCK_DETECTED: &str = "40P01";
    pub const SERIALIZATION_FAILURE: &str = "40001";
    pub const QUERY_CANCELED: &str = "57014";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const UNIQUE_VIOLATION: &str = "23505";
}

pub fn decode_command_complete(frame: &Frame) -> WireResult<String> {
    Reader::new(frame).cstr()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub type_oid: u32,
    pub format: FormatCode,
}

/// Zero codes mean text, one code applies to every column, otherwise one
/// code per column.
pub(crate) fn broadcast(formats: &[FormatCode], i: usize) -> FormatCode {
    match formats {
        [] => FormatCode::Text,
        [one] => *one,
        many => many.get(i).copied().unwrap_or(FormatCode::Text),
    }
}

pub fn decode_row_description(frame: &Frame) -> WireResult<Vec<ColumnDesc>> {
    let mut r = Reader::new(frame);
    let n = r.count()?;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.cstr()?;
        let _table_oid = r.i32()?;
        let _attnum = r.i16()?;
        let type_oid = r.i32()? as u32;
        let _typlen = r.i16()?;
        let _typmod = r.i32()?;
        let format = FormatCode::from_i16(r.i16()?);
        cols.push(ColumnDesc {
            name,
            type_oid,
            format,
        });
    }
    Ok(cols)
}

pub fn decode_data_row(frame: &Frame) -> WireResult<Vec<Option<Bytes>>> {
    let mut r = Reader::new(frame);
    let n = r.count()?;
    let mut values = Vec::with_capacity(n);
    for _ in 0..n {
        let len = r.i32()?;
        if len < 0 {
            values.push(None);
        } else {
            values.push(Some(Bytes::copy_from_slice(r.take(len as usize)?)));
        }
    }
    Ok(values)
}

/// Message builders shared by the proxy, the test kit and unit tests.
pub mod build {
    use super::*;

    fn cstr(dst: &mut BytesMut, s: &str) {
        dst.put_slice(s.as_bytes());
        dst.put_u8(0);
    }

    pub fn query(sql: &str) -> Frame {
        let mut b = BytesMut::new();
        cstr(&mut b, sql);
        Frame::new(tag::QUERY, b.freeze())
    }

    pub fn parse(statement: &str, sql: &str, param_types: &[u32]) -> Frame {
        let mut b = BytesMut::new();
        cstr(&mut b, statement);
        cstr(&mut b, sql);
        b.put_i16(param_types.len() as i16);
        for oid in param_types {
            b.put_u32(*oid);
        }
        Frame::new(tag::PARSE, b.freeze())
    }

    pub fn bind(portal: &str, statement: &str, params: &[Option<&[u8]>]) -> Frame {
        bind_with_formats(portal, statement, &[], params)
    }

    pub fn bind_with_formats(
        portal: &str,
        statement: &str,
        formats: &[i16],
        params: &[Option<&[u8]>],
    ) -> Frame {
        let mut b = BytesMut::new();
        cstr(&mut b, portal);
        cstr(&mut b, statement);
        b.put_i16(formats.len() as i16);
        for f in formats {
            b.put_i16(*f);
        }
        b.put_i16(params.len() as i16);
        for p in params {
            match p {
                Some(v) => {
                    b.put_i32(v.len() as i32);
                    b.put_slice(v);
                }
                None => b.put_i32(-1),
            }
        }
        b.put_i16(0);
        Frame::new(tag::BIND, b.freeze())
    }

    pub fn describe_statement(statement: &str) -> Frame {
        let mut b = BytesMut::new();
        b.put_u8(b'S');
        cstr(&mut b, statement);
        Frame::new(tag::DESCRIBE, b.freeze())
    }

    pub fn describe_portal(portal: &str) -> Frame {
        let mut b = BytesMut::new();
        b.put_u8(b'P');
        cstr(&mut b, portal);
        Frame::new(tag::DESCRIBE, b.freeze())
    }

    pub fn execute(portal: &str, max_rows: i32) -> Frame {
        let mut b = BytesMut::new();
        cstr(&mut b, portal);
        b.put_i32(max_rows);
        Frame::new(tag::EXECUTE, b.freeze())
    }

    pub fn close_statement(statement: &str) -> Frame {
        let mut b = BytesMut::new();
        b.put_u8(b'S');
        cstr(&mut b, statement);
        Frame::new(tag::CLOSE, b.freeze())
    }

    pub fn sync() -> Frame {
        Frame::new(tag::SYNC, Bytes::new())
    }

    pub fn terminate() -> Frame {
        Frame::new(tag::TERMINATE, Bytes::new())
    }

    pub fn authentication_ok() -> Frame {
        Frame::new(
            backend_tag::AUTHENTICATION,
            Bytes::from_static(&[0, 0, 0, 0]),
        )
    }

    pub fn parameter_status(key: &str, value: &str) -> Frame {
        let mut b = BytesMut::new();
        cstr(&mut b, key);
        cstr(&mut b, value);
        Frame::new(backend_tag::PARAMETER_STATUS, b.freeze())
    }

    pub fn backend_key_data(pid: i32, secret_key: i32) -> Frame {
        let mut b = BytesMut::with_capacity(8);
        b.put_i32(pid);
        b.put_i32(secret_key);
        Frame::new(backend_tag::BACKEND_KEY_DATA, b.freeze())
    }

    pub fn ready_for_query(status: TxStatus) -> Frame {
        Frame::new(
            backend_tag::READY_FOR_QUERY,
            Bytes::copy_from_slice(&[status.as_byte()]),
        )
    }

    pub fn command_complete(command_tag: &str) -> Frame {
        let mut b = BytesMut::new();
        cstr(&mut b, command_tag);
        Frame::new(backend_tag::COMMAND_COMPLETE, b.freeze())
    }

    pub fn error_response(code: &str, message: &str) -> Frame {
        let mut b = BytesMut::new();
        b.put_u8(b'S');
        cstr(&mut b, "ERROR");
        b.put_u8(b'V');
        cstr(&mut b, "ERROR");
        b.put_u8(b'C');
        cstr(&mut b, code);
        b.put_u8(b'M');
        cstr(&mut b, message);
        b.put_u8(0);
        Frame::new(backend_tag::ERROR_RESPONSE, b.freeze())
    }

    /// RowDescription with text-format columns of the given type OIDs.
    pub fn row_description(columns: &[(&str, u32)]) -> Frame {
        let mut b = BytesMut::new();
        b.put_i16(columns.len() as i16);
        for (name, oid) in columns {
            cstr(&mut b, name);
            b.put_i32(0);
            b.put_i16(0);
            b.put_u32(*oid);
            b.put_i16(-1);
            b.put_i32(-1);
            b.put_i16(0);
        }
        Frame::new(backend_tag::ROW_DESCRIPTION, b.freeze())
    }

    pub fn data_row(values: &[Option<&str>]) -> Frame {
        let mut b = BytesMut::new();
        b.put_i16(values.len() as i16);
        for v in values {
            match v {
                Some(s) => {
                    b.put_i32(s.len() as i32);
                    b.put_slice(s.as_bytes());
                }
                None => b.put_i32(-1),
            }
        }
        Frame::new(backend_tag::DATA_ROW, b.freeze())
    }

    pub fn parse_complete() -> Frame {
        Frame::new(backend_tag::PARSE_COMPLETE, Bytes::new())
    }

    pub fn bind_complete() -> Frame {
        Frame::new(backend_tag::BIND_COMPLETE, Bytes::new())
    }

    pub fn no_data() -> Frame {
        Frame::new(b'n', Bytes::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_bind_roundtrip() {
        let p = build::parse("s1", "SELECT * FROM t WHERE id = $1", &[23]);
        let parsed = ParseMessage::decode(&p).unwrap();
        assert_eq!(parsed.statement, "s1");
        assert_eq!(parsed.sql, "SELECT * FROM t WHERE id = $1");
        assert_eq!(parsed.param_types, vec![23]);

        let b = build::bind("", "s1", &[Some(b"42"), None]);
        let bound = BindMessage::decode(&b).unwrap();
        assert_eq!(bound.statement, "s1");
        assert_eq!(bound.params, vec![Some(Bytes::from_static(b"42")), None]);
        assert_eq!(bound.format_of(0), FormatCode::Text);
    }

    #[test]
    fn bind_format_broadcast_rules() {
        let b = build::bind_with_formats("", "s", &[1], &[Some(b"ab"), Some(b"cd")]);
        let bound = BindMessage::decode(&b).unwrap();
        assert_eq!(bound.format_of(0), FormatCode::Binary);
        assert_eq!(bound.format_of(1), FormatCode::Binary);
        let b = build::bind_with_formats("", "s", &[0, 1], &[Some(b"ab"), Some(b"cd")]);
        let bound = BindMessage::decode(&b).unwrap();
        assert_eq!(bound.format_of(0), FormatCode::Text);
        assert_eq!(bound.format_of(1), FormatCode::Binary);
    }

    #[test]
    fn extracts_backend_pid_and_secret() {
        let k = build::backend_key_data(4812, -99);
        let key = BackendKeyData::decode(&k).unwrap();
        assert_eq!(key.pid, BackendPid(4812));
        assert_eq!(key.secret_key, -99);
    }

    #[test]
    fn extracts_sqlstate_codes() {
        let e = build::error_response("40P01", "deadlock detected");
        let fields = ErrorFields::decode(&e).unwrap();
        assert!(fields.is_deadlock());
        assert_eq!(fields.message, "deadlock detected");
        assert_eq!(fields.severity, "ERROR");

        let e = build::error_response("40001", "could not serialize access");
        assert!(ErrorFields::decode(&e).unwrap().is_serialization_failure());
    }

    #[test]
    fn ready_for_query_statuses() {
        for status in [TxStatus::Idle, TxStatus::InTransaction, TxStatus::Failed] {
            let z = build::ready_for_query(status);
            assert_eq!(decode_ready_for_query(&z).unwrap(), status);
        }
        let bad = Frame::new(b'Z', Bytes::from_static(b"X"));
        assert!(decode_ready_for_query(&bad).is_err());
        let empty = Frame::new(b'Z', Bytes::new());
        assert!(decode_ready_for_query(&empty).is_err());
    }

    #[test]
    fn truncated_messages_are_errors_not_panics() {
        let cases = [
            Frame::new(b'P', Bytes::from_static(b"s1")),
            Frame::new(b'P', Bytes::from_static(b"s1\0SELECT\0\x00")),
            Frame::new(b'B', Bytes::from_static(b"\0s1\0\x00\x01")),
            Frame::new(
                b'B',
                Bytes::from_static(b"\0s1\0\0\0\0\x01\x00\x00\x00\x09ab"),
            ),
            Frame::new(b'K', Bytes::from_static(b"\x00\x01")),
            Frame::new(b'D', Bytes::from_static(b"\x00\x01\x00\x00\x00\x05ab")),
            Frame::new(b'E', Bytes::from_static(b"Cabc")),
            Frame::new(b'T', Bytes::from_static(b"\xff\xff")),
        ];
        for f in &cases {
            let res = match f.tag {
                b'P' => ParseMessage::decode(f).map(|_| ()),
                b'B' => BindMessage::decode(f).map(|_| ()),
                b'K' => BackendKeyData::decode(f).map(|_| ()),
                b'D' => decode_data_row(f).map(|_| ()),
                b'E' => ErrorFields::decode(f).map(|_| ()),
                b'T' => decode_row_description(f).map(|_| ()),
                _ => unreachable!(),
            };
            assert!(res.is_err(), "expected error for {f:?}");
        }
    }

    #[test]
    fn row_description_and_data_row() {
        let t = build::row_description(&[("balance", 20), ("name", 25)]);
        let cols = decode_row_description(&t).unwrap();
        assert_eq!(cols[0].name, "balance");
        assert_eq!(cols[0].type_oid, 20);
        let d = build::data_row(&[Some("100"), None]);
        let row = decode_data_row(&d).unwrap();
        assert_eq!(row, vec![Some(Bytes::from_static(b"100")), None]);
    }
}
