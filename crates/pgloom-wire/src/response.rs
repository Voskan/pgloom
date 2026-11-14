//! Summaries of backend responses, used for reports and invariants.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::error::WireResult;
use crate::frame::Frame;
use crate::messages::{
    ColumnDesc, ErrorFields, FormatCode, TxStatus, backend_tag, broadcast, decode_command_complete,
    decode_data_row, decode_ready_for_query, decode_row_description,
};
use crate::turn::{DescribeTarget, ResultShape};
use crate::values::render_value;

/// Number of leading rows whose values are retained for reporting.
const RETAINED_ROWS: usize = 3;

/// Cached column layout of a bound portal: `(backing_statement, columns)`.
pub(crate) type PortalLayout = (Option<String>, Vec<ColumnDesc>);

/// What the server answered to one turn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnResponse {
    /// Status from the closing `ReadyForQuery`; `None` if the turn has none.
    pub tx_status: Option<TxStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorFields>,
    #[serde(default)]
    pub row_count: u64,
    /// Leading rows as `(column, rendered value)` pairs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<Vec<(String, String)>>,
}

impl TurnResponse {
    pub fn sqlstate(&self) -> Option<&str> {
        self.error.as_ref().map(|e| e.code.as_str())
    }

    pub fn is_deadlock(&self) -> bool {
        self.error.as_ref().is_some_and(ErrorFields::is_deadlock)
    }

    /// Compact one-line summary, e.g. `returns balance=100` or `UPDATE 1`.
    pub fn summary(&self) -> String {
        if let Some(err) = &self.error {
            return format!("ERROR {}: {}", err.code, err.message);
        }
        if let Some(row) = self.rows.first() {
            let values = row
                .iter()
                .map(|(c, v)| {
                    if row.len() == 1 {
                        v.clone()
                    } else {
                        format!("{c}={v}")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            if self.row_count > 1 {
                return format!("returns {values} (+{} rows)", self.row_count - 1);
            }
            return format!("returns {values}");
        }
        if self.row_count == 0
            && self
                .command_tags
                .last()
                .is_some_and(|t| t.starts_with("SELECT"))
        {
            return "returns 0 rows".into();
        }
        self.command_tags.last().cloned().unwrap_or_default()
    }
}

/// Incrementally builds a [`TurnResponse`] from backend frames.
#[derive(Debug, Default)]
pub struct ResponseCollector {
    response: TurnResponse,
    columns: Vec<ColumnDesc>,
    pending_describes: VecDeque<DescribeTarget>,
    described: Vec<(DescribeTarget, Vec<ColumnDesc>)>,
    result_shapes: VecDeque<ResultShape>,
    statement_layouts: HashMap<String, Vec<ColumnDesc>>,
    portal_layouts: HashMap<String, PortalLayout>,
    /// Result formats requested by a single `Bind` when used standalone.
    bind_formats: Option<Vec<FormatCode>>,
}

impl ResponseCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// For executions whose rows are not preceded by a `RowDescription`
    /// (`Bind` of a statement described in an earlier turn).
    pub fn with_columns(columns: Vec<ColumnDesc>) -> Self {
        Self {
            columns,
            ..Self::default()
        }
    }

    /// The result formats a single `Bind` requested. A `RowDescription`
    /// answering `Describe` of a *statement* reports every column as text,
    /// because formats are only chosen at `Bind`.
    pub fn with_bind_formats(mut self, formats: Option<Vec<FormatCode>>) -> Self {
        self.bind_formats = formats;
        self
    }

    /// Queues the `Describe` targets sent in this turn, in wire order.
    pub fn with_describes(mut self, describes: VecDeque<DescribeTarget>) -> Self {
        self.pending_describes = describes;
        self
    }

    /// Queues the `Execute` result shapes sent in this turn, in wire order.
    pub fn with_result_shapes(mut self, shapes: VecDeque<ResultShape>) -> Self {
        self.result_shapes = shapes;
        self
    }

    /// Seeds cached statement and portal column layouts from earlier turns.
    pub(crate) fn with_layouts(
        mut self,
        statement_layouts: HashMap<String, Vec<ColumnDesc>>,
        portal_layouts: HashMap<String, PortalLayout>,
    ) -> Self {
        self.statement_layouts = statement_layouts;
        self.portal_layouts = portal_layouts;
        self
    }

    /// Every `(target, columns)` resolved from `RowDescription` / `NoData`
    /// frames during this turn, in wire order.
    pub fn described(&self) -> &[(DescribeTarget, Vec<ColumnDesc>)] {
        &self.described
    }

    /// True once every queued `Describe` has received a response (or an error
    /// aborted the pipeline).
    pub fn pending_describes_done(&self) -> bool {
        self.pending_describes.is_empty()
    }

    fn resolve_column(&self, i: usize) -> (String, u32, FormatCode) {
        if let Some(shape) = self.result_shapes.front() {
            if let Some((saved_stmt, cols)) = self.portal_layouts.get(&shape.portal)
                && (saved_stmt.is_none() || saved_stmt.as_deref() == Some(shape.statement.as_str()))
                && let Some(c) = cols.get(i)
            {
                let fmt = if shape.formats.contains(&FormatCode::Binary)
                    && cols.iter().all(|col| col.format == FormatCode::Text)
                {
                    broadcast(&shape.formats, i)
                } else {
                    c.format
                };
                return (c.name.clone(), c.type_oid, fmt);
            }
            if let Some(cols) = self.statement_layouts.get(&shape.statement)
                && let Some(c) = cols.get(i)
            {
                return (c.name.clone(), c.type_oid, broadcast(&shape.formats, i));
            }
            return (format!("col{i}"), 0, broadcast(&shape.formats, i));
        }

        match self.columns.get(i) {
            Some(c) => (c.name.clone(), c.type_oid, c.format),
            None => (
                format!("col{i}"),
                0,
                self.bind_formats
                    .as_deref()
                    .map_or(FormatCode::Text, |f| broadcast(f, i)),
            ),
        }
    }

    /// Observes one backend frame. Returns `true` on `ReadyForQuery`.
    pub fn observe(&mut self, frame: &Frame) -> WireResult<bool> {
        match frame.tag {
            backend_tag::ROW_DESCRIPTION => {
                let raw_cols = decode_row_description(frame)?;
                if let Some(target) = self.pending_describes.pop_front() {
                    match &target {
                        DescribeTarget::Statement(name) => {
                            self.statement_layouts
                                .insert(name.clone(), raw_cols.clone());
                        }
                        DescribeTarget::Portal { portal, statement } => {
                            self.portal_layouts
                                .insert(portal.clone(), (statement.clone(), raw_cols.clone()));
                        }
                    }
                    self.described.push((target, raw_cols.clone()));
                }
                self.columns = raw_cols;
                let active_formats = self
                    .result_shapes
                    .front()
                    .map(|s| s.formats.as_slice())
                    .or(self.bind_formats.as_deref());
                if let Some(formats) = active_formats
                    && formats.contains(&FormatCode::Binary)
                    && self.columns.iter().all(|c| c.format == FormatCode::Text)
                {
                    for (i, c) in self.columns.iter_mut().enumerate() {
                        c.format = broadcast(formats, i);
                    }
                }
            }
            backend_tag::NO_DATA => {
                if let Some(target) = self.pending_describes.pop_front() {
                    match &target {
                        DescribeTarget::Statement(name) => {
                            self.statement_layouts.insert(name.clone(), Vec::new());
                        }
                        DescribeTarget::Portal { portal, statement } => {
                            self.portal_layouts
                                .insert(portal.clone(), (statement.clone(), Vec::new()));
                        }
                    }
                    self.described.push((target, Vec::new()));
                } else {
                    self.columns.clear();
                }
            }
            backend_tag::DATA_ROW => {
                self.response.row_count += 1;
                if self.response.rows.len() < RETAINED_ROWS {
                    let values = decode_data_row(frame)?;
                    let row = values
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let (name, oid, fmt) = self.resolve_column(i);
                            (name, render_value(v.as_deref(), fmt, oid))
                        })
                        .collect();
                    self.response.rows.push(row);
                }
            }
            backend_tag::COMMAND_COMPLETE => {
                self.response
                    .command_tags
                    .push(decode_command_complete(frame)?);
                self.result_shapes.pop_front();
            }
            backend_tag::EMPTY_QUERY | backend_tag::PORTAL_SUSPENDED => {
                self.result_shapes.pop_front();
            }
            backend_tag::ERROR_RESPONSE => {
                if self.response.error.is_none() {
                    self.response.error = Some(ErrorFields::decode(frame)?);
                }
                self.pending_describes.clear();
                self.result_shapes.clear();
            }
            backend_tag::READY_FOR_QUERY => {
                self.response.tx_status = Some(decode_ready_for_query(frame)?);
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }

    pub fn finish(self) -> TurnResponse {
        self.response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::Frame;
    use crate::messages::build;

    fn binary_row(values: &[&[u8]]) -> Frame {
        let mut b = bytes::BytesMut::new();
        b.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            b.extend_from_slice(&(val.len() as i32).to_be_bytes());
            b.extend_from_slice(val);
        }
        Frame::new(backend_tag::DATA_ROW, b.freeze())
    }

    #[test]
    fn statement_descriptions_take_their_formats_from_bind() {
        // `Describe` of a statement reports text for every column; the rows
        // that follow use the binary format the `Bind` asked for.
        let mut c = ResponseCollector::new().with_bind_formats(Some(vec![FormatCode::Binary]));
        for f in [
            build::row_description(&[("stock", 23)]),
            binary_row(&[&7i32.to_be_bytes()]),
            build::command_complete("SELECT 1"),
        ] {
            c.observe(&f).unwrap();
        }
        assert_eq!(c.finish().summary(), "returns 7");
    }

    #[test]
    fn pipelined_describes_preserve_each_statement_and_portal_layout() {
        let mut c = ResponseCollector::new()
            .with_describes(VecDeque::from([
                DescribeTarget::Statement("s1".into()),
                DescribeTarget::Statement("s2".into()),
                DescribeTarget::Portal {
                    portal: "p1".into(),
                    statement: Some("s1".into()),
                },
            ]))
            .with_result_shapes(VecDeque::from([
                ResultShape {
                    portal: "p1".into(),
                    statement: "s1".into(),
                    formats: vec![FormatCode::Binary],
                },
                ResultShape {
                    portal: "p2".into(),
                    statement: "s2".into(),
                    formats: Vec::new(),
                },
            ]));

        assert!(!c.pending_describes_done());
        c.observe(&build::row_description(&[("balance", 20)]))
            .unwrap();
        assert!(!c.pending_describes_done());
        // `NoData` for `s2` must NOT wipe `s1`'s column layout!
        c.observe(&build::no_data()).unwrap();
        assert!(!c.pending_describes_done());
        c.observe(&build::row_description(&[("balance", 20)]))
            .unwrap();
        assert!(c.pending_describes_done());

        assert_eq!(c.described().len(), 3);
        assert_eq!(c.described()[0].0, DescribeTarget::Statement("s1".into()));
        assert_eq!(c.described()[0].1.len(), 1);
        assert_eq!(c.described()[1].0, DescribeTarget::Statement("s2".into()));
        assert!(c.described()[1].1.is_empty());

        c.observe(&binary_row(&[&250i64.to_be_bytes()])).unwrap();
        c.observe(&build::command_complete("SELECT 1")).unwrap();
        c.observe(&build::command_complete("UPDATE 1")).unwrap();
        c.observe(&build::ready_for_query(TxStatus::InTransaction))
            .unwrap();

        let r = c.finish();
        assert_eq!(r.rows, vec![vec![("balance".into(), "250".into())]]);
        assert_eq!(r.command_tags, vec!["SELECT 1", "UPDATE 1"]);
    }

    #[test]
    fn pipelined_executes_switch_layouts_per_result_shape() {
        let mut stmt_layouts = HashMap::new();
        stmt_layouts.insert(
            "s1".into(),
            vec![ColumnDesc {
                name: "wallet_id".into(),
                type_oid: 23,
                format: FormatCode::Text,
            }],
        );
        stmt_layouts.insert(
            "s2".into(),
            vec![ColumnDesc {
                name: "balance".into(),
                type_oid: 20,
                format: FormatCode::Text,
            }],
        );

        let mut c = ResponseCollector::new()
            .with_layouts(stmt_layouts, HashMap::new())
            .with_result_shapes(VecDeque::from([
                ResultShape {
                    portal: String::new(),
                    statement: "s1".into(),
                    formats: vec![FormatCode::Binary],
                },
                ResultShape {
                    portal: String::new(),
                    statement: "s2".into(),
                    formats: vec![FormatCode::Binary],
                },
            ]));

        c.observe(&binary_row(&[&9i32.to_be_bytes()])).unwrap();
        c.observe(&build::command_complete("SELECT 1")).unwrap();
        c.observe(&binary_row(&[&500i64.to_be_bytes()])).unwrap();
        c.observe(&build::command_complete("SELECT 1")).unwrap();
        c.observe(&build::ready_for_query(TxStatus::Idle)).unwrap();

        let r = c.finish();
        assert_eq!(
            r.rows,
            vec![
                vec![("wallet_id".into(), "9".into())],
                vec![("balance".into(), "500".into())],
            ]
        );
    }

    #[test]
    fn collects_rows_tags_and_status() {
        let mut c = ResponseCollector::new();
        for f in [
            build::row_description(&[("balance", 20)]),
            build::data_row(&[Some("100")]),
            build::command_complete("SELECT 1"),
        ] {
            assert!(!c.observe(&f).unwrap());
        }
        assert!(
            c.observe(&build::ready_for_query(TxStatus::InTransaction))
                .unwrap()
        );
        let r = c.finish();
        assert_eq!(r.tx_status, Some(TxStatus::InTransaction));
        assert_eq!(r.summary(), "returns 100");
        assert_eq!(r.row_count, 1);
    }

    #[test]
    fn error_summary_and_deadlock() {
        let mut c = ResponseCollector::new();
        c.observe(&build::error_response("40P01", "deadlock detected"))
            .unwrap();
        c.observe(&build::ready_for_query(TxStatus::Failed))
            .unwrap();
        let r = c.finish();
        assert!(r.is_deadlock());
        assert_eq!(r.sqlstate(), Some("40P01"));
        assert!(r.summary().starts_with("ERROR 40P01"));
    }

    #[test]
    fn command_tag_summary() {
        let mut c = ResponseCollector::new();
        c.observe(&build::command_complete("UPDATE 1")).unwrap();
        assert_eq!(c.finish().summary(), "UPDATE 1");
        let mut c = ResponseCollector::new();
        c.observe(&build::row_description(&[("id", 23), ("balance", 20)]))
            .unwrap();
        c.observe(&build::data_row(&[Some("1"), Some("5")]))
            .unwrap();
        c.observe(&build::data_row(&[Some("2"), Some("6")]))
            .unwrap();
        c.observe(&build::command_complete("SELECT 2")).unwrap();
        assert_eq!(c.finish().summary(), "returns id=1, balance=5 (+1 rows)");
        let mut c = ResponseCollector::new();
        c.observe(&build::command_complete("SELECT 0")).unwrap();
        assert_eq!(c.finish().summary(), "returns 0 rows");
    }
}
