//! Summaries of backend responses, used for reports and invariants.

use serde::{Deserialize, Serialize};

use crate::error::WireResult;
use crate::frame::Frame;
use crate::messages::{
    ColumnDesc, ErrorFields, TxStatus, backend_tag, decode_command_complete, decode_data_row,
    decode_ready_for_query, decode_row_description,
};
use crate::values::render_value;

/// Number of leading rows whose values are retained for reporting.
const RETAINED_ROWS: usize = 3;

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
    described: bool,
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

    /// `Some(columns)` once a `RowDescription` was seen, `Some(empty)` after
    /// `NoData`.
    pub fn described(&self) -> Option<&[ColumnDesc]> {
        self.described.then_some(self.columns.as_slice())
    }

    /// Observes one backend frame. Returns `true` on `ReadyForQuery`.
    pub fn observe(&mut self, frame: &Frame) -> WireResult<bool> {
        match frame.tag {
            backend_tag::ROW_DESCRIPTION => {
                self.columns = decode_row_description(frame)?;
                self.described = true;
            }
            backend_tag::NO_DATA => {
                self.columns.clear();
                self.described = true;
            }
            backend_tag::DATA_ROW => {
                self.response.row_count += 1;
                if self.response.rows.len() < RETAINED_ROWS {
                    let values = decode_data_row(frame)?;
                    let row = values
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let (name, oid, fmt) = match self.columns.get(i) {
                                Some(c) => (c.name.clone(), c.type_oid, c.format),
                                None => (format!("col{i}"), 0, crate::messages::FormatCode::Text),
                            };
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
            }
            backend_tag::ERROR_RESPONSE => {
                if self.response.error.is_none() {
                    self.response.error = Some(ErrorFields::decode(frame)?);
                }
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
    use crate::messages::build;

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
