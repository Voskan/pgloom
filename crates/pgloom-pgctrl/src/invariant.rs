//! Post-episode SQL invariants.

use serde::{Deserialize, Serialize};

use crate::control::PgControl;
use crate::error::PgCtrlResult;

/// A boolean assertion over database state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlInvariant {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Must return no rows, or `true` in the first column of the first row.
    pub assert_true: String,
    /// Run on failure; its rows are shown as the observed state.
    #[serde(default)]
    pub diagnostic_query: Option<String>,
}

/// Result of checking one invariant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvariantResult {
    pub name: String,
    pub passed: bool,
    /// Diagnostic rows (or the offending value) when the check failed.
    pub observed: String,
}

/// Interprets the first column of the first row as a verdict.
pub fn verdict(first_value: Option<Option<&str>>) -> bool {
    match first_value {
        // No rows: nothing violates the invariant.
        None => true,
        Some(Some(v)) => matches!(v.trim(), "t" | "true" | "TRUE" | "True"),
        Some(None) => false,
    }
}

/// Formats rows as `col=value, col=value; ...` (at most `max_rows`).
pub fn format_rows(rows: &[Vec<(String, Option<String>)>], max_rows: usize) -> String {
    let mut parts: Vec<String> = rows
        .iter()
        .take(max_rows)
        .map(|row| {
            row.iter()
                .map(|(c, v)| format!("{c}={}", v.as_deref().unwrap_or("NULL")))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();
    if rows.len() > max_rows {
        parts.push(format!("… {} more rows", rows.len() - max_rows));
    }
    parts.join("; ")
}

impl PgControl {
    pub async fn check_invariant(&self, inv: &SqlInvariant) -> PgCtrlResult<InvariantResult> {
        let rows = self
            .simple_rows(&inv.assert_true, &format!("invariant `{}`", inv.name))
            .await?;
        let first = rows
            .first()
            .map(|r| r.first().and_then(|(_, v)| v.as_deref()));
        let passed = verdict(first);
        let observed = if passed {
            String::new()
        } else if let Some(diag) = &inv.diagnostic_query {
            let diag_rows = self
                .simple_rows(diag, &format!("diagnostic query of `{}`", inv.name))
                .await?;
            format_rows(&diag_rows, 5)
        } else {
            format_rows(&rows, 5)
        };
        Ok(InvariantResult {
            name: inv.name.clone(),
            passed,
            observed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_rules() {
        assert!(verdict(None));
        assert!(verdict(Some(Some("t"))));
        assert!(verdict(Some(Some("true"))));
        assert!(!verdict(Some(Some("f"))));
        assert!(!verdict(Some(Some("1"))));
        assert!(!verdict(Some(None)));
    }

    #[test]
    fn formats_rows() {
        let rows = vec![
            vec![
                ("id".to_string(), Some("1".to_string())),
                ("balance".into(), Some("-100".into())),
            ],
            vec![
                ("id".to_string(), Some("2".to_string())),
                ("balance".into(), None),
            ],
        ];
        assert_eq!(
            format_rows(&rows, 5),
            "id=1, balance=-100; id=2, balance=NULL"
        );
        assert_eq!(format_rows(&rows, 1), "id=1, balance=-100; … 1 more rows");
    }
}
