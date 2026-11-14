use tokio_postgres::Row;
use tokio_postgres::types::FromSql;

/// Errors from the control connection.
#[derive(Debug, thiserror::Error)]
pub enum PgCtrlError {
    #[error("{0}")]
    Config(String),

    #[error("cannot connect to PostgreSQL at {target}: {}", detail(source))]
    Connect {
        target: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("{context}: {}", detail(source))]
    Query {
        context: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("{context}: timed out after {ms} ms")]
    Timeout { context: String, ms: u64 },

    #[error("snapshot error: {0}")]
    Snapshot(String),
}

impl PgCtrlError {
    pub(crate) fn query(context: impl Into<String>) -> impl FnOnce(tokio_postgres::Error) -> Self {
        let context = context.into();
        move |source| PgCtrlError::Query { context, source }
    }

    /// The server's SQLSTATE, if the error came from PostgreSQL.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            PgCtrlError::Query { source, .. } | PgCtrlError::Connect { source, .. } => {
                source.code().map(|c| c.code())
            }
            _ => None,
        }
    }
}

pub type PgCtrlResult<T> = Result<T, PgCtrlError>;

/// The actionable part of a driver error. `tokio_postgres::Error` displays
/// only its category ("db error", "error connecting to server"); the server
/// message or the operating-system cause sits behind it.
pub fn detail(err: &tokio_postgres::Error) -> String {
    if let Some(db) = err.as_db_error() {
        let mut out = format!("{} (SQLSTATE {})", db.message(), db.code().code());
        if let Some(hint) = db.hint() {
            out.push_str("; hint: ");
            out.push_str(hint);
        }
        return out;
    }
    let mut out = err.to_string();
    let mut cause = std::error::Error::source(err);
    while let Some(c) = cause {
        let text = c.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        cause = c.source();
    }
    out
}

/// Column access that reports a type mismatch or a missing column as an
/// error instead of panicking like `Row::get`: catalog columns differ across
/// server versions and extensions.
pub(crate) trait RowExt {
    fn col<'a, T: FromSql<'a>>(&'a self, idx: usize, context: &str) -> PgCtrlResult<T>;
}

impl RowExt for Row {
    fn col<'a, T: FromSql<'a>>(&'a self, idx: usize, context: &str) -> PgCtrlResult<T> {
        self.try_get(idx).map_err(PgCtrlError::query(context))
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::testing::TestDatabase;

    #[tokio::test]
    async fn column_type_mismatch_is_an_error_not_a_panic() {
        let Some(db) = TestDatabase::create().await else {
            return;
        };
        let ctl = crate::PgControl::connect(db.url()).await.unwrap();
        let row = ctl
            .client()
            .query_one("SELECT 'x'::text, NULL::int4", &[])
            .await
            .unwrap();
        let err = row.col::<i64>(0, "reading things").unwrap_err();
        assert!(err.to_string().starts_with("reading things: "), "{err}");
        assert!(
            row.col::<i32>(1, "nullable").is_err(),
            "NULL into non-Option"
        );
        assert_eq!(row.col::<Option<i32>>(1, "nullable").unwrap(), None);
        assert!(row.col::<String>(7, "out of range").is_err());
        assert_eq!(row.col::<String>(0, "ok").unwrap(), "x");
        drop(ctl);
        db.cleanup().await;
    }
}
