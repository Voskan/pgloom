/// Errors from the control connection.
#[derive(Debug, thiserror::Error)]
pub enum PgCtrlError {
    #[error("{0}")]
    Config(String),

    #[error("cannot connect to PostgreSQL at {target}: {source}")]
    Connect {
        target: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("{context}: {source}")]
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
