//! The control plane PgLoom uses next to the proxied application traffic:
//! a direct PostgreSQL session for setup, `pg_blocking_pids()` inspection,
//! backend cancellation, snapshot/restore and SQL invariants.

#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod catalog;
pub mod control;
pub mod error;
pub mod invariant;
pub mod snapshot;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing;
pub mod url;

pub use catalog::{CatalogLink, CatalogRelation, LinkKind, RelationKind, SchemaCatalog};
pub use control::{BlockingInfo, PgControl};
pub use error::{PgCtrlError, PgCtrlResult};
pub use invariant::{InvariantResult, SqlInvariant};
pub use snapshot::{RestoreStats, Snapshot, SnapshotOptions, quote_ident};
pub use url::PgUrl;
