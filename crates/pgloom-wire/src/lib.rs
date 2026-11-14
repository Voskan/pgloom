//! PostgreSQL v3 wire protocol support for PgLoom.
//!
//! This crate provides an incremental frame codec, typed decoders for the
//! messages that matter to scheduling, a lexer-based SQL classifier, the
//! [`turn::TurnAssembler`] that groups client messages into atomic
//! [`turn::WireTurn`]s, and a transparent L7 [`proxy`] whose [`proxy::Gate`]
//! lets a controller decide which turn reaches the server next.

#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod classify;
pub mod error;
pub mod frame;
pub mod io;
pub mod messages;
pub mod proxy;
pub mod response;
#[cfg(feature = "testkit")]
#[doc(hidden)]
pub mod testkit;
pub mod turn;
pub mod values;

pub use classify::{
    Access, RowLock, StatementKind, TurnKind, TurnMeta, TxControl, classify_access, classify_sql,
};
pub use error::{WireError, WireResult};
pub use messages::{BackendPid, ErrorFields, TxStatus};
pub use proxy::{
    ConnId, FinishedTurn, Gate, GateEvent, PendingTurn, Permit, ProxyConfig, ProxyHandle, Release,
    TurnInfo, spawn_proxy,
};
pub use response::TurnResponse;
pub use turn::{BoundStatement, DescribeTarget, WireTurn};
