//! Reporting for PgLoom: the deterministic execution timeline, the Fix
//! Advisor, JSON output and `.pgloom-repro.json` replay files.

#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]

pub mod advisor;
pub mod json;
pub mod repro;
mod style;
pub mod text;
pub mod timeline;

pub use advisor::{Advice, Fix, Pattern, advise};
pub use json::{JsonReport, json_report, render_json};
pub use repro::{DEFAULT_REPRO_PATH, FORMAT_VERSION, ReproError, ReproFile, ReproStep, sha256_hex};
pub use text::{
    AUDIT_FOOTER, ReportContext, format_duration, render_episode, render_failure, render_run,
};
pub use timeline::{TimelineOptions, render_timeline};
