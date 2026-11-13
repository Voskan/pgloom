//! Disposable databases for integration tests (enabled by the `testing` feature).
//!
//! Tests read `PGLOOM_TEST_DATABASE_URL` (an administrative URL such as
//! `postgres://postgres:postgres@127.0.0.1:5432/postgres`). When it is unset,
//! the conventional local default is tried; when no server is reachable the
//! caller receives `None` and should skip.

#![allow(clippy::print_stderr, clippy::panic)]

use std::sync::atomic::{AtomicU32, Ordering};

use tokio_postgres::NoTls;

use crate::url::PgUrl;

pub const ENV_VAR: &str = "PGLOOM_TEST_DATABASE_URL";
const DEFAULT_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/postgres";

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A freshly created database, dropped by [`TestDatabase::cleanup`].
#[derive(Debug)]
pub struct TestDatabase {
    admin_url: String,
    name: String,
    url: String,
}

impl TestDatabase {
    /// Creates an empty database, or returns `None` if PostgreSQL is unavailable.
    pub async fn create() -> Option<Self> {
        let explicit = std::env::var(ENV_VAR).ok();
        let admin_url = explicit.clone().unwrap_or_else(|| DEFAULT_URL.to_owned());
        let admin = match tokio_postgres::connect(&admin_url, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(conn);
                client
            }
            Err(err) => {
                if explicit.is_some() {
                    panic!("{ENV_VAR} is set but PostgreSQL is unreachable: {err}");
                }
                eprintln!("skipping: no PostgreSQL at {DEFAULT_URL} (set {ENV_VAR}): {err}");
                return None;
            }
        };
        let name = format!(
            "pgloom_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        // CREATE DATABASE cannot share a multi-statement (implicitly
        // transactional) batch, and concurrent creations from template1 can
        // transiently fail, so issue it alone and retry briefly.
        let _ = admin
            .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
            .await;
        let mut attempt = 0;
        loop {
            match admin
                .batch_execute(&format!("CREATE DATABASE {name}"))
                .await
            {
                Ok(()) => break,
                Err(err) if attempt < 20 => {
                    attempt += 1;
                    eprintln!("retrying CREATE DATABASE {name}: {err:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(err) => panic!("cannot create test database {name}: {err:?}"),
            }
        }
        let parsed = PgUrl::parse(&admin_url).unwrap_or_else(|e| panic!("{e}"));
        let base = parsed.to_string();
        let url = match base.split_once("://") {
            Some((scheme, rest)) => {
                let (authority, _) = rest.split_once('/').unwrap_or((rest, ""));
                format!("{scheme}://{authority}/{name}")
            }
            None => unreachable!("PgUrl always renders a scheme"),
        };
        Some(Self {
            admin_url,
            name,
            url,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Drops the database, terminating any remaining sessions.
    pub async fn cleanup(self) {
        if let Ok((client, conn)) = tokio_postgres::connect(&self.admin_url, NoTls).await {
            tokio::spawn(conn);
            let _ = client
                .batch_execute(&format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    self.name
                ))
                .await;
        }
    }
}
