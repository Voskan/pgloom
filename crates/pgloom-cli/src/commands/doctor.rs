use std::net::SocketAddr;
use std::time::Instant;

use pgloom_driver::{Config, DEFAULT_PROXY_LISTEN};
use pgloom_pgctrl::{PgControl, PgUrl};

use crate::cli::DoctorArgs;
use crate::error::{CliError, EXIT_CONFIG, EXIT_PASS};
use crate::output::Printer;

#[derive(Debug)]
struct Check {
    ok: bool,
    /// Not fatal: PgLoom works, but with a limitation.
    warn: bool,
    label: String,
    detail: String,
}

fn pass(label: &str, detail: impl Into<String>) -> Check {
    Check {
        ok: true,
        warn: false,
        label: label.into(),
        detail: detail.into(),
    }
}

fn fail(label: &str, detail: impl Into<String>) -> Check {
    Check {
        ok: false,
        warn: false,
        label: label.into(),
        detail: detail.into(),
    }
}

fn warn(label: &str, detail: impl Into<String>) -> Check {
    Check {
        ok: true,
        warn: true,
        label: label.into(),
        detail: detail.into(),
    }
}

async fn bool_query(ctl: &PgControl, sql: &str) -> Result<bool, String> {
    let rows = ctl
        .simple_rows(sql, "doctor")
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .first()
        .and_then(|r| r.first())
        .and_then(|(_, v)| v.as_deref())
        == Some("t"))
}

pub async fn doctor(args: DoctorArgs, printer: Printer) -> Result<u8, CliError> {
    let mut checks = Vec::new();
    let mut upstream = args.connection.upstream.clone();
    let mut listen = args.connection.listen.clone();
    let mut has_reset_sql = false;
    if args.config.exists() {
        match Config::load_with_overrides(
            &args.config,
            args.connection.upstream.as_deref(),
            args.connection.listen.as_deref(),
        ) {
            Ok(loaded) => {
                checks.push(pass(
                    "scenario",
                    format!("{} is valid", args.config.display()),
                ));
                has_reset_sql = loaded.config.database.reset_sql.is_some();
                upstream = upstream.or(Some(loaded.config.database.upstream));
                listen = listen.or(Some(loaded.config.database.proxy_listen));
            }
            Err(e) => checks.push(fail("scenario", e.to_string())),
        }
    }
    let listen = listen.unwrap_or_else(|| DEFAULT_PROXY_LISTEN.to_owned());

    match listen.parse::<SocketAddr>() {
        Ok(addr) => match tokio::net::TcpListener::bind(addr).await {
            Ok(_) => checks.push(pass("proxy port", format!("{addr} is free"))),
            Err(e) => checks.push(fail("proxy port", format!("cannot listen on {addr}: {e}"))),
        },
        Err(_) => checks.push(fail("proxy port", format!("`{listen}` is not HOST:PORT"))),
    }

    match upstream {
        None => checks.push(fail(
            "database",
            "no scenario file and no --upstream / PGLOOM_UPSTREAM_URL given",
        )),
        Some(url) => database_checks(&url, has_reset_sql, &mut checks).await,
    }

    let mut failed = false;
    let mut out = String::new();
    for c in &checks {
        let mark = if !c.ok {
            failed = true;
            printer.paint("✗", "31")
        } else if c.warn {
            printer.paint("!", "33")
        } else {
            printer.paint("✓", "32")
        };
        out.push_str(&format!("{mark} {:<18} {}\n", c.label, c.detail));
    }
    printer.report(&out);
    Ok(if failed { EXIT_CONFIG } else { EXIT_PASS })
}

async fn database_checks(url: &str, has_reset_sql: bool, checks: &mut Vec<Check>) {
    let redacted = PgUrl::parse(url).map_or_else(|_| url.to_owned(), |u| u.redacted());
    let ctl = match PgControl::connect(url).await {
        Ok(c) => c,
        Err(e) => {
            checks.push(fail("database", e.to_string()));
            return;
        }
    };
    let version = ctl.server_version().await.unwrap_or_default();
    checks.push(pass(
        "database",
        format!("{redacted} (PostgreSQL {version})"),
    ));

    let major: u32 = version
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if major >= 12 {
        checks.push(pass(
            "server version",
            "pg_blocking_pids() and OVERRIDING SYSTEM VALUE available",
        ));
    } else {
        checks.push(fail("server version", "PostgreSQL 12 or newer is required"));
    }

    match bool_query(
        &ctl,
        "SELECT rolsuper OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') \
         FROM pg_roles WHERE rolname = current_user",
    )
    .await
    {
        Ok(true) => checks.push(pass("pg_stat_activity", "can see every session's state")),
        Ok(false) => checks.push(warn(
            "pg_stat_activity",
            "only own-role sessions are visible; the service must connect as the same role",
        )),
        Err(e) => checks.push(fail("pg_stat_activity", e)),
    }

    match bool_query(
        &ctl,
        "SELECT rolsuper OR pg_has_role(current_user, 'pg_signal_backend', 'MEMBER') \
         FROM pg_roles WHERE rolname = current_user",
    )
    .await
    {
        Ok(true) => checks.push(pass("cancel/terminate", "can signal other backends")),
        Ok(false) => checks.push(warn(
            "cancel/terminate",
            "can signal only own-role backends (grant pg_signal_backend otherwise)",
        )),
        Err(e) => checks.push(fail("cancel/terminate", e)),
    }

    if has_reset_sql {
        checks.push(pass(
            "snapshot schema",
            "not needed (database.reset_sql is set)",
        ));
    } else {
        match bool_query(
            &ctl,
            "SELECT has_database_privilege(current_database(), 'CREATE')",
        )
        .await
        {
            Ok(true) => checks.push(pass("snapshot schema", "may create _pgloom_snap")),
            Ok(false) => checks.push(fail(
                "snapshot schema",
                "CREATE on the database is required for the _pgloom_snap schema (or set reset_sql)",
            )),
            Err(e) => checks.push(fail("snapshot schema", e)),
        }
    }

    let samples = 10;
    let started = Instant::now();
    let mut ok = true;
    for _ in 0..samples {
        ok &= ctl.batch("SELECT 1", "latency probe").await.is_ok();
    }
    let avg = started.elapsed().as_secs_f64() * 1000.0 / f64::from(samples);
    if !ok {
        checks.push(fail("latency", "probe queries failed"));
    } else if avg > 5.0 {
        checks.push(warn(
            "latency",
            format!("{avg:.2}ms per round trip; consider raising exploration.lock_probe_ms"),
        ));
    } else {
        checks.push(pass("latency", format!("{avg:.2}ms per round trip")));
    }
}
