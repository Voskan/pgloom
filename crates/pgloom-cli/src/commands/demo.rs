use std::net::SocketAddr;
use std::time::{Duration, Instant};

use pgloom_driver::{
    Config, DatabaseConfig, ExplorationConfig, HttpWorker, InvariantsConfig, ResponseInvariant,
    SqlInvariantConfig, StrategyName, WorkerConfig,
};
use pgloom_engine::{RunOutcome, RunReport};
use pgloom_pgctrl::{PgControl, PgUrl};
use pgloom_report::render_run;
use tokio::process::Command;

use crate::cli::DemoArgs;
use crate::commands::run::{plan_line, report_context};
use crate::error::{CliError, EXIT_PASS};
use crate::output::Printer;
use crate::session::{RunOptions, Session};
use crate::wallet::{WalletMode, WalletServer};

const SCHEMA: &str = "_pgloom_demo";
const LOCAL_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/postgres";
const DOCKER_IMAGE: &str = "postgres:17-alpine";

/// Builds the demo scenario against `upstream`, with the proxy on
/// `proxy_listen` and the wallet service at `service`.
pub fn demo_config(upstream: &str, proxy_listen: SocketAddr, service: SocketAddr) -> Config {
    let table = format!("{SCHEMA}.wallets");
    let worker = |id: &str| WorkerConfig {
        id: id.into(),
        http: Some(HttpWorker {
            method: "POST".into(),
            url: format!("http://{service}/withdraw"),
            headers: [("Content-Type".to_owned(), "application/json".to_owned())].into(),
            body: Some(r#"{"account_id": 1, "amount": 100}"#.into()),
            timeout_ms: 10_000,
        }),
        sql: None,
        exec: None,
    };
    Config {
        version: 1,
        name: "demo-wallet-withdraw".into(),
        database: DatabaseConfig {
            upstream: upstream.into(),
            proxy_listen: proxy_listen.to_string(),
            setup_sql: Some(format!(
                "CREATE SCHEMA IF NOT EXISTS {SCHEMA};\n{}",
                crate::wallet::setup_sql(&table, 100)
            )),
            seed_sql: None,
            reset_sql: Some(format!("UPDATE {table} SET balance = 100;")),
            snapshot_exclude: Vec::new(),
        },
        target: None,
        exploration: ExplorationConfig {
            strategy: StrategyName::Bounded,
            max_preemptions: 2,
            max_schedules: 50,
            ..ExplorationConfig::default()
        },
        workers: vec![worker("req_a"), worker("req_b")],
        invariants: InvariantsConfig {
            sql: vec![SqlInvariantConfig {
                name: "no_negative_balance".into(),
                description: Some("Wallet balance must never drop below zero".into()),
                assert_true: format!("SELECT COALESCE(MIN(balance), 0) >= 0 FROM {table}"),
                diagnostic_query: Some(format!("SELECT id, balance FROM {table} WHERE id = 1")),
            }],
            responses: vec![ResponseInvariant {
                name: "at_most_one_successful_withdrawal".into(),
                description: None,
                max_2xx_count: Some(1),
                min_2xx_count: None,
                allowed_statuses: None,
                ..ResponseInvariant::default()
            }],
            fail_on_deadlock: true,
        },
    }
}

/// Reserves an ephemeral loopback port.
pub async fn free_port() -> Result<SocketAddr, CliError> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| CliError::env("cannot reserve a local port", e))?;
    l.local_addr()
        .map_err(|e| CliError::env("cannot reserve a local port", e))
}

#[derive(Debug)]
struct DemoDatabase {
    url: String,
    container: Option<String>,
}

impl DemoDatabase {
    async fn stop(self) {
        if let Some(id) = self.container {
            let _ = Command::new("docker")
                .args(["stop", "-t", "1", &id])
                .output()
                .await;
        }
    }
}

async fn reachable(url: &str) -> bool {
    tokio::time::timeout(Duration::from_secs(3), PgControl::connect(url))
        .await
        .is_ok_and(|r| r.is_ok())
}

async fn find_database(
    explicit: Option<String>,
    printer: &Printer,
) -> Result<DemoDatabase, CliError> {
    let candidates = [
        explicit,
        std::env::var("PGLOOM_UPSTREAM_URL").ok(),
        std::env::var("DATABASE_URL").ok(),
    ];
    if let Some(url) = candidates.into_iter().flatten().find(|u| !u.is_empty()) {
        PgControl::connect(&url)
            .await
            .map_err(|e| CliError::env("cannot connect to the demo database", e))?;
        return Ok(DemoDatabase {
            url,
            container: None,
        });
    }
    if reachable(LOCAL_URL).await {
        return Ok(DemoDatabase {
            url: LOCAL_URL.into(),
            container: None,
        });
    }
    printer.progress(&format!(
        "  no PostgreSQL on localhost:5432; starting a throwaway {DOCKER_IMAGE} container"
    ));
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "-e",
            "POSTGRES_PASSWORD=pgloom",
            "-p",
            "127.0.0.1::5432",
            DOCKER_IMAGE,
        ])
        .output()
        .await
        .map_err(|e| CliError::env("no database found; pass --upstream or install Docker", e))?;
    if !out.status.success() {
        return Err(CliError::Environment(format!(
            "docker run failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    let id = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    let db = DemoDatabase {
        url: String::new(),
        container: Some(id.clone()),
    };
    let port = Command::new("docker")
        .args(["port", &id, "5432/tcp"])
        .output()
        .await
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .next()
                .and_then(|l| l.rsplit(':').next())
                .and_then(|p| p.trim().parse::<u16>().ok())
        });
    let Some(port) = port else {
        db.stop().await;
        return Err(CliError::Environment(
            "cannot determine the container's port".into(),
        ));
    };
    let url = format!("postgres://postgres:pgloom@127.0.0.1:{port}/postgres");
    let deadline = Instant::now() + Duration::from_secs(60);
    while !reachable(&url).await {
        if Instant::now() > deadline {
            db.stop().await;
            return Err(CliError::Environment(
                "the PostgreSQL container did not become ready within 60s".into(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(DemoDatabase {
        url,
        container: Some(id),
    })
}

async fn act(
    config: &Config,
    wallet: &WalletServer,
    printer: &Printer,
) -> Result<RunReport, CliError> {
    let session = Session::start(config.clone(), RunOptions::default()).await?;
    let result = session.run().await;
    let workers = session.workers().to_vec();
    session.close().await;
    wallet.clear_pool().await;
    let report = result?;
    printer.report(&render_run(
        &report_context(config, &workers, printer),
        &report,
    ));
    if report.interrupted {
        return Err(CliError::Interrupted);
    }
    Ok(report)
}

pub async fn demo(args: DemoArgs, printer: Printer) -> Result<u8, CliError> {
    let banner = |t: &str| printer.paint(t, "1");
    printer.report(&format!(
        "{}\n  A wallet service holding $100 receives two concurrent $100 withdrawals.\n",
        banner("PgLoom demo: the classic READ COMMITTED double spend")
    ));
    let db = find_database(args.upstream, &printer).await?;
    let result = run_acts(&db.url, &printer).await;
    if let Ok(ctl) = PgControl::connect(&db.url).await {
        let _ = ctl
            .batch(
                &format!("DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"),
                "demo cleanup",
            )
            .await;
    }
    db.stop().await;
    result
}

async fn run_acts(url: &str, printer: &Printer) -> Result<u8, CliError> {
    let proxy = free_port().await?;
    let proxy_url = PgUrl::parse(url)
        .map_err(|e| CliError::Config(e.to_string()))?
        .with_host_port("127.0.0.1", proxy.port())
        .with_ssl_disabled()
        .to_string();
    let wallet = WalletServer::start(
        "127.0.0.1:0"
            .parse()
            .map_err(|_| CliError::Internal("address".into()))?,
        &proxy_url,
        &format!("{SCHEMA}.wallets"),
        WalletMode::CheckThenAct,
    )
    .await
    .map_err(|e| CliError::env("cannot start the demo service", e))?;
    let config = demo_config(url, proxy, wallet.addr());
    printer.progress(&format!(
        "  database {} · proxy {proxy} · service http://{}\n  {}\n",
        PgUrl::parse(url).map_or_else(|_| url.to_owned(), |u| u.redacted()),
        wallet.addr(),
        plan_line(&config)
    ));

    printer.report(&format!(
        "{}\n  BEGIN; SELECT balance ...; if balance >= amount {{ UPDATE ... balance - amount }}; COMMIT\n",
        banner_line(printer, "Act 1: the handler as written")
    ));
    let first = act(&config, &wallet, printer).await;
    let first = match first {
        Ok(r) => r,
        Err(e) => {
            wallet.stop().await;
            return Err(e);
        }
    };

    wallet.set_mode(WalletMode::RowLock);
    printer.report(&format!(
        "\n{}\n  SELECT balance FROM wallets WHERE id = $1 FOR NO KEY UPDATE\n",
        banner_line(printer, "Act 2: the Fix Advisor's patch applied")
    ));
    let second = act(&config, &wallet, printer).await;
    wallet.stop().await;
    let second = second?;

    if first.outcome == RunOutcome::ViolationFound && second.outcome == RunOutcome::Pass {
        printer.report(&format!(
            "\n{}",
            printer.paint(
                "PASS (all schedules verified, 0 anomalies). The race is fixed.",
                "1;32"
            )
        ));
        Ok(EXIT_PASS)
    } else {
        Err(CliError::Internal(
            "the demo did not behave as expected (see the reports above)".into(),
        ))
    }
}

fn banner_line(printer: &Printer, title: &str) -> String {
    printer.paint(&format!("━━ {title} ━━"), "1;36")
}
