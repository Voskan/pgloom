//! The canonical end-to-end suite (C-01..C-06): a real wallet service, the
//! PgLoom proxy and a real PostgreSQL server.
//!
//! Requires `PGLOOM_TEST_DATABASE_URL` (default
//! `postgres://postgres:postgres@127.0.0.1:5432/postgres`); skipped when the
//! server is unreachable.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use assert_cmd::Command;
use pgloom_cli::commands::demo::free_port;
use pgloom_cli::session::{RunOptions, Session};
use pgloom_cli::wallet::{WalletMode, WalletServer, setup_sql};
use pgloom_driver::Config;
use pgloom_engine::{RunOutcome, RunReport, Violation};
use pgloom_pgctrl::PgUrl;
use pgloom_pgctrl::testing::TestDatabase;
use pgloom_report::{Pattern, ReproFile, advise};

/// Latency budgets are enforced strictly on a developer machine. Shared CI
/// runners execute the PostgreSQL matrix in parallel on noisy hardware, so
/// there the budget only guards against order-of-magnitude regressions.
fn budget(local: Duration) -> Duration {
    if std::env::var_os("CI").is_some() {
        local * 10
    } else {
        local
    }
}

struct Env {
    db: TestDatabase,
    wallet: WalletServer,
    proxy: SocketAddr,
}

impl Env {
    async fn start(mode: WalletMode) -> Option<Self> {
        let db = TestDatabase::create().await?;
        let proxy = free_port().await.unwrap();
        let proxy_url = PgUrl::parse(db.url())
            .unwrap()
            .with_host_port("127.0.0.1", proxy.port())
            .with_ssl_disabled()
            .to_string();
        let wallet =
            WalletServer::start("127.0.0.1:0".parse().unwrap(), &proxy_url, "wallets", mode)
                .await
                .unwrap();
        Some(Self { db, wallet, proxy })
    }

    fn yaml(&self, workers: &str, invariants: &str) -> String {
        format!(
            r#"
version: 1
name: canonical
database:
  upstream: "{upstream}"
  proxy_listen: "{proxy}"
  setup_sql: |
{setup}
exploration:
  strategy: bounded
  max_preemptions: 2
  max_schedules: 20
workers:
{workers}
invariants:
{invariants}
"#,
            upstream = self.db.url(),
            proxy = self.proxy,
            setup = setup_sql("wallets", 100)
                .lines()
                .map(|l| format!("    {l}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }

    fn withdraw_yaml(&self) -> String {
        let url = self.wallet.url("/withdraw");
        self.yaml(
            &format!(
                r#"  - id: req_a
    http: {{ method: POST, url: "{url}", headers: {{ Content-Type: application/json }}, body: '{{"account_id": 1, "amount": 100}}' }}
  - id: req_b
    http: {{ method: POST, url: "{url}", headers: {{ Content-Type: application/json }}, body: '{{"account_id": 1, "amount": 100}}' }}"#
            ),
            r#"  sql:
    - name: no_negative_balance
      description: Wallet balance must never drop below zero
      assert_true: "SELECT COALESCE(MIN(balance), 0) >= 0 FROM wallets"
      diagnostic_query: "SELECT id, balance FROM wallets WHERE id = 1"
  responses:
    - name: at_most_one_successful_withdrawal
      max_2xx_count: 1"#,
        )
    }

    fn config(yaml: &str) -> Config {
        Config::parse_with_env(yaml, "canonical.yaml", |_| None).unwrap()
    }

    async fn run(&self, yaml: &str) -> RunReport {
        self.run_with(yaml, RunOptions::default()).await
    }

    async fn run_with(&self, yaml: &str, options: RunOptions) -> RunReport {
        let session = Session::start(Self::config(yaml), options).await.unwrap();
        let report = session.run().await;
        session.close().await;
        self.wallet.clear_pool().await;
        report.unwrap()
    }

    async fn finish(self) {
        self.wallet.stop().await;
        self.db.cleanup().await;
    }
}

fn names(violations: &[Violation]) -> Vec<String> {
    violations.iter().map(Violation::name).collect()
}

/// C-01: check-then-act double spend is found quickly and shrunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c01_check_then_act_double_spend() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let report = env.run(&env.withdraw_yaml()).await;
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    assert!(
        report.elapsed < budget(Duration::from_secs(1)),
        "{:?}",
        report.elapsed
    );
    let f = &report.failures[0];
    assert_eq!(
        names(&f.violations),
        ["no_negative_balance", "at_most_one_successful_withdrawal"]
    );
    assert!(matches!(
        &f.violations[0],
        Violation::InvariantFailed { observed, .. } if observed == "id=1, balance=-100"
    ));
    assert_eq!(f.schedule.preemptions(), 1, "minimal trace");
    assert_eq!(f.record.success_count(), 2);
    let advice = advise(&f.record, &f.violations, &["req_a".into(), "req_b".into()]);
    assert_eq!(advice[0].pattern, Pattern::CheckThenAct);
    env.finish().await;
}

/// C-02: a lost update leaves a plausible balance but pays out twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c02_lost_update_is_caught_by_response_invariant() {
    let Some(env) = Env::start(WalletMode::LostUpdate).await else {
        return;
    };
    let report = env.run(&env.withdraw_yaml()).await;
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    let f = &report.failures[0];
    assert_eq!(names(&f.violations), ["at_most_one_successful_withdrawal"]);
    let advice = advise(&f.record, &f.violations, &["req_a".into(), "req_b".into()]);
    assert_eq!(advice[0].pattern, Pattern::LostUpdate);
    env.finish().await;
}

/// C-03: `FOR NO KEY UPDATE` passes every schedule; lock waits are parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c03_row_lock_passes_with_parks() {
    let Some(env) = Env::start(WalletMode::RowLock).await else {
        return;
    };
    let report = env.run(&env.withdraw_yaml()).await;
    assert_eq!(report.outcome, RunOutcome::Pass, "{:?}", report.failures);
    assert!(report.stats.complete);
    assert!(report.stats.schedules >= 2);
    assert!(report.parks > 0, "lock waits must be parked, not timed out");
    env.finish().await;
}

/// C-04: an atomic guarded UPDATE passes every schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c04_atomic_guard_passes() {
    let Some(env) = Env::start(WalletMode::AtomicGuard).await else {
        return;
    };
    let report = env.run(&env.withdraw_yaml()).await;
    assert_eq!(report.outcome, RunOutcome::Pass, "{:?}", report.failures);
    assert!(report.stats.complete);
    env.finish().await;
}

/// C-05: opposite-order transfers deadlock deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c05_transfer_deadlock() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let url = env.wallet.url("/transfer");
    let yaml = env.yaml(
        &format!(
            r#"  - id: a_to_b
    http: {{ method: POST, url: "{url}", headers: {{ Content-Type: application/json }}, body: '{{"from": 1, "to": 2, "amount": 10}}' }}
  - id: b_to_a
    http: {{ method: POST, url: "{url}", headers: {{ Content-Type: application/json }}, body: '{{"from": 2, "to": 1, "amount": 10}}' }}"#
        ),
        r#"  sql:
    - name: money_is_conserved
      assert_true: "SELECT SUM(balance) = 200 FROM wallets"
  fail_on_deadlock: true"#,
    );
    let report = env.run(&yaml).await;
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    let f = &report.failures[0];
    let Some(Violation::Deadlock {
        cycle, statements, ..
    }) = f.violations.iter().find(|v| v.is_deadlock())
    else {
        panic!("expected a deadlock, got {:?}", f.violations);
    };
    assert_eq!(cycle.len(), 2);
    assert_eq!(statements.len(), 2);
    assert!(
        statements
            .iter()
            .all(|(_, sql)| sql.starts_with("UPDATE wallets"))
    );
    let advice = advise(
        &f.record,
        &f.violations,
        &["a_to_b".into(), "b_to_a".into()],
    );
    assert_eq!(advice[0].pattern, Pattern::LockOrderInversion);
    env.finish().await;
}

/// C-06: the C-01 repro file reproduces the failure on every replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c06_replay_fidelity() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let yaml = env.withdraw_yaml();
    let report = env.run(&yaml).await;
    let failure = &report.failures[0];
    let workers = vec!["req_a".to_string(), "req_b".to_string()];
    let repro = ReproFile::from_failure("canonical", "canonical.yaml", &yaml, &workers, failure);
    let repro = ReproFile::parse(&repro.to_json(), "repro").unwrap();

    let config = Config::parse_with_env(&repro.config_source, "repro", |_| None).unwrap();
    let session = Session::start(config, RunOptions::default()).await.unwrap();
    let mut slowest = Duration::ZERO;
    for i in 0..10 {
        let started = Instant::now();
        let episode = session.replay(repro.choices()).await.unwrap();
        slowest = slowest.max(started.elapsed());
        assert_eq!(
            names(&episode.violations),
            repro.violated_invariants,
            "replay {i}"
        );
        assert_eq!(
            episode.schedule.choices(),
            failure.schedule.choices(),
            "replay {i}"
        );
        let sql: Vec<_> = episode
            .record
            .steps
            .iter()
            .map(|s| (&s.sql, &s.action))
            .collect();
        let expected: Vec<_> = failure
            .record
            .steps
            .iter()
            .map(|s| (&s.sql, &s.action))
            .collect();
        assert_eq!(sql, expected, "replay {i} took a different path");
    }
    session.close().await;
    assert!(
        slowest < budget(Duration::from_millis(250)),
        "slowest replay {slowest:?}"
    );

    env.wallet.set_mode(WalletMode::RowLock);
    env.wallet.clear_pool().await;
    let session = Session::start(Env::config(&repro.config_source), RunOptions::default())
        .await
        .unwrap();
    let fixed = session.replay(repro.choices()).await.unwrap();
    session.close().await;
    assert!(fixed.violations.is_empty(), "{:?}", fixed.violations);
    env.finish().await;
}

/// The binary end to end: exit 1, a repro file, and `pgloom replay` exit 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_run_and_replay_exit_codes() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("pgloom.yaml"), env.withdraw_yaml()).unwrap();
    let cwd = dir.path().to_owned();
    let run = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("pgloom")
            .unwrap()
            .current_dir(&cwd)
            .args(["run", "--color", "never"])
            .env_remove("PGLOOM_UPSTREAM_URL")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert_eq!(
        run.status.code(),
        Some(1),
        "{stdout}\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(stdout.contains("✗ RACE CONDITION DETECTED"), "{stdout}");
    assert!(
        stdout.contains("pgloom replay .pgloom-repro.json"),
        "{stdout}"
    );
    assert!(dir.path().join(".pgloom-repro.json").exists());
    assert!(
        stdout.trim_end().ends_with(pgloom_report::AUDIT_FOOTER),
        "{stdout}"
    );
    env.wallet.clear_pool().await;

    let cwd = dir.path().to_owned();
    let quiet = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("pgloom")
            .unwrap()
            .current_dir(&cwd)
            .args(["run", "--color", "never", "--quiet"])
            .env_remove("PGLOOM_UPSTREAM_URL")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let stdout = String::from_utf8_lossy(&quiet.stdout);
    assert_eq!(quiet.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains("✗ RACE CONDITION DETECTED"), "{stdout}");
    assert!(!stdout.contains(pgloom_report::AUDIT_FOOTER), "{stdout}");
    env.wallet.clear_pool().await;

    let cwd = dir.path().to_owned();
    let replay = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("pgloom")
            .unwrap()
            .current_dir(&cwd)
            .args(["replay", "--color", "never"])
            .env_remove("PGLOOM_UPSTREAM_URL")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let stdout = String::from_utf8_lossy(&replay.stdout);
    assert_eq!(
        replay.status.code(),
        Some(1),
        "{stdout}\n{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert!(stdout.contains("✗ REPRODUCED"), "{stdout}");

    env.wallet.set_mode(WalletMode::AtomicGuard);
    env.wallet.clear_pool().await;
    let cwd = dir.path().to_owned();
    let json = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("pgloom")
            .unwrap()
            .current_dir(&cwd)
            .args(["run", "--format", "json", "--quiet"])
            .env_remove("PGLOOM_UPSTREAM_URL")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(json.status.code(), Some(0));
    let v: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(v["result"], "pass");
    env.finish().await;
}

/// Every distinct combination of violations a run found.
fn verdicts(report: &RunReport) -> BTreeSet<Vec<String>> {
    report
        .failures
        .iter()
        .map(|f| names(&f.violations))
        .collect()
}

fn exhaustive(yaml: &str, reduction: &str) -> String {
    yaml.replace("strategy: bounded", "strategy: exhaustive")
        .replace("max_schedules: 20", "max_schedules: 500")
        .replace(
            "  max_preemptions: 2\n",
            &format!("  max_preemptions: 2\n  reduction: {reduction}\n"),
        )
}

const FIND_ALL: RunOptions = RunOptions {
    stop_on_first_failure: false,
    shrink: false,
};

/// Partial-order reduction keeps every outcome of the full exploration of the
/// wallet service, for the vulnerable and the fixed handlers alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dpor_preserves_every_outcome() {
    for mode in [
        WalletMode::CheckThenAct,
        WalletMode::LostUpdate,
        WalletMode::RowLock,
        WalletMode::AtomicGuard,
    ] {
        let Some(env) = Env::start(mode).await else {
            return;
        };
        let yaml = env.withdraw_yaml();
        let full = env.run_with(&exhaustive(&yaml, "none"), FIND_ALL).await;
        let reduced = env.run_with(&exhaustive(&yaml, "dpor"), FIND_ALL).await;
        assert!(full.stats.complete && reduced.stats.complete, "{mode:?}");
        assert!(reduced.stats.reduced && !full.stats.reduced);
        assert_eq!(reduced.outcome, full.outcome, "{mode:?}");
        assert_eq!(verdicts(&reduced), verdicts(&full), "{mode:?}");
        assert!(
            reduced.stats.schedules <= full.stats.schedules,
            "{mode:?}: {} > {}",
            reduced.stats.schedules,
            full.stats.schedules
        );
        env.finish().await;
    }
}

/// Response-body invariants see what each handler reported: a negative
/// balance in the check-then-act race, and two withdrawals both claiming the
/// same new balance in the lost-update race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_invariants_read_response_bodies() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let url = env.wallet.url("/withdraw");
    let body = r#"'{"account_id": 1, "amount": 100}'"#;
    let yaml = env.yaml(
        &format!(
            "  - id: req_a\n    http: {{ method: POST, url: \"{url}\", headers: {{ Content-Type: application/json }}, body: {body} }}\n\
             \x20 - id: req_b\n    http: {{ method: POST, url: \"{url}\", headers: {{ Content-Type: application/json }}, body: {body} }}"
        ),
        r#"  responses:
    - name: reported_balance_never_negative
      json:
        - { pointer: /status, equals: withdrawn }
        - { pointer: /balance, min: 0 }
    - name: each_withdrawal_sees_a_new_balance
      distinct: /balance"#,
    );
    let failure = |report: &RunReport, name: &str| -> Option<String> {
        report
            .failures
            .iter()
            .flat_map(|f| &f.violations)
            .find_map(|v| match v {
                Violation::InvariantFailed {
                    name: n,
                    description,
                    ..
                } if n == name => Some(description.clone()),
                _ => None,
            })
    };

    let report = env.run_with(&yaml, FIND_ALL).await;
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    let negative = failure(&report, "reported_balance_never_negative").expect("negative balance");
    assert!(
        negative.ends_with("/balance = -100, expected >= 0"),
        "{negative}"
    );

    env.wallet.set_mode(WalletMode::LostUpdate);
    let report = env.run_with(&yaml, FIND_ALL).await;
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    assert_eq!(
        failure(&report, "each_withdrawal_sees_a_new_balance").as_deref(),
        Some("/balance must differ between responses, got req_a=0, req_b=0")
    );
    assert!(failure(&report, "reported_balance_never_negative").is_none());

    env.wallet.set_mode(WalletMode::AtomicGuard);
    let report = env.run_with(&yaml, FIND_ALL).await;
    assert_eq!(report.outcome, RunOutcome::Pass, "{:?}", report.failures);
    env.finish().await;
}

/// Workers on unrelated tables commute: one schedule covers them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dpor_collapses_independent_workers() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let yaml = env
        .yaml(
            r#"  - id: credit
    sql:
      - "BEGIN"
      - "UPDATE wallets SET balance = balance + 1 WHERE id = 1"
      - "UPDATE wallets SET balance = balance + 1 WHERE id = 1"
      - "COMMIT"
  - id: audit
    sql:
      - "BEGIN"
      - "INSERT INTO audit_log (note) VALUES ('a')"
      - "INSERT INTO audit_log (note) VALUES ('b')"
      - "COMMIT""#,
            r#"  sql:
    - name: credited_twice
      assert_true: "SELECT balance = 102 FROM wallets WHERE id = 1""#,
        )
        .replace(
            "  setup_sql: |\n",
            "  setup_sql: |\n    CREATE TABLE IF NOT EXISTS audit_log (note TEXT);\n    TRUNCATE audit_log;\n",
        );
    let full = env.run_with(&exhaustive(&yaml, "none"), FIND_ALL).await;
    let reduced = env.run_with(&exhaustive(&yaml, "dpor"), FIND_ALL).await;
    assert_eq!(full.outcome, RunOutcome::Pass, "{:?}", full.failures);
    assert_eq!(reduced.outcome, RunOutcome::Pass, "{:?}", reduced.failures);
    assert!(reduced.stats.complete);
    // Both transactions start with a snapshot-taking statement, which is
    // conservatively dependent on the other's commit; everything else
    // commutes.
    assert!(
        reduced.stats.schedules * 3 <= full.stats.schedules,
        "{} vs {}",
        reduced.stats.schedules,
        full.stats.schedules
    );
    env.finish().await;
}

/// A service that talks to PostgreSQL directly, bypassing the proxy, must
/// not be reported as race-free: nothing was explored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bypassing_the_proxy_is_an_error_not_a_pass() {
    let Some(env) = Env::start(WalletMode::CheckThenAct).await else {
        return;
    };
    let direct = WalletServer::start(
        "127.0.0.1:0".parse().unwrap(),
        env.db.url(),
        "wallets",
        WalletMode::CheckThenAct,
    )
    .await
    .unwrap();
    let yaml = env
        .withdraw_yaml()
        .replace(&env.wallet.url("/withdraw"), &direct.url("/withdraw"));
    let session = Session::start(Env::config(&yaml), RunOptions::default())
        .await
        .unwrap();
    let result = session.run().await;
    session.close().await;
    direct.stop().await;
    let err = result.expect_err("a run with no proxied SQL must fail");
    assert!(
        err.to_string().contains("no SQL from the workers reached"),
        "{err}"
    );
    assert_eq!(err.exit_code(), pgloom_cli::error::EXIT_CONFIG);
    env.finish().await;
}

/// Under `fail_on_deadlock: false`, PostgreSQL breaks the lock cycle with
/// genuine SQLSTATE `40P01`, letting a handler that catches `40P01` roll back
/// and retry to completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fail_on_deadlock_false_lets_postgres_raise_40p01_and_handler_retry() {
    use axum::Router;
    use axum::extract::{Json, State};
    use axum::http::StatusCode;
    use axum::routing::post;
    use serde::Deserialize;
    use std::sync::Arc;
    use tokio_postgres::NoTls;

    #[derive(Deserialize)]
    struct TransferReq {
        from: i32,
        to: i32,
        amount: i64,
    }

    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let proxy = free_port().await.unwrap();
    let proxy_url = PgUrl::parse(db.url())
        .unwrap()
        .with_host_port("127.0.0.1", proxy.port())
        .with_ssl_disabled()
        .to_string();

    let state = Arc::new(proxy_url);
    let app = Router::new()
        .route(
            "/transfer",
            post(
                |State(url): State<Arc<String>>, Json(req): Json<TransferReq>| async move {
                    for _attempt in 0..5 {
                        let Ok((mut client, conn)) = tokio_postgres::connect(&url, NoTls).await
                        else {
                            return StatusCode::SERVICE_UNAVAILABLE;
                        };
                        let conn_task = tokio::spawn(async move {
                            let _ = conn.await;
                        });
                        let res = async {
                            let tx = client.transaction().await?;
                            tx.execute(
                                "UPDATE wallets SET balance = balance - $1 WHERE id = $2",
                                &[&req.amount, &req.from],
                            )
                            .await?;
                            tx.execute(
                                "UPDATE wallets SET balance = balance + $1 WHERE id = $2",
                                &[&req.amount, &req.to],
                            )
                            .await?;
                            tx.commit().await
                        }
                        .await;
                        drop(client);
                        conn_task.abort();
                        match res {
                            Ok(()) => return StatusCode::OK,
                            Err(e) if e.code().map(|c| c.code()) == Some("40P01") => continue,
                            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
                        }
                    }
                    StatusCode::CONFLICT
                },
            ),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let svc_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let yaml = format!(
        r#"
version: 1
name: retry-on-deadlock
database:
  upstream: "{upstream}"
  proxy_listen: "{proxy}"
  setup_sql: |
    CREATE TABLE IF NOT EXISTS wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
    TRUNCATE TABLE wallets;
    INSERT INTO wallets (id, balance) VALUES (1, 100), (2, 100);
exploration:
  strategy: bounded
  max_preemptions: 2
  max_schedules: 12
  step_timeout_ms: 5000
workers:
  - id: a_to_b
    http: {{ method: POST, url: "http://{svc_addr}/transfer", headers: {{ Content-Type: application/json }}, body: '{{"from": 1, "to": 2, "amount": 10}}' }}
  - id: b_to_a
    http: {{ method: POST, url: "http://{svc_addr}/transfer", headers: {{ Content-Type: application/json }}, body: '{{"from": 2, "to": 1, "amount": 10}}' }}
invariants:
  sql:
    - name: money_is_conserved
      assert_true: "SELECT SUM(balance) = 200 FROM wallets"
  responses:
    - name: both_transfers_succeed
      min_2xx_count: 2
  fail_on_deadlock: false
"#,
        upstream = db.url(),
    );

    let session = Session::start(Env::config(&yaml), RunOptions::default())
        .await
        .unwrap();
    let report = session.run().await.unwrap();
    session.close().await;
    server.abort();
    db.cleanup().await;

    assert_eq!(report.outcome, RunOutcome::Pass, "{:?}", report.failures);
    assert!(report.parks > 0);
}

/// DPOR must not treat a STABLE user-defined SQL function as pure: it reads
/// tables hidden inside its body, so concurrent writes to those tables must
/// not be pruned away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dpor_catches_race_through_stable_sql_function() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let proxy = free_port().await.unwrap();
    let yaml = format!(
        r#"
version: 1
name: stable-function-dpor
database:
  upstream: "{upstream}"
  proxy_listen: "{proxy}"
  setup_sql: |
    CREATE TABLE IF NOT EXISTS wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
    CREATE TABLE IF NOT EXISTS credit_limits (id INT PRIMARY KEY, cap BIGINT NOT NULL);
    TRUNCATE TABLE wallets, credit_limits;
    INSERT INTO wallets VALUES (1, 100);
    INSERT INTO credit_limits VALUES (1, 100);
    CREATE OR REPLACE FUNCTION active_limit() RETURNS BIGINT
      LANGUAGE sql STABLE AS $$ SELECT cap FROM credit_limits WHERE id = 1 $$;
exploration:
  strategy: exhaustive
  reduction: dpor
  max_schedules: 50
workers:
  - id: reader
    sql:
      - "UPDATE wallets SET balance = active_limit() WHERE id = 1"
  - id: lower_cap
    sql:
      - "UPDATE credit_limits SET cap = 0 WHERE id = 1"
invariants:
  sql:
    - name: balance_stays_at_initial_cap
      assert_true: "SELECT balance = 100 FROM wallets WHERE id = 1"
"#,
        upstream = db.url(),
    );
    let session = Session::start(Env::config(&yaml), RunOptions::default())
        .await
        .unwrap();
    let report = session.run().await.unwrap();
    session.close().await;
    db.cleanup().await;

    assert!(report.stats.reduced);
    assert_eq!(
        report.outcome,
        RunOutcome::ViolationFound,
        "DPOR must explore the interleaving where credit_limits changes before active_limit() runs"
    );
}

/// Every concrete SQL statement proposed by the Fix Advisor must parse and
/// plan cleanly in real PostgreSQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix_advisor_proposed_sql_is_valid_postgres_syntax() {
    use pgloom_engine::{DeadlockSource, EpisodeRecord, RecordedStep, StepAction, WorkerId};
    use pgloom_pgctrl::PgControl;

    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE wallets (
             id INT PRIMARY KEY,
             ext_id INT,
             balance BIGINT NOT NULL,
             status TEXT,
             updated_at TIMESTAMPTZ
         );
         CREATE TABLE holds (
             id INT PRIMARY KEY,
             wallet_id INT REFERENCES wallets(id)
         );
         CREATE TABLE accounts (
             id INT PRIMARY KEY,
             balance BIGINT NOT NULL
         );",
        "advisor schema",
    )
    .await
    .unwrap();

    let mk_record = |steps: &[(u32, &str)]| {
        let mut r = EpisodeRecord::default();
        for (idx, &(w, sql)) in steps.iter().enumerate() {
            let mut step = RecordedStep::new(idx as u32, WorkerId(w), StepAction::Execute);
            step.sql = sql.to_owned();
            r.steps.push(step);
        }
        r
    };
    let workers = vec!["req_a".to_owned(), "req_b".to_owned()];

    let cases: Vec<(EpisodeRecord, Vec<Violation>)> = vec![
        // CheckThenAct with OR predicate
        (
            mk_record(&[
                (0, "SELECT balance FROM wallets WHERE id = $1"),
                (
                    0,
                    "UPDATE wallets SET balance = balance - $1 WHERE id = $2 OR ext_id = $3",
                ),
            ]),
            vec![],
        ),
        // CheckThenAct with multi-term subtraction and RETURNING
        (
            mk_record(&[
                (0, "SELECT balance FROM wallets WHERE id = $1"),
                (
                    0,
                    "UPDATE wallets SET balance = balance - $1 - $2 WHERE id = $3 RETURNING balance",
                ),
            ]),
            vec![],
        ),
        // CheckThenAct with LEFT JOIN
        (
            mk_record(&[
                (
                    0,
                    "SELECT w.balance FROM wallets w LEFT JOIN holds h ON h.wallet_id = w.id WHERE w.id = $1",
                ),
                (0, "UPDATE wallets SET balance = balance - $1 WHERE id = $2"),
            ]),
            vec![],
        ),
        // CheckThenAct with FOR SHARE
        (
            mk_record(&[
                (0, "SELECT balance FROM wallets WHERE id = $1 FOR SHARE"),
                (0, "UPDATE wallets SET balance = balance - $1 WHERE id = $2"),
            ]),
            vec![],
        ),
        // LostUpdate with multi-column SET
        (
            mk_record(&[
                (0, "SELECT balance FROM wallets WHERE id = $1"),
                (
                    0,
                    "UPDATE wallets SET balance = $1, status = $2, updated_at = $3 WHERE id = $4",
                ),
            ]),
            vec![],
        ),
        // PhantomInsert without ON CONFLICT
        (
            mk_record(&[
                (0, "SELECT count(*) FROM holds WHERE id = $1"),
                (0, "INSERT INTO holds (id, wallet_id) VALUES ($1, $2)"),
            ]),
            vec![],
        ),
        // PhantomInsert with existing ON CONFLICT
        (
            mk_record(&[
                (0, "SELECT count(*) FROM holds WHERE id = $1"),
                (
                    0,
                    "INSERT INTO holds (id, wallet_id) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET wallet_id = EXCLUDED.wallet_id",
                ),
            ]),
            vec![],
        ),
        // CheckThenAct with window function (must emit separate pre-lock, not FOR UPDATE on OVER)
        (
            mk_record(&[
                (
                    0,
                    "SELECT balance, row_number() OVER () FROM wallets WHERE id = $1",
                ),
                (0, "UPDATE wallets SET balance = balance - $1 WHERE id = $2"),
            ]),
            vec![],
        ),
        // LockOrderInversion
        (
            EpisodeRecord::default(),
            vec![Violation::Deadlock {
                cycle: vec![WorkerId(0), WorkerId(1)],
                source: DeadlockSource::WaitForGraph,
                statements: vec![
                    (
                        WorkerId(0),
                        "UPDATE accounts SET balance = balance - 1 WHERE id = 2".into(),
                    ),
                    (
                        WorkerId(1),
                        "UPDATE accounts SET balance = balance - 1 WHERE id = 1".into(),
                    ),
                ],
            }],
        ),
    ];

    let mut checked = 0usize;
    for (rec, viols) in &cases {
        let advice = advise(rec, viols, &workers);
        assert!(!advice.is_empty());
        for a in &advice {
            for fix in &a.fixes {
                for raw_sql in &fix.sql {
                    let delta_replacement = if raw_sql.contains("$2") && !raw_sql.contains("$1") {
                        "$1"
                    } else {
                        "1"
                    };
                    let sql = raw_sql
                        .replace("<delta>", delta_replacement)
                        .replace("<value you read>", "100")
                        .replace("<natural key columns>", "id")
                        .replace("<natural key>", "'key'")
                        .replace("<table>", "accounts");
                    if sql.starts_with("--") || sql.contains("...") {
                        continue;
                    }
                    let upper = sql.to_ascii_uppercase();
                    if upper.starts_with("CREATE UNIQUE INDEX CONCURRENTLY") {
                        // CONCURRENTLY cannot run inside a transaction block.
                        ctl.batch(&sql, "validating advisor CONCURRENTLY index SQL")
                            .await
                            .unwrap_or_else(|e| panic!("invalid advisor SQL `{sql}`: {e}"));
                    } else if upper.starts_with("CREATE ") || upper.starts_with("BEGIN") {
                        ctl.batch(
                            &format!("BEGIN; {sql} ROLLBACK;"),
                            "validating advisor DDL/TX SQL",
                        )
                        .await
                        .unwrap_or_else(|e| panic!("invalid advisor SQL `{sql}`: {e}"));
                    } else {
                        let stmt = format!("adv_{checked}");
                        let clean = sql.trim_end_matches(';');
                        ctl.batch(
                            &format!("PREPARE {stmt} AS {clean}; DEALLOCATE {stmt};"),
                            "validating advisor DML SQL",
                        )
                        .await
                        .unwrap_or_else(|e| panic!("invalid advisor SQL `{sql}`: {e}"));
                    }
                    checked += 1;
                }
            }
        }
    }
    assert!(
        checked >= 15,
        "expected to validate >= 15 SQL patches, checked {checked}"
    );
    drop(ctl);
    db.cleanup().await;
}

/// `target.reset_http` is invoked after every schedule so services with
/// in-process caches can clear state alongside the database snapshot restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_reset_http_is_invoked_between_schedules() {
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let proxy = free_port().await.unwrap();
    let resets = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/reset",
            post(|State(n): State<Arc<AtomicUsize>>| async move {
                n.fetch_add(1, Ordering::SeqCst);
                StatusCode::NO_CONTENT
            }),
        )
        .with_state(Arc::clone(&resets));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let svc_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let yaml = format!(
        r#"
version: 1
name: reset-http-hook
database:
  upstream: "{upstream}"
  proxy_listen: "{proxy}"
  setup_sql: |
    CREATE TABLE IF NOT EXISTS wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
    TRUNCATE TABLE wallets;
    INSERT INTO wallets VALUES (1, 100);
target:
  reset_http: "http://{svc_addr}/reset"
exploration:
  strategy: exhaustive
  reduction: none
  max_schedules: 4
workers:
  - id: a
    sql: ["UPDATE wallets SET balance = balance + 1 WHERE id = 1"]
  - id: b
    sql: ["UPDATE wallets SET balance = balance + 1 WHERE id = 1"]
invariants:
  sql:
    - name: ok
      assert_true: "SELECT balance = 102 FROM wallets WHERE id = 1"
"#,
        upstream = db.url(),
    );

    let session = Session::start(Env::config(&yaml), RunOptions::default())
        .await
        .unwrap();
    let report = session.run().await.unwrap();
    session.close().await;
    server.abort();
    db.cleanup().await;

    assert_eq!(report.outcome, RunOutcome::Pass);
    assert!(
        resets.load(Ordering::SeqCst) >= report.stats.schedules,
        "expected >= {} reset_http calls, got {}",
        report.stats.schedules,
        resets.load(Ordering::SeqCst)
    );
}

/// `pgloom doctor` passes the snapshot-schema check when `reset_sql` is
/// configured even if the database role lacks `CREATE` on the database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn doctor_passes_snapshot_schema_when_reset_sql_set_for_unprivileged_role() {
    use pgloom_pgctrl::PgControl;

    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    let role = format!("pgloom_nocreate_{}", std::process::id());
    ctl.batch(
        &format!(
            "DROP ROLE IF EXISTS {role};
             CREATE ROLE {role} LOGIN PASSWORD 'pw';
             REVOKE CREATE ON DATABASE \"{}\" FROM PUBLIC;
             GRANT CONNECT ON DATABASE \"{}\" TO {role};",
            db.name(),
            db.name()
        ),
        "create restricted role",
    )
    .await
    .unwrap();
    let role_url = db
        .url()
        .replace("postgres:postgres@", &format!("{role}:pw@"));

    let dir = tempfile::tempdir().unwrap();
    let cfg_path = dir.path().join("pgloom.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            r#"version: 1
name: doctor-reset-sql
database:
  upstream: "{role_url}"
  proxy_listen: "127.0.0.1:0"
  reset_sql: "SELECT 1;"
workers:
  - id: a
    sql: ["SELECT 1"]
  - id: b
    sql: ["SELECT 1"]
"#
        ),
    )
    .unwrap();

    let cwd = dir.path().to_owned();
    let out = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("pgloom")
            .unwrap()
            .current_dir(&cwd)
            .args(["doctor", "--color", "never"])
            .env_remove("PGLOOM_UPSTREAM_URL")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let _ = ctl
        .batch(&format!("DROP ROLE IF EXISTS {role};"), "drop role")
        .await;
    drop(ctl);
    db.cleanup().await;

    assert_eq!(
        out.status.code(),
        Some(0),
        "doctor should pass when reset_sql is set:\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("not needed (database.reset_sql is set)"),
        "{stdout}"
    );
}
