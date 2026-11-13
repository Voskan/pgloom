use pgloom_driver::{Config, ScenarioWorkload};
use pgloom_engine::WorkerId;
use pgloom_pgctrl::PgControl;
use pgloom_pgctrl::testing::TestDatabase;

#[tokio::test]
async fn sql_worker_runs_statements_and_rolls_back_on_error() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
         INSERT INTO wallets VALUES (1, 100);",
        "setup",
    )
    .await
    .unwrap();
    let src = r#"
version: 1
name: sql-driver
database:
  upstream: postgres://unused@127.0.0.1/x
workers:
  - id: ok
    sql:
      - BEGIN
      - UPDATE wallets SET balance = balance - 30 WHERE id = 1
      - COMMIT
  - id: fails
    sql:
      - BEGIN
      - UPDATE wallets SET balance = balance - 30 WHERE id = 1
      - INSERT INTO wallets VALUES (1, 0)
      - COMMIT
"#;
    let cfg = Config::parse_with_env(src, "t", |_| None).unwrap();
    let wl = ScenarioWorkload::new(&cfg.workers, db.url()).unwrap();

    let ok = wl.run(WorkerId(0)).await;
    assert!(ok.success, "{ok:?}");
    let failed = wl.run(WorkerId(1)).await;
    assert!(!failed.success);
    assert_eq!(failed.label, "SQLSTATE 23505");

    let rows = ctl
        .simple_rows("SELECT balance FROM wallets WHERE id = 1", "check")
        .await
        .unwrap();
    assert_eq!(rows[0][0].1.as_deref(), Some("70"));
    drop(ctl);
    db.cleanup().await;
}
