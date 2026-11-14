//! Integration tests against a real PostgreSQL server.

use std::collections::BTreeSet;
use std::time::Duration;

use pgloom_pgctrl::testing::TestDatabase;
use pgloom_pgctrl::{
    CatalogLink, LinkKind, PgControl, RelationKind, Snapshot, SnapshotOptions, SqlInvariant,
};
use tokio_postgres::NoTls;

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

async fn session(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(conn);
    client
}

const SCHEMA: &str = "
    CREATE TABLE wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
    CREATE TABLE transfers (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        wallet_id INT NOT NULL REFERENCES wallets(id),
        amount BIGINT NOT NULL,
        doubled BIGINT GENERATED ALWAYS AS (amount * 2) STORED
    );
    CREATE TABLE \"Audit Log\" (id SERIAL PRIMARY KEY, note TEXT);
    CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY);
    INSERT INTO wallets VALUES (1, 100), (2, 50);
    INSERT INTO transfers (wallet_id, amount) VALUES (1, 10), (2, 20);
    INSERT INTO \"Audit Log\" (note) VALUES ('created');
    INSERT INTO _sqlx_migrations VALUES (1);
";

#[tokio::test]
async fn snapshot_restore_roundtrip_preserves_data_identity_and_sequences() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(SCHEMA, "schema").await.unwrap();

    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();
    let names: BTreeSet<String> = snap.tables().map(|t| t.qualified()).collect();
    assert_eq!(
        names,
        ["public.\"Audit Log\"", "public.transfers", "public.wallets"]
            .map(String::from)
            .into()
    );

    let app = session(db.url()).await;
    app.batch_execute(
        "UPDATE wallets SET balance = -100 WHERE id = 1;
         INSERT INTO transfers (wallet_id, amount) VALUES (1, 99);
         INSERT INTO \"Audit Log\" (note) VALUES ('mutated');
         INSERT INTO _sqlx_migrations VALUES (2);",
    )
    .await
    .unwrap();

    // Only wallets is reported dirty; transfers references it and must follow.
    let dirty: BTreeSet<String> = ["wallets".to_string()].into();
    let started = std::time::Instant::now();
    let stats = snap.restore(&ctl, &dirty, false).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(stats.tables, vec!["public.wallets", "public.transfers"]);
    assert!(
        elapsed < budget(Duration::from_millis(250)),
        "restore took {elapsed:?}"
    );

    let balance: i64 = app
        .query_one("SELECT balance FROM wallets WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(balance, 100);
    let count: i64 = app
        .query_one("SELECT count(*) FROM transfers", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
    // Identity sequence rewound: the next id is 3 again.
    let next: i64 = app
        .query_one(
            "INSERT INTO transfers (wallet_id, amount) VALUES (2, 1) RETURNING id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(next, 3);
    let doubled: i64 = app
        .query_one("SELECT doubled FROM transfers WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(doubled, 20);
    // Not dirty, not restored.
    let notes: i64 = app
        .query_one("SELECT count(*) FROM \"Audit Log\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(notes, 2);
    // Migration tables are never touched.
    let migrations: i64 = app
        .query_one("SELECT count(*) FROM _sqlx_migrations", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(migrations, 2);

    snap.restore_all(&ctl).await.unwrap();
    let notes: i64 = app
        .query_one("SELECT count(*) FROM \"Audit Log\"", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(notes, 1);

    Snapshot::discard(&ctl).await.unwrap();
    drop(app);
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn blocking_pids_cancel_and_terminate() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(SCHEMA, "schema").await.unwrap();

    let holder = session(db.url()).await;
    let waiter = session(db.url()).await;
    let holder_pid: i32 = holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let waiter_pid: i32 = waiter
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);

    holder
        .batch_execute("BEGIN; UPDATE wallets SET balance = 0 WHERE id = 1")
        .await
        .unwrap();
    waiter.batch_execute("BEGIN").await.unwrap();
    let blocked = tokio::spawn(async move {
        let r = waiter
            .batch_execute("UPDATE wallets SET balance = balance - 1 WHERE id = 1")
            .await;
        (waiter, r)
    });

    let mut info = None;
    for _ in 0..200 {
        let map = ctl.blocking_pids(&[holder_pid, waiter_pid]).await.unwrap();
        // The waiter enters the lock queue before it reports the wait event.
        if let Some(w) = map
            .get(&waiter_pid)
            .filter(|w| !w.blocked_by.is_empty() && w.wait_event_type.as_deref() == Some("Lock"))
        {
            info = Some(w.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let info = info.expect("waiter never showed up as blocked");
    assert_eq!(info.blocked_by, vec![holder_pid]);
    assert_eq!(info.wait_event_type.as_deref(), Some("Lock"));
    assert!(
        ctl.blocking_pids(&[holder_pid]).await.unwrap()[&holder_pid]
            .blocked_by
            .is_empty()
    );

    assert!(ctl.cancel_backend(waiter_pid).await.unwrap());
    let (waiter, result) = blocked.await.unwrap();
    let err = result.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("57014"));

    let open = ctl
        .backends_in_transaction(&[holder_pid, waiter_pid])
        .await
        .unwrap();
    assert_eq!(open, {
        let mut v = vec![holder_pid, waiter_pid];
        v.sort_unstable();
        v
    });
    let terminated = ctl
        .terminate_open_transactions(&[holder_pid, waiter_pid])
        .await
        .unwrap();
    assert_eq!(terminated.len(), 2);
    assert!(holder.simple_query("SELECT 1").await.is_err());
    assert!(waiter.simple_query("SELECT 1").await.is_err());
    // The aborted UPDATE never committed.
    let balance: i64 = ctl
        .client()
        .query_one("SELECT balance FROM wallets WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(balance, 100);

    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn sql_invariants_pass_fail_and_diagnose() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(SCHEMA, "schema").await.unwrap();
    let inv = SqlInvariant {
        name: "no_negative_balance".into(),
        description: Some("balances stay non-negative".into()),
        assert_true: "SELECT COALESCE(MIN(balance), 0) >= 0 FROM wallets".into(),
        diagnostic_query: Some(
            "SELECT id, balance FROM wallets WHERE balance < 0 ORDER BY id".into(),
        ),
    };
    assert!(ctl.check_invariant(&inv).await.unwrap().passed);
    ctl.batch("UPDATE wallets SET balance = -100 WHERE id = 1", "mutate")
        .await
        .unwrap();
    let r = ctl.check_invariant(&inv).await.unwrap();
    assert!(!r.passed);
    assert_eq!(r.observed, "id=1, balance=-100");

    let empty = SqlInvariant {
        name: "no_rows".into(),
        description: None,
        assert_true: "SELECT false FROM wallets WHERE id = 999".into(),
        diagnostic_query: None,
    };
    assert!(ctl.check_invariant(&empty).await.unwrap().passed);

    let broken = SqlInvariant {
        name: "broken".into(),
        description: None,
        assert_true: "SELECT nope FROM nowhere".into(),
        diagnostic_query: None,
    };
    assert_eq!(
        ctl.check_invariant(&broken).await.unwrap_err().sqlstate(),
        Some("42P01")
    );
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn schema_catalog_reads_kinds_links_policies_and_functions() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
         CREATE TABLE ledger (id SERIAL PRIMARY KEY, wallet_id INT REFERENCES wallets(id));
         CREATE TABLE events (id INT, at DATE) PARTITION BY RANGE (at);
         CREATE TABLE events_2026 PARTITION OF events
             FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
         CREATE TABLE accounts (id INT PRIMARY KEY, org_id INT);
         ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
         CREATE VIEW rich AS SELECT * FROM wallets WHERE balance > 1000;
         CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS
             'BEGIN RETURN NEW; END';
         CREATE TRIGGER audit BEFORE UPDATE ON wallets
             FOR EACH ROW EXECUTE FUNCTION touch();
         CREATE FUNCTION Pay(INT) RETURNS INT LANGUAGE sql AS 'SELECT $1';",
        "schema",
    )
    .await
    .unwrap();
    let catalog = ctl.schema_catalog().await.unwrap();
    let rel = |name: &str| {
        catalog
            .relations
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("{name} missing from {:?}", catalog.relations))
    };
    let oid = |name: &str| rel(name).oid;

    assert_eq!(rel("wallets").kind, RelationKind::Table);
    assert_eq!(rel("events").kind, RelationKind::Table);
    assert_eq!(rel("rich").kind, RelationKind::View);
    assert_eq!(rel("ledger_id_seq").kind, RelationKind::Sequence);
    assert!(rel("wallets").side_effects, "user trigger");
    assert!(!rel("ledger").side_effects);
    assert!(rel("accounts").row_security);
    assert!(!rel("wallets").row_security);

    let has = |kind, from: &str, to: &str| {
        catalog.links.contains(&CatalogLink {
            kind,
            from: oid(from),
            to: oid(to),
        })
    };
    assert!(has(LinkKind::ForeignKey, "ledger", "wallets"));
    assert!(has(LinkKind::Inherits, "events_2026", "events"));
    assert!(has(LinkKind::Sequence, "ledger", "ledger_id_seq"));
    assert!(catalog.volatile_functions.contains(&"pay".to_owned()));
    assert!(catalog.volatile_functions.contains(&"touch".to_owned()));
    db.cleanup().await;
}
