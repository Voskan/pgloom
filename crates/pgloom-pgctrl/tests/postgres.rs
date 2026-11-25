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

#[tokio::test]
async fn snapshot_restore_handles_triggers_for_superuser_and_table_owner() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
         CREATE TABLE audit_log (id SERIAL PRIMARY KEY, wallet_id INT NOT NULL);
         CREATE FUNCTION log_wallet_insert() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO audit_log (wallet_id) VALUES (NEW.id);
             RETURN NEW;
         END;
         $$;
         CREATE TRIGGER wallets_after_insert AFTER INSERT ON wallets
             FOR EACH ROW EXECUTE FUNCTION log_wallet_insert();
         INSERT INTO wallets VALUES (1, 100);",
        "schema",
    )
    .await
    .unwrap();

    // Superuser path (session_replication_role = replica).
    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();
    ctl.batch(
        "UPDATE wallets SET balance = -10 WHERE id = 1;
         INSERT INTO wallets VALUES (2, 50);",
        "mutate",
    )
    .await
    .unwrap();
    let dirty: BTreeSet<String> = ["wallets".into(), "audit_log".into()].into();
    snap.restore(&ctl, &dirty, false).await.unwrap();
    let audit_count: i64 = ctl
        .client()
        .query_one("SELECT count(*) FROM audit_log", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        audit_count, 1,
        "superuser restore must not re-fire triggers"
    );
    Snapshot::discard(&ctl).await.unwrap();

    // Non-superuser table owner path (ALTER TABLE ... DISABLE/ENABLE TRIGGER USER).
    let role = format!("pgloom_owner_{}", std::process::id());
    ctl.batch(
        &format!(
            "DROP ROLE IF EXISTS {role};
             CREATE ROLE {role} LOGIN PASSWORD 'pw';
             GRANT CREATE ON DATABASE \"{}\" TO {role};
             GRANT USAGE, CREATE ON SCHEMA public TO {role};
             ALTER TABLE wallets OWNER TO {role};
             ALTER TABLE audit_log OWNER TO {role};
             ALTER SEQUENCE audit_log_id_seq OWNER TO {role};",
            db.name()
        ),
        "create owner role",
    )
    .await
    .unwrap();
    let owner_url = db
        .url()
        .replace("postgres:postgres@", &format!("{role}:pw@"));
    let owner_ctl = PgControl::connect(&owner_url).await.unwrap();
    let owner_snap = Snapshot::capture(&owner_ctl, &SnapshotOptions::default())
        .await
        .unwrap();
    owner_ctl
        .batch(
            "UPDATE wallets SET balance = -50 WHERE id = 1;
             INSERT INTO wallets VALUES (3, 75);",
            "owner mutate",
        )
        .await
        .unwrap();
    owner_snap.restore(&owner_ctl, &dirty, false).await.unwrap();
    let audit_count: i64 = owner_ctl
        .client()
        .query_one("SELECT count(*) FROM audit_log", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        audit_count, 1,
        "non-superuser table owner restore must not re-fire user triggers"
    );
    Snapshot::discard(&owner_ctl).await.unwrap();
    drop(owner_ctl);
    let _ = ctl
        .batch(
            &format!(
                "REASSIGN OWNED BY {role} TO postgres;
                 DROP OWNED BY {role};
                 DROP ROLE IF EXISTS {role};"
            ),
            "cleanup role",
        )
        .await;
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn snapshot_restore_handles_circular_foreign_keys() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE a (id INT PRIMARY KEY, b_id INT);
         CREATE TABLE b (id INT PRIMARY KEY, a_id INT REFERENCES a(id) DEFERRABLE INITIALLY DEFERRED);
         ALTER TABLE a ADD CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES b(id) DEFERRABLE INITIALLY DEFERRED;
         BEGIN;
         INSERT INTO a VALUES (1, 1);
         INSERT INTO b VALUES (1, 1);
         COMMIT;",
        "circular fks",
    )
    .await
    .unwrap();
    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();
    ctl.batch("UPDATE a SET b_id = NULL WHERE id = 1;", "mutate")
        .await
        .unwrap();
    let dirty: BTreeSet<String> = ["a".into()].into();
    snap.restore(&ctl, &dirty, false).await.unwrap();
    let b_id: Option<i32> = ctl
        .client()
        .query_one("SELECT b_id FROM a WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(b_id, Some(1));
    Snapshot::discard(&ctl).await.unwrap();
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn snapshot_restore_handles_table_inheritance_without_duplicating_rows() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE parent_items (id INT, note TEXT);
         CREATE TABLE child_items (extra INT) INHERITS (parent_items);
         INSERT INTO parent_items VALUES (1, 'parent-only');
         INSERT INTO child_items VALUES (2, 'child-row', 99);",
        "inheritance",
    )
    .await
    .unwrap();
    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();
    ctl.batch(
        "INSERT INTO parent_items VALUES (3, 'extra');
         INSERT INTO child_items VALUES (4, 'extra-child', 100);",
        "mutate",
    )
    .await
    .unwrap();
    snap.restore_all(&ctl).await.unwrap();
    let only_parent: i64 = ctl
        .client()
        .query_one("SELECT count(*) FROM ONLY parent_items", &[])
        .await
        .unwrap()
        .get(0);
    let total_parent: i64 = ctl
        .client()
        .query_one("SELECT count(*) FROM parent_items", &[])
        .await
        .unwrap()
        .get(0);
    let only_child: i64 = ctl
        .client()
        .query_one("SELECT count(*) FROM child_items", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(only_parent, 1, "parent must not duplicate child rows");
    assert_eq!(only_child, 1);
    assert_eq!(total_parent, 2);
    Snapshot::discard(&ctl).await.unwrap();
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn snapshot_restore_handles_direct_child_partition_and_updatable_view_writes() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE TABLE wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
         INSERT INTO wallets VALUES (1, 100);
         CREATE VIEW active_wallets AS SELECT * FROM wallets WHERE balance > 0;
         CREATE TABLE events (id INT NOT NULL, at DATE NOT NULL, note TEXT) PARTITION BY RANGE (at);
         CREATE TABLE events_2026 PARTITION OF events
             FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
         INSERT INTO events VALUES (1, '2026-06-01', 'seed');",
        "partition and view",
    )
    .await
    .unwrap();
    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();

    // Direct write to child partition.
    ctl.batch(
        "UPDATE events_2026 SET note = 'mutated' WHERE id = 1;",
        "mutate child partition",
    )
    .await
    .unwrap();
    let dirty_part: BTreeSet<String> = ["events_2026".into()].into();
    snap.restore(&ctl, &dirty_part, false).await.unwrap();
    let note: String = ctl
        .client()
        .query_one("SELECT note FROM events WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(note, "seed");

    // Write through updatable view: fails closed to restoring all tables.
    ctl.batch(
        "UPDATE active_wallets SET balance = -5 WHERE id = 1;",
        "mutate view",
    )
    .await
    .unwrap();
    let dirty_view: BTreeSet<String> = ["active_wallets".into()].into();
    snap.restore(&ctl, &dirty_view, false).await.unwrap();
    let balance: i64 = ctl
        .client()
        .query_one("SELECT balance FROM wallets WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(balance, 100);

    Snapshot::discard(&ctl).await.unwrap();
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn snapshot_restore_rewinds_standalone_sequences() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE SEQUENCE invoice_seq START 10;
         CREATE SEQUENCE raw_seq START 100;
         CREATE TABLE invoices (id BIGINT PRIMARY KEY DEFAULT nextval('invoice_seq'), note TEXT);
         INSERT INTO invoices (note) VALUES ('seed');",
        "sequences",
    )
    .await
    .unwrap();
    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();

    // Advance table-default sequence via INSERT and standalone sequence directly.
    ctl.batch(
        "INSERT INTO invoices (note) VALUES ('second');
         SELECT nextval('raw_seq');
         SELECT nextval('raw_seq');",
        "advance sequences",
    )
    .await
    .unwrap();

    let dirty: BTreeSet<String> = ["invoices".into()].into();
    snap.restore(&ctl, &dirty, true).await.unwrap();

    let next_inv: i64 = ctl
        .client()
        .query_one(
            "INSERT INTO invoices (note) VALUES ('after-reset') RETURNING id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(next_inv, 11);
    let next_raw: i64 = ctl
        .client()
        .query_one("SELECT nextval('raw_seq')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(next_raw, 100);

    Snapshot::discard(&ctl).await.unwrap();
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn snapshot_restore_respects_non_public_search_path() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();
    ctl.batch(
        "CREATE SCHEMA tenant;
         CREATE TABLE public.wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
         CREATE TABLE tenant.wallets (id INT PRIMARY KEY, balance BIGINT NOT NULL);
         INSERT INTO public.wallets VALUES (1, 100);
         INSERT INTO tenant.wallets VALUES (1, 200);
         SET search_path = tenant, public;",
        "multi-schema",
    )
    .await
    .unwrap();
    let snap = Snapshot::capture(&ctl, &SnapshotOptions::default())
        .await
        .unwrap();

    ctl.batch(
        "UPDATE public.wallets SET balance = 999 WHERE id = 1;
         UPDATE tenant.wallets SET balance = -50 WHERE id = 1;",
        "mutate both schemas",
    )
    .await
    .unwrap();
    let dirty: BTreeSet<String> = ["wallets".into()].into();
    let stats = snap.restore(&ctl, &dirty, false).await.unwrap();
    assert_eq!(stats.tables, vec!["tenant.wallets"]);
    let tenant_bal: i64 = ctl
        .client()
        .query_one("SELECT balance FROM tenant.wallets WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    let public_bal: i64 = ctl
        .client()
        .query_one("SELECT balance FROM public.wallets WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(tenant_bal, 200);
    assert_eq!(public_bal, 999);

    Snapshot::discard(&ctl).await.unwrap();
    drop(ctl);
    db.cleanup().await;
}

#[tokio::test]
async fn terminate_open_transactions_cleans_leaked_advisory_locks_and_temp_tables() {
    let Some(db) = TestDatabase::create().await else {
        return;
    };
    let ctl = PgControl::connect(db.url()).await.unwrap();

    let lock_holder = session(db.url()).await;
    let temp_holder = session(db.url()).await;
    let clean_holder = session(db.url()).await;
    let lock_pid: i32 = lock_holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let temp_pid: i32 = temp_holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let clean_pid: i32 = clean_holder
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);

    // Both dirty statements commit immediately (idle connections, no open
    // transaction), while clean_holder remains an innocent pooled connection.
    lock_holder
        .batch_execute("SELECT pg_advisory_lock(77)")
        .await
        .unwrap();
    temp_holder
        .batch_execute("CREATE TEMP TABLE leaked_scratch (x INT)")
        .await
        .unwrap();
    clean_holder.batch_execute("SELECT 1").await.unwrap();

    let dirty = ctl
        .backends_in_transaction(&[lock_pid, temp_pid, clean_pid])
        .await
        .unwrap();
    assert!(
        dirty.contains(&lock_pid),
        "advisory lock holder must be detected: {dirty:?}"
    );
    assert!(
        dirty.contains(&temp_pid),
        "temp table owner must be detected: {dirty:?}"
    );
    assert!(
        !dirty.contains(&clean_pid),
        "clean idle connection must not be flagged by another session's temp table: {dirty:?}"
    );

    let terminated = ctl
        .terminate_open_transactions(&[lock_pid, temp_pid, clean_pid])
        .await
        .unwrap();
    assert_eq!(terminated.len(), 2);
    assert!(!terminated.contains(&clean_pid));
    let still_alive: i32 = clean_holder
        .query_one("SELECT 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(still_alive, 1);

    // Advisory lock 77 is now free.
    let acquired: bool = ctl
        .client()
        .query_one("SELECT pg_try_advisory_lock(77)", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        acquired,
        "advisory lock 77 should be released after termination"
    );
    let _ = ctl
        .client()
        .query_one("SELECT pg_advisory_unlock(77)", &[])
        .await;

    drop(lock_holder);
    drop(temp_holder);
    drop(clean_holder);
    drop(ctl);
    db.cleanup().await;
}
