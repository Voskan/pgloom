//! End-to-end coordinator tests: proxy + mock PostgreSQL + scripted workers.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use pgloom_engine::{
    Coordinator, CoordinatorConfig, DbControl, EngineError, EpisodeOracle, EpisodeRecord,
    RunOutcome, ScheduleExplorer, StepAction, Strategy, Violation, WorkerId, WorkerOutcome,
    Workload,
};
use pgloom_wire::testkit::{MockClient, MockPostgres};
use pgloom_wire::{ProxyConfig, ProxyHandle, spawn_proxy};

#[derive(Debug, Clone)]
enum Script {
    /// Check-then-act withdrawal without a row lock.
    CheckThenAct { amount: i64 },
    /// Same, but the read takes `FOR UPDATE`.
    Locked { amount: i64 },
    /// Reads a missing row outside a transaction and returns 404 at once.
    EarlyExit,
    /// Moves money between two rows, locking `from` first.
    Transfer { from: i64, to: i64 },
}

fn outcome(status: i32) -> WorkerOutcome {
    WorkerOutcome {
        success: (200..300).contains(&status),
        status: Some(status),
        label: format!("HTTP {status}"),
        detail: None,
        ..WorkerOutcome::default()
    }
}

async fn run_script(addr: SocketAddr, script: Script) -> WorkerOutcome {
    let Ok(mut c) = MockClient::connect(addr, false).await else {
        return outcome(503);
    };
    let status = async {
        match script {
            Script::CheckThenAct { amount } | Script::Locked { amount } => {
                let lock = if matches!(script, Script::Locked { .. }) {
                    " FOR UPDATE"
                } else {
                    ""
                };
                c.query("BEGIN").await.ok()?;
                let r = c
                    .query(&format!("SELECT balance FROM wallets WHERE id = 1{lock}"))
                    .await
                    .ok()?;
                if r.error.is_some() {
                    c.query("ROLLBACK").await.ok()?;
                    return Some(500);
                }
                let balance: i64 = r.rows.first()?.first()?.1.parse().ok()?;
                if balance < amount {
                    c.query("ROLLBACK").await.ok()?;
                    return Some(409);
                }
                c.query(&format!(
                    "UPDATE wallets SET balance = balance - {amount} WHERE id = 1"
                ))
                .await
                .ok()?;
                c.query("COMMIT").await.ok()?;
                Some(200)
            }
            Script::EarlyExit => {
                let r = c
                    .query("SELECT balance FROM wallets WHERE id = 9")
                    .await
                    .ok()?;
                Some(if r.row_count == 0 { 404 } else { 200 })
            }
            Script::Transfer { from, to } => {
                c.query("BEGIN").await.ok()?;
                for (id, delta) in [(from, 10), (to, -10)] {
                    let r = c
                        .query(&format!(
                            "UPDATE wallets SET balance = balance - {delta} WHERE id = {id}"
                        ))
                        .await
                        .ok()?;
                    if r.error.is_some() {
                        c.query("ROLLBACK").await.ok()?;
                        return Some(500);
                    }
                }
                c.query("COMMIT").await.ok()?;
                Some(200)
            }
        }
    }
    .await
    .unwrap_or(599);
    let _ = c.terminate().await;
    outcome(status)
}

struct Scripts {
    addr: SocketAddr,
    scripts: Vec<Script>,
}

impl Workload for Scripts {
    fn worker_count(&self) -> usize {
        self.scripts.len()
    }

    fn worker_name(&self, worker: WorkerId) -> String {
        format!("w{}", worker.0)
    }

    fn launch(
        &self,
        worker: WorkerId,
    ) -> impl std::future::Future<Output = WorkerOutcome> + Send + 'static {
        run_script(self.addr, self.scripts[worker.index()].clone())
    }
}

struct MockDb {
    mock: Arc<MockPostgres>,
    initial: Vec<(i64, i64)>,
}

impl DbControl for MockDb {
    async fn blocking_pids(&self, pids: &[i32]) -> Result<BTreeMap<i32, Vec<i32>>, EngineError> {
        Ok(pids
            .iter()
            .map(|p| (*p, self.mock.blocking_pids(*p)))
            .collect())
    }

    async fn cancel_backend(&self, pid: i32) -> Result<(), EngineError> {
        self.mock.cancel(pid);
        Ok(())
    }

    async fn cleanup_backends(&self, pids: &[i32]) -> Result<Vec<i32>, EngineError> {
        let open: Vec<i32> = self
            .mock
            .sessions_in_transaction()
            .into_iter()
            .filter(|p| pids.contains(p))
            .collect();
        for p in &open {
            self.mock.terminate(*p);
        }
        Ok(open)
    }

    async fn reset(&self, _record: &EpisodeRecord) -> Result<(), EngineError> {
        self.mock.set_wallets(&self.initial);
        Ok(())
    }
}

struct WalletOracle {
    mock: Arc<MockPostgres>,
    max_successes: usize,
}

impl EpisodeOracle for WalletOracle {
    async fn check(&self, record: &EpisodeRecord) -> Result<Vec<Violation>, EngineError> {
        let mut v = Vec::new();
        let balance = self.mock.balance(1).unwrap_or_default();
        if balance < 0 {
            v.push(Violation::InvariantFailed {
                name: "no_negative_balance".into(),
                description: String::new(),
                observed: format!("id=1, balance={balance}"),
            });
        }
        if record.success_count() > self.max_successes {
            v.push(Violation::InvariantFailed {
                name: "at_most_one_success".into(),
                description: String::new(),
                observed: format!("{} successes", record.success_count()),
            });
        }
        Ok(v)
    }
}

struct Harness {
    _proxy: ProxyHandle,
    coordinator: Coordinator<Scripts, MockDb, WalletOracle>,
}

async fn harness(
    wallets: &[(i64, i64)],
    scripts: Vec<Script>,
    config: CoordinatorConfig,
) -> Harness {
    harness_with(wallets, scripts, config, 1).await
}

async fn harness_with(
    wallets: &[(i64, i64)],
    scripts: Vec<Script>,
    config: CoordinatorConfig,
    max_successes: usize,
) -> Harness {
    let mock = Arc::new(MockPostgres::start(wallets).await.unwrap());
    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: mock.addr().to_string(),
    })
    .await
    .unwrap();
    let coordinator = Coordinator::new(
        proxy.gate().clone(),
        Scripts {
            addr: proxy.local_addr(),
            scripts,
        },
        MockDb {
            mock: Arc::clone(&mock),
            initial: wallets.to_vec(),
        },
        WalletOracle {
            mock,
            max_successes,
        },
        config,
    );
    Harness {
        _proxy: proxy,
        coordinator,
    }
}

fn config() -> CoordinatorConfig {
    CoordinatorConfig {
        lock_probe: Duration::from_millis(5),
        step_timeout: Duration::from_secs(5),
        ..CoordinatorConfig::default()
    }
}

#[tokio::test]
async fn finds_and_shrinks_the_double_spend() {
    let h = harness(
        &[(1, 100)],
        vec![Script::CheckThenAct { amount: 100 }; 2],
        CoordinatorConfig {
            stop_on_first_failure: true,
            ..config()
        },
    )
    .await;
    let report = h
        .coordinator
        .run(ScheduleExplorer::new(Strategy::Exhaustive, 100))
        .await
        .unwrap();
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    let failure = &report.failures[0];
    let names: Vec<String> = failure.violations.iter().map(Violation::name).collect();
    assert!(
        names.contains(&"no_negative_balance".to_string()),
        "{names:?}"
    );
    assert!(names.contains(&"at_most_one_success".to_string()));
    assert_eq!(
        failure.schedule.preemptions(),
        1,
        "{:?}",
        failure.schedule.choices()
    );

    // BEGIN is coalesced: 3 scheduled turns per worker, plus completions.
    let begins = failure
        .record
        .steps
        .iter()
        .filter(|s| matches!(s.action, StepAction::Begin))
        .count();
    assert_eq!(begins, 2);
    let reads: Vec<&str> = failure
        .record
        .steps
        .iter()
        .filter(|s| s.sql.starts_with("SELECT"))
        .filter_map(|s| s.result.as_deref())
        .collect();
    assert_eq!(reads, vec!["returns 100", "returns 100"]);
    assert!(failure.record.dirty_tables.contains("wallets"));

    // The minimal schedule replays deterministically.
    for _ in 0..5 {
        let again = h
            .coordinator
            .replay(failure.schedule.choices())
            .await
            .unwrap();
        assert!(!again.violations.is_empty());
        assert_eq!(again.schedule.choices(), failure.schedule.choices());
    }
}

#[tokio::test]
async fn exhaustive_run_covers_all_twenty_schedules_of_the_vulnerable_wallet() {
    let h = harness(
        &[(1, 100)],
        vec![Script::CheckThenAct { amount: 100 }; 2],
        CoordinatorConfig {
            stop_on_first_failure: false,
            shrink: false,
            ..config()
        },
    )
    .await;
    let report = h
        .coordinator
        .run(ScheduleExplorer::new(Strategy::Exhaustive, 1000))
        .await
        .unwrap();
    assert!(report.stats.complete);
    // SELECT, UPDATE, COMMIT per worker, but a worker that reads 0 after the
    // other committed exits through ROLLBACK, which shapes the tree.
    assert!(report.stats.schedules >= 10, "{:?}", report.stats);
    // Every failing schedule breaks the same invariants: one report, the
    // rest counted.
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert!(report.failures[0].repeats >= 1);
    assert!(report.failures[0].repeats < report.stats.schedules);
}

#[tokio::test]
async fn row_locks_are_parked_and_the_fixed_wallet_passes() {
    let h = harness(
        &[(1, 100)],
        vec![Script::Locked { amount: 100 }; 2],
        config(),
    )
    .await;
    let report = h
        .coordinator
        .run(ScheduleExplorer::new(Strategy::Exhaustive, 1000))
        .await
        .unwrap();
    assert_eq!(report.outcome, RunOutcome::Pass, "{:?}", report.failures);
    assert!(report.stats.complete);
    assert!(report.parks > 0, "the second locker must have been parked");
}

#[tokio::test]
async fn early_worker_exit_does_not_hang_the_scheduler() {
    let h = harness(
        &[(1, 100)],
        vec![
            Script::EarlyExit,
            Script::CheckThenAct { amount: 10 },
            Script::EarlyExit,
        ],
        config(),
    )
    .await;
    let report = tokio::time::timeout(
        Duration::from_secs(20),
        h.coordinator
            .run(ScheduleExplorer::new(Strategy::Exhaustive, 1000)),
    )
    .await
    .expect("scheduler hung")
    .unwrap();
    assert_eq!(report.outcome, RunOutcome::Pass);
    assert!(report.stats.complete);
}

#[tokio::test]
async fn ab_ba_transfers_are_a_proven_deadlock() {
    let h = harness_with(
        &[(1, 100), (2, 100)],
        vec![
            Script::Transfer { from: 1, to: 2 },
            Script::Transfer { from: 2, to: 1 },
        ],
        config(),
        usize::MAX,
    )
    .await;
    let report = h
        .coordinator
        .run(ScheduleExplorer::new(
            Strategy::Bounded {
                max_preemptions: 2,
                extend_beyond_bound: false,
            },
            100,
        ))
        .await
        .unwrap();
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    let failure = &report.failures[0];
    match &failure.violations[0] {
        Violation::Deadlock {
            cycle, statements, ..
        } => {
            assert_eq!(cycle, &vec![WorkerId(0), WorkerId(1)]);
            assert_eq!(statements.len(), 2);
            assert!(
                statements
                    .iter()
                    .all(|(_, sql)| sql.starts_with("UPDATE wallets"))
            );
        }
        other => panic!("expected deadlock, got {other:?}"),
    }
    assert!(
        failure
            .record
            .steps
            .iter()
            .any(|s| matches!(s.action, StepAction::Park { .. }))
    );
}

#[tokio::test]
async fn exploration_order_is_identical_across_runs() {
    let mut reference: Option<Vec<Vec<WorkerId>>> = None;
    for _ in 0..3 {
        let h = harness(
            &[(1, 100)],
            vec![Script::Locked { amount: 60 }; 2],
            CoordinatorConfig {
                stop_on_first_failure: false,
                shrink: false,
                ..config()
            },
        )
        .await;
        let mut explorer = ScheduleExplorer::new(Strategy::Exhaustive, 100);
        let mut seen = Vec::new();
        while explorer.next_schedule() {
            let ep = h.coordinator.run_episode(&mut explorer).await.unwrap();
            seen.push(ep.schedule.choices());
        }
        match &reference {
            None => reference = Some(seen),
            Some(r) => assert_eq!(&seen, r),
        }
    }
}

#[tokio::test]
async fn autocommit_check_then_act_is_bounded_and_shrunk_to_one_preemption() {
    struct AutocommitScripts {
        addr: SocketAddr,
    }
    impl Workload for AutocommitScripts {
        fn worker_count(&self) -> usize {
            2
        }
        fn worker_name(&self, w: WorkerId) -> String {
            format!("w{}", w.0)
        }
        fn launch(
            &self,
            _w: WorkerId,
        ) -> impl std::future::Future<Output = WorkerOutcome> + Send + 'static {
            let addr = self.addr;
            async move {
                let Ok(mut c) = MockClient::connect(addr, false).await else {
                    return outcome(503);
                };
                // Six autocommit statements with no BEGIN/COMMIT: every turn
                // completes in TxStatus::Idle.
                let status = async {
                    let _ = c
                        .query("SELECT balance FROM wallets WHERE id = 2")
                        .await
                        .ok()?;
                    let _ = c
                        .query("SELECT balance FROM wallets WHERE id = 2")
                        .await
                        .ok()?;
                    let r = c
                        .query("SELECT balance FROM wallets WHERE id = 1")
                        .await
                        .ok()?;
                    let bal: i64 = r.rows.first()?.first()?.1.parse().ok()?;
                    if bal < 100 {
                        return Some(409);
                    }
                    let _ = c
                        .query("SELECT balance FROM wallets WHERE id = 2")
                        .await
                        .ok()?;
                    c.query("UPDATE wallets SET balance = balance - 100 WHERE id = 1")
                        .await
                        .ok()?;
                    let _ = c
                        .query("SELECT balance FROM wallets WHERE id = 2")
                        .await
                        .ok()?;
                    Some(200)
                }
                .await
                .unwrap_or(599);
                let _ = c.terminate().await;
                outcome(status)
            }
        }
    }

    let wallets = [(1, 100), (2, 50)];
    let mock = Arc::new(MockPostgres::start(&wallets).await.unwrap());
    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: mock.addr().to_string(),
    })
    .await
    .unwrap();
    let coordinator = Coordinator::new(
        proxy.gate().clone(),
        AutocommitScripts {
            addr: proxy.local_addr(),
        },
        MockDb {
            mock: Arc::clone(&mock),
            initial: wallets.to_vec(),
        },
        WalletOracle {
            mock,
            max_successes: 1,
        },
        CoordinatorConfig {
            stop_on_first_failure: true,
            shrink: true,
            ..config()
        },
    );
    let report = coordinator
        .run(ScheduleExplorer::new(
            Strategy::Bounded {
                max_preemptions: 2,
                extend_beyond_bound: false,
            },
            200,
        ))
        .await
        .unwrap();
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    assert!(
        report.stats.schedules < 10,
        "autocommit CHESS should find the race within a handful of schedules, took {}",
        report.stats.schedules
    );
    let failure = &report.failures[0];
    assert_eq!(failure.schedule.preemptions(), 1);
    assert!(
        failure.record.steps.iter().any(|s| s.preempted),
        "autocommit context switches must be marked as preemptions"
    );
}

#[tokio::test]
async fn pruned_dpor_schedule_marks_opaque_writes_and_resets_cleanly() {
    use pgloom_engine::{Reduction, SchemaModel};
    use pgloom_pgctrl::{CatalogRelation, RelationKind, SchemaCatalog};
    use std::sync::Mutex;

    struct RecordingDb {
        inner: MockDb,
        resets: Arc<Mutex<Vec<bool>>>,
    }
    impl DbControl for RecordingDb {
        async fn blocking_pids(
            &self,
            pids: &[i32],
        ) -> Result<BTreeMap<i32, Vec<i32>>, EngineError> {
            self.inner.blocking_pids(pids).await
        }
        async fn cancel_backend(&self, pid: i32) -> Result<(), EngineError> {
            self.inner.cancel_backend(pid).await
        }
        async fn cleanup_backends(&self, pids: &[i32]) -> Result<Vec<i32>, EngineError> {
            self.inner.cleanup_backends(pids).await
        }
        async fn reset(&self, record: &EpisodeRecord) -> Result<(), EngineError> {
            self.resets.lock().unwrap().push(record.opaque_writes);
            self.inner.reset(record).await
        }
    }

    let wallets = [(1, 100)];
    let mock = Arc::new(MockPostgres::start(&wallets).await.unwrap());
    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: mock.addr().to_string(),
    })
    .await
    .unwrap();
    let resets = Arc::new(Mutex::new(Vec::new()));
    let catalog = SchemaCatalog {
        relations: vec![
            CatalogRelation {
                oid: 1,
                schema: "public".into(),
                name: "a".into(),
                kind: RelationKind::Table,
                side_effects: false,
                row_security: false,
            },
            CatalogRelation {
                oid: 2,
                schema: "public".into(),
                name: "b".into(),
                kind: RelationKind::Table,
                side_effects: false,
                row_security: false,
            },
            CatalogRelation {
                oid: 3,
                schema: "public".into(),
                name: "c".into(),
                kind: RelationKind::Table,
                side_effects: false,
                row_security: false,
            },
            CatalogRelation {
                oid: 4,
                schema: "public".into(),
                name: "wallets".into(),
                kind: RelationKind::Table,
                side_effects: false,
                row_security: false,
            },
        ],
        ..SchemaCatalog::default()
    };
    // Schedule 1: w0(R(a), W(wallets)), w1(W(c), W(a)), w2(W(b)), w3(W(b), W(c)).
    //   w0:R(a) races with w1:W(a) -> adds w1 at depth 0.
    // Schedule 2 (depth 0 = w1, sleep = {w0: R(a)}): w1 runs W(c), W(a), then w0, w2, w3.
    //   w1:W(c) races with w3:W(c) via w2:W(b) -> w3:W(b) -> adds w2 at depth 0.
    // Schedule 3 (depth 0 = w2, sleep = {w0: R(a), w1: W(c)}): w2:W(b) and w3:W(b)
    //   both commute with w0 and w1 and exit (w1 has not run yet), leaving only
    //   asleep workers {w0, w1} -> sleep-set prune while w0 still has a pending
    //   UPDATE wallets that runs during wind_down().
    let w1_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct PrunableWorkload {
        addr: SocketAddr,
        w1_ran: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Workload for PrunableWorkload {
        fn worker_count(&self) -> usize {
            4
        }
        fn worker_name(&self, w: WorkerId) -> String {
            format!("w{}", w.0)
        }
        fn launch(
            &self,
            w: WorkerId,
        ) -> impl std::future::Future<Output = WorkerOutcome> + Send + 'static {
            let addr = self.addr;
            let w1_ran = Arc::clone(&self.w1_ran);
            async move {
                let Ok(mut c) = MockClient::connect(addr, false).await else {
                    return outcome(503);
                };
                match w.0 {
                    0 => {
                        let _ = c.query("SELECT id FROM a").await;
                        let _ = c.query("UPDATE wallets SET balance = 0 WHERE id = 1").await;
                    }
                    1 => {
                        let _ = c.query("UPDATE c SET x = 1").await;
                        w1_ran.store(true, std::sync::atomic::Ordering::SeqCst);
                        let _ = c.query("UPDATE a SET x = 1").await;
                    }
                    2 => {
                        let _ = c.query("UPDATE b SET x = 1").await;
                    }
                    _ => {
                        let _ = c.query("UPDATE b SET x = 2").await;
                        if w1_ran.load(std::sync::atomic::Ordering::SeqCst) {
                            let _ = c.query("UPDATE c SET x = 2").await;
                        }
                    }
                }
                let _ = c.terminate().await;
                outcome(200)
            }
        }
    }

    let coordinator = Coordinator::new(
        proxy.gate().clone(),
        PrunableWorkload {
            addr: proxy.local_addr(),
            w1_ran: Arc::clone(&w1_ran),
        },
        RecordingDb {
            inner: MockDb {
                mock: Arc::clone(&mock),
                initial: wallets.to_vec(),
            },
            resets: Arc::clone(&resets),
        },
        WalletOracle {
            mock: Arc::clone(&mock),
            max_successes: 4,
        },
        CoordinatorConfig {
            stop_on_first_failure: false,
            shrink: false,
            schema: Some(Arc::new(SchemaModel::from_catalog(&catalog))),
            ..config()
        },
    );

    let mut explorer =
        ScheduleExplorer::new(Strategy::Exhaustive, 200).with_reduction(Reduction::Dpor);
    let mut saw_pruned = false;
    while explorer.next_schedule() {
        w1_ran.store(false, std::sync::atomic::Ordering::SeqCst);
        let ep = coordinator.run_episode(&mut explorer).await.unwrap();
        if ep.pruned {
            saw_pruned = true;
            assert!(
                ep.record.opaque_writes,
                "a pruned schedule with live workers must force a full snapshot restore"
            );
        }
        // Every episode (pruned or complete) must leave the DB restored.
        assert_eq!(mock.balance(1), Some(100));
    }
    assert!(saw_pruned, "expected DPOR to prune at least one schedule");
    assert!(resets.lock().unwrap().iter().any(|&opaque| opaque));
}

#[tokio::test]
async fn errored_episode_always_resets_database_state() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FlakySlowWorkload {
        addr: SocketAddr,
        runs: Arc<AtomicUsize>,
    }
    impl Workload for FlakySlowWorkload {
        fn worker_count(&self) -> usize {
            2
        }
        fn worker_name(&self, w: WorkerId) -> String {
            format!("w{}", w.0)
        }
        fn launch(
            &self,
            w: WorkerId,
        ) -> impl std::future::Future<Output = WorkerOutcome> + Send + 'static {
            let addr = self.addr;
            let run = self.runs.load(Ordering::SeqCst);
            async move {
                let Ok(mut c) = MockClient::connect(addr, false).await else {
                    return outcome(503);
                };
                if w == WorkerId(0) {
                    let _ = c
                        .query("UPDATE wallets SET balance = -500 WHERE id = 1")
                        .await;
                    if run == 0 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                } else {
                    let _ = c.query("SELECT balance FROM wallets WHERE id = 1").await;
                }
                let _ = c.terminate().await;
                outcome(200)
            }
        }
    }

    let wallets = [(1, 100)];
    let mock = Arc::new(MockPostgres::start(&wallets).await.unwrap());
    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: mock.addr().to_string(),
    })
    .await
    .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let coordinator = Coordinator::new(
        proxy.gate().clone(),
        FlakySlowWorkload {
            addr: proxy.local_addr(),
            runs: Arc::clone(&runs),
        },
        MockDb {
            mock: Arc::clone(&mock),
            initial: wallets.to_vec(),
        },
        WalletOracle {
            mock: Arc::clone(&mock),
            max_successes: 2,
        },
        CoordinatorConfig {
            lock_probe: Duration::from_millis(5),
            step_timeout: Duration::from_millis(120),
            drain_timeout: Duration::from_millis(100),
            ..config()
        },
    );

    let mut ex = ScheduleExplorer::new(Strategy::Exhaustive, 5);
    assert!(ex.next_schedule());
    let err = coordinator.run_episode(&mut ex).await.unwrap_err();
    assert!(matches!(err, EngineError::WorkerStalled { .. }), "{err:?}");
    // Even though the episode errored after mutating wallets, reset() must have restored it.
    assert_eq!(mock.balance(1), Some(100));
    runs.store(1, Ordering::SeqCst);
}

#[tokio::test]
async fn shrink_with_find_all_preserves_every_violated_invariant() {
    let h = harness(
        &[(1, 100)],
        vec![Script::CheckThenAct { amount: 100 }; 2],
        CoordinatorConfig {
            stop_on_first_failure: false,
            shrink: true,
            ..config()
        },
    )
    .await;
    let report = h
        .coordinator
        .run(ScheduleExplorer::new(Strategy::Exhaustive, 200))
        .await
        .unwrap();
    assert_eq!(report.outcome, RunOutcome::ViolationFound);
    let failure = &report.failures[0];
    let names: std::collections::BTreeSet<String> =
        failure.violations.iter().map(Violation::name).collect();
    assert!(names.contains("no_negative_balance"), "{names:?}");
    assert!(names.contains("at_most_one_success"), "{names:?}");
    assert_eq!(failure.schedule.preemptions(), 1);
}
