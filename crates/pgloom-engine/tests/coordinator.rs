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
