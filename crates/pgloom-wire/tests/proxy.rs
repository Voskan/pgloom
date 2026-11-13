//! End-to-end tests of the gating proxy against the in-process mock server.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use pgloom_wire::testkit::{MockClient, MockPostgres};
use pgloom_wire::{
    ConnId, GateEvent, PendingTurn, ProxyConfig, ProxyHandle, TurnInfo, TurnKind, TurnResponse,
    TxStatus, spawn_proxy,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const STEP: Duration = Duration::from_secs(5);

async fn setup() -> (MockPostgres, ProxyHandle) {
    let mock = MockPostgres::start(&[(1, 100), (2, 50)]).await.unwrap();
    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: mock.addr().to_string(),
    })
    .await
    .unwrap();
    (mock, proxy)
}

/// Test-side scheduler: grants turns in a scripted order.
struct Controller {
    rx: UnboundedReceiver<GateEvent>,
    ready: BTreeMap<ConnId, VecDeque<PendingTurn>>,
    closed: Vec<ConnId>,
}

impl Controller {
    fn new(rx: UnboundedReceiver<GateEvent>) -> Self {
        Self {
            rx,
            ready: BTreeMap::new(),
            closed: Vec::new(),
        }
    }

    fn absorb(&mut self, ev: GateEvent) -> Option<pgloom_wire::FinishedTurn> {
        match ev {
            GateEvent::TurnReady(p) => {
                self.ready.entry(p.info.conn).or_default().push_back(p);
                None
            }
            GateEvent::TurnFinished(f) => Some(f),
            GateEvent::ConnectionClosed { conn, .. } => {
                self.closed.push(conn);
                None
            }
        }
    }

    async fn next_event(&mut self) -> GateEvent {
        timeout(STEP, self.rx.recv())
            .await
            .expect("gate event timed out")
            .expect("gate closed")
    }

    async fn wait_ready(&mut self, conn: ConnId) -> PendingTurn {
        loop {
            if let Some(p) = self.ready.get_mut(&conn).and_then(VecDeque::pop_front) {
                return p;
            }
            let ev = self.next_event().await;
            assert!(self.absorb(ev).is_none(), "unexpected finish while waiting");
        }
    }

    /// Grants the next turn of `conn` and waits for its response.
    async fn step(&mut self, conn: ConnId) -> (TurnInfo, TurnResponse) {
        let pending = self.wait_ready(conn).await;
        let info = pending.info.clone();
        pending.permit.grant();
        loop {
            let ev = self.next_event().await;
            if let Some(f) = self.absorb(ev) {
                assert_eq!(f.conn, conn, "a different connection finished");
                let response = f.response.clone();
                f.release.release();
                return (info, response);
            }
        }
    }
}

fn spawn_script(
    addr: std::net::SocketAddr,
    client: MockClient,
    script: &'static [&'static str],
) -> JoinHandle<Vec<TurnResponse>> {
    let _ = addr;
    tokio::spawn(async move {
        let mut client = client;
        let mut out = Vec::new();
        for sql in script {
            out.push(client.query(sql).await.unwrap());
        }
        client.terminate().await.unwrap();
        out
    })
}

#[tokio::test]
async fn passthrough_without_episode_answers_ssl_probe_and_relays_pid() {
    let (mock, proxy) = setup().await;
    let mut client = MockClient::connect(proxy.local_addr(), true).await.unwrap();
    assert!(
        client.backend_pid() > 4800,
        "BackendKeyData must be relayed"
    );
    let r = client
        .query("SELECT balance FROM wallets WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(r.summary(), "returns 100");
    assert_eq!(r.tx_status, Some(TxStatus::Idle));
    assert_eq!(mock.log().len(), 1);
    client.terminate().await.unwrap();
}

#[tokio::test]
async fn turns_reach_the_server_in_controller_order() {
    let (mock, proxy) = setup().await;
    let events = proxy.gate().open_episode();
    let mut ctl = Controller::new(events);

    const SCRIPT: &[&str] = &[
        "BEGIN",
        "SELECT balance FROM wallets WHERE id = 1",
        "UPDATE wallets SET balance = balance - 100 WHERE id = 1",
        "COMMIT",
    ];
    let c1 = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let pid1 = c1.backend_pid();
    let w1 = spawn_script(proxy.local_addr(), c1, SCRIPT);
    let c2 = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let pid2 = c2.backend_pid();
    let w2 = spawn_script(proxy.local_addr(), c2, SCRIPT);

    // Conn 2 first: the classic double-spend interleaving.
    let order: [ConnId; 8] = [2, 2, 1, 1, 2, 2, 1, 1];
    let mut infos = Vec::new();
    for conn in order {
        let (info, _resp) = ctl.step(conn).await;
        infos.push(info);
    }
    let r1 = timeout(STEP, w1).await.unwrap().unwrap();
    let r2 = timeout(STEP, w2).await.unwrap().unwrap();

    let log: Vec<(i32, String)> = mock.log();
    let expected: Vec<(i32, String)> = order
        .iter()
        .zip([0, 1, 0, 1, 2, 3, 2, 3])
        .map(|(conn, i)| {
            let pid = if *conn == 1 { pid1 } else { pid2 };
            (pid, SCRIPT[i].to_string())
        })
        .collect();
    assert_eq!(
        log, expected,
        "server must observe the scheduled order verbatim"
    );

    // Both read 100 before either wrote: the double spend is reproduced.
    assert_eq!(r1[1].summary(), "returns 100");
    assert_eq!(r2[1].summary(), "returns 100");
    assert_eq!(mock.balance(1), Some(-100));

    assert_eq!(infos[0].tx_status, TxStatus::Idle);
    assert_eq!(infos[1].tx_status, TxStatus::InTransaction);
    assert_eq!(infos[1].backend_pid.map(|p| p.0), Some(pid2));
    assert!(matches!(
        infos[4].meta.kind(),
        TurnKind::DataWrite { ref target_tables } if target_tables == &["wallets".to_string()]
    ));
}

#[tokio::test]
async fn noise_and_stray_commits_bypass_the_gate() {
    let (_mock, proxy) = setup().await;
    let mut events = proxy.gate().open_episode();
    let mut client = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let r = timeout(STEP, client.query("SELECT 1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.summary(), "returns 1");
    // COMMIT outside a transaction block is pool hygiene, not a scheduling point.
    let r = timeout(STEP, client.query("COMMIT"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.tx_status, Some(TxStatus::Idle));
    assert!(
        events.try_recv().is_err(),
        "noise must not reach the controller"
    );
    client.terminate().await.unwrap();
}

#[tokio::test]
async fn lock_waiter_response_is_held_until_released() {
    let (mock, proxy) = setup().await;
    let events = proxy.gate().open_episode();
    let mut ctl = Controller::new(events);

    let c1 = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let w1 = spawn_script(
        proxy.local_addr(),
        c1,
        &[
            "BEGIN",
            "UPDATE wallets SET balance = 10 WHERE id = 1",
            "COMMIT",
        ],
    );
    let c2 = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let pid2 = c2.backend_pid();
    let w2 = spawn_script(
        proxy.local_addr(),
        c2,
        &[
            "BEGIN",
            "UPDATE wallets SET balance = balance - 1 WHERE id = 1",
            "COMMIT",
        ],
    );

    ctl.step(1).await;
    ctl.step(1).await;
    ctl.step(2).await;
    // Conn 2's UPDATE blocks on conn 1's row lock inside the server.
    let blocked = ctl.wait_ready(2).await;
    blocked.permit.grant();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(ctl.rx.try_recv().is_err(), "blocked turn must not finish");
    let blockers = mock.blocking_pids(pid2);
    assert_eq!(blockers.len(), 1);

    // Commit the holder; the waiter then completes but stays held.
    let (_, commit) = ctl.step(1).await;
    assert_eq!(commit.command_tags, vec!["COMMIT"]);
    let finished = loop {
        let ev = ctl.next_event().await;
        if let Some(f) = ctl.absorb(ev) {
            break f;
        }
    };
    assert_eq!(finished.conn, 2);
    assert_eq!(finished.response.summary(), "UPDATE 1");
    assert!(
        !w2.is_finished(),
        "client must not see the response before release"
    );
    finished.release.release();
    ctl.step(2).await;

    timeout(STEP, w1).await.unwrap().unwrap();
    timeout(STEP, w2).await.unwrap().unwrap();
    assert_eq!(mock.balance(1), Some(9));
}

#[tokio::test]
async fn dropping_permits_abandons_supervision_safely() {
    let (mock, proxy) = setup().await;
    let events = proxy.gate().open_episode();
    let mut ctl = Controller::new(events);
    let c1 = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let w1 = spawn_script(
        proxy.local_addr(),
        c1,
        &[
            "BEGIN",
            "UPDATE wallets SET balance = 7 WHERE id = 2",
            "COMMIT",
        ],
    );
    let first = ctl.wait_ready(1).await;
    proxy.gate().close_episode();
    drop(first);
    drop(ctl);
    let results = timeout(STEP, w1).await.unwrap().unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(mock.balance(2), Some(7));
}

#[tokio::test]
async fn extended_protocol_turns_carry_cached_sql_and_params() {
    let (_mock, proxy) = setup().await;
    let events = proxy.gate().open_episode();
    let mut ctl = Controller::new(events);
    let mut client = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        let a = client
            .query_extended(
                "s1",
                "SELECT balance FROM wallets WHERE id = $1",
                &["1"],
                true,
            )
            .await
            .unwrap();
        let b = client
            .query_extended("s1", "", &["2"], false)
            .await
            .unwrap();
        client.terminate().await.unwrap();
        (a, b)
    });
    let (first, _) = ctl.step(1).await;
    assert_eq!(
        first.statements[0].sql,
        "SELECT balance FROM wallets WHERE id = $1"
    );
    assert_eq!(first.statements[0].params, vec!["1"]);
    let (second, resp) = ctl.step(1).await;
    assert_eq!(
        second.statements[0].sql,
        "SELECT balance FROM wallets WHERE id = $1"
    );
    assert_eq!(second.statements[0].params, vec!["2"]);
    assert_eq!(resp.summary(), "returns 50");
    let (a, b) = timeout(STEP, task).await.unwrap().unwrap();
    assert_eq!(a.summary(), "returns 100");
    assert_eq!(b.summary(), "returns 50");
}

#[tokio::test]
async fn connection_close_is_reported() {
    let (_mock, proxy) = setup().await;
    let events = proxy.gate().open_episode();
    let mut ctl = Controller::new(events);
    let client = MockClient::connect(proxy.local_addr(), false)
        .await
        .unwrap();
    client.terminate().await.unwrap();
    loop {
        let ev = ctl.next_event().await;
        ctl.absorb(ev);
        if ctl.closed.contains(&1) {
            break;
        }
    }
}
