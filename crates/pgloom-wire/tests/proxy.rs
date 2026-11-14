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

fn bind_with_result_formats(
    portal: &str,
    statement: &str,
    params: &[Option<&[u8]>],
    result_formats: &[i16],
) -> pgloom_wire::frame::Frame {
    use bytes::BufMut;
    let base = pgloom_wire::messages::build::bind(portal, statement, params);
    let mut body = bytes::BytesMut::from(&base.body[..base.body.len() - 2]);
    body.put_i16(result_formats.len() as i16);
    for fmt in result_formats {
        body.put_i16(*fmt);
    }
    pgloom_wire::frame::Frame::new(pgloom_wire::messages::tag::BIND, body.freeze())
}

fn binary_data_row(cols: &[&[u8]]) -> pgloom_wire::frame::Frame {
    use bytes::BufMut;
    let mut body = bytes::BytesMut::new();
    body.put_i16(cols.len() as i16);
    for col in cols {
        body.put_i32(col.len() as i32);
        body.put_slice(col);
    }
    pgloom_wire::frame::Frame::new(pgloom_wire::messages::backend_tag::DATA_ROW, body.freeze())
}

#[tokio::test]
async fn pipelined_describes_and_portal_describe_decode_binary_rows() {
    use bytes::BytesMut;
    use pgloom_wire::error::Peer;
    use pgloom_wire::frame::encode_startup;
    use pgloom_wire::io::FrameReader;
    use pgloom_wire::messages::{backend_tag, build, tag};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, mut wr) = sock.into_split();
        let mut reader = FrameReader::new(rd, Peer::Client);
        let _ = reader.next_startup().await.unwrap().unwrap();

        let mut init = BytesMut::new();
        build::authentication_ok().encode(&mut init);
        build::backend_key_data(5100, 99).encode(&mut init);
        build::ready_for_query(TxStatus::Idle).encode(&mut init);
        wr.write_all(&init).await.unwrap();

        // Batch 1: Parse(s1) + Describe(Statement s1) + Parse(s2) + Describe(Statement s2) + Flush
        for expected in [
            tag::PARSE,
            tag::DESCRIBE,
            tag::PARSE,
            tag::DESCRIBE,
            tag::FLUSH,
        ] {
            let f = reader.next().await.unwrap().unwrap();
            assert_eq!(f.tag, expected);
        }
        let mut resp = BytesMut::new();
        build::parse_complete().encode(&mut resp);
        build::row_description(&[("balance", 20)]).encode(&mut resp);
        build::parse_complete().encode(&mut resp);
        build::no_data().encode(&mut resp);
        wr.write_all(&resp).await.unwrap();

        // Batch 2: Bind("", s1, binary result) + Execute("") + Sync
        for expected in [tag::BIND, tag::EXECUTE, tag::SYNC] {
            let f = reader.next().await.unwrap().unwrap();
            assert_eq!(f.tag, expected);
        }
        let mut resp = BytesMut::new();
        build::bind_complete().encode(&mut resp);
        binary_data_row(&[&420i64.to_be_bytes()]).encode(&mut resp);
        build::command_complete("SELECT 1").encode(&mut resp);
        build::ready_for_query(TxStatus::Idle).encode(&mut resp);
        wr.write_all(&resp).await.unwrap();

        // Batch 3: Parse(s3) + Bind(p3, s3, binary result) + Describe(Portal p3) + Flush
        for expected in [tag::PARSE, tag::BIND, tag::DESCRIBE, tag::FLUSH] {
            let f = reader.next().await.unwrap().unwrap();
            assert_eq!(f.tag, expected);
        }
        let mut resp = BytesMut::new();
        build::parse_complete().encode(&mut resp);
        build::bind_complete().encode(&mut resp);
        build::row_description(&[("stock", 23)]).encode(&mut resp);
        wr.write_all(&resp).await.unwrap();

        // Batch 4: Execute(p3) + Sync
        for expected in [tag::EXECUTE, tag::SYNC] {
            let f = reader.next().await.unwrap().unwrap();
            assert_eq!(f.tag, expected);
        }
        let mut resp = BytesMut::new();
        binary_data_row(&[&19i32.to_be_bytes()]).encode(&mut resp);
        build::command_complete("SELECT 1").encode(&mut resp);
        build::ready_for_query(TxStatus::Idle).encode(&mut resp);
        wr.write_all(&resp).await.unwrap();

        let term = reader.next().await.unwrap().unwrap();
        assert_eq!(term.tag, tag::TERMINATE);
    });

    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: upstream_addr.to_string(),
    })
    .await
    .unwrap();
    let mut ctl = Controller::new(proxy.gate().open_episode());

    let client = tokio::spawn({
        let addr = proxy.local_addr();
        async move {
            let sock = TcpStream::connect(addr).await.unwrap();
            let (rd, mut wr) = sock.into_split();
            let mut reader = FrameReader::new(rd, Peer::Server);
            wr.write_all(&encode_startup(&[("user", "test"), ("database", "test")]))
                .await
                .unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }

            // Pipelined multi-Describe + Flush.
            let mut b1 = BytesMut::new();
            build::parse("s1", "SELECT balance FROM wallets WHERE id = $1", &[23]).encode(&mut b1);
            build::describe_statement("s1").encode(&mut b1);
            build::parse("s2", "UPDATE wallets SET balance = 0 WHERE id = $1", &[23])
                .encode(&mut b1);
            build::describe_statement("s2").encode(&mut b1);
            build::flush().encode(&mut b1);
            wr.write_all(&b1).await.unwrap();
            for expected in [
                backend_tag::PARSE_COMPLETE,
                backend_tag::ROW_DESCRIPTION,
                backend_tag::PARSE_COMPLETE,
                backend_tag::NO_DATA,
            ] {
                let f = reader.next().await.unwrap().unwrap();
                assert_eq!(f.tag, expected);
            }

            // Execute s1 with binary result format (no new Describe!).
            let mut b2 = BytesMut::new();
            bind_with_result_formats("", "s1", &[Some(b"1")], &[1]).encode(&mut b2);
            build::execute("", 0).encode(&mut b2);
            build::sync().encode(&mut b2);
            wr.write_all(&b2).await.unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }

            // Bind(p3) + Describe(Portal p3) + Flush, then Execute(p3) + Sync.
            let mut b3 = BytesMut::new();
            build::parse("s3", "SELECT stock FROM items WHERE id = $1", &[23]).encode(&mut b3);
            bind_with_result_formats("p3", "s3", &[Some(b"1")], &[1]).encode(&mut b3);
            build::describe_portal("p3").encode(&mut b3);
            build::flush().encode(&mut b3);
            wr.write_all(&b3).await.unwrap();
            for expected in [
                backend_tag::PARSE_COMPLETE,
                backend_tag::BIND_COMPLETE,
                backend_tag::ROW_DESCRIPTION,
            ] {
                let f = reader.next().await.unwrap().unwrap();
                assert_eq!(f.tag, expected);
            }

            let mut b4 = BytesMut::new();
            build::execute("p3", 0).encode(&mut b4);
            build::sync().encode(&mut b4);
            wr.write_all(&b4).await.unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }

            wr.write_all(&build::terminate().to_bytes()).await.unwrap();
        }
    });

    let (_info1, resp1) = ctl.step(1).await;
    assert_eq!(resp1.summary(), "returns 420");

    let (_info2, resp2) = ctl.step(1).await;
    assert_eq!(resp2.summary(), "returns 19");

    timeout(STEP, client).await.unwrap().unwrap();
    timeout(STEP, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn supervised_copy_in_and_copy_out_stream_cleanly() {
    use bytes::{Bytes, BytesMut};
    use pgloom_wire::error::Peer;
    use pgloom_wire::frame::{Frame, encode_startup};
    use pgloom_wire::io::FrameReader;
    use pgloom_wire::messages::{backend_tag, build, tag};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, mut wr) = sock.into_split();
        let mut reader = FrameReader::new(rd, Peer::Client);
        let _ = reader.next_startup().await.unwrap().unwrap();

        let mut init = BytesMut::new();
        build::authentication_ok().encode(&mut init);
        build::backend_key_data(5200, 77).encode(&mut init);
        build::ready_for_query(TxStatus::Idle).encode(&mut init);
        wr.write_all(&init).await.unwrap();

        // 1. Eagerly-pipelined Simple COPY IN (pgx style): Query MUST arrive
        // before CopyData even though both were sent while gated!
        let q = reader.next().await.unwrap().unwrap();
        assert_eq!(q.tag, tag::QUERY);
        wr.write_all(&build::copy_in_response(2).to_bytes())
            .await
            .unwrap();
        let d = reader.next().await.unwrap().unwrap();
        assert_eq!(d.tag, tag::COPY_DATA);
        assert_eq!(&d.body[..], b"1\t100\n");
        let done = reader.next().await.unwrap().unwrap();
        assert_eq!(done.tag, tag::COPY_DONE);
        let mut fin = BytesMut::new();
        build::command_complete("COPY 1").encode(&mut fin);
        build::ready_for_query(TxStatus::Idle).encode(&mut fin);
        wr.write_all(&fin).await.unwrap();

        // 2. Extended COPY IN with Flush: Parse + Bind + Execute + Flush,
        // wait for CopyInResponse, then CopyData + CopyDone + Sync.
        for expected in [tag::PARSE, tag::BIND, tag::EXECUTE, tag::FLUSH] {
            let f = reader.next().await.unwrap().unwrap();
            assert_eq!(f.tag, expected);
        }
        let mut resp = BytesMut::new();
        build::parse_complete().encode(&mut resp);
        build::bind_complete().encode(&mut resp);
        build::copy_in_response(2).encode(&mut resp);
        wr.write_all(&resp).await.unwrap();
        for expected in [tag::COPY_DATA, tag::COPY_DONE, tag::SYNC] {
            let f = reader.next().await.unwrap().unwrap();
            assert_eq!(f.tag, expected);
        }
        let mut fin = BytesMut::new();
        build::command_complete("COPY 1").encode(&mut fin);
        build::ready_for_query(TxStatus::Idle).encode(&mut fin);
        wr.write_all(&fin).await.unwrap();

        // 3. Simple COPY OUT: CopyOutResponse ('H') + CopyData + CopyDone
        // stream immediately, while ReadyForQuery is held for release.
        let q_out = reader.next().await.unwrap().unwrap();
        assert_eq!(q_out.tag, tag::QUERY);
        let mut out = BytesMut::new();
        build::copy_out_response(2).encode(&mut out);
        Frame::new(backend_tag::COPY_DATA, Bytes::from_static(b"1\t100\n")).encode(&mut out);
        Frame::new(backend_tag::COPY_DATA, Bytes::from_static(b"2\t200\n")).encode(&mut out);
        Frame::new(backend_tag::COPY_DONE, Bytes::new()).encode(&mut out);
        build::command_complete("COPY 2").encode(&mut out);
        build::ready_for_query(TxStatus::Idle).encode(&mut out);
        wr.write_all(&out).await.unwrap();

        let term = reader.next().await.unwrap().unwrap();
        assert_eq!(term.tag, tag::TERMINATE);
    });

    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: upstream_addr.to_string(),
    })
    .await
    .unwrap();
    let mut ctl = Controller::new(proxy.gate().open_episode());

    let client = tokio::spawn({
        let addr = proxy.local_addr();
        async move {
            let sock = TcpStream::connect(addr).await.unwrap();
            let (rd, mut wr) = sock.into_split();
            let mut reader = FrameReader::new(rd, Peer::Server);
            wr.write_all(&encode_startup(&[("user", "test"), ("database", "test")]))
                .await
                .unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }

            // 1. Eagerly send Query + CopyData + CopyDone in one TCP write.
            let mut eager = BytesMut::new();
            build::query("COPY wallets FROM STDIN").encode(&mut eager);
            Frame::new(tag::COPY_DATA, Bytes::from_static(b"1\t100\n")).encode(&mut eager);
            Frame::new(tag::COPY_DONE, Bytes::new()).encode(&mut eager);
            wr.write_all(&eager).await.unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }

            // 2. Extended COPY IN with Flush.
            let mut ext = BytesMut::new();
            build::parse("", "COPY wallets FROM STDIN", &[]).encode(&mut ext);
            build::bind("", "", &[]).encode(&mut ext);
            build::execute("", 0).encode(&mut ext);
            build::flush().encode(&mut ext);
            wr.write_all(&ext).await.unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::COPY_IN_RESPONSE {
                    break;
                }
            }
            let mut payload = BytesMut::new();
            Frame::new(tag::COPY_DATA, Bytes::from_static(b"2\t200\n")).encode(&mut payload);
            Frame::new(tag::COPY_DONE, Bytes::new()).encode(&mut payload);
            build::sync().encode(&mut payload);
            wr.write_all(&payload).await.unwrap();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }

            // 3. COPY TO STDOUT.
            wr.write_all(&build::query("COPY wallets TO STDOUT").to_bytes())
                .await
                .unwrap();
            let mut copied = Vec::new();
            loop {
                let f = reader.next().await.unwrap().unwrap();
                if f.tag == backend_tag::COPY_DATA {
                    copied.extend_from_slice(&f.body);
                } else if f.tag == backend_tag::READY_FOR_QUERY {
                    break;
                }
            }
            assert_eq!(copied, b"1\t100\n2\t200\n");

            wr.write_all(&build::terminate().to_bytes()).await.unwrap();
        }
    });

    // Ensure eager CopyData was buffered while the turn was held at the gate.
    let pending1 = ctl.wait_ready(1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    pending1.permit.grant();
    let f1 = loop {
        let ev = ctl.next_event().await;
        if let Some(f) = ctl.absorb(ev) {
            break f;
        }
    };
    assert_eq!(f1.response.command_tags, vec!["COPY 1"]);
    f1.release.release();

    let (_info2, resp2) = ctl.step(1).await;
    assert_eq!(resp2.command_tags, vec!["COPY 1"]);

    let (_info3, resp3) = ctl.step(1).await;
    assert_eq!(resp3.command_tags, vec!["COPY 2"]);

    timeout(STEP, client).await.unwrap().unwrap();
    timeout(STEP, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn real_postgres_tokio_postgres_copy_in_and_copy_out_under_gate() {
    use bytes::Bytes;
    use futures::{SinkExt, TryStreamExt};
    use tokio_postgres::NoTls;

    let Ok(db_url) = std::env::var("PGLOOM_TEST_DATABASE_URL") else {
        return;
    };
    let cfg: tokio_postgres::Config = db_url.parse().expect("valid PGLOOM_TEST_DATABASE_URL");
    let host = match cfg.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
        _ => return,
    };
    let port = cfg.get_ports().first().copied().unwrap_or(5432);

    let proxy = spawn_proxy(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: format!("{host}:{port}"),
    })
    .await
    .unwrap();

    let mut client_cfg = tokio_postgres::Config::new();
    if let Some(user) = cfg.get_user() {
        client_cfg.user(user);
    }
    if let Some(password) = cfg.get_password() {
        client_cfg.password(password);
    }
    if let Some(dbname) = cfg.get_dbname() {
        client_cfg.dbname(dbname);
    }
    client_cfg.host("127.0.0.1");
    client_cfg.port(proxy.local_addr().port());
    let (client, connection) = client_cfg.connect(NoTls).await.unwrap();
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("CREATE TEMP TABLE pgloom_copy_test (id INT, note TEXT)")
        .await
        .unwrap();

    let mut ctl = Controller::new(proxy.gate().open_episode());

    let worker = tokio::spawn(async move {
        let sink = client
            .copy_in("COPY pgloom_copy_test (id, note) FROM STDIN")
            .await
            .unwrap();
        let mut sink = std::pin::pin!(sink);
        sink.send(Bytes::from_static(b"1\talpha\n2\tbeta\n"))
            .await
            .unwrap();
        let inserted = sink.finish().await.unwrap();
        assert_eq!(inserted, 2);

        let stream = client
            .copy_out("COPY pgloom_copy_test (id, note) TO STDOUT")
            .await
            .unwrap();
        let chunks: Vec<Bytes> = stream.try_collect().await.unwrap();
        let mut all = Vec::new();
        for c in chunks {
            all.extend_from_slice(&c);
        }
        assert_eq!(all, b"1\talpha\n2\tbeta\n");

        let rows = client
            .query("SELECT id, note FROM pgloom_copy_test ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i32>(0), 1);
        assert_eq!(rows[0].get::<_, String>(1), "alpha");
    });

    // Three gated turns: COPY FROM STDIN, COPY TO STDOUT, then the SELECT.
    let (_i1, r1) = ctl.step(1).await;
    assert_eq!(r1.command_tags, vec!["COPY 2"]);
    let (_i2, r2) = ctl.step(1).await;
    assert_eq!(r2.command_tags, vec!["COPY 2"]);
    let (_i3, r3) = ctl.step(1).await;
    assert_eq!(r3.summary(), "returns id=1, note=alpha (+1 rows)");

    timeout(STEP, worker).await.unwrap().unwrap();
    drop(ctl);
    let _ = timeout(STEP, conn_task).await;
}
