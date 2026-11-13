use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use pgloom_driver::{Config, ScenarioWorkload, check_responses};
use pgloom_engine::{Violation, WorkerId, Workload};

#[derive(Clone)]
struct Bank {
    balance: Arc<AtomicI64>,
    guarded: bool,
}

async fn withdraw(
    State(bank): State<Bank>,
    headers: HeaderMap,
    body: String,
) -> (StatusCode, String) {
    if headers.get("x-test").and_then(|v| v.to_str().ok()) != Some("yes") {
        return (StatusCode::BAD_REQUEST, "missing header".into());
    }
    let amount: i64 = body.trim().parse().unwrap_or(0);
    if bank.guarded {
        let prev = bank.balance.fetch_sub(amount, Ordering::SeqCst);
        if prev < amount {
            bank.balance.fetch_add(amount, Ordering::SeqCst);
            return (StatusCode::CONFLICT, "insufficient funds".into());
        }
    } else {
        bank.balance.fetch_sub(amount, Ordering::SeqCst);
    }
    (
        StatusCode::OK,
        format!("balance={}", bank.balance.load(Ordering::SeqCst)),
    )
}

async fn serve(guarded: bool) -> (String, Arc<AtomicI64>) {
    let balance = Arc::new(AtomicI64::new(100));
    let app = axum::Router::new()
        .route("/withdraw", post(withdraw))
        .with_state(Bank {
            balance: balance.clone(),
            guarded,
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), balance)
}

fn scenario(base: &str) -> Config {
    let src = format!(
        r#"
version: 1
name: http-driver
database:
  upstream: postgres://postgres@127.0.0.1/app
workers:
  - id: req_a
    http:
      method: post
      url: "{base}/withdraw"
      headers: {{ x-test: "yes" }}
      body: "100"
  - id: req_b
    http:
      method: POST
      url: "${{BASE_URL}}/withdraw"
      headers: {{ x-test: "yes" }}
      body: "100"
invariants:
  responses:
    - name: at_most_one_successful_withdrawal
      max_2xx_count: 1
"#
    );
    let base = base.to_owned();
    Config::parse_with_env(&src, "test.yaml", move |k| {
        (k == "BASE_URL").then(|| base.clone())
    })
    .unwrap()
}

async fn run_all(wl: &ScenarioWorkload) -> Vec<Option<pgloom_engine::WorkerOutcome>> {
    let futs: Vec<_> = (0..wl.worker_count() as u32)
        .map(|i| wl.launch(WorkerId(i)))
        .collect();
    futures_join(futs).await.into_iter().map(Some).collect()
}

async fn futures_join<F: Future + Send + 'static>(futs: Vec<F>) -> Vec<F::Output>
where
    F::Output: Send + 'static,
{
    let handles: Vec<_> = futs.into_iter().map(tokio::spawn).collect();
    let mut out = Vec::new();
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

#[tokio::test]
async fn vulnerable_service_violates_max_2xx_count() {
    let (base, balance) = serve(false).await;
    let cfg = scenario(&base);
    let wl = ScenarioWorkload::new(&cfg.workers, "postgres://unused").unwrap();
    assert_eq!(wl.worker_name(WorkerId(1)), "req_b");
    let outcomes = run_all(&wl).await;
    assert!(
        outcomes.iter().flatten().all(|o| o.label == "HTTP 200 OK"),
        "{outcomes:?}"
    );
    assert_eq!(balance.load(Ordering::SeqCst), -100);
    let v = check_responses(&cfg.invariants.responses, wl.ids(), &outcomes);
    assert_eq!(
        v,
        vec![Violation::InvariantFailed {
            name: "at_most_one_successful_withdrawal".into(),
            description: "expected <= 1 HTTP 2xx, got 2 (req_a=200, req_b=200)".into(),
            observed: String::new(),
        }]
    );
}

#[tokio::test]
async fn guarded_service_passes() {
    let (base, balance) = serve(true).await;
    let cfg = scenario(&base);
    let wl = ScenarioWorkload::new(&cfg.workers, "postgres://unused").unwrap();
    let outcomes = run_all(&wl).await;
    let statuses: Vec<_> = outcomes.iter().flatten().map(|o| o.status).collect();
    assert!(
        statuses.contains(&Some(200)) && statuses.contains(&Some(409)),
        "{statuses:?}"
    );
    assert_eq!(balance.load(Ordering::SeqCst), 0);
    assert!(check_responses(&cfg.invariants.responses, wl.ids(), &outcomes).is_empty());
}

#[tokio::test]
async fn unreachable_service_is_a_failed_outcome() {
    let cfg = scenario("http://127.0.0.1:1");
    let wl = ScenarioWorkload::new(&cfg.workers, "postgres://unused").unwrap();
    let out = wl.run(WorkerId(0)).await;
    assert!(!out.success);
    assert_eq!(out.label, "HTTP error");
}
