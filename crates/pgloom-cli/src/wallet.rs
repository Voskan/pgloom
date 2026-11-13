//! A tiny wallet HTTP service used by `pgloom demo` and the end-to-end tests.
//! Each mode implements withdrawal the way real services do, correctly or not.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalletMode {
    /// `SELECT balance`, check in code, `UPDATE ... balance - $amount`.
    CheckThenAct = 0,
    /// `SELECT balance`, compute in code, `UPDATE ... SET balance = $new`.
    LostUpdate = 1,
    /// `SELECT ... FOR NO KEY UPDATE`, then update.
    RowLock = 2,
    /// A single guarded `UPDATE ... WHERE balance >= $amount`.
    AtomicGuard = 3,
}

impl WalletMode {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::LostUpdate,
            2 => Self::RowLock,
            3 => Self::AtomicGuard,
            _ => Self::CheckThenAct,
        }
    }
}

/// Creates `table` with one wallet holding `balance`.
pub fn setup_sql(table: &str, balance: i64) -> String {
    format!(
        "DROP TABLE IF EXISTS {table};\n\
         CREATE TABLE {table} (id INT PRIMARY KEY, balance BIGINT NOT NULL);\n\
         INSERT INTO {table} (id, balance) VALUES (1, {balance}), (2, {balance});\n"
    )
}

#[derive(Debug)]
struct Pool {
    url: String,
    idle: Mutex<Vec<Client>>,
}

impl Pool {
    async fn get(&self) -> Result<Client, tokio_postgres::Error> {
        while let Some(c) = self.idle.lock().await.pop() {
            if !c.is_closed() {
                return Ok(c);
            }
        }
        let (client, conn) = tokio_postgres::connect(&self.url, NoTls).await?;
        tokio::spawn(conn);
        Ok(client)
    }

    async fn put(&self, client: Client) {
        if !client.is_closed() {
            self.idle.lock().await.push(client);
        }
    }
}

#[derive(Debug)]
struct App {
    pool: Pool,
    table: String,
    mode: AtomicU8,
}

/// A running wallet service.
#[derive(Debug)]
pub struct WalletServer {
    addr: SocketAddr,
    app: Arc<App>,
    task: JoinHandle<()>,
}

impl WalletServer {
    /// Serves on `listen` (port 0 picks a free port), reaching PostgreSQL at
    /// `database_url` (normally the PgLoom proxy).
    pub async fn start(
        listen: SocketAddr,
        database_url: &str,
        table: &str,
        mode: WalletMode,
    ) -> std::io::Result<Self> {
        let app = Arc::new(App {
            pool: Pool {
                url: database_url.to_owned(),
                idle: Mutex::new(Vec::new()),
            },
            table: table.to_owned(),
            mode: AtomicU8::new(mode as u8),
        });
        let router = Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .route("/withdraw", post(withdraw))
            .route("/transfer", post(transfer))
            .with_state(app.clone());
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(Self { addr, app, task })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    pub fn set_mode(&self, mode: WalletMode) {
        self.app.mode.store(mode as u8, Ordering::SeqCst);
    }

    /// Drops pooled connections (e.g. before the proxy goes away).
    pub async fn clear_pool(&self) {
        self.app.pool.idle.lock().await.clear();
    }

    pub async fn stop(self) {
        self.clear_pool().await;
        self.task.abort();
        let _ = self.task.await;
    }
}

#[derive(Debug, Deserialize)]
struct Withdraw {
    #[serde(alias = "wallet_id")]
    account_id: i32,
    amount: i64,
}

#[derive(Debug, Deserialize)]
struct Transfer {
    from: i32,
    to: i32,
    amount: i64,
}

type Reply = (StatusCode, Json<Value>);

fn reply(status: StatusCode, body: Value) -> Reply {
    (status, Json(body))
}

fn failure(status: StatusCode, error: &str) -> Reply {
    reply(status, json!({ "error": error }))
}

fn withdrawn(balance: i64) -> Reply {
    reply(
        StatusCode::OK,
        json!({ "status": "withdrawn", "balance": balance }),
    )
}

fn db_error(e: &tokio_postgres::Error) -> Reply {
    let code = e.code().map_or("", |c| c.code());
    failure(
        StatusCode::INTERNAL_SERVER_ERROR,
        &format!("database error {code}: {e}"),
    )
}

async fn withdraw(State(app): State<Arc<App>>, Json(req): Json<Withdraw>) -> Reply {
    let client = match app.pool.get().await {
        Ok(c) => c,
        Err(e) => return db_error(&e),
    };
    let mode = WalletMode::from_u8(app.mode.load(Ordering::SeqCst));
    match run_withdraw(&client, &app.table, mode, &req).await {
        Ok(reply) => {
            app.pool.put(client).await;
            reply
        }
        // The connection may be inside an aborted transaction; discard it.
        Err(e) => db_error(&e),
    }
}

async fn run_withdraw(
    c: &Client,
    table: &str,
    mode: WalletMode,
    req: &Withdraw,
) -> Result<Reply, tokio_postgres::Error> {
    let insufficient = failure(StatusCode::CONFLICT, "insufficient funds");
    if mode == WalletMode::AtomicGuard {
        let row = c
            .query_opt(
                &format!(
                    "UPDATE {table} SET balance = balance - $1 \
                     WHERE id = $2 AND balance >= $1 RETURNING balance"
                ),
                &[&req.amount, &req.account_id],
            )
            .await?;
        return Ok(row.map_or(insufficient, |r| withdrawn(r.get(0))));
    }
    c.batch_execute("BEGIN").await?;
    let lock = if mode == WalletMode::RowLock {
        " FOR NO KEY UPDATE"
    } else {
        ""
    };
    let row = c
        .query_opt(
            &format!("SELECT balance FROM {table} WHERE id = $1{lock}"),
            &[&req.account_id],
        )
        .await?;
    let Some(row) = row else {
        c.batch_execute("ROLLBACK").await?;
        return Ok(failure(StatusCode::NOT_FOUND, "no such wallet"));
    };
    let balance: i64 = row.get(0);
    if balance < req.amount {
        c.batch_execute("ROLLBACK").await?;
        return Ok(insufficient);
    }
    let row = if mode == WalletMode::LostUpdate {
        c.query_one(
            &format!("UPDATE {table} SET balance = $1 WHERE id = $2 RETURNING balance"),
            &[&(balance - req.amount), &req.account_id],
        )
        .await?
    } else {
        c.query_one(
            &format!("UPDATE {table} SET balance = balance - $1 WHERE id = $2 RETURNING balance"),
            &[&req.amount, &req.account_id],
        )
        .await?
    };
    c.batch_execute("COMMIT").await?;
    Ok(withdrawn(row.get(0)))
}

async fn transfer(State(app): State<Arc<App>>, Json(req): Json<Transfer>) -> Reply {
    let client = match app.pool.get().await {
        Ok(c) => c,
        Err(e) => return db_error(&e),
    };
    let t = &app.table;
    let result = async {
        client.batch_execute("BEGIN").await?;
        client
            .execute(
                &format!("UPDATE {t} SET balance = balance - $1 WHERE id = $2"),
                &[&req.amount, &req.from],
            )
            .await?;
        client
            .execute(
                &format!("UPDATE {t} SET balance = balance + $1 WHERE id = $2"),
                &[&req.amount, &req.to],
            )
            .await?;
        client.batch_execute("COMMIT").await
    }
    .await;
    match result {
        Ok(()) => {
            app.pool.put(client).await;
            reply(StatusCode::OK, json!({ "status": "transferred" }))
        }
        Err(e) => db_error(&e),
    }
}
