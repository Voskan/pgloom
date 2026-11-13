use std::io::Write as _;

use crate::cli::InitArgs;
use crate::error::{CliError, EXIT_PASS};
use crate::output::Printer;

pub const TEMPLATE: &str = r#"# PgLoom scenario: https://github.com/Voskan/pgloom
#
# PgLoom runs the workers below concurrently through a PostgreSQL proxy and
# systematically explores how their transactions interleave. After every
# schedule it checks the invariants and restores the database.
version: 1
name: "wallet-double-spend-withdraw"

database:
  # The real PostgreSQL server. ${VAR:-default} reads the environment.
  upstream: "${PGLOOM_UPSTREAM_URL:-postgres://postgres:postgres@127.0.0.1:5432/app_test}"
  # Point your service's DATABASE_URL here (with sslmode=disable).
  proxy_listen: "127.0.0.1:15432"
  # Runs once before the service starts.
  setup_sql: |
    CREATE TABLE IF NOT EXISTS wallets (
      id INT PRIMARY KEY,
      balance BIGINT NOT NULL
    );
    TRUNCATE TABLE wallets;
    INSERT INTO wallets (id, balance) VALUES (1, 100);
  # Runs once the service is ready (after any migrations it runs on startup)
  # and before the reference snapshot is taken.
  seed_sql: null
  # Between schedules, tables written by the workers are restored from a
  # snapshot. Set reset_sql to use your own reset script instead.
  reset_sql: null

# Optional: let PgLoom start and stop the service under test.
target:
  command: null            # e.g. "go run ./cmd/api", run next to this file
  env:
    DATABASE_URL: "postgres://postgres:postgres@127.0.0.1:15432/app_test?sslmode=disable"
  ready_http: "http://127.0.0.1:8080/healthz"
  startup_timeout_ms: 5000

exploration:
  strategy: "bounded"      # exhaustive | bounded | random
  max_preemptions: 2
  max_schedules: 200
  # With `strategy: exhaustive`, partial-order reduction (`dpor`, the
  # default) runs each class of equivalent schedules once. Turn it off if
  # the workers also share state outside the database (in-memory caches).
  # reduction: "none"      # dpor | none
  lock_probe_ms: 2         # when to check pg_blocking_pids() for a waiting turn
  step_timeout_ms: 3000    # hard limit for a turn that is not waiting on a lock

# Concurrent triggers. Instead of `http`, a worker may run `sql:` (a list of
# statements sent through the proxy) or `exec:` (a shell command with
# DATABASE_URL pointing at the proxy).
workers:
  - id: "req_a"
    http:
      method: "POST"
      url: "http://127.0.0.1:8080/withdraw"
      headers:
        Content-Type: "application/json"
        Idempotency-Key: "key-a"
      body: '{"wallet_id": 1, "amount": 100}'
  - id: "req_b"
    http:
      method: "POST"
      url: "http://127.0.0.1:8080/withdraw"
      headers:
        Content-Type: "application/json"
        Idempotency-Key: "key-b"
      body: '{"wallet_id": 1, "amount": 100}'

invariants:
  # Must return true (or no rows) after every schedule.
  sql:
    - name: "no_negative_balance"
      description: "Wallet balance must never drop below zero"
      assert_true: "SELECT COALESCE(MIN(balance), 0) >= 0 FROM wallets"
      diagnostic_query: "SELECT id, balance FROM wallets WHERE id = 1"
  # Checked over the workers' outcomes: catches lost updates where the
  # database looks consistent but both callers were told they succeeded.
  responses:
    - name: "at_most_one_successful_withdrawal"
      description: "With a balance of 100, at most one withdrawal of 100 may succeed"
      max_2xx_count: 1
    # Field checks on JSON response bodies (RFC 6901 pointers), applied to
    # 2xx responses unless `apply_to: all`; `workers:` narrows the scope.
    - name: "reported_balance_is_consistent"
      json:
        - { pointer: "/balance", min: 0 }
      # Two withdrawals from one wallet cannot both leave the same balance.
      distinct: "/balance"
  fail_on_deadlock: true
"#;

pub fn init(args: &InitArgs, printer: Printer) -> Result<u8, CliError> {
    let path = &args.path;
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    if args.force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let mut file = options.open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            CliError::Config(format!(
                "{} already exists (use --force to overwrite)",
                path.display()
            ))
        } else {
            CliError::env(&format!("cannot write {}", path.display()), e)
        }
    })?;
    file.write_all(TEMPLATE.as_bytes())
        .map_err(|e| CliError::env(&format!("cannot write {}", path.display()), e))?;
    printer.report(&format!(
        "Created {}. Edit the workers and invariants, then run `pgloom run {}`.",
        path.display(),
        path.display()
    ));
    Ok(EXIT_PASS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_is_a_valid_scenario() {
        let c = pgloom_driver::Config::parse_with_env(TEMPLATE, "template", |_| None).unwrap();
        assert_eq!(c.workers.len(), 2);
        assert_eq!(c.invariants.responses[0].max_2xx_count, Some(1));
        assert!(c.invariants.responses[1].checks_bodies());
        // Uncommenting only the strategy must not disable the default reduction.
        let exhaustive = TEMPLATE.replace("strategy: \"bounded\"", "strategy: \"exhaustive\"");
        let c = pgloom_driver::Config::parse_with_env(&exhaustive, "template", |_| None).unwrap();
        assert_eq!(
            c.exploration.effective_reduction(),
            pgloom_driver::ReductionName::Dpor
        );
    }
}
