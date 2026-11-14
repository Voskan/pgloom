# sql-transfer

Two transfers between the same pair of accounts, in opposite directions,
written as plain SQL workers. No service is involved: PgLoom runs each
worker's statements on its own connection through the proxy and decides
which one executes next.

```console
$ export PGLOOM_UPSTREAM_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres
$ pgloom run examples/sql-transfer/pgloom.yaml   # ✗ DEADLOCK DETECTED, exit 1
$ pgloom replay                                  # ✗ REPRODUCED, from .pgloom-repro.json
```

`a_to_b` debits account 1 and then credits account 2; `b_to_a` does the
reverse. When each has run its first `UPDATE`, each waits for the row the
other holds. PgLoom sees the cycle in the wait-for graph built from
`pg_blocking_pids()` and reports it at once, instead of waiting for
PostgreSQL's `deadlock_timeout` to cancel one of the transactions.

The fix is to lock both rows in one canonical order before writing either:

```sql
BEGIN;
SELECT id FROM accounts WHERE id = ANY('{1,2}') ORDER BY id FOR NO KEY UPDATE;
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
UPDATE accounts SET balance = balance + 10 WHERE id = 2;
COMMIT;
```

SQL workers are the quickest way to check a stored procedure or a migration
for races before any service code exists. Each entry in `sql:` is one turn;
`BEGIN` and `COMMIT` are sent as written, so the transactions commit for real
and the next schedule starts from the snapshot PgLoom restores.
