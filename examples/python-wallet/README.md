# python-wallet

A small Python service using a `psycopg` 3 connection pool. Its withdrawal
reads the balance, computes the new one in Python and writes it back: the
lost update that ORMs make easy. PgLoom starts it, sends two concurrent $60
withdrawals from a wallet holding $100, and explores every interleaving of
their transactions.

It needs Python 3.9 or newer; CI runs it on 3.9 and 3.13.

```console
$ python3 -m venv .venv && . .venv/bin/activate
$ pip install -r requirements.txt
$ export PGLOOM_UPSTREAM_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres
$ pgloom run                      # ✗ RACE CONDITION DETECTED, exit 1
$ pgloom replay                   # ✗ REPRODUCED, from .pgloom-repro.json
$ WALLET_FIX=lock pgloom run      # SELECT ... FOR NO KEY UPDATE: ✓, exit 0
$ WALLET_FIX=atomic pgloom run    # UPDATE ... WHERE balance >= %s: ✓, exit 0
```

The balance never goes negative here, so the SQL invariant alone would miss
the bug. The response invariants catch it twice over: both requests succeed,
and both report the same new balance of 40 (`distinct: /balance`).

The service creates its own table on startup, as services with embedded
migrations do. `setup_sql` runs before the service starts, `seed_sql` after
it is ready, and PgLoom snapshots the database only then.

The service is unaware of PgLoom: `pgloom.yaml` only points its
`DATABASE_URL` at the proxy on `127.0.0.1:15433`.
