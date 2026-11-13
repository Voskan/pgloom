# go-wallet

A small Go service using a `pgx` connection pool, with the classic
check-then-act withdrawal bug. PgLoom starts it, sends two concurrent
withdrawals of $100 from a wallet holding $100, and explores how their
transactions interleave.

```console
$ export PGLOOM_UPSTREAM_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres
$ pgloom run                      # ✗ RACE CONDITION DETECTED, exit 1
$ pgloom replay                   # ✗ REPRODUCED, from .pgloom-repro.json
$ WALLET_FIX=lock pgloom run      # SELECT ... FOR NO KEY UPDATE: ✓, exit 0
$ WALLET_FIX=atomic pgloom run    # UPDATE ... WHERE balance >= $1: ✓, exit 0
```

The service is unaware of PgLoom: `pgloom.yaml` only points its
`DATABASE_URL` at the proxy on `127.0.0.1:15432`.
