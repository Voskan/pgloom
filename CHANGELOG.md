# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-11-25

### Fixed

- Episode wind-down (`DPOR` sleep-set pruning, deadlock short-circuits, worker
  errors and shrink attempts) marks `opaque_writes` whenever live connections
  are drained, and error paths always reset database state before the next
  schedule runs.
- Snapshot capture and restore handle user triggers (via `SET LOCAL
  session_replication_role = replica` when permitted, or `DISABLE`/`ENABLE
  TRIGGER USER` with `SET CONSTRAINTS ALL DEFERRED` for table owners), circular
  foreign keys, direct writes to child partitions, classic `INHERITS` tables
  (`DELETE FROM ONLY`), standalone and `DEFAULT nextval(...)` sequences, and
  non-`public` `search_path` schemas; unresolved non-temp write targets such as
  updatable views fail closed instead of leaving stale rows.
- Idle pooled connections holding session-level advisory locks or temporary
  tables after a schedule are detected and terminated during reset so session
  state cannot leak into subsequent schedules.
- Extended-protocol `Bind` + `Execute` + `Flush` batches (`pgjdbc`
  `setFetchSize`, `asyncpg` cursors, `libpq` pipelines) are scheduled as
  `ExecuteFlush` turns and completed cleanly on `CommandComplete`,
  `PortalSuspended`, `EmptyQueryResponse` or `ErrorResponse`; `Parse` +
  `Describe` + `Bind` + `Flush` drains `BindComplete` in wire order.
- Per-connection prepared statement and column caches roll back on failed
  `Parse`, `Bind` or `Close` turns, stay synchronized across `DISCARD ALL`,
  `DEALLOCATE` and SQL `PREPARE`, and classify unknown prepared statements
  fail-closed as opaque writes.
- SQL classification and DPOR dependency analysis treat sequence mutations
  (`nextval`, `setval`, `currval`, `lastval`), side-effecting built-ins
  (`set_config`, `pg_advisory_unlock*`, `lo_*`), `TABLE`, `DECLARE` / `FETCH` /
  `MOVE`, `NOTIFY`, `SELECT ... INTO` inside `WITH`, role switches, and user or
  `STABLE` functions conservatively instead of bypassing the gate or commuting
  across concurrent writes; multi-table `FROM` / `USING` scans preserve tables
  after quoted aliases, column alias lists, subqueries and set-returning
  functions.
- Preemption-bounded exploration (`bounded`) updates the active worker across
  autocommit turns (`current` is preserved outside explicit `BEGIN ... COMMIT`
  blocks) so preemption budgets are enforced accurately for autocommit
  services.
- When `invariants.fail_on_deadlock` is `false`, wait-for cycles release held
  waiters so PostgreSQL raises `SQLSTATE 40P01` to the application's retry
  logic instead of terminating the run as a violation.
- Schedule shrinking with `--find-all` requires a candidate trace to preserve
  the full set of violated invariants rather than collapsing onto an unrelated
  single-invariant failure.
- Fix Advisor parenthesizes `OR` predicates before appending `WHERE` guards,
  negates multi-term subtractions (`balance - $1 - $2` -> `balance >= ($1) +
  ($2)`), scopes `SET` rewrites to numeric columns read in the transaction,
  handles `FOR SHARE` / `FOR KEY SHARE` upgrades, emits `FOR NO KEY UPDATE OF
  <table>` on outer joins, avoids invalid `FOR UPDATE` on `UNION`, `WITH` CTE
  and window queries, and skips duplicate `ON CONFLICT` clauses.
- `pgloom doctor` skips the `_pgloom_snap` `CREATE` privilege check when
  `database.reset_sql` is configured, `PgUrl::with_ssl_disabled` strips
  `channel_binding` and mixed-case SSL parameters, `replay` validates repro
  files atomically and honours CLI `--upstream` / `--listen` overrides,
  JSON response invariants compare 64-bit integers without `f64` rounding, wire
  frame decoding caps incremental buffer reservation, and `exec` workers cap
  captured output and clean up their process group on exit or timeout.

### Added

- `target.reset_http`: optional HTTP `POST` endpoint called between schedules
  after database reset to clear in-process caches in the target service.

## [0.1.2] - 2026-11-14

### Fixed

- Log lines on stderr (warnings such as an unhealthy `ready_http`) honor
  `--color` and are plain text when stderr is not a terminal, instead of
  carrying ANSI escapes into CI logs and redirected files.
- Pipelined `Parse` + `Describe` batches (multiple `Describe(Statement)` and
  `Describe(Portal)` messages in a single `Flush` or `Sync` pipeline) preserve
  each object's column layout in wire order instead of overwriting earlier
  schemas on `NoData`, and invalidate cached layouts on `Parse`, `Bind`,
  `Close` and transaction end.
- Supervised `COPY` streams safely across `CopyInResponse` (`'G'`) and
  `CopyOutResponse` (`'H'`) in both Simple Query and Extended Query protocols
  (`Execute` + `Flush` and `Execute` + `Sync` with post-`CopyDone` `Sync`),
  and keeps eagerly pipelined `CopyData` ordered behind a gated `COPY` turn.
- Fix Advisor handles multibyte UTF-8 at keyword boundaries, SQL comments
  (`/* ... */` and `-- ...`), double-quoted identifiers, `E'...'` escape
  strings, dollar-quoted literals and deeply nested `CAST(...)` expressions
  without panicking or commenting out proposed SQL.

### Added

- Property-based test suites (`proptest`) and `cargo-fuzz` targets (`fuzz/`)
  for wire frame/startup/message decoding, `classify_sql` and
  `pgloom_report::advisor::advise`.

### Changed

- Update `github.com/jackc/pgx/v5` to 5.12.0 in `examples/go-wallet`.

## [0.1.1] - 2026-11-14

### Security

- Update `ctutils` to 0.4.8 for RUSTSEC-2026-0385 (non-constant-time
  comparisons). It reaches PgLoom through the HMAC that `postgres-protocol`
  uses for SCRAM authentication.

### Fixed

- A run in which no SQL from the workers reached the proxy is an error (exit
  2), not a pass. This happens when the service ignores the proxy's
  `DATABASE_URL`, or when a stale copy of it already answers on the
  `ready_http` port; PgLoom now refuses to start in the second case. A service
  that exits during the run is reported with its output.
- Results of statements prepared with `Describe` and `Flush` (asyncpg, JDBC)
  are decoded with the formats their `Bind` requested: binary values no longer
  appear as raw bytes and columns keep their names. `date`, `timestamp` and
  `timestamptz` are decoded, and control characters in values are escaped.
- Connection and query errors show PostgreSQL's message, SQLSTATE and hint
  instead of `db error` or `error connecting to server`.
- Fix Advisor never proposes `FOR NO KEY UPDATE` on an aggregate, `GROUP BY`
  or `DISTINCT` read, which PostgreSQL rejects; it locks the rows the read
  covered instead.
- Fix Advisor reports write skew (a check over a set of rows followed by a
  write to a different row in each transaction) and proposes locking the
  checked rows or `SERIALIZABLE`.
- Fix Advisor treats ORM-style casts (`$1::INTEGER`, `CAST($1 AS numeric)`)
  as values computed in the application, and proposes an in-SQL update and a
  compare-and-set for lost updates.
- Fix Advisor proposes locking the parent row read in the same transaction
  (for example the coupon whose usage limit is checked) for phantom inserts.
- Catalog, snapshot and lock-inspection queries no longer panic on an
  unexpected column type; they fail with a typed error that names the query.
- Fix Advisor keeps `RETURNING` at the end of the SQL it proposes.
- Fix Advisor splits `SET` clauses only on top-level commas, so assignments
  such as `GREATEST(balance - $1, 0)` or `'a, b'` stay intact.
- Fix Advisor starts a `WHERE` clause when it guards a statement that has
  none, instead of appending a dangling `AND`.
- `pgloom replay` finds the scenario next to the repro file when run from
  another directory.

### Added

- Ctrl+C stops after the current schedule, restores the database and prints
  what was explored (exit 130); a second Ctrl+C aborts at once.
- A progress line on interactive terminals for runs longer than a second.
- `--quiet` prints only the verdict, the violated invariants and errors.
- Warnings when `ready_http` answers with a 4xx status, and a hint to use
  `sslmode=disable` when the service fails to start on a TLS error.

### Changed

- The `examples/python-wallet` requirements accept `psycopg-pool` 3.2 again,
  so the example runs on Python 3.9; CI runs it on 3.9 and 3.13.
- Help texts for `--strategy` and `--max-preemptions` state that bounded
  exploration goes deeper while `--max-schedules` allows.
- Release archives are also published under version-independent names, so
  `releases/latest/download/pgloom-<target>.tar.gz` always resolves.
- CI runs the full suite against PostgreSQL 12, 13, 14, 15, 16 and 17 and
  builds the minimum supported Rust version (1.89).
- `cargo-deny` checks advisories, licenses and sources on every change and
  weekly; Dependabot keeps dependencies and actions current.

## [0.1.0] - 2026-11-13

First public release.

- PostgreSQL v3 wire proxy that schedules whole turns (`Q` or `Sync` up to
  `ReadyForQuery`), with `SSLRequest`, `GSSENCRequest`, `CancelRequest`,
  `COPY` and per-connection prepared statement tracking.
- Park-on-lock through `pg_blocking_pids()` and deterministic deadlock
  detection from the wait-for graph.
- Exhaustive, preemption-bounded and random exploration, source-DPOR with
  sleep sets, schedule shrinking and bit-for-bit replay.
- State reset between schedules from unlogged `_pgloom_snap` shadow tables,
  or from `reset_sql`.
- HTTP, SQL and `exec` workers; SQL invariants and HTTP response invariants,
  including JSON body checks.
- Fix Advisor for check-then-act, lost update, phantom insert and lock-order
  inversion.
- Commands `run`, `replay`, `demo`, `init` and `doctor`; text and JSON
  reports; `.pgloom-repro.json` files.

[Unreleased]: https://github.com/Voskan/pgloom/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/Voskan/pgloom/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/Voskan/pgloom/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Voskan/pgloom/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Voskan/pgloom/releases/tag/v0.1.0
