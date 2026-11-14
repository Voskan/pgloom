# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Update `ctutils` to 0.4.8 for RUSTSEC-2026-0385 (non-constant-time
  comparisons). It reaches PgLoom through the HMAC that `postgres-protocol`
  uses for SCRAM authentication.

### Fixed

- Catalog, snapshot and lock-inspection queries no longer panic on an
  unexpected column type; they fail with a typed error that names the query.
- Fix Advisor keeps `RETURNING` at the end of the SQL it proposes.
- Fix Advisor splits `SET` clauses only on top-level commas, so assignments
  such as `GREATEST(balance - $1, 0)` or `'a, b'` stay intact.
- Fix Advisor starts a `WHERE` clause when it guards a statement that has
  none, instead of appending a dangling `AND`.
- `pgloom replay` finds the scenario next to the repro file when run from
  another directory.

### Changed

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

[Unreleased]: https://github.com/Voskan/pgloom/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Voskan/pgloom/releases/tag/v0.1.0
