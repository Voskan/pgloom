# Security Policy

## Supported versions

Security fixes land on `main` and ship in the next release of the latest
minor version. Older versions are not patched.

## Reporting a vulnerability

Please do not open a public issue. Report it privately through
[GitHub security advisories](https://github.com/Voskan/pgloom/security/advisories/new)
with the affected version, a reproduction and the impact you observed.

Examples of what qualifies: a crafted server or client message that crashes
or hangs the proxy, traffic forwarded to a host other than the configured
upstream, or snapshot and reset logic touching tables outside the scenario.

## Operating PgLoom safely

PgLoom is a testing tool. It runs `setup_sql`, `seed_sql` and `reset_sql`,
creates the `_pgloom_snap` schema and restores table contents between
schedules, so it must only ever point at a disposable test database, never
at production.

The proxy speaks plaintext and performs no authentication of its own; it
listens on `127.0.0.1` by default. Do not bind `database.proxy_listen` to a
public interface.
