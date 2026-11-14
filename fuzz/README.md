# PgLoom Fuzz Targets

Coverage-guided [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) targets
for PgLoom's wire codec, SQL classifier, and Fix Advisor.

## Running

```sh
cargo install cargo-fuzz

cargo +nightly fuzz run frame_decode
cargo +nightly fuzz run classify_sql
cargo +nightly fuzz run advisor_advise
```

For deterministic property-based fuzzing on stable Rust (executed on every CI
run without nightly), see `crates/pgloom-wire/tests/prop_wire.rs` and
`crates/pgloom-report/tests/prop_advisor.rs`.
