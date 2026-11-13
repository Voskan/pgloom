use assert_cmd::Command;
use predicates::prelude::*;

fn pgloom() -> Command {
    let mut cmd = Command::cargo_bin("pgloom").unwrap();
    cmd.env_remove("PGLOOM_UPSTREAM_URL").env("NO_COLOR", "1");
    cmd
}

#[test]
fn help_lists_commands_and_exit_codes() {
    pgloom()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("replay"))
        .stdout(predicate::str::contains("demo"))
        .stdout(predicate::str::contains("doctor"))
        .stdout(predicate::str::contains("Exit codes"));
    pgloom()
        .args(["run", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--max-preemptions"))
        .stdout(predicate::str::contains("--repro-out"));
}

#[test]
fn version_is_printed() {
    pgloom()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("pgloom "));
}

#[test]
fn init_writes_a_valid_template_once() {
    let dir = tempfile::tempdir().unwrap();
    pgloom()
        .current_dir(dir.path())
        .arg("init")
        .assert()
        .success()
        .stdout(predicate::str::contains("Created pgloom.yaml"));
    let text = std::fs::read_to_string(dir.path().join("pgloom.yaml")).unwrap();
    assert!(text.contains("max_2xx_count: 1"));
    pgloom()
        .current_dir(dir.path())
        .arg("init")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("already exists"));
    pgloom()
        .current_dir(dir.path())
        .args(["init", "--force"])
        .assert()
        .success();
}

#[test]
fn invalid_scenarios_exit_with_2() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.yaml");
    std::fs::write(
        &path,
        "version: 1\nname: bad\ndatabase: { upstream: postgres://h/d }\nworkers:\n  - id: only\n    sql: [\"SELECT 1\"]\n",
    )
    .unwrap();
    pgloom()
        .arg("run")
        .arg(&path)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("at least 2 concurrent workers"));

    std::fs::write(&path, "version: 1\nname: x\nunknown_key: 1\n").unwrap();
    pgloom()
        .arg("run")
        .arg(&path)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("invalid YAML"));

    pgloom()
        .arg("run")
        .arg(dir.path().join("missing.yaml"))
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot read"));
}

#[test]
fn unreachable_database_exits_with_2() {
    let dir = tempfile::tempdir().unwrap();
    pgloom()
        .current_dir(dir.path())
        .arg("init")
        .assert()
        .success();
    pgloom()
        .current_dir(dir.path())
        .args(["run", "--upstream", "postgres://u:p@127.0.0.1:1/db"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "cannot connect to the upstream database",
        ));
    pgloom()
        .current_dir(dir.path())
        .args(["doctor", "--upstream", "postgres://u:p@127.0.0.1:1/db"])
        .assert()
        .code(2)
        .stdout(predicate::str::contains("✓ scenario"))
        .stdout(predicate::str::contains("✗ database"));
}

#[test]
fn replay_rejects_missing_or_foreign_files() {
    let dir = tempfile::tempdir().unwrap();
    pgloom()
        .current_dir(dir.path())
        .arg("replay")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(".pgloom-repro.json"));
    std::fs::write(dir.path().join("x.json"), r#"{"format_version": 7}"#).unwrap();
    pgloom()
        .current_dir(dir.path())
        .args(["replay", "x.json"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("format 7"));
}

#[test]
fn conflicting_flags_are_usage_errors() {
    pgloom()
        .args(["run", "--find-all", "--stop-on-first-failure"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot be used with"));
}
