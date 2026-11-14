use std::path::{Path, PathBuf};
use std::time::Instant;

use pgloom_driver::Config;
use pgloom_report::{ReproFile, format_duration, render_episode};

use crate::cli::ReplayArgs;
use crate::commands::run::report_context;
use crate::error::{CliError, EXIT_PASS, EXIT_VIOLATION};
use crate::output::Printer;
use crate::session::{Overrides, RunOptions, Session};

pub async fn replay(args: ReplayArgs, printer: Printer) -> Result<u8, CliError> {
    let repro = ReproFile::load(&args.repro)?;
    // The embedded scenario is authoritative: the schedule refers to its workers.
    let mut config = Config::parse(&repro.config_source, &repro.config_path)?;
    let scenario = locate_scenario(&repro.config_path, &args.repro);
    config.resolve_paths(match &scenario {
        Some(path) => path.parent().unwrap_or(Path::new("")),
        None => args.repro.parent().unwrap_or(Path::new("")),
    });
    if let Some(path) = &scenario
        && let Ok(current) = std::fs::read_to_string(path)
        && !repro.matches_config(&current)
    {
        printer.warn(&format!(
            "{} changed since this failure was recorded; replaying the recorded scenario",
            repro.config_path
        ));
    }
    Overrides {
        upstream: args.connection.upstream.clone(),
        listen: args.connection.listen.clone(),
        ..Overrides::default()
    }
    .apply(&mut config)?;
    if config
        .workers
        .iter()
        .map(|w| &w.id)
        .ne(repro.workers.iter())
    {
        return Err(CliError::Config(format!(
            "{} does not match its embedded scenario's workers",
            args.repro.display()
        )));
    }
    printer.progress(&format!(
        "▶ pgloom replay [{}]: {} recorded decisions, {} preemption(s)",
        config.name,
        repro.choice_sequence.len(),
        repro.preemptions
    ));
    let session = Session::start(
        config,
        RunOptions {
            stop_on_first_failure: true,
            shrink: false,
        },
    )
    .await?;
    let started = Instant::now();
    let result = session.replay(repro.choices()).await;
    let elapsed = started.elapsed();
    let config = session.config().clone();
    let workers = session.workers().to_vec();
    session.close().await;
    let episode = result?;

    let ctx = report_context(&config, &workers, &printer);
    let mut text = String::new();
    if episode.violations.is_empty() {
        text.push_str(&format!(
            "{}  [{}]\n  The recorded schedule replayed in {} and now satisfies every invariant.\n  Run `pgloom run` to verify all other interleavings.\n\n",
            printer.paint("✓ NOT REPRODUCED", "32"),
            config.name,
            format_duration(elapsed)
        ));
        text.push_str(&render_episode(&ctx, &episode.record, &[]));
        printer.report(&text);
        Ok(EXIT_PASS)
    } else {
        text.push_str(&format!(
            "{}  [{}]\n  Replayed the recorded schedule in {}.\n\n",
            printer.paint("✗ REPRODUCED", "31"),
            config.name,
            format_duration(elapsed)
        ));
        text.push_str(&render_episode(&ctx, &episode.record, &episode.violations));
        printer.report(&text);
        Ok(EXIT_VIOLATION)
    }
}

/// The scenario file a repro was recorded from. Its path is relative to the
/// directory `pgloom run` ran in, which need not be the current one: check
/// beside the repro file first (where `run` writes `.pgloom-repro.json` by
/// default) so an unrelated `./pgloom.yaml` in the current directory does not
/// shadow an example's scenario, then fall back to the recorded path as-is.
fn locate_scenario(recorded: &str, repro: &Path) -> Option<PathBuf> {
    let recorded = Path::new(recorded);
    let beside_repro = repro
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .map(|dir| dir.join(recorded));
    beside_repro
        .into_iter()
        .chain(std::iter::once(recorded.to_owned()))
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_is_found_from_any_directory() {
        let root = tempfile::tempdir().unwrap();
        let example = root.path().join("examples/wallet");
        std::fs::create_dir_all(&example).unwrap();
        std::fs::write(example.join("pgloom.yaml"), "name: example\n").unwrap();
        // An unrelated scenario in a parent directory does not shadow the
        // scenario next to the repro file.
        std::fs::write(root.path().join("pgloom.yaml"), "name: root\n").unwrap();
        let repro = example.join(".pgloom-repro.json");

        // Recorded inside the example directory, replayed from elsewhere.
        assert_eq!(
            locate_scenario("pgloom.yaml", &repro),
            Some(example.join("pgloom.yaml"))
        );
        // Recorded with a path that still resolves as written when `--repro-out`
        // placed the repro file elsewhere.
        let other_repro = root.path().join("artifacts/repro.json");
        let absolute = example.join("pgloom.yaml");
        assert_eq!(
            locate_scenario(absolute.to_str().unwrap(), &other_repro),
            Some(absolute)
        );
        assert_eq!(locate_scenario("missing.yaml", &repro), None);
    }
}
