use std::path::Path;
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
    config.resolve_paths(
        Path::new(&repro.config_path)
            .parent()
            .unwrap_or(Path::new("")),
    );
    if let Ok(current) = std::fs::read_to_string(Path::new(&repro.config_path))
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
