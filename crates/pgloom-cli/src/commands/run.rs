use std::time::Instant;

use pgloom_driver::{Config, ReductionName, StrategyName};
use pgloom_engine::{RunOutcome, RunReport};
use pgloom_report::{AUDIT_FOOTER, ReportContext, ReproFile, render_json, render_run};

use crate::cli::{Format, RunArgs};
use crate::error::{CliError, EXIT_INTERRUPTED, EXIT_PASS, EXIT_VIOLATION};
use crate::output::Printer;
use crate::session::{Overrides, RunOptions, Session};

pub(crate) fn overrides(args: &RunArgs) -> Overrides {
    Overrides {
        upstream: args.connection.upstream.clone(),
        listen: args.connection.listen.clone(),
        strategy: args.strategy.map(Into::into),
        max_preemptions: args.max_preemptions,
        max_schedules: args.max_schedules,
        seed: args.seed,
        reduction: args.reduction.map(Into::into),
    }
}

/// Describes the exploration settings in one line.
pub fn plan_line(config: &Config) -> String {
    let e = &config.exploration;
    let strategy = match e.strategy {
        StrategyName::Exhaustive => match e.effective_reduction() {
            ReductionName::Dpor => "exhaustive (dpor)".to_owned(),
            ReductionName::None => "exhaustive".to_owned(),
        },
        StrategyName::Bounded => format!(
            "bounded (≤{} preemptions first, deeper while the budget lasts)",
            e.max_preemptions
        ),
        StrategyName::Random => format!("random (seed {})", e.seed.unwrap_or(0x5EED)),
    };
    format!(
        "{} workers · strategy {strategy} · max {} schedules",
        config.workers.len(),
        e.max_schedules
    )
}

pub fn report_context(config: &Config, workers: &[String], printer: &Printer) -> ReportContext {
    let mut ctx = ReportContext::new(config.name.clone(), workers.to_vec());
    ctx.color = printer.color;
    ctx.footer = (!printer.quiet).then(|| AUDIT_FOOTER.to_owned());
    ctx.compact = printer.quiet;
    ctx.max_schedules = config.exploration.max_schedules;
    ctx.strategy = match config.exploration.strategy {
        StrategyName::Exhaustive => "exhaustive",
        StrategyName::Bounded => "bounded",
        StrategyName::Random => "random",
    }
    .to_owned();
    ctx.descriptions = config
        .invariants
        .sql
        .iter()
        .filter_map(|i| i.description.clone().map(|d| (i.name.clone(), d)))
        .collect();
    ctx
}

pub async fn run(args: RunArgs, printer: Printer) -> Result<u8, CliError> {
    let loaded = Config::load_with_overrides(
        &args.config,
        args.connection.upstream.as_deref(),
        args.connection.listen.as_deref(),
    )?;
    let literal_password = loaded.upstream_has_literal_password();
    let mut config = loaded.config;
    overrides(&args).apply(&mut config)?;
    let pretty = args.format == Format::Pretty;
    if pretty {
        printer.progress(&format!(
            "▶ pgloom run [{}]: {}",
            config.name,
            plan_line(&config)
        ));
    }
    let options = RunOptions {
        stop_on_first_failure: !args.find_all,
        shrink: !args.no_shrink,
    };
    let started = Instant::now();
    let session = Session::start(config, options).await?;
    if pretty {
        printer.progress(&format!(
            "  proxy {} → {} (setup {}ms)",
            session
                .proxy_addr()
                .map_or_else(String::new, |a| a.to_string()),
            session.upstream(),
            started.elapsed().as_millis()
        ));
    }
    let ticker = pretty.then(|| printer.spawn_progress(session.control()));
    let result = session.run().await;
    if let Some(t) = ticker {
        t.finish().await;
    }
    let config = session.config().clone();
    let workers = session.workers().to_vec();
    session.close().await;
    let report: RunReport = result?;

    if report.unsupervised_turns > 0 {
        printer.warn(&format!(
            "{} turn(s) bypassed scheduling because no worker slot was free; \
             check whether background tasks or connection pools are hitting the proxy",
            report.unsupervised_turns
        ));
    }

    let mut ctx = report_context(&config, &workers, &printer);
    if let Some(first) = report.failures.first() {
        let repro = ReproFile::from_failure(
            &config.name,
            &args.config.display().to_string(),
            &loaded.source,
            &workers,
            first,
        );
        match repro.save(&args.repro_out) {
            Ok(()) => {
                if literal_password {
                    printer.warn(&format!(
                        "{} embeds database.upstream with a literal password; \
                         use ${{PGLOOM_UPSTREAM_URL}} before sharing or committing it",
                        args.repro_out.display()
                    ));
                }
                ctx.repro_path = Some(args.repro_out.display().to_string());
            }
            Err(e) => printer.warn(&e.to_string()),
        }
    }
    let text = match args.format {
        Format::Pretty => render_run(&ctx, &report),
        Format::Json => render_json(&ctx, &report),
    };
    printer.report(&text);
    Ok(match report.outcome {
        RunOutcome::Pass if report.interrupted => EXIT_INTERRUPTED,
        RunOutcome::Pass => EXIT_PASS,
        RunOutcome::ViolationFound => EXIT_VIOLATION,
    })
}
