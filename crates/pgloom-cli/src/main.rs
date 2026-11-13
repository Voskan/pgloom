use std::process::ExitCode;

use clap::Parser;
use pgloom_cli::cli::Cli;
use tracing_subscriber::EnvFilter;

const EXIT_INTERRUPTED: u8 = 130;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let default = match cli.verbose {
        0 => "warn",
        1 => "pgloom=debug,pgloom_engine=debug,pgloom_wire=debug,pgloom_cli=debug",
        _ => "pgloom=trace,pgloom_engine=trace,pgloom_wire=trace,pgloom_cli=trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default)),
        )
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("cannot start the async runtime: {e}");
            return ExitCode::from(pgloom_cli::error::EXIT_INTERNAL);
        }
    };
    let code = runtime.block_on(async {
        tokio::select! {
            code = pgloom_cli::execute(cli) => code,
            _ = tokio::signal::ctrl_c() => EXIT_INTERRUPTED,
        }
    });
    // Do not wait for detached tasks (proxy sessions, HTTP keep-alives).
    runtime.shutdown_timeout(std::time::Duration::from_millis(200));
    ExitCode::from(code)
}
