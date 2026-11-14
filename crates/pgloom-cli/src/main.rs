use std::io::IsTerminal;
use std::process::ExitCode;

use clap::Parser;
use pgloom_cli::cli::{Cli, ColorChoice};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let default = match cli.verbose {
        0 => "warn",
        1 => "pgloom=debug,pgloom_engine=debug,pgloom_wire=debug,pgloom_cli=debug",
        _ => "pgloom=trace,pgloom_engine=trace,pgloom_wire=trace,pgloom_cli=trace",
    };
    // Log lines go to stderr, which CI and `2> file` capture separately from
    // the report: color them only for a terminal.
    let ansi = match cli.color {
        ColorChoice::Always => true,
        ColorChoice::Never => false,
        ColorChoice::Auto => {
            std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none()
        }
    };
    tracing_subscriber::fmt()
        .with_ansi(ansi)
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
            () = interrupts() => pgloom_cli::error::EXIT_INTERRUPTED,
        }
    });
    // Do not wait for detached tasks (proxy sessions, HTTP keep-alives).
    runtime.shutdown_timeout(std::time::Duration::from_millis(200));
    ExitCode::from(code)
}

/// Resolves on the second Ctrl+C; the first one asks the command to wind
/// down cleanly.
#[allow(clippy::print_stderr)]
async fn interrupts() {
    if tokio::signal::ctrl_c().await.is_err() {
        return std::future::pending().await;
    }
    pgloom_cli::interrupt::request();
    eprintln!(
        "\ninterrupted: finishing the current schedule and restoring the database (Ctrl+C again to abort)"
    );
    let _ = tokio::signal::ctrl_c().await;
}
