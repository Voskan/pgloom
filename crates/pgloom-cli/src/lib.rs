//! The `pgloom` command-line tool as a library, so that scenarios can also be
//! driven programmatically (and by the end-to-end test-suite).

pub mod cli;
pub mod commands;
pub mod error;
pub mod oracle;
pub mod output;
pub mod session;
pub mod target;
pub mod wallet;

use cli::{Cli, Command};
use error::CliError;
use output::Printer;

/// Runs a parsed command line and returns the process exit code.
pub async fn execute(cli: Cli) -> u8 {
    let printer = Printer::new(cli.color, cli.quiet);
    let result: Result<u8, CliError> = match cli.command {
        Command::Run(args) => commands::run::run(args, printer).await,
        Command::Replay(args) => commands::replay::replay(args, printer).await,
        Command::Demo(args) => commands::demo::demo(args, printer).await,
        Command::Init(args) => commands::init::init(&args, printer),
        Command::Doctor(args) => commands::doctor::doctor(args, printer).await,
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            printer.error(&e.to_string());
            e.exit_code()
        }
    }
}
