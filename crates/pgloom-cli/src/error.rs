//! Process exit codes and the errors that map onto them.

use pgloom_engine::EngineError;

/// All explored schedules satisfied every invariant.
pub const EXIT_PASS: u8 = 0;
/// A race, lost update, invariant failure or deadlock was found.
pub const EXIT_VIOLATION: u8 = 1;
/// Invalid scenario, unreachable database or target service.
pub const EXIT_CONFIG: u8 = 2;
/// Protocol error, step timeout or other unexpected failure.
pub const EXIT_INTERNAL: u8 = 3;

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// The scenario file or command-line arguments are invalid.
    #[error("{0}")]
    Config(String),
    /// The database, the proxy port or the target service is unavailable.
    #[error("{0}")]
    Environment(String),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("{0}")]
    Internal(String),
}

impl CliError {
    pub fn exit_code(&self) -> u8 {
        match self {
            CliError::Config(_) | CliError::Environment(_) => EXIT_CONFIG,
            CliError::Engine(_) | CliError::Internal(_) => EXIT_INTERNAL,
        }
    }

    pub fn env(context: &str, err: impl std::fmt::Display) -> Self {
        CliError::Environment(format!("{context}: {err}"))
    }
}

impl From<pgloom_driver::ConfigError> for CliError {
    fn from(e: pgloom_driver::ConfigError) -> Self {
        CliError::Config(e.to_string())
    }
}

impl From<pgloom_report::ReproError> for CliError {
    fn from(e: pgloom_report::ReproError) -> Self {
        CliError::Config(e.to_string())
    }
}
