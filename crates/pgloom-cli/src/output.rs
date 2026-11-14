//! Terminal output. The report goes to stdout; progress and diagnostics go
//! to stderr so that `--format json` output stays machine-readable.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pgloom_engine::RunControl;
use tokio::task::JoinHandle;

use crate::cli::ColorChoice;

/// A live "schedules explored" line on stderr, shown only on a terminal and
/// only once a run takes long enough to need it.
#[derive(Debug)]
pub struct ProgressTicker {
    handle: Option<JoinHandle<()>>,
    shown: Arc<std::sync::atomic::AtomicBool>,
}

impl ProgressTicker {
    /// Stops the ticker and erases its line.
    pub async fn finish(mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
            let _ = h.await;
        }
        if self.shown.load(std::sync::atomic::Ordering::SeqCst) {
            eprint!("\r\x1b[2K");
            let _ = std::io::stderr().flush();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Printer {
    pub color: bool,
    pub quiet: bool,
}

impl Printer {
    pub fn new(choice: ColorChoice, quiet: bool) -> Self {
        let color = match choice {
            ColorChoice::Always => true,
            ColorChoice::Never => false,
            ColorChoice::Auto => {
                std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
            }
        };
        Self { color, quiet }
    }

    /// The report (always printed).
    pub fn report(&self, text: &str) {
        print!("{text}");
        if !text.ends_with('\n') {
            println!();
        }
    }

    /// Starts a progress line for a running exploration.
    pub fn spawn_progress(&self, control: Arc<RunControl>) -> ProgressTicker {
        let shown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if self.quiet || !std::io::stderr().is_terminal() {
            return ProgressTicker {
                handle: None,
                shown,
            };
        }
        let flag = shown.clone();
        let printer = *self;
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tick.tick().await;
                let done = control.schedules();
                let secs = started.elapsed().as_secs_f64().max(0.001);
                let failures = match control.failures() {
                    0 => String::new(),
                    n => format!(" · {n} failing"),
                };
                let line = format!(
                    "  … {done} schedules explored ({:.0}/s){failures}",
                    done as f64 / secs
                );
                eprint!("\r\x1b[2K{}", printer.dim(&line));
                let _ = std::io::stderr().flush();
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        ProgressTicker {
            handle: Some(handle),
            shown,
        }
    }

    /// Progress lines, suppressed by `--quiet`.
    pub fn progress(&self, text: &str) {
        if !self.quiet {
            eprintln!("{}", self.dim(text));
        }
    }

    pub fn error(&self, text: &str) {
        let label = if self.color {
            "\x1b[1;31merror:\x1b[0m"
        } else {
            "error:"
        };
        eprintln!("{label} {text}");
    }

    pub fn warn(&self, text: &str) {
        let label = if self.color {
            "\x1b[1;33mwarning:\x1b[0m"
        } else {
            "warning:"
        };
        eprintln!("{label} {text}");
    }

    pub fn dim(&self, text: &str) -> String {
        if self.color {
            format!("\x1b[2m{text}\x1b[0m")
        } else {
            text.to_owned()
        }
    }

    pub fn paint(&self, text: &str, code: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_owned()
        }
    }
}
