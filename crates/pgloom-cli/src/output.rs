//! Terminal output. The report goes to stdout; progress and diagnostics go
//! to stderr so that `--format json` output stays machine-readable.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::io::IsTerminal;

use crate::cli::ColorChoice;

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
