use std::io;

/// Errors raised while decoding PostgreSQL v3 traffic or running the proxy.
///
/// Every malformed byte sequence surfaces as a typed error; the codec never
/// panics on untrusted input.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("frame length {len} is invalid for message '{tag}'")]
    InvalidLength { tag: char, len: i32 },

    #[error("frame length {len} exceeds the {max}-byte limit")]
    FrameTooLarge { len: usize, max: usize },

    #[error("unsupported startup code {0:#010x}")]
    UnsupportedStartupCode(i32),

    #[error("unsupported protocol version {major}.{minor}")]
    UnsupportedProtocol { major: u16, minor: u16 },

    #[error("malformed '{tag}' message: {reason}")]
    Malformed { tag: char, reason: &'static str },

    #[error("connection closed by {0} in the middle of a message")]
    UnexpectedEof(Peer),

    #[error("upstream connection failed: {0}")]
    Upstream(String),
}

/// The side of a proxied connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Peer {
    Client,
    Server,
}

impl std::fmt::Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Peer::Client => "client",
            Peer::Server => "server",
        })
    }
}

pub type WireResult<T> = Result<T, WireError>;
