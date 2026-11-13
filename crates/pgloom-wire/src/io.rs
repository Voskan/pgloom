//! Async frame readers over byte streams.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{Peer, WireError, WireResult};
use crate::frame::{Frame, StartupPacket, decode_frame, decode_startup};

const READ_CHUNK: usize = 16 * 1024;

/// Buffered, cancel-safe reader of protocol messages.
///
/// `next` and `next_startup` only consume bytes from the internal buffer once
/// a full message is available, and `read_buf` never loses data when its
/// future is dropped, so both may be used inside `tokio::select!`.
#[derive(Debug)]
pub struct FrameReader<R> {
    rd: R,
    buf: BytesMut,
    peer: Peer,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(rd: R, peer: Peer) -> Self {
        Self {
            rd,
            buf: BytesMut::with_capacity(READ_CHUNK),
            peer,
        }
    }

    /// Reads the next tagged message; `Ok(None)` on a clean EOF.
    pub async fn next(&mut self) -> WireResult<Option<Frame>> {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf)? {
                return Ok(Some(frame));
            }
            if !self.fill().await? {
                return self.eof();
            }
        }
    }

    /// Reads the next untagged startup-phase packet; `Ok(None)` on a clean EOF.
    pub async fn next_startup(&mut self) -> WireResult<Option<StartupPacket>> {
        loop {
            if let Some(packet) = decode_startup(&mut self.buf)? {
                return Ok(Some(packet));
            }
            if !self.fill().await? {
                return self.eof();
            }
        }
    }

    /// Reads a single raw byte (e.g. the server's answer to `SSLRequest`).
    pub async fn read_u8(&mut self) -> WireResult<Option<u8>> {
        if self.buf.is_empty() && !self.fill().await? {
            return Ok(None);
        }
        let b = self.buf[0];
        bytes::Buf::advance(&mut self.buf, 1);
        Ok(Some(b))
    }

    /// True when a complete message is already buffered, i.e. the next call
    /// to [`FrameReader::next`] will not wait for the network.
    pub fn has_buffered_frame(&self) -> bool {
        if self.buf.len() < 5 {
            return false;
        }
        let len = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        len >= 4 && self.buf.len() > len as usize
    }

    async fn fill(&mut self) -> WireResult<bool> {
        if self.buf.capacity() - self.buf.len() < 1024 {
            self.buf.reserve(READ_CHUNK);
        }
        Ok(self.rd.read_buf(&mut self.buf).await? > 0)
    }

    fn eof<T>(&self) -> WireResult<Option<T>> {
        if self.buf.is_empty() {
            Ok(None)
        } else {
            Err(WireError::UnexpectedEof(self.peer))
        }
    }
}
