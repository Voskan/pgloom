//! Incremental framing for the PostgreSQL v3 protocol.
//!
//! Regular messages are `[tag: u8][len: i32 BE][payload: len - 4]`, where `len`
//! counts itself but not the tag. Startup-phase packets carry no tag:
//! `[len: i32 BE][code: i32 BE][payload]`. Both decoders consume bytes from a
//! `BytesMut` only once a complete message is buffered, so they are safe to
//! drive with arbitrarily fragmented TCP reads.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{WireError, WireResult};

/// Upper bound for a single message. PostgreSQL itself caps messages at 1 GiB.
pub const MAX_FRAME_LEN: usize = 1 << 30;

/// Startup packets are tiny; anything larger is a protocol violation.
const MAX_STARTUP_LEN: usize = 10_000;

pub const SSL_REQUEST_CODE: i32 = 80_877_103;
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
pub const PROTOCOL_V3: i32 = 196_608;

/// A complete tagged protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub tag: u8,
    /// Payload without the tag and the length prefix.
    pub body: Bytes,
}

impl Frame {
    pub fn new(tag: u8, body: impl Into<Bytes>) -> Self {
        Self {
            tag,
            body: body.into(),
        }
    }

    /// Total encoded size on the wire.
    pub fn wire_len(&self) -> usize {
        1 + 4 + self.body.len()
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(self.wire_len());
        dst.put_u8(self.tag);
        // Frames are only built from decoded input (bounded by MAX_FRAME_LEN)
        // or from small locally constructed messages, so this cannot truncate.
        dst.put_i32((self.body.len() + 4) as i32);
        dst.put_slice(&self.body);
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.wire_len());
        self.encode(&mut buf);
        buf.freeze()
    }

    pub fn tag_char(&self) -> char {
        char::from(self.tag)
    }
}

/// Maximum bytes reserved per incremental read while waiting for a large frame.
const MAX_RESERVE_STEP: usize = 64 * 1024;

/// Decodes one tagged message from `src`, or returns `Ok(None)` when more
/// bytes are needed.
pub fn decode_frame(src: &mut BytesMut) -> WireResult<Option<Frame>> {
    if src.len() < 5 {
        return Ok(None);
    }
    let tag = src[0];
    let len = i32::from_be_bytes([src[1], src[2], src[3], src[4]]);
    if len < 4 {
        return Err(WireError::InvalidLength {
            tag: char::from(tag),
            len,
        });
    }
    let len = len as usize;
    if len > MAX_FRAME_LEN {
        return Err(WireError::FrameTooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let total = 1 + len;
    if src.len() < total {
        src.reserve((total - src.len()).min(MAX_RESERVE_STEP));
        return Ok(None);
    }
    let mut frame = src.split_to(total);
    frame.advance(5);
    Ok(Some(Frame {
        tag,
        body: frame.freeze(),
    }))
}

/// Untagged packets a client may send before the regular message flow starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupPacket {
    SslRequest,
    GssEncRequest,
    CancelRequest {
        backend_pid: i32,
        secret_key: Bytes,
    },
    Startup {
        /// The complete packet, including the length prefix, for verbatim forwarding.
        raw: Bytes,
        params: Vec<(String, String)>,
    },
}

impl StartupPacket {
    pub fn param(&self, key: &str) -> Option<&str> {
        match self {
            StartupPacket::Startup { params, .. } => params
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str()),
            _ => None,
        }
    }
}

/// Decodes one untagged startup-phase packet.
pub fn decode_startup(src: &mut BytesMut) -> WireResult<Option<StartupPacket>> {
    if src.len() < 8 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    if len < 8 || len as usize > MAX_STARTUP_LEN {
        return Err(WireError::InvalidLength { tag: '\0', len });
    }
    let len = len as usize;
    if src.len() < len {
        return Ok(None);
    }
    let raw = src.split_to(len).freeze();
    let code = i32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
    match code {
        SSL_REQUEST_CODE => Ok(Some(StartupPacket::SslRequest)),
        GSSENC_REQUEST_CODE => Ok(Some(StartupPacket::GssEncRequest)),
        CANCEL_REQUEST_CODE => {
            // Protocol 3.0 uses a 4-byte key; 3.2 allows up to 256 bytes.
            if len < 16 {
                return Err(WireError::Malformed {
                    tag: '\0',
                    reason: "CancelRequest shorter than 16 bytes",
                });
            }
            let backend_pid = i32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);
            Ok(Some(StartupPacket::CancelRequest {
                backend_pid,
                secret_key: raw.slice(12..),
            }))
        }
        _ => {
            let major = (code >> 16) as u16;
            let minor = (code & 0xffff) as u16;
            if major != 3 {
                if (1234..=1235).contains(&major) {
                    return Err(WireError::UnsupportedStartupCode(code));
                }
                return Err(WireError::UnsupportedProtocol { major, minor });
            }
            let params = parse_startup_params(&raw[8..])?;
            Ok(Some(StartupPacket::Startup { raw, params }))
        }
    }
}

fn parse_startup_params(mut body: &[u8]) -> WireResult<Vec<(String, String)>> {
    let mut params = Vec::new();
    loop {
        let key = take_cstr(&mut body, '\0')?;
        if key.is_empty() {
            return Ok(params);
        }
        let value = take_cstr(&mut body, '\0')?;
        params.push((key, value));
    }
}

fn take_cstr(buf: &mut &[u8], tag: char) -> WireResult<String> {
    let nul = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or(WireError::Malformed {
            tag,
            reason: "unterminated string",
        })?;
    let s = String::from_utf8_lossy(&buf[..nul]).into_owned();
    *buf = &buf[nul + 1..];
    Ok(s)
}

/// Builds a StartupMessage packet (used by the test kit and tooling).
pub fn encode_startup(params: &[(&str, &str)]) -> Bytes {
    let mut body = BytesMut::new();
    body.put_i32(PROTOCOL_V3);
    for (k, v) in params {
        body.put_slice(k.as_bytes());
        body.put_u8(0);
        body.put_slice(v.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0);
    let mut out = BytesMut::with_capacity(body.len() + 4);
    out.put_i32((body.len() + 4) as i32);
    out.put_slice(&body);
    out.freeze()
}

/// Builds a fixed 8-byte startup request such as `SSLRequest`.
pub fn encode_startup_code(code: i32) -> Bytes {
    let mut out = BytesMut::with_capacity(8);
    out.put_i32(8);
    out.put_i32(code);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame_bytes(tag: u8, body: &[u8]) -> Vec<u8> {
        Frame::new(tag, Bytes::copy_from_slice(body))
            .to_bytes()
            .to_vec()
    }

    fn feed_in_chunks(bytes: &[u8], chunk: usize) -> Vec<Frame> {
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for piece in bytes.chunks(chunk) {
            buf.extend_from_slice(piece);
            while let Some(f) = decode_frame(&mut buf).unwrap() {
                out.push(f);
            }
        }
        assert!(buf.is_empty(), "leftover bytes: {buf:?}");
        out
    }

    fn sample_stream() -> (Vec<u8>, Vec<Frame>) {
        let frames = vec![
            Frame::new(b'P', Bytes::from_static(b"s1\0SELECT 1\0\0\0")),
            Frame::new(b'B', Bytes::from_static(b"\0s1\0\0\0\0\0\0\0")),
            Frame::new(b'E', Bytes::from_static(b"\0\0\0\0\0")),
            Frame::new(b'S', Bytes::new()),
            Frame::new(b'Q', Bytes::from(vec![b'x'; 5000])),
        ];
        let mut bytes = Vec::new();
        for f in &frames {
            bytes.extend_from_slice(&f.to_bytes());
        }
        (bytes, frames)
    }

    #[test]
    fn decodes_byte_by_byte() {
        let (bytes, frames) = sample_stream();
        assert_eq!(feed_in_chunks(&bytes, 1), frames);
    }

    #[test]
    fn decodes_three_byte_chunks_and_split_length_headers() {
        let (bytes, frames) = sample_stream();
        assert_eq!(feed_in_chunks(&bytes, 3), frames);
        assert_eq!(feed_in_chunks(&bytes, 2), frames);
    }

    #[test]
    fn decodes_coalesced_4k_reads() {
        let (bytes, frames) = sample_stream();
        assert_eq!(feed_in_chunks(&bytes, 4096), frames);
        assert_eq!(feed_in_chunks(&bytes, bytes.len()), frames);
    }

    #[test]
    fn rejects_negative_and_short_lengths() {
        let mut buf = BytesMut::from(&[b'Q', 0xff, 0xff, 0xff, 0xff][..]);
        assert!(matches!(
            decode_frame(&mut buf),
            Err(WireError::InvalidLength { tag: 'Q', .. })
        ));
        let mut buf = BytesMut::from(&[b'Q', 0, 0, 0, 3][..]);
        assert!(decode_frame(&mut buf).is_err());
    }

    #[test]
    fn rejects_oversized_frames_without_allocating() {
        let mut buf = BytesMut::from(&[b'D', 0x7f, 0xff, 0xff, 0xff][..]);
        assert!(matches!(
            decode_frame(&mut buf),
            Err(WireError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn empty_body_frame_roundtrip() {
        let bytes = frame_bytes(b'S', &[]);
        assert_eq!(bytes, vec![b'S', 0, 0, 0, 4]);
        let frames = feed_in_chunks(&bytes, 1);
        assert_eq!(frames, vec![Frame::new(b'S', Bytes::new())]);
    }

    #[test]
    fn decodes_ssl_request_then_startup() {
        let mut stream = BytesMut::new();
        stream.extend_from_slice(&encode_startup_code(SSL_REQUEST_CODE));
        stream.extend_from_slice(&encode_startup(&[("user", "app"), ("database", "db")]));
        let mut buf = BytesMut::new();
        let mut packets = Vec::new();
        for b in stream.iter() {
            buf.extend_from_slice(&[*b]);
            if let Some(p) = decode_startup(&mut buf).unwrap() {
                packets.push(p);
            }
        }
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0], StartupPacket::SslRequest);
        assert_eq!(packets[1].param("user"), Some("app"));
        assert_eq!(packets[1].param("database"), Some("db"));
    }

    #[test]
    fn decodes_gssenc_and_cancel_requests() {
        let mut buf = BytesMut::from(&encode_startup_code(GSSENC_REQUEST_CODE)[..]);
        assert_eq!(
            decode_startup(&mut buf).unwrap(),
            Some(StartupPacket::GssEncRequest)
        );

        let mut cancel = BytesMut::new();
        cancel.put_i32(16);
        cancel.put_i32(CANCEL_REQUEST_CODE);
        cancel.put_i32(4812);
        cancel.put_i32(77);
        match decode_startup(&mut cancel).unwrap() {
            Some(StartupPacket::CancelRequest {
                backend_pid,
                secret_key,
            }) => {
                assert_eq!(backend_pid, 4812);
                assert_eq!(&secret_key[..], &77i32.to_be_bytes());
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_protocol_versions() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(2 << 16);
        assert!(matches!(
            decode_startup(&mut buf),
            Err(WireError::UnsupportedProtocol { major: 2, .. })
        ));
    }
}
