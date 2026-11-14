//! Best-effort rendering of parameter and column values for human reports.
//!
//! Drivers such as `pgx` and `tokio-postgres` send many values in binary
//! format. The renderer understands the common scalar types and degrades to a
//! byte-count placeholder for everything else; it never fails.

use crate::messages::FormatCode;

pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const CHAR: u32 = 18;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// Maximum rendered length of a single value.
const MAX_RENDERED: usize = 64;

/// Renders a value; `None` renders as `NULL`.
pub fn render_value(value: Option<&[u8]>, format: FormatCode, type_oid: u32) -> String {
    let Some(bytes) = value else {
        return "NULL".to_owned();
    };
    let rendered = match format {
        FormatCode::Text => String::from_utf8_lossy(bytes).into_owned(),
        FormatCode::Binary => render_binary(bytes, type_oid),
    };
    truncate(escape_controls(rendered))
}

/// Keeps reports printable: raw control bytes would corrupt the timeline and
/// make tools such as `grep` treat a whole CI log as binary.
fn escape_controls(s: String) -> String {
    if !s.chars().any(char::is_control) {
        return s;
    }
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\x{:02x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out
}

/// Microseconds or days since 2000-01-01, PostgreSQL's binary epoch.
fn render_date(days_since_2000: i64) -> String {
    // Howard Hinnant's civil_from_days, shifted from the Unix epoch.
    let z = days_since_2000 + 10_957 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

fn render_timestamp(micros: i64, utc: bool) -> String {
    let days = micros.div_euclid(86_400_000_000);
    let rest = micros.rem_euclid(86_400_000_000);
    let (secs, frac) = (rest / 1_000_000, rest % 1_000_000);
    let time = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    let frac = if frac == 0 {
        String::new()
    } else {
        format!(".{frac:06}").trim_end_matches('0').to_owned()
    };
    let zone = if utc { "+00" } else { "" };
    format!("{} {time}{frac}{zone}", render_date(days))
}

fn render_binary(b: &[u8], type_oid: u32) -> String {
    let fallback = || format!("<{} bytes>", b.len());
    match type_oid {
        oid::BOOL if b.len() == 1 => (b[0] != 0).to_string(),
        oid::INT2 if b.len() == 2 => i16::from_be_bytes([b[0], b[1]]).to_string(),
        oid::INT4 | oid::OID if b.len() == 4 => {
            i32::from_be_bytes([b[0], b[1], b[2], b[3]]).to_string()
        }
        oid::INT8 if b.len() == 8 => i64::from_be_bytes(arr8(b)).to_string(),
        oid::FLOAT4 if b.len() == 4 => f32::from_be_bytes([b[0], b[1], b[2], b[3]]).to_string(),
        oid::FLOAT8 if b.len() == 8 => f64::from_be_bytes(arr8(b)).to_string(),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR | oid::JSON => {
            String::from_utf8_lossy(b).into_owned()
        }
        oid::JSONB if !b.is_empty() => String::from_utf8_lossy(&b[1..]).into_owned(),
        oid::UUID if b.len() == 16 => render_uuid(b),
        oid::NUMERIC => render_numeric(b).unwrap_or_else(fallback),
        oid::BYTEA => format!("\\x{}", hex(b)),
        oid::DATE if b.len() == 4 => {
            render_date(i64::from(i32::from_be_bytes([b[0], b[1], b[2], b[3]])))
        }
        oid::TIMESTAMP if b.len() == 8 => render_timestamp(i64::from_be_bytes(arr8(b)), false),
        oid::TIMESTAMPTZ if b.len() == 8 => render_timestamp(i64::from_be_bytes(arr8(b)), true),
        // Unknown OID (0 means "let the server infer"): guess common widths.
        0 => match b.len() {
            4 => i32::from_be_bytes([b[0], b[1], b[2], b[3]]).to_string(),
            8 => i64::from_be_bytes(arr8(b)).to_string(),
            _ if b.iter().all(|c| c.is_ascii_graphic() || *c == b' ') => {
                String::from_utf8_lossy(b).into_owned()
            }
            _ => fallback(),
        },
        _ => fallback(),
    }
}

fn arr8(b: &[u8]) -> [u8; 8] {
    let mut out = [0u8; 8];
    out.copy_from_slice(&b[..8]);
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn render_uuid(b: &[u8]) -> String {
    let h = hex(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// Decodes PostgreSQL's binary NUMERIC: base-10000 digits with a weight.
fn render_numeric(b: &[u8]) -> Option<String> {
    let rd = |i: usize| -> Option<i16> { Some(i16::from_be_bytes([*b.get(i)?, *b.get(i + 1)?])) };
    let ndigits = usize::try_from(rd(0)?).ok()?;
    let weight = rd(2)? as i32;
    let sign = rd(4)? as u16;
    let dscale = usize::try_from(rd(6)?).ok()?;
    match sign {
        0xC000 => return Some("NaN".into()),
        0xD000 => return Some("Infinity".into()),
        0xF000 => return Some("-Infinity".into()),
        _ => {}
    }
    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        digits.push(rd(8 + 2 * i)?);
    }
    let mut int_part = String::new();
    for w in 0..=weight.max(-1) {
        let d = digits.get(w as usize).copied().unwrap_or(0);
        if int_part.is_empty() {
            int_part.push_str(&d.to_string());
        } else {
            int_part.push_str(&format!("{d:04}"));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }
    let mut frac = String::new();
    if dscale > 0 {
        let mut idx = weight + 1;
        while frac.len() < dscale {
            let d = if idx < 0 {
                0
            } else {
                digits.get(idx as usize).copied().unwrap_or(0)
            };
            frac.push_str(&format!("{d:04}"));
            idx += 1;
        }
        frac.truncate(dscale);
    }
    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(&int_part);
    if !frac.is_empty() {
        out.push('.');
        out.push_str(&frac);
    }
    Some(out)
}

fn truncate(mut s: String) -> String {
    if s.chars().count() > MAX_RENDERED {
        s = s.chars().take(MAX_RENDERED - 1).collect();
        s.push('…');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_and_timestamps_decode_from_the_2000_epoch() {
        assert_eq!(render_date(0), "2000-01-01");
        assert_eq!(render_date(-1), "1999-12-31");
        assert_eq!(render_date(9_445), "2025-11-10");
        let micros = (9_445_i64 * 86_400 + 13 * 3600 + 5 * 60 + 7) * 1_000_000 + 250_000;
        assert_eq!(
            render_value(
                Some(&micros.to_be_bytes()),
                FormatCode::Binary,
                oid::TIMESTAMPTZ
            ),
            "2025-11-10 13:05:07.25+00"
        );
    }

    #[test]
    fn control_bytes_are_escaped_not_printed() {
        assert_eq!(
            render_value(Some(b"\0\0\0\x01"), FormatCode::Text, 0),
            "\\x00\\x00\\x00\\x01"
        );
        assert_eq!(render_value(Some(b"a\nb"), FormatCode::Text, 25), "a\\nb");
    }

    #[test]
    fn renders_text_and_null() {
        assert_eq!(render_value(None, FormatCode::Text, 0), "NULL");
        assert_eq!(
            render_value(Some(b"100"), FormatCode::Text, oid::INT8),
            "100"
        );
    }

    #[test]
    fn renders_binary_scalars() {
        let b = FormatCode::Binary;
        assert_eq!(render_value(Some(&42i32.to_be_bytes()), b, oid::INT4), "42");
        assert_eq!(
            render_value(Some(&(-7i64).to_be_bytes()), b, oid::INT8),
            "-7"
        );
        assert_eq!(render_value(Some(&[1]), b, oid::BOOL), "true");
        assert_eq!(render_value(Some(b"abc"), b, oid::TEXT), "abc");
        assert_eq!(render_value(Some(&[1, 2]), b, 9999), "<2 bytes>");
        assert_eq!(render_value(Some(&5i64.to_be_bytes()), b, 0), "5");
    }

    #[test]
    fn renders_binary_numeric() {
        // 12345.67 => digits [1, 2345, 6700], weight 1, dscale 2
        let mut buf = Vec::new();
        for v in [3i16, 1, 0, 2, 1, 2345, 6700] {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        assert_eq!(
            render_value(Some(&buf), FormatCode::Binary, oid::NUMERIC),
            "12345.67"
        );
        // -0.05 => digits [500], weight -1, sign neg, dscale 2
        let mut buf = Vec::new();
        for v in [1i16, -1, 0x4000u16 as i16, 2, 500] {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        assert_eq!(
            render_value(Some(&buf), FormatCode::Binary, oid::NUMERIC),
            "-0.05"
        );
    }

    #[test]
    fn renders_uuid_and_truncates_long_values() {
        let id: Vec<u8> = (0u8..16).collect();
        assert_eq!(
            render_value(Some(&id), FormatCode::Binary, oid::UUID),
            "00010203-0405-0607-0809-0a0b0c0d0e0f"
        );
        let long = "x".repeat(200);
        let out = render_value(Some(long.as_bytes()), FormatCode::Text, oid::TEXT);
        assert_eq!(out.chars().count(), MAX_RENDERED);
        assert!(out.ends_with('…'));
    }
}
