//! Terminal styling and display-width aware text layout.

use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Style {
    Plain,
    Bold,
    Dim,
    Ok,
    Error,
    Lock,
    Switch,
}

impl Style {
    fn code(self) -> Option<&'static str> {
        match self {
            Style::Plain => None,
            Style::Bold => Some("1"),
            Style::Dim => Some("2"),
            Style::Ok => Some("32"),
            Style::Error => Some("31"),
            Style::Lock => Some("35"),
            Style::Switch => Some("33"),
        }
    }
}

/// Wraps `text` in ANSI escapes when `color` is on.
pub(crate) fn paint(text: &str, style: Style, color: bool) -> String {
    match style.code() {
        Some(code) if color && !text.is_empty() => format!("\x1b[{code}m{text}\x1b[0m"),
        _ => text.to_owned(),
    }
}

pub(crate) fn width(s: &str) -> usize {
    UnicodeWidthStr::width(s)
}

/// Pads (or truncates with `…`) to exactly `w` display columns.
pub(crate) fn fit(s: &str, w: usize) -> String {
    let sw = width(s);
    if sw <= w {
        let mut out = s.to_owned();
        out.extend(std::iter::repeat_n(' ', w - sw));
        return out;
    }
    let mut out = String::new();
    let mut used = 0;
    for ch in s.chars() {
        let cw = width(ch.encode_utf8(&mut [0; 4]));
        if used + cw + 1 > w {
            break;
        }
        out.push(ch);
        used += cw;
    }
    out.push('…');
    used += 1;
    out.extend(std::iter::repeat_n(' ', w.saturating_sub(used)));
    out
}

/// Centers `s` within `w` display columns.
pub(crate) fn center(s: &str, w: usize) -> String {
    let sw = width(s);
    if sw >= w {
        return fit(s, w);
    }
    let left = (w - sw) / 2;
    let mut out = " ".repeat(left);
    out.push_str(s);
    out.extend(std::iter::repeat_n(' ', w - sw - left));
    out
}

/// Greedy word wrap by display width; words wider than `w` are split.
pub(crate) fn wrap(text: &str, w: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        let mut word = word.to_owned();
        loop {
            let need = if line.is_empty() {
                width(&word)
            } else {
                width(&line) + 1 + width(&word)
            };
            if need <= w {
                if !line.is_empty() {
                    line.push(' ');
                }
                line.push_str(&word);
                break;
            }
            if !line.is_empty() {
                lines.push(std::mem::take(&mut line));
                continue;
            }
            let (head, tail) = split_at_width(&word, w);
            lines.push(head);
            word = tail;
            if word.is_empty() {
                break;
            }
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

fn split_at_width(s: &str, w: usize) -> (String, String) {
    let mut used = 0;
    for (i, ch) in s.char_indices() {
        let cw = width(ch.encode_utf8(&mut [0; 4]));
        if used + cw > w && i > 0 {
            return (s[..i].to_owned(), s[i..].to_owned());
        }
        used += cw;
    }
    (s.to_owned(), String::new())
}

/// Collapses runs of whitespace (including newlines) into single spaces.
pub(crate) fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_helpers_respect_display_width() {
        assert_eq!(fit("abc", 5), "abc  ");
        assert_eq!(fit("abcdef", 4), "abc…");
        assert_eq!(width(&fit("🔒 lock", 4)), 4);
        assert_eq!(center("t1", 6), "  t1  ");
        assert_eq!(
            wrap("SELECT balance FROM wallets WHERE id = 1", 16),
            vec!["SELECT balance", "FROM wallets", "WHERE id = 1"]
        );
        assert_eq!(wrap("abcdefghij", 4), vec!["abcd", "efgh", "ij"]);
        assert_eq!(one_line("a\n   b\tc"), "a b c");
        assert_eq!(paint("x", Style::Ok, false), "x");
        assert_eq!(paint("x", Style::Ok, true), "\x1b[32mx\x1b[0m");
    }
}
