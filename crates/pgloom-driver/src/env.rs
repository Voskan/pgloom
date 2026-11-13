//! `${VAR}` / `${VAR:-default}` interpolation, as in shells and Compose files.

/// Replaces `${NAME}` and `${NAME:-default}`; `$${` yields a literal `${`.
/// Any other `$` (SQL placeholders such as `$1`, `$$` or `$tag$` quotes) is kept.
/// Fails listing every referenced variable that is unset and has no default.
pub fn interpolate(src: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String, String> {
    let mut out = String::with_capacity(src.len());
    let mut missing: Vec<String> = Vec::new();
    let mut rest = src;
    while let Some(i) = rest.find('$') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        if let Some(stripped) = after.strip_prefix("${") {
            out.push_str("${");
            rest = stripped;
            continue;
        }
        if let Some(stripped) = after.strip_prefix('$') {
            out.push_str("$$");
            rest = stripped;
            continue;
        }
        let Some(body_start) = after.strip_prefix('{') else {
            out.push('$');
            rest = after;
            continue;
        };
        let Some(end) = body_start.find('}') else {
            return Err("unterminated `${` in scenario file".into());
        };
        let body = &body_start[..end];
        let (name, default) = match body.split_once(":-") {
            Some((n, d)) => (n, Some(d)),
            None => (body, None),
        };
        let valid = !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid {
            return Err(format!("invalid variable reference `${{{body}}}`"));
        }
        match lookup(name).filter(|v| !v.is_empty()) {
            Some(v) => out.push_str(&v),
            None => match default {
                Some(d) => out.push_str(d),
                None => {
                    if !missing.iter().any(|m| m == name) {
                        missing.push(name.to_owned());
                    }
                }
            },
        }
        rest = &body_start[end + 1..];
    }
    out.push_str(rest);
    if missing.is_empty() {
        Ok(out)
    } else {
        Err(format!(
            "environment variable(s) not set and without default: {}",
            missing.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(k: &str) -> Option<String> {
        match k {
            "HOST" => Some("db".into()),
            "EMPTY" => Some(String::new()),
            _ => None,
        }
    }

    #[test]
    fn substitutes_and_defaults() {
        assert_eq!(
            interpolate("${HOST}:${PORT:-5432}", env).unwrap(),
            "db:5432"
        );
        assert_eq!(interpolate("${EMPTY:-fallback}", env).unwrap(), "fallback");
        assert_eq!(interpolate("${PORT:-}", env).unwrap(), "");
    }

    #[test]
    fn leaves_sql_dollars_alone() {
        let sql = "UPDATE t SET x = $1; DO $$ BEGIN END $$; SELECT $tag$a$tag$";
        assert_eq!(interpolate(sql, env).unwrap(), sql);
        assert_eq!(
            interpolate("lit: $${HOST} ${HOST}", env).unwrap(),
            "lit: ${HOST} db"
        );
    }

    #[test]
    fn reports_all_missing_variables() {
        let err = interpolate("${A} ${B} ${A}", env).unwrap_err();
        assert!(err.ends_with("A, B"), "{err}");
    }

    #[test]
    fn rejects_malformed_references() {
        assert!(interpolate("${", env).is_err());
        assert!(interpolate("${bad-name}", env).is_err());
    }
}
