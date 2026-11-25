//! Helpers for PostgreSQL connection URLs.

use crate::error::{PgCtrlError, PgCtrlResult};

/// Components of a `postgres://` URL that PgLoom needs to rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgUrl {
    scheme: String,
    userinfo: Option<String>,
    pub host: String,
    pub port: u16,
    /// Path and query, including the leading `/` (may be empty).
    rest: String,
}

impl PgUrl {
    /// Parses `postgres://[user[:password]@]host[:port][/db][?params]`.
    pub fn parse(url: &str) -> PgCtrlResult<Self> {
        let invalid =
            |reason: &str| PgCtrlError::Config(format!("invalid PostgreSQL URL: {reason}"));
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| invalid("expected postgres://user:password@host:port/database"))?;
        if scheme != "postgres" && scheme != "postgresql" {
            return Err(invalid("scheme must be postgres:// or postgresql://"));
        }
        let (authority, tail) = match rest.find(['/', '?']) {
            Some(i) => rest.split_at(i),
            None => (rest, ""),
        };
        let (userinfo, hostport) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u.to_owned()), h),
            None => (None, authority),
        };
        if hostport.contains(',') {
            return Err(invalid("multi-host URLs are not supported"));
        }
        let (host, port) = if let Some(stripped) = hostport.strip_prefix('[') {
            let (h, after) = stripped
                .split_once(']')
                .ok_or_else(|| invalid("unterminated IPv6 host"))?;
            let port = after.strip_prefix(':').unwrap_or("");
            (format!("[{h}]"), port)
        } else {
            match hostport.rsplit_once(':') {
                Some((h, p)) => (h.to_owned(), p),
                None => (hostport.to_owned(), ""),
            }
        };
        if host.is_empty() {
            return Err(invalid(
                "missing host (Unix-domain sockets are not supported)",
            ));
        }
        let port = if port.is_empty() {
            5432
        } else {
            port.parse().map_err(|_| invalid("port is not a number"))?
        };
        Ok(Self {
            scheme: scheme.to_owned(),
            userinfo,
            host,
            port,
            rest: tail.to_owned(),
        })
    }

    /// `host:port` suitable for `TcpStream::connect`.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// The same URL pointed at another host and port, e.g. the PgLoom proxy.
    pub fn with_host_port(&self, host: &str, port: u16) -> Self {
        Self {
            host: host.to_owned(),
            port,
            ..self.clone()
        }
    }

    /// The same URL with `sslmode=disable` (the proxy hop is plaintext).
    pub fn with_ssl_disabled(&self) -> Self {
        let mut out = self.clone();
        let (path, query) = match out.rest.split_once('?') {
            Some((p, q)) => (p.to_owned(), q.to_owned()),
            None => (out.rest.clone(), String::new()),
        };
        let mut params: Vec<String> = query
            .split('&')
            .filter(|kv| {
                if kv.is_empty() {
                    return false;
                }
                let key = kv.split_once('=').map_or(*kv, |(k, _)| k);
                !key.eq_ignore_ascii_case("sslmode")
                    && !key.eq_ignore_ascii_case("channel_binding")
                    && !key.eq_ignore_ascii_case("sslrootcert")
                    && !key.eq_ignore_ascii_case("sslcert")
                    && !key.eq_ignore_ascii_case("sslkey")
                    && !key.eq_ignore_ascii_case("sslpassword")
                    && !key.eq_ignore_ascii_case("sslsni")
            })
            .map(str::to_owned)
            .collect();
        params.push("sslmode=disable".into());
        out.rest = format!("{path}?{}", params.join("&"));
        out
    }

    /// The URL with the password replaced by `***`, for logs and reports.
    pub fn redacted(&self) -> String {
        let mut out = self.clone();
        if let Some(u) = &self.userinfo
            && let Some((user, _)) = u.split_once(':')
        {
            out.userinfo = Some(format!("{user}:***"));
        }
        out.to_string()
    }
}

impl std::fmt::Display for PgUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://", self.scheme)?;
        if let Some(u) = &self.userinfo {
            write!(f, "{u}@")?;
        }
        write!(f, "{}:{}{}", self.host, self.port, self.rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_rewrites_urls() {
        let u =
            PgUrl::parse("postgres://app:s3cret@db.local:6543/app_test?sslmode=require").unwrap();
        assert_eq!(u.host, "db.local");
        assert_eq!(u.port, 6543);
        assert_eq!(u.socket_addr(), "db.local:6543");
        let proxied = u.with_host_port("127.0.0.1", 15432).with_ssl_disabled();
        assert_eq!(
            proxied.to_string(),
            "postgres://app:s3cret@127.0.0.1:15432/app_test?sslmode=disable"
        );
        assert_eq!(
            u.redacted(),
            "postgres://app:***@db.local:6543/app_test?sslmode=require"
        );
    }

    #[test]
    fn defaults_port_and_handles_ipv6() {
        let u = PgUrl::parse("postgresql://localhost/db").unwrap();
        assert_eq!(u.port, 5432);
        let u = PgUrl::parse("postgres://u@[::1]:5433").unwrap();
        assert_eq!(u.host, "[::1]");
        assert_eq!(u.port, 5433);
        assert_eq!(u.to_string(), "postgres://u@[::1]:5433");
    }

    #[test]
    fn rejects_unsupported_forms() {
        for bad in [
            "host=localhost dbname=x",
            "mysql://localhost/x",
            "postgres:///db?host=/tmp",
            "postgres://a:1,b:2/db",
            "postgres://localhost:notaport/db",
        ] {
            assert!(PgUrl::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn with_ssl_disabled_strips_channel_binding_and_mixed_case_ssl_params() {
        let u = PgUrl::parse(
            "postgres://u:p@ep.neon.tech/db?SSLMODE=verify-full&Channel_Binding=require&\
             sslrootcert=/etc/ca.pem&sslcert=/c.pem&sslkey=/k.pem&sslpassword=pw&sslsni=1&\
             application_name=wallet",
        )
        .unwrap();
        let proxied = u.with_host_port("127.0.0.1", 15432).with_ssl_disabled();
        assert_eq!(
            proxied.to_string(),
            "postgres://u:p@127.0.0.1:15432/db?application_name=wallet&sslmode=disable"
        );
    }
}
