//! Fast database reset between schedules.
//!
//! PgLoom never rewrites `COMMIT` into savepoints: genuine top-level commits
//! are what give `READ COMMITTED` its cross-session visibility. Instead, user
//! tables are copied once into unlogged shadow tables in `_pgloom_snap`, and
//! after each schedule only the tables that were written (plus every table
//! that references them through foreign keys) are truncated and refilled in a
//! single transaction. Owned sequences are reset to their captured positions so
//! generated ids are identical across schedules.

use std::collections::{BTreeMap, BTreeSet};

use tracing::debug;

use crate::control::PgControl;
use crate::error::{PgCtrlError, PgCtrlResult};

pub const SNAPSHOT_SCHEMA: &str = "_pgloom_snap";

/// Tables managed by migration tools; never snapshotted or restored.
const MIGRATION_TABLES: &[&str] = &[
    "_sqlx_migrations",
    "schema_migrations",
    "__diesel_schema_migrations",
    "goose_db_version",
    "flyway_schema_history",
    "ar_internal_metadata",
    "alembic_version",
    "knex_migrations",
    "knex_migrations_lock",
    "_prisma_migrations",
    "atlas_schema_revisions",
    "gorp_migrations",
    "migrations",
];

/// Quotes an identifier the way `quote_ident()` does.
pub fn quote_ident(ident: &str) -> String {
    let plain = !ident.is_empty()
        && ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !is_reserved(ident);
    if plain {
        ident.to_owned()
    } else {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

fn is_reserved(word: &str) -> bool {
    matches!(
        word,
        "all"
            | "analyse"
            | "analyze"
            | "and"
            | "any"
            | "array"
            | "as"
            | "asc"
            | "asymmetric"
            | "both"
            | "case"
            | "cast"
            | "check"
            | "collate"
            | "column"
            | "constraint"
            | "create"
            | "current_catalog"
            | "current_date"
            | "current_role"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "default"
            | "deferrable"
            | "desc"
            | "distinct"
            | "do"
            | "else"
            | "end"
            | "except"
            | "false"
            | "fetch"
            | "for"
            | "foreign"
            | "from"
            | "grant"
            | "group"
            | "having"
            | "in"
            | "initially"
            | "intersect"
            | "into"
            | "lateral"
            | "leading"
            | "limit"
            | "localtime"
            | "localtimestamp"
            | "not"
            | "null"
            | "offset"
            | "on"
            | "only"
            | "or"
            | "order"
            | "placing"
            | "primary"
            | "references"
            | "returning"
            | "select"
            | "session_user"
            | "some"
            | "symmetric"
            | "system_user"
            | "table"
            | "then"
            | "to"
            | "trailing"
            | "true"
            | "union"
            | "unique"
            | "user"
            | "using"
            | "variadic"
            | "when"
            | "where"
            | "window"
            | "with"
    )
}

/// Quotes a string literal.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A captured sequence position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceState {
    /// Qualified, quoted sequence name.
    pub name: String,
    pub last_value: i64,
    pub is_called: bool,
}

/// One snapshotted user table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSnapshot {
    pub oid: i64,
    pub schema: String,
    pub name: String,
    /// Insertable (non-generated) columns, in attribute order.
    pub columns: Vec<String>,
    pub has_user_triggers: bool,
    pub sequences: Vec<SequenceState>,
}

impl TableSnapshot {
    /// `schema.table`, quoted.
    pub fn qualified(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    pub fn shadow(&self) -> String {
        format!("{SNAPSHOT_SCHEMA}.t_{}", self.oid)
    }

    fn column_list(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Options for [`Snapshot::capture`].
#[derive(Debug, Clone, Default)]
pub struct SnapshotOptions {
    /// Extra table names (`table` or `schema.table`) to leave untouched.
    pub exclude: Vec<String>,
}

/// Restore statistics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreStats {
    pub tables: Vec<String>,
    /// Dirty table references that matched no snapshotted table.
    pub unresolved: Vec<String>,
}

/// Reference image of all user tables.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Keyed by OID for deterministic ordering.
    tables: BTreeMap<i64, TableSnapshot>,
    /// `parent -> children` foreign-key edges (OIDs).
    referenced_by: BTreeMap<i64, BTreeSet<i64>>,
}

impl Snapshot {
    /// Builds a snapshot description without touching the database (tests).
    pub fn from_parts(tables: Vec<TableSnapshot>, foreign_keys: &[(i64, i64)]) -> Self {
        let mut s = Snapshot::default();
        for t in tables {
            s.tables.insert(t.oid, t);
        }
        for &(child, parent) in foreign_keys {
            if child != parent && s.tables.contains_key(&child) && s.tables.contains_key(&parent) {
                s.referenced_by.entry(parent).or_default().insert(child);
            }
        }
        s
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableSnapshot> {
        self.tables.values()
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Captures every user table into `_pgloom_snap`.
    pub async fn capture(ctl: &PgControl, options: &SnapshotOptions) -> PgCtrlResult<Self> {
        let client = ctl.client();
        let rows = client
            .query(
                "SELECT c.oid::int8, n.nspname::text, c.relname::text, \
                        EXISTS (SELECT 1 FROM pg_trigger t \
                                WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', '_pgloom_snap') \
                   AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%' \
                 ORDER BY c.oid",
                &[],
            )
            .await
            .map_err(PgCtrlError::query("listing user tables"))?;

        let excluded = |schema: &str, name: &str| {
            MIGRATION_TABLES.contains(&name)
                || options
                    .exclude
                    .iter()
                    .any(|e| e == name || *e == format!("{schema}.{name}"))
        };

        let mut tables: BTreeMap<i64, TableSnapshot> = BTreeMap::new();
        for row in rows {
            let schema: String = row.get(1);
            let name: String = row.get(2);
            if excluded(&schema, &name) {
                continue;
            }
            let oid: i64 = row.get(0);
            tables.insert(
                oid,
                TableSnapshot {
                    oid,
                    schema,
                    name,
                    columns: Vec::new(),
                    has_user_triggers: row.get(3),
                    sequences: Vec::new(),
                },
            );
        }
        let oids: Vec<i64> = tables.keys().copied().collect();

        let cols = client
            .query(
                "SELECT a.attrelid::int8, a.attname::text FROM pg_attribute a \
                 WHERE a.attrelid = ANY($1::int8[]::oid[]) AND a.attnum > 0 \
                   AND NOT a.attisdropped AND a.attgenerated = '' \
                 ORDER BY a.attrelid, a.attnum",
                &[&oids],
            )
            .await
            .map_err(PgCtrlError::query("listing table columns"))?;
        for row in cols {
            let oid: i64 = row.get(0);
            if let Some(t) = tables.get_mut(&oid) {
                t.columns.push(row.get(1));
            }
        }

        let seqs = client
            .query(
                "SELECT d.refobjid::int8, \
                        quote_ident(n.nspname) || '.' || quote_ident(s.relname) \
                 FROM pg_class s \
                 JOIN pg_namespace n ON n.oid = s.relnamespace \
                 JOIN pg_depend d ON d.objid = s.oid \
                   AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass \
                 WHERE s.relkind = 'S' AND d.deptype IN ('a', 'i') \
                   AND d.refobjid = ANY($1::int8[]::oid[]) \
                 ORDER BY s.oid",
                &[&oids],
            )
            .await
            .map_err(PgCtrlError::query("listing owned sequences"))?;
        for row in seqs {
            let oid: i64 = row.get(0);
            let name: String = row.get(1);
            let state = client
                .query_one(&format!("SELECT last_value, is_called FROM {name}"), &[])
                .await
                .map_err(PgCtrlError::query(format!("reading sequence {name}")))?;
            if let Some(t) = tables.get_mut(&oid) {
                t.sequences.push(SequenceState {
                    name,
                    last_value: state.get(0),
                    is_called: state.get(1),
                });
            }
        }

        let fks = client
            .query(
                "SELECT conrelid::int8, confrelid::int8 FROM pg_constraint \
                 WHERE contype = 'f' ORDER BY oid",
                &[],
            )
            .await
            .map_err(PgCtrlError::query("listing foreign keys"))?;
        let fk_pairs: Vec<(i64, i64)> = fks.iter().map(|r| (r.get(0), r.get(1))).collect();

        let snapshot = Snapshot::from_parts(tables.into_values().collect(), &fk_pairs);
        ctl.batch(&snapshot.capture_sql(), "creating snapshot tables")
            .await?;
        debug!(tables = snapshot.len(), "snapshot captured");
        Ok(snapshot)
    }

    /// DDL that (re)creates the shadow schema and copies every table.
    pub fn capture_sql(&self) -> String {
        let mut sql = format!(
            "DROP SCHEMA IF EXISTS {SNAPSHOT_SCHEMA} CASCADE;\nCREATE SCHEMA {SNAPSHOT_SCHEMA};\n"
        );
        for t in self.tables.values() {
            sql.push_str(&format!(
                "CREATE UNLOGGED TABLE {} AS SELECT {} FROM {};\n",
                t.shadow(),
                t.column_list(),
                t.qualified()
            ));
        }
        sql
    }

    /// Maps a table reference as written in SQL to snapshotted tables.
    ///
    /// Unqualified names are resolved like a default `search_path`: a match in
    /// `public` wins; otherwise every schema with that table name matches.
    pub fn resolve(&self, reference: &str) -> Vec<i64> {
        let (schema, name) = match reference.rsplit_once('.') {
            Some((s, n)) => (Some(s), n),
            None => (None, reference),
        };
        let matches: Vec<&TableSnapshot> = self
            .tables
            .values()
            .filter(|t| t.name == name && schema.is_none_or(|s| s == t.schema))
            .collect();
        if schema.is_none()
            && let Some(public) = matches.iter().find(|t| t.schema == "public")
        {
            return vec![public.oid];
        }
        matches.iter().map(|t| t.oid).collect()
    }

    /// Tables to restore for `dirty` references, closed over foreign keys and
    /// widened to every table if a write was opaque or touched a table with
    /// user triggers. Returned parent-first.
    pub fn restore_plan(
        &self,
        dirty: &BTreeSet<String>,
        opaque_writes: bool,
    ) -> (Vec<i64>, Vec<String>) {
        let mut unresolved = Vec::new();
        let mut set: BTreeSet<i64> = BTreeSet::new();
        for reference in dirty {
            let oids = self.resolve(reference);
            if oids.is_empty() {
                unresolved.push(reference.clone());
            }
            set.extend(oids);
        }
        let triggers = set
            .iter()
            .any(|oid| self.tables.get(oid).is_some_and(|t| t.has_user_triggers));
        if opaque_writes || triggers {
            set = self.tables.keys().copied().collect();
        }
        // Children referencing a restored table must be restored as well:
        // truncating a parent requires truncating its referencing tables.
        let mut stack: Vec<i64> = set.iter().copied().collect();
        while let Some(oid) = stack.pop() {
            if let Some(children) = self.referenced_by.get(&oid) {
                for &child in children {
                    if set.insert(child) {
                        stack.push(child);
                    }
                }
            }
        }
        (self.parent_first(&set), unresolved)
    }

    /// Kahn's algorithm over the restore set; ties broken by OID.
    fn parent_first(&self, set: &BTreeSet<i64>) -> Vec<i64> {
        let mut indegree: BTreeMap<i64, usize> = set.iter().map(|&o| (o, 0)).collect();
        for (parent, children) in &self.referenced_by {
            if !set.contains(parent) {
                continue;
            }
            for child in children {
                if let Some(d) = indegree.get_mut(child) {
                    *d += 1;
                }
            }
        }
        let mut ready: BTreeSet<i64> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(o, _)| *o)
            .collect();
        let mut order = Vec::with_capacity(set.len());
        while let Some(oid) = ready.pop_first() {
            order.push(oid);
            indegree.remove(&oid);
            if let Some(children) = self.referenced_by.get(&oid) {
                for child in children {
                    if let Some(d) = indegree.get_mut(child) {
                        *d -= 1;
                        if *d == 0 {
                            ready.insert(*child);
                        }
                    }
                }
            }
        }
        // Foreign-key cycles: append the rest in OID order; PostgreSQL checks
        // non-deferred constraints per statement, so self-contained cycles
        // still restore when each table's rows are self-consistent.
        order.extend(indegree.keys().copied());
        order
    }

    /// The single-transaction restore script for `plan` (parent-first OIDs).
    pub fn restore_sql(&self, plan: &[i64]) -> String {
        let tables: Vec<&TableSnapshot> = plan.iter().filter_map(|o| self.tables.get(o)).collect();
        if tables.is_empty() {
            return String::new();
        }
        // The restore need not survive a crash (the snapshot does), so its
        // commit skips the WAL flush.
        let mut sql = String::from("BEGIN;\nSET LOCAL synchronous_commit = off;\n");
        sql.push_str(&format!(
            "TRUNCATE TABLE {};\n",
            tables
                .iter()
                .map(|t| t.qualified())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        for t in &tables {
            if t.columns.is_empty() {
                continue;
            }
            let cols = t.column_list();
            sql.push_str(&format!(
                "INSERT INTO {} ({cols}) OVERRIDING SYSTEM VALUE SELECT {cols} FROM {};\n",
                t.qualified(),
                t.shadow()
            ));
        }
        for t in &tables {
            for s in &t.sequences {
                sql.push_str(&format!(
                    "SELECT setval({}, {}, {});\n",
                    quote_literal(&s.name),
                    s.last_value,
                    s.is_called
                ));
            }
        }
        sql.push_str("COMMIT;\n");
        sql
    }

    /// Restores the tables touched by an episode.
    pub async fn restore(
        &self,
        ctl: &PgControl,
        dirty: &BTreeSet<String>,
        opaque_writes: bool,
    ) -> PgCtrlResult<RestoreStats> {
        let (plan, unresolved) = self.restore_plan(dirty, opaque_writes);
        let sql = self.restore_sql(&plan);
        if !sql.is_empty()
            && let Err(err) = ctl.batch(&sql, "restoring snapshot").await
        {
            let _ = ctl.batch("ROLLBACK", "rolling back failed restore").await;
            return Err(err);
        }
        Ok(RestoreStats {
            tables: plan
                .iter()
                .filter_map(|o| self.tables.get(o).map(TableSnapshot::qualified))
                .collect(),
            unresolved,
        })
    }

    /// Restores every snapshotted table.
    pub async fn restore_all(&self, ctl: &PgControl) -> PgCtrlResult<RestoreStats> {
        self.restore(ctl, &BTreeSet::new(), true).await
    }

    /// Drops the shadow schema.
    pub async fn discard(ctl: &PgControl) -> PgCtrlResult<()> {
        ctl.batch(
            &format!("DROP SCHEMA IF EXISTS {SNAPSHOT_SCHEMA} CASCADE"),
            "dropping snapshot schema",
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(oid: i64, schema: &str, name: &str, cols: &[&str]) -> TableSnapshot {
        TableSnapshot {
            oid,
            schema: schema.into(),
            name: name.into(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
            has_user_triggers: false,
            sequences: Vec::new(),
        }
    }

    #[test]
    fn quoting_follows_postgres_rules() {
        assert_eq!(quote_ident("wallets"), "wallets");
        assert_eq!(quote_ident("Ledger"), "\"Ledger\"");
        assert_eq!(quote_ident("user"), "\"user\"");
        assert_eq!(quote_ident("we\"ird name"), "\"we\"\"ird name\"");
        assert_eq!(quote_ident("1abc"), "\"1abc\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn capture_and_restore_sql_quote_everything() {
        let mut t = table(16384, "billing", "Ledger", &["id", "order", "amount"]);
        t.sequences.push(SequenceState {
            name: "billing.\"Ledger_id_seq\"".into(),
            last_value: 42,
            is_called: true,
        });
        let s = Snapshot::from_parts(vec![t], &[]);
        let capture = s.capture_sql();
        assert!(capture.contains("DROP SCHEMA IF EXISTS _pgloom_snap CASCADE;"));
        assert!(capture.contains(
            "CREATE UNLOGGED TABLE _pgloom_snap.t_16384 AS SELECT id, \"order\", amount FROM billing.\"Ledger\";"
        ));
        let restore = s.restore_sql(&[16384]);
        assert_eq!(
            restore,
            "BEGIN;\n\
             SET LOCAL synchronous_commit = off;\n\
             TRUNCATE TABLE billing.\"Ledger\";\n\
             INSERT INTO billing.\"Ledger\" (id, \"order\", amount) OVERRIDING SYSTEM VALUE SELECT id, \"order\", amount FROM _pgloom_snap.t_16384;\n\
             SELECT setval('billing.\"Ledger_id_seq\"', 42, true);\n\
             COMMIT;\n"
        );
    }

    #[test]
    fn resolution_prefers_public_and_honours_qualification() {
        let s = Snapshot::from_parts(
            vec![
                table(1, "public", "wallets", &["id"]),
                table(2, "audit", "wallets", &["id"]),
                table(3, "audit", "events", &["id"]),
            ],
            &[],
        );
        assert_eq!(s.resolve("wallets"), vec![1]);
        assert_eq!(s.resolve("audit.wallets"), vec![2]);
        assert_eq!(s.resolve("events"), vec![3]);
        assert!(s.resolve("missing").is_empty());
    }

    #[test]
    fn restore_plan_closes_over_referencing_tables_parent_first() {
        // transfers -> wallets, entries -> transfers
        let s = Snapshot::from_parts(
            vec![
                table(30, "public", "entries", &["id"]),
                table(10, "public", "wallets", &["id"]),
                table(20, "public", "transfers", &["id"]),
                table(40, "public", "unrelated", &["id"]),
            ],
            &[(20, 10), (30, 20)],
        );
        let dirty: BTreeSet<String> = ["wallets".to_string()].into();
        let (plan, unresolved) = s.restore_plan(&dirty, false);
        assert_eq!(plan, vec![10, 20, 30]);
        assert!(unresolved.is_empty());

        let dirty: BTreeSet<String> = ["entries".to_string(), "temp_x".to_string()].into();
        let (plan, unresolved) = s.restore_plan(&dirty, false);
        assert_eq!(plan, vec![30]);
        assert_eq!(unresolved, vec!["temp_x"]);
    }

    #[test]
    fn opaque_writes_and_triggers_widen_to_all_tables() {
        let mut trig = table(2, "public", "accounts", &["id"]);
        trig.has_user_triggers = true;
        let s = Snapshot::from_parts(vec![table(1, "public", "a", &["id"]), trig], &[]);
        let (plan, _) = s.restore_plan(&BTreeSet::new(), true);
        assert_eq!(plan, vec![1, 2]);
        let dirty: BTreeSet<String> = ["accounts".to_string()].into();
        let (plan, _) = s.restore_plan(&dirty, false);
        assert_eq!(plan, vec![1, 2]);
    }

    #[test]
    fn empty_plan_produces_no_sql() {
        let s = Snapshot::from_parts(vec![table(1, "public", "a", &["id"])], &[]);
        assert_eq!(s.restore_sql(&[]), "");
    }
}
