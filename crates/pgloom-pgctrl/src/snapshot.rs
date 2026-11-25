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
use crate::error::{PgCtrlError, PgCtrlResult, RowExt};

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
    /// Non-partitioned table with classic `INHERITS` children: queries and
    /// `TRUNCATE` must use `ONLY` so child rows are not duplicated or wiped.
    pub has_inheritance_children: bool,
    pub sequences: Vec<SequenceState>,
}

impl TableSnapshot {
    /// `schema.table`, quoted.
    pub fn qualified(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }

    /// `ONLY schema.table` when the table is a classic inheritance parent,
    /// otherwise `schema.table`.
    pub fn qualified_only(&self) -> String {
        if self.has_inheritance_children {
            format!("ONLY {}", self.qualified())
        } else {
            self.qualified()
        }
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
    /// Snapshotted tables referenced via foreign key by a table outside the
    /// snapshot (excluded / migration tables), which therefore cannot be
    /// truncated without `CASCADE` and must be cleared with `DELETE`.
    referenced_by_excluded: BTreeSet<i64>,
    /// `(schema, partition_name) -> root partitioned table OID`.
    partition_roots: BTreeMap<(String, String), i64>,
    /// User-schema sequences not attached to any snapshotted table.
    standalone_sequences: Vec<SequenceState>,
    /// Active schema resolution order from `current_schemas(false)`.
    search_path: Vec<String>,
    /// Whether this role may set `session_replication_role = replica`.
    can_replica_role: bool,
}

impl Snapshot {
    /// Builds a snapshot description without touching the database (tests).
    pub fn from_parts(tables: Vec<TableSnapshot>, foreign_keys: &[(i64, i64)]) -> Self {
        let mut s = Snapshot {
            search_path: vec!["public".into()],
            ..Snapshot::default()
        };
        for t in tables {
            s.tables.insert(t.oid, t);
        }
        for &(child, parent) in foreign_keys {
            if child != parent && s.tables.contains_key(&parent) {
                if s.tables.contains_key(&child) {
                    s.referenced_by.entry(parent).or_default().insert(child);
                } else {
                    s.referenced_by_excluded.insert(parent);
                }
            }
        }
        s
    }

    /// Overrides the `search_path` used when resolving unqualified names (tests).
    #[must_use]
    pub fn with_search_path(mut self, schemas: Vec<String>) -> Self {
        self.search_path = schemas;
        self
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
        let can_replica_role = ctl
            .batch(
                "BEGIN; SET LOCAL session_replication_role = replica; ROLLBACK;",
                "probing session_replication_role",
            )
            .await
            .is_ok();
        if !can_replica_role {
            let _ = ctl.batch("ROLLBACK", "cleaning up failed probe").await;
        }

        let sp_rows = client
            .query("SELECT unnest(current_schemas(false))::text", &[])
            .await
            .map_err(PgCtrlError::query("reading search_path"))?;
        let mut search_path: Vec<String> = sp_rows
            .iter()
            .map(|r| r.col(0, "reading search_path"))
            .collect::<PgCtrlResult<_>>()?;
        if search_path.is_empty() {
            search_path.push("public".into());
        }

        let rows = client
            .query(
                "SELECT c.oid::int8, n.nspname::text, c.relname::text, \
                        EXISTS (SELECT 1 FROM pg_trigger t \
                                WHERE t.tgrelid = c.oid AND NOT t.tgisinternal), \
                        c.relkind = 'r' AND EXISTS ( \
                            SELECT 1 FROM pg_inherits i WHERE i.inhparent = c.oid \
                        ) \
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
            const CONTEXT: &str = "listing user tables";
            let schema: String = row.col(1, CONTEXT)?;
            let name: String = row.col(2, CONTEXT)?;
            if excluded(&schema, &name) {
                continue;
            }
            let oid: i64 = row.col(0, CONTEXT)?;
            tables.insert(
                oid,
                TableSnapshot {
                    oid,
                    schema,
                    name,
                    columns: Vec::new(),
                    has_user_triggers: row.col(3, CONTEXT)?,
                    has_inheritance_children: row.col(4, CONTEXT)?,
                    sequences: Vec::new(),
                },
            );
        }
        let oids: Vec<i64> = tables.keys().copied().collect();

        let part_rows = client
            .query(
                "WITH RECURSIVE tree(leaf, root) AS ( \
                   SELECT c.oid, i.inhparent \
                   FROM pg_class c \
                   JOIN pg_inherits i ON i.inhrelid = c.oid \
                   WHERE c.relispartition \
                   UNION ALL \
                   SELECT t.leaf, i.inhparent \
                   FROM tree t \
                   JOIN pg_inherits i ON i.inhrelid = t.root \
                 ) \
                 SELECT n.nspname::text, c.relname::text, t.root::int8 \
                 FROM tree t \
                 JOIN pg_class c ON c.oid = t.leaf \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 JOIN pg_class r ON r.oid = t.root \
                 WHERE NOT r.relispartition \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', '_pgloom_snap') \
                   AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%' \
                 ORDER BY t.leaf",
                &[],
            )
            .await
            .map_err(PgCtrlError::query("listing table partitions"))?;
        let mut partition_roots: BTreeMap<(String, String), i64> = BTreeMap::new();
        for row in part_rows {
            const CONTEXT: &str = "listing table partitions";
            let schema: String = row.col(0, CONTEXT)?;
            let name: String = row.col(1, CONTEXT)?;
            let root: i64 = row.col(2, CONTEXT)?;
            if tables.contains_key(&root) {
                partition_roots.insert((schema, name), root);
            }
        }

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
            let oid: i64 = row.col(0, "listing table columns")?;
            if let Some(t) = tables.get_mut(&oid) {
                t.columns.push(row.col(1, "listing table columns")?);
            }
        }

        // Capture all user sequences: owned (SERIAL/IDENTITY), referenced by
        // column defaults (pg_attrdef), and standalone sequences.
        let all_seq_rows = client
            .query(
                "SELECT s.oid::int8, \
                        quote_ident(n.nspname) || '.' || quote_ident(s.relname) \
                 FROM pg_class s \
                 JOIN pg_namespace n ON n.oid = s.relnamespace \
                 WHERE s.relkind = 'S' \
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', '_pgloom_snap') \
                   AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%' \
                 ORDER BY s.oid",
                &[],
            )
            .await
            .map_err(PgCtrlError::query("listing user sequences"))?;
        let mut seq_states: BTreeMap<i64, SequenceState> = BTreeMap::new();
        for row in all_seq_rows {
            let seq_oid: i64 = row.col(0, "listing user sequences")?;
            let name: String = row.col(1, "listing user sequences")?;
            let state = client
                .query_one(&format!("SELECT last_value, is_called FROM {name}"), &[])
                .await
                .map_err(PgCtrlError::query(format!("reading sequence {name}")))?;
            seq_states.insert(
                seq_oid,
                SequenceState {
                    name,
                    last_value: state.col(0, "reading sequence state")?,
                    is_called: state.col(1, "reading sequence state")?,
                },
            );
        }

        let seq_links = client
            .query(
                "SELECT d.refobjid::int8, d.objid::int8 \
                 FROM pg_depend d \
                 JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
                 WHERE d.classid = 'pg_class'::regclass \
                   AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i') \
                   AND d.refobjid = ANY($1::int8[]::oid[]) \
                 UNION \
                 SELECT ad.adrelid::int8, d.refobjid::int8 \
                 FROM pg_attrdef ad \
                 JOIN pg_depend d ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid \
                   AND d.refclassid = 'pg_class'::regclass \
                 JOIN pg_class s ON s.oid = d.refobjid AND s.relkind = 'S' \
                 WHERE ad.adrelid = ANY($1::int8[]::oid[]) \
                 ORDER BY 1, 2",
                &[&oids],
            )
            .await
            .map_err(PgCtrlError::query("listing table sequences"))?;
        let mut attached_seqs: BTreeSet<i64> = BTreeSet::new();
        for row in seq_links {
            let table_oid: i64 = row.col(0, "listing table sequences")?;
            let seq_oid: i64 = row.col(1, "listing table sequences")?;
            if let (Some(t), Some(state)) = (tables.get_mut(&table_oid), seq_states.get(&seq_oid)) {
                t.sequences.push(state.clone());
                attached_seqs.insert(seq_oid);
            }
        }
        let standalone_sequences: Vec<SequenceState> = seq_states
            .into_iter()
            .filter(|(oid, _)| !attached_seqs.contains(oid))
            .map(|(_, s)| s)
            .collect();

        let fks = client
            .query(
                "SELECT conrelid::int8, confrelid::int8 FROM pg_constraint \
                 WHERE contype = 'f' ORDER BY oid",
                &[],
            )
            .await
            .map_err(PgCtrlError::query("listing foreign keys"))?;
        let fk_pairs: Vec<(i64, i64)> = fks
            .iter()
            .map(|r| {
                Ok((
                    r.col(0, "listing foreign keys")?,
                    r.col(1, "listing foreign keys")?,
                ))
            })
            .collect::<PgCtrlResult<_>>()?;

        let mut snapshot = Snapshot::from_parts(tables.into_values().collect(), &fk_pairs);
        snapshot.partition_roots = partition_roots;
        snapshot.standalone_sequences = standalone_sequences;
        snapshot.search_path = search_path;
        snapshot.can_replica_role = can_replica_role;
        ctl.batch(&snapshot.capture_sql(), "creating snapshot tables")
            .await?;
        debug!(
            tables = snapshot.len(),
            can_replica_role,
            standalone_sequences = snapshot.standalone_sequences.len(),
            "snapshot captured"
        );
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
                t.qualified_only()
            ));
        }
        sql
    }

    /// Maps a table (or child partition) reference as written in SQL to
    /// snapshotted root tables, honouring the captured `search_path`.
    pub fn resolve(&self, reference: &str) -> Vec<i64> {
        let (schema, name) = match reference.rsplit_once('.') {
            Some((s, n)) => (Some(s), n),
            None => (None, reference),
        };
        let mut matches: Vec<(&str, i64)> = self
            .tables
            .values()
            .filter(|t| t.name == name && schema.is_none_or(|s| s == t.schema))
            .map(|t| (t.schema.as_str(), t.oid))
            .collect();
        for ((p_schema, p_name), &root_oid) in &self.partition_roots {
            if p_name == name && schema.is_none_or(|s| s == p_schema) {
                matches.push((p_schema.as_str(), root_oid));
            }
        }
        if schema.is_none() {
            for sp in &self.search_path {
                let in_schema: BTreeSet<i64> = matches
                    .iter()
                    .filter(|(s, _)| *s == sp.as_str())
                    .map(|(_, oid)| *oid)
                    .collect();
                if !in_schema.is_empty() {
                    return in_schema.into_iter().collect();
                }
            }
        }
        let deduped: BTreeSet<i64> = matches.into_iter().map(|(_, oid)| oid).collect();
        deduped.into_iter().collect()
    }

    /// Tables to restore for `dirty` references, closed over foreign keys and
    /// widened to every table if a write was opaque, targeted an unresolved
    /// non-temporary relation (e.g. an updatable view), or touched a table
    /// with user triggers. Returned parent-first.
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
        let non_temp_unresolved = unresolved.iter().any(|r| {
            let lower = r.to_ascii_lowercase();
            !lower.starts_with("pg_temp.") && !lower.starts_with("pg_temp_")
        });
        let triggers = set
            .iter()
            .any(|oid| self.tables.get(oid).is_some_and(|t| t.has_user_triggers));
        if opaque_writes || triggers || non_temp_unresolved {
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
        let (order, _) = self.parent_first(&set);
        (order, unresolved)
    }

    /// Kahn's algorithm over the restore set; ties broken by OID.
    /// Returns `(order, has_cycle)`.
    fn parent_first(&self, set: &BTreeSet<i64>) -> (Vec<i64>, bool) {
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
        // Foreign-key cycles cannot be topologically ordered: append the
        // remaining tables in OID order and signal `restore_sql` to suppress
        // or defer constraint checks for the restore transaction.
        let has_cycle = !indegree.is_empty();
        order.extend(indegree.keys().copied());
        (order, has_cycle)
    }

    fn plan_has_cycle(&self, plan: &[i64]) -> bool {
        let set: BTreeSet<i64> = plan.iter().copied().collect();
        self.parent_first(&set).1
    }

    /// The single-transaction restore script for `plan` (parent-first OIDs).
    pub fn restore_sql(&self, plan: &[i64]) -> String {
        self.restore_sql_with(plan, self.plan_has_cycle(plan), false)
    }

    fn restore_sql_with(
        &self,
        plan: &[i64],
        has_cycle: bool,
        include_standalone_sequences: bool,
    ) -> String {
        let tables: Vec<&TableSnapshot> = plan.iter().filter_map(|o| self.tables.get(o)).collect();
        let has_standalone = include_standalone_sequences && !self.standalone_sequences.is_empty();
        if tables.is_empty() && !has_standalone {
            return String::new();
        }
        // The restore need not survive a crash (the snapshot does), so its
        // commit skips the WAL flush.
        let mut sql = String::from("BEGIN;\nSET LOCAL synchronous_commit = off;\n");
        let has_triggers = tables.iter().any(|t| t.has_user_triggers);
        let use_replica_role = self.can_replica_role && (has_triggers || has_cycle);
        if use_replica_role {
            sql.push_str("SET LOCAL session_replication_role = replica;\n");
        } else {
            if has_cycle {
                sql.push_str("SET CONSTRAINTS ALL DEFERRED;\n");
            }
            for t in &tables {
                if t.has_user_triggers {
                    sql.push_str(&format!(
                        "ALTER TABLE {} DISABLE TRIGGER USER;\n",
                        t.qualified()
                    ));
                }
            }
        }

        let (deletable, truncatable): (Vec<&TableSnapshot>, Vec<&TableSnapshot>) = tables
            .iter()
            .copied()
            .partition(|t| self.referenced_by_excluded.contains(&t.oid));
        for t in deletable.iter().rev() {
            sql.push_str(&format!("DELETE FROM {};\n", t.qualified_only()));
        }
        if !truncatable.is_empty() {
            sql.push_str(&format!(
                "TRUNCATE TABLE {};\n",
                truncatable
                    .iter()
                    .map(|t| t.qualified_only())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

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
        if !use_replica_role {
            for t in &tables {
                if t.has_user_triggers {
                    sql.push_str(&format!(
                        "ALTER TABLE {} ENABLE TRIGGER USER;\n",
                        t.qualified()
                    ));
                }
            }
        }
        let mut seen_seqs: BTreeSet<&str> = BTreeSet::new();
        for t in &tables {
            for s in &t.sequences {
                if seen_seqs.insert(s.name.as_str()) {
                    sql.push_str(&format!(
                        "SELECT setval({}, {}, {});\n",
                        quote_literal(&s.name),
                        s.last_value,
                        s.is_called
                    ));
                }
            }
        }
        if include_standalone_sequences {
            for s in &self.standalone_sequences {
                if seen_seqs.insert(s.name.as_str()) {
                    sql.push_str(&format!(
                        "SELECT setval({}, {}, {});\n",
                        quote_literal(&s.name),
                        s.last_value,
                        s.is_called
                    ));
                }
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
        let has_cycle = self.plan_has_cycle(&plan);
        let reset_standalone = opaque_writes || !unresolved.is_empty();
        let sql = self.restore_sql_with(&plan, has_cycle, reset_standalone);
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
            has_inheritance_children: false,
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

        let dirty: BTreeSet<String> = ["entries".to_string(), "pg_temp.temp_x".to_string()].into();
        let (plan, unresolved) = s.restore_plan(&dirty, false);
        assert_eq!(plan, vec![30]);
        assert_eq!(unresolved, vec!["pg_temp.temp_x"]);

        // An unresolved non-temporary target (e.g. an updatable view) fails
        // closed by restoring every snapshotted table.
        let dirty: BTreeSet<String> = ["active_wallets".to_string()].into();
        let (plan, unresolved) = s.restore_plan(&dirty, false);
        assert_eq!(plan, vec![10, 20, 30, 40]);
        assert_eq!(unresolved, vec!["active_wallets"]);
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
