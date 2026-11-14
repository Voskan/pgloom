//! The parts of the system catalog that decide which statements can interfere
//! with each other: relation kinds, foreign keys, inheritance, sequences used
//! by column defaults, triggers and rules, and user-defined volatile functions.

use crate::control::PgControl;
use crate::error::{PgCtrlError, PgCtrlResult, RowExt};

/// Kind of a relation, from `pg_class.relkind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationKind {
    /// Ordinary, partitioned table or materialized view: data lives in it.
    Table,
    View,
    Sequence,
    Foreign,
    Other,
}

impl RelationKind {
    fn from_relkind(kind: &str) -> Self {
        match kind {
            "r" | "p" | "m" => RelationKind::Table,
            "v" => RelationKind::View,
            "S" => RelationKind::Sequence,
            "f" => RelationKind::Foreign,
            _ => RelationKind::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRelation {
    pub oid: i64,
    pub schema: String,
    pub name: String,
    pub kind: RelationKind,
    /// User triggers or rewrite rules: writes may touch arbitrary relations.
    pub side_effects: bool,
    /// Row-level security is enabled: policies may read arbitrary relations.
    pub row_security: bool,
}

/// How two relations are tied together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LinkKind {
    /// `(child, parent)`: `child` has a foreign key referencing `parent`.
    ForeignKey,
    /// `(child, parent)`: inheritance or partitioning.
    Inherits,
    /// `(table, sequence)`: the table draws values from the sequence.
    Sequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogLink {
    pub kind: LinkKind,
    pub from: i64,
    pub to: i64,
}

/// A point-in-time view of the user schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaCatalog {
    pub relations: Vec<CatalogRelation>,
    pub links: Vec<CatalogLink>,
    /// Names (lowercase) of volatile functions outside the system schemas.
    pub volatile_functions: Vec<String>,
}

const USER_NAMESPACE: &str = "n.nspname NOT IN ('pg_catalog', 'information_schema', '_pgloom_snap') \
     AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'";

impl PgControl {
    /// Reads the catalog in three round trips.
    pub async fn schema_catalog(&self) -> PgCtrlResult<SchemaCatalog> {
        let client = self.client();
        let rows = client
            .query(
                &format!(
                    "SELECT c.oid::int8, n.nspname::text, c.relname::text, c.relkind::text, \
                            c.relhasrules OR EXISTS (SELECT 1 FROM pg_trigger t \
                                WHERE t.tgrelid = c.oid AND NOT t.tgisinternal), \
                            c.relrowsecurity \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.relkind IN ('r', 'p', 'm', 'v', 'S', 'f') AND {USER_NAMESPACE} \
                     ORDER BY c.oid"
                ),
                &[],
            )
            .await
            .map_err(PgCtrlError::query("reading relations"))?;
        const RELATIONS: &str = "reading relations";
        let relations = rows
            .iter()
            .map(|r| {
                Ok(CatalogRelation {
                    oid: r.col(0, RELATIONS)?,
                    schema: r.col(1, RELATIONS)?,
                    name: r.col(2, RELATIONS)?,
                    kind: RelationKind::from_relkind(r.col::<&str>(3, RELATIONS)?),
                    side_effects: r.col(4, RELATIONS)?,
                    row_security: r.col(5, RELATIONS)?,
                })
            })
            .collect::<PgCtrlResult<_>>()?;

        let rows = client
            .query(
                "SELECT 0, conrelid::int8, confrelid::int8 FROM pg_constraint WHERE contype = 'f' \
                 UNION \
                 SELECT 1, inhrelid::int8, inhparent::int8 FROM pg_inherits \
                 UNION \
                 SELECT 2, d.refobjid::int8, d.objid::int8 FROM pg_depend d \
                   JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
                   WHERE d.classid = 'pg_class'::regclass \
                     AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i') \
                 UNION \
                 SELECT 2, ad.adrelid::int8, d.refobjid::int8 FROM pg_attrdef ad \
                   JOIN pg_depend d ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid \
                     AND d.refclassid = 'pg_class'::regclass \
                   JOIN pg_class s ON s.oid = d.refobjid AND s.relkind = 'S' \
                 ORDER BY 1, 2, 3",
                &[],
            )
            .await
            .map_err(PgCtrlError::query("reading relation dependencies"))?;
        const LINKS: &str = "reading relation dependencies";
        let links = rows
            .iter()
            .map(|r| {
                Ok(CatalogLink {
                    kind: match r.col::<i32>(0, LINKS)? {
                        0 => LinkKind::ForeignKey,
                        1 => LinkKind::Inherits,
                        _ => LinkKind::Sequence,
                    },
                    from: r.col(1, LINKS)?,
                    to: r.col(2, LINKS)?,
                })
            })
            .collect::<PgCtrlResult<_>>()?;

        let rows = client
            .query(
                &format!(
                    "SELECT DISTINCT lower(p.proname::text) FROM pg_proc p \
                     JOIN pg_namespace n ON n.oid = p.pronamespace \
                     WHERE p.provolatile = 'v' AND {USER_NAMESPACE} ORDER BY 1"
                ),
                &[],
            )
            .await
            .map_err(PgCtrlError::query("reading user functions"))?;
        let volatile_functions = rows
            .iter()
            .map(|r| r.col(0, "reading user functions"))
            .collect::<PgCtrlResult<_>>()?;

        Ok(SchemaCatalog {
            relations,
            links,
            volatile_functions,
        })
    }
}
