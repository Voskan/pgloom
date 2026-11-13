//! The independence relation used by partial-order reduction.
//!
//! A scheduling step is summarised by its *footprint*: the database objects it
//! reads and writes. Two steps of different workers are independent when
//! neither writes an object the other reads or writes; independent steps
//! commute, so schedules that differ only in their order are equivalent and
//! only one of them needs to run.
//!
//! The footprint must over-approximate what PostgreSQL really does:
//!
//! * Row locks (`FOR UPDATE`, `LOCK TABLE`) count as writes: they block.
//! * Writing a table also writes what the write can reach through the catalog:
//!   foreign-key parents (key-share locks), referencing children (cascades and
//!   restrict checks), the inheritance or partition tree, and sequences used by
//!   column defaults. Triggers, rules, views and unknown relations make a write
//!   reach everything.
//! * `COMMIT` and `ROLLBACK` publish or discard the transaction's writes and
//!   release its locks, so they carry everything the transaction touched.
//! * The first statement of an explicit transaction may fix the snapshot for
//!   the whole transaction (`REPEATABLE READ`, `SERIALIZABLE`), so it reads
//!   everything.
//! * Volatile user functions, `nextval`-style builtins, DDL and anything the
//!   classifier cannot analyse touch everything.
//!
//! * Row-level security policies may query other tables, so reading a table
//!   with RLS enabled reads everything, and so does writing it.
//!
//! Delivering a held response back to the application (a *resume*) carries
//! the footprint of the turn it completes: until the response arrives, the
//! worker cannot act on what that turn read or wrote. The model is sound as
//! long as the database is the only state the workers share; services that
//! also share in-memory state need `reduction: none`.

use std::collections::{BTreeMap, BTreeSet};

use pgloom_pgctrl::{LinkKind, RelationKind, SchemaCatalog};
use pgloom_wire::{Access, TxControl};

/// A database object for the purpose of conflict detection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Object {
    /// A relation by lowercase, unqualified name. Same-named relations in
    /// different schemas are deliberately conflated.
    Relation(String),
    /// The advisory lock space.
    AdvisoryLocks,
}

/// A set of objects, possibly every object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Objects {
    Only(BTreeSet<Object>),
    All,
}

impl Default for Objects {
    fn default() -> Self {
        Objects::Only(BTreeSet::new())
    }
}

impl Objects {
    pub fn is_empty(&self) -> bool {
        matches!(self, Objects::Only(s) if s.is_empty())
    }

    fn insert(&mut self, object: Object) {
        if let Objects::Only(set) = self {
            set.insert(object);
        }
    }

    fn union(&mut self, other: &Objects) {
        match (&mut *self, other) {
            (Objects::All, _) => {}
            (_, Objects::All) => *self = Objects::All,
            (Objects::Only(a), Objects::Only(b)) => a.extend(b.iter().cloned()),
        }
    }

    fn intersects(&self, other: &Objects) -> bool {
        match (self, other) {
            (Objects::All, o) | (o, Objects::All) => !o.is_empty(),
            (Objects::Only(a), Objects::Only(b)) => {
                let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
                small.iter().any(|x| large.contains(x))
            }
        }
    }
}

/// What one scheduling step reads and writes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Footprint {
    pub reads: Objects,
    pub writes: Objects,
}

impl Footprint {
    /// A step that touches nothing (a resume).
    pub fn none() -> Self {
        Self::default()
    }

    /// A step that conflicts with every step that touches anything.
    pub fn everything() -> Self {
        Self {
            reads: Objects::All,
            writes: Objects::All,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.reads.is_empty() && self.writes.is_empty()
    }

    /// Whether executing the two steps in either order may differ.
    pub fn conflicts(&self, other: &Footprint) -> bool {
        self.writes.intersects(&other.writes)
            || self.writes.intersects(&other.reads)
            || self.reads.intersects(&other.writes)
    }

    fn union(&mut self, other: &Footprint) {
        self.reads.union(&other.reads);
        self.writes.union(&other.writes);
    }
}

/// Builtins with effects outside the transaction's own rows.
const SIDE_EFFECT_BUILTINS: &[&str] = &[
    "nextval",
    "setval",
    "pg_notify",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "lo_create",
    "lo_creat",
    "lo_import",
    "lo_unlink",
    "lo_put",
    "lo_from_bytea",
    "dblink",
    "dblink_exec",
    "pg_logical_emit_message",
    "pg_stat_reset",
];

fn is_advisory_function(name: &str) -> bool {
    name.starts_with("pg_advisory") || name.starts_with("pg_try_advisory")
}

#[derive(Debug, Clone)]
struct RelationModel {
    /// Relations a plain read touches; `None` means everything.
    read: Option<BTreeSet<Object>>,
    /// Relations a write or lock touches; `None` means everything.
    write: Option<BTreeSet<Object>>,
    /// Row-level security: writes are checked by policies that may read
    /// any table.
    policy_reads: bool,
}

/// The catalog, reduced to what conflict detection needs.
#[derive(Debug, Clone, Default)]
pub struct SchemaModel {
    relations: BTreeMap<String, RelationModel>,
    volatile_functions: BTreeSet<String>,
}

fn key(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_lowercase()
}

impl SchemaModel {
    pub fn from_catalog(catalog: &SchemaCatalog) -> Self {
        let by_oid: BTreeMap<i64, &pgloom_pgctrl::CatalogRelation> =
            catalog.relations.iter().map(|r| (r.oid, r)).collect();
        let mut fk_parents: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
        let mut fk_children: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
        let mut family: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
        let mut sequences: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
        for link in &catalog.links {
            match link.kind {
                LinkKind::ForeignKey => {
                    fk_parents.entry(link.from).or_default().insert(link.to);
                    fk_children.entry(link.to).or_default().insert(link.from);
                }
                LinkKind::Inherits => {
                    family.entry(link.from).or_default().insert(link.to);
                    family.entry(link.to).or_default().insert(link.from);
                }
                LinkKind::Sequence => {
                    sequences.entry(link.from).or_default().insert(link.to);
                }
            }
        }

        // Everything a write to `start` can reach: its inheritance family,
        // referencing children transitively (cascades), each reached table's
        // foreign-key parents (key-share locks) and their families, and the
        // sequences behind column defaults.
        let reach = |start: i64| -> Option<BTreeSet<Object>> {
            let mut seen: BTreeSet<i64> = BTreeSet::new();
            let mut stack = vec![start];
            let mut extra: BTreeSet<i64> = BTreeSet::new();
            while let Some(oid) = stack.pop() {
                if !seen.insert(oid) {
                    continue;
                }
                stack.extend(family.get(&oid).into_iter().flatten().copied());
                stack.extend(fk_children.get(&oid).into_iter().flatten().copied());
                for &parent in fk_parents.get(&oid).into_iter().flatten() {
                    extra.insert(parent);
                    let mut fam = vec![parent];
                    while let Some(p) = fam.pop() {
                        for &q in family.get(&p).into_iter().flatten() {
                            if extra.insert(q) {
                                fam.push(q);
                            }
                        }
                    }
                }
            }
            let mut out = BTreeSet::new();
            for oid in seen.iter().chain(extra.iter()) {
                let rel = by_oid.get(oid)?;
                if rel.side_effects || rel.kind != RelationKind::Table {
                    return None;
                }
                out.insert(Object::Relation(key(&rel.name)));
                for seq in sequences.get(oid).into_iter().flatten() {
                    out.insert(Object::Relation(key(&by_oid.get(seq)?.name)));
                }
            }
            Some(out)
        };

        let mut relations: BTreeMap<String, RelationModel> = BTreeMap::new();
        for rel in &catalog.relations {
            let name = key(&rel.name);
            let read = match rel.kind {
                RelationKind::Table | RelationKind::Sequence if !rel.row_security => {
                    Some(BTreeSet::from([Object::Relation(name.clone())]))
                }
                _ => None,
            };
            let write = match rel.kind {
                RelationKind::Table => reach(rel.oid),
                RelationKind::Sequence => Some(BTreeSet::from([Object::Relation(name.clone())])),
                _ => None,
            };
            let model = RelationModel {
                read,
                write,
                policy_reads: rel.row_security,
            };
            relations
                .entry(name)
                .and_modify(|m| {
                    m.read = merge(m.read.take(), model.read.clone());
                    m.write = merge(m.write.take(), model.write.clone());
                    m.policy_reads |= model.policy_reads;
                })
                .or_insert(model);
        }
        Self {
            relations,
            volatile_functions: catalog.volatile_functions.iter().cloned().collect(),
        }
    }

    fn read_of(&self, name: &str) -> Objects {
        match self.relations.get(&key(name)).and_then(|m| m.read.as_ref()) {
            Some(set) => Objects::Only(set.clone()),
            None => Objects::All,
        }
    }

    fn write_of(&self, name: &str) -> Objects {
        match self
            .relations
            .get(&key(name))
            .and_then(|m| m.write.as_ref())
        {
            Some(set) => Objects::Only(set.clone()),
            None => Objects::All,
        }
    }

    /// The footprint of one data statement, ignoring transaction effects.
    pub fn statement_footprint(&self, access: &Access) -> Footprint {
        if access.opaque {
            return Footprint::everything();
        }
        let mut fp = Footprint::none();
        for call in &access.calls {
            if is_advisory_function(call) {
                fp.writes.insert(Object::AdvisoryLocks);
            } else if SIDE_EFFECT_BUILTINS.contains(&call.as_str())
                || self.volatile_functions.contains(call)
            {
                return Footprint::everything();
            }
        }
        if access.advisory {
            fp.writes.insert(Object::AdvisoryLocks);
        }
        for r in &access.reads {
            fp.reads.union(&self.read_of(r));
        }
        for w in &access.writes {
            fp.writes.union(&self.write_of(w));
            if self.relations.get(&key(w)).is_some_and(|m| m.policy_reads) {
                fp.reads = Objects::All;
            }
        }
        fp
    }

    /// The footprint of a whole turn, advancing the worker's transaction
    /// state. Compute it on a copy of `tx` to inspect a turn without
    /// executing it.
    pub fn turn_footprint(&self, accesses: &[Access], tx: &mut TxTrack) -> Footprint {
        let mut fp = Footprint::none();
        for access in accesses {
            match access.tx {
                Some(TxControl::Begin) => tx.open = true,
                Some(TxControl::Commit | TxControl::Rollback | TxControl::PrepareTransaction) => {
                    fp.union(&tx.touched);
                    *tx = TxTrack::default();
                }
                Some(TxControl::RollbackToSavepoint) => fp.union(&tx.touched),
                Some(TxControl::Savepoint | TxControl::ReleaseSavepoint) => {}
                None => {
                    if access.snapshot && tx.open && !tx.snapshot {
                        tx.snapshot = true;
                        fp.reads = Objects::All;
                    }
                    let s = self.statement_footprint(access);
                    fp.union(&s);
                    if tx.open {
                        tx.touched.union(&s);
                    }
                }
            }
        }
        fp
    }
}

fn merge(a: Option<BTreeSet<Object>>, b: Option<BTreeSet<Object>>) -> Option<BTreeSet<Object>> {
    let mut a = a?;
    a.extend(b?);
    Some(a)
}

/// A worker's transaction as far as conflicts are concerned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxTrack {
    open: bool,
    /// The transaction snapshot has been taken.
    snapshot: bool,
    /// Everything the open transaction has read, written or locked.
    touched: Footprint,
}

impl TxTrack {
    /// Aligns the model with the transaction status the server reported.
    pub fn sync(&mut self, in_transaction: bool) {
        if !in_transaction {
            *self = TxTrack::default();
        } else if !self.open {
            // A transaction the model did not see begin: assume nothing about
            // its snapshot or what it touched.
            self.open = true;
            self.snapshot = false;
            self.touched = Footprint::everything();
        }
    }
}

#[cfg(test)]
mod tests {
    use pgloom_pgctrl::{CatalogLink, CatalogRelation};
    use pgloom_wire::classify_access;

    use super::*;

    fn rel(oid: i64, name: &str, kind: RelationKind) -> CatalogRelation {
        CatalogRelation {
            oid,
            schema: "public".into(),
            name: name.into(),
            kind,
            side_effects: false,
            row_security: false,
        }
    }

    fn link(kind: LinkKind, from: i64, to: i64) -> CatalogLink {
        CatalogLink { kind, from, to }
    }

    /// wallets <- ledger (FK), ledger_id_seq, audit (trigger), wallet_view,
    /// events partitioned into events_2026, unrelated `coupons`.
    fn model() -> SchemaModel {
        let mut audit = rel(5, "audit", RelationKind::Table);
        audit.side_effects = true;
        SchemaModel::from_catalog(&SchemaCatalog {
            relations: vec![
                rel(1, "wallets", RelationKind::Table),
                rel(2, "ledger", RelationKind::Table),
                rel(3, "ledger_id_seq", RelationKind::Sequence),
                rel(4, "wallet_view", RelationKind::View),
                audit,
                rel(6, "events", RelationKind::Table),
                rel(7, "events_2026", RelationKind::Table),
                rel(8, "coupons", RelationKind::Table),
            ],
            links: vec![
                link(LinkKind::ForeignKey, 2, 1),
                link(LinkKind::Sequence, 2, 3),
                link(LinkKind::Inherits, 7, 6),
            ],
            volatile_functions: vec!["transfer".into()],
        })
    }

    fn fp(m: &SchemaModel, sql: &str) -> Footprint {
        m.turn_footprint(&classify_access(sql), &mut TxTrack::default())
    }

    #[test]
    fn plain_reads_commute_and_writes_do_not() {
        let m = model();
        let read = fp(&m, "SELECT balance FROM wallets WHERE id = 1");
        let write = fp(&m, "UPDATE wallets SET balance = 0 WHERE id = 1");
        let lock = fp(&m, "SELECT balance FROM wallets WHERE id = 1 FOR UPDATE");
        let other = fp(&m, "UPDATE coupons SET used = true");
        assert!(!read.conflicts(&read));
        assert!(read.conflicts(&write));
        assert!(lock.conflicts(&lock));
        assert!(lock.conflicts(&read), "a lock is modelled as a write");
        assert!(!write.conflicts(&other));
    }

    #[test]
    fn writes_reach_through_foreign_keys_sequences_and_partitions() {
        let m = model();
        let insert_ledger = fp(&m, "INSERT INTO ledger (wallet_id) VALUES (1)");
        assert!(insert_ledger.conflicts(&fp(&m, "DELETE FROM wallets WHERE id = 1")));
        assert!(insert_ledger.conflicts(&fp(&m, "SELECT last_value FROM ledger_id_seq")));
        let delete_wallet = fp(&m, "DELETE FROM wallets WHERE id = 1");
        assert!(
            delete_wallet.conflicts(&fp(&m, "SELECT * FROM ledger")),
            "cascade"
        );
        let into_parent = fp(&m, "INSERT INTO events VALUES (1)");
        assert!(into_parent.conflicts(&fp(&m, "SELECT * FROM events_2026")));
        assert!(
            fp(&m, "SELECT * FROM events").conflicts(&fp(&m, "INSERT INTO events_2026 VALUES (1)"))
        );
        assert!(!insert_ledger.conflicts(&fp(&m, "UPDATE coupons SET used = true")));
    }

    #[test]
    fn row_level_security_policies_read_everything() {
        let mut accounts = rel(10, "accounts", RelationKind::Table);
        accounts.row_security = true;
        let m = SchemaModel::from_catalog(&SchemaCatalog {
            relations: vec![
                accounts,
                rel(11, "memberships", RelationKind::Table),
                rel(12, "coupons", RelationKind::Table),
            ],
            ..SchemaCatalog::default()
        });
        // A policy such as `USING (org_id IN (SELECT org_id FROM memberships))`.
        let revoke = fp(&m, "DELETE FROM memberships WHERE user_id = 1");
        let read = fp(&m, "SELECT balance FROM accounts WHERE id = 1");
        let write = fp(&m, "UPDATE accounts SET balance = 0 WHERE id = 1");
        assert!(read.conflicts(&revoke));
        assert!(write.conflicts(&revoke));
        assert!(!read.conflicts(&read));
        assert!(!fp(&m, "SELECT * FROM memberships").conflicts(&fp(&m, "SELECT * FROM coupons")));
    }

    #[test]
    fn unknown_and_side_effecting_objects_touch_everything() {
        let m = model();
        let coupons = fp(&m, "UPDATE coupons SET used = true");
        assert_eq!(fp(&m, "UPDATE audit SET x = 1").writes, Objects::All);
        assert_eq!(fp(&m, "SELECT * FROM wallet_view").reads, Objects::All);
        assert_eq!(fp(&m, "SELECT * FROM nowhere").reads, Objects::All);
        assert!(fp(&m, "SELECT transfer(1, 2)").conflicts(&coupons));
        assert!(
            fp(&m, "SELECT * FROM wallets WHERE transfer(id, 2)").conflicts(&coupons),
            "a volatile user function hides arbitrary writes"
        );
        assert!(fp(&m, "SELECT nextval('ledger_id_seq')").conflicts(&coupons));
        assert!(fp(&m, "CREATE INDEX ON coupons (code)").conflicts(&coupons));
        assert!(!fp(&m, "SELECT COALESCE(SUM(balance), 0) FROM wallets").conflicts(&coupons));
    }

    #[test]
    fn advisory_locks_conflict_only_with_each_other() {
        let m = model();
        let a = fp(&m, "SELECT pg_advisory_xact_lock(1)");
        assert!(a.conflicts(&fp(&m, "SELECT pg_advisory_unlock(1)")));
        assert!(!a.conflicts(&fp(&m, "UPDATE coupons SET used = true")));
    }

    #[test]
    fn transactions_publish_what_they_touched_and_fix_their_snapshot() {
        let m = model();
        let mut tx = TxTrack::default();
        let first = m.turn_footprint(&classify_access("BEGIN; SELECT * FROM coupons"), &mut tx);
        assert_eq!(
            first.reads,
            Objects::All,
            "the first statement fixes the snapshot"
        );
        let second = m.turn_footprint(&classify_access("UPDATE wallets SET balance = 0"), &mut tx);
        assert!(second.reads.is_empty() && !second.writes.is_empty());
        let commit = m.turn_footprint(&classify_access("COMMIT"), &mut tx);
        assert!(commit.conflicts(&fp(&m, "SELECT * FROM wallets")));
        assert!(commit.conflicts(&fp(&m, "UPDATE coupons SET used = false")));
        assert!(!commit.conflicts(&fp(&m, "SELECT * FROM coupons")));
        assert_eq!(tx, TxTrack::default());

        // Autocommit statements are their own transaction.
        let mut tx = TxTrack::default();
        let solo = m.turn_footprint(&classify_access("SELECT * FROM coupons"), &mut tx);
        assert!(!solo.conflicts(&fp(&m, "UPDATE wallets SET balance = 0")));
    }

    #[test]
    fn unexpected_transactions_are_treated_conservatively() {
        let mut tx = TxTrack::default();
        tx.sync(true);
        let m = model();
        let commit = m.turn_footprint(&classify_access("COMMIT"), &mut tx);
        assert_eq!(commit, Footprint::everything());
    }

    #[test]
    fn empty_and_universal_sets_behave() {
        assert!(!Footprint::none().conflicts(&Footprint::everything()));
        assert!(Footprint::everything().conflicts(&Footprint::everything()));
    }
}
