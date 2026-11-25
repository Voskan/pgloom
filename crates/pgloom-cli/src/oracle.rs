//! Scenario invariants: SQL assertions on the upstream database plus
//! assertions over the workers' responses.

use futures::future::try_join_all;
use pgloom_driver::{ResponseInvariant, check_responses};
use pgloom_engine::{EngineError, EpisodeOracle, EpisodeRecord, Violation};
use pgloom_pgctrl::{PgControl, SqlInvariant};

#[derive(Debug)]
pub struct ScenarioOracle {
    pub ctl: PgControl,
    pub sql: Vec<SqlInvariant>,
    pub responses: Vec<ResponseInvariant>,
    pub workers: Vec<String>,
}

impl EpisodeOracle for ScenarioOracle {
    async fn check(&self, record: &EpisodeRecord) -> Result<Vec<Violation>, EngineError> {
        let mut out: Vec<Violation> = Vec::new();
        if !self.sql.is_empty() {
            // Evaluate every SQL invariant and its diagnostic query against one
            // repeatable-read snapshot so concurrent background activity cannot
            // produce a torn view across assertions.
            self.ctl
                .batch(
                    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                    "starting oracle snapshot",
                )
                .await
                .map_err(|e| EngineError::Oracle(e.to_string()))?;
            let results =
                try_join_all(self.sql.iter().map(|inv| self.ctl.check_invariant(inv))).await;
            let _ = self.ctl.batch("ROLLBACK", "closing oracle snapshot").await;
            let results = results.map_err(|e| EngineError::Oracle(e.to_string()))?;
            out.extend(
                self.sql
                    .iter()
                    .zip(results)
                    .filter(|(_, r)| !r.passed)
                    .map(|(inv, r)| Violation::InvariantFailed {
                        name: inv.name.clone(),
                        description: inv.description.clone().unwrap_or_default(),
                        observed: r.observed,
                    }),
            );
        }
        out.extend(check_responses(
            &self.responses,
            &self.workers,
            &record.outcomes,
        ));
        Ok(out)
    }
}
