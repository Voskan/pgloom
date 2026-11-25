//! `.pgloom-repro.json`: everything needed to re-execute a failing schedule.

use std::path::Path;

use pgloom_engine::{
    EpisodeRecord, Failure, RecordedStep, StepAction, Violation, WorkerId, WorkerOutcome,
};
use pgloom_wire::TxStatus;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const FORMAT_VERSION: u32 = 1;
pub const DEFAULT_REPRO_PATH: &str = ".pgloom-repro.json";

#[derive(Debug, thiserror::Error)]
pub enum ReproError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot write {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not a valid PgLoom repro file: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("{path} uses repro format {found}; this PgLoom reads format {FORMAT_VERSION}")]
    Version { path: String, found: u32 },
    #[error("{path} refers to unknown worker `{worker}`")]
    UnknownWorker { path: String, worker: String },
}

/// One timeline row, with the worker named as in `pgloom.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReproStep {
    pub step: u32,
    pub worker: String,
    #[serde(flatten)]
    pub action: StepAction,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_status: Option<TxStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_pid: Option<i32>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub preempted: bool,
}

impl ReproStep {
    pub fn from_recorded(step: &RecordedStep, workers: &[String]) -> Self {
        Self {
            step: step.step,
            worker: workers
                .get(step.worker.index())
                .cloned()
                .unwrap_or_else(|| step.worker.to_string()),
            action: step.action.clone(),
            sql: step.sql.clone(),
            params: step.params.clone(),
            tx_status: step.tx_status,
            result: step.result.clone(),
            sqlstate: step.sqlstate.clone(),
            backend_pid: step.backend_pid,
            preempted: step.preempted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReproFile {
    pub format_version: u32,
    pub pgloom_version: String,
    pub scenario_name: String,
    /// Scenario path as given on the command line.
    pub config_path: String,
    /// SHA-256 of `config_source`; replay warns when the file on disk differs.
    pub config_sha256: String,
    /// The scenario file exactly as it was when the failure was found.
    pub config_source: String,
    /// `InvariantFailed` or `Deadlock`.
    pub violation_kind: String,
    pub violated_invariants: Vec<String>,
    pub violations: Vec<Violation>,
    pub workers: Vec<String>,
    /// Worker index chosen at each scheduling decision.
    pub choice_sequence: Vec<u32>,
    pub preemptions: u32,
    pub recorded_steps: Vec<ReproStep>,
}

pub fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl ReproFile {
    pub fn from_failure(
        scenario_name: &str,
        config_path: &str,
        config_source: &str,
        workers: &[String],
        failure: &Failure,
    ) -> Self {
        let violation_kind = if failure.violations.iter().all(Violation::is_deadlock)
            && !failure.violations.is_empty()
        {
            "Deadlock"
        } else {
            "InvariantFailed"
        };
        Self {
            format_version: FORMAT_VERSION,
            pgloom_version: env!("CARGO_PKG_VERSION").to_owned(),
            scenario_name: scenario_name.to_owned(),
            config_path: config_path.to_owned(),
            config_sha256: sha256_hex(config_source.as_bytes()),
            config_source: config_source.to_owned(),
            violation_kind: violation_kind.to_owned(),
            violated_invariants: failure.violations.iter().map(Violation::name).collect(),
            violations: failure.violations.clone(),
            workers: workers.to_vec(),
            choice_sequence: failure.schedule.choices().iter().map(|w| w.0).collect(),
            preemptions: failure.schedule.preemptions(),
            recorded_steps: failure
                .record
                .steps
                .iter()
                .map(|s| ReproStep::from_recorded(s, workers))
                .collect(),
        }
    }

    pub fn choices(&self) -> Vec<WorkerId> {
        self.choice_sequence.iter().map(|c| WorkerId(*c)).collect()
    }

    /// Whether `source` is the scenario this file was recorded from.
    pub fn matches_config(&self, source: &str) -> bool {
        sha256_hex(source.as_bytes()) == self.config_sha256
    }

    /// Rebuilds the recorded episode (for rendering without re-execution).
    pub fn to_record(&self, path: &str) -> Result<EpisodeRecord, ReproError> {
        let n = self.workers.len();
        let mut record = EpisodeRecord {
            outcomes: vec![None; n],
            backend_pids: vec![None; n],
            ..Default::default()
        };
        for s in &self.recorded_steps {
            let idx = self
                .workers
                .iter()
                .position(|w| *w == s.worker)
                .ok_or_else(|| ReproError::UnknownWorker {
                    path: path.to_owned(),
                    worker: s.worker.clone(),
                })?;
            let worker = WorkerId(idx as u32);
            if let StepAction::Done { outcome } = &s.action {
                record.outcomes[idx] = Some(WorkerOutcome::clone(outcome));
            }
            if s.backend_pid.is_some() {
                record.backend_pids[idx] = s.backend_pid;
            }
            record.steps.push(RecordedStep {
                step: s.step,
                worker,
                action: s.action.clone(),
                sql: s.sql.clone(),
                params: s.params.clone(),
                tx_status: s.tx_status,
                result: s.result.clone(),
                sqlstate: s.sqlstate.clone(),
                backend_pid: s.backend_pid,
                preempted: s.preempted,
                meta: None,
            });
        }
        record.deadlock = self.violations.iter().find(|v| v.is_deadlock()).cloned();
        Ok(record)
    }

    pub fn to_json(&self) -> String {
        // Serializing plain data structures cannot fail.
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// Writes atomically using a unique temporary file in the target directory.
    pub fn save(&self, path: &Path) -> Result<(), ReproError> {
        use std::io::Write as _;

        let err = |source| ReproError::Write {
            path: path.display().to_string(),
            source,
        };
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(err)?;
        tmp.write_all((self.to_json() + "\n").as_bytes())
            .map_err(err)?;
        tmp.flush().map_err(err)?;
        tmp.persist(path).map_err(|e| err(e.error))?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self, ReproError> {
        let p = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|source| ReproError::Read {
            path: p.clone(),
            source,
        })?;
        Self::parse(&text, &p)
    }

    pub fn parse(text: &str, path: &str) -> Result<Self, ReproError> {
        #[derive(Deserialize)]
        struct Header {
            format_version: u32,
        }
        let header: Header = serde_json::from_str(text).map_err(|source| ReproError::Parse {
            path: path.to_owned(),
            source,
        })?;
        if header.format_version != FORMAT_VERSION {
            return Err(ReproError::Version {
                path: path.to_owned(),
                found: header.format_version,
            });
        }
        serde_json::from_str(text).map_err(|source| ReproError::Parse {
            path: path.to_owned(),
            source,
        })
    }
}
