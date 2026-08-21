//! Read-only quality signals projected from a deterministic evidence replay.
//!
//! CP-08-94 intentionally stops before policy: this value helps a separately
//! governed evaluator see the shape of a replay, but it does not score work,
//! dispatch an agent, alter delivery, or persist any decision.

use crate::{EvidenceReplayInput, QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION};
use altai_control_protocol::{AttemptId, OrganizationId, WorkItemId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Schema version for the `QM-084-evaluation-projection-v1` read model.
pub const QM_084_EVALUATION_PROJECTION_SCHEMA_VERSION: u16 = 1;

/// A compact, deterministic description of an evidence replay's coverage.
///
/// The projection contains aggregate counts and distinct evidence kinds only.
/// It deliberately excludes evidence references, activity identifiers,
/// correlation values, timestamps, source text, evaluator output, and scores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationReplayProjection {
    pub schema_version: u16,
    pub source_schema_version: u16,
    pub organization_id: OrganizationId,
    pub work_item_id: WorkItemId,
    pub attempt_id: AttemptId,
    pub evidence_count: usize,
    pub activity_count: usize,
    pub unique_correlation_count: usize,
    pub evidence_kinds: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvaluationProjectionError {
    UnsupportedSourceSchema { actual: u16 },
    MissingEvidence,
    MissingActivity,
    Serialization { reason: String },
}

impl std::fmt::Display for EvaluationProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSourceSchema { actual } => write!(
                f,
                "evaluation projection requires evidence replay schema {}, got {}",
                QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION, actual
            ),
            Self::MissingEvidence => write!(f, "evaluation projection requires replay evidence"),
            Self::MissingActivity => write!(f, "evaluation projection requires replay activity"),
            Self::Serialization { reason } => {
                write!(f, "evaluation projection serialization failed: {reason}")
            }
        }
    }
}

impl std::error::Error for EvaluationProjectionError {}

impl EvaluationReplayProjection {
    /// Project stable, non-authoritative coverage signals from a replay input.
    pub fn from_replay(input: &EvidenceReplayInput) -> Result<Self, EvaluationProjectionError> {
        if input.schema_version != QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION {
            return Err(EvaluationProjectionError::UnsupportedSourceSchema {
                actual: input.schema_version,
            });
        }
        if input.evidence.is_empty() {
            return Err(EvaluationProjectionError::MissingEvidence);
        }
        if input.activity.is_empty() {
            return Err(EvaluationProjectionError::MissingActivity);
        }

        let evidence_kinds = input
            .evidence
            .iter()
            .map(|item| item.kind.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let unique_correlation_count = input
            .activity
            .iter()
            .map(|item| item.correlation_id.as_str())
            .collect::<BTreeSet<_>>()
            .len();

        Ok(Self {
            schema_version: QM_084_EVALUATION_PROJECTION_SCHEMA_VERSION,
            source_schema_version: input.schema_version,
            organization_id: input.organization_id.clone(),
            work_item_id: input.work_item_id.clone(),
            attempt_id: input.attempt_id.clone(),
            evidence_count: input.evidence.len(),
            activity_count: input.activity.len(),
            unique_correlation_count,
            evidence_kinds,
        })
    }

    /// Return stable bytes suitable for a separately governed evaluator input.
    pub fn canonical_json(&self) -> Result<Vec<u8>, EvaluationProjectionError> {
        serde_json::to_vec(self).map_err(|error| EvaluationProjectionError::Serialization {
            reason: error.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvidenceReplayActivity, EvidenceReplayArtifact};

    fn replay() -> EvidenceReplayInput {
        EvidenceReplayInput {
            schema_version: QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION,
            organization_id: OrganizationId::new("org_qm_084"),
            work_item_id: WorkItemId::new("wi_qm_084"),
            attempt_id: AttemptId::new("att_qm_084"),
            evidence: vec![
                EvidenceReplayArtifact {
                    evidence_id: "ev_second".into(),
                    kind: "trace".into(),
                    reference: "artifact://second".into(),
                },
                EvidenceReplayArtifact {
                    evidence_id: "ev_first".into(),
                    kind: "test_result".into(),
                    reference: "artifact://first".into(),
                },
                EvidenceReplayArtifact {
                    evidence_id: "ev_third".into(),
                    kind: "test_result".into(),
                    reference: "artifact://third".into(),
                },
            ],
            activity: vec![
                EvidenceReplayActivity {
                    event_id: "evt_first".into(),
                    correlation_id: "run_a".into(),
                },
                EvidenceReplayActivity {
                    event_id: "evt_second".into(),
                    correlation_id: "run_a".into(),
                },
                EvidenceReplayActivity {
                    event_id: "evt_third".into(),
                    correlation_id: "run_b".into(),
                },
            ],
        }
    }

    #[test]
    fn projection_is_deterministic_and_excludes_source_references() {
        let input = replay();
        let first = EvaluationReplayProjection::from_replay(&input).unwrap();
        let second = EvaluationReplayProjection::from_replay(&input).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.evidence_count, 3);
        assert_eq!(first.activity_count, 3);
        assert_eq!(first.unique_correlation_count, 2);
        assert_eq!(first.evidence_kinds, vec!["test_result", "trace"]);
        let bytes = first.canonical_json().unwrap();
        assert_eq!(bytes, second.canonical_json().unwrap());
        let text = String::from_utf8(bytes).unwrap();
        assert!(!text.contains("artifact://"));
        assert!(!text.contains("run_a"));
        assert!(!text.contains("score"));
    }

    #[test]
    fn projection_fails_closed_for_invalid_replay_shape() {
        let mut input = replay();
        input.schema_version = 99;
        assert_eq!(
            EvaluationReplayProjection::from_replay(&input),
            Err(EvaluationProjectionError::UnsupportedSourceSchema { actual: 99 })
        );

        let mut input = replay();
        input.evidence.clear();
        assert_eq!(
            EvaluationReplayProjection::from_replay(&input),
            Err(EvaluationProjectionError::MissingEvidence)
        );

        let mut input = replay();
        input.activity.clear();
        assert_eq!(
            EvaluationReplayProjection::from_replay(&input),
            Err(EvaluationProjectionError::MissingActivity)
        );
    }
}
