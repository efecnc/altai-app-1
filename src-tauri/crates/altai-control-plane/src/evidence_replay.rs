//! Deterministic, read-only comparison input for CP-08-91.
//!
//! This module turns immutable [`Evidence`] and the already ordered
//! [`ActivityEvent`] read for one canonical Work/Attempt into a small,
//! versioned value. It deliberately contains no timestamps, model output,
//! external score, delivery decision, or runtime authority. Callers retain
//! ownership of querying repositories and of any later evaluation policy.

use altai_control_protocol::{ActivityEvent, AttemptId, Evidence, OrganizationId, WorkItemId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Schema version for the `QM-084-evidence-replay-v1` comparison input.
pub const QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION: u16 = 1;

/// One canonical evidence reference included in a comparison input.
///
/// Creation time is intentionally absent. Repository ordering decides the
/// order, and a comparison must not change merely because a clock value does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceReplayArtifact {
    pub evidence_id: String,
    pub kind: String,
    pub reference: String,
}

/// One ordered Activity correlation included in a comparison input.
///
/// The source repository's stable event order is preserved; timestamps and
/// human-readable summaries are not evaluation inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceReplayActivity {
    pub event_id: String,
    pub correlation_id: String,
}

/// The portable, deterministic input a later evaluator may compare.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceReplayInput {
    pub schema_version: u16,
    pub organization_id: OrganizationId,
    pub work_item_id: WorkItemId,
    pub attempt_id: AttemptId,
    pub evidence: Vec<EvidenceReplayArtifact>,
    pub activity: Vec<EvidenceReplayActivity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceReplayError {
    MissingEvidence,
    MissingActivity,
    EvidenceScopeMismatch { evidence_id: String },
    UnsafeReference { evidence_id: String },
    DuplicateEvidence { evidence_id: String },
    ActivityScopeMismatch { event_id: String },
    MissingCorrelation { event_id: String },
    DuplicateActivity { event_id: String },
    Serialization { reason: String },
}

impl std::fmt::Display for EvidenceReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEvidence => {
                write!(f, "evidence replay requires at least one evidence record")
            }
            Self::MissingActivity => {
                write!(f, "evidence replay requires at least one activity record")
            }
            Self::EvidenceScopeMismatch { evidence_id } => {
                write!(
                    f,
                    "evidence {evidence_id} is outside the requested organization/work/attempt"
                )
            }
            Self::UnsafeReference { evidence_id } => {
                write!(
                    f,
                    "evidence {evidence_id} contains a credential-like reference"
                )
            }
            Self::DuplicateEvidence { evidence_id } => {
                write!(
                    f,
                    "evidence replay contains duplicate evidence id {evidence_id}"
                )
            }
            Self::ActivityScopeMismatch { event_id } => {
                write!(
                    f,
                    "activity {event_id} is outside the requested organization/work/attempt"
                )
            }
            Self::MissingCorrelation { event_id } => {
                write!(f, "activity {event_id} has no correlation id")
            }
            Self::DuplicateActivity { event_id } => {
                write!(
                    f,
                    "evidence replay contains duplicate activity id {event_id}"
                )
            }
            Self::Serialization { reason } => {
                write!(f, "evidence replay serialization failed: {reason}")
            }
        }
    }
}
impl std::error::Error for EvidenceReplayError {}

impl EvidenceReplayInput {
    /// Normalize repository reads into a deterministic comparison input.
    ///
    /// Evidence is canonicalized by the immutable repository order
    /// (`created_at_unix_seconds`, then id). Activity order is intentionally
    /// not re-sorted: callers pass the repository's sequence-ordered replay,
    /// and that order is part of the audited trace.
    pub fn from_records(
        organization_id: OrganizationId,
        work_item_id: WorkItemId,
        attempt_id: AttemptId,
        evidence: &[Evidence],
        activity: &[ActivityEvent],
    ) -> Result<Self, EvidenceReplayError> {
        if evidence.is_empty() {
            return Err(EvidenceReplayError::MissingEvidence);
        }
        if activity.is_empty() {
            return Err(EvidenceReplayError::MissingActivity);
        }

        let mut seen_evidence = BTreeSet::new();
        let mut normalized_evidence = evidence.to_vec();
        normalized_evidence.sort_by(|left, right| {
            left.created_at_unix_seconds
                .cmp(&right.created_at_unix_seconds)
                .then_with(|| left.id.value.cmp(&right.id.value))
        });
        let evidence = normalized_evidence
            .into_iter()
            .map(|item| {
                if item.organization_id != organization_id
                    || item.work_item_id != work_item_id
                    || item.attempt_id != attempt_id
                {
                    return Err(EvidenceReplayError::EvidenceScopeMismatch {
                        evidence_id: item.id.value,
                    });
                }
                if credential_like_reference(&item.reference) {
                    return Err(EvidenceReplayError::UnsafeReference {
                        evidence_id: item.id.value,
                    });
                }
                if !seen_evidence.insert(item.id.value.clone()) {
                    return Err(EvidenceReplayError::DuplicateEvidence {
                        evidence_id: item.id.value,
                    });
                }
                Ok(EvidenceReplayArtifact {
                    evidence_id: item.id.value,
                    kind: item.kind,
                    reference: item.reference,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut seen_activity = BTreeSet::new();
        let activity = activity
            .iter()
            .map(|event| {
                if event.organization_id != organization_id
                    || event.work_item_id.as_ref() != Some(&work_item_id)
                    || event.attempt_id.as_ref() != Some(&attempt_id)
                {
                    return Err(EvidenceReplayError::ActivityScopeMismatch {
                        event_id: event.event_id.clone(),
                    });
                }
                let correlation_id = event
                    .correlation_id
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| EvidenceReplayError::MissingCorrelation {
                        event_id: event.event_id.clone(),
                    })?
                    .to_string();
                if !seen_activity.insert(event.event_id.clone()) {
                    return Err(EvidenceReplayError::DuplicateActivity {
                        event_id: event.event_id.clone(),
                    });
                }
                Ok(EvidenceReplayActivity {
                    event_id: event.event_id.clone(),
                    correlation_id,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            schema_version: QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION,
            organization_id,
            work_item_id,
            attempt_id,
            evidence,
            activity,
        })
    }

    /// Stable JSON bytes for comparison, storage as an external artifact, or
    /// a separately governed evaluator input. This method has no side effects.
    pub fn canonical_json(&self) -> Result<Vec<u8>, EvidenceReplayError> {
        serde_json::to_vec(self).map_err(|error| EvidenceReplayError::Serialization {
            reason: error.to_string(),
        })
    }
}

fn credential_like_reference(reference: &str) -> bool {
    let value = reference.to_ascii_lowercase();
    [
        "api_key=",
        "token=",
        "password=",
        "secret=",
        "authorization:",
    ]
    .iter()
    .any(|marker| value.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActivityEventRepository, EvidenceRepository, SqliteActivityEventRepository,
        SqliteEvidenceRepository,
    };
    use altai_control_protocol::{
        ActivityQueryRequest, Actor, AttemptId, EventKind, EvidenceId, PageRequest,
    };

    fn evidence(id: &str, work: &str, attempt: &str, created_at: u64) -> Evidence {
        Evidence {
            id: EvidenceId::new(id),
            organization_id: OrganizationId::new("org_qm_084"),
            work_item_id: WorkItemId::new(work),
            attempt_id: AttemptId::new(attempt),
            kind: "test_result".into(),
            reference: format!("artifact://{id}"),
            created_at_unix_seconds: created_at,
        }
    }

    fn event(id: &str, work: &str, attempt: &str, correlation: Option<&str>) -> ActivityEvent {
        ActivityEvent {
            event_id: id.into(),
            kind: EventKind::AttemptTransitioned,
            actor: Actor::System {
                component: "qm-084-fixture".into(),
            },
            timestamp: "2031-01-02T03:04:05Z".into(),
            organization_id: OrganizationId::new("org_qm_084"),
            project_id: None,
            work_item_id: Some(WorkItemId::new(work)),
            attempt_id: Some(AttemptId::new(attempt)),
            summary: "deliberately excluded from comparison input".into(),
            correlation_id: correlation.map(str::to_string),
            causation_id: None,
        }
    }

    fn activity_request(work: &str) -> ActivityQueryRequest {
        ActivityQueryRequest {
            organization_id: OrganizationId::new("org_qm_084"),
            page: PageRequest::new(None, Some(50)),
            kind: None,
            work_item_id: Some(WorkItemId::new(work)),
        }
    }

    #[test]
    fn repository_replay_is_byte_stable_across_idempotent_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let evidence_repo = SqliteEvidenceRepository::open(&database).unwrap();
        let activity_repo = SqliteActivityEventRepository::open(&database).unwrap();
        let work = WorkItemId::new("wi_qm_084");
        let attempt = AttemptId::new("att_qm_084");
        let first = evidence("ev_qm_084_first", &work.value, &attempt.value, 20);
        let second = evidence("ev_qm_084_second", &work.value, &attempt.value, 10);
        evidence_repo.record(first.clone()).unwrap();
        evidence_repo.record(second.clone()).unwrap();
        activity_repo
            .append(event(
                "evt_qm_084_started",
                &work.value,
                &attempt.value,
                Some("run_qm_084"),
            ))
            .unwrap();
        activity_repo
            .append(event(
                "evt_qm_084_finished",
                &work.value,
                &attempt.value,
                Some("run_qm_084"),
            ))
            .unwrap();

        let before = EvidenceReplayInput::from_records(
            OrganizationId::new("org_qm_084"),
            work.clone(),
            attempt.clone(),
            &evidence_repo.list_for_work(&work).unwrap(),
            &activity_repo
                .query(&activity_request(&work.value))
                .unwrap()
                .items,
        )
        .unwrap();
        assert_eq!(
            before
                .evidence
                .iter()
                .map(|item| item.evidence_id.as_str())
                .collect::<Vec<_>>(),
            vec!["ev_qm_084_second", "ev_qm_084_first"],
            "immutable evidence repository order is normalized"
        );
        assert_eq!(
            before
                .activity
                .iter()
                .map(|item| item.event_id.as_str())
                .collect::<Vec<_>>(),
            vec!["evt_qm_084_started", "evt_qm_084_finished"],
            "activity replay retains repository sequence order"
        );
        let before_bytes = before.canonical_json().unwrap();
        assert!(!String::from_utf8_lossy(&before_bytes).contains("timestamp"));
        assert!(!String::from_utf8_lossy(&before_bytes).contains("summary"));

        drop(evidence_repo);
        let reopened = SqliteEvidenceRepository::open(&database).unwrap();
        assert_eq!(
            reopened.record(first).unwrap(),
            evidence("ev_qm_084_first", &work.value, &attempt.value, 20)
        );
        let after = EvidenceReplayInput::from_records(
            OrganizationId::new("org_qm_084"),
            work.clone(),
            attempt.clone(),
            &reopened.list_for_work(&work).unwrap(),
            &activity_repo
                .query(&activity_request(&work.value))
                .unwrap()
                .items,
        )
        .unwrap();
        assert_eq!(before_bytes, after.canonical_json().unwrap());
        assert_eq!(
            reopened.record(Evidence {
                reference: "artifact://conflict".into(),
                ..evidence("ev_qm_084_first", &work.value, &attempt.value, 20)
            }),
            Err(crate::EvidenceError::Conflict {
                evidence_id: "ev_qm_084_first".into(),
            }),
            "conflicting immutable evidence fails without changing the valid replay"
        );
    }

    #[test]
    fn scope_and_correlation_fail_closed_without_normalization() {
        let work = WorkItemId::new("wi_qm_084");
        let attempt = AttemptId::new("att_qm_084");
        let valid = evidence("ev_qm_084", &work.value, &attempt.value, 10);
        assert_eq!(
            EvidenceReplayInput::from_records(
                OrganizationId::new("org_qm_084"),
                work.clone(),
                attempt.clone(),
                std::slice::from_ref(&valid),
                &[event(
                    "evt_foreign",
                    "wi_foreign",
                    &attempt.value,
                    Some("run_qm_084")
                )],
            ),
            Err(EvidenceReplayError::ActivityScopeMismatch {
                event_id: "evt_foreign".into(),
            })
        );
        assert_eq!(
            EvidenceReplayInput::from_records(
                OrganizationId::new("org_qm_084"),
                work.clone(),
                attempt.clone(),
                &[evidence("ev_foreign", &work.value, "att_foreign", 10)],
                &[event(
                    "evt_qm_084",
                    &work.value,
                    &attempt.value,
                    Some("run_qm_084")
                )],
            ),
            Err(EvidenceReplayError::EvidenceScopeMismatch {
                evidence_id: "ev_foreign".into(),
            })
        );
        assert_eq!(
            EvidenceReplayInput::from_records(
                OrganizationId::new("org_qm_084"),
                work.clone(),
                attempt.clone(),
                &[valid],
                &[event("evt_uncorrelated", &work.value, &attempt.value, None)],
            ),
            Err(EvidenceReplayError::MissingCorrelation {
                event_id: "evt_uncorrelated".into(),
            })
        );
    }

    #[test]
    fn duplicate_source_ids_are_rejected_deterministically() {
        let work = WorkItemId::new("wi_qm_084");
        let attempt = AttemptId::new("att_qm_084");
        let record = evidence("ev_qm_084", &work.value, &attempt.value, 10);
        assert_eq!(
            EvidenceReplayInput::from_records(
                OrganizationId::new("org_qm_084"),
                work.clone(),
                attempt.clone(),
                &[record.clone(), record],
                &[event(
                    "evt_qm_084",
                    &work.value,
                    &attempt.value,
                    Some("run_qm_084")
                )],
            ),
            Err(EvidenceReplayError::DuplicateEvidence {
                evidence_id: "ev_qm_084".into(),
            })
        );
        let record = evidence("ev_qm_084", &work.value, &attempt.value, 10);
        let activity = event(
            "evt_qm_084",
            &work.value,
            &attempt.value,
            Some("run_qm_084"),
        );
        assert_eq!(
            EvidenceReplayInput::from_records(
                OrganizationId::new("org_qm_084"),
                work.clone(),
                attempt.clone(),
                &[record],
                &[activity.clone(), activity],
            ),
            Err(EvidenceReplayError::DuplicateActivity {
                event_id: "evt_qm_084".into(),
            })
        );
    }

    #[test]
    fn credential_like_references_are_not_comparison_inputs() {
        let work = WorkItemId::new("wi_qm_084");
        let attempt = AttemptId::new("att_qm_084");
        let unsafe_evidence = Evidence {
            reference: "https://example.invalid/output?token=secret-value".into(),
            ..evidence("ev_unsafe", &work.value, &attempt.value, 10)
        };
        assert_eq!(
            EvidenceReplayInput::from_records(
                OrganizationId::new("org_qm_084"),
                work.clone(),
                attempt.clone(),
                &[unsafe_evidence],
                &[event(
                    "evt_qm_084",
                    &work.value,
                    &attempt.value,
                    Some("run_qm_084")
                )],
            ),
            Err(EvidenceReplayError::UnsafeReference {
                evidence_id: "ev_unsafe".into(),
            })
        );
    }
}
