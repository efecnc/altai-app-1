//! Pure, read-only attempt evidence summary for CP-08-96.
//!
//! This module joins an already sanitized evaluation replay projection with
//! immutable usage ledger facts that share its exact Organization, Work and
//! Attempt scope. It is not an evaluator, price catalogue, dashboard store or
//! policy authority.

use crate::{EvaluationReplayProjection, QM_084_EVALUATION_PROJECTION_SCHEMA_VERSION};
use altai_control_protocol::UsageRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Schema version for the `QM-084-evaluation-attempt-summary-v1` read model.
pub const QM_084_EVALUATION_ATTEMPT_SUMMARY_SCHEMA_VERSION: u16 = 1;

/// One named immutable usage meter total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageMeterTotal {
    pub meter: String,
    pub total_amount: u64,
}

/// Whether exact-scope cost evidence was recorded for an attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CostEvidence {
    /// No exact-scope immutable usage record exists. This is not a zero-cost
    /// assertion and must never be rendered as an estimate.
    Unavailable,
    /// Deterministic totals grouped only by the ledger's named meter.
    Available { meters: Vec<UsageMeterTotal> },
}

/// A non-authoritative, deterministic attempt summary suitable for a later
/// dashboard surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationAttemptSummary {
    pub schema_version: u16,
    pub projection: EvaluationReplayProjection,
    pub cost_evidence: CostEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvaluationSummaryError {
    UnsupportedProjectionSchema { actual: u16 },
    MissingUsageAttribution { usage_record_id: String },
    UsageScopeMismatch { usage_record_id: String },
    DuplicateUsage { usage_record_id: String },
    EmptyMeter { usage_record_id: String },
    MeterOverflow { meter: String },
    Serialization { reason: String },
}

impl std::fmt::Display for EvaluationSummaryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedProjectionSchema { actual } => write!(
                f,
                "evaluation attempt summary requires projection schema {}, got {}",
                QM_084_EVALUATION_PROJECTION_SCHEMA_VERSION, actual
            ),
            Self::MissingUsageAttribution { usage_record_id } => write!(
                f,
                "usage record {usage_record_id} lacks exact work and attempt attribution"
            ),
            Self::UsageScopeMismatch { usage_record_id } => write!(
                f,
                "usage record {usage_record_id} is outside the projected organization/work/attempt"
            ),
            Self::DuplicateUsage { usage_record_id } => {
                write!(
                    f,
                    "evaluation summary contains duplicate usage id {usage_record_id}"
                )
            }
            Self::EmptyMeter { usage_record_id } => {
                write!(f, "usage record {usage_record_id} has an empty meter")
            }
            Self::MeterOverflow { meter } => {
                write!(
                    f,
                    "usage meter {meter} overflows while forming an evaluation summary"
                )
            }
            Self::Serialization { reason } => {
                write!(
                    f,
                    "evaluation attempt summary serialization failed: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for EvaluationSummaryError {}

impl EvaluationAttemptSummary {
    /// Combine one replay projection with exact-scope immutable usage facts.
    ///
    /// Callers query the ledger before this function. The function performs no
    /// I/O and cannot write a ledger record, mutate an Attempt or emit a score.
    pub fn from_projection_and_usage(
        projection: EvaluationReplayProjection,
        usage: &[UsageRecord],
    ) -> Result<Self, EvaluationSummaryError> {
        if projection.schema_version != QM_084_EVALUATION_PROJECTION_SCHEMA_VERSION {
            return Err(EvaluationSummaryError::UnsupportedProjectionSchema {
                actual: projection.schema_version,
            });
        }

        let mut usage_ids = BTreeSet::new();
        let mut totals = BTreeMap::<String, u64>::new();
        for record in usage {
            let usage_record_id = record.id.value.clone();
            if !usage_ids.insert(usage_record_id.clone()) {
                return Err(EvaluationSummaryError::DuplicateUsage { usage_record_id });
            }
            let work_item_id = record.scope.work_item_id.as_ref().ok_or_else(|| {
                EvaluationSummaryError::MissingUsageAttribution {
                    usage_record_id: usage_record_id.clone(),
                }
            })?;
            let attempt_id = record.scope.attempt_id.as_ref().ok_or_else(|| {
                EvaluationSummaryError::MissingUsageAttribution {
                    usage_record_id: usage_record_id.clone(),
                }
            })?;
            if record.scope.organization_id != projection.organization_id
                || work_item_id != &projection.work_item_id
                || attempt_id != &projection.attempt_id
            {
                return Err(EvaluationSummaryError::UsageScopeMismatch { usage_record_id });
            }
            if record.meter.is_empty() {
                return Err(EvaluationSummaryError::EmptyMeter { usage_record_id });
            }
            let total = totals.entry(record.meter.clone()).or_default();
            *total = total.checked_add(record.amount).ok_or_else(|| {
                EvaluationSummaryError::MeterOverflow {
                    meter: record.meter.clone(),
                }
            })?;
        }

        let cost_evidence = if totals.is_empty() {
            CostEvidence::Unavailable
        } else {
            CostEvidence::Available {
                meters: totals
                    .into_iter()
                    .map(|(meter, total_amount)| UsageMeterTotal {
                        meter,
                        total_amount,
                    })
                    .collect(),
            }
        };
        Ok(Self {
            schema_version: QM_084_EVALUATION_ATTEMPT_SUMMARY_SCHEMA_VERSION,
            projection,
            cost_evidence,
        })
    }

    /// Stable bytes for a later read-only consumer; this performs no I/O.
    pub fn canonical_json(&self) -> Result<Vec<u8>, EvaluationSummaryError> {
        serde_json::to_vec(self).map_err(|error| EvaluationSummaryError::Serialization {
            reason: error.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use altai_control_protocol::{
        AttemptId, OrganizationId, UsageRecordId, UsageScope, WorkItemId,
    };

    fn projection() -> EvaluationReplayProjection {
        EvaluationReplayProjection {
            schema_version: QM_084_EVALUATION_PROJECTION_SCHEMA_VERSION,
            source_schema_version: 1,
            organization_id: OrganizationId::new("org_qm_084"),
            work_item_id: WorkItemId::new("wi_qm_084"),
            attempt_id: AttemptId::new("att_qm_084"),
            evidence_count: 2,
            activity_count: 3,
            unique_correlation_count: 1,
            evidence_kinds: vec!["test_result".into()],
        }
    }

    fn usage(id: &str, meter: &str, amount: u64) -> UsageRecord {
        UsageRecord {
            id: UsageRecordId::new(id),
            scope: UsageScope {
                organization_id: OrganizationId::new("org_qm_084"),
                project_id: None,
                agent_instance_id: None,
                work_item_id: Some(WorkItemId::new("wi_qm_084")),
                attempt_id: Some(AttemptId::new("att_qm_084")),
            },
            meter: meter.into(),
            amount,
            recorded_at_unix_seconds: 10,
        }
    }

    #[test]
    fn exact_scope_usage_is_summed_deterministically_without_a_price() {
        let summary = EvaluationAttemptSummary::from_projection_and_usage(
            projection(),
            &[
                usage("usage_second", "input_tokens", 12),
                usage("usage_first", "compute_seconds", 4),
                usage("usage_third", "input_tokens", 8),
            ],
        )
        .unwrap();
        assert_eq!(
            summary.cost_evidence,
            CostEvidence::Available {
                meters: vec![
                    UsageMeterTotal {
                        meter: "compute_seconds".into(),
                        total_amount: 4,
                    },
                    UsageMeterTotal {
                        meter: "input_tokens".into(),
                        total_amount: 20,
                    },
                ],
            }
        );
        let bytes = summary.canonical_json().unwrap();
        assert_eq!(bytes, summary.canonical_json().unwrap());
        let text = String::from_utf8(bytes).unwrap();
        assert!(!text.contains("price"));
        assert!(!text.contains("score"));
    }

    #[test]
    fn absent_usage_is_explicitly_unavailable_not_zero() {
        let summary =
            EvaluationAttemptSummary::from_projection_and_usage(projection(), &[]).unwrap();
        assert_eq!(summary.cost_evidence, CostEvidence::Unavailable);
    }

    #[test]
    fn foreign_partial_duplicate_and_overflow_usage_fail_closed() {
        let mut foreign = usage("usage_foreign", "input_tokens", 1);
        foreign.scope.organization_id = OrganizationId::new("org_foreign");
        assert_eq!(
            EvaluationAttemptSummary::from_projection_and_usage(projection(), &[foreign]),
            Err(EvaluationSummaryError::UsageScopeMismatch {
                usage_record_id: "usage_foreign".into(),
            })
        );

        let mut partial = usage("usage_partial", "input_tokens", 1);
        partial.scope.attempt_id = None;
        assert_eq!(
            EvaluationAttemptSummary::from_projection_and_usage(projection(), &[partial]),
            Err(EvaluationSummaryError::MissingUsageAttribution {
                usage_record_id: "usage_partial".into(),
            })
        );

        let duplicate = usage("usage_duplicate", "input_tokens", 1);
        assert_eq!(
            EvaluationAttemptSummary::from_projection_and_usage(
                projection(),
                &[duplicate.clone(), duplicate],
            ),
            Err(EvaluationSummaryError::DuplicateUsage {
                usage_record_id: "usage_duplicate".into(),
            })
        );

        assert_eq!(
            EvaluationAttemptSummary::from_projection_and_usage(
                projection(),
                &[
                    usage("usage_max", "input_tokens", u64::MAX),
                    usage("usage_one", "input_tokens", 1)
                ],
            ),
            Err(EvaluationSummaryError::MeterOverflow {
                meter: "input_tokens".into(),
            })
        );
    }
}
