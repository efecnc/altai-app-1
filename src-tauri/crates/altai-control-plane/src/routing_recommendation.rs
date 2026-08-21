//! Deterministic, non-authoritative routing recommendations for CP-08-98.
//!
//! This module only orders caller-supplied candidate snapshots. It neither
//! reads repositories nor selects, claims, dispatches, persists or learns.

use altai_control_protocol::{AgentInstanceId, WorkItemId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const ROUTING_RECOMMENDATION_SCHEMA_VERSION: u16 = 1;

/// A named hard blocker copied from an existing eligibility/governance result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoutingBlocker {
    AgentUnavailable { agent_instance_id: String },
    DependencyIncomplete { work_item_id: String },
    BudgetStopped { scope: String },
    GovernancePending { approval_id: String },
    GovernanceDenied { approval_id: String },
}

/// One immutable caller-supplied recommendation candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingCandidate {
    pub work_item_id: WorkItemId,
    pub agent_instance_id: AgentInstanceId,
    /// Explicit stable priority; lower values rank first. It is not a score.
    pub priority_key: u64,
    pub blockers: Vec<RoutingBlocker>,
}

/// Candidate explanation retained by the read-only recommendation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingCandidateView {
    pub agent_instance_id: AgentInstanceId,
    pub priority_key: u64,
    pub blockers: Vec<RoutingBlocker>,
}

/// Ordered eligible candidates plus visible ineligible candidates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingRecommendation {
    pub schema_version: u16,
    pub work_item_id: WorkItemId,
    pub eligible: Vec<RoutingCandidateView>,
    pub ineligible: Vec<RoutingCandidateView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingRecommendationError {
    CandidateWorkMismatch { agent_instance_id: String },
    DuplicateAgent { agent_instance_id: String },
    Serialization { reason: String },
}

impl std::fmt::Display for RoutingRecommendationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CandidateWorkMismatch { agent_instance_id } => write!(
                f,
                "routing candidate {agent_instance_id} belongs to a different work item"
            ),
            Self::DuplicateAgent { agent_instance_id } => {
                write!(
                    f,
                    "routing recommendation has duplicate agent {agent_instance_id}"
                )
            }
            Self::Serialization { reason } => {
                write!(f, "routing recommendation serialization failed: {reason}")
            }
        }
    }
}
impl std::error::Error for RoutingRecommendationError {}

impl RoutingRecommendation {
    /// Form a reproducible suggestion without taking any execution action.
    pub fn from_candidates(
        work_item_id: WorkItemId,
        candidates: &[RoutingCandidate],
    ) -> Result<Self, RoutingRecommendationError> {
        let mut seen_agents = BTreeSet::new();
        let mut eligible = Vec::new();
        let mut ineligible = Vec::new();
        for candidate in candidates {
            let agent_instance_id = candidate.agent_instance_id.value.clone();
            if candidate.work_item_id != work_item_id {
                return Err(RoutingRecommendationError::CandidateWorkMismatch {
                    agent_instance_id,
                });
            }
            if !seen_agents.insert(agent_instance_id.clone()) {
                return Err(RoutingRecommendationError::DuplicateAgent { agent_instance_id });
            }
            let view = RoutingCandidateView {
                agent_instance_id: candidate.agent_instance_id.clone(),
                priority_key: candidate.priority_key,
                blockers: candidate.blockers.clone(),
            };
            if view.blockers.is_empty() {
                eligible.push(view);
            } else {
                ineligible.push(view);
            }
        }
        let by_priority_then_agent = |left: &RoutingCandidateView, right: &RoutingCandidateView| {
            left.priority_key.cmp(&right.priority_key).then_with(|| {
                left.agent_instance_id
                    .value
                    .cmp(&right.agent_instance_id.value)
            })
        };
        eligible.sort_by(by_priority_then_agent);
        ineligible.sort_by(by_priority_then_agent);
        Ok(Self {
            schema_version: ROUTING_RECOMMENDATION_SCHEMA_VERSION,
            work_item_id,
            eligible,
            ineligible,
        })
    }

    pub fn canonical_json(&self) -> Result<Vec<u8>, RoutingRecommendationError> {
        serde_json::to_vec(self).map_err(|error| RoutingRecommendationError::Serialization {
            reason: error.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(agent: &str, priority: u64, blockers: Vec<RoutingBlocker>) -> RoutingCandidate {
        RoutingCandidate {
            work_item_id: WorkItemId::new("wi_route"),
            agent_instance_id: AgentInstanceId::new(agent),
            priority_key: priority,
            blockers,
        }
    }

    #[test]
    fn sorts_eligible_candidates_and_preserves_hard_blockers() {
        let recommendation = RoutingRecommendation::from_candidates(
            WorkItemId::new("wi_route"),
            &[
                candidate("agent_b", 2, vec![]),
                candidate("agent_a", 2, vec![]),
                candidate(
                    "agent_blocked",
                    0,
                    vec![RoutingBlocker::BudgetStopped {
                        scope: "org=org_route".into(),
                    }],
                ),
            ],
        )
        .unwrap();
        assert_eq!(
            recommendation
                .eligible
                .iter()
                .map(|candidate| candidate.agent_instance_id.value.as_str())
                .collect::<Vec<_>>(),
            vec!["ai_agent_a", "ai_agent_b"]
        );
        assert_eq!(recommendation.ineligible.len(), 1);
        assert!(matches!(
            recommendation.ineligible[0].blockers.as_slice(),
            [RoutingBlocker::BudgetStopped { .. }]
        ));
        assert_eq!(
            recommendation.canonical_json().unwrap(),
            recommendation.canonical_json().unwrap()
        );
    }

    #[test]
    fn foreign_work_and_duplicate_agent_fail_closed() {
        let mut foreign = candidate("agent_foreign", 1, vec![]);
        foreign.work_item_id = WorkItemId::new("wi_foreign");
        assert_eq!(
            RoutingRecommendation::from_candidates(WorkItemId::new("wi_route"), &[foreign]),
            Err(RoutingRecommendationError::CandidateWorkMismatch {
                agent_instance_id: "ai_agent_foreign".into(),
            })
        );
        let duplicate = candidate("agent_duplicate", 1, vec![]);
        assert_eq!(
            RoutingRecommendation::from_candidates(
                WorkItemId::new("wi_route"),
                &[duplicate.clone(), duplicate],
            ),
            Err(RoutingRecommendationError::DuplicateAgent {
                agent_instance_id: "ai_agent_duplicate".into(),
            })
        );
    }
}
