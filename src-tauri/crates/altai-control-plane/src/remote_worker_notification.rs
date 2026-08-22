//! Deterministic remote-worker notification proposal ledger (CP-08-100,
//! package 093 PR 2). An untrusted remote worker may propose an attributed
//! event; this module stores the proposal insert-only and moves delivery
//! state only on a canonical control-plane acknowledgement. The proposer
//! never obtains a credential, cannot touch Attempt state, and can never be
//! the source of the fact that a notification was delivered.
//!
//! Like the routing recommendation, this module performs no repository or
//! transport I/O; the durable store and real delivery land with the deployed
//! adapter in a later package 093 PR.

use altai_control_protocol::{ExternalAccountId, OrganizationId, PluginId, WorkspaceId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const REMOTE_WORKER_NOTIFICATION_SCHEMA_VERSION: u16 = 1;

/// A proposal payload stays bounded like every other control-plane pack.
pub const MAX_PROPOSAL_PAYLOAD_BYTES: usize = 4096;

/// The already-authenticated registration identity of the proposing worker.
/// This fixture receives it as caller-supplied fact; issuing or brokering a
/// credential is never part of any path here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteWorkerIdentity {
    pub plugin_id: PluginId,
    pub account_id: ExternalAccountId,
}

/// Org/workspace scope a proposal must match exactly; foreign scopes fail
/// closed instead of being stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationScope {
    pub organization_id: OrganizationId,
    pub workspace_id: WorkspaceId,
}

/// One proposed notification from an untrusted remote worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationProposal {
    /// Caller-supplied idempotency key for the eventual delivery.
    pub delivery_id: String,
    pub worker: RemoteWorkerIdentity,
    pub scope: NotificationScope,
    /// Explicit stable kind; free-form classification is not derived here.
    pub event_kind: String,
    pub payload: serde_json::Value,
}

/// Delivery state. `Delivered` is reachable only through
/// [`NotificationProposalLedger::acknowledge`] — a worker self-report never
/// produces this transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationDeliveryState {
    Pending,
    Delivered,
}

/// The stored form of one accepted proposal. Insert-only: an identical
/// re-proposal is a no-op and a divergent re-proposal is a conflict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationProposalRecord {
    pub schema_version: u16,
    pub delivery_id: String,
    pub worker: RemoteWorkerIdentity,
    pub scope: NotificationScope,
    pub event_kind: String,
    pub payload: serde_json::Value,
    pub delivery_state: NotificationDeliveryState,
}

/// A worker's self-report that it delivered something. It is recorded so the
/// claim is visible, and nothing else: the canonical acknowledgement alone
/// decides the delivery state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDeliveryClaim {
    pub delivery_id: String,
    pub worker: RemoteWorkerIdentity,
}

/// Canonical control-plane acknowledgement — the only path to `Delivered`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationAcknowledgement {
    pub delivery_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteWorkerNotificationError {
    /// A required attribution field is blank; an unattributed event is not a
    /// proposal.
    InvalidProposal {
        reason: String,
    },
    ForeignScope {
        delivery_id: String,
    },
    UnboundedPayload {
        delivery_id: String,
    },
    DuplicateConflict {
        delivery_id: String,
    },
    UnknownDelivery {
        delivery_id: String,
    },
    Serialization {
        reason: String,
    },
    /// The durable store hit a SQLite-level fault (open, statement, or
    /// decode of stored state); the ledger itself never produces this.
    Database {
        reason: String,
    },
}

impl std::fmt::Display for RemoteWorkerNotificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProposal { reason } => {
                write!(f, "remote-worker proposal is invalid: {reason}")
            }
            Self::ForeignScope { delivery_id } => write!(
                f,
                "remote-worker proposal {delivery_id} belongs to a foreign scope"
            ),
            Self::UnboundedPayload { delivery_id } => write!(
                f,
                "remote-worker proposal {delivery_id} exceeds the bounded payload size"
            ),
            Self::DuplicateConflict { delivery_id } => write!(
                f,
                "remote-worker proposal {delivery_id} conflicts with the stored record"
            ),
            Self::UnknownDelivery { delivery_id } => write!(
                f,
                "remote-worker delivery {delivery_id} is not known to the ledger"
            ),
            Self::Serialization { reason } => {
                write!(
                    f,
                    "remote-worker notification serialization failed: {reason}"
                )
            }
            Self::Database { reason } => {
                write!(f, "remote-worker notification store failed: {reason}")
            }
        }
    }
}
impl std::error::Error for RemoteWorkerNotificationError {}

/// Shared field validation and payload bounding for both the pure ledger and
/// the durable store: an unattributed, unserializable, or oversized proposal
/// is rejected before any storage layer — in-memory or SQLite — ever sees it.
/// Returns the serialized payload size on success.
pub(crate) fn validated_payload(
    proposal: &NotificationProposal,
) -> Result<usize, RemoteWorkerNotificationError> {
    if proposal.delivery_id.is_empty() {
        return Err(RemoteWorkerNotificationError::InvalidProposal {
            reason: "delivery_id is blank".to_string(),
        });
    }
    if proposal.event_kind.trim().is_empty() {
        return Err(RemoteWorkerNotificationError::InvalidProposal {
            reason: "event_kind is blank".to_string(),
        });
    }
    if proposal.worker.plugin_id.value.is_empty() || proposal.worker.account_id.value.is_empty() {
        return Err(RemoteWorkerNotificationError::InvalidProposal {
            reason: "worker identity is incomplete".to_string(),
        });
    }
    let payload_bytes = serde_json::to_vec(&proposal.payload).map_err(|error| {
        RemoteWorkerNotificationError::Serialization {
            reason: error.to_string(),
        }
    })?;
    if payload_bytes.len() > MAX_PROPOSAL_PAYLOAD_BYTES {
        return Err(RemoteWorkerNotificationError::UnboundedPayload {
            delivery_id: proposal.delivery_id.clone(),
        });
    }
    Ok(payload_bytes.len())
}

/// Pure, insertion-ordered proposal ledger for one org/workspace scope.
/// Records are keyed by `delivery_id` and iterated in lexicographic order, so
/// identical input histories produce byte-stable output regardless of arrival
/// order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationProposalLedger {
    scope: NotificationScope,
    proposals: BTreeMap<String, NotificationProposalRecord>,
    claims: BTreeMap<String, Vec<WorkerDeliveryClaim>>,
}

impl NotificationProposalLedger {
    pub fn new(scope: NotificationScope) -> Self {
        Self {
            scope,
            proposals: BTreeMap::new(),
            claims: BTreeMap::new(),
        }
    }

    /// Validate and store one proposal. An identical re-proposal returns the
    /// stored record unchanged; a divergent re-proposal fails closed without
    /// touching the stored record.
    pub fn propose(
        &mut self,
        proposal: NotificationProposal,
    ) -> Result<NotificationProposalRecord, RemoteWorkerNotificationError> {
        let delivery_id = proposal.delivery_id.clone();
        validated_payload(&proposal)?;
        if proposal.scope != self.scope {
            return Err(RemoteWorkerNotificationError::ForeignScope { delivery_id });
        }

        let record = NotificationProposalRecord {
            schema_version: REMOTE_WORKER_NOTIFICATION_SCHEMA_VERSION,
            delivery_id: delivery_id.clone(),
            worker: proposal.worker,
            scope: proposal.scope,
            event_kind: proposal.event_kind,
            payload: proposal.payload,
            delivery_state: NotificationDeliveryState::Pending,
        };
        match self.proposals.get(&delivery_id) {
            Some(existing)
                if existing.worker == record.worker
                    && existing.scope == record.scope
                    && existing.event_kind == record.event_kind
                    && existing.payload == record.payload =>
            {
                Ok(existing.clone())
            }
            Some(_) => Err(RemoteWorkerNotificationError::DuplicateConflict { delivery_id }),
            None => {
                self.proposals.insert(delivery_id, record.clone());
                Ok(record)
            }
        }
    }

    /// Record a worker self-report as visible provenance. The delivery state
    /// never moves here — by construction there is no path from this method
    /// to [`NotificationDeliveryState::Delivered`].
    ///
    /// Returns the total number of recorded claims for the delivery.
    pub fn observe_worker_claim(
        &mut self,
        claim: WorkerDeliveryClaim,
    ) -> Result<usize, RemoteWorkerNotificationError> {
        if !self.proposals.contains_key(&claim.delivery_id) {
            return Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: claim.delivery_id,
            });
        }
        let claims = self.claims.entry(claim.delivery_id.clone()).or_default();
        claims.push(claim);
        Ok(claims.len())
    }

    /// Apply the canonical control-plane acknowledgement. Exactly-once:
    /// acknowledging a pending delivery transitions it once, and replaying
    /// the same acknowledgement is a no-op.
    pub fn acknowledge(
        &mut self,
        acknowledgement: NotificationAcknowledgement,
    ) -> Result<NotificationDeliveryState, RemoteWorkerNotificationError> {
        let record = self.proposals.get_mut(&acknowledgement.delivery_id).ok_or(
            RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: acknowledgement.delivery_id,
            },
        )?;
        record.delivery_state = NotificationDeliveryState::Delivered;
        Ok(NotificationDeliveryState::Delivered)
    }

    /// Stored records in deterministic `delivery_id` order.
    pub fn records(&self) -> impl Iterator<Item = &NotificationProposalRecord> {
        self.proposals.values()
    }

    /// Visible worker claims for one delivery, in arrival order.
    pub fn worker_claims(&self, delivery_id: &str) -> &[WorkerDeliveryClaim] {
        self.claims
            .get(delivery_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Byte-stable serialization of the whole ledger state.
    pub fn canonical_json(&self) -> Result<Vec<u8>, RemoteWorkerNotificationError> {
        // Collect through serde_json's sorted object keys so the output does
        // not depend on map iteration details.
        let snapshot: Vec<&NotificationProposalRecord> = self.records().collect();
        serde_json::to_vec(&snapshot).map_err(|error| {
            RemoteWorkerNotificationError::Serialization {
                reason: error.to_string(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scope() -> NotificationScope {
        NotificationScope {
            organization_id: OrganizationId::new("org_main"),
            workspace_id: WorkspaceId::new("ws_main"),
        }
    }

    fn worker() -> RemoteWorkerIdentity {
        RemoteWorkerIdentity {
            plugin_id: PluginId::new("plg_remote"),
            account_id: ExternalAccountId::new("exta_one"),
        }
    }

    fn proposal(delivery_id: &str) -> NotificationProposal {
        NotificationProposal {
            delivery_id: delivery_id.to_string(),
            worker: worker(),
            scope: scope(),
            event_kind: "heartbeat".to_string(),
            payload: json!({ "note": "alive" }),
        }
    }

    #[test]
    fn stores_proposal_pending_and_replays_identical_proposal_idempotently() {
        let mut ledger = NotificationProposalLedger::new(scope());
        let first = ledger.propose(proposal("dlv_1")).expect("store");
        assert_eq!(first.delivery_state, NotificationDeliveryState::Pending);
        let second = ledger.propose(proposal("dlv_1")).expect("replay");
        assert_eq!(first, second);
        assert_eq!(ledger.records().count(), 1);
    }

    #[test]
    fn foreign_scope_fails_closed_without_storing() {
        let mut ledger = NotificationProposalLedger::new(scope());
        let mut foreign = proposal("dlv_foreign");
        foreign.scope.workspace_id = WorkspaceId::new("ws_other");
        assert_eq!(
            ledger.propose(foreign),
            Err(RemoteWorkerNotificationError::ForeignScope {
                delivery_id: "dlv_foreign".to_string(),
            })
        );
        assert_eq!(ledger.records().count(), 0);
        // Validation deliberately precedes scope checking, so a foreign-scope
        // proposal with an oversized payload reports the payload fault. This
        // pins that order so a refactor cannot silently swap it.
        let mut bloated_foreign = proposal("dlv_bloated_foreign");
        bloated_foreign.scope.workspace_id = WorkspaceId::new("ws_other");
        bloated_foreign.payload = json!({ "blob": "x".repeat(MAX_PROPOSAL_PAYLOAD_BYTES + 1) });
        assert!(matches!(
            ledger.propose(bloated_foreign),
            Err(RemoteWorkerNotificationError::UnboundedPayload { .. })
        ));
        assert_eq!(ledger.records().count(), 0);
    }

    #[test]
    fn unbounded_payload_is_rejected() {
        let mut ledger = NotificationProposalLedger::new(scope());
        let mut bloated = proposal("dlv_big");
        bloated.payload = json!({ "blob": "x".repeat(MAX_PROPOSAL_PAYLOAD_BYTES + 1) });
        assert_eq!(
            ledger.propose(bloated),
            Err(RemoteWorkerNotificationError::UnboundedPayload {
                delivery_id: "dlv_big".to_string(),
            })
        );
    }

    #[test]
    fn unattributed_proposals_fail_closed() {
        let mut ledger = NotificationProposalLedger::new(scope());
        let mut blank_id = proposal("");
        blank_id.event_kind = "heartbeat".to_string();
        assert!(matches!(
            ledger.propose(blank_id),
            Err(RemoteWorkerNotificationError::InvalidProposal { .. })
        ));
        let mut blank_kind = proposal("dlv_kind");
        blank_kind.event_kind = "   ".to_string();
        assert!(matches!(
            ledger.propose(blank_kind),
            Err(RemoteWorkerNotificationError::InvalidProposal { .. })
        ));
    }

    #[test]
    fn divergent_reproposal_conflicts_and_keeps_stored_record() {
        let mut ledger = NotificationProposalLedger::new(scope());
        ledger.propose(proposal("dlv_1")).expect("store");
        let mut divergent = proposal("dlv_1");
        divergent.payload = json!({ "note": "changed" });
        assert_eq!(
            ledger.propose(divergent),
            Err(RemoteWorkerNotificationError::DuplicateConflict {
                delivery_id: "dlv_1".to_string(),
            })
        );
        assert_eq!(
            ledger.records().next().expect("stored").payload,
            json!({ "note": "alive" }),
        );
    }

    #[test]
    fn worker_self_report_never_moves_delivery_state() {
        let mut ledger = NotificationProposalLedger::new(scope());
        ledger.propose(proposal("dlv_1")).expect("store");
        let claim_count = ledger
            .observe_worker_claim(WorkerDeliveryClaim {
                delivery_id: "dlv_1".to_string(),
                worker: worker(),
            })
            .expect("claim visible");
        assert_eq!(claim_count, 1);
        assert_eq!(
            ledger.records().next().expect("stored").delivery_state,
            NotificationDeliveryState::Pending,
        );
        assert_eq!(ledger.worker_claims("dlv_1").len(), 1);
    }

    #[test]
    fn canonical_acknowledgement_delivers_exactly_once() {
        let mut ledger = NotificationProposalLedger::new(scope());
        ledger.propose(proposal("dlv_1")).expect("store");
        let ack = NotificationAcknowledgement {
            delivery_id: "dlv_1".to_string(),
        };
        assert_eq!(
            ledger.acknowledge(ack.clone()).expect("deliver"),
            NotificationDeliveryState::Delivered,
        );
        // Replaying the same acknowledgement is a no-op, not a second
        // transition or an error.
        assert_eq!(
            ledger.acknowledge(ack).expect("replay"),
            NotificationDeliveryState::Delivered,
        );
        assert_eq!(
            ledger.records().next().expect("stored").delivery_state,
            NotificationDeliveryState::Delivered,
        );
    }

    #[test]
    fn unknown_deliveries_are_typed_rejections() {
        let mut ledger = NotificationProposalLedger::new(scope());
        assert_eq!(
            ledger.observe_worker_claim(WorkerDeliveryClaim {
                delivery_id: "dlv_missing".to_string(),
                worker: worker(),
            }),
            Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: "dlv_missing".to_string(),
            })
        );
        assert_eq!(
            ledger.acknowledge(NotificationAcknowledgement {
                delivery_id: "dlv_missing".to_string(),
            }),
            Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: "dlv_missing".to_string(),
            })
        );
    }

    #[test]
    fn canonical_output_is_byte_stable_across_insertion_orders() {
        let mut first = NotificationProposalLedger::new(scope());
        first.propose(proposal("dlv_b")).expect("store b");
        first.propose(proposal("dlv_a")).expect("store a");

        let mut second = NotificationProposalLedger::new(scope());
        second.propose(proposal("dlv_a")).expect("store a");
        second.propose(proposal("dlv_b")).expect("store b");

        assert_eq!(first.canonical_json(), second.canonical_json());
    }
}
