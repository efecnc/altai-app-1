//! Durable remote-worker notification store (CP-08-101, package 093 PR 3).
//!
//! Persists the CP-08-100 proposal ledger into the local `work.db` without
//! weakening any of its invariants: proposals stay insert-only (identical
//! replay is a no-op, divergent replay conflicts), a delivery's scope
//! attribution is fixed by the first accepted proposal and never rewritten,
//! a worker self-report stays visible provenance that never moves delivery
//! state, and `Delivered` remains reachable only through the canonical
//! acknowledgement keyed by `delivery_id`. Like the pure ledger, this store
//! issues no credential and has no path into Attempt state.

use crate::remote_worker_notification::{
    validated_payload, NotificationAcknowledgement, NotificationDeliveryState,
    NotificationProposal, NotificationProposalRecord, NotificationScope,
    RemoteWorkerNotificationError, WorkerDeliveryClaim, REMOTE_WORKER_NOTIFICATION_SCHEMA_VERSION,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::{path::Path, sync::Mutex};

/// The durable counterpart of [`crate::remote_worker_notification::NotificationProposalLedger`]:
/// every ledger invariant holds here too, but the records survive a restart.
pub trait NotificationProposalRepository: Send + Sync {
    /// Validate and store one proposal. An identical re-proposal returns the
    /// stored record unchanged without writing; a divergent re-proposal fails
    /// closed without touching the stored row.
    fn propose(
        &self,
        proposal: NotificationProposal,
    ) -> Result<NotificationProposalRecord, RemoteWorkerNotificationError>;

    /// Record a worker self-report as visible provenance. The delivery state
    /// never moves here. The claim must address a delivery owned by `scope`:
    /// workers are untrusted, so their self-reports are scope-contained.
    /// Returns the total number of recorded claims for the delivery.
    fn observe_worker_claim(
        &self,
        scope: &NotificationScope,
        claim: WorkerDeliveryClaim,
    ) -> Result<usize, RemoteWorkerNotificationError>;

    /// Apply the canonical control-plane acknowledgement. Exactly-once:
    /// acknowledging a pending delivery transitions it once, and replaying
    /// the same acknowledgement is a no-op.
    ///
    /// Trust assumption: the acknowledgement arrives from the trusted
    /// control plane, so unlike worker-sourced claims (untrusted,
    /// scope-contained) it is deliberately not scope-gated — `delivery_id`
    /// alone addresses the row.
    fn acknowledge(
        &self,
        acknowledgement: NotificationAcknowledgement,
    ) -> Result<NotificationDeliveryState, RemoteWorkerNotificationError>;

    /// Stored records for one scope in deterministic `delivery_id` order, so
    /// identical histories produce byte-stable output regardless of arrival
    /// order.
    fn records(
        &self,
        scope: &NotificationScope,
    ) -> Result<Vec<NotificationProposalRecord>, RemoteWorkerNotificationError>;

    /// Visible worker claims for one delivery, in arrival order. An unknown
    /// delivery, or one outside the requested scope, reads as empty: this is
    /// not a discovery surface, `records` is.
    fn worker_claims(
        &self,
        scope: &NotificationScope,
        delivery_id: &str,
    ) -> Result<Vec<WorkerDeliveryClaim>, RemoteWorkerNotificationError>;
}

pub struct SqliteNotificationProposalRepository {
    connection: Mutex<Connection>,
}

impl SqliteNotificationProposalRepository {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection
            .execute_batch(
                "PRAGMA busy_timeout = 5000;
                 CREATE TABLE IF NOT EXISTS control_plane_notification_proposals (
                   delivery_id TEXT PRIMARY KEY,
                   organization_id TEXT NOT NULL,
                   workspace_id TEXT NOT NULL,
                   payload_json TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS control_plane_worker_delivery_claims (
                   delivery_id TEXT NOT NULL,
                   claim_ordinal INTEGER NOT NULL,
                   claim_json TEXT NOT NULL,
                   PRIMARY KEY (delivery_id, claim_ordinal)
                 );",
            )
            .map_err(|e| e.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, RemoteWorkerNotificationError> {
        self.connection
            .lock()
            .map_err(|_| RemoteWorkerNotificationError::Database {
                reason: "sqlite notification repository lock poisoned".into(),
            })
    }

    fn db(e: rusqlite::Error) -> RemoteWorkerNotificationError {
        RemoteWorkerNotificationError::Database {
            reason: e.to_string(),
        }
    }

    /// Stored state was written by this module; failing to read it back is a
    /// storage-level fault, not a caller error. A row written by a future
    /// format must fail closed rather than be lossily rewritten by an older
    /// writer, so a mismatched `schema_version` is rejected here.
    fn decode_proposal(
        payload: String,
    ) -> Result<NotificationProposalRecord, RemoteWorkerNotificationError> {
        let record: NotificationProposalRecord = serde_json::from_str(&payload).map_err(|e| {
            RemoteWorkerNotificationError::Database {
                reason: format!("notification proposal payload decode failed: {e}"),
            }
        })?;
        if record.schema_version != REMOTE_WORKER_NOTIFICATION_SCHEMA_VERSION {
            return Err(RemoteWorkerNotificationError::Database {
                reason: format!(
                    "notification proposal {} has schema version {}, expected {}",
                    record.delivery_id,
                    record.schema_version,
                    REMOTE_WORKER_NOTIFICATION_SCHEMA_VERSION
                ),
            });
        }
        Ok(record)
    }

    fn decode_claim(
        claim_json: String,
    ) -> Result<WorkerDeliveryClaim, RemoteWorkerNotificationError> {
        serde_json::from_str(&claim_json).map_err(|e| RemoteWorkerNotificationError::Database {
            reason: format!("worker delivery claim decode failed: {e}"),
        })
    }

    fn encode<T: serde::Serialize>(value: &T) -> Result<String, RemoteWorkerNotificationError> {
        serde_json::to_string(value).map_err(|error| RemoteWorkerNotificationError::Serialization {
            reason: error.to_string(),
        })
    }
}

impl NotificationProposalRepository for SqliteNotificationProposalRepository {
    fn propose(
        &self,
        proposal: NotificationProposal,
    ) -> Result<NotificationProposalRecord, RemoteWorkerNotificationError> {
        // Validation precedes any transaction: an invalid or oversized
        // proposal never opens a write window at all.
        validated_payload(&proposal)?;

        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        let stored_payload: Option<String> = transaction
            .query_row(
                "SELECT payload_json FROM control_plane_notification_proposals
                 WHERE delivery_id = ?1",
                params![proposal.delivery_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        let Some(payload_json) = stored_payload else {
            // First proposal wins: its scope becomes the delivery's permanent
            // attribution.
            let record = NotificationProposalRecord {
                schema_version: REMOTE_WORKER_NOTIFICATION_SCHEMA_VERSION,
                delivery_id: proposal.delivery_id.clone(),
                worker: proposal.worker,
                scope: proposal.scope,
                event_kind: proposal.event_kind,
                payload: proposal.payload,
                delivery_state: NotificationDeliveryState::Pending,
            };
            let payload_json = Self::encode(&record)?;
            transaction
                .execute(
                    "INSERT INTO control_plane_notification_proposals
                     (delivery_id, organization_id, workspace_id, payload_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        record.delivery_id,
                        record.scope.organization_id.value,
                        record.scope.workspace_id.value,
                        payload_json
                    ],
                )
                .map_err(Self::db)?;
            transaction.commit().map_err(Self::db)?;
            return Ok(record);
        };

        let stored = Self::decode_proposal(payload_json)?;
        if stored.scope != proposal.scope {
            // Checked before content divergence: attribution is never
            // rewritten, so a foreign-scope re-proposal fails closed even if
            // every other field matches.
            return Err(RemoteWorkerNotificationError::ForeignScope {
                delivery_id: proposal.delivery_id,
            });
        }
        if stored.worker == proposal.worker
            && stored.event_kind == proposal.event_kind
            && stored.payload == proposal.payload
        {
            return Ok(stored);
        }
        // Divergent re-proposal: no path below writes, so dropping the
        // transaction leaves the stored row untouched.
        Err(RemoteWorkerNotificationError::DuplicateConflict {
            delivery_id: proposal.delivery_id,
        })
    }

    fn observe_worker_claim(
        &self,
        scope: &NotificationScope,
        claim: WorkerDeliveryClaim,
    ) -> Result<usize, RemoteWorkerNotificationError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        // Scope-contained like every worker-sourced path: a claim may only
        // attach to a delivery the requesting scope itself owns.
        let known: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM control_plane_notification_proposals
                 WHERE delivery_id = ?1 AND organization_id = ?2 AND workspace_id = ?3)",
                params![
                    claim.delivery_id,
                    scope.organization_id.value,
                    scope.workspace_id.value
                ],
                |row| row.get(0),
            )
            .map_err(Self::db)?;
        if !known {
            return Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: claim.delivery_id,
            });
        }
        let recorded: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM control_plane_worker_delivery_claims
                 WHERE delivery_id = ?1",
                params![claim.delivery_id],
                |row| row.get(0),
            )
            .map_err(Self::db)?;
        let claim_ordinal = recorded + 1;
        let claim_json = Self::encode(&claim)?;
        transaction
            .execute(
                "INSERT INTO control_plane_worker_delivery_claims
                 (delivery_id, claim_ordinal, claim_json)
                 VALUES (?1, ?2, ?3)",
                params![claim.delivery_id, claim_ordinal, claim_json],
            )
            .map_err(Self::db)?;
        // Overflow is reported before commit, so the failed write rolls back
        // and nothing durable changes.
        let total = usize::try_from(claim_ordinal).map_err(|error| {
            RemoteWorkerNotificationError::Database {
                reason: error.to_string(),
            }
        })?;
        transaction.commit().map_err(Self::db)?;
        Ok(total)
    }

    fn acknowledge(
        &self,
        acknowledgement: NotificationAcknowledgement,
    ) -> Result<NotificationDeliveryState, RemoteWorkerNotificationError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        let stored_payload: Option<String> = transaction
            .query_row(
                "SELECT payload_json FROM control_plane_notification_proposals
                 WHERE delivery_id = ?1",
                params![acknowledgement.delivery_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        let Some(payload_json) = stored_payload else {
            return Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: acknowledgement.delivery_id,
            });
        };
        let mut record = Self::decode_proposal(payload_json)?;
        if record.delivery_state != NotificationDeliveryState::Delivered {
            record.delivery_state = NotificationDeliveryState::Delivered;
            transaction
                .execute(
                    "UPDATE control_plane_notification_proposals SET payload_json = ?1
                     WHERE delivery_id = ?2",
                    params![Self::encode(&record)?, record.delivery_id],
                )
                .map_err(Self::db)?;
            transaction.commit().map_err(Self::db)?;
        }
        Ok(NotificationDeliveryState::Delivered)
    }

    fn records(
        &self,
        scope: &NotificationScope,
    ) -> Result<Vec<NotificationProposalRecord>, RemoteWorkerNotificationError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT payload_json FROM control_plane_notification_proposals
                 WHERE organization_id = ?1 AND workspace_id = ?2
                 ORDER BY delivery_id",
            )
            .map_err(Self::db)?;
        let rows = statement
            .query_map(
                params![scope.organization_id.value, scope.workspace_id.value],
                |row| row.get::<_, String>(0),
            )
            .map_err(Self::db)?;
        rows.map(|row| Self::decode_proposal(row.map_err(Self::db)?))
            .collect()
    }

    fn worker_claims(
        &self,
        scope: &NotificationScope,
        delivery_id: &str,
    ) -> Result<Vec<WorkerDeliveryClaim>, RemoteWorkerNotificationError> {
        let connection = self.lock()?;
        // Scope filter first: an unknown or out-of-scope delivery reads as
        // empty rather than leaking that anything exists at all.
        let attributed: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM control_plane_notification_proposals
                 WHERE delivery_id = ?1 AND organization_id = ?2 AND workspace_id = ?3)",
                params![
                    delivery_id,
                    scope.organization_id.value,
                    scope.workspace_id.value
                ],
                |row| row.get(0),
            )
            .map_err(Self::db)?;
        if !attributed {
            return Ok(Vec::new());
        }
        let mut statement = connection
            .prepare(
                "SELECT claim_json FROM control_plane_worker_delivery_claims
                 WHERE delivery_id = ?1
                 ORDER BY claim_ordinal",
            )
            .map_err(Self::db)?;
        let rows = statement
            .query_map(params![delivery_id], |row| row.get::<_, String>(0))
            .map_err(Self::db)?;
        rows.map(|row| Self::decode_claim(row.map_err(Self::db)?))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_worker_notification::{RemoteWorkerIdentity, MAX_PROPOSAL_PAYLOAD_BYTES};
    use altai_control_protocol::{ExternalAccountId, OrganizationId, PluginId, WorkspaceId};
    use serde_json::json;

    fn scope() -> NotificationScope {
        NotificationScope {
            organization_id: OrganizationId::new("org_main"),
            workspace_id: WorkspaceId::new("ws_main"),
        }
    }

    fn other_scope() -> NotificationScope {
        NotificationScope {
            organization_id: OrganizationId::new("org_other"),
            workspace_id: WorkspaceId::new("ws_other"),
        }
    }

    fn worker() -> RemoteWorkerIdentity {
        RemoteWorkerIdentity {
            plugin_id: PluginId::new("plg_remote"),
            account_id: ExternalAccountId::new("exta_one"),
        }
    }

    fn other_worker() -> RemoteWorkerIdentity {
        RemoteWorkerIdentity {
            plugin_id: PluginId::new("plg_remote_two"),
            account_id: ExternalAccountId::new("exta_two"),
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

    /// The TempDir must outlive the repository: returning it keeps the
    /// database writable for the whole test.
    fn repository() -> (SqliteNotificationProposalRepository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repository =
            SqliteNotificationProposalRepository::open(&dir.path().join("work.db")).unwrap();
        (repository, dir)
    }

    #[test]
    fn stores_proposal_pending_and_replays_identically() {
        let (repository, _dir) = repository();
        let first = repository.propose(proposal("dlv_1")).expect("store");
        assert_eq!(first.delivery_state, NotificationDeliveryState::Pending);
        let second = repository.propose(proposal("dlv_1")).expect("replay");
        assert_eq!(first, second);
        assert_eq!(
            repository.records(&scope()).expect("records").len(),
            1,
            "an identical replay must not mint a second row"
        );
    }

    #[test]
    fn foreign_scope_reproposal_fails_closed_keeping_attribution() {
        let (repository, _dir) = repository();
        repository.propose(proposal("dlv_foreign")).expect("store");
        let mut foreign = proposal("dlv_foreign");
        foreign.scope = other_scope();
        assert_eq!(
            repository.propose(foreign),
            Err(RemoteWorkerNotificationError::ForeignScope {
                delivery_id: "dlv_foreign".to_string(),
            })
        );
        let stored = &repository.records(&scope()).expect("records")[0];
        assert_eq!(stored.scope, scope(), "attribution is never rewritten");
        assert_eq!(stored.worker, worker());
    }

    #[test]
    fn divergent_reproposal_conflicts_and_keeps_stored_record() {
        let (repository, _dir) = repository();
        repository.propose(proposal("dlv_1")).expect("store");
        let mut divergent = proposal("dlv_1");
        divergent.payload = json!({ "note": "changed" });
        assert_eq!(
            repository.propose(divergent),
            Err(RemoteWorkerNotificationError::DuplicateConflict {
                delivery_id: "dlv_1".to_string(),
            })
        );
        let mut divergent_kind = proposal("dlv_1");
        divergent_kind.event_kind = "alert".to_string();
        assert_eq!(
            repository.propose(divergent_kind),
            Err(RemoteWorkerNotificationError::DuplicateConflict {
                delivery_id: "dlv_1".to_string(),
            }),
            "event_kind divergence conflicts too"
        );
        let mut divergent_worker = proposal("dlv_1");
        divergent_worker.worker = other_worker();
        assert_eq!(
            repository.propose(divergent_worker),
            Err(RemoteWorkerNotificationError::DuplicateConflict {
                delivery_id: "dlv_1".to_string(),
            }),
            "worker divergence conflicts too"
        );
        let stored = &repository.records(&scope()).expect("records")[0];
        assert_eq!(stored.payload, json!({ "note": "alive" }));
        assert_eq!(stored.delivery_state, NotificationDeliveryState::Pending);
    }

    #[test]
    fn invalid_and_unbounded_proposals_fail_closed_without_storing() {
        let (repository, _dir) = repository();
        let mut blank_id = proposal("");
        blank_id.event_kind = "heartbeat".to_string();
        assert!(matches!(
            repository.propose(blank_id),
            Err(RemoteWorkerNotificationError::InvalidProposal { .. })
        ));
        let mut blank_kind = proposal("dlv_kind");
        blank_kind.event_kind = "   ".to_string();
        assert!(matches!(
            repository.propose(blank_kind),
            Err(RemoteWorkerNotificationError::InvalidProposal { .. })
        ));
        let mut incomplete = proposal("dlv_incomplete");
        // Typed-ID constructors always prepend their prefix, so incompleteness
        // is expressed on the raw value the validator inspects.
        incomplete.worker.plugin_id.value = String::new();
        assert!(matches!(
            repository.propose(incomplete),
            Err(RemoteWorkerNotificationError::InvalidProposal { .. })
        ));
        let mut bloated = proposal("dlv_big");
        bloated.payload = json!({ "blob": "x".repeat(MAX_PROPOSAL_PAYLOAD_BYTES + 1) });
        assert_eq!(
            repository.propose(bloated),
            Err(RemoteWorkerNotificationError::UnboundedPayload {
                delivery_id: "dlv_big".to_string(),
            })
        );
        assert_eq!(
            repository.records(&scope()).expect("records").len(),
            0,
            "a rejected proposal must never reach storage"
        );
    }

    #[test]
    fn worker_self_report_never_moves_delivery_state() {
        let (repository, _dir) = repository();
        repository.propose(proposal("dlv_1")).expect("store");
        let first = repository
            .observe_worker_claim(
                &scope(),
                WorkerDeliveryClaim {
                    delivery_id: "dlv_1".to_string(),
                    worker: worker(),
                },
            )
            .expect("claim visible");
        let second = repository
            .observe_worker_claim(
                &scope(),
                WorkerDeliveryClaim {
                    delivery_id: "dlv_1".to_string(),
                    worker: other_worker(),
                },
            )
            .expect("second claim visible");
        assert_eq!(first, 1);
        assert_eq!(second, 2, "counts grow with each self-report");
        assert_eq!(
            repository.records(&scope()).expect("records")[0].delivery_state,
            NotificationDeliveryState::Pending,
        );
        assert_eq!(
            repository
                .worker_claims(&scope(), "dlv_1")
                .expect("claims")
                .len(),
            2
        );
        // A claim addressed at a real delivery from a foreign scope fails
        // closed: workers are untrusted and scope-contained.
        assert_eq!(
            repository.observe_worker_claim(
                &other_scope(),
                WorkerDeliveryClaim {
                    delivery_id: "dlv_1".to_string(),
                    worker: other_worker(),
                },
            ),
            Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: "dlv_1".to_string(),
            })
        );
        assert_eq!(
            repository.observe_worker_claim(
                &scope(),
                WorkerDeliveryClaim {
                    delivery_id: "dlv_missing".to_string(),
                    worker: worker(),
                },
            ),
            Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: "dlv_missing".to_string(),
            })
        );
        // The rejected foreign-scope claim left the owner's provenance
        // untouched.
        assert_eq!(
            repository
                .worker_claims(&scope(), "dlv_1")
                .expect("claims")
                .len(),
            2,
            "a foreign-scope claim must not attach to the delivery"
        );
    }

    #[test]
    fn canonical_acknowledgement_delivers_exactly_once() {
        let (repository, _dir) = repository();
        repository.propose(proposal("dlv_1")).expect("store");
        let ack = NotificationAcknowledgement {
            delivery_id: "dlv_1".to_string(),
        };
        assert_eq!(
            repository.acknowledge(ack.clone()).expect("deliver"),
            NotificationDeliveryState::Delivered,
        );
        // Replaying the same acknowledgement is a no-op, not a second
        // transition or an error.
        assert_eq!(
            repository.acknowledge(ack).expect("replay"),
            NotificationDeliveryState::Delivered,
        );
        assert_eq!(
            repository.records(&scope()).expect("records")[0].delivery_state,
            NotificationDeliveryState::Delivered,
        );
        assert_eq!(
            repository.acknowledge(NotificationAcknowledgement {
                delivery_id: "dlv_missing".to_string(),
            }),
            Err(RemoteWorkerNotificationError::UnknownDelivery {
                delivery_id: "dlv_missing".to_string(),
            })
        );
    }

    #[test]
    fn records_are_scope_filtered_and_delivery_id_ordered() {
        let (repository, _dir) = repository();
        // Arrival order deliberately differs from delivery_id order.
        repository.propose(proposal("dlv_b")).expect("store b");
        repository.propose(proposal("dlv_a")).expect("store a");
        let mut foreign = proposal("dlv_z");
        foreign.scope = other_scope();
        repository.propose(foreign).expect("store z");

        let main_records = repository.records(&scope()).expect("records");
        assert_eq!(
            main_records
                .iter()
                .map(|record| record.delivery_id.as_str())
                .collect::<Vec<_>>(),
            vec!["dlv_a", "dlv_b"],
            "records read in delivery_id order regardless of insertion order"
        );
        let other_records = repository.records(&other_scope()).expect("records");
        assert_eq!(
            other_records
                .iter()
                .map(|record| record.delivery_id.as_str())
                .collect::<Vec<_>>(),
            vec!["dlv_z"],
            "one scope never observes another scope's deliveries"
        );
    }

    #[test]
    fn worker_claims_read_in_arrival_order_and_fail_scoped_closed() {
        let (repository, dir) = repository();
        repository.propose(proposal("dlv_1")).expect("store");
        let first_claim = WorkerDeliveryClaim {
            delivery_id: "dlv_1".to_string(),
            worker: worker(),
        };
        let second_claim = WorkerDeliveryClaim {
            delivery_id: "dlv_1".to_string(),
            worker: other_worker(),
        };
        repository
            .observe_worker_claim(&scope(), first_claim.clone())
            .expect("claim one");
        repository
            .observe_worker_claim(&scope(), second_claim.clone())
            .expect("claim two");

        let reopened = SqliteNotificationProposalRepository::open(&dir.path().join("work.db"))
            .expect("reopen");
        assert_eq!(
            reopened.worker_claims(&scope(), "dlv_1").expect("claims"),
            vec![first_claim, second_claim],
            "claims survive a reopen in arrival order"
        );
        assert_eq!(
            reopened
                .worker_claims(&other_scope(), "dlv_1")
                .expect("scoped"),
            Vec::new(),
            "an in-scope delivery is invisible from a foreign scope"
        );
        assert_eq!(
            reopened
                .worker_claims(&scope(), "dlv_missing")
                .expect("unknown"),
            Vec::new(),
            "an unknown delivery reads as empty, not an error"
        );
    }

    #[test]
    fn records_and_delivery_state_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        {
            let repository = SqliteNotificationProposalRepository::open(&database).unwrap();
            repository.propose(proposal("dlv_acked")).expect("store");
            repository.propose(proposal("dlv_pending")).expect("store");
            repository
                .observe_worker_claim(
                    &scope(),
                    WorkerDeliveryClaim {
                        delivery_id: "dlv_acked".to_string(),
                        worker: worker(),
                    },
                )
                .expect("claim visible");
            repository
                .acknowledge(NotificationAcknowledgement {
                    delivery_id: "dlv_acked".to_string(),
                })
                .expect("deliver");
        }
        // The repository is dropped above; only the TempDir (and the file it
        // holds) carries the state forward.

        let reopened = SqliteNotificationProposalRepository::open(&database).expect("reopen");
        let records = reopened.records(&scope()).expect("records");
        assert_eq!(
            records
                .iter()
                .map(|record| (record.delivery_id.as_str(), record.delivery_state))
                .collect::<Vec<_>>(),
            vec![
                ("dlv_acked", NotificationDeliveryState::Delivered),
                ("dlv_pending", NotificationDeliveryState::Pending),
            ],
            "proposals, their delivery state, and the ack all survive a reopen"
        );
        assert_eq!(
            reopened
                .worker_claims(&scope(), "dlv_acked")
                .expect("claims")
                .len(),
            1,
            "recorded claims survive a reopen"
        );
        // The durable store keeps the ledger's exactly-once acknowledgement
        // even across process boundaries.
        assert_eq!(
            reopened
                .acknowledge(NotificationAcknowledgement {
                    delivery_id: "dlv_acked".to_string(),
                })
                .expect("replay"),
            NotificationDeliveryState::Delivered,
        );
        let _keep_alive = &dir;
    }
}
