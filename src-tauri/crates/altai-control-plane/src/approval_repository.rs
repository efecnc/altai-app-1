//! CP-08 durable Approval + immutable ApprovalDecision storage. An approval is
//! a governance decision request; its resolution is an append-only
//! [`ApprovalDecision`] audit row keyed one-per-approval (first-writer-wins). The
//! aggregate advances its `outcome` when a decision lands, mirroring how a
//! routine points at its current revision. Nothing here enqueues a wake or
//! mutates a work item's execution phase.

use altai_control_protocol::{Approval, ApprovalDecision, ApprovalId, OrganizationId};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::{path::Path, sync::Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalError {
    NotFound { approval_id: String },
    Conflict { approval_id: String },
    Internal { reason: String },
}

impl std::fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "approval error: {self:?}")
    }
}
impl std::error::Error for ApprovalError {}

pub trait ApprovalRepository: Send + Sync {
    /// Create an approval (governance decision request). Idempotent when the same
    /// approval is re-created; fails closed with [`ApprovalError::Conflict`] when
    /// a different approval already owns the id.
    fn create(&self, approval: Approval) -> Result<Approval, ApprovalError>;
    /// Record the immutable decision that resolves an approval. First-writer-wins:
    /// re-recording an identical decision is idempotent; a divergent decision is
    /// rejected so the audit never contradicts itself. Advances the aggregate's
    /// `outcome`, `resolved_at`, and `revision` only on a genuinely new decision.
    fn record_decision(&self, decision: ApprovalDecision) -> Result<Approval, ApprovalError>;
    fn get(&self, id: &ApprovalId) -> Result<Option<Approval>, ApprovalError>;
    fn get_decision(&self, approval_id: &ApprovalId) -> Result<Option<ApprovalDecision>, ApprovalError>;
    /// All approvals with no recorded decision yet, for the scheduler to scan.
    fn list_pending(&self) -> Result<Vec<Approval>, ApprovalError>;
    /// Every approval in an organization, resolved or not (org equality filter).
    fn list_in_org(&self, organization_id: &OrganizationId) -> Result<Vec<Approval>, ApprovalError>;
}

pub struct SqliteApprovalRepository {
    connection: Mutex<Connection>,
}

impl SqliteApprovalRepository {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection.execute_batch("PRAGMA busy_timeout = 5000; CREATE TABLE IF NOT EXISTS control_plane_approvals (approval_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL); CREATE TABLE IF NOT EXISTS control_plane_approval_decisions (approval_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL);").map_err(|e| e.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, ApprovalError> {
        self.connection.lock().map_err(|_| ApprovalError::Internal {
            reason: "sqlite approval lock poisoned".into(),
        })
    }
    fn db(e: rusqlite::Error) -> ApprovalError {
        ApprovalError::Internal { reason: e.to_string() }
    }
}

impl ApprovalRepository for SqliteApprovalRepository {
    fn create(&self, approval: Approval) -> Result<Approval, ApprovalError> {
        let payload = serde_json::to_string(&approval).map_err(|e| ApprovalError::Internal {
            reason: e.to_string(),
        })?;
        let mut connection = self.lock()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        let inserted = tx
            .execute(
                "INSERT INTO control_plane_approvals (approval_id, payload_json) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                params![approval.id.value, payload],
            )
            .map_err(Self::db)?;
        if inserted == 1 {
            tx.commit().map_err(Self::db)?;
            return Ok(approval);
        }
        // A row already owns this id: idempotent only if byte-identical.
        let existing = Self::read_approval(&tx, &approval.id)?.ok_or_else(|| ApprovalError::Internal {
            reason: "approval disappeared after insert conflict".into(),
        })?;
        if existing == approval {
            tx.commit().map_err(Self::db)?;
            Ok(existing)
        } else {
            Err(ApprovalError::Conflict {
                approval_id: approval.id.value,
            })
        }
    }

    fn record_decision(&self, decision: ApprovalDecision) -> Result<Approval, ApprovalError> {
        let decision_payload = serde_json::to_string(&decision).map_err(|e| ApprovalError::Internal {
            reason: e.to_string(),
        })?;
        let mut connection = self.lock()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        let mut approval = Self::read_approval(&tx, &decision.approval_id)?.ok_or_else(|| {
            ApprovalError::NotFound {
                approval_id: decision.approval_id.value.clone(),
            }
        })?;
        // First-writer-wins: an approval is decided exactly once. The insert no-ops
        // when a decision row already owns the approval; the stored row (ours or
        // the earlier writer's) then arbitrates identical replay vs divergence.
        let inserted = tx
            .execute(
                "INSERT INTO control_plane_approval_decisions (approval_id, payload_json) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                params![decision.approval_id.value, decision_payload],
            )
            .map_err(Self::db)?;
        let stored_payload: String = tx
            .query_row(
                "SELECT payload_json FROM control_plane_approval_decisions WHERE approval_id=?1",
                [&decision.approval_id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?
            .ok_or_else(|| ApprovalError::Internal {
                reason: "decision row missing after insert".into(),
            })?;
        let stored: ApprovalDecision =
            serde_json::from_str(&stored_payload).map_err(|e| ApprovalError::Internal {
                reason: e.to_string(),
            })?;
        // Idempotent when the same decision is re-recorded; reject a divergent one.
        if stored != decision {
            return Err(ApprovalError::Conflict {
                approval_id: decision.approval_id.value,
            });
        }
        // Only a genuinely new decision advances the aggregate; a replay returns
        // the durable approval as the earlier writer left it.
        if inserted == 1 {
            approval.outcome = Some(decision.outcome);
            approval.resolved_at_unix_seconds = Some(decision.decided_at_unix_seconds);
            approval.revision = approval.revision.next();
            let approval_payload =
                serde_json::to_string(&approval).map_err(|e| ApprovalError::Internal {
                    reason: e.to_string(),
                })?;
            tx.execute(
                "UPDATE control_plane_approvals SET payload_json=?2 WHERE approval_id=?1",
                params![approval.id.value, approval_payload],
            )
            .map_err(Self::db)?;
        }
        tx.commit().map_err(Self::db)?;
        Ok(approval)
    }

    fn get(&self, id: &ApprovalId) -> Result<Option<Approval>, ApprovalError> {
        let connection = self.lock()?;
        Self::read_approval(&connection, id)
    }

    fn get_decision(&self, approval_id: &ApprovalId) -> Result<Option<ApprovalDecision>, ApprovalError> {
        let payload: Option<String> = self
            .lock()?
            .query_row(
                "SELECT payload_json FROM control_plane_approval_decisions WHERE approval_id=?1",
                [&approval_id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p).map_err(|e| ApprovalError::Internal {
                    reason: e.to_string(),
                })
            })
            .transpose()
    }

    fn list_pending(&self) -> Result<Vec<Approval>, ApprovalError> {
        let connection = self.lock()?;
        let mut stmt = connection
            .prepare("SELECT payload_json FROM control_plane_approvals")
            .map_err(Self::db)?;
        let payloads = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(Self::db)?;
        let mut approvals = Vec::new();
        for payload in payloads {
            let approval: Approval =
                serde_json::from_str(&payload.map_err(Self::db)?).map_err(|e| ApprovalError::Internal {
                    reason: e.to_string(),
                })?;
            if approval.outcome.is_none() {
                approvals.push(approval);
            }
        }
        Ok(approvals)
    }

    fn list_in_org(&self, organization_id: &OrganizationId) -> Result<Vec<Approval>, ApprovalError> {
        let connection = self.lock()?;
        let mut stmt = connection
            .prepare("SELECT payload_json FROM control_plane_approvals")
            .map_err(Self::db)?;
        let payloads = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(Self::db)?;
        let mut approvals = Vec::new();
        for payload in payloads {
            let approval: Approval =
                serde_json::from_str(&payload.map_err(Self::db)?).map_err(|e| ApprovalError::Internal {
                    reason: e.to_string(),
                })?;
            if approval.organization_id == *organization_id {
                approvals.push(approval);
            }
        }
        Ok(approvals)
    }
}

impl SqliteApprovalRepository {
    fn read_approval(
        connection: &rusqlite::Connection,
        id: &ApprovalId,
    ) -> Result<Option<Approval>, ApprovalError> {
        let payload: Option<String> = connection
            .query_row(
                "SELECT payload_json FROM control_plane_approvals WHERE approval_id=?1",
                [&id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p).map_err(|e| ApprovalError::Internal {
                    reason: e.to_string(),
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use altai_control_protocol::{
        ApprovalOutcome, ApprovalScope, AttemptId, OrganizationId, Revision,
    };

    fn approval(id: &str) -> Approval {
        Approval {
            id: ApprovalId::new(id),
            organization_id: OrganizationId::new("org"),
            scope: ApprovalScope::Plan {
                attempt_id: AttemptId::new("att"),
            },
            payload_revision: Revision::new(1),
            outcome: None,
            revision: Revision::INITIAL,
            created_at_unix_seconds: 10,
            resolved_at_unix_seconds: None,
        }
    }

    fn decision(id: &str, outcome: ApprovalOutcome) -> ApprovalDecision {
        ApprovalDecision {
            approval_id: ApprovalId::new(id),
            outcome,
            decided_by: "principal".into(),
            decided_at_unix_seconds: 20,
            reason: Some("ok".into()),
        }
    }

    #[test]
    fn approval_is_durable_and_outcome_advances_on_decision() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteApprovalRepository::open(&database).unwrap();
        let approval_id = ApprovalId::new("apv");
        repo.create(approval("apv")).unwrap();

        let resolved = repo.record_decision(decision("apv", ApprovalOutcome::Approved)).unwrap();
        assert_eq!(resolved.outcome, Some(ApprovalOutcome::Approved));
        assert_eq!(resolved.resolved_at_unix_seconds, Some(20));
        assert_eq!(resolved.revision, Revision::new(1));

        // Durable across reopen: the aggregate and the immutable decision survive.
        let reopened = SqliteApprovalRepository::open(&database).unwrap();
        let stored = reopened.get(&approval_id).unwrap().unwrap();
        assert_eq!(stored.outcome, Some(ApprovalOutcome::Approved));
        assert_eq!(stored.revision, Revision::new(1));
        let stored_decision = reopened.get_decision(&approval_id).unwrap().unwrap();
        assert_eq!(stored_decision.outcome, ApprovalOutcome::Approved);
    }

    #[test]
    fn create_is_idempotent_and_rejects_a_divergent_approval() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteApprovalRepository::open(&dir.path().join("work.db")).unwrap();
        repo.create(approval("apv")).unwrap();
        // Same approval re-created is idempotent.
        repo.create(approval("apv")).unwrap();
        // A different approval under the same id fails closed.
        let mut divergent = approval("apv");
        divergent.organization_id = OrganizationId::new("other");
        assert!(matches!(
            repo.create(divergent),
            Err(ApprovalError::Conflict { .. })
        ));
    }

    #[test]
    fn record_decision_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteApprovalRepository::open(&dir.path().join("work.db")).unwrap();
        repo.create(approval("apv")).unwrap();
        let first = repo.record_decision(decision("apv", ApprovalOutcome::Approved)).unwrap();
        let second = repo.record_decision(decision("apv", ApprovalOutcome::Approved)).unwrap();
        // Replaying the same decision does not advance the aggregate again.
        assert_eq!(first.revision, second.revision);
        assert_eq!(second.revision, Revision::new(1));
    }

    #[test]
    fn record_decision_rejects_a_divergent_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteApprovalRepository::open(&dir.path().join("work.db")).unwrap();
        let approval_id = ApprovalId::new("apv");
        repo.create(approval("apv")).unwrap();
        repo.record_decision(decision("apv", ApprovalOutcome::Approved)).unwrap();
        // A contradicting decision is rejected: the audit never contradicts itself.
        let err = repo
            .record_decision(decision("apv", ApprovalOutcome::Denied))
            .unwrap_err();
        assert!(matches!(err, ApprovalError::Conflict { .. }));
        // The approval is unchanged: still approved.
        let stored = repo.get(&approval_id).unwrap().unwrap();
        assert_eq!(stored.outcome, Some(ApprovalOutcome::Approved));
    }

    #[test]
    fn record_decision_first_writer_wins_across_connections() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let first = SqliteApprovalRepository::open(&database).unwrap();
        let second = SqliteApprovalRepository::open(&database).unwrap();
        let approval_id = ApprovalId::new("apv");
        first.create(approval("apv")).unwrap();

        let resolved = first
            .record_decision(decision("apv", ApprovalOutcome::Approved))
            .unwrap();
        // A second writer replaying the identical decision is idempotent and
        // does not advance the aggregate again.
        let replay = second
            .record_decision(decision("apv", ApprovalOutcome::Approved))
            .unwrap();
        assert_eq!(replay.revision, resolved.revision);
        // A divergent second writer cannot overwrite the first decision.
        assert!(matches!(
            second.record_decision(decision("apv", ApprovalOutcome::Denied)),
            Err(ApprovalError::Conflict { .. })
        ));
        let stored = second.get(&approval_id).unwrap().unwrap();
        assert_eq!(stored.outcome, Some(ApprovalOutcome::Approved));
        assert_eq!(stored.revision, resolved.revision);
    }

    #[test]
    fn record_decision_rejects_unknown_approval() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteApprovalRepository::open(&dir.path().join("work.db")).unwrap();
        let err = repo
            .record_decision(decision("ghost", ApprovalOutcome::Approved))
            .unwrap_err();
        assert!(matches!(err, ApprovalError::NotFound { .. }));
    }

    #[test]
    fn list_pending_excludes_resolved_approvals() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteApprovalRepository::open(&dir.path().join("work.db")).unwrap();
        repo.create(approval("pending")).unwrap();
        repo.create(approval("resolved")).unwrap();
        repo.record_decision(decision("resolved", ApprovalOutcome::Denied)).unwrap();

        let pending: Vec<String> = repo
            .list_pending()
            .unwrap()
            .into_iter()
            .map(|a| a.id.value)
            .collect();
        assert_eq!(pending, vec!["apv_pending".to_string()]);
    }

    #[test]
    fn list_in_org_returns_only_that_orgs_approvals() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteApprovalRepository::open(&dir.path().join("work.db")).unwrap();
        repo.create(approval("mine-1")).unwrap();
        repo.create(approval("mine-2")).unwrap();
        let mut foreign = approval("foreign");
        foreign.organization_id = OrganizationId::new("other");
        repo.create(foreign).unwrap();

        let mut ids: Vec<String> = repo
            .list_in_org(&OrganizationId::new("org"))
            .unwrap()
            .into_iter()
            .map(|a| a.id.value)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["apv_mine-1".to_string(), "apv_mine-2".to_string()]);
        // The foreign org sees only its own.
        let other = repo.list_in_org(&OrganizationId::new("other")).unwrap();
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].id.value, "apv_foreign");
    }
}
