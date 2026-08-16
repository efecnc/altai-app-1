//! CP-08 durable Routine + immutable RoutineRevision storage. A routine's
//! intent is append-only: each accepted revision becomes the current revision
//! and the aggregate revision advances. Routines do not register a scheduler;
//! package 041 bridges a revision into Wake/RoutineRun when its trigger fires.

use altai_control_protocol::{
    Routine, RoutineId, RoutineRevision, RoutineRevisionId, RoutineStatus,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::{path::Path, sync::Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutineError {
    NotFound { routine_id: String },
    RevisionNotFound { routine_revision_id: String },
    Conflict { routine_id: String },
    Internal { reason: String },
}

impl std::fmt::Display for RoutineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "routine error: {self:?}")
    }
}
impl std::error::Error for RoutineError {}

pub trait RoutineRepository: Send + Sync {
    /// Create a routine aggregate. Idempotent when the same routine is
    /// re-created; fails closed with [`RoutineError::Conflict`] when a different
    /// routine already owns the id.
    fn create(&self, routine: Routine) -> Result<Routine, RoutineError>;
    /// Append an immutable intent revision and advance the routine's current
    /// revision. Idempotent when the same revision id is appended again.
    fn append_revision(
        &self,
        routine_id: &RoutineId,
        revision: RoutineRevision,
    ) -> Result<Routine, RoutineError>;
    fn get(&self, id: &RoutineId) -> Result<Option<Routine>, RoutineError>;
    fn get_revision(
        &self,
        revision_id: &RoutineRevisionId,
    ) -> Result<Option<RoutineRevision>, RoutineError>;
    /// All routines in the `Active` lifecycle state, for the cron bridge to scan.
    fn list_active(&self) -> Result<Vec<Routine>, RoutineError>;
    /// Every routine regardless of lifecycle state, for read-side
    /// projections: a paused or retired routine is still scheduled work
    /// someone chose to silence, not a fact that stops existing.
    fn list_all(&self) -> Result<Vec<Routine>, RoutineError>;
    /// The most recent cron fire the bridge materialized for this routine, if any.
    fn last_fired(&self, routine_id: &RoutineId) -> Result<Option<u64>, RoutineError>;
    /// Record that the bridge materialized a fire at `fired_at_unix_seconds`,
    /// advancing the routine's anchor. Upserts: a later fire overwrites an earlier.
    fn record_fire(
        &self,
        routine_id: &RoutineId,
        fired_at_unix_seconds: u64,
    ) -> Result<(), RoutineError>;
}

pub struct SqliteRoutineRepository {
    connection: Mutex<Connection>,
}

impl SqliteRoutineRepository {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection.execute_batch("PRAGMA busy_timeout = 5000; CREATE TABLE IF NOT EXISTS control_plane_routines (routine_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL); CREATE TABLE IF NOT EXISTS control_plane_routine_revisions (routine_revision_id TEXT PRIMARY KEY, routine_id TEXT NOT NULL, revision INTEGER NOT NULL, payload_json TEXT NOT NULL); CREATE TABLE IF NOT EXISTS control_plane_routine_fires (routine_id TEXT PRIMARY KEY, last_fired_at INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS control_plane_routine_revisions_routine_id ON control_plane_routine_revisions (routine_id);").map_err(|e| e.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, RoutineError> {
        self.connection.lock().map_err(|_| RoutineError::Internal {
            reason: "sqlite routine lock poisoned".into(),
        })
    }
    fn db(e: rusqlite::Error) -> RoutineError {
        RoutineError::Internal { reason: e.to_string() }
    }
}

impl RoutineRepository for SqliteRoutineRepository {
    fn create(&self, routine: Routine) -> Result<Routine, RoutineError> {
        let payload = serde_json::to_string(&routine).map_err(|e| RoutineError::Internal {
            reason: e.to_string(),
        })?;
        let mut connection = self.lock()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        let inserted = tx
            .execute(
                "INSERT INTO control_plane_routines (routine_id, payload_json) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                params![routine.id.value, payload],
            )
            .map_err(Self::db)?;
        if inserted == 1 {
            tx.commit().map_err(Self::db)?;
            return Ok(routine);
        }
        // A row already owns this id: idempotent only if it is byte-identical.
        let existing = Self::read_routine(&tx, &routine.id)?.ok_or_else(|| RoutineError::Internal {
            reason: "routine disappeared after insert conflict".into(),
        })?;
        if existing == routine {
            tx.commit().map_err(Self::db)?;
            Ok(existing)
        } else {
            Err(RoutineError::Conflict {
                routine_id: routine.id.value,
            })
        }
    }

    fn append_revision(
        &self,
        routine_id: &RoutineId,
        revision: RoutineRevision,
    ) -> Result<Routine, RoutineError> {
        if revision.routine_id != *routine_id {
            return Err(RoutineError::Conflict {
                routine_id: routine_id.value.clone(),
            });
        }
        let payload = serde_json::to_string(&revision).map_err(|e| RoutineError::Internal {
            reason: e.to_string(),
        })?;
        let mut connection = self.lock()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Self::db)?;
        let mut routine = Self::read_routine(&tx, routine_id)?.ok_or_else(|| RoutineError::NotFound {
            routine_id: routine_id.value.clone(),
        })?;
        // Idempotent: appending the same revision id again is a no-op.
        let already_present: bool = tx
            .query_row(
                "SELECT 1 FROM control_plane_routine_revisions WHERE routine_revision_id=?1",
                [&revision.id.value],
                |_| Ok(()),
            )
            .optional()
            .map_err(Self::db)?
            .is_some();
        if !already_present {
            // Enforce a monotonic intent sequence: a genuinely new revision must
            // advance past the current one. This keeps the append-only log ordered
            // once the command port exposes append_revision over the wire.
            if let Some(current_revision_id) = routine.current_revision_id {
                let current = Self::read_revision(&tx, &current_revision_id)?.ok_or_else(|| {
                    RoutineError::Internal {
                        reason: "routine current revision missing".into(),
                    }
                })?;
                if revision.revision.value() <= current.revision.value() {
                    return Err(RoutineError::Conflict {
                        routine_id: routine_id.value.clone(),
                    });
                }
            }
            tx.execute(
                "INSERT INTO control_plane_routine_revisions (routine_revision_id, routine_id, revision, payload_json) VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
                params![
                    revision.id.value,
                    revision.routine_id.value,
                    revision.revision.value() as i64,
                    payload
                ],
            )
            .map_err(Self::db)?;
            routine.current_revision_id = Some(revision.id);
            routine.revision = routine.revision.next();
            routine.updated_at_unix_seconds = revision.created_at_unix_seconds;
            let routine_payload =
                serde_json::to_string(&routine).map_err(|e| RoutineError::Internal {
                    reason: e.to_string(),
                })?;
            tx.execute(
                "UPDATE control_plane_routines SET payload_json=?2 WHERE routine_id=?1",
                params![routine.id.value, routine_payload],
            )
            .map_err(Self::db)?;
        }
        tx.commit().map_err(Self::db)?;
        Ok(routine)
    }

    fn get(&self, id: &RoutineId) -> Result<Option<Routine>, RoutineError> {
        let payload: Option<String> = self
            .lock()?
            .query_row(
                "SELECT payload_json FROM control_plane_routines WHERE routine_id=?1",
                [&id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p).map_err(|e| RoutineError::Internal {
                    reason: e.to_string(),
                })
            })
            .transpose()
    }

    fn get_revision(
        &self,
        revision_id: &RoutineRevisionId,
    ) -> Result<Option<RoutineRevision>, RoutineError> {
        let payload: Option<String> = self
            .lock()?
            .query_row(
                "SELECT payload_json FROM control_plane_routine_revisions WHERE routine_revision_id=?1",
                [&revision_id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p).map_err(|e| RoutineError::Internal {
                    reason: e.to_string(),
                })
            })
            .transpose()
    }

    fn list_active(&self) -> Result<Vec<Routine>, RoutineError> {
        Ok(self
            .list_all()?
            .into_iter()
            .filter(|routine| routine.status == RoutineStatus::Active)
            .collect())
    }

    fn list_all(&self) -> Result<Vec<Routine>, RoutineError> {
        let connection = self.lock()?;
        let mut stmt = connection
            .prepare("SELECT payload_json FROM control_plane_routines")
            .map_err(Self::db)?;
        let payloads = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(Self::db)?;
        let mut routines = Vec::new();
        for payload in payloads {
            let routine: Routine =
                serde_json::from_str(&payload.map_err(Self::db)?).map_err(|e| RoutineError::Internal {
                    reason: e.to_string(),
                })?;
            routines.push(routine);
        }
        Ok(routines)
    }

    fn last_fired(&self, routine_id: &RoutineId) -> Result<Option<u64>, RoutineError> {
        let seconds: Option<i64> = self
            .lock()?
            .query_row(
                "SELECT last_fired_at FROM control_plane_routine_fires WHERE routine_id=?1",
                [&routine_id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        Ok(seconds.map(|s| s.max(0) as u64))
    }

    fn record_fire(
        &self,
        routine_id: &RoutineId,
        fired_at_unix_seconds: u64,
    ) -> Result<(), RoutineError> {
        self.lock()?
            .execute(
                "INSERT INTO control_plane_routine_fires (routine_id, last_fired_at) VALUES (?1, ?2) ON CONFLICT(routine_id) DO UPDATE SET last_fired_at=excluded.last_fired_at",
                params![routine_id.value, fired_at_unix_seconds as i64],
            )
            .map_err(Self::db)?;
        Ok(())
    }
}

impl SqliteRoutineRepository {
    fn read_routine(
        tx: &rusqlite::Transaction<'_>,
        id: &RoutineId,
    ) -> Result<Option<Routine>, RoutineError> {
        let payload: Option<String> = tx
            .query_row(
                "SELECT payload_json FROM control_plane_routines WHERE routine_id=?1",
                [&id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p).map_err(|e| RoutineError::Internal {
                    reason: e.to_string(),
                })
            })
            .transpose()
    }

    fn read_revision(
        tx: &rusqlite::Transaction<'_>,
        id: &RoutineRevisionId,
    ) -> Result<Option<RoutineRevision>, RoutineError> {
        let payload: Option<String> = tx
            .query_row(
                "SELECT payload_json FROM control_plane_routine_revisions WHERE routine_revision_id=?1",
                [&id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p).map_err(|e| RoutineError::Internal {
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
        OrganizationId, Revision, RoutineTrigger, WorkItemId,
    };

    fn routine(id: &str) -> Routine {
        Routine {
            id: RoutineId::new(id),
            organization_id: OrganizationId::new("org"),
            current_revision_id: None,
            status: altai_control_protocol::RoutineStatus::Active,
            revision: Revision::INITIAL,
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        }
    }

    fn revision(routine_id: &RoutineId, seq: u64, trigger: RoutineTrigger) -> RoutineRevision {
        RoutineRevision {
            id: RoutineRevisionId::new(format!("rev-{seq}")),
            routine_id: routine_id.clone(),
            revision: Revision::new(seq),
            trigger,
            target_work_item_id: WorkItemId::new("work"),
            created_at_unix_seconds: seq * 10,
        }
    }

    #[test]
    fn routine_revisions_are_indexed_by_routine_id() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        SqliteRoutineRepository::open(&database).unwrap();

        // The revision-history scan path filters revisions by routine_id; an
        // index keeps that scan off a full table walk.
        let connection = Connection::open(&database).unwrap();
        let indexed: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='control_plane_routine_revisions_routine_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexed, 1);
    }

    #[test]
    fn routine_is_durable_and_current_revision_advances() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteRoutineRepository::open(&database).unwrap();
        let routine_id = RoutineId::new("rt");
        repo.create(routine("rt")).unwrap();

        let after_one = repo
            .append_revision(
                &routine_id,
                revision(
                    &routine_id,
                    1,
                    RoutineTrigger::Recurring {
                        cron_expression: "0 9 * * *".into(),
                    },
                ),
            )
            .unwrap();
        assert_eq!(after_one.current_revision_id, Some(RoutineRevisionId::new("rev-1")));
        assert_eq!(after_one.revision, Revision::new(1));

        let after_two = repo
            .append_revision(
                &routine_id,
                revision(
                    &routine_id,
                    2,
                    RoutineTrigger::Event {
                        source: "pull_request".into(),
                    },
                ),
            )
            .unwrap();
        assert_eq!(after_two.current_revision_id, Some(RoutineRevisionId::new("rev-2")));
        assert_eq!(after_two.revision, Revision::new(2));
        assert_eq!(after_two.updated_at_unix_seconds, 20);

        // Durable across reopen: current revision and aggregate version survive.
        let reopened = SqliteRoutineRepository::open(&database).unwrap();
        let stored = reopened.get(&routine_id).unwrap().unwrap();
        assert_eq!(stored.current_revision_id, Some(RoutineRevisionId::new("rev-2")));
        assert_eq!(stored.revision, Revision::new(2));

        // Immutable intent revisions are retained and readable.
        assert_eq!(
            reopened
                .get_revision(&RoutineRevisionId::new("rev-1"))
                .unwrap()
                .unwrap()
                .revision,
            Revision::new(1)
        );
        assert!(matches!(
            reopened
                .get_revision(&RoutineRevisionId::new("rev-2"))
                .unwrap()
                .unwrap()
                .trigger,
            RoutineTrigger::Event { .. }
        ));
    }

    #[test]
    fn append_revision_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteRoutineRepository::open(&database).unwrap();
        let routine_id = RoutineId::new("rt");
        repo.create(routine("rt")).unwrap();
        let rev = revision(
            &routine_id,
            1,
            RoutineTrigger::Recurring {
                cron_expression: "0 9 * * *".into(),
            },
        );

        let first = repo.append_revision(&routine_id, rev.clone()).unwrap();
        let second = repo.append_revision(&routine_id, rev).unwrap();
        // Replaying the same revision id does not advance the aggregate again.
        assert_eq!(first.revision, second.revision);
        assert_eq!(second.revision, Revision::new(1));
    }

    #[test]
    fn append_revision_rejects_unknown_routine() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteRoutineRepository::open(&dir.path().join("work.db")).unwrap();
        let routine_id = RoutineId::new("ghost");
        let err = repo
            .append_revision(
                &routine_id,
                revision(
                    &routine_id,
                    1,
                    RoutineTrigger::Recurring {
                        cron_expression: "0 9 * * *".into(),
                    },
                ),
            )
            .unwrap_err();
        assert!(matches!(err, RoutineError::NotFound { .. }));
    }

    #[test]
    fn list_all_keeps_paused_and_retired_routines_readable() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteRoutineRepository::open(&database).unwrap();
        let mut paused = routine("rt-paused");
        paused.status = altai_control_protocol::RoutineStatus::Paused;
        let mut retired = routine("rt-retired");
        retired.status = altai_control_protocol::RoutineStatus::Retired;
        repo.create(routine("rt-active")).unwrap();
        repo.create(paused).unwrap();
        repo.create(retired).unwrap();

        let all = repo.list_all().unwrap();
        assert_eq!(all.len(), 3, "every lifecycle state stays readable");
        // list_active remains the cron bridge's scan: Active only.
        assert_eq!(repo.list_active().unwrap().len(), 1);
    }

    #[test]
    fn create_is_idempotent_and_rejects_a_divergent_routine() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteRoutineRepository::open(&database).unwrap();
        repo.create(routine("rt")).unwrap();
        // Same routine re-created is idempotent.
        repo.create(routine("rt")).unwrap();
        // A different routine under the same id fails closed.
        let mut divergent = routine("rt");
        divergent.organization_id = OrganizationId::new("other");
        assert!(matches!(
            repo.create(divergent),
            Err(RoutineError::Conflict { .. })
        ));
    }

    #[test]
    fn append_revision_rejects_a_non_monotonic_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteRoutineRepository::open(&database).unwrap();
        let routine_id = RoutineId::new("rt");
        repo.create(routine("rt")).unwrap();
        let trigger = RoutineTrigger::Recurring {
            cron_expression: "0 9 * * *".into(),
        };
        // Advance to intent revision 2.
        repo.append_revision(&routine_id, revision(&routine_id, 2, trigger.clone()))
            .unwrap();
        // A different revision id carrying a lower intent version is rejected:
        // the append-only log must stay monotonic.
        let err = repo
            .append_revision(&routine_id, revision(&routine_id, 1, trigger))
            .unwrap_err();
        assert!(matches!(err, RoutineError::Conflict { .. }));
        // The routine is unchanged: still pointing at revision 2.
        let stored = repo.get(&routine_id).unwrap().unwrap();
        assert_eq!(
            stored.current_revision_id,
            Some(RoutineRevisionId::new("rev-2"))
        );
        assert_eq!(stored.revision, Revision::new(1));
    }
}
