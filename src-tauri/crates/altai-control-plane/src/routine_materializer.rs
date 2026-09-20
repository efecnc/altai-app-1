//! Routine cron materialization (package 041). Scans active routines and, for
//! each whose current revision carries a [`RoutineTrigger::Recurring`] that has
//! fired since its last materialized fire, enqueues one coalesced
//! [`WakeSource::Routine`] wake for the revision's target work item and advances
//! the routine's anchor. The existing single-writer scheduler then claims and
//! dispatches that wake like any other.
//!
//! This is the in-process seam: a caller (the daemon driver, a later package)
//! invokes [`RoutineMaterializer::materialize_due`] at `now`. Event-triggered
//! routines are not cron-driven and are skipped here — they are materialized by
//! an event listener, not a tick.

use crate::{
    cron_due, RoutineError, RoutineRepository, WakeError, WakeRepository,
};
use altai_control_protocol::{RoutineTrigger, WakeSource};
use std::sync::Arc;

#[derive(Debug)]
pub enum RoutineMaterializationError {
    Routine(RoutineError),
    Wake(WakeError),
}

impl From<RoutineError> for RoutineMaterializationError {
    fn from(value: RoutineError) -> Self {
        Self::Routine(value)
    }
}
impl From<WakeError> for RoutineMaterializationError {
    fn from(value: WakeError) -> Self {
        Self::Wake(value)
    }
}
impl std::fmt::Display for RoutineMaterializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Routine(e) => write!(f, "routine materialization failed: {e}"),
            Self::Wake(e) => write!(f, "wake enqueue failed: {e:?}"),
        }
    }
}
impl std::error::Error for RoutineMaterializationError {}

pub struct RoutineMaterializer {
    routines: Arc<dyn RoutineRepository>,
    wakes: Arc<dyn WakeRepository>,
}

impl RoutineMaterializer {
    pub fn new(routines: Arc<dyn RoutineRepository>, wakes: Arc<dyn WakeRepository>) -> Self {
        Self { routines, wakes }
    }

    /// Materialize every active recurring routine that is due at `now`. Returns
    /// the number of wakes enqueued. Idempotent within a period: a second call at
    /// the same `now` enqueues nothing because the anchor advanced to the fire.
    pub fn materialize_due(&self, now_unix_seconds: u64) -> Result<usize, RoutineMaterializationError> {
        self.materialize_due_excluding(now_unix_seconds, &[])
    }

    /// The same scan, skipping every routine whose id is in
    /// `excluded_routine_ids`. The scheduling cutover's legacy tick path
    /// passes the mapped routine ids here: a mapped routine is a mirror of a
    /// live legacy automation the legacy authorities still drive, and driving
    /// the mirror too would fire the automation twice. The canonical paths
    /// pass nothing — once the ledger names an owner, that owner drives
    /// every active routine.
    pub fn materialize_due_excluding(
        &self,
        now_unix_seconds: u64,
        excluded_routine_ids: &[String],
    ) -> Result<usize, RoutineMaterializationError> {
        let mut enqueued = 0;
        for routine in self.routines.list_active()? {
            if excluded_routine_ids.contains(&routine.id.value) {
                continue;
            }
            let Some(revision_id) = routine.current_revision_id else {
                continue;
            };
            let Some(revision) = self.routines.get_revision(&revision_id)? else {
                continue;
            };
            let RoutineTrigger::Recurring { cron_expression } = &revision.trigger else {
                // Event-triggered routines are not cron-driven; skip.
                continue;
            };
            let anchor = self
                .routines
                .last_fired(&routine.id)?
                .unwrap_or(revision.created_at_unix_seconds);
            let Some(fire) = cron_due::next_fire_after(cron_expression, anchor) else {
                // Unparseable expression: skip rather than halt every other routine.
                continue;
            };
            if fire <= now_unix_seconds {
                self.wakes.enqueue(
                    revision.target_work_item_id.clone(),
                    WakeSource::Routine,
                    fire.to_string(),
                )?;
                self.routines.record_fire(&routine.id, fire)?;
                enqueued += 1;
            }
        }
        Ok(enqueued)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryWakeRepository, SqliteRoutineRepository};
    use altai_control_protocol::{
        OrganizationId, Revision, Routine, RoutineId, RoutineRevisionId, RoutineStatus,
        RoutineTrigger, WorkItemId,
    };

    fn materializer(
        dir: &tempfile::TempDir,
    ) -> (
        RoutineMaterializer,
        Arc<SqliteRoutineRepository>,
        Arc<InMemoryWakeRepository>,
    ) {
        let routines = Arc::new(SqliteRoutineRepository::open(&dir.path().join("work.db")).unwrap());
        let wakes = Arc::new(InMemoryWakeRepository::default());
        (
            RoutineMaterializer::new(routines.clone() as Arc<dyn RoutineRepository>, wakes.clone()),
            routines,
            wakes,
        )
    }

    fn recurring_routine(
        routines: &SqliteRoutineRepository,
        id: &str,
        expression: &str,
        created_at: u64,
    ) -> RoutineId {
        let routine_id = RoutineId::new(id);
        routines
            .create(Routine {
                id: routine_id.clone(),
                organization_id: OrganizationId::new("org"),
                current_revision_id: None,
                status: RoutineStatus::Active,
                revision: Revision::INITIAL,
                created_at_unix_seconds: created_at,
                updated_at_unix_seconds: created_at,
            })
            .unwrap();
        routines
            .append_revision(
                &routine_id,
                altai_control_protocol::RoutineRevision {
                    id: RoutineRevisionId::new(format!("{id}-rev-1")),
                    routine_id: routine_id.clone(),
                    revision: Revision::new(1),
                    trigger: RoutineTrigger::Recurring {
                        cron_expression: expression.into(),
                    },
                    target_work_item_id: WorkItemId::new("work-1"),
                    created_at_unix_seconds: created_at,
                },
            )
            .unwrap();
        routine_id
    }

    #[test]
    fn due_routine_enqueues_a_routine_wake_and_advances_its_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, wakes) = materializer(&dir);
        recurring_routine(&routines, "rt", "* * * * *", 0);

        // First fire is at 60s; at now=60 the routine is due.
        assert_eq!(materializer.materialize_due(60).unwrap(), 1);
        assert_eq!(routines.last_fired(&RoutineId::new("rt")).unwrap(), Some(60));

        // The wake exists for the target work item and carries the Routine source.
        let wake = wakes
            .claim_wake(&WorkItemId::new("work-1"), "now".into())
            .unwrap();
        assert!(wake.sources.iter().any(|s| matches!(s, WakeSource::Routine)));

        // Re-evaluating at the same now enqueues nothing: the anchor advanced to 60.
        assert_eq!(materializer.materialize_due(60).unwrap(), 0);
    }

    #[test]
    fn materialization_advances_one_period_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, _wakes) = materializer(&dir);
        recurring_routine(&routines, "rt", "* * * * *", 0);

        assert_eq!(materializer.materialize_due(60).unwrap(), 1);
        assert_eq!(materializer.materialize_due(120).unwrap(), 1);
        assert_eq!(materializer.materialize_due(180).unwrap(), 1);
        // Missed periods collapse: a single call far ahead enqueues once, not many.
        assert_eq!(materializer.materialize_due(600).unwrap(), 1);
    }

    #[test]
    fn not_yet_due_routine_enqueues_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, _wakes) = materializer(&dir);
        // Daily at 09:00; created at the epoch, the first fire (32400) is in the future.
        recurring_routine(&routines, "rt", "0 9 * * *", 0);

        assert_eq!(materializer.materialize_due(60).unwrap(), 0);
        assert_eq!(routines.last_fired(&RoutineId::new("rt")).unwrap(), None);
        // Once the fire time is reached, it materializes.
        assert_eq!(materializer.materialize_due(32400).unwrap(), 1);
    }

    #[test]
    fn inactive_routine_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, _wakes) = materializer(&dir);
        let routine_id = RoutineId::new("rt");
        routines
            .create(Routine {
                id: routine_id.clone(),
                organization_id: OrganizationId::new("org"),
                current_revision_id: None,
                status: RoutineStatus::Paused,
                revision: Revision::INITIAL,
                created_at_unix_seconds: 0,
                updated_at_unix_seconds: 0,
            })
            .unwrap();
        routines
            .append_revision(
                &routine_id,
                altai_control_protocol::RoutineRevision {
                    id: RoutineRevisionId::new("rev-1"),
                    routine_id: routine_id.clone(),
                    revision: Revision::new(1),
                    trigger: RoutineTrigger::Recurring {
                        cron_expression: "* * * * *".into(),
                    },
                    target_work_item_id: WorkItemId::new("work-1"),
                    created_at_unix_seconds: 0,
                },
            )
            .unwrap();

        assert_eq!(materializer.materialize_due(60).unwrap(), 0);
    }

    #[test]
    fn event_triggered_routine_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, _wakes) = materializer(&dir);
        let routine_id = RoutineId::new("rt");
        routines
            .create(Routine {
                id: routine_id.clone(),
                organization_id: OrganizationId::new("org"),
                current_revision_id: None,
                status: RoutineStatus::Active,
                revision: Revision::INITIAL,
                created_at_unix_seconds: 0,
                updated_at_unix_seconds: 0,
            })
            .unwrap();
        routines
            .append_revision(
                &routine_id,
                altai_control_protocol::RoutineRevision {
                    id: RoutineRevisionId::new("rev-1"),
                    routine_id: routine_id.clone(),
                    revision: Revision::new(1),
                    trigger: RoutineTrigger::Event {
                        source: "pull_request".into(),
                    },
                    target_work_item_id: WorkItemId::new("work-1"),
                    created_at_unix_seconds: 0,
                },
            )
            .unwrap();

        assert_eq!(materializer.materialize_due(u64::MAX).unwrap(), 0);
    }

    #[test]
    fn malformed_cron_expression_is_skipped_without_aborting_others() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, _wakes) = materializer(&dir);
        // A routine with a bad expression...
        recurring_routine(&routines, "bad", "not a cron", 0);
        // ...and a healthy one alongside it.
        recurring_routine(&routines, "good", "* * * * *", 0);

        assert_eq!(materializer.materialize_due(60).unwrap(), 1);
    }

    /// N1 regression: the legacy tick's exclusion seam. An excluded routine
    /// materializes nothing while its un-excluded peer does, and its anchor
    /// stays put — the undecided/rollback bridge path passes the mapped
    /// mirror ids through exactly this parameter.
    #[test]
    fn excluded_routine_ids_materialize_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (materializer, routines, wakes) = materializer(&dir);
        recurring_routine(&routines, "mirror", "* * * * *", 0);
        recurring_routine(&routines, "free", "* * * * *", 0);

        let enqueued = materializer
            .materialize_due_excluding(60, &[RoutineId::new("mirror").value])
            .unwrap();
        assert_eq!(enqueued, 1, "only the un-excluded routine materializes");
        assert!(wakes.claim_wake(&WorkItemId::new("work-1"), "now".into()).is_ok());
        assert_eq!(
            routines.last_fired(&RoutineId::new("mirror")).unwrap(),
            None,
            "the excluded routine's anchor must not advance"
        );
        assert_eq!(routines.last_fired(&RoutineId::new("free")).unwrap(), Some(60));
    }
}
