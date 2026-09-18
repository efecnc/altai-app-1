//! Periodic driver for the routine cron materializer (package 041). On each tick
//! the bridge asks the [`RoutineMaterializer`] to enqueue a wake for every active
//! recurring routine whose cron has fired since its last materialized fire. The
//! materializer is the tested unit — idempotent, one wake per due routine — so
//! this driver owns only the loop and the wall-clock `now`, mirroring how the
//! single-writer scheduler owns `claim_if_eligible` but not its driver loop.
//!
//! A failed tick is logged to stderr and the next tick proceeds: one bad tick
//! must not halt scheduling for every other routine. The bridge is the managed
//! cron backend; native deployments do not run this daemon, and the per-attempt
//! native-vs-managed `ScheduleBackend` binding happens downstream when a
//! routine wake becomes an attempt.

use crate::{
    resolve_schedule_authority, FeatureFlagRepository, RoutineMaterializer,
    RoutineMaterializationError, SCHEDULE_OWNER_DAEMON,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Default bridge cadence. A recurring routine's finest resolution is one
/// minute, so ticking faster would only re-evaluate already-coalesced fires.
pub const DEFAULT_CRON_TICK: Duration = Duration::from_secs(60);

pub struct RoutineCronBridge {
    materializer: Arc<RoutineMaterializer>,
    period: Duration,
}

impl RoutineCronBridge {
    pub fn new(materializer: Arc<RoutineMaterializer>, period: Duration) -> Self {
        Self {
            materializer,
            period,
        }
    }

    /// Materialize due routines at `now`. Exposed so a caller drives a single tick
    /// deterministically; the loop in [`RoutineCronBridge::run`] supplies the
    /// wall-clock `now`. Delegates to the tested [`RoutineMaterializer`].
    pub fn tick(
        &self,
        now_unix_seconds: u64,
    ) -> Result<usize, RoutineMaterializationError> {
        self.materializer.materialize_due(now_unix_seconds)
    }

    /// Run the bridge until the runtime drops the task. Each tick materializes at
    /// the wall-clock `now`; a failed tick is logged and the loop continues.
    ///
    /// Unconditional form: kept for deployments that do not participate in
    /// the scheduling cutover (tests, flag-free standalone runs). The daemon
    /// entry point uses [`Self::run_gated`].
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.period);
        loop {
            ticker.tick().await;
            let now = wall_clock_now();
            if let Err(error) = self.tick(now) {
                eprintln!("routine cron bridge tick at {now} failed: {error}");
            }
        }
    }

    /// Authority-gated loop for the scheduling cutover (CP-08-108): each
    /// tick re-reads the feature-flag ledger and materializes only while it
    /// names this daemon as the schedule's owner with the rollback switch
    /// un-pulled. While owning, the daemon additionally holds the
    /// workspace's single-writer lock — the same kernel-mediated lock the
    /// desktop holds for its app run — so a live desktop's workspace is
    /// never double-driven; when ownership moves away, the lock is
    /// released so the desktop can resume. A `WorkspaceHeld` refusal is
    /// logged and the tick stays idle.
    pub async fn run_gated(
        self,
        ledger: std::sync::Arc<dyn FeatureFlagRepository>,
        work_db: std::path::PathBuf,
    ) {
        let mut ticker = tokio::time::interval(self.period);
        // Held across iterations only while this process is the named
        // owner; dropping the handle releases the kernel lock.
        let mut owned_workspace: Option<std::sync::Arc<altai_core::WorkStore>> = None;
        loop {
            ticker.tick().await;
            let now = wall_clock_now();
            let authority = match resolve_schedule_authority(ledger.as_ref()) {
                Ok(authority) => authority,
                Err(error) => {
                    eprintln!("schedule authority lookup failed: {error}");
                    continue;
                }
            };
            if authority.authorizes(SCHEDULE_OWNER_DAEMON) {
                if owned_workspace.is_none() {
                    match altai_core::WorkStore::open(&work_db) {
                        Ok(store) => owned_workspace = Some(std::sync::Arc::new(store)),
                        Err(error) => {
                            // The desktop (or another binary) owns this
                            // workspace's writer lock: fail closed, stay idle.
                            eprintln!(
                                "schedule authority names this daemon but the workspace is held: {error}"
                            );
                            continue;
                        }
                    }
                }
                if let Err(error) = self.tick(now) {
                    eprintln!("routine cron bridge tick at {now} failed: {error}");
                }
            } else {
                // Authority moved away (flag off, owner renamed, rollback
                // pulled): release the lock so the desktop can resume.
                owned_workspace = None;
            }
        }
    }
}

fn wall_clock_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryWakeRepository, RoutineRepository, SqliteRoutineRepository, WakeRepository};
    use altai_control_protocol::{
        OrganizationId, Revision, Routine, RoutineId, RoutineRevisionId, RoutineStatus,
        RoutineTrigger, WakeSource, WorkItemId,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Build a bridge over fresh sqlite routine state and an in-memory wake queue.
    fn bridge(
        dir: &tempfile::TempDir,
    ) -> (
        RoutineCronBridge,
        Arc<SqliteRoutineRepository>,
        Arc<InMemoryWakeRepository>,
    ) {
        let routines = Arc::new(SqliteRoutineRepository::open(&dir.path().join("work.db")).unwrap());
        let wakes = Arc::new(InMemoryWakeRepository::default());
        let materializer = Arc::new(RoutineMaterializer::new(routines.clone(), wakes.clone()));
        (
            RoutineCronBridge::new(materializer, DEFAULT_CRON_TICK),
            routines,
            wakes,
        )
    }

    fn recurring_routine(routines: &SqliteRoutineRepository, id: &str, expression: &str, created_at: u64) {
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
    }

    fn now_minus(seconds: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs().saturating_sub(seconds))
            .unwrap_or(0)
    }

    /// A due routine materializes on a single tick and the anchor advances, so a
    /// second tick at the same `now` enqueues nothing.
    #[test]
    fn tick_materializes_a_due_routine_once() {
        let dir = tempfile::tempdir().unwrap();
        let (bridge, routines, _wakes) = bridge(&dir);
        recurring_routine(&routines, "rt", "* * * * *", 0);

        assert_eq!(bridge.tick(60).unwrap(), 1);
        assert_eq!(bridge.tick(60).unwrap(), 0);
        assert_eq!(routines.last_fired(&RoutineId::new("rt")).unwrap(), Some(60));
    }

    /// A routine whose first fire is in the future enqueues nothing; the bridge
    /// delegates due-evaluation to the materializer.
    #[test]
    fn tick_skips_a_routine_that_is_not_yet_due() {
        let dir = tempfile::tempdir().unwrap();
        let (bridge, routines, _wakes) = bridge(&dir);
        // Daily at 09:00 from the epoch: first fire (32400) is in the future at now=60.
        recurring_routine(&routines, "rt", "0 9 * * *", 0);

        assert_eq!(bridge.tick(60).unwrap(), 0);
        assert_eq!(routines.last_fired(&RoutineId::new("rt")).unwrap(), None);
    }

    /// The loop actually fires: spawning the bridge enqueues a wake for a routine
    /// that is already due relative to the wall clock, with no test-supplied `now`.
    #[tokio::test]
    async fn run_enqueues_a_due_routine_on_the_first_tick() {
        let dir = tempfile::tempdir().unwrap();
        let (routines, wakes) = (
            Arc::new(SqliteRoutineRepository::open(&dir.path().join("work.db")).unwrap()),
            Arc::new(InMemoryWakeRepository::default()),
        );
        // Created two minutes ago; an every-minute cron is therefore already due now.
        recurring_routine(&routines, "rt", "* * * * *", now_minus(120));
        let materializer = Arc::new(RoutineMaterializer::new(routines.clone(), wakes.clone()));
        let cron_bridge = RoutineCronBridge::new(materializer, Duration::from_millis(50));
        tokio::spawn(cron_bridge.run());

        let work = WorkItemId::new("work-1");
        for _ in 0..40 {
            if wakes.claim_wake(&work, "now".to_string()).is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("routine cron bridge never materialized the due wake");
    }

    // Confirm the enqueued wake carries the Routine source once claimed.
    #[tokio::test]
    async fn run_enqueues_a_routine_source_wake() {
        let dir = tempfile::tempdir().unwrap();
        let routines = Arc::new(SqliteRoutineRepository::open(&dir.path().join("work.db")).unwrap());
        let wakes = Arc::new(InMemoryWakeRepository::default());
        recurring_routine(&routines, "rt", "* * * * *", now_minus(120));
        let materializer = Arc::new(RoutineMaterializer::new(routines.clone(), wakes.clone()));
        let cron_bridge = RoutineCronBridge::new(materializer, Duration::from_millis(50));
        tokio::spawn(cron_bridge.run());

        let work = WorkItemId::new("work-1");
        let wake = loop {
            match wakes.claim_wake(&work, "now".to_string()) {
                Ok(wake) => break wake,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        assert!(wake.sources.iter().any(|s| matches!(s, WakeSource::Routine)));
    }
}
