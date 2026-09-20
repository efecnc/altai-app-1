//! The canonical scheduling driver (package 101, slice C.a).
//!
//! [`SchedulerDriver`] gives the `SingleWriterScheduler` domain its first
//! production loop: each tick re-reads the feature-flag ledger and
//! materializes due routines into wakes only while the ledger names this
//! process as the schedule's owner. The per-tick re-read is deliberate —
//! a flag flip changes authority at a tick boundary without a restart,
//! and an absent flag is "not yet decided", i.e. exactly today's legacy
//! behavior. Authority is a predicate on recorded facts plus process
//! identity; the workspace single-writer lock (CP-08-106) arbitrates the
//! process pairing on top of it.

use crate::{
    FeatureFlagError, FeatureFlagRepository, RoutineMaterializer, CONTROL_PLANE_ENABLED_FLAG,
};
use std::sync::Arc;

/// Ledger keys owned by the scheduling cutover. `control_plane_enabled`
/// lives in `feature_flag_repository`; the owner and rollback keys are
/// new in slice C.a.
pub const SCHEDULE_OWNER_FLAG: &str = "schedule_owner";
pub const LEGACY_CRON_COMPATIBILITY_FLAG: &str = "legacy_cron_compatibility";

/// The schedule-owner value for the desktop embedded host.
pub const SCHEDULE_OWNER_DESKTOP: &str = "desktop_host";
/// The schedule-owner value for the standalone control-plane daemon.
pub const SCHEDULE_OWNER_DAEMON: &str = "daemon";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverError {
    FeatureFlag(FeatureFlagError),
    Materialization(String),
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FeatureFlag(error) => write!(f, "schedule authority lookup failed: {error}"),
            Self::Materialization(error) => write!(f, "wake materialization failed: {error}"),
        }
    }
}

impl std::error::Error for DriverError {}

/// The scheduling authority recorded in the ledger. Absent rows are
/// "not yet decided": canonical scheduling is off, legacy behavior stands.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScheduleAuthority {
    pub enabled: bool,
    pub owner: Option<String>,
    pub legacy_compatibility: bool,
}

impl ScheduleAuthority {
    /// True once the ledger has decided the cutover question at all:
    /// canonical scheduling is enabled, the legacy rollback switch is not
    /// pulled, and an owner is named. False means undecided or rolled
    /// back — the pre-cutover legacy behavior stands, and a legacy
    /// driver runs unconditionally exactly as it did before this ledger
    /// existed.
    pub fn canonical_decided(&self) -> bool {
        self.enabled && !self.legacy_compatibility && self.owner.is_some()
    }

    /// True when the ledger gives `process` the scheduling authority: the
    /// cutover is decided ([`Self::canonical_decided`]) and this process
    /// is the named owner. Never true by accident — every absent flag
    /// resolves to legacy.
    pub fn authorizes(&self, process: &str) -> bool {
        self.canonical_decided() && self.owner.as_deref() == Some(process)
    }
}

/// Re-read the ledger and resolve the current authority.
pub fn resolve_schedule_authority(
    ledger: &dyn FeatureFlagRepository,
) -> Result<ScheduleAuthority, DriverError> {
    let enabled = ledger.get(CONTROL_PLANE_ENABLED_FLAG).map_err(DriverError::FeatureFlag)?;
    let owner = ledger.get(SCHEDULE_OWNER_FLAG).map_err(DriverError::FeatureFlag)?;
    let legacy = ledger
        .get(LEGACY_CRON_COMPATIBILITY_FLAG)
        .map_err(DriverError::FeatureFlag)?;
    Ok(ScheduleAuthority {
        enabled: enabled.as_deref() == Some("true"),
        owner,
        legacy_compatibility: legacy.as_deref() == Some("true"),
    })
}

/// What one driver tick decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickOutcome {
    /// The ledger does not authorize this process; nothing ran.
    Idle,
    /// Canonical authority: due routines materialized into wakes.
    Materialized { enqueued: usize },
}

pub struct SchedulerDriver {
    materializer: Arc<RoutineMaterializer>,
    ledger: Arc<dyn FeatureFlagRepository>,
    process: &'static str,
}

impl SchedulerDriver {
    pub fn new(
        materializer: Arc<RoutineMaterializer>,
        ledger: Arc<dyn FeatureFlagRepository>,
        process: &'static str,
    ) -> Self {
        Self {
            materializer,
            ledger,
            process,
        }
    }

    /// One scheduling tick: resolve authority, then materialize due
    /// routines when this process owns the schedule. Fail-closed — a
    /// lookup or materialization error is returned typed, never skipped
    /// silently.
    pub fn tick(&self, now_unix_seconds: u64) -> Result<TickOutcome, DriverError> {
        let authority = resolve_schedule_authority(self.ledger.as_ref())?;
        if !authority.authorizes(self.process) {
            return Ok(TickOutcome::Idle);
        }
        let enqueued = self
            .materializer
            .materialize_due(now_unix_seconds)
            .map_err(|error| DriverError::Materialization(error.to_string()))?;
        Ok(TickOutcome::Materialized { enqueued })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RoutineRepository, SqliteFeatureFlagRepository, SqliteRoutineRepository,
        SqliteWakeRepository,
    };
    use altai_control_protocol::{
        OrganizationId, Revision, Routine, RoutineId, RoutineRevision, RoutineRevisionId,
        RoutineStatus, RoutineTrigger, WorkItemId,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestHarness {
        dir: tempfile::TempDir,
        driver: SchedulerDriver,
        ledger: Arc<SqliteFeatureFlagRepository>,
    }

    fn harness(process: &'static str) -> TestHarness {
        let dir = tempfile::tempdir().unwrap();
        let work_db = dir.path().join("work.db");
        let routines = Arc::new(SqliteRoutineRepository::open(&work_db).unwrap());
        let wakes = Arc::new(SqliteWakeRepository::open(&work_db).unwrap());
        let ledger = Arc::new(crate::SqliteFeatureFlagRepository::open(&work_db).unwrap());
        let materializer = Arc::new(RoutineMaterializer::new(routines, wakes));
        let driver = SchedulerDriver::new(materializer, ledger.clone(), process);
        TestHarness { dir, driver, ledger }
    }

    fn seed_active_cron_routine(dir: &std::path::Path, cron_expr: &str) {
        let work_db = dir.join("work.db");
        let routines = SqliteRoutineRepository::open(&work_db).unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let routine_id = RoutineId::new("driver-routine");
        routines
            .create(Routine {
                id: routine_id.clone(),
                organization_id: OrganizationId::new("org"),
                current_revision_id: None,
                status: RoutineStatus::Active,
                revision: Revision::INITIAL,
                created_at_unix_seconds: now,
                updated_at_unix_seconds: now,
            })
            .unwrap();
        routines
            .append_revision(
                &routine_id,
                RoutineRevision {
                    id: RoutineRevisionId::new("driver-routine_r0"),
                    routine_id: routine_id.clone(),
                    revision: Revision::INITIAL,
                    trigger: RoutineTrigger::Recurring {
                        cron_expression: cron_expr.to_string(),
                    },
                    target_work_item_id: WorkItemId::new("driver-target"),
                    created_at_unix_seconds: now,
                },
            )
            .unwrap();
    }

    #[test]
    fn absent_flags_mean_idle_everywhere() {
        let h = harness(SCHEDULE_OWNER_DAEMON);
        assert_eq!(h.driver.tick(0).unwrap(), TickOutcome::Idle);
        let desktop = harness(SCHEDULE_OWNER_DESKTOP);
        assert_eq!(desktop.driver.tick(0).unwrap(), TickOutcome::Idle);
    }

    #[test]
    fn authority_requires_enable_owner_and_no_legacy_rollback() {
        let h = harness(SCHEDULE_OWNER_DAEMON);
        let daemon_driver = &h.driver;
        let ledger = &h.ledger;
        // Enabled but no owner named: still undecided for any process.
        ledger.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        assert_eq!(daemon_driver.tick(0).unwrap(), TickOutcome::Idle);
        // Owner names the other process.
        ledger.set(SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DESKTOP).unwrap();
        assert_eq!(daemon_driver.tick(0).unwrap(), TickOutcome::Idle);
        // Legacy rollback switch overrides a matching owner.
        ledger.set(SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DAEMON).unwrap();
        ledger
            .set(LEGACY_CRON_COMPATIBILITY_FLAG, "true")
            .unwrap();
        assert_eq!(daemon_driver.tick(0).unwrap(), TickOutcome::Idle);
        // Full canonical state: the daemon drives.
        ledger
            .set(LEGACY_CRON_COMPATIBILITY_FLAG, "false")
            .unwrap();
        let outcome = daemon_driver.tick(u64::MAX).unwrap();
        assert!(matches!(outcome, TickOutcome::Materialized { .. }));
    }

    #[test]
    fn the_named_owner_materializes_due_routines() {
        let h = harness(SCHEDULE_OWNER_DESKTOP);
        h.ledger.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        h.ledger
            .set(SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DESKTOP)
            .unwrap();
        // No routines: a tick is authorized but enqueues nothing.
        assert_eq!(
            h.driver.tick(0).unwrap(),
            TickOutcome::Materialized { enqueued: 0 }
        );
        seed_active_cron_routine(h.dir.path(), "* * * * *");
        // A due routine (cron "*" fires every minute) materializes a wake:
        // tick from two minutes out so the next fire is already due.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let outcome = h.driver.tick(now + 120).unwrap();
        assert!(
            matches!(&outcome, TickOutcome::Materialized { enqueued } if *enqueued >= 1),
            "expected a materialized wake, got {outcome:?}"
        );
    }
}
