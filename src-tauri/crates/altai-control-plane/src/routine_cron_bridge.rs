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
    RoutineMaterializationError, SqliteCronAutomationTransfer, SCHEDULE_OWNER_DAEMON,
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

    /// The legacy tick's mapping-aware form: due routines materialize except
    /// those whose id is in `excluded_routine_ids`. See
    /// [`Self::gated_tick`]'s undecided branch for why the exclusions exist
    /// only there.
    pub fn tick_excluding(
        &self,
        now_unix_seconds: u64,
        excluded_routine_ids: &[String],
    ) -> Result<usize, RoutineMaterializationError> {
        self.materializer
            .materialize_due_excluding(now_unix_seconds, excluded_routine_ids)
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

    /// Authority-gated loop for the scheduling cutover (CP-08-108). Each
    /// tick re-reads the feature-flag ledger and takes exactly one of
    /// three paths:
    ///
    /// * Cutover undecided or rolled back (`canonical_decided` false):
    ///   the legacy tick runs unconditionally and the workspace
    ///   single-writer lock is never taken — byte-for-byte the
    ///   pre-cutover daemon, which drove every due routine without
    ///   consulting any flag or holding any lock.
    /// * Decided and owned by this daemon: while owning, the daemon
    ///   holds the workspace's single-writer lock — the same
    ///   kernel-mediated lock the desktop holds for its app run — so a
    ///   live desktop's workspace is never double-driven; a
    ///   `WorkspaceHeld` refusal is logged and the tick stays idle.
    /// * Decided and owned elsewhere (or the ledger is unreadable): the
    ///   tick stays idle and the lock is released so the owner can take
    ///   it.
    ///
    /// The undecided/rollback legacy tick reads the workspace's v6
    /// cron-automation mapping store (same `work.db`) and skips every
    /// mapped routine — see [`Self::gated_tick`]. The store is opened once
    /// here; if it cannot be opened, the legacy tick fails closed (skips)
    /// rather than risk driving a mapped mirror twice.
    pub async fn run_gated(
        self,
        ledger: std::sync::Arc<dyn FeatureFlagRepository>,
        work_db: std::path::PathBuf,
    ) {
        let mappings = match SqliteCronAutomationTransfer::open(&work_db) {
            Ok(transfer) => Some(transfer),
            Err(error) => {
                eprintln!(
                    "routine cron bridge cannot open the cron automation mapping store: {error}"
                );
                None
            }
        };
        let mut ticker = tokio::time::interval(self.period);
        // Held across iterations only while this process is the named
        // owner; dropping the handle releases the kernel lock.
        let mut owned_workspace: Option<std::sync::Arc<altai_core::WorkStore>> = None;
        loop {
            ticker.tick().await;
            let now = wall_clock_now();
            self.gated_tick(
                now,
                ledger.as_ref(),
                &work_db,
                mappings.as_ref(),
                &mut owned_workspace,
            );
        }
    }

    /// One authority-gated tick: resolve the ledger, then either run the
    /// mapping-aware legacy tick (never touching the workspace lock), hold
    /// the lock and drive canonically as the named daemon owner, or stay
    /// idle and release. Exposed for deterministic tests; [`Self::run_gated`]
    /// supplies the wall-clock `now`, the mapping store, and carries
    /// `owned_workspace` across ticks.
    fn gated_tick(
        &self,
        now_unix_seconds: u64,
        ledger: &dyn FeatureFlagRepository,
        work_db: &std::path::Path,
        mappings: Option<&SqliteCronAutomationTransfer>,
        owned_workspace: &mut Option<std::sync::Arc<altai_core::WorkStore>>,
    ) {
        let authority = match resolve_schedule_authority(ledger) {
            Ok(authority) => authority,
            Err(error) => {
                // The ledger is unreadable, so ownership cannot be
                // re-confirmed this tick: release the lock so whichever
                // process the last good tick named — or none — can take it.
                eprintln!("schedule authority lookup failed: {error}");
                *owned_workspace = None;
                return;
            }
        };
        if !authority.canonical_decided() {
            // Undecided or rolled back: legacy behavior stands. The legacy
            // daemon never took the workspace lock, so this path must not
            // either — opening a WorkStore here would lock live legacy
            // workspaces out of their own writer.
            //
            // Never dual-run (CP-08-108): a routine with a v6 mapping row is
            // a snapshot mirror of a live legacy automation that the legacy
            // authorities (the desktop's CronActor over `cron_jobs`) still
            // drive in this state, so this tick materializes only routines
            // with no mapping row. Zero mapping rows leave the filter empty
            // and this tick byte-for-byte the pre-cutover one. Once the
            // ledger decides (the branches below), the canonical owner
            // drives every Active routine and the mirror exclusion is gone.
            let Some(mappings) = mappings else {
                eprintln!(
                    "routine cron bridge skipped its legacy tick: the mapping store is unavailable"
                );
                return;
            };
            let excluded = match mappings.mapped_routine_ids() {
                Ok(ids) => ids,
                Err(error) => {
                    eprintln!(
                        "routine cron bridge skipped its legacy tick: mapping lookup failed: {error}"
                    );
                    return;
                }
            };
            if let Err(error) = self.tick_excluding(now_unix_seconds, &excluded) {
                eprintln!("routine cron bridge tick at {now_unix_seconds} failed: {error}");
            }
            return;
        }
        if authority.owner.as_deref() != Some(SCHEDULE_OWNER_DAEMON) {
            // Authority moved to another owner (the desktop): release the
            // lock so the owner can take it.
            *owned_workspace = None;
            return;
        }
        if owned_workspace.is_none() {
            match altai_core::WorkStore::open(work_db) {
                Ok(store) => *owned_workspace = Some(std::sync::Arc::new(store)),
                Err(error) => {
                    // The desktop (or another binary) owns this
                    // workspace's writer lock: fail closed, stay idle.
                    eprintln!(
                        "schedule authority names this daemon but the workspace is held: {error}"
                    );
                    return;
                }
            }
        }
        if let Err(error) = self.tick(now_unix_seconds) {
            eprintln!("routine cron bridge tick at {now_unix_seconds} failed: {error}");
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
    use crate::{
        FeatureFlagRepository, InMemoryWakeRepository, RoutineRepository, SqliteFeatureFlagRepository,
        SqliteRoutineRepository, WakeRepository, CONTROL_PLANE_ENABLED_FLAG,
        LEGACY_CRON_COMPATIBILITY_FLAG, SCHEDULE_OWNER_DESKTOP, SCHEDULE_OWNER_FLAG,
    };
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
        recurring_routine_to(routines, id, expression, created_at, "work-1");
    }

    fn recurring_routine_to(
        routines: &SqliteRoutineRepository,
        id: &str,
        expression: &str,
        created_at: u64,
        target_work_item: &str,
    ) {
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
                    target_work_item_id: WorkItemId::new(target_work_item),
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

    /// A gated bridge over fresh sqlite routine state, an in-memory wake
    /// queue, a sqlite feature-flag ledger beside the routine state, and the
    /// v6 mapping store the legacy tick consults.
    #[allow(clippy::type_complexity)]
    fn gated_bridge(
        dir: &tempfile::TempDir,
    ) -> (
        RoutineCronBridge,
        Arc<SqliteRoutineRepository>,
        Arc<InMemoryWakeRepository>,
        Arc<SqliteFeatureFlagRepository>,
        std::path::PathBuf,
        SqliteCronAutomationTransfer,
    ) {
        let work_db = dir.path().join("work.db");
        let routines = Arc::new(SqliteRoutineRepository::open(&work_db).unwrap());
        let wakes = Arc::new(InMemoryWakeRepository::default());
        let ledger = Arc::new(SqliteFeatureFlagRepository::open(&work_db).unwrap());
        let mappings = SqliteCronAutomationTransfer::open(&work_db).unwrap();
        let materializer = Arc::new(RoutineMaterializer::new(routines.clone(), wakes.clone()));
        (
            RoutineCronBridge::new(materializer, DEFAULT_CRON_TICK),
            routines,
            wakes,
            ledger,
            work_db,
            mappings,
        )
    }

    /// What `schedule snapshot` leaves behind for one enabled automation:
    /// an Active recurring mirror routine (with its own target work item)
    /// plus the v6 mapping row naming it (written through the mapping
    /// store's own table, which `gated_bridge` already materialized).
    fn mapped_mirror(
        routines: &SqliteRoutineRepository,
        work_db: &std::path::Path,
        id: &str,
        target_work_item: &str,
    ) {
        // Same id derivation as the snapshot's map_automation: the mirror
        // routine is `RoutineId::new("snap_" + automation_id)` and the
        // mapping row stores that id's `.value`.
        let mirror_routine_id = RoutineId::new(format!("snap_{id}"));
        recurring_routine_to(routines, &mirror_routine_id.value, "* * * * *", 0, target_work_item);
        let mapping_db = rusqlite::Connection::open(work_db).unwrap();
        mapping_db
            .execute(
                "INSERT INTO control_plane_cron_automation_mappings (automation_id, disposition, routine_id, work_item_id, content_hash, provenance_json) VALUES (?1, 'mapped', ?2, ?3, 'hash', '{}')",
                rusqlite::params![format!("auto-{id}"), mirror_routine_id.value, target_work_item],
            )
            .unwrap();
    }

    /// A due recurring routine for the deterministic tick at `now` = 60.
    fn seed_due_routine(routines: &SqliteRoutineRepository) {
        recurring_routine(routines, "rt", "* * * * *", 0);
    }

    fn canonical_daemon_flags(ledger: &SqliteFeatureFlagRepository) {
        ledger.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        ledger.set(SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DAEMON).unwrap();
        ledger
            .set(LEGACY_CRON_COMPATIBILITY_FLAG, "false")
            .unwrap();
    }

    /// F1 regression: with the scheduling ledger entirely absent — the
    /// state of every deployment that predates the cutover — the gated
    /// bridge still runs the legacy tick and materializes the due wake,
    /// and it never takes the workspace writer lock the legacy daemon
    /// never held. With the mapping store present but EMPTY (no snapshot
    /// has ever run) the exclusion filter is a no-op: every Active
    /// routine materializes, byte-for-byte the pre-cutover tick.
    #[test]
    fn undecided_ledger_materializes_the_legacy_tick_without_the_workspace_lock() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);
        recurring_routine_to(&routines, "rt-2", "* * * * *", 0, "work-2");

        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);

        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_ok(),
            "an undecided ledger must leave legacy scheduling running"
        );
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-2"), "now".to_string())
                .is_ok(),
            "zero mapping rows must leave the legacy exclusion filter a no-op"
        );
        assert!(
            owned.is_none(),
            "the undecided path must not open the workspace writer lock"
        );
        // The lock is free for the desktop: an open succeeds.
        let _store = altai_core::WorkStore::open(&work_db).unwrap();
    }

    /// F1 regression: the pulled legacy rollback switch also leaves the
    /// legacy tick running, unconditionally and lock-free.
    #[test]
    fn legacy_rollback_materializes_the_legacy_tick_without_the_workspace_lock() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);
        canonical_daemon_flags(&ledger);
        ledger
            .set(LEGACY_CRON_COMPATIBILITY_FLAG, "true")
            .unwrap();

        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);

        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_ok(),
            "a pulled rollback switch must leave legacy scheduling running"
        );
        assert!(owned.is_none());
        let _store = altai_core::WorkStore::open(&work_db).unwrap();
    }

    /// N1 regression: in the undecided state the desktop's CronActor still
    /// fires the legacy automation a mapped mirror was snapshotted from, so
    /// the legacy tick must skip the mirror — driving it too would fire the
    /// automation twice. Unmapped routines materialize as before.
    #[test]
    fn undecided_legacy_tick_skips_mapped_mirrors_and_drives_unmapped_routines() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);
        mapped_mirror(&routines, &work_db, "rt-mirror", "work-mapped");

        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);

        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_ok(),
            "an unmapped routine must still materialize on the undecided legacy tick"
        );
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-mapped"), "now".to_string())
                .is_err(),
            "a mapped mirror must stay dormant while legacy drives the automation"
        );
        assert!(owned.is_none());
    }

    /// N1 regression: after a rollback the legacy authorities fire again,
    /// so the mapped mirror stays dormant there too.
    #[test]
    fn legacy_rollback_tick_also_skips_mapped_mirrors() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);
        mapped_mirror(&routines, &work_db, "rt-mirror", "work-mapped");
        canonical_daemon_flags(&ledger);
        ledger
            .set(LEGACY_CRON_COMPATIBILITY_FLAG, "true")
            .unwrap();

        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);

        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_ok(),
            "a pulled rollback switch must leave legacy scheduling running"
        );
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-mapped"), "now".to_string())
                .is_err(),
            "a mapped mirror must stay dormant after the rollback switch is pulled"
        );
    }

    /// N1 regression: once the ledger decides and names the daemon, the
    /// canonical tick drives EVERY Active routine — the mirror exclusion
    /// must not over-suppress decided scheduling.
    #[test]
    fn decided_daemon_tick_drives_mapped_mirrors_too() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);
        mapped_mirror(&routines, &work_db, "rt-mirror", "work-mapped");
        canonical_daemon_flags(&ledger);

        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);

        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_ok(),
            "the canonical owner drives unmapped routines"
        );
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-mapped"), "now".to_string())
                .is_ok(),
            "the canonical owner drives mapped mirrors too — no over-suppression"
        );
        assert!(
            owned.is_some(),
            "the daemon-owned tick must hold the workspace writer lock"
        );
    }

    /// Desktop ownership idles the daemon bridge — a due routine stays
    /// unmaterialized — and any lock the daemon held while it owned the
    /// schedule is released for the desktop to take.
    #[test]
    fn desktop_ownership_idles_the_bridge_and_releases_the_workspace_lock() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);

        // Canonical, but the desktop owns the schedule: idle, lock-free.
        ledger.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        ledger
            .set(SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DESKTOP)
            .unwrap();
        ledger
            .set(LEGACY_CRON_COMPATIBILITY_FLAG, "false")
            .unwrap();
        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_err(),
            "the desktop-owned schedule must keep the daemon bridge idle"
        );
        assert!(owned.is_none());

        // Ownership moves to the daemon: the tick drives and holds the lock.
        canonical_daemon_flags(&ledger);
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);
        assert!(
            owned.is_some(),
            "the daemon-owned tick must hold the workspace writer lock"
        );
        assert!(matches!(
            altai_core::WorkStore::open(&work_db),
            Err(altai_core::WorkStoreError::WorkspaceHeld { .. })
        ));

        // Ownership moves back: the tick idles and releases the lock.
        ledger.set(SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DESKTOP).unwrap();
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);
        assert!(
            owned.is_none(),
            "the desktop-owner path must release the workspace lock"
        );
        let _store = altai_core::WorkStore::open(&work_db).unwrap();
    }

    /// A held workspace keeps the canonical daemon bridge idle, and the
    /// refusal leaves no sticky failure state: once the holder releases
    /// the lock, the next tick opens the workspace and materializes.
    #[test]
    fn a_held_workspace_keeps_the_bridge_idle_until_the_lock_is_released() {
        let dir = tempfile::tempdir().unwrap();
        let (cron_bridge, routines, wakes, ledger, work_db, mappings) = gated_bridge(&dir);
        seed_due_routine(&routines);
        canonical_daemon_flags(&ledger);

        let holder = altai_core::workspace_lock::WorkspaceFileLock::acquire(&work_db).unwrap();
        let mut owned = None;
        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_err(),
            "a held workspace must keep the daemon bridge idle"
        );
        assert!(
            owned.is_none(),
            "a refused open must not leave a half-held lock behind"
        );
        drop(holder);

        cron_bridge.gated_tick(60, ledger.as_ref(), &work_db, Some(&mappings), &mut owned);
        assert!(
            wakes
                .claim_wake(&WorkItemId::new("work-1"), "now".to_string())
                .is_ok(),
            "after the holder releases, the bridge must materialize the due wake"
        );
        assert!(owned.is_some());
    }
}
