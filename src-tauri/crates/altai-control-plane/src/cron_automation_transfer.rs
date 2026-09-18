//! Mechanical freeze for CronActor automations (package 101, slice C.a).
//!
//! Canonical scheduling (CP-08-108) may only enable after every live
//! automation in the workspace's `agent_memory.db` `cron_jobs` table is
//! accounted for: Cron-schedule automations are mapped to canonical Work
//! items plus `Recurring` routines with verbatim provenance recorded in
//! `control_plane_cron_automation_mappings` (work.db schema v6);
//! At/Every schedules have no canonical trigger vocabulary yet and are
//! recorded as `incompatible`, which blocks enable until they are removed
//! (never silent loss, never dual-run). Re-runs are idempotent on
//! `(automation_id, content_hash)`; a changed automation after mapping is
//! a typed error, never a silent rewrite of canonical state.
//!
//! Store ownership is respected in both directions: this module reads
//! `agent_memory.db` (the legacy store stays writable only by IsanAgent)
//! and writes only canonical work.db rows. Retirement is bookkeeping on
//! the mapping row plus the fire-time gate in the desktop host — the
//! gate, not the bookkeeping, is the enforcement point.

use crate::{
    RoutineRepository, SqliteRoutineRepository, SqliteWorkItemRepository, WorkItemRepository,
};
use altai_control_protocol::{
    Actor, OrganizationId, ProjectId, Routine, RoutineId, RoutineRevision, RoutineRevisionId,
    RoutineStatus, RoutineTrigger, Revision, WorkItem, WorkItemId, WorkItemKind, WorkStatus,
    ExecutionPhase,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

/// The schedule vocabulary of the legacy `cron_jobs` rows, mirrored exactly
/// from the pinned IsanAgent `ScheduleKind` (externally tagged JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegacySchedule {
    #[serde(rename = "at")]
    At { at_ms: i64 },
    #[serde(rename = "every")]
    Every { every_ms: i64 },
    #[serde(rename = "cron")]
    Cron { cron_expr: String },
}

/// One active automation read from the workspace's legacy store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronAutomationRecord {
    pub id: String,
    pub schedule: LegacySchedule,
    pub message: String,
    pub chat_id: String,
    pub channel: String,
    pub enabled: bool,
}

/// The scheduling attributes the transfer needs, supplied fail-closed by
/// the caller (same shape as the legacy importer's attribution rule).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferAttribution {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub actor: Actor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// The automation's content changed after it was mapped. Re-mapping is
    /// an operator decision, never an automatic rewrite.
    AutomationChanged { automation_id: String },
    /// Enable is blocked: every active automation must be mapped and every
    /// mapping must be compatible; blockers name exactly what stands in
    /// the way.
    EnableBlocked { blockers: Vec<(String, String)> },
    /// The attribution's project does not exist or belongs to another
    /// organization.
    ProjectResolution { reason: String },
    Database { reason: String },
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AutomationChanged { automation_id } => write!(
                f,
                "automation {automation_id} changed after it was mapped; re-mapping is an explicit operator decision"
            ),
            Self::EnableBlocked { blockers } => write!(
                f,
                "canonical scheduling is blocked by {} unmapped automation(s): {}",
                blockers.len(),
                blockers
                    .iter()
                    .map(|(id, reason)| format!("{id}: {reason}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::ProjectResolution { reason } => write!(f, "{reason}"),
            Self::Database { reason } => write!(f, "cron automation transfer failed: {reason}"),
        }
    }
}

impl std::error::Error for TransferError {}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotReport {
    pub total_active: usize,
    pub newly_mapped: usize,
    pub already_mapped: usize,
    pub incompatible: usize,
}

/// Whether the legacy row is canonical now, or structurally unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    Mapped {
        routine_id: RoutineId,
        work_item_id: WorkItemId,
    },
    /// At/Every schedules have no canonical trigger vocabulary yet. The
    /// row is recorded with a reason and blocks enable.
    Incompatible { reason: String },
}

pub struct SqliteCronAutomationTransfer {
    connection: Mutex<Connection>,
}

impl SqliteCronAutomationTransfer {
    /// Owns the schema v6 mapping table. Opened by the migration runner
    /// like every other local schema owner.
    pub fn open(work_db: &Path) -> Result<Self, String> {
        let connection = Connection::open(work_db).map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;
                 CREATE TABLE IF NOT EXISTS control_plane_cron_automation_mappings (
                   automation_id TEXT PRIMARY KEY,
                   disposition TEXT NOT NULL,
                   routine_id TEXT,
                   work_item_id TEXT,
                   content_hash TEXT NOT NULL,
                   provenance_json TEXT NOT NULL,
                   retired_at_unix_seconds INTEGER
                 );",
            )
            .map_err(|error| error.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, TransferError> {
        self.connection.lock().map_err(|_| TransferError::Database {
            reason: "mapping lock poisoned".into(),
        })
    }

    fn db(error: rusqlite::Error) -> TransferError {
        TransferError::Database {
            reason: error.to_string(),
        }
    }

    /// Read every active (`completed_at_ms IS NULL`) automation from the
    /// workspace's legacy `agent_memory.db`. Read-only: the legacy store
    /// stays writable only by IsanAgent.
    pub fn read_active_automations(memory_db: &Path) -> Result<Vec<CronAutomationRecord>, TransferError> {
        let connection = Connection::open_with_flags(
            memory_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(Self::db)?;
        let mut statement = connection
            .prepare(
                "SELECT id, schedule, message, chat_id, channel, enabled
                 FROM cron_jobs
                 WHERE completed_at_ms IS NULL
                 ORDER BY id",
            )
            .map_err(Self::db)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(Self::db)?;
        let mut automations = Vec::new();
        for row in rows {
            let (id, schedule_json, message, chat_id, channel, enabled) = row.map_err(Self::db)?;
            let schedule: LegacySchedule = serde_json::from_str(&schedule_json)
                .map_err(|error| TransferError::Database {
                    reason: format!("automation {id} has an unreadable schedule: {error}"),
                })?;
            automations.push(CronAutomationRecord {
                id,
                schedule,
                message,
                chat_id,
                channel,
                enabled: enabled != 0,
            });
        }
        Ok(automations)
    }

    /// Snapshot every active automation into the canonical domain,
    /// idempotently. Re-runs with unchanged content map nothing new; a
    /// changed automation is a typed error; the work item and routine ids
    /// are derived deterministically from the automation id so a crash
    /// mid-run resumes into the same rows.
    pub fn snapshot(
        &self,
        memory_db: &Path,
        attribution: &TransferAttribution,
        work_items: &SqliteWorkItemRepository,
        routines: &SqliteRoutineRepository,
    ) -> Result<SnapshotReport, TransferError> {
        // Fail closed before any canonical write: the attribution's project
        // must exist and belong to the claimed organization.
        let project_organization = work_items
            .project_organization(&attribution.project_id)
            .map_err(|error| TransferError::ProjectResolution {
                reason: error.to_string(),
            })?;
        if project_organization != attribution.organization_id {
            return Err(TransferError::ProjectResolution {
                reason: format!(
                    "organization {} does not contain project {}",
                    attribution.organization_id.value, attribution.project_id.value
                ),
            });
        }

        let automations = Self::read_active_automations(memory_db)?;
        let mut report = SnapshotReport {
            total_active: automations.len(),
            ..Default::default()
        };
        for automation in &automations {
            let content_hash = Self::content_hash(automation);
            let existing = self
                .lock()?
                .query_row(
                    "SELECT disposition, content_hash FROM control_plane_cron_automation_mappings WHERE automation_id = ?1",
                    [&automation.id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(Self::db)?;
            if let Some((existing_disposition, existing_hash)) = existing {
                if existing_hash != content_hash {
                    return Err(TransferError::AutomationChanged {
                        automation_id: automation.id.clone(),
                    });
                }
                match existing_disposition.as_str() {
                    "mapped" => report.already_mapped += 1,
                    "incompatible" => report.incompatible += 1,
                    _ => {}
                }
                continue;
            }
            match Self::map_automation(automation, attribution, work_items, routines)? {
                Disposition::Mapped {
                    routine_id,
                    work_item_id,
                } => {
                    self.lock()?
                        .execute(
                            "INSERT INTO control_plane_cron_automation_mappings (automation_id, disposition, routine_id, work_item_id, content_hash, provenance_json) VALUES (?1, 'mapped', ?2, ?3, ?4, ?5)",
                            params![
                                automation.id,
                                routine_id.value,
                                work_item_id.value,
                                content_hash,
                                Self::provenance_json(automation),
                            ],
                        )
                        .map_err(Self::db)?;
                    report.newly_mapped += 1;
                }
                Disposition::Incompatible { reason } => {
                    self.lock()?
                        .execute(
                            "INSERT INTO control_plane_cron_automation_mappings (automation_id, disposition, routine_id, work_item_id, content_hash, provenance_json) VALUES (?1, 'incompatible', NULL, NULL, ?2, ?3)",
                            params![automation.id, content_hash, Self::provenance_json_with_reason(automation, &reason),],
                        )
                        .map_err(Self::db)?;
                    report.incompatible += 1;
                }
            }
        }
        Ok(report)
    }

    /// Enable gate: every active automation must be mapped and compatible.
    /// Blockers name exactly what stands in the way, so enabling is a
    /// recorded, inspectable decision.
    pub fn verify_enable_ready(&self, memory_db: &Path) -> Result<(), TransferError> {
        let automations = Self::read_active_automations(memory_db)?;
        let connection = self.lock()?;
        let mut blockers = Vec::new();
        for automation in &automations {
            let row: Option<String> = connection
                .query_row(
                    "SELECT disposition FROM control_plane_cron_automation_mappings WHERE automation_id = ?1",
                    [&automation.id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(Self::db)?;
            match row.as_deref() {
                None => blockers.push((
                    automation.id.clone(),
                    "not snapshotted yet".to_string(),
                )),
                Some("incompatible") => blockers.push((
                    automation.id.clone(),
                    "schedule has no canonical trigger vocabulary (At/Every); remove the automation or extend the vocabulary".to_string(),
                )),
                _ => {}
            }
        }
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(TransferError::EnableBlocked { blockers })
        }
    }

    /// Retirement bookkeeping on the mapping row. Enforcement lives in the
    /// desktop host's fire-time gate; this only records the fact.
    pub fn mark_retired(&self, automation_id: &str) -> Result<(), TransferError> {
        self.lock()?
            .execute(
                "UPDATE control_plane_cron_automation_mappings SET retired_at_unix_seconds = ?2 WHERE automation_id = ?1 AND retired_at_unix_seconds IS NULL",
                params![automation_id, now_unix_seconds() as i64],
            )
            .map_err(Self::db)?;
        Ok(())
    }

    /// Deterministic canonical mapping for one automation. Cron schedules
    /// become a canonical Work item (the message is the intent) plus an
    /// Active `Recurring` routine targeting it; At/Every schedules are
    /// recorded incompatible with a reason.
    fn map_automation(
        automation: &CronAutomationRecord,
        attribution: &TransferAttribution,
        work_items: &SqliteWorkItemRepository,
        routines: &SqliteRoutineRepository,
    ) -> Result<Disposition, TransferError> {
        let cron_expr = match &automation.schedule {
            LegacySchedule::Cron { cron_expr } => cron_expr.clone(),
            LegacySchedule::At { .. } => {
                return Ok(Disposition::Incompatible {
                    reason: "one-shot (At) schedule has no canonical trigger".to_string(),
                })
            }
            LegacySchedule::Every { .. } => {
                return Ok(Disposition::Incompatible {
                    reason: "interval (Every) schedule has no canonical trigger".to_string(),
                })
            }
        };
        let work_item_id = WorkItemId::new(format!("snap_{}", automation.id));
        let now_unix = now_unix_seconds();
        let title: String = {
            let first_line = automation.message.lines().next().unwrap_or("").trim();
            let mut title = first_line.to_string();
            if title.is_empty() {
                title = format!("automation {}", automation.id);
            }
            truncate_bytes(&title, MAX_SNAPSHOT_TITLE_BYTES)
        };
        let work_item = WorkItem {
            id: work_item_id.clone(),
            project_id: attribution.project_id.clone(),
            goal_id: None,
            parent_work_item_id: None,
            kind: WorkItemKind::Task,
            title,
            description: automation.message.clone(),
            status: WorkStatus::Backlog,
            execution_phase: ExecutionPhase::None,
            revision: Revision::INITIAL,
            created_at: rfc3339_now(),
            updated_at: rfc3339_now(),
        };
        work_items
            .create(work_item)
            .map_err(|error| TransferError::Database {
                reason: format!("work item snapshot failed: {error}"),
            })?;

        let routine_id = RoutineId::new(format!("snap_{}", automation.id));
        let revision_id =
            RoutineRevisionId::new(format!("snap_{}_r0", automation.id));
        let routine = Routine {
            id: routine_id.clone(),
            organization_id: attribution.organization_id.clone(),
            current_revision_id: None,
            status: RoutineStatus::Active,
            revision: Revision::INITIAL,
            created_at_unix_seconds: now_unix,
            updated_at_unix_seconds: now_unix,
        };
        routines
            .create(routine)
            .map_err(|error| TransferError::Database {
                reason: format!("routine snapshot failed: {error}"),
            })?;
        let revision = RoutineRevision {
            id: revision_id,
            routine_id: routine_id.clone(),
            revision: Revision::INITIAL,
            trigger: RoutineTrigger::Recurring {
                cron_expression: cron_expr,
            },
            target_work_item_id: work_item_id.clone(),
            created_at_unix_seconds: now_unix,
        };
        routines
            .append_revision(&routine_id, revision)
            .map_err(|error| TransferError::Database {
                reason: format!("routine revision snapshot failed: {error}"),
            })?;
        Ok(Disposition::Mapped {
            routine_id,
            work_item_id,
        })
    }

    fn content_hash(automation: &CronAutomationRecord) -> String {
        let schedule_json =
            serde_json::to_string(&automation.schedule).unwrap_or_default();
        let digest = Sha256::digest(
            [
                schedule_json.as_bytes(),
                b"\n",
                automation.message.as_bytes(),
                b"\n",
                automation.chat_id.as_bytes(),
                b"\n",
                automation.channel.as_bytes(),
                b"\n",
                if automation.enabled { b"1" } else { b"0" },
            ]
            .concat(),
        );
        hex(&digest)
    }

    fn provenance_json(automation: &CronAutomationRecord) -> String {
        Self::provenance_with(automation, None)
    }

    fn provenance_json_with_reason(
        automation: &CronAutomationRecord,
        reason: &str,
    ) -> String {
        Self::provenance_with(automation, Some(reason))
    }

    fn provenance_with(automation: &CronAutomationRecord, reason: Option<&str>) -> String {
        let mut value = serde_json::json!({
            "source": "cron_actor",
            "chat_id": automation.chat_id,
            "channel": automation.channel,
            "message": automation.message,
            "enabled": automation.enabled,
        });
        if let Some(reason) = reason {
            value["incompatible_reason"] = serde_json::Value::String(reason.to_string());
        }
        value.to_string()
    }
}

/// Title cap mirrors the protocol's bounded-prose rule for work items.
const MAX_SNAPSHOT_TITLE_BYTES: usize = 200;

fn truncate_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn rfc3339_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScopeRepository;
    use altai_control_protocol::{Organization, Project, ProjectStatus};
    use std::sync::Arc;

    const CRON_JOBS_DDL: &str = "CREATE TABLE IF NOT EXISTS cron_jobs (
        id TEXT PRIMARY KEY,
        schedule TEXT NOT NULL,
        message TEXT NOT NULL,
        last_run_at_ms INTEGER,
        chat_id TEXT NOT NULL DEFAULT 'unknown',
        channel TEXT NOT NULL DEFAULT 'unknown',
        webhook_token TEXT NOT NULL DEFAULT '',
        trigger_claim_token TEXT NOT NULL DEFAULT '',
        trigger_claimed_at_ms INTEGER,
        completed_at_ms INTEGER,
        enabled INTEGER NOT NULL DEFAULT 1
    );";

    struct Harness {
        _dir: tempfile::TempDir,
        transfer: SqliteCronAutomationTransfer,
        work_items: Arc<SqliteWorkItemRepository>,
        routines: Arc<SqliteRoutineRepository>,
        memory_db: std::path::PathBuf,
        attribution: TransferAttribution,
    }

    fn harness() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let work_db = dir.path().join("work.db");
        let scope = crate::SqliteScopeRepository::open(&work_db).unwrap();
        let organization_id = OrganizationId::new("org");
        scope
            .create_organization(Organization {
                id: organization_id.clone(),
                name: "Transfer org".into(),
                revision: Revision::INITIAL,
                created_at: "2026-09-18T00:00:00.000Z".into(),
                updated_at: "2026-09-18T00:00:00.000Z".into(),
            })
            .unwrap();
        let project_id = ProjectId::new("proj");
        scope
            .create_project(Project {
                id: project_id.clone(),
                organization_id: organization_id.clone(),
                goal_ids: Vec::new(),
                name: "Transfer project".into(),
                description: String::new(),
                status: ProjectStatus::Active,
                revision: Revision::INITIAL,
                created_at: "2026-09-18T00:00:00.000Z".into(),
                updated_at: "2026-09-18T00:00:00.000Z".into(),
            })
            .unwrap();
        let transfer = SqliteCronAutomationTransfer::open(&work_db).unwrap();
        let work_items = Arc::new(SqliteWorkItemRepository::open(&work_db).unwrap());
        let routines = Arc::new(SqliteRoutineRepository::open(&work_db).unwrap());
        let memory_db = dir.path().join("agent_memory.db");
        let connection = Connection::open(&memory_db).unwrap();
        connection.execute_batch(CRON_JOBS_DDL).unwrap();
        drop(connection);
        Harness {
            _dir: dir,
            transfer,
            work_items,
            routines,
            memory_db,
            attribution: TransferAttribution {
                organization_id,
                project_id,
                actor: Actor::System {
                    component: "transfer-test".into(),
                },
            },
        }
    }

    fn insert_job(connection: &Connection, id: &str, schedule_json: &str, message: &str) {
        connection
            .execute(
                "INSERT INTO cron_jobs (id, schedule, message, chat_id, channel) VALUES (?1, ?2, ?3, 'chat-1', 'tauri')",
                params![id, schedule_json, message],
            )
            .unwrap();
    }

    #[test]
    fn cron_automations_map_to_canonical_work_and_routine() {
        let h = harness();
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            insert_job(
                &connection,
                "job-1",
                r#"{"cron":{"cron_expr":"0 9 * * MON"}}"#,
                "Weekly report",
            );
        }
        let report = h
            .transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .unwrap();
        assert_eq!(
            report,
            SnapshotReport {
                total_active: 1,
                newly_mapped: 1,
                already_mapped: 0,
                incompatible: 0,
            }
        );
        let work_item = h
            .work_items
            .get(&WorkItemId::new("snap_job-1"))
            .unwrap();
        assert_eq!(work_item.status, WorkStatus::Backlog);
        assert_eq!(work_item.description, "Weekly report");
        let routine = h
            .routines
            .get(&RoutineId::new("snap_job-1"))
            .unwrap()
            .expect("routine must exist");
        assert_eq!(routine.status, RoutineStatus::Active);
        let revision = h
            .routines
            .get_revision(&RoutineRevisionId::new("snap_job-1_r0"))
            .unwrap()
            .expect("revision must exist");
        assert_eq!(
            revision.trigger,
            RoutineTrigger::Recurring {
                cron_expression: "0 9 * * MON".to_string(),
            }
        );
        assert_eq!(revision.target_work_item_id, work_item.id);
        h.transfer.verify_enable_ready(&h.memory_db).unwrap();
    }

    #[test]
    fn snapshot_reruns_are_idempotent_and_enable_blocks_on_incompatible() {
        let h = harness();
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            insert_job(
                &connection,
                "job-cron",
                r#"{"cron":{"cron_expr":"*/5 * * * *"}}"#,
                "Cron job",
            );
            insert_job(
                &connection,
                "job-every",
                r#"{"every":{"every_ms":90000}}"#,
                "Interval job",
            );
        }
        let first = h
            .transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .unwrap();
        assert_eq!(
            (first.newly_mapped, first.incompatible, first.already_mapped),
            (1, 1, 0)
        );
        // Re-run: everything is already recorded, nothing new is written.
        let second = h
            .transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .unwrap();
        assert_eq!(
            (second.newly_mapped, second.incompatible, second.already_mapped),
            (0, 1, 1)
        );
        // Enable is blocked by the incompatible row, naming it exactly.
        let blocked = h
            .transfer
            .verify_enable_ready(&h.memory_db)
            .expect_err("enable must be blocked while an incompatible automation exists");
        match blocked {
            TransferError::EnableBlocked { blockers } => {
                assert_eq!(blockers.len(), 1);
                assert_eq!(blockers[0].0, "job-every");
            }
            other => panic!("expected EnableBlocked, got {other:?}"),
        }
    }

    #[test]
    fn unmapped_automations_block_enable_and_changed_content_is_a_typed_error() {
        let h = harness();
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            insert_job(
                &connection,
                "job-2",
                r#"{"cron":{"cron_expr":"0 0 * * *"}}"#,
                "Daily job",
            );
        }
        let blocked = h
            .transfer
            .verify_enable_ready(&h.memory_db)
            .expect_err("unmapped automation must block enable");
        assert!(matches!(blocked, TransferError::EnableBlocked { ref blockers } if blockers.len() == 1));

        h.transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .unwrap();
        h.transfer.verify_enable_ready(&h.memory_db).unwrap();

        // The automation's content changes after mapping: a re-run must
        // refuse typed instead of silently rewriting canonical state.
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            connection
                .execute(
                    "UPDATE cron_jobs SET message = 'Changed intent' WHERE id = 'job-2'",
                    [],
                )
                .unwrap();
        }
        let changed = h
            .transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .expect_err("changed automation must be a typed error");
        assert_eq!(
            changed,
            TransferError::AutomationChanged {
                automation_id: "job-2".to_string(),
            }
        );
    }

    #[test]
    fn completed_automations_are_ignored_and_retirement_is_recorded() {
        let h = harness();
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            insert_job(
                &connection,
                "job-gone",
                r#"{"cron":{"cron_expr":"0 0 * * *"}}"#,
                "Already completed",
            );
            connection
                .execute(
                    "UPDATE cron_jobs SET completed_at_ms = 123 WHERE id = 'job-gone'",
                    [],
                )
                .unwrap();
        }
        let report = h
            .transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .unwrap();
        assert_eq!(report.total_active, 0);
        assert_eq!(report.newly_mapped, 0);
        h.transfer.verify_enable_ready(&h.memory_db).unwrap();

        // Retirement bookkeeping lands on the mapping row, not on the
        // legacy store (IsanAgent owns cron_jobs writes).
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            insert_job(
                &connection,
                "job-live",
                r#"{"cron":{"cron_expr":"0 0 * * *"}}"#,
                "Live",
            );
        }
        h.transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items, &h.routines)
            .unwrap();
        h.transfer.mark_retired("job-live").unwrap();
        h.transfer.mark_retired("job-live").unwrap(); // idempotent
        let retired: Option<i64> = h
            .transfer
            .lock()
            .unwrap()
            .query_row(
                "SELECT retired_at_unix_seconds FROM control_plane_cron_automation_mappings WHERE automation_id = 'job-live'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(retired.is_some());
    }

    #[test]
    fn foreign_attribution_is_refused_before_any_canonical_write() {
        let h = harness();
        {
            let connection = Connection::open(&h.memory_db).unwrap();
            insert_job(
                &connection,
                "job-3",
                r#"{"cron":{"cron_expr":"0 0 * * *"}}"#,
                "Daily job",
            );
        }
        let foreign = TransferAttribution {
            organization_id: OrganizationId::new("elsewhere"),
            project_id: h.attribution.project_id.clone(),
            actor: h.attribution.actor.clone(),
        };
        let error = h
            .transfer
            .snapshot(&h.memory_db, &foreign, &h.work_items, &h.routines)
            .expect_err("foreign organization must be refused");
        assert!(matches!(error, TransferError::ProjectResolution { .. }));
        // Nothing was written to the mapping table.
        let count: i64 = h
            .transfer
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM control_plane_cron_automation_mappings",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}
