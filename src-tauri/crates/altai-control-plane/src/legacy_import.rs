//! Read-only, idempotent projection of legacy frontend stores into canonical
//! WorkItems (CP-08-104, package 100 PR 2), built on the accepted boundary in
//! `LEGACY_IMPORTER_DISCOVERY.md`.
//!
//! The legacy Tauri plugin-store files stay authoritative until cutover: this
//! module only reads them, never writes back, and records each record under
//! its own stable key plus a content hash in
//! `control_plane_legacy_import_mappings`, following the dedicated-mapping
//! pattern of [`crate::legacy_work_bridge`]. An identical re-import writes
//! nothing; changed content updates exactly one WorkItem through optimistic
//! concurrency; nothing is ever deleted. Legacy statuses are recorded
//! verbatim as provenance payload — they classify storage-side into a real
//! [`WorkStatus`] but are never translated into lifecycle transitions.
//! Issue/pr sources are recorded verbatim in that payload instead of minting
//! ExternalObjects (immutable provider ids are out of scope here). Attribution
//! is resolved by the host — the CP-04 default local organization plus one
//! importer-owned project; this module never creates organizations or
//! projects, mints no credential, touches no Attempt, and enqueues no wake.
//! Stability is per record, never per snapshot: a smaller subsequent import
//! deletes nothing, and a mid-import failure leaves already-projected records
//! in place.
//!
//! Conceptually distinct from `altai_core::legacy_import` (the read-only
//! preview of SQLite backends), which has its own stable-key scheme; this
//! module owns the frontend-store import keyspace.

use crate::{SqliteWorkItemRepository, WorkItemRepository, WorkItemRepositoryError};
use altai_control_protocol::{
    ExecutionPhase, OrganizationId, ProjectId, Revision, WorkItem, WorkItemId, WorkStatus,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::Path, sync::Mutex};

/// Legacy assignments store, relative to the host-resolved app-data directory.
const ASSIGNMENTS_STORE_FILE: &str = "altai-assignments.json";
/// Legacy per-session todo store, relative to the same directory.
const TODOS_STORE_FILE: &str = "altai-ai-todos.json";
/// Key inside the assignments store holding the record array.
const ASSIGNMENTS_STORE_KEY: &str = "assignments";
/// Every todo session lives under one `todos:<sessionId>` key per session.
const TODOS_STORE_KEY_PREFIX: &str = "todos:";
/// Per-file byte cap, matching the sibling altai-core legacy-import limits:
/// these stores are small by construction, so oversize input is corruption.
const MAX_LEGACY_JSON_BYTES: u64 = 4 * 1024 * 1024;
/// Per-array entry cap for the same reason.
const MAX_JSON_SOURCE_ENTRIES: usize = 2_000;

/// Org/project scope every imported WorkItem lands under, resolved **by the
/// host** before [`LegacyImportRepository::import`] is called: the CP-04
/// default local organization
/// ([`crate::SqliteScopeRepository::ensure_default_local_organization`]) plus
/// one designated importer-owned project. This module never auto-creates
/// either; callers that have not resolved attribution yet pass `None` and are
/// refused instead of silently misattributing history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyImportAttribution {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
}

/// Typed counts for one import run. `*_imported`/`*_updated` count records
/// whose canonical row was actually written this run; a re-import of unchanged
/// content reports `unchanged` instead and writes nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LegacyImportReport {
    pub assignments_total: usize,
    pub assignments_imported: usize,
    pub assignments_unchanged: usize,
    pub assignments_updated: usize,
    pub assignments_skipped_corrupt: usize,
    pub todos_total: usize,
    pub todos_imported_manual_only: usize,
    pub todos_skipped_agent_plan: usize,
    pub todos_skipped_corrupt: usize,
    pub external_objects_skipped: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyImportError {
    /// The store file is missing, unreadable, or structurally corrupt (bad
    /// JSON, non-object root, mis-typed top-level key). Entry-level corruption
    /// is never this: those entries are skipped and counted.
    InvalidStore {
        reason: String,
    },
    /// `attribution` was `None`; the host must resolve the default local
    /// organization and the importer-owned project first.
    AttributionRequired,
    /// Another writer moved the canonical WorkItem under a known legacy key;
    /// the import refuses instead of overwriting.
    RevisionConflict {
        legacy_key: String,
    },
    Database {
        reason: String,
    },
    Serialization {
        reason: String,
    },
}

impl std::fmt::Display for LegacyImportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStore { reason } => {
                write!(formatter, "legacy import store is invalid: {reason}")
            }
            Self::AttributionRequired => write!(
                formatter,
                "legacy import requires host-resolved organization/project attribution"
            ),
            Self::RevisionConflict { legacy_key } => write!(
                formatter,
                "legacy import lost the canonical row for {legacy_key} to another writer"
            ),
            Self::Database { reason } => {
                write!(formatter, "legacy import database error: {reason}")
            }
            Self::Serialization { reason } => {
                write!(formatter, "legacy import serialization error: {reason}")
            }
        }
    }
}

impl std::error::Error for LegacyImportError {}

pub trait LegacyImportRepository: Send + Sync {
    /// Read the legacy stores under `store` (the app-data directory holding
    /// both plugin-store files) and project every record into canonical
    /// WorkItems under `attribution`.
    fn import(
        &self,
        store: &Path,
        attribution: Option<LegacyImportAttribution>,
    ) -> Result<LegacyImportReport, LegacyImportError>;
}

pub struct SqliteLegacyImportRepository {
    connection: Mutex<Connection>,
    work_items: SqliteWorkItemRepository,
}

impl SqliteLegacyImportRepository {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                "PRAGMA busy_timeout = 5000;
                 CREATE TABLE IF NOT EXISTS control_plane_legacy_import_mappings (
                    legacy_key TEXT PRIMARY KEY,
                    canonical_work_item_id TEXT NOT NULL UNIQUE,
                    content_hash TEXT NOT NULL
                 );",
            )
            .map_err(|error| error.to_string())?;
        let work_items = SqliteWorkItemRepository::open(path)?;
        Ok(Self {
            connection: Mutex::new(connection),
            work_items,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LegacyImportError> {
        self.connection
            .lock()
            .map_err(|_| LegacyImportError::Database {
                reason: "legacy import lock poisoned".into(),
            })
    }
}

/// What one record's projection did to storage this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordOutcome {
    Imported,
    Unchanged,
    Updated,
}

impl LegacyImportRepository for SqliteLegacyImportRepository {
    fn import(
        &self,
        store: &Path,
        attribution: Option<LegacyImportAttribution>,
    ) -> Result<LegacyImportReport, LegacyImportError> {
        let Some(attribution) = attribution else {
            return Err(LegacyImportError::AttributionRequired);
        };
        // Both stores are parsed before anything is written: structural
        // corruption refuses the whole run instead of half-importing it.
        let assignment_entries = read_assignments_store(store)?;
        let todo_sessions = read_todos_store(store)?;
        let mut report = LegacyImportReport::default();

        let (assignments, skipped_corrupt) = parse_entries::<LegacyAssignment>(&assignment_entries);
        report.assignments_total = assignment_entries.len();
        report.assignments_skipped_corrupt = skipped_corrupt;
        for record in assignments {
            if matches!(
                record.source,
                LegacyAssignmentSource::Issue { .. } | LegacyAssignmentSource::Pr { .. }
            ) {
                // Immutable provider ids are out of scope; the source stays
                // verbatim in the WorkItem payload instead.
                report.external_objects_skipped += 1;
            }
            let projection = project_assignment(&record)?;
            match self.project(&attribution, &projection)? {
                RecordOutcome::Imported => report.assignments_imported += 1,
                RecordOutcome::Unchanged => report.assignments_unchanged += 1,
                RecordOutcome::Updated => report.assignments_updated += 1,
            }
        }

        for (session_id, entries) in todo_sessions {
            let (todos, skipped_corrupt) = parse_entries::<LegacyTodo>(&entries);
            report.todos_total += entries.len();
            report.todos_skipped_corrupt += skipped_corrupt;
            for record in todos {
                // Only explicit board todos are work requests; agent-plan and
                // origin-less records are RunPlanItem territory (G3).
                if record.origin != Some(LegacyTodoOrigin::Manual) {
                    report.todos_skipped_agent_plan += 1;
                    continue;
                }
                let projection = project_manual_todo(&session_id, &record)?;
                match self.project(&attribution, &projection)? {
                    RecordOutcome::Unchanged => {}
                    // Both a create and an optimistic update wrote canonical
                    // state; the contract's todo buckets have one write count.
                    RecordOutcome::Imported | RecordOutcome::Updated => {
                        report.todos_imported_manual_only += 1;
                    }
                }
            }
        }
        Ok(report)
    }
}

impl SqliteLegacyImportRepository {
    /// Project one parsed legacy record through the mapping table: absent key
    /// creates, equal hash is a no-op, changed content updates exactly one row
    /// under optimistic concurrency.
    fn project(
        &self,
        attribution: &LegacyImportAttribution,
        projection: &LegacyProjection,
    ) -> Result<RecordOutcome, LegacyImportError> {
        let work_item_id = WorkItemId::new(projection.legacy_key.clone());
        let mapping: Option<(String, String)> = self
            .lock()?
            .query_row(
                "SELECT canonical_work_item_id, content_hash
                 FROM control_plane_legacy_import_mappings WHERE legacy_key = ?1",
                [&projection.legacy_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(db_error)?;

        let Some((canonical_id, mapped_hash)) = mapping else {
            let created_at = legacy_or_import_stamp(projection.legacy_created_at_unix_ms);
            let updated_at = legacy_or_import_stamp(projection.legacy_updated_at_unix_ms);
            let candidate = assembled_work_item(attribution, projection, created_at, updated_at);
            match self.work_items.create(candidate.clone()) {
                Ok(()) => {}
                // Crash-recovery window: the deterministic id exists without a
                // mapping row. Identical content recovers by recording the
                // mapping; anything else fails closed.
                Err(WorkItemRepositoryError::AlreadyExists { .. }) => {
                    let stored = self
                        .work_items
                        .get(&work_item_id)
                        .map_err(repository_error)?;
                    if stored.project_id != candidate.project_id
                        || stored.description != candidate.description
                    {
                        return Err(LegacyImportError::RevisionConflict {
                            legacy_key: projection.legacy_key.clone(),
                        });
                    }
                }
                Err(error) => return Err(repository_error(error)),
            }
            // Crash convergence: no transaction can span the WorkItem create
            // and this mapping insert (separate connections), but every
            // interleaving converges to the same fixed point on the next run.
            // Create-without-mapping is recovered by the byte-equal
            // AlreadyExists comparison above; mapping-without-create cannot
            // happen because this insert only follows a verified create.
            // Re-import is therefore always safe after a crash.
            self.lock()?
                .execute(
                    "INSERT INTO control_plane_legacy_import_mappings
                     (legacy_key, canonical_work_item_id, content_hash) VALUES (?1, ?2, ?3)",
                    params![
                        projection.legacy_key,
                        work_item_id.value,
                        projection.content_hash
                    ],
                )
                .map_err(db_error)?;
            return Ok(RecordOutcome::Imported);
        };

        // A mapping row pointing outside this importer's deterministic
        // keyspace means someone rewired it; never follow it.
        if canonical_id != work_item_id.value {
            return Err(LegacyImportError::RevisionConflict {
                legacy_key: projection.legacy_key.clone(),
            });
        }
        if mapped_hash == projection.content_hash {
            // Unchanged content still owns a row: verify it exists and still
            // sits in the attributed project, otherwise a deleted or
            // re-attributed canonical row would report "unchanged" forever.
            let stored = match self.work_items.get(&work_item_id) {
                Ok(stored) => stored,
                Err(WorkItemRepositoryError::NotFound { .. }) => {
                    return Err(LegacyImportError::RevisionConflict {
                        legacy_key: projection.legacy_key.clone(),
                    });
                }
                Err(error) => return Err(repository_error(error)),
            };
            if stored.project_id != attribution.project_id {
                return Err(LegacyImportError::RevisionConflict {
                    legacy_key: projection.legacy_key.clone(),
                });
            }
            return Ok(RecordOutcome::Unchanged);
        }

        let current = self
            .work_items
            .get(&work_item_id)
            .map_err(repository_error)?;
        let mut next = assembled_work_item(
            attribution,
            projection,
            current.created_at.clone(),
            legacy_or_import_stamp(projection.legacy_updated_at_unix_ms),
        );
        next.revision = current.revision.next();
        match self.work_items.replace_if_revision(next, current.revision) {
            Ok(_) => {}
            Err(WorkItemRepositoryError::StaleRevision { .. })
            | Err(WorkItemRepositoryError::ProjectMismatch { .. }) => {
                return Err(LegacyImportError::RevisionConflict {
                    legacy_key: projection.legacy_key.clone(),
                });
            }
            Err(error) => return Err(repository_error(error)),
        }
        // Crash convergence: the guarded replace and this hash refresh are two
        // commits with no transaction spanning them (separate connections).
        // A crash between them leaves new content under an old hash, so the
        // next run re-enters this update path and rewrites byte-identical
        // content through the same optimistic guard — content converges to
        // the fixed point; only the row's revision may advance one extra step.
        self.lock()?
            .execute(
                "UPDATE control_plane_legacy_import_mappings SET content_hash = ?2
                 WHERE legacy_key = ?1",
                params![projection.legacy_key, projection.content_hash],
            )
            .map_err(db_error)?;
        Ok(RecordOutcome::Updated)
    }
}

fn repository_error(error: WorkItemRepositoryError) -> LegacyImportError {
    LegacyImportError::Database {
        reason: error.to_string(),
    }
}

fn db_error(error: rusqlite::Error) -> LegacyImportError {
    LegacyImportError::Database {
        reason: error.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Store parsing — read-only over tauri plugin-store JSON files.
// ---------------------------------------------------------------------------

/// Read one store file behind its size caps: the byte cap is checked on file
/// metadata before any read, the entry cap per decoded record array.
fn read_bounded_store(path: &Path) -> Result<Vec<u8>, LegacyImportError> {
    let metadata = fs::metadata(path).map_err(|error| LegacyImportError::InvalidStore {
        reason: format!("{} is unreadable: {error}", path.display()),
    })?;
    if metadata.len() > MAX_LEGACY_JSON_BYTES {
        return Err(LegacyImportError::InvalidStore {
            reason: format!(
                "{} exceeds the {MAX_LEGACY_JSON_BYTES} byte store cap",
                path.display()
            ),
        });
    }
    fs::read(path).map_err(|error| LegacyImportError::InvalidStore {
        reason: format!("{} is unreadable: {error}", path.display()),
    })
}

fn bounded_entries(entries: &[serde_json::Value], what: &str) -> Result<(), LegacyImportError> {
    if entries.len() > MAX_JSON_SOURCE_ENTRIES {
        return Err(LegacyImportError::InvalidStore {
            reason: format!("{what} exceeds the {MAX_JSON_SOURCE_ENTRIES} entry cap"),
        });
    }
    Ok(())
}

/// The assignments store is one JSON object whose `assignments` key holds the
/// record array; every other shape is structural corruption.
fn read_assignments_store(store: &Path) -> Result<Vec<serde_json::Value>, LegacyImportError> {
    let path = store.join(ASSIGNMENTS_STORE_FILE);
    let bytes = read_bounded_store(&path)?;
    let root: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&bytes).map_err(|error| LegacyImportError::InvalidStore {
            reason: format!("{} is not valid JSON: {error}", path.display()),
        })?;
    match root.get(ASSIGNMENTS_STORE_KEY) {
        None => Ok(Vec::new()),
        Some(value) => {
            let entries =
                value
                    .as_array()
                    .cloned()
                    .ok_or_else(|| LegacyImportError::InvalidStore {
                        reason: "the assignments key must hold an array".into(),
                    })?;
            bounded_entries(&entries, "the assignment source")?;
            Ok(entries)
        }
    }
}

/// The todos store maps one `todos:<sessionId>` key per session to that
/// session's record array; sessions are returned sorted for determinism.
fn read_todos_store(
    store: &Path,
) -> Result<BTreeMap<String, Vec<serde_json::Value>>, LegacyImportError> {
    let path = store.join(TODOS_STORE_FILE);
    let bytes = read_bounded_store(&path)?;
    let root: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&bytes).map_err(|error| LegacyImportError::InvalidStore {
            reason: format!("{} is not valid JSON: {error}", path.display()),
        })?;
    let mut sessions = BTreeMap::new();
    for (key, value) in &root {
        let Some(session_id) = key.strip_prefix(TODOS_STORE_KEY_PREFIX) else {
            continue;
        };
        let Some(array) = value.as_array() else {
            return Err(LegacyImportError::InvalidStore {
                reason: format!("todo session '{key}' must hold an array"),
            });
        };
        bounded_entries(array, &format!("todo session '{key}'"))?;
        sessions.insert(session_id.to_string(), array.clone());
    }
    Ok(sessions)
}

/// Per-entry decode with the stores' own zod discipline: corrupt entries are
/// dropped and counted, never fatal.
fn parse_entries<T: DeserializeOwned>(entries: &[serde_json::Value]) -> (Vec<T>, usize) {
    let mut parsed = Vec::with_capacity(entries.len());
    let mut skipped_corrupt = 0;
    for entry in entries {
        match serde_json::from_value::<T>(entry.clone()) {
            Ok(record) => parsed.push(record),
            Err(_) => skipped_corrupt += 1,
        }
    }
    (parsed, skipped_corrupt)
}

// ---------------------------------------------------------------------------
// Canonical projection
// ---------------------------------------------------------------------------

/// The stable, comparable projection of one legacy record: everything the
/// content hash covers. Import wall-clock stamps stay outside so re-imports
/// hash identically.
struct LegacyProjection {
    legacy_key: String,
    title: String,
    status: WorkStatus,
    description: String,
    content_hash: String,
    legacy_created_at_unix_ms: Option<u64>,
    legacy_updated_at_unix_ms: Option<u64>,
}

/// The provenance payload recorded verbatim as the WorkItem description.
#[derive(Serialize)]
struct LegacyImportedProvenance<'a> {
    legacy_surface: &'static str,
    legacy_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    legacy_created_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    legacy_updated_at_unix_ms: Option<u64>,
    legacy_record: serde_json::Value,
}

fn provenance(
    surface: &'static str,
    legacy_key: &str,
    created_at_unix_ms: Option<u64>,
    updated_at_unix_ms: Option<u64>,
    record: &impl Serialize,
) -> Result<String, LegacyImportError> {
    let serialization_error = |error: serde_json::Error| LegacyImportError::Serialization {
        reason: error.to_string(),
    };
    let legacy_record = serde_json::to_value(record).map_err(serialization_error)?;
    let payload = LegacyImportedProvenance {
        legacy_surface: surface,
        legacy_key,
        legacy_created_at_unix_ms: created_at_unix_ms,
        legacy_updated_at_unix_ms: updated_at_unix_ms,
        legacy_record,
    };
    serde_json::to_string(&payload).map_err(serialization_error)
}

fn content_digest(description: &str) -> String {
    format!("{:x}", Sha256::digest(description.as_bytes()))
}

/// Storage-side classification only: terminal legacy outcomes land on
/// `done`, everything else waits in `backlog`. The legacy status itself is
/// never translated away — it stays verbatim in the provenance payload.
fn assignment_storage_status(status: LegacyAssignmentStatus) -> WorkStatus {
    match status {
        LegacyAssignmentStatus::Done
        | LegacyAssignmentStatus::Failed
        | LegacyAssignmentStatus::Cancelled => WorkStatus::Done,
        LegacyAssignmentStatus::Dispatching
        | LegacyAssignmentStatus::Running
        | LegacyAssignmentStatus::AwaitingApproval => WorkStatus::Backlog,
    }
}

fn todo_storage_status(status: LegacyTodoStatus) -> WorkStatus {
    match status {
        LegacyTodoStatus::Completed => WorkStatus::Done,
        LegacyTodoStatus::Pending | LegacyTodoStatus::InProgress => WorkStatus::Backlog,
    }
}

fn execution_phase_for(status: WorkStatus) -> ExecutionPhase {
    if status == WorkStatus::Done {
        ExecutionPhase::Terminal
    } else {
        ExecutionPhase::None
    }
}

fn project_assignment(record: &LegacyAssignment) -> Result<LegacyProjection, LegacyImportError> {
    let legacy_key = framed_legacy_key("assignment", &[&record.id]);
    let description = provenance(
        "assignment",
        &legacy_key,
        Some(record.created_at),
        Some(record.updated_at),
        record,
    )?;
    let content_hash = content_digest(&description);
    Ok(LegacyProjection {
        status: assignment_storage_status(record.status),
        title: record.title.clone(),
        description,
        content_hash,
        legacy_key,
        legacy_created_at_unix_ms: Some(record.created_at),
        legacy_updated_at_unix_ms: Some(record.updated_at),
    })
}

/// Length-framed stable key in the sibling altai-core legacy-import scheme
/// (`legacy:v1:<kind>:<len>:<component>:…`): each component carries its byte
/// length, so ("a:b", "c") and ("a", "b:c") frame to different keys and
/// composite identities can never collide across a different split. Used for
/// both surfaces so this module owns one uniform keyspace.
fn framed_legacy_key(kind: &str, components: &[&str]) -> String {
    let mut key = format!("legacy:v1:{kind}");
    for component in components {
        key.push(':');
        key.push_str(&component.len().to_string());
        key.push(':');
        key.push_str(component);
    }
    key
}

fn project_manual_todo(
    session_id: &str,
    record: &LegacyTodo,
) -> Result<LegacyProjection, LegacyImportError> {
    let legacy_key = framed_legacy_key("todo", &[session_id, &record.id]);
    let description = provenance("manual_todo", &legacy_key, None, None, record)?;
    let content_hash = content_digest(&description);
    Ok(LegacyProjection {
        status: todo_storage_status(record.status),
        title: record.title.clone(),
        description,
        content_hash,
        legacy_key,
        legacy_created_at_unix_ms: None,
        legacy_updated_at_unix_ms: None,
    })
}

/// Assignments keep their preserved epoch-ms timestamps; records without any
/// (manual todos) are stamped at import time.
fn legacy_or_import_stamp(unix_ms: Option<u64>) -> String {
    unix_ms
        .map(|ms| format!("legacy-ms:{ms}"))
        .unwrap_or_else(import_timestamp)
}

fn import_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn assembled_work_item(
    attribution: &LegacyImportAttribution,
    projection: &LegacyProjection,
    created_at: String,
    updated_at: String,
) -> WorkItem {
    WorkItem {
        id: WorkItemId::new(projection.legacy_key.clone()),
        project_id: attribution.project_id.clone(),
        goal_id: None,
        parent_work_item_id: None,
        kind: altai_control_protocol::WorkItemKind::Task,
        title: projection.title.clone(),
        description: projection.description.clone(),
        status: projection.status,
        execution_phase: execution_phase_for(projection.status),
        revision: Revision::INITIAL,
        created_at,
        updated_at,
    }
}

// ---------------------------------------------------------------------------
// Legacy record shapes — serde mirrors of the TypeScript store schemas
// (LEGACY_IMPORTER_DISCOVERY.md L1/L2). Unknown fields are dropped exactly
// like the stores' own zod validation drops them.
// ---------------------------------------------------------------------------

/// Six-value assignment lifecycle, kept verbatim in provenance payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LegacyAssignmentStatus {
    Dispatching,
    Running,
    AwaitingApproval,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyTodoStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyAssignmentOrigin {
    Manual,
    Orchestrator,
}

/// Absent on legacy/runtime todos; only `Manual` marks an import candidate
/// (G3: origin-less records are skipped rather than guessed at).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyTodoOrigin {
    Agent,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum LegacyAssignmentSource {
    Issue {
        owner: String,
        repo: String,
        number: u64,
        url: String,
    },
    Pr {
        owner: String,
        repo: String,
        number: u64,
        url: String,
    },
    Todo {
        #[serde(rename = "todoId")]
        todo_id: String,
    },
    Task {
        prompt: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyAssignmentOrchestration {
    pub workspace_key: String,
    pub task_session_id: String,
    pub task_key: String,
    pub attempt: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyAssignmentRunConfig {
    pub agent_id: Option<String>,
    pub model_id: Option<String>,
    pub skills: Option<Vec<String>>,
    pub permission_mode: Option<String>,
    pub workspace_path: Option<String>,
    pub branch_name: Option<String>,
    pub base_branch: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDeliveryRun {
    pub workspace_path: String,
    pub branch_name: String,
    pub base_branch: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyDeliveryDraftPr {
    pub workspace_path: String,
    pub branch_name: String,
    pub base_branch: String,
    pub pull_number: u64,
    pub pull_url: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum LegacyAssignmentDelivery {
    #[serde(rename = "worktree")]
    Worktree(LegacyDeliveryRun),
    #[serde(rename = "publishing")]
    Publishing(LegacyDeliveryRun),
    #[serde(rename = "applying")]
    Applying(LegacyDeliveryRun),
    #[serde(rename = "applied")]
    Applied(LegacyDeliveryRun),
    #[serde(rename = "failed")]
    Failed(LegacyDeliveryRun),
    #[serde(rename = "draft-pr")]
    DraftPr(LegacyDeliveryDraftPr),
}

/// One legacy agent-run assignment (discovery L1): epoch-ms timestamps are
/// preserved; `status` is recorded verbatim, never translated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyAssignment {
    pub id: String,
    pub source: LegacyAssignmentSource,
    pub session_id: String,
    pub title: String,
    pub status: LegacyAssignmentStatus,
    pub origin: Option<LegacyAssignmentOrigin>,
    pub orchestration: Option<LegacyAssignmentOrchestration>,
    pub run_config: Option<LegacyAssignmentRunConfig>,
    pub delivery: Option<LegacyAssignmentDelivery>,
    pub created_at: u64,
    pub updated_at: u64,
}

/// One legacy todo (discovery L2): no timestamps exist anywhere, so import
/// time becomes the canonical timestamp at projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyTodo {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: LegacyTodoStatus,
    pub origin: Option<LegacyTodoOrigin>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ScopeRepository, SqliteScopeRepository};
    use altai_control_protocol::{Project, ProjectStatus};

    const ASSIGNMENT_A: &str = r#"{
        "id": "asg_a",
        "source": {"kind": "task", "prompt": "Ship the importer"},
        "sessionId": "sess_1",
        "title": "Ship importer",
        "status": "done",
        "createdAt": 1700000000000,
        "updatedAt": 1700000001000
    }"#;

    const ASSIGNMENT_B_ISSUE_SOURCE: &str = r#"{
        "id": "asg_b",
        "source": {"kind": "issue", "owner": "altaidevorg", "repo": "altai-app", "number": 101, "url": "https://github.com/altaidevorg/altai-app/issues/101"},
        "sessionId": "sess_2",
        "title": "Triage issue",
        "status": "running",
        "createdAt": 1700000010000,
        "updatedAt": 1700000020000
    }"#;

    const MANUAL_TODO: &str = r#"{
        "id": "t_1",
        "title": "Manual board request",
        "description": "User-created durable work",
        "status": "pending",
        "origin": "manual"
    }"#;

    struct Fixture {
        dir: tempfile::TempDir,
        repository: SqliteLegacyImportRepository,
        work_items: SqliteWorkItemRepository,
        project_id: ProjectId,
        other_project_id: ProjectId,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let scopes = SqliteScopeRepository::open(&database).unwrap();
        scopes.ensure_default_local_organization().unwrap();
        let project_id = ProjectId::new("proj_legacy_import");
        scopes
            .create_project(Project {
                id: project_id.clone(),
                organization_id: OrganizationId::new("local"),
                goal_ids: vec![],
                name: "Legacy import".into(),
                description: String::new(),
                status: ProjectStatus::Active,
                revision: Revision::INITIAL,
                created_at: "now".into(),
                updated_at: "now".into(),
            })
            .unwrap();
        let other_project_id = ProjectId::new("proj_other");
        scopes
            .create_project(Project {
                id: other_project_id.clone(),
                organization_id: OrganizationId::new("local"),
                goal_ids: vec![],
                name: "Other".into(),
                description: String::new(),
                status: ProjectStatus::Active,
                revision: Revision::INITIAL,
                created_at: "now".into(),
                updated_at: "now".into(),
            })
            .unwrap();
        let repository = SqliteLegacyImportRepository::open(&database).unwrap();
        let work_items = SqliteWorkItemRepository::open(&database).unwrap();
        Fixture {
            dir,
            repository,
            work_items,
            project_id,
            other_project_id,
        }
    }

    fn attribution(fixture: &Fixture) -> LegacyImportAttribution {
        LegacyImportAttribution {
            organization_id: OrganizationId::new("local"),
            project_id: fixture.project_id.clone(),
        }
    }

    fn write_store(fixture: &Fixture, name: &str, contents: &str) {
        fs::write(fixture.dir.path().join(name), contents).unwrap();
    }

    fn assignments_file(entries: &[&str]) -> String {
        format!("{{\"{ASSIGNMENTS_STORE_KEY}\": [{}]}}", entries.join(",\n"))
    }

    fn todos_file(session_id: &str, entries: &[&str]) -> String {
        format!(
            "{{\"{TODOS_STORE_KEY_PREFIX}{session_id}\": [{}]}}",
            entries.join(",\n")
        )
    }

    fn mapping_row_count(fixture: &Fixture) -> usize {
        let connection = fixture.repository.lock().unwrap();
        connection
            .query_row(
                "SELECT COUNT(*) FROM control_plane_legacy_import_mappings",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap() as usize
    }

    fn imported_work_item_ids(fixture: &Fixture) -> Vec<String> {
        let connection = fixture.repository.lock().unwrap();
        let mut statement = connection
            .prepare("SELECT id FROM control_plane_work_items ORDER BY id")
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap();
        rows.map(|row| row.unwrap()).collect()
    }

    fn seed_standard_stores(fixture: &Fixture) {
        write_store(
            fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[ASSIGNMENT_A, ASSIGNMENT_B_ISSUE_SOURCE]),
        );
        write_store(
            fixture,
            TODOS_STORE_FILE,
            &todos_file("sess_board", &[MANUAL_TODO]),
        );
    }

    fn assignment_legacy_key(id: &str) -> String {
        framed_legacy_key("assignment", &[id])
    }

    fn todo_legacy_key(session_id: &str, id: &str) -> String {
        framed_legacy_key("todo", &[session_id, id])
    }

    fn assignment_work_item_id(id: &str) -> WorkItemId {
        WorkItemId::new(assignment_legacy_key(id))
    }

    fn manual_todo_work_item_id(session_id: &str, id: &str) -> WorkItemId {
        WorkItemId::new(todo_legacy_key(session_id, id))
    }

    #[test]
    fn reimport_is_a_full_no_op() {
        let fixture = fixture();
        seed_standard_stores(&fixture);
        let attr = attribution(&fixture);

        let first = fixture
            .repository
            .import(fixture.dir.path(), Some(attr.clone()))
            .unwrap();
        assert_eq!(first.assignments_total, 2);
        assert_eq!(first.assignments_imported, 2);
        assert_eq!(first.todos_imported_manual_only, 1);
        assert_eq!(first.external_objects_skipped, 1);
        assert_eq!(mapping_row_count(&fixture), 3);
        assert_eq!(imported_work_item_ids(&fixture).len(), 3);

        let second = fixture
            .repository
            .import(fixture.dir.path(), Some(attr))
            .unwrap();
        assert_eq!(second.assignments_imported, 0);
        assert_eq!(second.assignments_unchanged, 2);
        assert_eq!(second.todos_imported_manual_only, 0);
        assert_eq!(second.todos_skipped_agent_plan, 0);
        // Nothing duplicated: one mapping row and one WorkItem per record.
        assert_eq!(mapping_row_count(&fixture), 3);
        assert_eq!(imported_work_item_ids(&fixture).len(), 3);
    }

    #[test]
    fn changed_assignment_content_updates_exactly_one_row() {
        let fixture = fixture();
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[ASSIGNMENT_A]),
        );
        write_store(&fixture, TODOS_STORE_FILE, "{}");
        let attr = attribution(&fixture);

        fixture
            .repository
            .import(fixture.dir.path(), Some(attr.clone()))
            .unwrap();

        let renamed = r#"{
            "id": "asg_a",
            "source": {"kind": "task", "prompt": "Ship the importer"},
            "sessionId": "sess_1",
            "title": "Renamed by user edit",
            "status": "done",
            "createdAt": 1700000000000,
            "updatedAt": 1700000099999
        }"#;
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[renamed]),
        );

        let second = fixture
            .repository
            .import(fixture.dir.path(), Some(attr.clone()))
            .unwrap();
        assert_eq!(second.assignments_updated, 1);
        assert_eq!(second.assignments_imported, 0);

        let stored = fixture
            .work_items
            .get(&assignment_work_item_id("asg_a"))
            .unwrap();
        assert_eq!(stored.title, "Renamed by user edit");

        // The refreshed hash makes the third run a no-op again.
        let third = fixture
            .repository
            .import(fixture.dir.path(), Some(attr))
            .unwrap();
        assert_eq!(third.assignments_unchanged, 1);
        assert_eq!(third.assignments_updated, 0);
        assert_eq!(mapping_row_count(&fixture), 1);
    }

    #[test]
    fn corrupt_entries_are_skipped_and_counted() {
        let fixture = fixture();
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[ASSIGNMENT_A, r#"{"id": 42}"#, ASSIGNMENT_B_ISSUE_SOURCE]),
        );
        write_store(
            &fixture,
            TODOS_STORE_FILE,
            &todos_file("sess_board", &[MANUAL_TODO, r#"{"nonsense": true}"#]),
        );

        let report = fixture
            .repository
            .import(fixture.dir.path(), Some(attribution(&fixture)))
            .unwrap();
        assert_eq!(report.assignments_total, 3);
        assert_eq!(report.assignments_skipped_corrupt, 1);
        assert_eq!(report.assignments_imported, 2);
        assert_eq!(report.todos_total, 2);
        assert_eq!(report.todos_skipped_corrupt, 1);
        assert_eq!(report.todos_imported_manual_only, 1);
        assert_eq!(mapping_row_count(&fixture), 3);
    }

    #[test]
    fn agent_plan_and_originless_todos_never_become_work_items() {
        let fixture = fixture();
        let agent_plan =
            r#"{"id": "t_plan", "title": "Plan step", "status": "in_progress", "origin": "agent"}"#;
        let origin_less =
            r#"{"id": "t_runtime", "title": "Runtime leftover", "status": "completed"}"#;
        write_store(&fixture, ASSIGNMENTS_STORE_FILE, "{\"assignments\": []}");
        write_store(
            &fixture,
            TODOS_STORE_FILE,
            &todos_file("sess_x", &[agent_plan, origin_less, MANUAL_TODO]),
        );

        let report = fixture
            .repository
            .import(fixture.dir.path(), Some(attribution(&fixture)))
            .unwrap();
        assert_eq!(report.todos_total, 3);
        assert_eq!(report.todos_skipped_agent_plan, 2);
        assert_eq!(report.todos_imported_manual_only, 1);

        let ids = imported_work_item_ids(&fixture);
        assert_eq!(ids.len(), 1);
        let imported = manual_todo_work_item_id("sess_x", "t_1");
        assert_eq!(ids[0], imported.value);
    }

    #[test]
    fn manual_todo_is_stamped_at_import_time_and_classified() {
        let fixture = fixture();
        let pending = MANUAL_TODO;
        let completed = r#"{"id": "t_2", "title": "Finished board request", "status": "completed", "origin": "manual"}"#;
        write_store(&fixture, ASSIGNMENTS_STORE_FILE, "{\"assignments\": []}");
        write_store(
            &fixture,
            TODOS_STORE_FILE,
            &todos_file("sess_board", &[pending, completed]),
        );
        fixture
            .repository
            .import(fixture.dir.path(), Some(attribution(&fixture)))
            .unwrap();

        let pending_item = fixture
            .work_items
            .get(&manual_todo_work_item_id("sess_board", "t_1"))
            .unwrap();
        // Import-time stamps: neither carries the preserved-legacy marker, and
        // a fresh import stamps created/updated identically.
        assert!(!pending_item.created_at.starts_with("legacy-ms:"));
        assert_eq!(pending_item.created_at, pending_item.updated_at);
        assert_eq!(pending_item.status, WorkStatus::Backlog);

        let completed_item = fixture
            .work_items
            .get(&manual_todo_work_item_id("sess_board", "t_2"))
            .unwrap();
        // Terminal legacy statuses classify as done; the legacy status itself
        // stays verbatim inside the provenance payload.
        assert_eq!(completed_item.status, WorkStatus::Done);
        assert_eq!(completed_item.execution_phase, ExecutionPhase::Terminal);
        let provenance: serde_json::Value =
            serde_json::from_str(&completed_item.description).unwrap();
        assert_eq!(provenance["legacy_record"]["status"], "completed");
    }

    #[test]
    fn attribution_and_provenance_flow_into_created_work_items() {
        let fixture = fixture();
        seed_standard_stores(&fixture);
        fixture
            .repository
            .import(fixture.dir.path(), Some(attribution(&fixture)))
            .unwrap();

        let item = fixture
            .work_items
            .get_in_project(&fixture.project_id, &assignment_work_item_id("asg_a"))
            .unwrap();
        assert_eq!(item.status, WorkStatus::Done);
        // Assignment epoch-ms timestamps are preserved verbatim.
        assert_eq!(item.created_at, "legacy-ms:1700000000000");
        assert_eq!(item.updated_at, "legacy-ms:1700000001000");
        let provenance: serde_json::Value = serde_json::from_str(&item.description).unwrap();
        assert_eq!(provenance["legacy_key"], assignment_legacy_key("asg_a"));
        assert_eq!(provenance["legacy_record"]["status"], "done");
        assert_eq!(
            provenance["legacy_record"]["source"]["prompt"],
            "Ship the importer"
        );

        // The issue-sourced record keeps its provider identity in the payload
        // instead of minting an ExternalObject.
        let issue_item = fixture
            .work_items
            .get(&assignment_work_item_id("asg_b"))
            .unwrap();
        let issue_provenance: serde_json::Value =
            serde_json::from_str(&issue_item.description).unwrap();
        assert_eq!(issue_provenance["legacy_record"]["source"]["kind"], "issue");
        assert_eq!(issue_provenance["legacy_record"]["source"]["number"], 101);

        // Foreign-project lookups stay refused.
        assert!(matches!(
            fixture
                .work_items
                .get_in_project(&fixture.other_project_id, &assignment_work_item_id("asg_a")),
            Err(WorkItemRepositoryError::ProjectMismatch { .. })
        ));
    }

    #[test]
    fn missing_store_files_and_corrupt_top_level_are_invalid_store() {
        let fixture = fixture();
        // Neither store file exists.
        assert!(matches!(
            fixture
                .repository
                .import(fixture.dir.path(), Some(attribution(&fixture))),
            Err(LegacyImportError::InvalidStore { .. })
        ));

        // Assignments present but todos missing.
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[ASSIGNMENT_A]),
        );
        assert!(matches!(
            fixture
                .repository
                .import(fixture.dir.path(), Some(attribution(&fixture))),
            Err(LegacyImportError::InvalidStore { .. })
        ));

        // Corrupt top-level JSON.
        write_store(&fixture, TODOS_STORE_FILE, "not json at all");
        assert!(matches!(
            fixture
                .repository
                .import(fixture.dir.path(), Some(attribution(&fixture))),
            Err(LegacyImportError::InvalidStore { .. })
        ));
    }

    #[test]
    fn missing_attribution_is_refused() {
        let fixture = fixture();
        seed_standard_stores(&fixture);
        assert_eq!(
            fixture
                .repository
                .import(fixture.dir.path(), None)
                .unwrap_err(),
            LegacyImportError::AttributionRequired
        );
        assert_eq!(mapping_row_count(&fixture), 0);
    }

    #[test]
    fn mapping_rows_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let scopes = SqliteScopeRepository::open(&database).unwrap();
        scopes.ensure_default_local_organization().unwrap();
        let project_id = ProjectId::new("proj_legacy_import");
        scopes
            .create_project(Project {
                id: project_id.clone(),
                organization_id: OrganizationId::new("local"),
                goal_ids: vec![],
                name: "Legacy import".into(),
                description: String::new(),
                status: ProjectStatus::Active,
                revision: Revision::INITIAL,
                created_at: "now".into(),
                updated_at: "now".into(),
            })
            .unwrap();
        let store_dir = dir.path().to_path_buf();
        let store_path = store_dir.clone();
        let write = |contents: &str| {
            fs::write(store_path.join(ASSIGNMENTS_STORE_FILE), contents).unwrap();
            fs::write(store_path.join(TODOS_STORE_FILE), "{\"todos:x\": []}").unwrap();
        };
        write(&assignments_file(&[ASSIGNMENT_A]));

        let attr = LegacyImportAttribution {
            organization_id: OrganizationId::new("local"),
            project_id: project_id.clone(),
        };
        {
            let repository = SqliteLegacyImportRepository::open(&database).unwrap();
            let first = repository.import(&store_dir, Some(attr.clone())).unwrap();
            assert_eq!(first.assignments_imported, 1);
        }
        // A fresh instance over the same database sees the persisted mappings.
        let reopened = SqliteLegacyImportRepository::open(&database).unwrap();
        let second = reopened.import(&store_dir, Some(attr)).unwrap();
        assert_eq!(second.assignments_imported, 0);
        assert_eq!(second.assignments_unchanged, 1);
    }

    #[test]
    fn revision_conflict_fails_closed_naming_the_key() {
        let fixture = fixture();
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[ASSIGNMENT_A]),
        );
        write_store(&fixture, TODOS_STORE_FILE, "{}");
        let attr = attribution(&fixture);
        fixture
            .repository
            .import(fixture.dir.path(), Some(attr.clone()))
            .unwrap();

        // Simulate the concurrent-writer window the optimistic update guards:
        // point the mapping row at a WorkItem outside the attributed project,
        // so the guarded replace must refuse instead of overwriting.
        {
            let connection = fixture.repository.lock().unwrap();
            connection
                .execute(
                    "UPDATE control_plane_legacy_import_mappings
                     SET canonical_work_item_id = 'wi_foreign'
                     WHERE legacy_key = ?1",
                    [&assignment_legacy_key("asg_a")],
                )
                .unwrap();
        }
        let foreign_project = ProjectId::new("proj_other");
        let foreign = WorkItem {
            id: WorkItemId::new("foreign"),
            project_id: foreign_project,
            goal_id: None,
            parent_work_item_id: None,
            kind: altai_control_protocol::WorkItemKind::Task,
            title: "Foreign row".into(),
            description: String::new(),
            status: WorkStatus::Backlog,
            execution_phase: ExecutionPhase::None,
            revision: Revision::INITIAL,
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        fixture.work_items.create(foreign).unwrap();

        let renamed = r#"{
            "id": "asg_a",
            "source": {"kind": "task", "prompt": "Ship the importer"},
            "sessionId": "sess_1",
            "title": "Changed underneath",
            "status": "done",
            "createdAt": 1700000000000,
            "updatedAt": 1700000055555
        }"#;
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&[renamed]),
        );

        match fixture.repository.import(fixture.dir.path(), Some(attr)) {
            Err(LegacyImportError::RevisionConflict { legacy_key }) => {
                assert_eq!(legacy_key, assignment_legacy_key("asg_a"));
            }
            other => panic!("expected a revision conflict, got {other:?}"),
        }
    }

    #[test]
    fn framed_keys_keep_different_composite_splits_apart() {
        // ("a:b", "c") and ("a", "b:c") share the same raw characters; the
        // length-framed scheme must give them distinct keys and WorkItems.
        let fixture = fixture();
        let todo = |id: &str| {
            format!(
                r#"{{"id": "{id}", "title": "Split probe", "status": "pending", "origin": "manual"}}"#
            )
        };
        write_store(&fixture, ASSIGNMENTS_STORE_FILE, "{\"assignments\": []}");
        write_store(
            &fixture,
            TODOS_STORE_FILE,
            &format!(
                "{{\"todos:a:b\": [{}], \"todos:a\": [{}]}}",
                todo("c"),
                todo("b:c")
            ),
        );

        let report = fixture
            .repository
            .import(fixture.dir.path(), Some(attribution(&fixture)))
            .unwrap();
        assert_eq!(report.todos_imported_manual_only, 2);
        assert_eq!(mapping_row_count(&fixture), 2);
        // Both projections exist as separate rows.
        fixture
            .work_items
            .get(&manual_todo_work_item_id("a:b", "c"))
            .unwrap();
        fixture
            .work_items
            .get(&manual_todo_work_item_id("a", "b:c"))
            .unwrap();
        assert_ne!(
            todo_legacy_key("a:b", "c"),
            todo_legacy_key("a", "b:c"),
            "different splits of the same characters must not collide"
        );
    }

    #[test]
    fn oversized_files_and_entry_arrays_are_invalid_stores() {
        let fixture = fixture();
        // Byte cap: one byte over MAX_LEGACY_JSON_BYTES is refused before any
        // read, regardless of which key holds the bulk.
        let padding = "x".repeat(MAX_LEGACY_JSON_BYTES as usize + 1);
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &format!("{{\"{ASSIGNMENTS_STORE_KEY}\": [], \"pad\": \"{padding}\"}}"),
        );
        write_store(&fixture, TODOS_STORE_FILE, "{}");
        assert!(matches!(
            fixture
                .repository
                .import(fixture.dir.path(), Some(attribution(&fixture))),
            Err(LegacyImportError::InvalidStore { .. })
        ));

        // Entry cap: 2001 assignments in one array exceed the per-array limit.
        let over_cap: Vec<&str> = vec![r#"{"junk": true}"#; MAX_JSON_SOURCE_ENTRIES + 1];
        write_store(
            &fixture,
            ASSIGNMENTS_STORE_FILE,
            &assignments_file(&over_cap),
        );
        assert!(matches!(
            fixture
                .repository
                .import(fixture.dir.path(), Some(attribution(&fixture))),
            Err(LegacyImportError::InvalidStore { .. })
        ));
    }

    #[test]
    fn crash_recovery_converges_or_fails_closed() {
        let record_json = ASSIGNMENT_A;
        let build_candidate = |fixture: &Fixture| {
            let record: LegacyAssignment = serde_json::from_str(record_json).unwrap();
            let projection = project_assignment(&record).unwrap();
            assembled_work_item(
                &attribution(fixture),
                &projection,
                "legacy-ms:1700000000000".into(),
                "legacy-ms:1700000001000".into(),
            )
        };

        // Byte-identical row without a mapping entry (crash after create):
        // the next import converges by recording the mapping, counted Imported.
        {
            let fixture = fixture();
            write_store(
                &fixture,
                ASSIGNMENTS_STORE_FILE,
                &assignments_file(&[record_json]),
            );
            write_store(&fixture, TODOS_STORE_FILE, "{}");
            fixture
                .work_items
                .create(build_candidate(&fixture))
                .unwrap();
            let recovered = fixture
                .repository
                .import(fixture.dir.path(), Some(attribution(&fixture)))
                .unwrap();
            assert_eq!(recovered.assignments_imported, 1);
            assert_eq!(mapping_row_count(&fixture), 1);
        }

        // Same crash window but with drifted description content: refuse.
        {
            let fixture = fixture();
            write_store(
                &fixture,
                ASSIGNMENTS_STORE_FILE,
                &assignments_file(&[record_json]),
            );
            write_store(&fixture, TODOS_STORE_FILE, "{}");
            let mut drifted = build_candidate(&fixture);
            drifted.description = "{}".into();
            fixture.work_items.create(drifted).unwrap();
            assert!(matches!(
                fixture
                    .repository
                    .import(fixture.dir.path(), Some(attribution(&fixture))),
                Err(LegacyImportError::RevisionConflict { .. })
            ));
        }

        // Same crash window but attributed to a foreign project: refuse.
        {
            let fixture = fixture();
            write_store(
                &fixture,
                ASSIGNMENTS_STORE_FILE,
                &assignments_file(&[record_json]),
            );
            write_store(&fixture, TODOS_STORE_FILE, "{}");
            let mut foreign = build_candidate(&fixture);
            foreign.project_id = fixture.other_project_id.clone();
            fixture.work_items.create(foreign).unwrap();
            assert!(matches!(
                fixture
                    .repository
                    .import(fixture.dir.path(), Some(attribution(&fixture))),
                Err(LegacyImportError::RevisionConflict { .. })
            ));
        }
    }
}
