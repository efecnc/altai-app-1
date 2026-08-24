//! Work OS Milestone 1 — durable Work IPC over the existing host.
//!
//! Wraps `altai_core::WorkStore` at `WorkspacePaths::work_db()`. Authoritative
//! lifecycle lives here; IsanAgent remains execution-only (ADR 0003).

use std::path::Path;

use std::collections::HashMap;

use altai_core::{
    journal::{ChatUsageTotals, EventJournal},
    resolve_workspace_from, AgentRecord, AgentStatus, AttemptPhase, AttemptReconcileMode,
    AttemptRecord, CreateWorkInput, RecentAttemptRecord, RecentEventRecord, WorkAttemptStart,
    WorkEventRecord, WorkInboxRecord, WorkItemKind, WorkItemRecord, WorkListFilter, WorkState,
    WorkStore,
};
use serde::{Deserialize, Serialize};
use tauri::State;

use crate::altai::agent::runtime::{self, AgentRuntime};
use crate::modules::workspace::WorkspaceRegistry;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkItemDto {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub description: String,
    pub acceptance_criteria: String,
    pub kind: String,
    pub parent_work_id: Option<String>,
    pub state: String,
    pub assignee_ref: Option<String>,
    pub blocker: Option<String>,
    pub revision: i64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl From<WorkItemRecord> for WorkItemDto {
    fn from(value: WorkItemRecord) -> Self {
        Self {
            id: value.id,
            project_id: value.project_id,
            title: value.title,
            description: value.description,
            acceptance_criteria: value.acceptance_criteria,
            kind: value.kind.as_str().to_string(),
            parent_work_id: value.parent_work_id,
            state: value.state.as_str().to_string(),
            assignee_ref: value.assignee_ref,
            blocker: value.blocker,
            revision: value.revision,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkAttemptDto {
    pub id: String,
    pub work_id: String,
    pub number: i64,
    pub role: String,
    pub phase: String,
    pub chat_id: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub input_json: Option<String>,
    pub result_json: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl From<AttemptRecord> for WorkAttemptDto {
    fn from(value: AttemptRecord) -> Self {
        Self {
            id: value.id,
            work_id: value.work_id,
            number: value.number,
            role: value.role,
            phase: value.phase.as_str().to_string(),
            chat_id: value.chat_id,
            session_id: value.session_id,
            run_id: value.run_id,
            input_json: value.input_json,
            result_json: value.result_json,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkStartResultDto {
    pub work: WorkItemDto,
    pub attempt: WorkAttemptDto,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkReconcileResultDto {
    pub changed_work_ids: Vec<String>,
}

impl From<WorkAttemptStart> for WorkStartResultDto {
    fn from(value: WorkAttemptStart) -> Self {
        Self {
            work: value.work.into(),
            attempt: value.attempt.into(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkInboxItemDto {
    pub id: String,
    pub work_id: String,
    pub kind: String,
    pub title: String,
    pub why: String,
    pub created_at_ms: u64,
    pub attempt_id: Option<String>,
    pub chat_id: Option<String>,
    pub run_id: Option<String>,
}

impl From<WorkInboxRecord> for WorkInboxItemDto {
    fn from(value: WorkInboxRecord) -> Self {
        Self {
            id: value.id,
            work_id: value.work_id,
            kind: value.kind.as_str().to_string(),
            title: value.title,
            why: value.why,
            created_at_ms: value.created_at_ms,
            attempt_id: value.attempt_id,
            chat_id: value.chat_id,
            run_id: value.run_id,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCreateArgs {
    pub workspace_path: String,
    pub title: String,
    pub description: Option<String>,
    pub acceptance_criteria: Option<String>,
    pub assignee_ref: Option<String>,
    pub kind: Option<String>,
    pub parent_work_id: Option<String>,
}

pub(crate) fn authorized_workspace(
    workspace_path: &str,
    registry: &WorkspaceRegistry,
) -> Result<String, String> {
    let raw = workspace_path.trim();
    if raw.is_empty() {
        return Err("workspacePath is required".into());
    }
    let canonical = registry
        .canonicalize_cached(raw)
        .map_err(|error| format!("Workspace is not accessible: {error}"))?;
    if !canonical.is_dir() || !registry.is_authorized(&canonical) {
        return Err("Workspace is not authorized.".into());
    }
    Ok(canonical.to_string_lossy().replace('\\', "/"))
}

pub(crate) fn open_store(
    registry: &WorkspaceRegistry,
    workspace_path: &str,
) -> Result<(String, std::sync::Arc<WorkStore>), String> {
    let paths = resolve_workspace_from(Some(Path::new(workspace_path)), Path::new(workspace_path))
        .map_err(|error| error.to_string())?;
    let project_id = paths
        .root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workspace")
        .to_string();
    // The registry caches one store per work.db, so every command shares the
    // single-writer lock for the app run instead of colliding with its own
    // concurrent commands; the cache also runs the migration lifecycle, so
    // core and control-plane schema owners come up (or are refused) together.
    let store = registry.work_store(&paths.work_db())?;
    store
        .ensure_project(&project_id, &project_id, &paths.root.to_string_lossy())
        .map_err(|error| error.to_string())?;
    Ok((project_id, store))
}

/// The workspace's `work.db` path, for read-side companions that share
/// the store's file (the routine aggregates the control plane owns).
/// Resolve only — callers that also need the store go through
/// [`open_store`], which owns the migration lifecycle.
pub(crate) fn resolve_work_db(workspace: &str) -> Result<std::path::PathBuf, String> {
    let paths = resolve_workspace_from(Some(Path::new(workspace)), Path::new(workspace))
        .map_err(|error| error.to_string())?;
    Ok(paths.work_db())
}

fn parse_filter(raw: Option<&str>) -> Result<WorkListFilter, String> {
    match raw.unwrap_or("my_active") {
        "my_active" | "my-active" | "active" => Ok(WorkListFilter::MyActive),
        "review" => Ok(WorkListFilter::Review),
        "backlog" => Ok(WorkListFilter::Backlog),
        "done" => Ok(WorkListFilter::Done),
        other => Err(format!("unknown work filter: {other}")),
    }
}

fn parse_state(raw: &str) -> Result<WorkState, String> {
    WorkState::parse(raw).ok_or_else(|| format!("unknown work state: {raw}"))
}

fn parse_terminal_phase(raw: &str) -> Result<AttemptPhase, String> {
    match raw.trim() {
        "succeeded" => Ok(AttemptPhase::Succeeded),
        "failed" => Ok(AttemptPhase::Failed),
        "cancelled" => Ok(AttemptPhase::Cancelled),
        other => Err(format!("unknown terminal attempt phase: {other}")),
    }
}

#[tauri::command]
pub fn work_create(
    registry: State<'_, WorkspaceRegistry>,
    args: WorkCreateArgs,
) -> Result<WorkItemDto, String> {
    let workspace = authorized_workspace(&args.workspace_path, &registry)?;
    let (project_id, store) = open_store(&registry, &workspace)?;
    let kind = args
        .kind
        .as_deref()
        .map(WorkItemKind::parse)
        .unwrap_or(Some(WorkItemKind::Task))
        .ok_or_else(|| "kind must be task, ticket, or campaign".to_string())?;
    let created = store
        .create_work_item(
            CreateWorkInput {
                project_id,
                title: args.title,
                description: args.description.unwrap_or_default(),
                acceptance_criteria: args.acceptance_criteria.unwrap_or_default(),
                assignee_ref: args.assignee_ref,
            },
            kind,
            args.parent_work_id,
        )
        .map_err(|error| error.to_string())?;
    Ok(created.into())
}

#[tauri::command]
pub fn work_list(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    filter: Option<String>,
) -> Result<Vec<WorkItemDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (project_id, store) = open_store(&registry, &workspace)?;
    let listed = store
        .list_work(&project_id, parse_filter(filter.as_deref())?)
        .map_err(|error| error.to_string())?;
    Ok(listed.into_iter().map(WorkItemDto::from).collect())
}

#[tauri::command]
pub fn work_children(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    parent_work_id: String,
) -> Result<Vec<WorkItemDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let children = store
        .list_child_work(&parent_work_id)
        .map_err(|error| error.to_string())?;
    Ok(children.into_iter().map(WorkItemDto::from).collect())
}

#[tauri::command]
pub fn work_inbox_list(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
) -> Result<Vec<WorkInboxItemDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (project_id, store) = open_store(&registry, &workspace)?;
    let listed = store
        .list_work_inbox(&project_id)
        .map_err(|error| error.to_string())?;
    Ok(listed.into_iter().map(WorkInboxItemDto::from).collect())
}

#[tauri::command]
pub fn work_get(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
) -> Result<Option<WorkItemDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    Ok(store
        .get_work(work_id.trim())
        .map_err(|error| error.to_string())?
        .map(WorkItemDto::from))
}

#[tauri::command]
pub fn work_transition(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
    expected_revision: i64,
    next_state: String,
) -> Result<WorkItemDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let updated = store
        .transition(
            work_id.trim(),
            expected_revision,
            parse_state(next_state.trim())?,
        )
        .map_err(|error| error.to_string())?;
    Ok(updated.into())
}

#[tauri::command]
pub fn work_start(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
    expected_revision: i64,
) -> Result<WorkItemDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let updated = store
        .start_attempt(work_id.trim(), expected_revision)
        .map_err(|error| error.to_string())?;
    Ok(updated.into())
}

async fn reconcile_store<'a>(
    agent_runtime: &'a AgentRuntime,
    workspace: &str,
    store: &WorkStore,
) -> Result<(Vec<String>, runtime::WorkRecoveryLease<'a>), String> {
    let lease = runtime::work_recovery_lease(agent_runtime, workspace).await?;
    let mode = if lease.first_recovery_pass {
        AttemptReconcileMode::RestartRecovery
    } else {
        AttemptReconcileMode::Live
    };
    let changed_work_ids = store
        .reconcile_attempts_from_journal(&lease.event_journal, mode)
        .map_err(|error| error.to_string())?;
    Ok((changed_work_ids, lease))
}

#[tauri::command]
pub async fn work_start_attempt(
    agent_runtime: State<'_, AgentRuntime>,
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
    expected_revision: i64,
    chat_id: String,
    session_id: Option<String>,
) -> Result<WorkStartResultDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    // A direct Start may be the first Work command in this host process. Run
    // the one restart-recovery pass before creating current-process state so
    // that the new cold dispatch can never be mistaken for an inherited orphan.
    let (_changed_work_ids, recovery_lease) =
        reconcile_store(&agent_runtime, &workspace, &store).await?;
    let chat_id = chat_id.trim();
    if chat_id.is_empty() {
        return Err("chatId is required before starting a Desktop Attempt".into());
    }
    let started = store
        .start_attempt_with_dispatch(
            work_id.trim(),
            expected_revision,
            Some(chat_id),
            session_id.as_deref(),
        )
        .map_err(|error| error.to_string())?;
    let result = started.into();
    recovery_lease.commit();
    Ok(result)
}

#[tauri::command]
pub async fn work_attempt_reconcile(
    agent_runtime: State<'_, AgentRuntime>,
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
) -> Result<WorkReconcileResultDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let (changed_work_ids, recovery_lease) =
        reconcile_store(&agent_runtime, &workspace, &store).await?;
    recovery_lease.commit();
    Ok(WorkReconcileResultDto { changed_work_ids })
}

#[tauri::command]
pub fn work_attempts(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
) -> Result<Vec<WorkAttemptDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let attempts = store
        .list_attempts(work_id.trim())
        .map_err(|error| error.to_string())?;
    Ok(attempts.into_iter().map(WorkAttemptDto::from).collect())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkEventDto {
    pub id: i64,
    pub work_id: String,
    pub kind: String,
    pub payload_json: String,
    pub created_at_ms: u64,
}

impl From<WorkEventRecord> for WorkEventDto {
    fn from(value: WorkEventRecord) -> Self {
        Self {
            id: value.id,
            work_id: value.work_id,
            kind: value.kind,
            payload_json: value.payload_json,
            created_at_ms: value.created_at_ms,
        }
    }
}

#[tauri::command]
pub fn work_events(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
) -> Result<Vec<WorkEventDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let events = store
        .list_work_events(work_id.trim())
        .map_err(|error| error.to_string())?;
    Ok(events.into_iter().map(WorkEventDto::from).collect())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkRunDto {
    pub id: String,
    pub work_id: String,
    pub work_title: String,
    pub work_state: String,
    pub number: i64,
    pub role: String,
    pub phase: String,
    pub chat_id: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl From<RecentAttemptRecord> for WorkRunDto {
    fn from(value: RecentAttemptRecord) -> Self {
        Self {
            id: value.id,
            work_id: value.work_id,
            work_title: value.work_title,
            work_state: value.work_state.as_str().to_string(),
            number: value.number,
            role: value.role,
            phase: value.phase.as_str().to_string(),
            chat_id: value.chat_id,
            session_id: value.session_id,
            run_id: value.run_id,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
        }
    }
}

/// Upper bound on runs returned per `work_runs` call.
const WORK_RUNS_LIMIT_MAX: u32 = 100;
const WORK_RUNS_LIMIT_DEFAULT: u32 = 20;

#[tauri::command]
pub fn work_runs(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    limit: Option<u32>,
) -> Result<Vec<WorkRunDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let bounded = limit.unwrap_or(WORK_RUNS_LIMIT_DEFAULT).min(WORK_RUNS_LIMIT_MAX);
    let runs = store
        .list_recent_attempts(bounded)
        .map_err(|error| error.to_string())?;
    Ok(runs.into_iter().map(WorkRunDto::from).collect())
}

#[tauri::command]
pub fn work_attempt_bind(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    attempt_id: String,
    chat_id: String,
    session_id: Option<String>,
    run_id: String,
) -> Result<WorkAttemptDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let attempt = store
        .bind_attempt_run(
            attempt_id.trim(),
            chat_id.trim(),
            session_id.as_deref(),
            run_id.trim(),
        )
        .map_err(|error| error.to_string())?;
    Ok(attempt.into())
}

#[tauri::command]
pub fn work_attempt_finish(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    attempt_id: Option<String>,
    run_id: Option<String>,
    phase: String,
    result_json: Option<String>,
) -> Result<Option<WorkItemDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let phase = parse_terminal_phase(&phase)?;
    let result_json = result_json.as_deref().unwrap_or("{}");
    let updated = match (
        attempt_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty()),
        run_id.as_deref().map(str::trim).filter(|id| !id.is_empty()),
    ) {
        (Some(id), None) => Some(
            store
                .finish_attempt_by_id(id, phase, result_json)
                .map_err(|error| error.to_string())?,
        ),
        (None, Some(id)) => store
            .finish_attempt_by_run(id, phase, result_json)
            .map_err(|error| error.to_string())?,
        _ => return Err("exactly one attemptId or runId is required".into()),
    };
    Ok(updated.map(WorkItemDto::from))
}

#[tauri::command]
pub fn work_ready_for_review(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
    expected_revision: i64,
) -> Result<WorkItemDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let updated = store
        .mark_attempt_ready_for_review(work_id.trim(), expected_revision)
        .map_err(|error| error.to_string())?;
    Ok(updated.into())
}

#[tauri::command]
pub fn work_review(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    work_id: String,
    expected_revision: i64,
    accept: bool,
    guidance: Option<String>,
) -> Result<WorkItemDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let updated = store
        .human_review(
            work_id.trim(),
            expected_revision,
            accept,
            guidance.as_deref().unwrap_or(""),
        )
        .map_err(|error| error.to_string())?;
    Ok(updated.into())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentDto {
    pub id: String,
    pub name: String,
    pub status: String,
    pub reports_to: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl From<AgentRecord> for AgentDto {
    fn from(value: AgentRecord) -> Self {
        Self {
            id: value.id,
            name: value.name,
            status: value.status.as_str().to_string(),
            reports_to: value.reports_to,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEventDto {
    pub id: i64,
    pub work_id: String,
    pub work_title: String,
    pub kind: String,
    pub payload_json: String,
    pub created_at_ms: u64,
}

impl From<RecentEventRecord> for AuditEventDto {
    fn from(value: RecentEventRecord) -> Self {
        Self {
            id: value.id,
            work_id: value.work_id,
            work_title: value.work_title,
            kind: value.kind,
            payload_json: value.payload_json,
            created_at_ms: value.created_at_ms,
        }
    }
}

/// Bounds on audit events returned per `work_events_recent` call.
const WORK_AUDIT_LIMIT_MAX: u32 = 100;
const WORK_AUDIT_LIMIT_DEFAULT: u32 = 50;

#[tauri::command]
pub fn work_events_recent(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    limit: Option<u32>,
) -> Result<Vec<AuditEventDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let bounded = limit.unwrap_or(WORK_AUDIT_LIMIT_DEFAULT).min(WORK_AUDIT_LIMIT_MAX);
    let events = store
        .list_recent_events(bounded)
        .map_err(|error| error.to_string())?;
    Ok(events.into_iter().map(AuditEventDto::from).collect())
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkUsageDto {
    pub attempt_id: String,
    pub work_id: String,
    pub work_title: String,
    pub number: i64,
    pub phase: String,
    pub chat_id: Option<String>,
    pub run_id: Option<String>,
    pub updated_at_ms: u64,
    /// Token usage attributed through the attempt's chat binding. `None`
    /// when the attempt never bound to a chat — there is nothing to
    /// attribute, which is a different fact from zero usage.
    pub usage: Option<WorkUsageTotalsDto>,
}

#[derive(Debug, Serialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkUsageTotalsDto {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub event_count: u64,
}

impl From<&ChatUsageTotals> for WorkUsageTotalsDto {
    fn from(value: &ChatUsageTotals) -> Self {
        Self {
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
            cache_read_tokens: value.cache_read_tokens,
            cache_creation_tokens: value.cache_creation_tokens,
            event_count: value.event_count,
        }
    }
}

/// Attribute journaled usage to the attempts that carry the chat
/// binding. Unbound attempts stay `usage: None`; a bound chat with no
/// recorded usage is zero, not missing.
fn attribute_usage(
    attempts: Vec<RecentAttemptRecord>,
    totals: HashMap<String, ChatUsageTotals>,
) -> Vec<WorkUsageDto> {
    attempts
        .into_iter()
        .map(|attempt| {
            let usage = attempt
                .chat_id
                .as_ref()
                .and_then(|chat_id| totals.get(chat_id))
                .map(WorkUsageTotalsDto::from)
                .or_else(|| {
                    attempt
                        .chat_id
                        .as_ref()
                        .map(|_| WorkUsageTotalsDto::default())
                });
            WorkUsageDto {
                attempt_id: attempt.id,
                work_id: attempt.work_id,
                work_title: attempt.work_title,
                number: attempt.number,
                phase: attempt.phase.as_str().to_string(),
                chat_id: attempt.chat_id,
                run_id: attempt.run_id,
                updated_at_ms: attempt.updated_at_ms,
                usage,
            }
        })
        .collect()
}

/// Open the workspace's agent event journal the same way the desktop
/// host does, and roll up usage for the given chats. A workspace whose
/// journal does not exist yet has simply recorded no usage.
fn load_workspace_usage_totals(
    workspace: &str,
    chat_ids: &[String],
) -> Result<HashMap<String, ChatUsageTotals>, String> {
    if chat_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let dir = isanagent::workspace::resolve_workspace_root(Some(workspace));
    let journal_path = dir.join(".system_generated").join("agent_event_journal.db");
    if !journal_path.exists() {
        return Ok(HashMap::new());
    }
    let journal = EventJournal::open(&journal_path).map_err(|error| error.to_string())?;
    let refs: Vec<&str> = chat_ids.iter().map(String::as_str).collect();
    let totals = journal
        .chat_usage_totals(&refs)
        .map_err(|error| error.to_string())?;
    Ok(totals
        .into_iter()
        .map(|record| (record.chat_id.clone(), record))
        .collect())
}

#[tauri::command]
pub fn work_usage_recent(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    limit: Option<u32>,
) -> Result<Vec<WorkUsageDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let bounded = limit.unwrap_or(WORK_RUNS_LIMIT_DEFAULT).min(WORK_RUNS_LIMIT_MAX);
    let attempts = store
        .list_recent_attempts(bounded)
        .map_err(|error| error.to_string())?;
    let chat_ids: Vec<String> = attempts.iter().filter_map(|a| a.chat_id.clone()).collect();
    let totals = load_workspace_usage_totals(&workspace, &chat_ids)?;
    Ok(attribute_usage(attempts, totals))
}

#[tauri::command]
pub fn agent_list(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
) -> Result<Vec<AgentDto>, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let agents = store.list_agents().map_err(|error| error.to_string())?;
    Ok(agents.into_iter().map(AgentDto::from).collect())
}

#[tauri::command]
pub fn agent_create(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    name: String,
    reports_to: Option<String>,
) -> Result<AgentDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let agent = store
        .create_agent(name.trim(), reports_to.as_deref())
        .map_err(|error| error.to_string())?;
    Ok(agent.into())
}

#[tauri::command]
pub fn agent_transition(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    agent_id: String,
    status: String,
) -> Result<AgentDto, String> {
    let next = AgentStatus::parse(status.trim())
        .ok_or_else(|| format!("unknown agent status: {status}"))?;
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let agent = store
        .transition_agent_status(agent_id.trim(), next)
        .map_err(|error| error.to_string())?;
    Ok(agent.into())
}

#[tauri::command]
pub fn agent_set_reporting(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    agent_id: String,
    reports_to: Option<String>,
) -> Result<AgentDto, String> {
    let workspace = authorized_workspace(&workspace_path, &registry)?;
    let (_project_id, store) = open_store(&registry, &workspace)?;
    let agent = store
        .set_agent_reporting(agent_id.trim(), reports_to.as_deref())
        .map_err(|error| error.to_string())?;
    Ok(agent.into())
}

#[cfg(test)]
mod tests {
    use super::{
        attribute_usage, load_workspace_usage_totals, open_store, WorkInboxItemDto,
        WorkUsageTotalsDto,
    };
    use crate::modules::workspace::WorkspaceRegistry;
    use altai_core::journal::{EventJournal, JournalEvent};
    use altai_core::{
        resolve_workspace_from, AttemptPhase, ChatUsageTotals, RecentAttemptRecord, WorkInboxKind,
        WorkInboxRecord, WorkState,
    };
    use std::collections::HashMap;

    #[test]
    fn open_store_migrates_the_workspace_work_db_and_is_idempotent_per_run() {
        let directory = tempfile::tempdir().unwrap();
        let workspace_root = directory.path().join("project");
        std::fs::create_dir_all(&workspace_root).unwrap();
        let registry = WorkspaceRegistry::default();
        let workspace = workspace_root.to_string_lossy().replace('\\', "/");

        open_store(&registry, &workspace)
            .map(|_| ())
            .expect("first open should migrate and succeed");
        open_store(&registry, &workspace)
            .map(|_| ())
            .expect("second open in the same run should reuse the lifecycle gate");
    }

    #[test]
    fn open_store_fails_closed_on_a_newer_work_db_schema() {
        let directory = tempfile::tempdir().unwrap();
        let workspace_root = directory.path().join("project");
        std::fs::create_dir_all(&workspace_root).unwrap();
        let paths = resolve_workspace_from(Some(&workspace_root), &workspace_root).unwrap();
        let database = paths.work_db();
        if let Some(parent) = database.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        // Seed a lifecycle ledger written by a newer host.
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE control_plane_local_migrations (
                   version INTEGER PRIMARY KEY,
                   applied_at_unix_seconds INTEGER NOT NULL
                 );
                 INSERT INTO control_plane_local_migrations VALUES (99, 0);",
            )
            .unwrap();
        drop(connection);

        let registry = WorkspaceRegistry::default();
        let workspace = workspace_root.to_string_lossy().replace('\\', "/");
        let error = open_store(&registry, &workspace)
            .map(|_| ())
            .expect_err("newer-schema work.db must fail closed");
        assert!(
            error.contains("newer than this build"),
            "unexpected error: {error}"
        );

        // Fail-closed: the refused open must not leave adapter DDL behind —
        // only the seeded ledger table exists.
        let connection = rusqlite::Connection::open(&database).unwrap();
        let adapter_tables: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table'
                   AND name LIKE 'control_plane_%'
                   AND name != 'control_plane_local_migrations'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(adapter_tables, 0);
    }

    fn usage_attempt(id: &str, chat_id: Option<&str>) -> RecentAttemptRecord {
        RecentAttemptRecord {
            id: id.into(),
            work_id: "work_1".into(),
            work_title: "Ship the ledger".into(),
            work_state: WorkState::InProgress,
            number: 1,
            role: "executor".into(),
            phase: AttemptPhase::Succeeded,
            chat_id: chat_id.map(Into::into),
            session_id: None,
            run_id: Some(format!("run_{id}")),
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    fn journal_usage_payload(prompt: u64, completion: u64) -> serde_json::Value {
        serde_json::json!({
            "type": "usage",
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
        })
    }

    #[test]
    fn attribute_usage_maps_bound_chats_and_keeps_unbound_attempts_distinct() {
        let mut totals = HashMap::new();
        totals.insert(
            "chat_with_usage".into(),
            ChatUsageTotals {
                chat_id: "chat_with_usage".into(),
                prompt_tokens: 130,
                completion_tokens: 70,
                total_tokens: 200,
                cache_read_tokens: 11,
                cache_creation_tokens: 7,
                event_count: 2,
            },
        );

        let rows = attribute_usage(
            vec![
                usage_attempt("a1", Some("chat_with_usage")),
                usage_attempt("a2", Some("chat_without_events")),
                usage_attempt("a3", None),
            ],
            totals,
        );

        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].usage,
            Some(WorkUsageTotalsDto {
                prompt_tokens: 130,
                completion_tokens: 70,
                total_tokens: 200,
                cache_read_tokens: 11,
                cache_creation_tokens: 7,
                event_count: 2,
            })
        );
        // A bound chat that journaled no usage is zero, not missing.
        assert_eq!(rows[1].usage, Some(WorkUsageTotalsDto::default()));
        // An attempt that never bound a chat has nothing to attribute.
        assert_eq!(rows[2].usage, None);
        assert_eq!(rows[2].chat_id, None);
        assert_eq!(rows[0].phase, "succeeded");
        assert_eq!(rows[0].work_title, "Ship the ledger");
    }

    #[test]
    fn load_workspace_usage_totals_reads_the_journal_and_tolerates_absence() {
        let directory = tempfile::tempdir().unwrap();
        let workspace_root = directory.path().join("project");
        std::fs::create_dir_all(&workspace_root).unwrap();
        let workspace = workspace_root.to_string_lossy().replace('\\', "/");

        // No journal yet: the workspace simply recorded no usage.
        let empty = load_workspace_usage_totals(&workspace, &["chat_1".to_string()]).unwrap();
        assert!(empty.is_empty());

        // Seed the journal exactly where the desktop host opens it.
        let journal_dir = workspace_root.join(".system_generated");
        std::fs::create_dir_all(&journal_dir).unwrap();
        let journal = EventJournal::open(journal_dir.join("agent_event_journal.db")).unwrap();
        journal
            .append(&JournalEvent::now(
                1,
                "run_1",
                1,
                "chat_1",
                "usage",
                journal_usage_payload(100, 50),
            ))
            .unwrap();
        journal
            .append(&JournalEvent::now(
                1,
                "run_1",
                2,
                "chat_1",
                "message",
                serde_json::json!({"type": "message"}),
            ))
            .unwrap();
        drop(journal);

        let totals = load_workspace_usage_totals(
            &workspace,
            &["chat_1".to_string(), "chat_other".to_string()],
        )
        .unwrap();
        assert_eq!(totals.len(), 1, "only chats with journaled rows appear");
        let chat_1 = &totals["chat_1"];
        assert_eq!(chat_1.prompt_tokens, 100);
        assert_eq!(chat_1.completion_tokens, 50);
        assert_eq!(chat_1.total_tokens, 150);
        assert_eq!(chat_1.event_count, 1);

        // Asking for nothing is a no-op even with a journal present.
        assert!(load_workspace_usage_totals(&workspace, &[])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn work_inbox_dto_is_camel_case_and_keeps_nullable_source_refs() {
        let dto = WorkInboxItemDto::from(WorkInboxRecord {
            id: "review_required:work_1".into(),
            work_id: "work_1".into(),
            kind: WorkInboxKind::ReviewRequired,
            title: "Review Work".into(),
            why: "Attempt finished".into(),
            created_at_ms: 42,
            attempt_id: Some("attempt_1".into()),
            chat_id: None,
            run_id: None,
        });
        let value = serde_json::to_value(dto).expect("serialize Work Inbox DTO");
        assert_eq!(value["workId"], "work_1");
        assert_eq!(value["createdAtMs"], 42);
        assert_eq!(value["attemptId"], "attempt_1");
        assert!(value["chatId"].is_null());
        assert!(value["runId"].is_null());
        assert!(value.get("work_id").is_none());
        assert!(value.get("created_at_ms").is_none());
    }
}
