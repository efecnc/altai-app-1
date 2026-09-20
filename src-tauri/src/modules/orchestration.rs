use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::State;

// O3 wires the runner contract and the coordinator decision core against the
// O1 domain model and the O2 ledger. These modules are exercised by tests; the
// async driver and Tauri commands land in O4.
#[allow(dead_code)]
pub mod coordinator;
// O4 wraps the O3 decision core in a long-lived actor with pause/stop semantics
// and forwards committed events to a sink (Tauri AppHandle in production).
#[allow(dead_code)]
pub mod actor;
#[allow(dead_code)]
pub mod domain;
// O5 reconciles the durable ledger on startup: parks ambiguous tasks in
// NeedsAttention and replays missed terminal reactions (crash safety).
#[allow(dead_code)]
pub mod recovery;
// O2 stages the durable ledger before O4 wires it into the coordinator.
// Keep it compiled and fully tested in this intermediate PR.
#[allow(dead_code)]
pub mod artifact;
#[allow(dead_code)]
pub mod browser_qa;
#[allow(dead_code)]
pub mod budget;
#[allow(dead_code)]
pub mod checks;
#[allow(dead_code)]
pub mod commands;
#[allow(dead_code)]
pub mod config_diff;
#[allow(dead_code)]
pub mod context;
#[allow(dead_code)]
pub mod credentials;
#[allow(dead_code)]
pub mod delivery;
#[allow(dead_code)]
pub mod docker;
#[allow(dead_code)]
pub mod environment;
#[allow(dead_code)]
pub mod eval_lab;
#[allow(dead_code)]
pub mod gardening;
#[allow(dead_code)]
pub mod hooks;
#[allow(dead_code)]
pub mod integration;
#[allow(dead_code)]
pub mod ledger;
#[allow(dead_code)]
pub mod notifications;
#[allow(dead_code)]
pub mod plans;
#[allow(dead_code)]
pub mod policy;
#[allow(dead_code)]
pub mod profiles;
#[allow(dead_code)]
pub mod projections;
#[allow(dead_code)]
pub mod prompt;
#[allow(dead_code)]
pub mod quality;
#[allow(dead_code)]
pub mod readiness;
#[allow(dead_code)]
pub mod routing;
#[allow(dead_code)]
pub mod runners;
#[allow(dead_code)]
pub mod security;
#[allow(dead_code)]
pub mod session_analysis;
#[cfg(test)]
mod soak;
#[allow(dead_code)]
pub mod sources;
#[allow(dead_code)]
pub mod task_graph;
#[allow(dead_code)]
pub mod team;
#[allow(dead_code)]
pub mod usage_wiring;
#[allow(dead_code)]
pub mod worker_pool;
pub mod workflow;
#[allow(dead_code)]
pub mod workflow_v2;

use workflow::WorkflowConfig;

const DEFAULT_MAX_CONCURRENT: usize = 2;
const MAX_CONCURRENT_LIMIT: usize = 8;

#[derive(Default)]
pub struct OrchestrationState(Mutex<HashMap<String, WorkspaceRuntime>>);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum OrchestrationStatus {
    #[default]
    Stopped,
    Running,
    Paused,
}

#[derive(Clone, Debug)]
enum TaskPhase {
    Claiming,
    Running { assignment_id: String },
    Retrying { retry_at_ms: u64 },
    Completed,
}

#[derive(Clone, Debug)]
struct TaskRuntime {
    attempt: u32,
    phase: TaskPhase,
    last_error: Option<String>,
    last_terminal_assignment_id: Option<String>,
}

#[derive(Clone, Debug)]
struct WorkspaceRuntime {
    status: OrchestrationStatus,
    task_session_id: String,
    max_concurrent: usize,
    max_attempts: u32,
    retry_base_ms: u64,
    retry_max_ms: u64,
    started_at_ms: Option<u64>,
    last_tick_ms: Option<u64>,
    active_count: usize,
    tasks: HashMap<String, TaskRuntime>,
}

impl Default for WorkspaceRuntime {
    fn default() -> Self {
        Self {
            status: OrchestrationStatus::Stopped,
            task_session_id: String::new(),
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            max_attempts: 4,
            retry_base_ms: 5_000,
            retry_max_ms: 300_000,
            started_at_ms: None,
            last_tick_ms: None,
            active_count: 0,
            tasks: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrchestrationSnapshot {
    status: OrchestrationStatus,
    task_session_id: Option<String>,
    max_concurrent: usize,
    active_count: usize,
    claiming_count: usize,
    retrying_count: usize,
    completed_count: usize,
    started_at_ms: Option<u64>,
    last_tick_ms: Option<u64>,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileInput {
    candidates: Vec<CandidateInput>,
    active_keys: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateInput {
    task_key: String,
    prior_attempts: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconcileResult {
    claims: Vec<TaskClaim>,
    snapshot: OrchestrationSnapshot,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskClaim {
    task_key: String,
    attempt: u32,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn clean_workspace_key(workspace_key: String) -> Result<String, String> {
    let key = workspace_key.trim();
    if key.is_empty() {
        return Err("A workspace is required to run orchestration.".to_string());
    }
    Ok(key.to_string())
}

fn clean_task_key(task_key: String) -> Result<String, String> {
    let key = task_key.trim();
    if key.is_empty() || key.len() > 512 {
        return Err("The orchestration task key is invalid.".to_string());
    }
    Ok(key.to_string())
}

fn snapshot(runtime: &WorkspaceRuntime) -> OrchestrationSnapshot {
    let mut claiming_count = 0;
    let mut retrying_count = 0;
    let mut completed_count = 0;
    let mut last_error = None;

    for task in runtime.tasks.values() {
        match task.phase {
            TaskPhase::Claiming => claiming_count += 1,
            TaskPhase::Retrying { .. } if task.attempt < runtime.max_attempts => {
                retrying_count += 1
            }
            TaskPhase::Retrying { .. } => {}
            TaskPhase::Completed => completed_count += 1,
            TaskPhase::Running { .. } => {}
        }
        if last_error.is_none() {
            last_error.clone_from(&task.last_error);
        }
    }

    OrchestrationSnapshot {
        status: runtime.status,
        task_session_id: (!runtime.task_session_id.is_empty())
            .then(|| runtime.task_session_id.clone()),
        max_concurrent: runtime.max_concurrent,
        active_count: runtime.active_count,
        claiming_count,
        retrying_count,
        completed_count,
        started_at_ms: runtime.started_at_ms,
        last_tick_ms: runtime.last_tick_ms,
        last_error,
    }
}

#[tauri::command]
pub fn orchestration_snapshot(
    workspace_key: String,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    let runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    Ok(runtimes
        .get(&workspace_key)
        .map(snapshot)
        .unwrap_or_else(|| snapshot(&WorkspaceRuntime::default())))
}

#[tauri::command]
pub fn orchestration_start(
    workspace_key: String,
    task_session_id: String,
    max_concurrent: usize,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    let task_session_id = task_session_id.trim();
    if task_session_id.is_empty() {
        return Err("Create or select a chat before starting orchestration.".to_string());
    }
    let max_concurrent = max_concurrent.clamp(1, MAX_CONCURRENT_LIMIT);
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    runtime.status = OrchestrationStatus::Running;
    runtime.task_session_id = task_session_id.to_string();
    runtime.max_concurrent = max_concurrent;
    runtime.started_at_ms.get_or_insert_with(now_ms);
    Ok(snapshot(runtime))
}

#[tauri::command]
pub fn orchestration_configure(
    workspace_key: String,
    config: WorkflowConfig,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    workflow::validate_config(&config, "configured workflow prompt")?;
    let workspace_key = clean_workspace_key(workspace_key)?;
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    runtime.max_concurrent = config.orchestration.max_concurrent;
    runtime.max_attempts = config.orchestration.max_attempts;
    runtime.retry_base_ms = config
        .orchestration
        .retry_base_seconds
        .saturating_mul(1_000);
    runtime.retry_max_ms = config.orchestration.retry_max_seconds.saturating_mul(1_000);
    Ok(snapshot(runtime))
}

#[tauri::command]
pub fn orchestration_pause(
    workspace_key: String,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    if runtime.status != OrchestrationStatus::Stopped {
        runtime.status = OrchestrationStatus::Paused;
    }
    Ok(snapshot(runtime))
}

#[tauri::command]
pub fn orchestration_stop(
    workspace_key: String,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    runtime.status = OrchestrationStatus::Stopped;
    runtime.started_at_ms = None;
    runtime.active_count = 0;
    runtime.tasks.clear();
    Ok(snapshot(runtime))
}

/// The claim-time scheduling-authority consult backing `orchestration_reconcile`
/// (CP-08-108). The renderer gates its own dispatch, but that gate can race a
/// flag flip by up to one tick, so reconcile re-reads the ledger before
/// claiming. A workspace that never materialized a `work.db` is legacy by
/// definition — flags cannot be canonical without it — and is answered
/// without opening the ledger, which would create the database as a side
/// effect of a read.
fn scheduling_claims_suppressed(workspace_key: &str) -> bool {
    let root = std::path::Path::new(workspace_key);
    let Ok(paths) = altai_core::resolve_workspace_from(Some(root), root) else {
        return false;
    };
    let work_db = paths.work_db();
    if !work_db.exists() {
        return false;
    }
    crate::altai::agent::desktop_host::canonical_scheduling_suppresses_fires(&paths.root)
}

#[tauri::command]
pub fn orchestration_reconcile(
    workspace_key: String,
    input: ReconcileInput,
    state: State<'_, OrchestrationState>,
) -> Result<ReconcileResult, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    // Consulted before the state lock so the ledger read never blocks other
    // workspaces' orchestration state.
    let claims_suppressed = scheduling_claims_suppressed(&workspace_key);
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    runtime.last_tick_ms = Some(now_ms());
    runtime.active_count = input.active_keys.len();

    if runtime.status != OrchestrationStatus::Running {
        return Ok(ReconcileResult {
            claims: Vec::new(),
            snapshot: snapshot(runtime),
        });
    }

    // Scheduling cutover (CP-08-108): when canonical scheduling owns this
    // workspace, renderer candidates are not claimed — a typed no-op. The
    // bookkeeping above stays live so the snapshot keeps observing state.
    if claims_suppressed {
        return Ok(ReconcileResult {
            claims: Vec::new(),
            snapshot: snapshot(runtime),
        });
    }

    let now = now_ms();
    let active: HashSet<&str> = input.active_keys.iter().map(String::as_str).collect();
    let mut occupied = active.len()
        + runtime
            .tasks
            .values()
            .filter(|task| matches!(task.phase, TaskPhase::Claiming))
            .count();
    let mut claims = Vec::new();
    let mut seen = HashSet::new();

    for candidate in input.candidates {
        let task_key = clean_task_key(candidate.task_key)?;
        if !seen.insert(task_key.clone()) || active.contains(task_key.as_str()) {
            continue;
        }
        if occupied >= runtime.max_concurrent {
            break;
        }

        let task = runtime
            .tasks
            .entry(task_key.clone())
            .or_insert(TaskRuntime {
                attempt: candidate.prior_attempts.min(runtime.max_attempts),
                phase: TaskPhase::Retrying { retry_at_ms: 0 },
                last_error: None,
                last_terminal_assignment_id: None,
            });

        let eligible = match task.phase {
            TaskPhase::Retrying { retry_at_ms } => retry_at_ms <= now,
            TaskPhase::Claiming | TaskPhase::Running { .. } | TaskPhase::Completed => false,
        };
        if !eligible || task.attempt >= runtime.max_attempts {
            continue;
        }

        task.attempt += 1;
        task.phase = TaskPhase::Claiming;
        task.last_error = None;
        occupied += 1;
        claims.push(TaskClaim {
            task_key,
            attempt: task.attempt,
        });
    }

    Ok(ReconcileResult {
        claims,
        snapshot: snapshot(runtime),
    })
}

#[tauri::command]
pub fn orchestration_dispatch_result(
    workspace_key: String,
    task_key: String,
    assignment_id: Option<String>,
    error: Option<String>,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    let task_key = clean_task_key(task_key)?;
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    let task = runtime
        .tasks
        .get_mut(&task_key)
        .ok_or_else(|| "The orchestration claim no longer exists.".to_string())?;

    if let Some(assignment_id) = assignment_id.filter(|id| !id.trim().is_empty()) {
        task.phase = TaskPhase::Running { assignment_id };
        task.last_error = None;
    } else {
        let delay = (runtime
            .retry_base_ms
            .saturating_mul(1_u64 << task.attempt.saturating_sub(1)))
        .min(runtime.retry_max_ms);
        task.phase = TaskPhase::Retrying {
            retry_at_ms: now_ms().saturating_add(delay),
        };
        task.last_error = Some(
            error
                .filter(|message| !message.trim().is_empty())
                .unwrap_or_else(|| "Agent dispatch failed.".to_string()),
        );
    }
    Ok(snapshot(runtime))
}

#[tauri::command]
pub fn orchestration_record_terminal(
    workspace_key: String,
    task_key: String,
    assignment_id: String,
    outcome: String,
    error: Option<String>,
    state: State<'_, OrchestrationState>,
) -> Result<OrchestrationSnapshot, String> {
    let workspace_key = clean_workspace_key(workspace_key)?;
    let task_key = clean_task_key(task_key)?;
    let assignment_id = assignment_id.trim();
    if assignment_id.is_empty() {
        return Err("A terminal assignment id is required.".to_string());
    }
    let mut runtimes = state
        .0
        .lock()
        .map_err(|_| "Orchestration state is unavailable.".to_string())?;
    let runtime = runtimes.entry(workspace_key).or_default();
    let Some(task) = runtime.tasks.get_mut(&task_key) else {
        return Ok(snapshot(runtime));
    };
    if task.last_terminal_assignment_id.as_deref() == Some(assignment_id) {
        return Ok(snapshot(runtime));
    }
    if let TaskPhase::Running {
        assignment_id: running_id,
    } = &task.phase
    {
        if running_id != assignment_id {
            return Ok(snapshot(runtime));
        }
    }

    task.last_terminal_assignment_id = Some(assignment_id.to_string());
    if outcome == "done" {
        task.phase = TaskPhase::Completed;
        task.last_error = None;
    } else {
        let delay = (runtime
            .retry_base_ms
            .saturating_mul(1_u64 << task.attempt.saturating_sub(1)))
        .min(runtime.retry_max_ms);
        task.phase = TaskPhase::Retrying {
            retry_at_ms: now_ms().saturating_add(delay),
        };
        task.last_error = Some(
            error
                .filter(|message| !message.trim().is_empty())
                .unwrap_or_else(|| format!("Agent run ended with status {outcome}.")),
        );
    }
    Ok(snapshot(runtime))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running_runtime(max_concurrent: usize) -> WorkspaceRuntime {
        WorkspaceRuntime {
            status: OrchestrationStatus::Running,
            task_session_id: "session-1".to_string(),
            max_concurrent,
            started_at_ms: Some(now_ms()),
            ..WorkspaceRuntime::default()
        }
    }

    #[test]
    fn snapshot_counts_task_phases() {
        let mut runtime = running_runtime(2);
        runtime.tasks.insert(
            "todo-a".into(),
            TaskRuntime {
                attempt: 1,
                phase: TaskPhase::Claiming,
                last_error: None,
                last_terminal_assignment_id: None,
            },
        );
        runtime.tasks.insert(
            "todo-b".into(),
            TaskRuntime {
                attempt: 2,
                phase: TaskPhase::Retrying { retry_at_ms: 42 },
                last_error: Some("boom".into()),
                last_terminal_assignment_id: None,
            },
        );
        let result = snapshot(&runtime);
        assert_eq!(result.claiming_count, 1);
        assert_eq!(result.retrying_count, 1);
        assert_eq!(result.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn concurrency_is_bounded_by_active_and_claiming_tasks() {
        let mut runtime = running_runtime(2);
        runtime.active_count = 1;
        runtime.tasks.insert(
            "todo-a".into(),
            TaskRuntime {
                attempt: 1,
                phase: TaskPhase::Claiming,
                last_error: None,
                last_terminal_assignment_id: None,
            },
        );
        let occupied = runtime.active_count
            + runtime
                .tasks
                .values()
                .filter(|task| matches!(task.phase, TaskPhase::Claiming))
                .count();
        assert_eq!(occupied, runtime.max_concurrent);
    }

    #[test]
    fn workspace_and_task_keys_are_validated() {
        assert!(clean_workspace_key(" ".into()).is_err());
        assert!(clean_task_key("".into()).is_err());
        assert!(clean_task_key("todo-1".into()).is_ok());
    }

    #[test]
    fn claim_consult_keeps_legacy_without_a_materialized_work_db() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        assert!(!scheduling_claims_suppressed(
            &root.to_string_lossy()
        ));
        let work_db = altai_core::resolve_workspace_from(Some(root), root)
            .unwrap()
            .work_db();
        assert!(
            !work_db.exists(),
            "a reconcile-time authority consult must not create work.db"
        );
    }

    #[test]
    fn claim_consult_suppresses_claims_when_canonical_scheduling_owns_the_workspace() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let work_db = altai_core::resolve_workspace_from(Some(root), root)
            .unwrap()
            .work_db();
        std::fs::create_dir_all(work_db.parent().unwrap()).unwrap();
        use altai_control_plane::FeatureFlagRepository;
        let ledger = altai_control_plane::SqliteFeatureFlagRepository::open(&work_db).unwrap();
        ledger.set("control_plane_enabled", "true").unwrap();
        drop(ledger);
        assert!(scheduling_claims_suppressed(&root.to_string_lossy()));
    }

    #[test]
    fn claim_consult_keeps_claims_when_the_work_db_exists_but_flags_are_absent() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let work_db = altai_core::resolve_workspace_from(Some(root), root)
            .unwrap()
            .work_db();
        std::fs::create_dir_all(work_db.parent().unwrap()).unwrap();
        let _ledger = altai_control_plane::SqliteFeatureFlagRepository::open(&work_db).unwrap();
        assert!(!scheduling_claims_suppressed(
            &root.to_string_lossy()
        ));
    }
}
