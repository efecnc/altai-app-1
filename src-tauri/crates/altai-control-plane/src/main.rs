use altai_control_plane::{
    router_with_control_repositories, BootstrapCredential, ControlPlane, ControlPlaneConfig,
    ControlPlaneStore, FeatureFlagRepository, RoutineCronBridge, RoutineMaterializer,
    SqliteActivityEventRepository, SqliteAgentRepository, SqliteApprovalRepository,
    SqliteAttemptRepository, SqliteControlEventRepository, SqliteCronAutomationTransfer,
    SqliteFeatureFlagRepository, SqlitePluginRegistry, SqliteRegistrationRepository,
    SqliteRoutineRepository, SqliteRunBindingRepository, SqliteScopeRepository,
    SqliteWakeRepository, SqliteWorkGraphRepository, SqliteWorkItemRepository, TransferAttribution,
    CONTROL_PLANE_ENABLED_FLAG, DEFAULT_CRON_TICK, LEGACY_CRON_COMPATIBILITY_FLAG,
    SCHEDULE_OWNER_DAEMON, SCHEDULE_OWNER_DESKTOP, SCHEDULE_OWNER_FLAG,
};
use altai_control_protocol::{Actor, OrganizationId, ProjectId};
use altai_core::resolve_workspace;
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, sync::Arc};

#[derive(Parser)]
#[command(
    name = "altai-control-plane",
    about = "ALTAI authenticated control-plane daemon",
    subcommand_negates_reqs = true
)]
struct Args {
    /// Loopback listener. Non-loopback listeners require a future TLS/proxy deployment path.
    #[arg(long, default_value = "127.0.0.1:8787")]
    bind: SocketAddr,
    /// Existing ALTAI workspace whose local work.db stores control-plane state.
    #[arg(long)]
    workspace: Option<std::path::PathBuf>,
    /// Bootstrap bearer credential. Prefer ALTAI_CONTROL_PLANE_BOOTSTRAP_TOKEN.
    #[arg(long, env = "ALTAI_CONTROL_PLANE_BOOTSTRAP_TOKEN")]
    bootstrap_token: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Scheduling-cutover transfer surface (CP-08-108): freeze the legacy
    /// cron automations, then cut the workspace's schedule over to a named
    /// owner. The steps are separate commands so each is inspectable; the
    /// freeze order snapshot → verify → flags → retire is enforced inside
    /// `enable`.
    Schedule {
        #[command(subcommand)]
        action: ScheduleAction,
    },
}

#[derive(Subcommand)]
enum ScheduleAction {
    /// Map every active legacy automation into canonical work items and
    /// routines. Idempotent; re-runs with unchanged content write nothing.
    Snapshot {
        /// Organization the transferred work items are attributed to.
        #[arg(long)]
        organization: String,
        /// Project the transferred work items land in (must belong to the
        /// organization).
        #[arg(long)]
        project: String,
    },
    /// Verify every active automation is mapped and compatible (typed-closed
    /// otherwise), record the schedule owner and `control_plane_enabled` as
    /// insert-only flag writes, then retire the mapped legacy rows.
    Enable {
        /// Schedule owner to record: `daemon` or `desktop_host`.
        #[arg(long)]
        owner: String,
    },
    /// Print the ledger flags and the transfer state of every active
    /// legacy automation.
    Status,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    match args.command {
        Some(Command::Schedule { action }) => run_schedule(args.workspace, action),
        None => serve(args).await,
    }
}

async fn serve(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    if !args.bind.ip().is_loopback() {
        return Err("control-plane daemon only permits loopback bind in this milestone".into());
    }
    let credential = BootstrapCredential::from_plaintext(&args.bootstrap_token)?;
    let workspace = resolve_workspace(args.workspace.as_deref())?;
    let work_db = workspace.work_db();
    if let Some(parent) = work_db.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let config = ControlPlaneConfig {
        service_version: env!("CARGO_PKG_VERSION").to_string(),
        store: ControlPlaneStore::Sqlite {
            database_path: work_db.to_string_lossy().to_string(),
        },
        registration_ttl_seconds: 300,
    };
    let scope_repository = Arc::new(SqliteScopeRepository::open(&work_db)?);
    scope_repository.ensure_default_local_organization()?;
    let agent_repository = Arc::new(SqliteAgentRepository::open(&work_db)?);
    let work_graph_repository = Arc::new(SqliteWorkGraphRepository::open(&work_db)?);
    let work_item_repository = Arc::new(SqliteWorkItemRepository::open(&work_db)?);
    let wake_repository = Arc::new(SqliteWakeRepository::open(&work_db)?);
    let run_binding_repository = Arc::new(SqliteRunBindingRepository::open(&work_db)?);
    let attempt_repository = Arc::new(SqliteAttemptRepository::open(&work_db)?);
    let routine_repository = Arc::new(SqliteRoutineRepository::open(&work_db)?);
    let approval_repository = Arc::new(SqliteApprovalRepository::open(&work_db)?);
    let activity_repository = Arc::new(SqliteActivityEventRepository::open(&work_db)?);
    let control_event_repository =
        Arc::new(SqliteControlEventRepository::open(&work_db)?);
    let plugin_registry = Arc::new(SqlitePluginRegistry::open(&work_db)?);
    // Managed cron bridge under the scheduling cutover's authority gate:
    // each tick re-reads the feature-flag ledger. A decided ledger that
    // names this daemon drives canonically while holding the workspace
    // single-writer lock; an undecided ledger (every flag absent — the
    // state of every deployment that predates the cutover) or a pulled
    // rollback switch keeps the byte-for-byte legacy tick running
    // unconditionally, without the lock.
    let materializer = Arc::new(RoutineMaterializer::new(
        routine_repository.clone(),
        wake_repository.clone(),
    ));
    let ledger = Arc::new(SqliteFeatureFlagRepository::open(&work_db)?);
    tokio::spawn(
        RoutineCronBridge::new(materializer, DEFAULT_CRON_TICK)
            .run_gated(ledger, work_db.clone()),
    );
    let plane = Arc::new(ControlPlane::with_registration_repository(
        config,
        Arc::new(SqliteRegistrationRepository::open(&work_db)?),
    )?);
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    eprintln!(
        "altai-control-plane listening on {}",
        listener.local_addr()?
    );
    axum::serve(
        listener,
        router_with_control_repositories(
            plane,
            credential,
            Some(scope_repository),
            Some(agent_repository),
            Some(work_graph_repository),
            Some(work_item_repository),
            wake_repository,
            Some(run_binding_repository),
            Some(attempt_repository),
            Some(routine_repository),
            Some(approval_repository),
            Some(activity_repository),
            Some(control_event_repository),
            Some(plugin_registry),
        ),
    )
    .await?;
    Ok(())
}

/// The `schedule` subcommands: the operator surface for the CP-08-108
/// transfer. All three share the workspace resolution and refuse to run
/// against a workspace with no legacy automation store — a missing store
/// must be a visible fact, not a vacuously empty transfer.
fn run_schedule(
    workspace_arg: Option<std::path::PathBuf>,
    action: ScheduleAction,
) -> Result<(), Box<dyn std::error::Error>> {
    let workspace = resolve_workspace(workspace_arg.as_deref())?;
    let work_db = workspace.work_db();
    if let Some(parent) = work_db.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // The legacy store IsanAgent writes; the transfer reads it read-only.
    let memory_db = workspace
        .isanagent_state
        .join(".system_generated")
        .join("agent_memory.db");
    if !memory_db.exists() {
        return Err(format!(
            "no legacy automation store at {}; there is nothing to transfer from this workspace",
            memory_db.display()
        )
        .into());
    }
    let transfer = SqliteCronAutomationTransfer::open(&work_db)?;
    match action {
        ScheduleAction::Snapshot {
            organization,
            project,
        } => {
            // Opening both repositories ensures their schemas exist before
            // the transfer transaction writes into them.
            let work_items = SqliteWorkItemRepository::open(&work_db)?;
            SqliteRoutineRepository::open(&work_db)?;
            let attribution = TransferAttribution {
                organization_id: OrganizationId::new(&organization),
                project_id: ProjectId::new(&project),
                actor: Actor::System {
                    component: "altai-control-plane schedule snapshot".into(),
                },
            };
            let report = transfer.snapshot(&memory_db, &attribution, &work_items)?;
            println!(
                "active automations: {} (newly mapped {}, already mapped {}, incompatible {})",
                report.total_active, report.newly_mapped, report.already_mapped, report.incompatible
            );
            println!(
                "next: `schedule status` to inspect, then `schedule enable --owner <daemon|desktop_host>`"
            );
        }
        ScheduleAction::Enable { owner } => {
            if owner != SCHEDULE_OWNER_DAEMON && owner != SCHEDULE_OWNER_DESKTOP {
                return Err(format!(
                    "unknown schedule owner {owner:?}: the ledger records `daemon` or `desktop_host`"
                )
                .into());
            }
            let ledger = SqliteFeatureFlagRepository::open(&work_db)?;
            let retired = enable_schedule(&transfer, &ledger, &memory_db, &owner)?;
            println!(
                "canonical scheduling enabled with owner {owner:?}; {retired} legacy row(s) retired"
            );
        }
        ScheduleAction::Status => {
            let ledger = SqliteFeatureFlagRepository::open(&work_db)?;
            println!(
                "control_plane_enabled: {:?}",
                ledger.get(CONTROL_PLANE_ENABLED_FLAG)?
            );
            println!("schedule_owner: {:?}", ledger.get(SCHEDULE_OWNER_FLAG)?);
            println!(
                "legacy_cron_compatibility: {:?}",
                ledger.get(LEGACY_CRON_COMPATIBILITY_FLAG)?
            );
            for row in transfer.status(&memory_db)? {
                println!(
                    "automation {} enabled={} disposition={} retired={}",
                    row.automation_id,
                    row.enabled,
                    row.disposition.as_deref().unwrap_or("unmapped"),
                    row.retired
                );
            }
        }
    }
    Ok(())
}

/// Insert-only flag write via `set_if_absent`: the first recorded decision
/// wins, an equal value is accepted as idempotent, and a conflicting one
/// fails typed instead of being silently overwritten.
fn record_flag(
    ledger: &dyn FeatureFlagRepository,
    flag_key: &str,
    flag_value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if ledger.set_if_absent(flag_key, flag_value)? {
        return Ok(());
    }
    let existing = ledger.get(flag_key)?.unwrap_or_default();
    if existing == flag_value {
        return Ok(());
    }
    Err(format!(
        "feature flag {flag_key} is already {existing:?}; refusing to overwrite it with {flag_value:?}"
    )
    .into())
}

/// The `enable` freeze order, enforced in sequence: verify every active
/// automation is mapped and compatible (typed-closed, naming each blocker)
/// before any flag moves; insert-only flag writes — the owner is named
/// first, then the handover happens, so a crash between the two leaves the
/// ledger undecided (legacy behavior stands); then the post-flag half in
/// [`reverify_and_retire`]. Returns the number of retired legacy rows.
fn enable_schedule(
    transfer: &SqliteCronAutomationTransfer,
    ledger: &dyn FeatureFlagRepository,
    memory_db: &std::path::Path,
    owner: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    transfer.verify_enable_ready(memory_db)?;
    record_flag(ledger, SCHEDULE_OWNER_FLAG, owner)?;
    record_flag(ledger, CONTROL_PLANE_ENABLED_FLAG, "true")?;
    reverify_and_retire(transfer, memory_db, owner)
}

/// The post-flag half of the freeze order. The verify→flag window is real:
/// an agent instance whose CronTool is still registered can create an
/// automation between the pre-flag verify and the flag writes, so the
/// ledger is re-verified now that it has decided. A newcomer fails typed,
/// naming it. The flags are already recorded at that point (insert-only,
/// first decision wins): the workspace is canonically decided, the newcomer
/// can neither fire (the desktop fire-time gate suppresses legacy firing
/// once the flags are on) nor transfer (it has no mapping row yet), and the
/// typed error is the visible fact of that stranded state. The operator
/// resolves it by re-running `schedule snapshot` (idempotent, maps the
/// newcomer) and then `schedule enable` (the equal-value flag writes are
/// accepted, then retirement completes).
fn reverify_and_retire(
    transfer: &SqliteCronAutomationTransfer,
    memory_db: &std::path::Path,
    owner: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    if let Err(error) = transfer.verify_enable_ready(memory_db) {
        eprintln!(
            "the ledger flags are already recorded (owner={owner:?}, control_plane_enabled=true); \
             the automation(s) above can neither fire nor transfer until they are snapshotted: \
             re-run `schedule snapshot` (idempotent), then `schedule enable`"
        );
        return Err(error.into());
    }
    let automations = SqliteCronAutomationTransfer::read_active_automations(memory_db)?;
    for automation in &automations {
        transfer.mark_retired(&automation.id)?;
    }
    Ok(automations.len())
}


#[cfg(test)]
mod tests {
    use super::*;
    use altai_control_plane::{ScopeRepository, TransferError};
    use altai_control_protocol::{Organization, Project, ProjectStatus, Revision};
    use rusqlite::{params, Connection};

    const CRON_JOBS_DDL: &str = "CREATE TABLE cron_jobs (
        id TEXT PRIMARY KEY,
        schedule TEXT NOT NULL,
        message TEXT NOT NULL,
        chat_id TEXT NOT NULL DEFAULT 'unknown',
        channel TEXT NOT NULL DEFAULT 'unknown',
        completed_at_ms INTEGER,
        enabled INTEGER NOT NULL DEFAULT 1
    );";

    struct Harness {
        _dir: tempfile::TempDir,
        work_db: std::path::PathBuf,
        transfer: SqliteCronAutomationTransfer,
        ledger: SqliteFeatureFlagRepository,
        memory_db: std::path::PathBuf,
        attribution: TransferAttribution,
        work_items: SqliteWorkItemRepository,
    }

    fn harness() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let work_db = dir.path().join("work.db");
        let scope = SqliteScopeRepository::open(&work_db).unwrap();
        let organization_id = OrganizationId::new("org");
        scope
            .create_organization(Organization {
                id: organization_id.clone(),
                name: "Enable org".into(),
                revision: Revision::INITIAL,
                created_at: "2026-09-20T00:00:00.000Z".into(),
                updated_at: "2026-09-20T00:00:00.000Z".into(),
            })
            .unwrap();
        let project_id = ProjectId::new("proj");
        scope
            .create_project(Project {
                id: project_id.clone(),
                organization_id: organization_id.clone(),
                goal_ids: Vec::new(),
                name: "Enable project".into(),
                description: String::new(),
                status: ProjectStatus::Active,
                revision: Revision::INITIAL,
                created_at: "2026-09-20T00:00:00.000Z".into(),
                updated_at: "2026-09-20T00:00:00.000Z".into(),
            })
            .unwrap();
        let memory_db = dir.path().join("agent_memory.db");
        Connection::open(&memory_db)
            .unwrap()
            .execute_batch(CRON_JOBS_DDL)
            .unwrap();
        // Same schema guarantee as the snapshot command: opening the
        // routine repository creates the routine tables the transfer
        // writes into.
        SqliteRoutineRepository::open(&work_db).unwrap();
        Harness {
            _dir: dir,
            work_db: work_db.clone(),
            transfer: SqliteCronAutomationTransfer::open(&work_db).unwrap(),
            ledger: SqliteFeatureFlagRepository::open(&work_db).unwrap(),
            memory_db,
            attribution: TransferAttribution {
                organization_id,
                project_id,
                actor: Actor::System {
                    component: "enable-test".into(),
                },
            },
            work_items: SqliteWorkItemRepository::open(&work_db).unwrap(),
        }
    }

    fn insert_cron_job(memory_db: &std::path::Path, id: &str) {
        Connection::open(memory_db)
            .unwrap()
            .execute(
                "INSERT INTO cron_jobs (id, schedule, message) VALUES (?1, ?2, 'job')",
                params![id, r#"{"kind":"Cron","cron_expr":"0 9 * * MON"}"#],
            )
            .unwrap();
    }

    fn retired_at(h: &Harness, automation_id: &str) -> Option<i64> {
        Connection::open(&h.work_db)
            .unwrap()
            .query_row(
                "SELECT retired_at_unix_seconds FROM control_plane_cron_automation_mappings WHERE automation_id = ?1",
                params![automation_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// The enable happy path: both ledger flags recorded, every mapped
    /// legacy row retired — and a re-run is idempotent (equal-value flag
    /// writes accepted, already-retired rows stay retired).
    #[test]
    fn enable_writes_both_flags_and_retires_mapped_rows_idempotently() {
        let h = harness();
        insert_cron_job(&h.memory_db, "job-1");
        h.transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items)
            .unwrap();

        assert_eq!(
            enable_schedule(&h.transfer, &h.ledger, &h.memory_db, SCHEDULE_OWNER_DAEMON).unwrap(),
            1
        );
        assert_eq!(
            h.ledger.get(SCHEDULE_OWNER_FLAG).unwrap().as_deref(),
            Some(SCHEDULE_OWNER_DAEMON)
        );
        assert_eq!(
            h.ledger.get(CONTROL_PLANE_ENABLED_FLAG).unwrap().as_deref(),
            Some("true")
        );
        assert!(retired_at(&h, "job-1").is_some());

        // Re-run: nothing new to do, nothing fails.
        assert_eq!(
            enable_schedule(&h.transfer, &h.ledger, &h.memory_db, SCHEDULE_OWNER_DAEMON).unwrap(),
            1
        );
    }

    /// N2 regression: the verify→flag window. An automation that appears
    /// after the pre-flag verify (created through a still-registered agent
    /// CronTool) must fail the post-flag re-verify typed, naming the
    /// newcomer — never proceed to strand it suppressed silently. The flags
    /// stay recorded (insert-only, first decision wins) and nothing is
    /// half-retired; the operator resolves by re-running snapshot + enable.
    #[test]
    fn a_newcomer_after_the_flag_writes_fails_the_reverify_typed() {
        let h = harness();
        insert_cron_job(&h.memory_db, "job-1");
        h.transfer
            .snapshot(&h.memory_db, &h.attribution, &h.work_items)
            .unwrap();
        // The pre-flag verify passed and both flags were written; then the
        // newcomer appeared through the still-registered CronTool window.
        record_flag(&h.ledger, SCHEDULE_OWNER_FLAG, SCHEDULE_OWNER_DAEMON).unwrap();
        record_flag(&h.ledger, CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        insert_cron_job(&h.memory_db, "job-newcomer");

        let error = reverify_and_retire(&h.transfer, &h.memory_db, SCHEDULE_OWNER_DAEMON)
            .expect_err("an unmapped newcomer must fail the post-flag re-verify");
        let blocked = error
            .downcast_ref::<TransferError>()
            .expect("the refusal must stay a TransferError");
        match blocked {
            TransferError::EnableBlocked { blockers } => {
                assert_eq!(blockers.len(), 1);
                assert_eq!(blockers[0].0, "job-newcomer");
            }
            other => panic!("expected EnableBlocked naming the newcomer, got {other:?}"),
        }
        // The recorded decision stands; nothing was half-retired.
        assert_eq!(
            h.ledger.get(SCHEDULE_OWNER_FLAG).unwrap().as_deref(),
            Some(SCHEDULE_OWNER_DAEMON)
        );
        assert_eq!(
            h.ledger.get(CONTROL_PLANE_ENABLED_FLAG).unwrap().as_deref(),
            Some("true")
        );
        assert!(retired_at(&h, "job-1").is_none());
    }
}

