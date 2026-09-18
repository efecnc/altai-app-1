use altai_control_plane::{
    router_with_control_repositories, BootstrapCredential, ControlPlane, ControlPlaneConfig,
    ControlPlaneStore, RoutineCronBridge, RoutineMaterializer, SqliteActivityEventRepository,
    SqliteAgentRepository, SqliteApprovalRepository, SqliteAttemptRepository,
    SqliteControlEventRepository, SqliteFeatureFlagRepository, SqlitePluginRegistry,
    SqliteRegistrationRepository,
    SqliteRoutineRepository,
    SqliteRunBindingRepository, SqliteScopeRepository, SqliteWakeRepository,
    SqliteWorkGraphRepository, SqliteWorkItemRepository, DEFAULT_CRON_TICK,
};
use altai_core::resolve_workspace;
use clap::Parser;
use std::{net::SocketAddr, sync::Arc};

#[derive(Parser)]
#[command(
    name = "altai-control-plane",
    about = "ALTAI authenticated control-plane daemon"
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
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
    // Managed cron bridge under the scheduling cutover's authority gate: the
    // loop re-reads the feature-flag ledger every tick and materializes only
    // while this daemon is the named schedule owner (holding the workspace
    // single-writer lock). Flag-free deployments stay byte-for-byte idle
    // here — which is exactly today's unconditional behavior, gated.
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
