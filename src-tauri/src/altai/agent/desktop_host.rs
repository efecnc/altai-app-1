//! Desktop HostAdapter — Tauri-specific seams for the shared AgentService.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use isanagent::bus::BusMessage;
use isanagent::clarification::ClarificationHub;
use isanagent::scheduler::{CronActor, CronSchedulingMode};
use isanagent::workspace::resolve_workspace_root;
use isanagent::{NodeHandle, Supervisor, SupervisorPolicy};
use tauri::async_runtime;
use tauri::{AppHandle, Manager};
use tokio::sync::mpsc;

use altai_agent_service::{
    build_shared_instance, AgentEventSink, BuildInstanceRequest, BuiltInstance, HostAdapter,
    ServiceChannel, SharedInstanceHooks, SharedRunCoordinator, WorkspaceBundle,
    WorkspaceServices as SharedWorkspaceServices,
};
use isanagent::tools::ToolRegistry;
use std::path::Path;
use super::runtime::{
    recover_background_jobs_after_owner_bind, trusted_tauri_inbound,
    validate_tauri_chat_id, WorkspaceDispatcher,
};

/// The scheduling-cutover fire-time gate (CP-08-108): once the workspace's
/// feature-flag ledger records canonical scheduling as enabled with the
/// legacy rollback switch un-pulled, the CronActor's ALTAI-hosted firing
/// path stops — regardless of any retirement bookkeeping, so the flag flip
/// alone is the switch. An unreadable ledger or workspace resolves to
/// legacy behavior (the flag is "not yet decided").
pub(crate) fn canonical_scheduling_suppresses_fires(workspace_root: &Path) -> bool {
    let work_db = match altai_core::resolve_workspace_from(Some(workspace_root), workspace_root) {
        Ok(paths) => paths.work_db(),
        Err(_) => return false,
    };
    let Ok(ledger) = altai_control_plane::SqliteFeatureFlagRepository::open(&work_db) else {
        return false;
    };
    matches!(
        altai_control_plane::resolve_schedule_authority(&ledger),
        Ok(authority) if authority.enabled && !authority.legacy_compatibility
    )
}

use super::tauri_sink::TauriEventSink;
use crate::modules::mcp;

struct WorkspaceLogger {
    handle: isanagent::logging::LoggerHandle,
    #[allow(dead_code)]
    node: NodeHandle<BusMessage>,
    forwarder: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for WorkspaceLogger {
    fn drop(&mut self) {
        if let Some(forwarder) = self.forwarder.lock().ok().and_then(|mut guard| guard.take()) {
            let _ = forwarder.join();
        }
    }
}

pub(crate) struct WorkspaceCron {
    pub node: NodeHandle<String>,
    #[allow(dead_code)]
    forwarder: async_runtime::JoinHandle<()>,
}

/// Services that must have exactly one owner for a workspace, independent of
/// how many provider/persona instances happen to serve that workspace.
pub(crate) struct DesktopWorkspaceServices {
    /// Durable paths/journal are opened and restart-classified by the shared,
    /// host-neutral service boundary. Desktop retains only its Tauri-specific
    /// actors and routes in this task.
    _shared: Arc<SharedWorkspaceServices>,
    pub memory_node: NodeHandle<isanagent::memory::MemoryMessage>,
    pub event_journal: Arc<altai_core::journal::EventJournal>,
    pub clarification_hub: Arc<ClarificationHub>,
    logger: WorkspaceLogger,
    pub dispatcher: Arc<WorkspaceDispatcher>,
    pub cron: WorkspaceCron,
}

/// Tauri host adapter. Owns workspace actor bundles; AgentService owns instances.
pub struct DesktopHost {
    app: AppHandle,
    run_coordinator: SharedRunCoordinator,
    workspace_services_by_root: tokio::sync::Mutex<HashMap<String, Arc<DesktopWorkspaceServices>>>,
}

impl DesktopHost {
    pub fn new(app: AppHandle, run_coordinator: SharedRunCoordinator) -> Self {
        Self {
            app,
            run_coordinator,
            workspace_services_by_root: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    pub async fn workspace_services(
        &self,
        workspace_root: &str,
    ) -> Result<Arc<DesktopWorkspaceServices>, String> {
        self.workspace_bundle_inner(workspace_root).await
    }

    async fn workspace_bundle_inner(
        &self,
        workspace_root: &str,
    ) -> Result<Arc<DesktopWorkspaceServices>, String> {
        let mut guard = self.workspace_services_by_root.lock().await;
        if let Some(existing) = guard.get(workspace_root) {
            return Ok(existing.clone());
        }
        let ws_opt = if workspace_root.is_empty() {
            None
        } else {
            Some(workspace_root)
        };
        let dir = resolve_workspace_root(ws_opt);
        let shared = Arc::new(
            SharedWorkspaceServices::open(&dir)
                .map_err(|error| format!("Failed to initialize workspace services: {error}"))?,
        );
        let db_path = shared.memory_db_path();
        let db_path_str = db_path
            .to_str()
            .ok_or("workspace DB path is not valid UTF-8")?;
        let event_journal = shared.event_journal();
        let memory_actor = isanagent::memory::SqliteMemoryActor::new(db_path_str)
            .map_err(|e| format!("Failed to initialize SqliteMemoryActor: {}", e))?;
        let node = NodeHandle::<isanagent::memory::MemoryMessage>::new(
            memory_actor,
            100,
            1,
            Duration::from_millis(5),
        );
        let (logger_handle, logger_rx) =
            isanagent::logging::create_logger_channel(isanagent::logging::LOGGER_QUEUE_CAPACITY);
        let logger_factory = {
            let workspace_dir = dir.clone();
            move || isanagent::logging::create_logging_actor_or_fallback(workspace_dir.clone())
        };
        let logger_node = NodeHandle::<BusMessage>::new(
            Supervisor::new(SupervisorPolicy::Restart, logger_factory),
            1_000,
            1,
            Duration::from_millis(10),
        );
        let logger_forward = logger_node.clone();
        let runtime_handle = tokio::runtime::Handle::current();
        let forwarder = std::thread::Builder::new()
            .name("altai-isanagent-logger".to_string())
            .spawn(move || {
                while let Ok(message) = logger_rx.recv() {
                    if runtime_handle
                        .block_on(logger_forward.send_packet(message))
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .map_err(|error| format!("Failed to start workspace logger forwarder: {error}"))?;

        let dispatcher = Arc::new(WorkspaceDispatcher::new(self.run_coordinator.clone()));
        let (cron_bus_tx, mut cron_bus_rx) = mpsc::channel::<BusMessage>(100);
        let cron_logic = CronActor::new(
            "AltaiWorkspaceCron",
            db_path_str,
            logger_handle.clone(),
            CronSchedulingMode::Local,
            cron_bus_tx,
        )
        .map_err(|error| format!("Failed to initialize workspace cron actor: {error}"))?;
        let cron_node = NodeHandle::new(cron_logic, 100, 1, Duration::from_millis(50));
        let dispatcher_for_cron = dispatcher.clone();
        let cron_workspace_root = dir.clone();
        let cron_forwarder = async_runtime::spawn(async move {
            while let Some(message) = cron_bus_rx.recv().await {
                let BusMessage::Inbound(inbound) = message else {
                    continue;
                };
                // Fire-time gate (CP-08-108): once canonical scheduling owns
                // this workspace, the legacy firing path stops here — the
                // flip is the switch, bookkeeping is not.
                if canonical_scheduling_suppresses_fires(&cron_workspace_root) {
                    log::info!(
                        "Suppressed cron fire for {}: canonical scheduling owns this workspace",
                        inbound.chat_id
                    );
                    continue;
                }
                let chat_id = inbound.chat_id.clone();
                if inbound.channel != "tauri"
                    || inbound.thread_id.is_some()
                    || validate_tauri_chat_id(&chat_id).is_err()
                {
                    log::warn!("Dropped cron delivery with an invalid ALTAI destination");
                    continue;
                }
                // A missing owner is expected after app restart. CronActor has
                // already persisted its running job, and `route_send` performs a
                // one-shot recovery when the user next reopens that conversation.
                if let Err(error) = dispatcher_for_cron
                    .dispatch(chat_id, trusted_tauri_inbound(inbound))
                    .await
                {
                    log::info!("Deferred cron delivery until its ALTAI chat is active: {error}");
                }
            }
        });

        let services = Arc::new(DesktopWorkspaceServices {
            _shared: shared,
            memory_node: node,
            event_journal,
            clarification_hub: ClarificationHub::shared(),
            logger: WorkspaceLogger {
                handle: logger_handle,
                node: logger_node,
                forwarder: std::sync::Mutex::new(Some(forwarder)),
            },
            dispatcher,
            cron: WorkspaceCron {
                node: cron_node,
                forwarder: cron_forwarder,
            },
        });
        guard.insert(workspace_root.to_string(), services.clone());
        Ok(services)
    }
}

#[async_trait]
impl HostAdapter for DesktopHost {
    type Channel = ServiceChannel;

    fn channel_owner_id(channel: &Self::Channel) -> &str {
        channel.owner_id()
    }

    fn event_sink(&self) -> Arc<dyn AgentEventSink> {
        Arc::new(TauriEventSink::new(self.app.clone()))
    }

    async fn workspace_bundle(&self, workspace_root: &str) -> Result<WorkspaceBundle, String> {
        let services = self.workspace_bundle_inner(workspace_root).await?;
        Ok(WorkspaceBundle {
            memory_node: services.memory_node.clone(),
            clarification_hub: services.clarification_hub.clone(),
            logger_handle: services.logger.handle.clone(),
            cron_node: services.cron.node.clone(),
            event_journal: services.event_journal.clone(),
        })
    }

    async fn retain_workspace_bundles(&self, keep_root: &str) {
        self.workspace_services_by_root
            .lock()
            .await
            .retain(|k, _| k == keep_root);
    }

    async fn on_chat_bound(
        &self,
        workspace_root: &str,
        chat_id: &str,
        owner_id: &str,
        bus_tx: mpsc::Sender<BusMessage>,
        is_first_bind: bool,
    ) {
        let Ok(services) = self.workspace_bundle_inner(workspace_root).await else {
            return;
        };
        services
            .dispatcher
            .bind(chat_id, bus_tx, owner_id)
            .await;
        if is_first_bind {
            if let Err(error) = recover_background_jobs_after_owner_bind(
                &services.memory_node,
                &services.dispatcher,
                chat_id,
            )
            .await
            {
                // The foreground message is already accepted. Recovery is a
                // best-effort side effect and must not turn that accepted send
                // into an IPC rejection that invites a duplicate retry.
                log::warn!(
                    "Could not recover persisted background work for chat {chat_id}: {error}"
                );
            }
        }
    }

    async fn build_instance(
        &self,
        request: BuildInstanceRequest<'_>,
    ) -> Result<BuiltInstance<Self::Channel>, String> {
        let checkpoint_root = self
            .app
            .path()
            .app_data_dir()
            .ok()
            .map(|dir| dir.join("checkpoints"));
        build_shared_instance(
            self,
            request,
            SharedInstanceHooks {
                checkpoint_root,
                scripted_responses: None,
                channel_name: "tauri",
            },
        )
        .await
    }

    async fn augment_tools(
        &self,
        sandbox_dir: &Path,
        tools: &mut ToolRegistry,
    ) -> Result<(), String> {
        let statuses = self.app.state::<mcp::McpStatusRegistry>();
        altai_agent_service::mcp::register_enabled_tools(sandbox_dir, tools, statuses.inner()).await
    }

    async fn clear_mcp_workspaces(&self, workspace_roots: &[String]) {
        if let Some(mcp_statuses) = self.app.try_state::<mcp::McpStatusRegistry>() {
            for root in workspace_roots {
                if !root.is_empty() {
                    mcp_statuses
                        .clear_workspace(std::path::Path::new(root))
                        .await;
                }
            }
        }
    }
}
