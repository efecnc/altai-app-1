//! Stdio HostAdapter — machine-facing seams for the shared AgentService.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use isanagent::bus::BusMessage;
use isanagent::clarification::ClarificationHub;
use isanagent::memory::{MemoryMessage, SharedReply};
use isanagent::scheduler::{CronActor, CronCommand, CronSchedulingMode, CronStore, ScheduleKind};
use isanagent::tools::ToolRegistry;
use isanagent::utils::ChatMessage;
use isanagent::workspace::resolve_workspace_root;
use isanagent::{NodeHandle, Supervisor, SupervisorPolicy};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use altai_agent_service::{
    build_shared_instance, AgentEventSink, BuildInstanceRequest, BuiltInstance, HostAdapter,
    ServiceChannel, SharedInstanceHooks, SharedRunCoordinator, WorkspaceBundle,
    WorkspaceServices as SharedWorkspaceServices,
};
use altai_core::WorkspacePaths;

struct WorkspaceLogger {
    handle: isanagent::logging::LoggerHandle,
    #[allow(dead_code)]
    node: NodeHandle<BusMessage>,
    /// Detached on drop — joining here deadlocks because the forwarder blocks
    /// on `recv` until `handle` is dropped, and field drop order joins first.
    #[allow(dead_code)]
    forwarder: std::thread::JoinHandle<()>,
}

struct WorkspaceCron {
    node: NodeHandle<String>,
    #[allow(dead_code)]
    forwarder: tokio::task::JoinHandle<()>,
}

struct StdioWorkspaceServices {
    _shared: Arc<SharedWorkspaceServices>,
    memory_node: NodeHandle<isanagent::memory::MemoryMessage>,
    event_journal: Arc<altai_core::journal::EventJournal>,
    clarification_hub: Arc<ClarificationHub>,
    logger: WorkspaceLogger,
    cron: WorkspaceCron,
}

/// A host-owned automation record. The scheduler keeps its opaque trigger
/// payload and webhook secrets private; this is the safe subset exposed by
/// the native protocol.
#[derive(Debug, Clone)]
pub struct StdioAutomation {
    pub id: String,
    pub chat_id: String,
    pub title: String,
    pub prompt: String,
    pub schedule: ScheduleKind,
    pub enabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct AutomationPayload {
    altai_automation: u8,
    title: String,
    prompt: String,
}

/// Stdio host adapter. Owns workspace actor bundles; AgentService owns instances.
pub struct StdioHost {
    workspace: WorkspacePaths,
    event_sink: Arc<dyn AgentEventSink>,
    session_shared: Arc<SharedWorkspaceServices>,
    workspace_services_by_root: tokio::sync::Mutex<HashMap<String, Arc<StdioWorkspaceServices>>>,
    cron_routes: Arc<tokio::sync::Mutex<HashMap<String, mpsc::Sender<BusMessage>>>>,
    mcp_statuses: altai_agent_service::mcp::McpStatusRegistry,
}

impl StdioHost {
    pub fn new(
        workspace: WorkspacePaths,
        event_sink: Arc<dyn AgentEventSink>,
        _run_coordinator: SharedRunCoordinator,
        session_shared: Arc<SharedWorkspaceServices>,
    ) -> Self {
        Self {
            workspace,
            event_sink,
            session_shared,
            workspace_services_by_root: tokio::sync::Mutex::new(HashMap::new()),
            cron_routes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mcp_statuses: altai_agent_service::mcp::McpStatusRegistry::new(),
        }
    }

    /// Deliver a reply only when this stdio chat has a live clarification wait.
    /// The hub removes the pending slot before delivery, making repeated replies
    /// fail rather than resuming the same tool twice.
    pub async fn deliver_clarification_reply(
        &self,
        chat_id: &str,
        text: String,
    ) -> Result<(), String> {
        let workspace_root = self.workspace.root.to_string_lossy().to_string();
        let services = self.workspace_bundle_inner(&workspace_root).await?;
        let session_key = isanagent::bus::clarification_session_key("stdio", chat_id, None);
        if services
            .clarification_hub
            .try_deliver_reply(&session_key, text)
        {
            Ok(())
        } else {
            Err("clarification_not_pending".to_string())
        }
    }

    /// Dismiss a live clarification exactly once.
    pub async fn dismiss_clarification(&self, chat_id: &str) -> Result<(), String> {
        let workspace_root = self.workspace.root.to_string_lossy().to_string();
        let services = self.workspace_bundle_inner(&workspace_root).await?;
        let session_key = isanagent::bus::clarification_session_key("stdio", chat_id, None);
        if services
            .clarification_hub
            .cancel_wait_if_pending(&session_key)
        {
            Ok(())
        } else {
            Err("clarification_not_pending".to_string())
        }
    }

    /// Rewind the most recent user turn so the host can retry it as a fresh run.
    ///
    /// The caller first verifies that the requested run is the chat's newest
    /// terminal run. Keeping that ownership check in the protocol layer avoids
    /// silently rewinding a newer turn when an old retry control is clicked.
    pub async fn rewind_latest_turn_for_retry(
        &self,
        chat_id: &str,
        replacement: Option<&str>,
    ) -> Result<String, String> {
        let workspace_root = format!(
            "{}/.isanagent",
            self.workspace.root.to_string_lossy().trim_end_matches('/')
        );
        let services = self.workspace_bundle_inner(&workspace_root).await?;
        let thread_id = isanagent::bus::clarification_session_key("stdio", chat_id, None);

        let (context_tx, context_rx) = oneshot::channel();
        services
            .memory_node
            .send_packet(MemoryMessage::GetContext {
                thread_id: thread_id.clone(),
                reply: SharedReply::new(context_tx),
            })
            .await
            .map_err(|error| format!("retry_memory_unavailable: {error}"))?;
        let context = context_rx
            .await
            .map_err(|_| "retry_memory_unavailable".to_string())??;

        let mut user_turns = 0_usize;
        let mut latest_prompt = None;
        for message in context {
            if message.role == "user" {
                user_turns += 1;
                latest_prompt = message.content.map(|content| content.text_content());
            }
        }
        let prompt = replacement
            .map(str::to_string)
            .or(latest_prompt)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "retry_prompt_not_found".to_string())?;
        if prompt.len() > 16_384 {
            return Err("invalid_retry_prompt".to_string());
        }

        let (truncate_tx, truncate_rx) = oneshot::channel();
        services
            .memory_node
            .send_packet(MemoryMessage::TruncateAfterUserMessage {
                thread_id,
                keep_user_messages: user_turns.saturating_sub(1),
                reply: SharedReply::new(truncate_tx),
            })
            .await
            .map_err(|error| format!("retry_memory_unavailable: {error}"))?;
        truncate_rx
            .await
            .map_err(|_| "retry_memory_unavailable".to_string())??;
        Ok(prompt)
    }

    /// Read the durable transcript for one stdio chat. The Webview never talks
    /// to the memory store directly; this keeps thread ownership and storage
    /// behind the native host.
    pub async fn get_session_messages(&self, chat_id: &str) -> Result<Vec<ChatMessage>, String> {
        let services = self.session_workspace_services().await?;
        let thread_id = isanagent::bus::clarification_session_key("stdio", chat_id, None);
        let (reply_tx, reply_rx) = oneshot::channel();
        services
            .memory_node
            .send_packet(MemoryMessage::GetContext {
                thread_id,
                reply: SharedReply::new(reply_tx),
            })
            .await
            .map_err(|error| format!("session_memory_unavailable: {error}"))?;
        reply_rx
            .await
            .map_err(|_| "session_memory_unavailable".to_string())?
    }

    /// Keep the transcript through the requested user turn. The IsanAgent
    /// memory actor performs the tail deletion atomically, including derived
    /// summaries and dropped tool-result cache rows.
    pub async fn truncate_session_after_user_message(
        &self,
        chat_id: &str,
        keep_user_messages: usize,
    ) -> Result<usize, String> {
        let services = self.session_workspace_services().await?;
        let thread_id = isanagent::bus::clarification_session_key("stdio", chat_id, None);
        let (reply_tx, reply_rx) = oneshot::channel();
        services
            .memory_node
            .send_packet(MemoryMessage::TruncateAfterUserMessage {
                thread_id,
                keep_user_messages,
                reply: SharedReply::new(reply_tx),
            })
            .await
            .map_err(|error| format!("session_memory_unavailable: {error}"))?;
        reply_rx
            .await
            .map_err(|_| "session_memory_unavailable".to_string())?
    }

    /// Permanently clear a stdio chat's memory transcript. Event-journal
    /// deletion remains in the protocol layer, after this host-owned action.
    pub async fn delete_session_memory(&self, chat_id: &str) -> Result<(), String> {
        if chat_id.trim().is_empty() || chat_id.len() > 256 {
            return Err("invalid_chat_id".to_string());
        }
        let services = self.session_workspace_services().await?;
        let thread_id = isanagent::bus::clarification_session_key("stdio", chat_id, None);
        let (reply_tx, reply_rx) = oneshot::channel();
        services.memory_node.send_packet(MemoryMessage::Clear {
            thread_id,
            keep_last: 0,
            reply: SharedReply::new(reply_tx),
        }).await.map_err(|error| format!("session_memory_unavailable: {error}"))?;
        reply_rx.await.map_err(|_| "session_memory_unavailable".to_string())?
    }

    pub async fn list_notifications(&self, unseen_only: bool) -> Result<Vec<isanagent::memory::NotificationRecord>, String> {
        let services = self.session_workspace_services().await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        services.memory_node.send_packet(MemoryMessage::ListNotifications {
            chat_id: None,
            channel: Some("stdio".to_string()),
            limit: 200,
            unseen_only,
            reply: SharedReply::new(reply_tx),
        }).await.map_err(|error| format!("inbox_memory_unavailable: {error}"))?;
        reply_rx.await.map_err(|_| "inbox_memory_unavailable".to_string())?
    }

    pub async fn mark_notification_seen(&self, notification_id: &str) -> Result<(), String> {
        self.mutate_notification(notification_id, false).await
    }

    pub async fn resolve_notification(&self, notification_id: &str) -> Result<(), String> {
        self.mutate_notification(notification_id, true).await
    }

    pub async fn list_mcp_servers(&self) -> Result<(Vec<altai_agent_service::mcp::McpServerConfig>, Vec<altai_agent_service::mcp::McpServerStatus>), String> {
        let workspace = PathBuf::from(self.session_workspace_root());
        Ok((altai_agent_service::mcp::load_servers(&workspace)?, self.mcp_statuses.snapshot(&workspace).await))
    }

    pub async fn configure_mcp_server(&self, server: altai_agent_service::mcp::McpServerConfig) -> Result<(), String> {
        altai_agent_service::mcp::validate_server(&server)?;
        let workspace = PathBuf::from(self.session_workspace_root());
        let mut servers = altai_agent_service::mcp::load_servers(&workspace)?;
        if let Some(existing) = servers.iter_mut().find(|existing| existing.id == server.id) { *existing = server.clone(); } else { servers.push(server.clone()); }
        altai_agent_service::mcp::save_servers(&workspace, &servers)?;
        self.mcp_statuses.clear_server(&workspace, &server.id).await;
        Ok(())
    }

    pub async fn set_mcp_server_enabled(&self, id: &str, enabled: bool) -> Result<(), String> {
        let workspace = PathBuf::from(self.session_workspace_root());
        let mut servers = altai_agent_service::mcp::load_servers(&workspace)?;
        let server = servers.iter_mut().find(|server| server.id == id).ok_or_else(|| "mcp_server_not_found".to_string())?;
        server.enabled = enabled;
        altai_agent_service::mcp::save_servers(&workspace, &servers)?;
        self.mcp_statuses.clear_server(&workspace, id).await;
        Ok(())
    }

    pub async fn restart_mcp_server(&self, id: &str) -> Result<altai_agent_service::mcp::McpProbeResult, String> {
        let workspace = PathBuf::from(self.session_workspace_root());
        let server = altai_agent_service::mcp::load_servers(&workspace)?.into_iter().find(|server| server.id == id).ok_or_else(|| "mcp_server_not_found".to_string())?;
        if !server.enabled { return Err("mcp_server_disabled".to_string()); }
        self.mcp_statuses.clear_server(&workspace, id).await;
        match altai_agent_service::mcp::probe_server(&server, &workspace).await {
            Ok(result) => { self.mcp_statuses.set(&workspace, altai_agent_service::mcp::McpServerStatus { server_id: server.id, state: altai_agent_service::mcp::McpState::Connected, tool_count: Some(result.tools.len()), last_error: None, updated_at_ms: 0 }).await; Ok(result) }
            Err(error) => { self.mcp_statuses.set(&workspace, altai_agent_service::mcp::McpServerStatus { server_id: server.id, state: altai_agent_service::mcp::McpState::Error, tool_count: None, last_error: Some(error.clone()), updated_at_ms: 0 }).await; Err(error) }
        }
    }

    async fn mutate_notification(&self, notification_id: &str, resolve: bool) -> Result<(), String> {
        if notification_id.trim().is_empty() || notification_id.len() > 512 { return Err("invalid_notification_id".to_string()); }
        let services = self.session_workspace_services().await?;
        let records = self.list_notifications(false).await?;
        if !records.iter().any(|record| record.notification_id == notification_id && record.channel == "stdio") { return Err("notification_not_found".to_string()); }
        let (reply_tx, reply_rx) = oneshot::channel();
        let message = if resolve { MemoryMessage::ResolveNotification { notification_id: notification_id.to_string(), reply: SharedReply::new(reply_tx) } } else { MemoryMessage::MarkNotificationSeen { notification_id: notification_id.to_string(), reply: SharedReply::new(reply_tx) } };
        services.memory_node.send_packet(message).await.map_err(|error| format!("inbox_memory_unavailable: {error}"))?;
        reply_rx.await.map_err(|_| "inbox_memory_unavailable".to_string())?
    }

    /// List only stdio-owned automations. Scheduler credentials and webhook
    /// tokens never cross this boundary.
    pub async fn list_automations(&self) -> Result<Vec<StdioAutomation>, String> {
        let services = self.session_workspace_services().await?;
        let store = Self::automation_store(&self.session_workspace_root())?;
        let mut jobs = store
            .load_jobs()?
            .into_iter()
            .filter(|job| job.channel == "stdio" && job.id.starts_with("altai:"))
            .map(|job| {
                let payload = decode_automation_payload(&job.message);
                StdioAutomation {
                    id: job.id,
                    chat_id: job.chat_id,
                    title: payload
                        .as_ref()
                        .map(|value| value.title.clone())
                        .unwrap_or_else(|| job.message.clone()),
                    prompt: payload
                        .map(|value| value.prompt)
                        .unwrap_or(job.message),
                    schedule: job.schedule,
                    enabled: job.enabled,
                }
            })
            .collect::<Vec<_>>();
        // Force construction of the workspace services before returning. This
        // keeps the scheduler actor alive for subsequent mutation commands.
        let _ = services;
        jobs.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(jobs)
    }

    pub async fn create_automation(
        &self,
        chat_id: &str,
        title: &str,
        prompt: &str,
        schedule: ScheduleKind,
    ) -> Result<StdioAutomation, String> {
        validate_automation_chat_id(chat_id)?;
        let title = validate_automation_title(title)?;
        let prompt = validate_automation_prompt(prompt)?;
        validate_automation_schedule(&schedule)?;
        let services = self.session_workspace_services().await?;
        let id = format!("altai:{}", uuid::Uuid::new_v4());
        let message = encode_automation_payload(&title, &prompt)?;
        services
            .cron
            .node
            .send_packet(
                serde_json::to_string(&CronCommand::Add {
                    id: id.clone(),
                    schedule: schedule.clone(),
                    message,
                    chat_id: chat_id.to_string(),
                    channel: "stdio".to_string(),
                })
                .map_err(|error| format!("automation_command_encode_failed: {error}"))?,
            )
            .await
            .map_err(|error| format!("automation_command_unavailable: {error}"))?;
        Ok(StdioAutomation {
            id,
            chat_id: chat_id.to_string(),
            title,
            prompt,
            schedule,
            enabled: true,
        })
    }

    pub async fn update_automation(
        &self,
        automation_id: &str,
        title: Option<&str>,
        prompt: Option<&str>,
        schedule: Option<ScheduleKind>,
        enabled: Option<bool>,
    ) -> Result<StdioAutomation, String> {
        let existing = self.find_automation(automation_id).await?;
        let title = match title {
            Some(value) => validate_automation_title(value)?,
            None => existing.title.clone(),
        };
        let prompt = match prompt {
            Some(value) => validate_automation_prompt(value)?,
            None => existing.prompt.clone(),
        };
        if let Some(schedule) = schedule.as_ref() {
            validate_automation_schedule(schedule)?;
        }
        let services = self.session_workspace_services().await?;
        services
            .cron
            .node
            .send_packet(
                serde_json::to_string(&CronCommand::Update {
                    id: existing.id.clone(),
                    schedule: schedule.clone(),
                    message: if title != existing.title || prompt != existing.prompt {
                        Some(encode_automation_payload(&title, &prompt)?)
                    } else {
                        None
                    },
                    enabled,
                })
                .map_err(|error| format!("automation_command_encode_failed: {error}"))?,
            )
            .await
            .map_err(|error| format!("automation_command_unavailable: {error}"))?;
        Ok(StdioAutomation {
            id: existing.id,
            chat_id: existing.chat_id,
            title,
            prompt,
            schedule: schedule.unwrap_or(existing.schedule),
            enabled: enabled.unwrap_or(existing.enabled),
        })
    }

    pub async fn trigger_automation(&self, automation_id: &str) -> Result<(), String> {
        let automation = self.find_automation(automation_id).await?;
        if !automation.enabled {
            return Err("automation_paused".to_string());
        }
        self.send_automation_command(CronCommand::Trigger { id: automation.id })
            .await
    }

    pub async fn pause_automation(&self, automation_id: &str) -> Result<(), String> {
        self.update_automation(automation_id, None, None, None, Some(false))
            .await
            .map(|_| ())
    }

    pub async fn delete_automation(&self, automation_id: &str) -> Result<(), String> {
        let automation = self.find_automation(automation_id).await?;
        self.send_automation_command(CronCommand::Remove { id: automation.id })
            .await
    }

    async fn find_automation(&self, automation_id: &str) -> Result<StdioAutomation, String> {
        validate_automation_id(automation_id)?;
        self.list_automations()
            .await?
            .into_iter()
            .find(|automation| automation.id == automation_id)
            .ok_or_else(|| "automation_not_found".to_string())
    }

    async fn send_automation_command(&self, command: CronCommand) -> Result<(), String> {
        let services = self.session_workspace_services().await?;
        services
            .cron
            .node
            .send_packet(
                serde_json::to_string(&command)
                    .map_err(|error| format!("automation_command_encode_failed: {error}"))?,
            )
            .await
            .map_err(|error| format!("automation_command_unavailable: {error}"))
    }

    fn automation_store(workspace_root: &str) -> Result<CronStore, String> {
        let root = resolve_workspace_root(Some(workspace_root));
        let database = root.join(".system_generated").join("agent_memory.db");
        CronStore::new(
            database
                .to_str()
                .ok_or("automation_database_path_invalid")?,
        )
    }

    fn session_workspace_root(&self) -> String {
        format!(
            "{}/.isanagent",
            self.workspace.root.to_string_lossy().trim_end_matches('/')
        )
    }

    async fn session_workspace_services(&self) -> Result<Arc<StdioWorkspaceServices>, String> {
        self.workspace_bundle_inner(&self.session_workspace_root())
            .await
    }

    async fn workspace_bundle_inner(
        &self,
        workspace_root: &str,
    ) -> Result<Arc<StdioWorkspaceServices>, String> {
        let fallback = self.workspace.root.to_string_lossy();
        let ws_opt = if workspace_root.is_empty() {
            Some(fallback.as_ref())
        } else {
            Some(workspace_root)
        };
        let dir = resolve_workspace_root(ws_opt);
        let service_key = dir.to_string_lossy().to_string();
        let mut guard = self.workspace_services_by_root.lock().await;
        if let Some(existing) = guard.get(&service_key) {
            return Ok(existing.clone());
        }
        let shared = if dir == self.session_shared.root() {
            self.session_shared.clone()
        } else {
            Arc::new(
                SharedWorkspaceServices::open(&dir).map_err(|error| {
                    format!("Failed to initialize workspace services: {error}")
                })?,
            )
        };
        let db_path = shared.memory_db_path();
        let db_path_str = db_path
            .to_str()
            .ok_or("workspace DB path is not valid UTF-8")?;
        let event_journal = shared.event_journal();
        let memory_actor = isanagent::memory::SqliteMemoryActor::new(db_path_str)
            .map_err(|e| format!("Failed to initialize SqliteMemoryActor: {e}"))?;
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
            .name("altai-stdio-logger".to_string())
            .spawn(move || {
                while let Ok(message) = logger_rx.recv() {
                    // Exit when the runtime can no longer dispatch — otherwise
                    // this thread would spin forever after serve shutdown.
                    let send = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime_handle.block_on(logger_forward.send_packet(message))
                    }));
                    match send {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) | Err(_) => break,
                    }
                }
            })
            .map_err(|error| format!("Failed to start workspace logger forwarder: {error}"))?;

        let (cron_bus_tx, mut cron_bus_rx) = mpsc::channel::<BusMessage>(100);
        let cron_logic = CronActor::new(
            "AltaiStdioCron",
            db_path_str,
            logger_handle.clone(),
            CronSchedulingMode::Local,
            cron_bus_tx,
        )
        .map_err(|error| format!("Failed to initialize workspace cron actor: {error}"))?;
        let cron_node = NodeHandle::new(cron_logic, 100, 1, Duration::from_millis(50));
        let cron_routes = self.cron_routes.clone();
        let cron_forwarder = tokio::spawn(async move {
            while let Some(message) = cron_bus_rx.recv().await {
                let BusMessage::Inbound(mut inbound) = message else {
                    continue;
                };
                if inbound.channel != "stdio" || inbound.thread_id.is_some() || inbound.chat_id.trim().is_empty() {
                    continue;
                }
                // ALTAI's native automation payload retains the user-facing
                // title separately from the instruction. Only the instruction
                // is delivered into the agent conversation.
                if let Some(payload) = decode_automation_payload(&inbound.content) {
                    inbound.content = payload.prompt;
                }
                let route = cron_routes.lock().await.get(&inbound.chat_id).cloned();
                if let Some(route) = route {
                    let _ = route.send(BusMessage::Inbound(inbound)).await;
                }
            }
        });

        let services = Arc::new(StdioWorkspaceServices {
            _shared: shared,
            memory_node: node,
            event_journal,
            clarification_hub: ClarificationHub::shared(),
            logger: WorkspaceLogger {
                handle: logger_handle,
                node: logger_node,
                forwarder,
            },
            cron: WorkspaceCron {
                node: cron_node,
                forwarder: cron_forwarder,
            },
        });
        guard.insert(service_key, services.clone());
        Ok(services)
    }
}

fn encode_automation_payload(title: &str, prompt: &str) -> Result<String, String> {
    serde_json::to_string(&AutomationPayload {
        altai_automation: 1,
        title: title.to_string(),
        prompt: prompt.to_string(),
    })
    .map_err(|error| format!("automation_payload_encode_failed: {error}"))
}

fn decode_automation_payload(message: &str) -> Option<AutomationPayload> {
    let payload = serde_json::from_str::<AutomationPayload>(message).ok()?;
    (payload.altai_automation == 1 && !payload.title.trim().is_empty() && !payload.prompt.trim().is_empty())
        .then_some(payload)
}

fn validate_automation_id(id: &str) -> Result<(), String> {
    if id.starts_with("altai:") && id.len() <= 512 {
        Ok(())
    } else {
        Err("invalid_automation_id".to_string())
    }
}

fn validate_automation_chat_id(chat_id: &str) -> Result<(), String> {
    if !chat_id.trim().is_empty() && chat_id.len() <= 256 && !chat_id.contains(':') {
        Ok(())
    } else {
        Err("invalid_automation_chat_id".to_string())
    }
}

fn validate_automation_title(title: &str) -> Result<String, String> {
    let title = title.trim();
    if title.is_empty() || title.len() > 256 {
        Err("invalid_automation_title".to_string())
    } else {
        Ok(title.to_string())
    }
}

fn validate_automation_prompt(prompt: &str) -> Result<String, String> {
    let prompt = prompt.trim();
    if prompt.is_empty() || prompt.len() > 10_000 {
        Err("invalid_automation_prompt".to_string())
    } else {
        Ok(prompt.to_string())
    }
}

fn validate_automation_schedule(schedule: &ScheduleKind) -> Result<(), String> {
    match schedule {
        ScheduleKind::At { at_ms } if *at_ms <= Utc::now().timestamp_millis() => {
            Err("automation_schedule_must_be_future".to_string())
        }
        ScheduleKind::Every { every_ms } if *every_ms < 60_000 => {
            Err("automation_interval_too_short".to_string())
        }
        ScheduleKind::Every { every_ms } if *every_ms > 366 * 24 * 60 * 60 * 1_000 => {
            Err("automation_interval_too_long".to_string())
        }
        ScheduleKind::Cron { .. } => Err("automation_cron_not_supported".to_string()),
        _ => Ok(()),
    }
}

#[async_trait]
impl HostAdapter for StdioHost {
    type Channel = ServiceChannel;

    fn channel_owner_id(channel: &Self::Channel) -> &str {
        channel.owner_id()
    }

    fn event_sink(&self) -> Arc<dyn AgentEventSink> {
        self.event_sink.clone()
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
        let fallback = self.workspace.root.to_string_lossy();
        let keep_root = resolve_workspace_root(Some(if keep_root.is_empty() {
            fallback.as_ref()
        } else {
            keep_root
        }))
            .to_string_lossy()
            .to_string();
        self.workspace_services_by_root
            .lock()
            .await
            .retain(|k, _| k == &keep_root);
    }

    async fn on_chat_bound(
        &self,
        _workspace_root: &str,
        chat_id: &str,
        _owner_id: &str,
        bus_tx: mpsc::Sender<BusMessage>,
        _is_first_bind: bool,
    ) {
        self.cron_routes.lock().await.insert(chat_id.to_string(), bus_tx);
    }

    async fn build_instance(
        &self,
        request: BuildInstanceRequest<'_>,
    ) -> Result<BuiltInstance<Self::Channel>, String> {
        #[cfg(debug_assertions)]
        let scripted = std::env::var("ALTAI_CLI_TEST_SCRIPTED_RESPONSE")
            .ok()
            .map(|response| vec![response]);
        #[cfg(not(debug_assertions))]
        let scripted = None;
        let checkpoint_root = dirs_checkpoint_root();
        build_shared_instance(
            self,
            request,
            SharedInstanceHooks {
                checkpoint_root,
                scripted_responses: scripted,
                channel_name: "stdio",
                suppress_agent_cron: false,
            },
        )
        .await
    }

    async fn clear_mcp_workspaces(&self, workspace_roots: &[String]) { for root in workspace_roots { if !root.is_empty() { self.mcp_statuses.clear_workspace(Path::new(root)).await; } } }

    async fn augment_tools(&self, sandbox_dir: &Path, tools: &mut ToolRegistry) -> Result<(), String> { altai_agent_service::mcp::register_enabled_tools(sandbox_dir, tools, &self.mcp_statuses).await }
}

fn dirs_checkpoint_root() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)?;
    let root = home.join(".altai").join("checkpoints");
    if let Err(error) = std::fs::create_dir_all(&root) {
        log::warn!("checkpoint: failed to create checkpoint directory: {error}");
        return None;
    }
    Some(root)
}

#[allow(dead_code)]
fn _path_marker(_: &Path) {}

#[cfg(test)]
mod tests {
    use super::StdioHost;
    use altai_agent_service::{
        AgentEventEnvelope, AgentEventSink, AgentEventSinkError, RunCoordinator,
        WorkspaceServices,
    };
    use altai_core::{resolve_workspace_from, JournalEvent};
    use serde_json::json;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    struct AcceptingSink;

    impl AgentEventSink for AcceptingSink {
        fn try_send(&self, _event: AgentEventEnvelope) -> Result<(), AgentEventSinkError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn canonical_workspace_cache_never_restart_classifies_a_live_process_run() {
        let temporary = tempfile::tempdir().expect("workspace");
        let workspace = resolve_workspace_from(Some(temporary.path()), Path::new("/unused"))
            .expect("workspace paths");
        let shared = Arc::new(
            WorkspaceServices::open(&workspace.isanagent_state).expect("workspace service"),
        );
        let startup = shared.event_journal();
        let host = StdioHost::new(
            workspace.clone(),
            Arc::new(AcceptingSink),
            Arc::new(Mutex::new(RunCoordinator::default())),
            shared,
        );

        startup
            .append(&JournalEvent::now(
                1,
                "run-live",
                1,
                "chat-live",
                "run_started",
                json!({"type":"run_started","run_id":"run-live"}),
            ))
            .expect("incomplete same-process run");

        let runtime_services = host
            .workspace_bundle_inner(workspace.isanagent_state.to_str().expect("UTF-8 root"))
            .await
            .expect("runtime services");
        assert!(Arc::ptr_eq(&startup, &runtime_services.event_journal));
        assert_eq!(
            startup
                .run_summary("run-live")
                .expect("run summary")
                .expect("live run")
                .terminal_seq,
            None,
            "opening the runtime route must not classify a same-process run as abandoned"
        );
    }
}
