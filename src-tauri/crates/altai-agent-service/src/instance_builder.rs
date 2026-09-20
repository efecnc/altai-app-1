//! Tauri-free IsanAgent instance construction shared by Desktop and stdio hosts.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use isanagent::agent::{AgentLogic, AgentLogicParams};
use isanagent::bus::BusMessage;
use isanagent::channels::Channel;
use isanagent::clarification::ClarificationHub;
use isanagent::provider;
use isanagent::session::SessionManager;
use isanagent::skills::SkillRegistry;
use isanagent::tools::builtin::{
    CronTool, EditFileTool, FetchMemoryByDateTool, GitWorktreeTool, GlobFilesTool, ListDirTool,
    ReadFileTool, SearchMemoryTool, SearchTextTool, ShellExecTool, WebFetchTool, WebSearchTool,
    WriteFileTool,
};
use isanagent::tools::ml_domain::{ArxivFetchTool, ArxivSearchTool, HfHubFileFetchTool};
use isanagent::tools::workflow::{AskUserTool, TodoWriteTool, ToolSearchTool};
use isanagent::tools::ToolRegistry;
use isanagent::workspace::{resolve_workspace_root, IsanagentWorkspace};
use isanagent::NodeHandle;
use tokio::sync::mpsc;

use crate::channel::ServiceChannel;
use crate::host::{BuildInstanceRequest, BuiltInstance, HostAdapter, WorkspaceBundle};

/// Host knobs that replace Desktop AppHandle seams inside the shared builder.
pub struct SharedInstanceHooks {
    pub checkpoint_root: Option<PathBuf>,
    pub scripted_responses: Option<Vec<String>>,
    /// `"tauri"` or `"stdio"`.
    pub channel_name: &'static str,
    /// Scheduling cutover (CP-08-108): withhold the agent-facing `cron` tool
    /// because canonical scheduling owns this workspace — an automation
    /// created through it would be suppressed at fire time and never mapped.
    pub suppress_agent_cron: bool,
}

impl Default for SharedInstanceHooks {
    fn default() -> Self {
        Self {
            checkpoint_root: None,
            scripted_responses: None,
            channel_name: "tauri",
            suppress_agent_cron: false,
        }
    }
}

pub async fn build_shared_instance<H>(
    host: &H,
    request: BuildInstanceRequest<'_>,
    hooks: SharedInstanceHooks,
) -> Result<BuiltInstance<ServiceChannel>, String>
where
    H: HostAdapter,
{
    let BuildInstanceRequest {
        workspace,
        run_coordinator,
        event_sink,
        provider_name,
        api_key,
        model_name,
        persona_instructions,
        base_url_override,
        workspace_root,
        permission_mode,
        compaction,
        fallback,
    } = request;
    let WorkspaceBundle {
        memory_node,
        clarification_hub,
        logger_handle,
        cron_node,
        event_journal,
    } = workspace;

    let owner_id = uuid::Uuid::new_v4().to_string();
    let channel = Arc::new(match hooks.channel_name {
        "stdio" => ServiceChannel::stdio(
            event_sink.clone(),
            uuid::Uuid::new_v4().to_string(),
            owner_id.clone(),
            run_coordinator.clone(),
            event_journal.clone(),
        ),
        _ => ServiceChannel::tauri(
            event_sink.clone(),
            uuid::Uuid::new_v4().to_string(),
            owner_id.clone(),
            run_coordinator.clone(),
            event_journal.clone(),
        ),
    });

// Resolve workspace — `<selected-folder>/.isanagent`, or `~/.isanagent`.
    let workspace_dir = resolve_workspace_root(workspace_root);
    if !workspace_dir.exists() {
        // Auto-create minimal workspace
        std::fs::create_dir_all(workspace_dir.join(".system_generated")).map_err(|error| {
            format!("Failed to create workspace directory {}: {error}", workspace_dir.display())
        })?;
    }

    let workspace = IsanagentWorkspace::new(workspace_root, None)
        .map_err(|e| format!("Failed to load IsanAgent workspace: {}", e))?;

    // Memory (SQLite) is the shared per-workspace actor passed in by
    // `ensure_instance` — one actor per project, reused across this
    // workspace's model-instances so history transfers and DB access is
    // serialized through a single actor (no contention).
    let session_manager = SessionManager::new(memory_node.clone());
    let mut skills = SkillRegistry::new(workspace.skills_path());
    // Agent Plugins 1.0 discovery must use the actual project root in embedded
    // hosts. `workspace.dir` is ALTAI's state directory (`<project>/.isanagent`),
    // while plugin roots live beside it (`<project>/.agents/plugins`,
    // `<project>/.isanagent/plugins`, and `<project>/plugins`).
    let plugin_workspace_root = workspace_dir
        .parent()
        .map(|path| path.to_path_buf())
        .unwrap_or_else(|| workspace.dir.clone());
    let global_plugin_root = resolve_workspace_root(None);
    let plugins = isanagent::plugins::PluginRegistry::discover(
        &plugin_workspace_root,
        Some(&global_plugin_root),
    );
    plugins.populate_skill_registry(&mut skills);
    // Outbound channel for agent → UI (typed as BusMessage per IsanAgent API)
    let (global_outbound_tx, mut global_outbound_rx) = mpsc::channel::<BusMessage>(100);
    // Inbound bus
    let (bus_tx, mut bus_rx) = mpsc::channel::<BusMessage>(100);

    // Tools
    let mut tools = ToolRegistry::new();
    let restrict = workspace.config.restrict_to_workspace.unwrap_or(true);
    // Sandbox root is the selected project folder (the parent of `.isanagent`),
    // matching the industry-standard pattern used by Claude Code, Codex CLI, and
    // Cline: the agent operates on the project root, NOT a nested
    // `.isanagent/workspace` subfolder. `workspace.sandbox_dir` resolves to that
    // nested folder (isanagent crate default), so we override it here. We fall
    // back to the crate default only when the parent can't be resolved (e.g. the
    // `~/.isanagent` default with no project selected).
    let sandbox_dir = workspace_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| workspace.sandbox_dir.clone());

    tools.register(Box::new(ReadFileTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
    }));
    tools.register(Box::new(WriteFileTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
    }));
    tools.register(Box::new(EditFileTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
    }));
    tools.register(Box::new(ListDirTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
    }));
    tools.register(Box::new(GlobFilesTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
    }));
    tools.register(Box::new(SearchTextTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
        ripgrep_timeout_secs: workspace
            .config
            .effective_search_text_ripgrep_timeout_secs(),
    }));
    // Host `exec` jobs emit synthetic completion messages. Route those onto
    // the instance's inbound bus so the run coordinator can assign a trusted
    // run ID and wake the owning chat; the outbound bus intentionally ignores
    // inbound messages.
    let exec_jobs = isanagent::tools::exec_jobs::ExecJobRegistry::new(Some(bus_tx.clone()));
    tools.register(Box::new(ShellExecTool {
        workspace_dir: sandbox_dir.clone(),
        restrict_to_workspace: restrict,
        exec_jobs: Some(exec_jobs.clone()),
        windows_runner: workspace.config.windows_shell_runner(),
    }));
    tools.register(Box::new(isanagent::tools::builtin::ExecStatusTool {
        exec_jobs: exec_jobs.clone(),
    }));
    tools.register(Box::new(isanagent::tools::builtin::ExecSendTool {
        exec_jobs,
    }));
    if workspace.config.git_worktree_tool_enabled() {
        tools.register(Box::new(GitWorktreeTool {
            workspace_dir: sandbox_dir.clone(),
            restrict_to_workspace: restrict,
            allow_path_outside_sandbox: workspace.config.git_worktree_allow_path_outside_sandbox(),
        }));
    }

    // Pre-edit checkpoints for one-step undo of agent edits (isanagent
    // #53/#56). WriteFileTool/EditFileTool snapshot a file's prior content
    // before mutating it; the `checkpoint` tool (and the `checkpoint_*` Tauri
    // commands) roll them back. isanagent's store is process-global and
    // set-once, while this runtime is rebuilt per workspace — so we root it at
    // an APP-level directory (not the workspace) and restore by absolute path
    // (`base = None`), which stays correct across workspace switches. Restores
    // are safe here because every checkpoint is created by our own sandboxed
    // edit tools. Trade-off: `base = None` forgoes isanagent's symlink/TOCTOU
    // restore guard (which only applies when a sandbox `base` is set) — an
    // acceptable choice since restores act only on agent-authored snapshots of
    // already sandbox-confined paths, and a single set-once `base` could not
    // stay correct once the workspace changes. Enabled by default; opt out with
    // `checkpoint_enabled = false` in `<workspace>/.isanagent/config.toml`.
    if workspace.config.checkpoint_enabled.unwrap_or(true) {
        // `init` sets a process-global set-once `OnceLock`. build_instance runs
        // again on every workspace/model switch, so only initialize when the
        // store isn't already up — a second `init` would allocate a throwaway
        // store the OnceLock silently drops. The app-level root means the first
        // init stays correct for the whole session regardless of workspace.
        if isanagent::checkpoint::store().is_none() {
            if let Some(root) = hooks.checkpoint_root.clone() {
                isanagent::checkpoint::init(root, None);
            } else {
                log::warn!("checkpoint: no checkpoint root configured; edit undo disabled");
            }
        }
        // Register the tool on every runtime build (each gets a fresh
        // ToolRegistry), but only while the store is actually active.
        if isanagent::checkpoint::store().is_some() {
            tools.register(Box::new(isanagent::checkpoint::CheckpointTool));
        }
    }

    // ML domain tools
    let max_web_chars = workspace.config.effective_max_web_tool_output_chars();
    let jina = workspace.config.jina_web_backend();
    tools.register(Box::new(WebSearchTool {
        jina: jina.clone(),
        max_output_chars: max_web_chars,
    }));
    tools.register(Box::new(WebFetchTool {
        jina,
        max_output_chars: max_web_chars,
        workspace_dir: workspace.dir.clone(),
    }));
    tools.register(Box::new(ArxivSearchTool {
        max_output_chars: max_web_chars,
    }));
    tools.register(Box::new(ArxivFetchTool {
        workspace_dir: workspace.dir.clone(),
    }));
    tools.register(Box::new(HfHubFileFetchTool {
        max_output_chars: max_web_chars,
    }));
    register_existing_claw_tools(
        &mut tools,
        memory_node.clone(),
        clarification_hub.clone(),
        global_outbound_tx.clone(),
    );
    let cron_db_path = workspace_dir
        .join(".system_generated")
        .join("agent_memory.db")
        .to_string_lossy()
        .to_string();
    // CronTool binds its destination to IsanAgent's trusted ToolExecCtx
    // (#67), while the actor itself is shared at workspace scope above.
    register_cron_tool(&mut tools, &hooks, cron_node, cron_db_path);
    tools.register(Box::new(TodoWriteTool {
        memory_node: memory_node.clone(),
    }));

    host.augment_tools(&sandbox_dir, &mut tools).await?;

    // Compaction overhaul (upstream isanagent — altaidevorg/isanagent#39). The agent can
    // now schedule a between-turns context compaction via `compact_context`
    // and re-fetch a tool result that fell out of the live context via
    // `recall_tool_result`. Both surface in the chat as their own tool
    // entries (TOOL_META in tool.tsx) so the user can see when compaction
    // ran and what got recalled.
    tools.register(Box::new(isanagent::tools::compact::CompactContextTool {
        outbound_tx: global_outbound_tx.clone(),
    }));
    tools.register(Box::new(isanagent::tools::recall::RecallToolResultTool {
        memory_node: memory_node.clone(),
        outbound_tx: global_outbound_tx.clone(),
        spill_store: Some(isanagent::spill::SpillStore::new(&workspace.dir)),
    }));

    // Execution harness (if enabled)
    if workspace.config.execution_harness_enabled() {
        let harness = isanagent::execution::build_execution_harness(
            workspace.dir.clone(),
            sandbox_dir.clone(),
            restrict,
            &workspace.config,
        )
        .map_err(|e| format!("execution harness: {e}"))?;

        let execution_jobs = Arc::new(isanagent::execution::ExecutionJobManager::new(
            harness.clone(),
            global_outbound_tx.clone(),
            Some(bus_tx.clone()),
            workspace.config.execution_wake_on_job_terminal(),
        ));
        let inflight_sync = Arc::new(isanagent::execution::InflightSyncRegistry::new());

        tools.register(Box::new(
            isanagent::tools::execution::ExecutionSessionCreateTool {
                harness: harness.clone(),
            },
        ));
        tools.register(Box::new(isanagent::tools::execution::ExecutionRunTool {
            harness: harness.clone(),
            outbound_tx: global_outbound_tx.clone(),
            jobs: Some(execution_jobs.clone()),
            inflight: Some(inflight_sync.clone()),
        }));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionRunBackgroundTool {
                harness: harness.clone(),
                jobs: execution_jobs.clone(),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionJobStatusTool {
                jobs: execution_jobs.clone(),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionJobResultTool {
                jobs: execution_jobs.clone(),
                max_tool_output_chars: workspace
                    .config
                    .resolved_max_tool_output_chars()
                    .unwrap_or(3000),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionJobListTool {
                jobs: execution_jobs.clone(),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionJobCancelTool {
                jobs: execution_jobs.clone(),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionArtifactListTool {
                harness: harness.clone(),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionEnvInfoTool {
                harness: harness.clone(),
            },
        ));
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionSessionCloseTool {
                harness: harness.clone(),
            },
        ));
        tools.register(Box::new(isanagent::tools::execution::ExecutionCancelTool {
            harness: harness.clone(),
        }));

        // Read background-job stdout/stderr line-by-line — lets the agent
        // inspect a long-running job's logs without fetching the full result.
        tools.register(Box::new(
            isanagent::tools::execution::ExecutionReadLogTool {
                jobs: execution_jobs.clone(),
                harness: harness.clone(),
            },
        ));

        // (The colab_mcp extra-tool-call proxy was removed upstream in
        // isanagent #47 "Colab CLI" — ColabMcpToolCallTool /
        // compile_colab_mcp_tool_allowlist and the related config accessors no
        // longer exist, so there's nothing to register here.)
    }

    if workspace.config.kernel_porting_harness_enabled() {
        isanagent::tools::kernel_porting::register_kernel_porting_tools(
            &mut tools,
            sandbox_dir.clone(),
            Arc::new(workspace.config.clone()),
        );
    }

    // Plugin-provided stdio MCP servers are started during instance creation
    // and their advertised tools become ordinary IsanAgent tools. Resolve uv
    // through the same workspace config used by the execution harness.
    let uv_binary = workspace.config.execution_uv_binary();
    plugins
        .populate_tool_registry(&mut tools, &uv_binary)
        .await;

    // Register discovery last so its shared catalog contains every concrete
    // tool available to this instance. This mirrors IsanAgent's reference
    // binary and lets the model find opt-in MCP, execution, worktree, and
    // Claw-parity tools without duplicating a static catalogue in ALTAI.
    let tool_catalog = tools.catalog_handle();
    tools.register(Box::new(ToolSearchTool {
        catalog: tool_catalog,
    }));

    // Provider — `base_url_override` (from the JS side, derived from the
    // active model) wins. Otherwise fall back to workspace config, then
    // to Gemini's `v1beta` as a last resort.
    //
    // Note: `cfg.resolved_base_url()` has shifted between `Option<String>`
    // and `Result<String, String>` across isanagent revisions. `.unwrap_or`
    // is defined on both, so this branch survives that drift without
    // pinning the crate.
    let resolved_base_url = if let Some(override_url) = base_url_override {
        override_url.to_string()
    } else {
        // Gemini's OpenAI-compatible chat-completions endpoint. The runtime
        // POSTs to `base_url` as-is (no path appended), so this must be the
        // *full* endpoint — `…/v1beta` alone would 404.
        let default =
            "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions".to_string();
        if let Some(ref cfg) = workspace.config.provider {
            cfg.resolved_base_url().unwrap_or(default)
        } else {
            default
        }
    };
    let llm_provider = match hooks.scripted_responses.clone() {
        Some(responses) => {
            // ScriptedProvider implements the crate's Provider trait; create_provider
            // returns the same boxed trait object used by AgentLogicParams.
            let _ = (provider_name, api_key, model_name);
            Box::new(isanagent::provider::ScriptedProvider::new(responses))
        }
        None => provider::create_provider(provider_name, &resolved_base_url, api_key, model_name),
    };
    let provider_credentials = isanagent::provider::ProviderCredentials {
        provider_name: provider_name.to_string(),
        base_url: resolved_base_url.clone(),
        api_key: api_key.to_string(),
        model_name: model_name.to_string(),
    };
    let fallback_providers = fallback
        .cloned()
        .map(|fallback| {
            isanagent::agent::build_fallback_specs(
                provider_name,
                &resolved_base_url,
                model_name,
                vec![fallback],
            )
        })
        .unwrap_or_default();
    // System prompt
    let mut system_prompt = workspace.compile_system_prompt();
    let plugin_overlays = plugins.compile_overlay_prompts();
    if !plugin_overlays.is_empty() {
        system_prompt.push_str(&plugin_overlays);
    }
    if workspace.config.ml_engineer_harness_enabled() {
        system_prompt.push_str("\n\n");
        system_prompt.push_str(isanagent::ml_engineer::HARNESS_OVERLAY);
    }
    if let Some(persona) = persona_instructions {
        system_prompt.push_str("\n\n## Persona\n\n");
        system_prompt.push_str(persona);
    }

    // Subagent prompt/summary fields are derived from the system prompt as it
    // stands *before* the named-agent catalog is appended below — subagents do
    // not spawn nested subagents, so they don't need the catalog. This mirrors
    // the ordering in isanagent's reference binary (src/main.rs).
    let subagent_system_prompt = if workspace.config.ml_engineer_subagent_research_overlay() {
        format!(
            "{}\n{}",
            system_prompt,
            isanagent::ml_engineer::SUBAGENT_RESEARCH_APPEND
        )
    } else {
        system_prompt.clone()
    };
    let harness_runtime_summary = workspace.config.runtime_harness_summary_lines().join("\n");
    let forbid_final_without_tools = workspace.config.ml_engineer_forbid_final_without_tools();

    // Named-agent registry (researcher / coder / evaluator, plus any defined in
    // `.isanagent/config.toml` under `[harness.agents.*]`). Fall back to the
    // crate's built-in defaults when none are configured. The catalog is
    // injected into the main agent's system prompt so the LLM knows which
    // specialized agents it can dispatch via `subagent_spawn`.
    let agent_defs = {
        let defs = workspace.config.agent_definitions();
        if defs.is_empty() {
            isanagent::agent::registry::default_agent_definitions()
        } else {
            defs
        }
    };
    let mut agent_registry = isanagent::agent::AgentRegistry::from_definitions(
        &agent_defs,
        &sandbox_dir,
    );
    plugins.populate_agent_registry(&mut agent_registry);
    let agent_registry = Arc::new(agent_registry);
    let agent_prompt_section = agent_registry.compile_agent_prompt_section();
    if !agent_prompt_section.is_empty() {
        system_prompt.push_str(&agent_prompt_section);
    }

    // Build the subagent harness params only when enabled in config
    // (`[harness.subagents] enabled = true`). When disabled, no subagent tools
    // are registered and no spawn can happen — but note this is *not* a total
    // no-op vs. the pre-subagent runtime: `harness_runtime_summary` (a per-step
    // harness snapshot) and the named-agent catalog are now always built into
    // the prompt regardless of this flag, matching isanagent's reference binary.
    // Subagent lifecycle telemetry (SubagentSpawned / SubagentFinished) is
    // emitted on `outbound_tx` and surfaced to the UI by the outbound router
    // below; wake-on-completion follow-ups ride `bus_tx`.
    let subagent = if workspace.config.subagent_harness_enabled() {
        Some(isanagent::agent::SubagentHarnessParams {
            cancel_children_on_parent_cancel: workspace
                .config
                .subagent_cancel_children_on_parent_cancel(),
            allowed_tools: workspace.config.subagent_allowed_tools_set().map(Arc::new),
            max_tasks: workspace.config.subagent_max_tasks(),
            max_wait_secs: workspace.config.subagent_max_wait_secs(),
            agent_registry: Some(agent_registry),
            wake_on_completion: workspace.config.subagent_wake_on_completion(),
            task_history_retention: workspace.config.subagent_task_history_retention(),
            bus_tx: Some(bus_tx.clone()),
            workspace_dir: sandbox_dir.clone(),
        })
    } else {
        None
    };

    // Match isanagent onboarding default (999). Peer agents (Claude Code, Cursor,
    // Codex) do not stop healthy work at a low turn ceiling; 50 felt like a crash.
    let max_iterations = workspace.config.resolved_max_iterations().unwrap_or(999);
    let max_tool_output_chars = workspace
        .config
        .resolved_max_tool_output_chars()
        .unwrap_or(3000);
    // Resolve compaction knobs. The user-facing prefs (auto/thresholdTokens/
    // tailTurns) flow in from JS; when absent we keep the isanagent crate's
    // built-in defaults so direct CLI/canonical callers aren't affected.
    let (max_recent_summaries, short_term_threshold_turns, short_term_threshold_tokens) =
        match compaction {
            Some(c) => c.to_logic_params(),
            None => (5, 20, 100_000),
        };
    // Start from the on-disk shell policy, then let the active UI permission
    // mode override BOTH the interactive shell gate and the file-edit gate.
    //
    // The two surfaces are mapped independently (see `permission_mode_to_*_mode`)
    // because their risk profiles differ: "auto-edit" auto-applies file changes
    // but still prompts for shell commands, while "plan" blocks edits entirely
    // but lets read-only shell runs through with approval. Without overriding
    // `interactive_edit_mode` here, edits would always fall back to the on-disk
    // default (`Ask`) and the toolbar toggle would silently do nothing for the
    // edit surface — which was the core ITEM 1 wiring gap.
    let mut shell_policy = workspace.config.resolved_shell_policy();
    if let Some(mode) = crate::permission::permission_mode_to_shell_mode(permission_mode) {
        shell_policy.interactive_mode = mode;
    }
    if let Some(mode) = crate::permission::permission_mode_to_edit_mode(permission_mode) {
        shell_policy.interactive_edit_mode = mode;
        // `unattended_*_mode` only matters for autonomous/background sessions,
        // but keep it in lockstep with the interactive setting so a background
        // turn doesn't silently use the on-disk default (which is `Deny`) while
        // the user picked `auto-edit` in the toolbar.
        shell_policy.unattended_edit_mode = mode;
    }
    let default_harness = isanagent::config::HarnessConfig::default();
    let harness_ref = workspace
        .config
        .harness
        .as_ref()
        .unwrap_or(&default_harness);
    let hook_tool_ctx = isanagent::hooks::ToolCallHookContext::from_harness_config(
        &workspace.dir,
        &sandbox_dir,
        harness_ref,
    );

    let agent_logic = AgentLogic::new_with_fallback_providers(
        AgentLogicParams {
            name: "altai-agent".to_string(),
            provider: llm_provider,
            provider_credentials,
            session_manager,
            tools,
            skills,
            system_prompt,
            max_iterations,
            max_tool_output_chars,
            max_recent_summaries,
            short_term_threshold_turns,
            short_term_threshold_tokens,
            outbound_tx: global_outbound_tx.clone(),
            logger_tx: logger_handle,
            clarification_hub,
            subagent,
            doom_loop_enabled: workspace.config.doom_loop_enabled(),
            harness_runtime_summary,
            subagent_system_prompt,
            forbid_final_without_tools,
            shell_policy,
            hook_tool_ctx,
        },
        fallback_providers,
    );

    let agent_node = NodeHandle::<BusMessage>::new(agent_logic, 100, 3, Duration::from_millis(50));

    // Start the TauriChannel
    channel
        .start(bus_tx.clone())
        .await
        .map_err(|e| format!("ServiceChannel start failed: {e}"))?;

    // Bus router: forward inbound → agent, outbound → channel. (Telemetry is
    // emitted by the outbound router below, not here — see note in the loop.)
    let channel_for_outbound = channel.clone();
    // Shutdown trigger: `agent_node` (moved into this task) holds `bus_tx`
    // clones, so `channel.stop()` can't drop the last sender. On teardown we
    // fire `shutdown_tx`; the task breaks, drops `agent_node`, and the cycle
    // unwinds (its `global_outbound_tx` clones drop, ending the outbound task).
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let coordinator_for_bus = run_coordinator.clone();
    let owner_for_bus = owner_id.clone();
    let hooks_channel_name = hooks.channel_name;
    let bus_router = tokio::spawn(async move {
        loop {
            let msg = tokio::select! {
                m = bus_rx.recv() => m,
                _ = &mut shutdown_rx => break,
            };
            let Some(msg) = msg else { break };
            match msg {
                BusMessage::Inbound(mut inbound) => {
                    let generated_run_id =
                        if inbound.channel == hooks_channel_name && crate::delivery::inbound_run_id(&inbound).is_none() {
                            inbound = crate::delivery::trusted_inbound(inbound);
                            crate::delivery::inbound_run_id(&inbound).map(str::to_string)
                        } else {
                            None
                        };
                    let run_id_for_rollback = crate::delivery::inbound_run_id(&inbound).map(str::to_string);
                    if let Some(run_id) = generated_run_id.as_deref() {
                        if let Err(error) = crate::routing::queue_run(
                            &coordinator_for_bus,
                            &inbound.chat_id,
                            run_id,
                            &owner_for_bus,
                        ) {
                            log::warn!(
                                "Dropped internal synthetic inbound for chat {}: {}",
                                inbound.chat_id,
                                error
                            );
                            continue;
                        }
                    }
                    let chat_id = inbound.chat_id.clone();
                    let result = agent_node.send_packet(BusMessage::Inbound(inbound)).await;
                    if result.is_err() {
                        if let Some(run_id) = run_id_for_rollback.as_deref() {
                            crate::routing::rollback_run_admission(
                                &coordinator_for_bus,
                                &chat_id,
                                run_id,
                                &owner_for_bus,
                            );
                        }
                    }
                }
                BusMessage::Outbound(outbound) => {
                    let _ = channel_for_outbound.send(outbound).await;
                }
                // NOTE: telemetry is intentionally NOT handled here. Agent and
                // tool telemetry flows through `global_outbound_tx` (the
                // outbound router below) and is emitted there exactly once.
                // `bus_tx` only ever carries Inbound (user + synthetic
                // execution-job follow-ups) and Cancel, so handling Telemetry
                // here would be dead code today and a double-emit footgun if
                // anything later routed telemetry to this channel.
                BusMessage::Cancel(chat_id) => {
                    let _ =
                        crate::routing::coordinator_guard(&coordinator_for_bus).cancel_requested(&chat_id, None);
                    let _ = agent_node.send_packet(BusMessage::Cancel(chat_id)).await;
                }
                BusMessage::CancelRun { chat_id, run_id }
                    if crate::routing::coordinator_guard(&coordinator_for_bus)
                        .cancel_requested(&chat_id, Some(&run_id))
                        .is_ok() =>
                {
                    let _ = agent_node
                        .send_packet(BusMessage::CancelRun { chat_id, run_id })
                        .await;
                }
                BusMessage::Steer {
                    chat_id,
                    run_id,
                    content,
                } if crate::routing::coordinator_guard(&coordinator_for_bus)
                    .accepts_steer(&chat_id, &run_id, &owner_for_bus)
                    .is_ok() =>
                {
                    let _ = agent_node
                        .send_packet(BusMessage::Steer {
                            chat_id,
                            run_id,
                            content,
                        })
                        .await;
                }
                _ => {}
            }
        }
    });

    // Outbound router: forward everything the agent emits on its outbound
    // channel — final assistant messages AND telemetry (tool calls, thoughts,
    // progress). Previously this task only handled `Outbound`, so every
    // `BusMessage::Telemetry(...)` the AgentLogic emitted was silently
    // dropped — the UI saw no tool calls or thinking between "Sending to
    // ALTAI…" and the final answer.
    let sink_for_outbound = event_sink.clone();
    let coordinator_for_outbound = run_coordinator.clone();
    let journal_for_outbound = event_journal.clone();
    let owner_for_outbound = owner_id.clone();
    let outbound_router = tokio::spawn(async move {
        while let Some(out_msg) = global_outbound_rx.recv().await {
            match out_msg {
                BusMessage::Outbound(outbound) => {
                    // Clarifications (`ask_user`) ride on outbound metadata —
                    // surface them as a distinct event so the UI can render the
                    // preset choices as buttons. A normal reply resolves them.
                    //
                    // The crate's edit gate additionally attaches a structured
                    // `edit_diff` to the same outbound when the clarification is
                    // really a file-mutation approval. We extract it here so the
                    // frontend can render a diff-review card instead of the plain
                    // "approve / deny" chips — the reply path is identical.
                    let chat_id = outbound.chat_id.clone();
                    let is_clarification = outbound
                        .metadata
                        .contains_key(isanagent::clarification::METADATA_CLARIFICATION);
                    let edit_diff = outbound.metadata.get("edit_diff").and_then(crate::delivery::parse_edit_diff);
                    let event = if is_clarification {
                        let choices = outbound
                            .metadata
                            .get(isanagent::clarification::METADATA_CLARIFICATION_CHOICES)
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|x| x.as_str().map(str::to_string))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        crate::event::Event::Clarification {
                            content: outbound.content,
                            choices,
                            edit_diff,
                        }
                    } else {
                        crate::event::Event::AgentMessage {
                            content: outbound.content,
                            role: "assistant".to_string(),
                        }
                    };
                    let transition = crate::delivery::persist_and_deliver_run_event(
                        &coordinator_for_outbound,
                        &journal_for_outbound,
                        &chat_id,
                        &owner_for_outbound,
                        &event,
                        crate::delivery::RunEventTransition::Next,
                        |run, payload| {
                            // Waiting-user is runtime state, not renderer
                            // state. Commit it after durability even when the
                            // live window is gone and must replay the prompt.
                            if is_clarification {
                                if let Err(error) = crate::routing::coordinator_guard(&coordinator_for_outbound)
                                    .mark_waiting_user(&chat_id, &owner_for_outbound)
                                {
                                    log::warn!(
                                        "Could not mark clarification wait for chat {chat_id}: {error:?}"
                                    );
                                }
                            }
                            crate::delivery::emit_payload(sink_for_outbound.as_ref(), &chat_id, payload, Some(run.clone()))
                        },
                    );
                    match transition {
                        Ok(_) => {}
                        Err(error @ crate::delivery::RunEventDeliveryError::Renderer(_)) => {
                            log::warn!("Agent event for chat {chat_id} awaits replay: {error}")
                        }
                        Err(error) => {
                            log::warn!("Dropped outbound event for chat {chat_id}: {error}")
                        }
                    }
                }
                BusMessage::Telemetry(ref telemetry) => {
                    if let Some(event) = crate::event_map::map_telemetry_to_event(telemetry) {
                        let chat_id = crate::event_map::telemetry_chat_id(telemetry).unwrap_or("");
                        if crate::delivery::is_system_event(&event) {
                            if let Err(error) =
                                crate::delivery::emit_event(sink_for_outbound.as_ref(), chat_id, &event, None)
                            {
                                log::warn!("Could not deliver system event: {error}");
                            }
                        } else {
                            if let Err(error) = crate::delivery::deliver_next_run_event(
                                sink_for_outbound.as_ref(),
                                &journal_for_outbound,
                                &coordinator_for_outbound,
                                chat_id,
                                &owner_for_outbound,
                                &event,
                            ) {
                                log::warn!("Dropped telemetry event for chat {chat_id}: {error}");
                            }
                        }
                    }
                }
                BusMessage::RunLifecycle(lifecycle) => {
                    use isanagent::bus::RunLifecycleEvent;

                    let event = crate::event_map::map_lifecycle_to_event(&lifecycle);
                    match lifecycle {
                        RunLifecycleEvent::Started { run_id, chat_id } => {
                            let transition = crate::delivery::persist_and_deliver_to_renderer(
                                sink_for_outbound.as_ref(),
                                &journal_for_outbound,
                                &coordinator_for_outbound,
                                &chat_id,
                                &owner_for_outbound,
                                &event,
                                crate::delivery::RunEventTransition::Started(&run_id),
                            );
                            match transition {
                                Ok(_) => {}
                                Err(error) => log::warn!(
                                    "Could not persist or deliver run_started for chat {chat_id}: {error}"
                                ),
                            }
                        }
                        RunLifecycleEvent::Warning {
                            run_id, chat_id, ..
                        } => {
                            let transition = crate::delivery::persist_and_deliver_to_renderer(
                                sink_for_outbound.as_ref(),
                                &journal_for_outbound,
                                &coordinator_for_outbound,
                                &chat_id,
                                &owner_for_outbound,
                                &event,
                                crate::delivery::RunEventTransition::NextForRun(&run_id),
                            );
                            match transition {
                                Ok(_) => {}
                                Err(error) => log::warn!(
                                    "Could not persist or deliver run_warning for chat {chat_id}: {error}"
                                ),
                            }
                        }
                        RunLifecycleEvent::WarningCleared {
                            run_id, chat_id, ..
                        } => {
                            let transition = crate::delivery::persist_and_deliver_to_renderer(
                                sink_for_outbound.as_ref(),
                                &journal_for_outbound,
                                &coordinator_for_outbound,
                                &chat_id,
                                &owner_for_outbound,
                                &event,
                                crate::delivery::RunEventTransition::NextForRun(&run_id),
                            );
                            match transition {
                                Ok(_) => {}
                                Err(error) => log::warn!(
                                    "Could not persist or deliver run_warning_cleared for chat {chat_id}: {error}"
                                ),
                            }
                        }
                        RunLifecycleEvent::Terminated {
                            run_id, chat_id, ..
                        } => {
                            let transition = crate::delivery::persist_and_deliver_to_renderer(
                                sink_for_outbound.as_ref(),
                                &journal_for_outbound,
                                &coordinator_for_outbound,
                                &chat_id,
                                &owner_for_outbound,
                                &event,
                                crate::delivery::RunEventTransition::Terminated(&run_id),
                            );
                            match transition {
                                Ok(_) => {}
                                Err(error) => log::warn!(
                                    "Could not persist or deliver run_terminated for chat {chat_id}: {error}"
                                ),
                            }
                        }
                    }
                }
                BusMessage::StreamDelta { chat_id, chunk } => {
                    let event = crate::event::Event::StreamDelta {
                        chunk: serde_json::to_value(chunk)
                            .expect("IsanAgent stream chunks are serializable by contract"),
                    };
                    if let Err(error) = crate::delivery::emit_event(
                        sink_for_outbound.as_ref(),
                        &chat_id,
                        &event,
                        None,
                    ) {
                        log::warn!("Could not deliver stream delta for chat {chat_id}: {error}");
                    }
                }
                BusMessage::SessionProjection(projection) => {
                    let chat_id = projection.chat_id;
                    let event = crate::event::Event::SessionProjection {
                        projection_seq: projection.seq,
                        timestamp_rfc3339: projection.timestamp_rfc3339,
                        run_status: projection.run_status,
                        todos: projection.todos,
                        subagents: projection.subagents,
                        jobs: projection.jobs,
                    };
                    if let Err(error) = crate::delivery::emit_event(
                        sink_for_outbound.as_ref(),
                        &chat_id,
                        &event,
                        None,
                    ) {
                        log::warn!(
                            "Could not deliver session projection for chat {chat_id}: {error}"
                        );
                    }
                }
                _ => {}
            }
        }
    });

    // Emit ready event under the runtime's bootstrap chat_id. It does not match
    // any ALTAI chat tab, so the frontend filters it out — it exists only as a
    // lifecycle signal, not a message to render in a user's chat.
    if let Err(error) = crate::delivery::emit_event(
        event_sink.as_ref(),
        channel.chat_id(),
        &crate::event::Event::AgentMessage {
            content: "IsanAgent runtime initialized.".to_string(),
            role: "system".to_string(),
        },
        None,
    ) {
        log::warn!("Could not deliver runtime bootstrap event: {error}");
    }

    Ok(BuiltInstance {
        channel,
        bus_tx,
        shutdown: shutdown_tx,
        bus_router,
        outbound_router,
    })
}

/// Register the agent-facing `cron` tool unless the host reports canonical
/// scheduling as owning this workspace (CP-08-108). Withholding the tool is
/// the typed-closed refusal: an automation created through it would join the
/// retired legacy firing path and be suppressed at fire time, never mapped
/// into the canonical ledger.
fn register_cron_tool(
    tools: &mut ToolRegistry,
    hooks: &SharedInstanceHooks,
    cron_node: NodeHandle<String>,
    cron_db_path: String,
) {
    if hooks.suppress_agent_cron {
        log::info!("cron tool withheld: canonical scheduling owns this workspace");
        return;
    }
    tools.register(Box::new(CronTool {
        cron_node,
        multi_tenant_edge_cron_enabled: false,
        mte_cron_scheduler: None,
        db_path: cron_db_path,
    }));
}

pub fn register_existing_claw_tools(
    tools: &mut ToolRegistry,
    memory_node: NodeHandle<isanagent::memory::MemoryMessage>,
    clarification_hub: Arc<ClarificationHub>,
    outbound_tx: mpsc::Sender<BusMessage>,
) {
    tools.register(Box::new(AskUserTool {
        clarification_hub,
        outbound_tx,
        memory_node: Some(memory_node.clone()),
    }));
    tools.register(Box::new(SearchMemoryTool {
        memory_node: memory_node.clone(),
    }));
    tools.register(Box::new(FetchMemoryByDateTool { memory_node }));
}

#[cfg(test)]
mod cron_tool_registration_tests {
    use super::*;

    /// A live-typed handle with no spawned actor behind it — registration
    /// only stores the tool, it never talks to the cron actor.
    fn test_cron_node() -> NodeHandle<String> {
        NodeHandle::create_listener("cron-test", 8).0
    }

    #[test]
    fn cron_tool_is_withheld_when_canonical_scheduling_suppresses_fires() {
        let mut tools = ToolRegistry::new();
        let hooks = SharedInstanceHooks {
            suppress_agent_cron: true,
            ..Default::default()
        };
        register_cron_tool(&mut tools, &hooks, test_cron_node(), "db".to_string());
        assert!(
            !tools.get_tool_names().iter().any(|name| name == "cron"),
            "the agent-facing cron tool must be absent under canonical scheduling"
        );
    }

    #[test]
    fn cron_tool_registers_when_scheduling_is_legacy() {
        let mut tools = ToolRegistry::new();
        let hooks = SharedInstanceHooks::default();
        register_cron_tool(&mut tools, &hooks, test_cron_node(), "db".to_string());
        assert!(
            tools.get_tool("cron").is_some(),
            "legacy workspaces keep the agent-facing cron tool"
        );
    }
}
