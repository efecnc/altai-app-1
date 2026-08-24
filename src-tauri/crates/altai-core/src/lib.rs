//! Tauri-independent primitives shared by the future ALTAI CLI and Desktop
//! adapters. This crate intentionally contains no UI-runtime dependency.

pub mod compaction;
pub mod config;
pub mod event;
pub mod journal;
pub mod legacy_import;
pub mod legacy_mapping;
#[cfg(test)]
mod legacy_mapping_tests;
pub mod palette;
pub mod policy;
pub mod work;
pub mod workspace;
pub mod workspace_lock;

pub use compaction::{
    resolve_compaction_prefs, CompactionLogicParams, CompactionOverrides, CompactionPrefs,
};
pub use config::{
    load_agent_config, resolve_agent_config_layers, resolve_config, AgentConfigError,
    AgentConfigLayer, ConfigSource, ResolvedAgentConfig, ResolvedConfig,
};
pub use event::{EventEnvelope, EVENT_SCHEMA_VERSION};
pub use journal::{ChatUsageTotals, 
    AppendStatus, ChatJournalSummary, EventJournal, JournalError, JournalEvent, JournalResult,
    RunJournalSummary, SessionJournalMetadata, TaskRunJournalMetadata,
};
pub use legacy_import::{
    legacy_assignment_source_key, legacy_task_run_source_key, legacy_todo_source_key,
    parse_legacy_assignments, parse_legacy_todos, preview_legacy_import, LegacyImportDisposition,
    LegacyImportPreviewItem, LegacyImportPreviewPaths, LegacyImportPreviewReport,
    LegacyImportSource, LegacyImportSourceKind, LegacySourceIdentity, LegacySqliteSource,
    LegacyWorkspaceRoot, LEGACY_IMPORT_EVENT_KIND, LEGACY_IMPORT_SOURCE_KEY_FIELD,
    LEGACY_IMPORT_SOURCE_KEY_MAX_BYTES,
};
pub use palette::{
    load_terminal_palette, resolve_terminal_appearance, resolve_terminal_appearance_from_env,
    EffectiveTerminalAppearance, PaletteError, ResolvedTerminalColors, Rgb, TerminalLayoutDensity,
    TerminalPaletteManifest, TerminalThemeMode, DARK_TERMINAL_COLORS, LIGHT_TERMINAL_COLORS,
};
pub use policy::{shell_edit_modes_for, PermissionPolicyMode, ShellEditPolicyModes};
pub use work::{
    AgentRecord, AgentStatus, AttemptPhase, AttemptReconcileMode, AttemptRecord, CreateWorkInput,
    RecentAttemptRecord, RecentEventRecord, WorkAttemptStart, WorkEventRecord, WorkInboxKind,
    WorkInboxRecord, WorkItemKind, WorkItemRecord, WorkListFilter, WorkState, WorkStore,
    WorkStoreError,
};
pub use workspace::{resolve_workspace, resolve_workspace_from, WorkspaceError, WorkspacePaths};
