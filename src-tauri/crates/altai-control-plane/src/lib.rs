//! Federated control-plane bootstrap primitives.
//!
//! This crate deliberately starts at the boundary: version/health reporting
//! and one-time authenticated execution-host registration. It has no Work
//! repository and does not move any existing `work.db` mutation. A later M3
//! sync layer will connect the global control database to local execution
//! ledgers through the registered host identity.

pub mod activity_repository;
pub mod agent_repository;
pub mod budget_enforcer;
pub mod budget_repository;
pub mod approval_repository;
pub mod plugin_registry;
pub mod plugin_worker;
pub mod plugin_worker_jobs;
pub mod plugin_worker_launcher;
pub mod plugin_worker_secrets;
pub mod plugin_worker_transport;
pub mod plugin_worker_webhooks;
pub mod attempt_finalizer;
pub mod attempt_repository;
pub mod control_event_projection;
pub mod control_event_repository;
pub mod completion_gate;
pub mod cron_due;
pub mod delivery_gate;
pub mod dispatch_eligibility;
pub mod evidence_repository;
pub mod evidence_replay;
pub mod account_credentials;
pub mod external_account_repository;
pub mod external_object_repository;
pub mod external_sync;
pub mod opentag_adapter;
pub mod execution_repository;
pub mod legacy_work_bridge;
pub mod liveness_monitor;
pub mod local_migration;
pub mod protocol_dispatch;
pub mod recovery_repository;
pub mod recovery_service;
pub mod repository_scope_repository;
pub mod routine_cron_bridge;
pub mod routine_materializer;
pub mod routine_repository;
pub mod run_binding_repository;
pub mod run_context;
pub mod schedule_backend_repository;
pub mod scheduler;
pub mod scope_repository;
mod service;
pub mod sqlite_agent;
pub mod sqlite_registration;
pub mod sqlite_scope;
pub mod sqlite_wake;
pub mod sqlite_work_graph;
pub mod transport;
pub mod usage_repository;
pub mod wake_repository;
pub mod work_graph_repository;
pub mod work_item_repository;
pub mod workspace_scope_gate;

pub use activity_repository::{
    ActivityEventError, ActivityEventRepository, SqliteActivityEventRepository,
};
pub use agent_repository::{AgentRepository, AgentRepositoryError, InMemoryAgentRepository};
pub use altai_control_protocol::{
    ControlPlaneHealth, HostCapabilities, HostRegistration, HostRegistrationRequest, RegisteredHost,
};
pub use attempt_finalizer::{
    finalize_attempt, AttemptFinalization, AttemptFinalizationError, RunOutcome,
};
pub use attempt_repository::{AttemptError, AttemptRepository, SqliteAttemptRepository};
pub use approval_repository::{ApprovalError, ApprovalRepository, SqliteApprovalRepository};
pub use plugin_registry::{PluginRegistry, PluginRegistryError, PluginRegistryOutcome, SqlitePluginRegistry};
pub use plugin_worker::{
    HealthProbePolicy, WorkerDirective, WorkerError, WorkerHealth, WorkerObservation,
    WorkerRestartPolicy, WorkerSupervisor,
};
pub use plugin_worker_launcher::{
    CommandWorkerLauncher, SupervisedWorker, WorkerLauncher, WorkerProcess,
};
pub use plugin_worker_jobs::{
    DispatchLedger, DispatchOutcome, DispatchState, JobRequest, JobResult,
};
pub use plugin_worker_secrets::{
    SecretAck, SecretHandoff, SecretHandoffOutcome, SecretString,
};
pub use plugin_worker_transport::{StdioWorkerTransport, WorkerFrame};
pub use plugin_worker_webhooks::{WebhookAck, WebhookDelivery};
pub use control_event_projection::{AggregateCheckpoint, fold_checkpoints};
pub use control_event_repository::{
    ControlEventError, ControlEventRepository, SqliteControlEventRepository,
};
pub use dispatch_eligibility::{
    DispatchBlocker, DispatchEligibility, DispatchEligibilityEngine, DispatchEligibilityError,
};
pub use execution_repository::{
    ExecutionSnapshot, ExecutionSnapshotError, ExecutionSnapshotRepository,
    SqliteExecutionSnapshotRepository,
};
pub use legacy_work_bridge::{LegacyWorkBridge, LegacyWorkBridgeError};
pub use local_migration::{
    LocalMigrationError, LocalMigrationReport, LocalMigrationRunner, LOCAL_WORK_DB_SCHEMA_VERSION,
};
pub use liveness_monitor::{LivenessError, LivenessMonitor};
pub use protocol_dispatch::{ProtocolDispatcher, capabilities_from_wiring};
pub use recovery_repository::{RecoveryError, RecoveryRepository, SqliteRecoveryRepository};
pub use recovery_service::{RecoveryOutcome, RecoveryService, RecoveryServiceError};
pub use repository_scope_repository::{
    RepositoryScopeError, RepositoryScopeRepository, SqliteRepositoryScopeRepository,
};
pub use routine_cron_bridge::{RoutineCronBridge, DEFAULT_CRON_TICK};
pub use routine_materializer::{RoutineMaterializationError, RoutineMaterializer};
pub use account_credentials::{
    AccountCredentialStore, InMemoryAccountCredentialStore,
};
pub use external_account_repository::{
    ExternalAccountError, ExternalAccountRepository, SqliteExternalAccountRepository,
};
pub use external_object_repository::{
    ConflictResolution, ExternalObjectError, ExternalObjectRepository, ExternalSyncOutcome,
    SqliteExternalObjectRepository,
};
pub use external_sync::{
    content_hash, resolve_external_conflict, ExternalObjectProvider, ExternalSyncConflict,
    ExternalSyncError, ExternalSyncReport, ExternalSyncService, ProviderObject,
};
pub use opentag_adapter::{
    normalize_opentag_event, NormalizedOpenTagEvent, OpenTagAdapterError,
    OpenTagAdapterPolicy, OpenTagInboundEvent,
};
pub use routine_repository::{RoutineError, RoutineRepository, SqliteRoutineRepository};
pub use run_binding_repository::{
    RunBindingError, RunBindingRepository, SqliteRunBindingRepository,
};
pub use run_context::{
    assemble_bounded_run_context, build_bounded_run_context, load_attempt_bound_run_context,
    load_bounded_run_context, BoundedRunContext, RunContextError, RunContextInput,
    MAX_RUN_CONTEXT_BYTES,
};
pub use scheduler::{ScheduleResult, SchedulerError, SingleWriterScheduler};
pub use schedule_backend_repository::{
    ScheduleBackendError, ScheduleBackendRepository, SqliteScheduleBackendRepository,
};
pub use scope_repository::{InMemoryScopeRepository, ScopeError, ScopeRepository};
pub use service::{
    ControlPlane, ControlPlaneConfig, ControlPlaneError, ControlPlaneStore, RegistrationCommit,
    RegistrationGrant, RegistrationRepository,
};
pub use sqlite_agent::SqliteAgentRepository;
pub use sqlite_registration::SqliteRegistrationRepository;
pub use sqlite_scope::SqliteScopeRepository;
pub use sqlite_wake::SqliteWakeRepository;
pub use sqlite_work_graph::SqliteWorkGraphRepository;
pub use transport::{
    router, router_with_all_repositories, router_with_control_repositories,
    router_with_repositories, router_with_scope_repository, BootstrapCredential, TransportError,
};
pub use wake_repository::{InMemoryWakeRepository, WakeError, WakeRepository};
pub use usage_repository::{SqliteUsageRepository, UsageError, UsageRepository};
pub use budget_repository::{BudgetError, BudgetRepository, SqliteBudgetRepository};
pub use budget_enforcer::BudgetEnforcer;
pub use completion_gate::{CompletionBlocker, CompletionError, CompletionGate, CompletionOutcome};
pub use delivery_gate::{DeliveryBlocker, DeliveryDecision, DeliveryError, DeliveryGate};
pub use evidence_repository::{EvidenceError, EvidenceRepository, SqliteEvidenceRepository};
pub use evidence_replay::{
    EvidenceReplayActivity, EvidenceReplayArtifact, EvidenceReplayError, EvidenceReplayInput,
    QM_084_EVIDENCE_REPLAY_SCHEMA_VERSION,
};
pub use work_graph_repository::{InMemoryWorkGraphRepository, WorkGraphError, WorkGraphRepository};
pub use workspace_scope_gate::{
    DenialReason, ScopePermit, WorkspaceScopeError, WorkspaceScopeGate,
};
pub use work_item_repository::{
    SqliteWorkItemRepository, WorkItemRepository, WorkItemRepositoryError,
};
