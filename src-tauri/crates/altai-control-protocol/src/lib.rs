//! Shared domain contracts for the ALTAI control plane.
//!
//! This crate defines the stable, product-neutral types that cross the
//! control-plane / execution-plane boundary (ADR 0003). It knows nothing
//! about SQLite, Tauri, network transport, or UI. Every canonical ID is a
//! distinct typed struct; no identity is ever a bare string.
//!
//! See: docs/PAPERCLIP_STYLE_CONTROL_PLANE_ENGINEERING_PLAN.md §3.3, §4, §5.

pub mod actor;
pub mod agent;
pub mod budget;
pub mod approval;
pub mod attempt;
pub mod error;
pub mod external;
pub mod event;
pub mod evidence;
pub mod id;
pub mod plugin;
pub mod plugin_ui;
pub mod protocol;
pub mod recovery;
pub mod registration;
pub mod revision;
pub mod routine;
pub mod scope;
pub mod usage;
pub mod workspace_scope;
pub mod wake;
pub mod work;
pub mod work_graph;

pub use actor::{Actor, ActorKind};
pub use agent::{AgentInstance, AgentProfileRevision, AgentStatus};
pub use approval::{Approval, ApprovalDecision, ApprovalOutcome, ApprovalScope};
pub use budget::Budget;
pub use attempt::{Attempt, AttemptState, RunBinding, ScheduleBackend, ScheduleBackendBinding};
pub use error::{ControlError, ControlErrorCode};
pub use event::{ActivityEvent, ControlEvent, EventKind};
pub use external::{ExternalAccount, ExternalAuthority, ExternalObject};
pub use evidence::Evidence;
pub use id::*;
pub use protocol::{
    ActivityQueryRequest, CapabilityNegotiationRequest, CapabilityNegotiationResponse,
    ControlPlaneCapabilities, CreateWorkItemCommand, DeploymentMode, EventReplayRequest,
    EventReplayResponse, PageRequest, PageResponse, ProtocolCommand, ProtocolError,
    ProtocolOutcome, ProtocolRequest, ProtocolResponse, ProtocolVersion,
    TransitionWorkItemCommand, CONTROL_PLANE_PROTOCOL_VERSION_MAJOR,
    CONTROL_PLANE_PROTOCOL_VERSION_MINOR, DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT,
    MAX_WORK_ITEM_DESCRIPTION_BYTES, MAX_WORK_ITEM_TITLE_BYTES,
};
pub use recovery::{RecoveryDisposition, RecoveryRecord};
pub use registration::{
    ControlPlaneHealth, HostCapabilities, HostRegistration, HostRegistrationRequest,
    RegisteredHost, CONTROL_PLANE_PROTOCOL_MAJOR,
};
pub use plugin::{
    PluginCapability, PluginKind, PluginManifest, PluginManifestError, PluginUpgradeDisclosure,
    PluginVersion,
};
pub use plugin_ui::{
    PluginUiAction, PluginUiDeclaration, PluginUiError, PluginUiNode, PluginUiSurface,
    MAX_DEPTH, MAX_NODES_PER_SURFACE, MAX_SURFACES, MAX_TABLE_COLUMNS, MAX_TABLE_ROWS,
};
pub use revision::Revision;
pub use routine::{Routine, RoutineRevision, RoutineStatus, RoutineTrigger};
pub use scope::{Goal, Organization, Project, ProjectStatus, ProjectWorkspace};
pub use usage::{UsageRecord, UsageScope};
pub use workspace_scope::RepositoryScope;
pub use wake::{WakeRequest, WakeSource, WorkCheckoutLease};
pub use work::{ExecutionPhase, WorkItem, WorkItemKind, WorkStatus};
pub use work_graph::{WorkComment, WorkDependency, WorkRelationKind};
