//! Public versioned control protocol contracts and capability negotiation.
//!
//! Provides the canonical framing, pagination, capability negotiation, and
//! query/event replay models shared across all ALTAI surfaces (Desktop, IDE,
//! Studio, CLI, and future remote workers).
//!
//! See: docs/PAPERCLIP_STYLE_CONTROL_PLANE_ENGINEERING_PLAN.md §8 CP-15.

use crate::actor::Actor;
use crate::error::{ControlError, ControlErrorCode};
use crate::event::{ActivityEvent, ControlEvent, EventKind};
use crate::id::{GoalId, OrganizationId, ProjectId, WorkItemId};
use crate::revision::Revision;
use crate::work::{WorkItem, WorkItemKind, WorkStatus};
use serde::{Deserialize, Serialize};

pub const CONTROL_PLANE_PROTOCOL_VERSION_MAJOR: u16 = 1;
pub const CONTROL_PLANE_PROTOCOL_VERSION_MINOR: u16 = 0;

pub const DEFAULT_PAGE_LIMIT: u32 = 50;
pub const MAX_PAGE_LIMIT: u32 = 250;

/// Bounded payload caps for work-item mutations. Every protocol-facing
/// write honors them so a single command cannot balloon a row.
pub const MAX_WORK_ITEM_TITLE_BYTES: usize = 200;
pub const MAX_WORK_ITEM_DESCRIPTION_BYTES: usize = 8_192;

/// Semantic protocol version (major.minor).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const CURRENT: Self = Self {
        major: CONTROL_PLANE_PROTOCOL_VERSION_MAJOR,
        minor: CONTROL_PLANE_PROTOCOL_VERSION_MINOR,
    };

    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Returns true if this version is wire-compatible with the server's current major version.
    pub fn is_compatible_with(&self, server: &ProtocolVersion) -> bool {
        self.major == server.major
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self::CURRENT
    }
}

/// Deployment mode of the control-plane service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentMode {
    /// Local desktop or headless daemon (work.db SQLite).
    LocalDaemon,
    /// Managed or remote deployed multi-tenant backend.
    DeployedBackend,
    /// Embedded in-process host.
    EmbeddedHost,
}

/// Typed capability set advertised and negotiated by the control-plane protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlPlaneCapabilities {
    pub organizations: bool,
    pub goals: bool,
    pub projects: bool,
    pub workspaces: bool,
    pub agents: bool,
    pub work_graph: bool,
    pub attempts: bool,
    pub routines: bool,
    pub approvals: bool,
    pub budgets: bool,
    pub evidence: bool,
    pub activity_audit: bool,
    pub event_replay: bool,
    pub workspace_scopes: bool,
}

impl ControlPlaneCapabilities {
    /// Standard full capabilities enabled on a fully featured local or deployed instance.
    pub fn full() -> Self {
        Self {
            organizations: true,
            goals: true,
            projects: true,
            workspaces: true,
            agents: true,
            work_graph: true,
            attempts: true,
            routines: true,
            approvals: true,
            budgets: true,
            evidence: true,
            activity_audit: true,
            event_replay: true,
            workspace_scopes: true,
        }
    }

    /// Minimal capability profile for light/bootstrap nodes.
    pub fn minimal() -> Self {
        Self {
            organizations: true,
            goals: false,
            projects: true,
            workspaces: true,
            agents: true,
            work_graph: true,
            attempts: true,
            routines: false,
            approvals: false,
            budgets: false,
            evidence: false,
            activity_audit: false,
            event_replay: false,
            workspace_scopes: false,
        }
    }

    /// Check whether a named capability is enabled.
    pub fn supports(&self, capability: &str) -> bool {
        match capability {
            "organizations" => self.organizations,
            "goals" => self.goals,
            "projects" => self.projects,
            "workspaces" => self.workspaces,
            "agents" => self.agents,
            "work_graph" => self.work_graph,
            "attempts" => self.attempts,
            "routines" => self.routines,
            "approvals" => self.approvals,
            "budgets" => self.budgets,
            "evidence" => self.evidence,
            "activity_audit" => self.activity_audit,
            "event_replay" => self.event_replay,
            "workspace_scopes" => self.workspace_scopes,
            _ => false,
        }
    }
}

impl Default for ControlPlaneCapabilities {
    fn default() -> Self {
        Self::full()
    }
}

/// Request to negotiate protocol version and capabilities with the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityNegotiationRequest {
    pub client_version: ProtocolVersion,
    pub client_name: String,
    pub required_capabilities: Vec<String>,
}

/// Server response for capability negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityNegotiationResponse {
    pub server_version: ProtocolVersion,
    pub deployment_mode: DeploymentMode,
    pub server_capabilities: ControlPlaneCapabilities,
    pub compatible: bool,
    pub missing_capabilities: Vec<String>,
}

impl CapabilityNegotiationResponse {
    pub fn evaluate(
        server_version: ProtocolVersion,
        deployment_mode: DeploymentMode,
        server_capabilities: ControlPlaneCapabilities,
        request: &CapabilityNegotiationRequest,
    ) -> Self {
        let compatible = request.client_version.is_compatible_with(&server_version);
        let mut missing = Vec::new();
        for req in &request.required_capabilities {
            if !server_capabilities.supports(req) {
                missing.push(req.clone());
            }
        }
        let is_fully_compatible = compatible && missing.is_empty();
        Self {
            server_version,
            deployment_mode,
            server_capabilities,
            compatible: is_fully_compatible,
            missing_capabilities: missing,
        }
    }
}

/// Standard cursor-based pagination request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    pub cursor: Option<String>,
    pub limit: u32,
}

impl PageRequest {
    pub fn new(cursor: Option<String>, limit: Option<u32>) -> Self {
        let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
        Self { cursor, limit }
    }

    pub fn effective_limit(&self) -> u32 {
        self.limit.clamp(1, MAX_PAGE_LIMIT)
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

/// Standard paginated response envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageResponse<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub total_count: Option<u64>,
}

impl<T> PageResponse<T> {
    pub fn new(
        items: Vec<T>,
        next_cursor: Option<String>,
        has_more: bool,
        total_count: Option<u64>,
    ) -> Self {
        Self {
            items,
            next_cursor,
            has_more,
            total_count,
        }
    }

    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
            has_more: false,
            total_count: Some(0),
        }
    }
}

/// Typed error returned in protocol responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ControlErrorCode,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

impl ProtocolError {
    pub fn new(code: ControlErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(
        code: ControlErrorCode,
        message: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
        }
    }

    pub fn from_control_error(err: &ControlError) -> Self {
        Self {
            code: err.code(),
            message: err.to_string(),
            data: None,
        }
    }
}

/// Uniform authenticated request envelope for public control protocol commands/queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolRequest<T> {
    pub id: String,
    pub version: ProtocolVersion,
    pub actor: Actor,
    pub payload: T,
}

/// Uniform response envelope for public control protocol commands/queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolResponse<T> {
    pub id: String,
    pub result: Result<T, ProtocolError>,
}

impl<T> ProtocolResponse<T> {
    pub fn ok(id: impl Into<String>, value: T) -> Self {
        Self {
            id: id.into(),
            result: Ok(value),
        }
    }

    pub fn error(id: impl Into<String>, error: ProtocolError) -> Self {
        Self {
            id: id.into(),
            result: Err(error),
        }
    }
}

/// Request to replay append-only control events since a sequence number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventReplayRequest {
    pub organization_id: OrganizationId,
    pub since_sequence: u64,
    pub limit: u32,
    pub aggregate: Option<String>,
}

impl EventReplayRequest {
    pub fn new(organization_id: OrganizationId, since_sequence: u64, limit: Option<u32>) -> Self {
        let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
        Self {
            organization_id,
            since_sequence,
            limit,
            aggregate: None,
        }
    }
}

/// Response containing sequential control events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventReplayResponse {
    pub events: Vec<ControlEvent>,
    pub next_sequence: u64,
    pub has_more: bool,
}

/// Query request for audit-level activity events with filtering and cursor pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityQueryRequest {
    pub organization_id: OrganizationId,
    pub page: PageRequest,
    pub kind: Option<EventKind>,
    pub work_item_id: Option<WorkItemId>,
}

/// Command payload: create one canonical work item. The caller supplies the
/// `work_item_id`, so a retried create observes the existing row as a typed
/// `Conflict` rather than a second row. The birth state is fixed — status
/// `backlog`, execution phase `none`, revision INITIAL — because create is
/// birth, not lifecycle; transitions move status afterwards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkItemCommand {
    /// Organization the work belongs to; carried for event attribution.
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub work_item_id: WorkItemId,
    pub goal_id: Option<GoalId>,
    /// Decomposition only — parentage is never a dependency edge.
    pub parent_work_item_id: Option<WorkItemId>,
    pub kind: WorkItemKind,
    pub title: String,
    pub description: String,
}

/// Command payload: transition a work item's status under optimistic
/// concurrency. `expected_revision` must equal the stored revision or the
/// command fails typed `StaleRevision` without writing. Execution phase is
/// dispatch-owned and is never touched by this command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionWorkItemCommand {
    /// Organization the work belongs to; carried for event attribution.
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub work_item_id: WorkItemId,
    pub to_status: WorkStatus,
    pub expected_revision: Revision,
}

/// A protocol-level command, query, or event operation framed by
/// [`ProtocolRequest`]. Adjacent tagging keeps the wire shape stable and
/// mirrorable: `{"type": "negotiate_capabilities", "payload": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ProtocolCommand {
    NegotiateCapabilities(CapabilityNegotiationRequest),
    QueryActivity(ActivityQueryRequest),
    ReplayEvents(EventReplayRequest),
    CreateWorkItem(CreateWorkItemCommand),
    TransitionWorkItem(TransitionWorkItemCommand),
}

/// The successful payload of a [`ProtocolResponse`] for each
/// [`ProtocolCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ProtocolOutcome {
    Negotiated(CapabilityNegotiationResponse),
    Activity(PageResponse<ActivityEvent>),
    Replayed(EventReplayResponse),
    /// Read-your-write: the work item as it exists after the create.
    WorkItemCreated(WorkItem),
    /// Read-your-write: the work item as it exists after the transition.
    WorkItemTransitioned(WorkItem),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_compatibility() {
        let v1_0 = ProtocolVersion::new(1, 0);
        let v1_1 = ProtocolVersion::new(1, 1);
        let v2_0 = ProtocolVersion::new(2, 0);

        assert!(v1_0.is_compatible_with(&v1_1));
        assert!(v1_1.is_compatible_with(&v1_0));
        assert!(!v1_0.is_compatible_with(&v2_0));
    }

    #[test]
    fn capability_negotiation_success() {
        let server_version = ProtocolVersion::CURRENT;
        let capabilities = ControlPlaneCapabilities::full();
        let request = CapabilityNegotiationRequest {
            client_version: ProtocolVersion::CURRENT,
            client_name: "altai-desktop-ui".to_string(),
            required_capabilities: vec!["organizations".to_string(), "work_graph".to_string()],
        };

        let response = CapabilityNegotiationResponse::evaluate(
            server_version,
            DeploymentMode::LocalDaemon,
            capabilities,
            &request,
        );

        assert!(response.compatible);
        assert!(response.missing_capabilities.is_empty());
        assert_eq!(response.deployment_mode, DeploymentMode::LocalDaemon);
    }

    #[test]
    fn capability_negotiation_reports_missing() {
        let server_version = ProtocolVersion::CURRENT;
        let capabilities = ControlPlaneCapabilities::minimal();
        let request = CapabilityNegotiationRequest {
            client_version: ProtocolVersion::CURRENT,
            client_name: "altai-enterprise-cli".to_string(),
            required_capabilities: vec![
                "organizations".to_string(),
                "budgets".to_string(),
                "event_replay".to_string(),
            ],
        };

        let response = CapabilityNegotiationResponse::evaluate(
            server_version,
            DeploymentMode::LocalDaemon,
            capabilities,
            &request,
        );

        assert!(!response.compatible);
        assert_eq!(
            response.missing_capabilities,
            vec!["budgets".to_string(), "event_replay".to_string()]
        );
    }

    #[test]
    fn page_request_clamps_limits() {
        let req_zero = PageRequest::new(None, Some(0));
        assert_eq!(req_zero.limit, 1);

        let req_huge = PageRequest::new(None, Some(1000));
        assert_eq!(req_huge.limit, MAX_PAGE_LIMIT);

        let req_normal = PageRequest::new(Some("cur_123".to_string()), Some(30));
        assert_eq!(req_normal.limit, 30);
        assert_eq!(req_normal.cursor.as_deref(), Some("cur_123"));
    }

    #[test]
    fn protocol_response_serialization() {
        let response_ok = ProtocolResponse::ok("req_1", "success_value");
        let json_ok = serde_json::to_string(&response_ok).unwrap();
        assert!(json_ok.contains("\"id\":\"req_1\""));
        assert!(json_ok.contains("\"Ok\":\"success_value\""));

        let proto_err = ProtocolError::new(ControlErrorCode::PolicyDenied, "Denied by policy");
        let response_err: ProtocolResponse<String> = ProtocolResponse::error("req_2", proto_err);
        let json_err = serde_json::to_string(&response_err).unwrap();
        assert!(json_err.contains("\"id\":\"req_2\""));
        assert!(json_err.contains("\"code\":\"PolicyDenied\""));
        assert!(json_err.contains("Denied by policy"));
    }

    #[test]
    fn protocol_error_from_control_error() {
        let ctrl_err = ControlError::BudgetStopped {
            scope: "org_1".to_string(),
        };
        let proto_err = ProtocolError::from_control_error(&ctrl_err);
        assert_eq!(proto_err.code, ControlErrorCode::BudgetStopped);
        assert!(proto_err.message.contains("budget stopped: org_1"));
    }

    #[test]
    fn protocol_command_uses_stable_adjacent_tagging() {
        let command = ProtocolCommand::NegotiateCapabilities(CapabilityNegotiationRequest {
            client_version: ProtocolVersion::CURRENT,
            client_name: "altai-cli".to_string(),
            required_capabilities: vec!["organizations".to_string()],
        });
        let json = serde_json::to_value(&command).unwrap();
        assert_eq!(json["type"], "negotiate_capabilities");
        assert_eq!(json["payload"]["client_name"], "altai-cli");
        let round_trip: ProtocolCommand = serde_json::from_value(json).unwrap();
        assert_eq!(round_trip, command);
    }

    #[test]
    fn protocol_outcome_round_trips_every_arm() {
        let outcomes = vec![
            ProtocolOutcome::Negotiated(CapabilityNegotiationResponse {
                server_version: ProtocolVersion::CURRENT,
                deployment_mode: DeploymentMode::EmbeddedHost,
                server_capabilities: ControlPlaneCapabilities::full(),
                compatible: true,
                missing_capabilities: Vec::new(),
            }),
            ProtocolOutcome::Activity(PageResponse::empty()),
            ProtocolOutcome::Replayed(EventReplayResponse {
                events: Vec::new(),
                next_sequence: 7,
                has_more: false,
            }),
            ProtocolOutcome::WorkItemCreated(WorkItem {
                id: WorkItemId::new("01923abc-def0-7abc-8def-0123456789ab"),
                project_id: ProjectId::new("proj"),
                goal_id: None,
                parent_work_item_id: None,
                kind: WorkItemKind::Task,
                title: "Born item".into(),
                description: String::new(),
                status: WorkStatus::Backlog,
                execution_phase: crate::work::ExecutionPhase::None,
                revision: Revision::INITIAL,
                created_at: "2026-09-01T00:00:00.000Z".into(),
                updated_at: "2026-09-01T00:00:00.000Z".into(),
            }),
            ProtocolOutcome::WorkItemTransitioned(WorkItem {
                id: WorkItemId::new("01923abc-def0-7abc-8def-0123456789ab"),
                project_id: ProjectId::new("proj"),
                goal_id: None,
                parent_work_item_id: None,
                kind: WorkItemKind::Task,
                title: "Born item".into(),
                description: String::new(),
                status: WorkStatus::InProgress,
                execution_phase: crate::work::ExecutionPhase::None,
                revision: Revision::new(1),
                created_at: "2026-09-01T00:00:00.000Z".into(),
                updated_at: "2026-09-01T00:00:01.000Z".into(),
            }),
        ];
        for outcome in outcomes {
            let json = serde_json::to_string(&outcome).unwrap();
            let round_trip: ProtocolOutcome = serde_json::from_str(&json).unwrap();
            assert_eq!(round_trip, outcome);
        }
    }

    #[test]
    fn work_item_commands_use_stable_adjacent_tagging_and_round_trip() {
        let create = ProtocolCommand::CreateWorkItem(CreateWorkItemCommand {
            organization_id: OrganizationId::new("org"),
            project_id: ProjectId::new("proj"),
            work_item_id: WorkItemId::new("01923abc-def0-7abc-8def-0123456789ab"),
            goal_id: None,
            parent_work_item_id: None,
            kind: WorkItemKind::Ticket,
            title: "Ship the thing".into(),
            description: "with bounded prose".into(),
        });
        let json = serde_json::to_value(&create).unwrap();
        assert_eq!(json["type"], "create_work_item");
        assert_eq!(json["payload"]["kind"], "ticket");
        let round_trip: ProtocolCommand = serde_json::from_value(json).unwrap();
        assert_eq!(round_trip, create);

        let transition = ProtocolCommand::TransitionWorkItem(TransitionWorkItemCommand {
            organization_id: OrganizationId::new("org"),
            project_id: ProjectId::new("proj"),
            work_item_id: WorkItemId::new("01923abc-def0-7abc-8def-0123456789ab"),
            to_status: WorkStatus::InProgress,
            expected_revision: Revision::INITIAL,
        });
        let json = serde_json::to_value(&transition).unwrap();
        assert_eq!(json["type"], "transition_work_item");
        assert_eq!(json["payload"]["to_status"], "in_progress");
        assert_eq!(json["payload"]["expected_revision"], 0);
        let round_trip: ProtocolCommand = serde_json::from_value(json).unwrap();
        assert_eq!(round_trip, transition);
    }

    #[test]
    fn non_canonical_vocabulary_is_rejected_at_deserialization() {
        let json = serde_json::json!({
            "type": "transition_work_item",
            "payload": {
                "organization_id": OrganizationId::new("org"),
                "project_id": ProjectId::new("proj"),
                "work_item_id": WorkItemId::new("wi"),
                "to_status": "almost_done",
                "expected_revision": 0,
            }
        });
        let parsed: Result<ProtocolCommand, _> = serde_json::from_value(json);
        assert!(parsed.is_err(), "a second status model must not parse");
    }
}
