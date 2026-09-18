//! The single protocol seam between every ALTAI surface and the control plane.
//!
//! [`ProtocolDispatcher`] turns a [`ProtocolRequest<ProtocolCommand>`] into a
//! [`ProtocolResponse<ProtocolOutcome>`]; the axum transport calls the very
//! same [`ProtocolDispatcher::execute`] that an in-process (local) caller
//! calls, so "command/query/event conformance across local and deployed
//! transports" is structural — there is no second implementation to drift.
//!
//! Capabilities are derived from what the deployment actually serves
//! ([`capabilities_from_wiring`]); nothing advertises what it cannot serve.
//! Commands whose producers are not wired (budgets, evidence, workspace
//! scopes — and the event stores on a minimal deployment) answer a typed
//! `PolicyDenied` identically on every transport rather than guessing or
//! silently 404ing.

use altai_control_protocol::{
    ActivityQueryRequest, CapabilityNegotiationRequest, CapabilityNegotiationResponse,
    ControlErrorCode, ControlPlaneCapabilities, CreateWorkItemCommand, DeploymentMode,
    EventKind, EventReplayRequest, ExecutionPhase, GoalId, MAX_WORK_ITEM_DESCRIPTION_BYTES,
    MAX_WORK_ITEM_TITLE_BYTES, OrganizationId, ProjectId, ProtocolCommand, ProtocolError,
    ProtocolOutcome, ProtocolRequest, ProtocolResponse, ProtocolVersion, Revision,
    TransitionWorkItemCommand, WorkItem, WorkItemId, WorkStatus,
};
use std::sync::Arc;

use crate::{ActivityEventRepository, ControlEventRepository, WorkItemRepository};

/// Capabilities honestly derived from the repositories wired into the
/// transport. Domains with protocol-facing routes advertise `true`; budgets,
/// evidence and workspace scopes stay `false` until they have serving. The
/// work-graph mutation surface additionally requires its audit stores: a
/// deployment that cannot attribute mutations does not advertise the domain.
#[allow(clippy::too_many_arguments)]
pub fn capabilities_from_wiring(
    scope_repository: bool,
    agent_repository: bool,
    work_graph_repository: bool,
    work_item_repository: bool,
    attempt_repository: bool,
    routine_repository: bool,
    approval_repository: bool,
    activity_repository: bool,
    control_event_repository: bool,
) -> ControlPlaneCapabilities {
    ControlPlaneCapabilities {
        organizations: scope_repository,
        goals: scope_repository,
        projects: scope_repository,
        workspaces: scope_repository,
        agents: agent_repository,
        work_graph: work_graph_repository
            || (work_item_repository && activity_repository && control_event_repository),
        attempts: attempt_repository,
        routines: routine_repository,
        approvals: approval_repository,
        budgets: false,
        evidence: false,
        activity_audit: activity_repository,
        event_replay: control_event_repository,
        workspace_scopes: false,
    }
}

/// Dispatcher for the public versioned control protocol. Holds only what it
/// serves; read-only commands touch no domain store beyond negotiation
/// state, and the only mutation path is the canonical work-item pipeline
/// (commands whose producers are absent answer typed errors).
pub struct ProtocolDispatcher {
    deployment_mode: DeploymentMode,
    capabilities: ControlPlaneCapabilities,
    work_items: Option<Arc<dyn WorkItemRepository>>,
    activity: Option<Arc<dyn ActivityEventRepository>>,
    control_events: Option<Arc<dyn ControlEventRepository>>,
}

impl ProtocolDispatcher {
    pub fn new(
        deployment_mode: DeploymentMode,
        capabilities: ControlPlaneCapabilities,
    ) -> Self {
        Self {
            deployment_mode,
            capabilities,
            work_items: None,
            activity: None,
            control_events: None,
        }
    }

    /// Attach the canonical work-item store; serving `CreateWorkItem` /
    /// `TransitionWorkItem` and advertising `work_graph` both derive from
    /// this wiring.
    pub fn with_work_item_repository(
        mut self,
        repository: Arc<dyn WorkItemRepository>,
    ) -> Self {
        self.work_items = Some(repository);
        self
    }

    /// Attach the durable activity stream; serving `QueryActivity` and
    /// advertising `activity_audit` both derive from this wiring.
    pub fn with_activity_repository(
        mut self,
        repository: Arc<dyn ActivityEventRepository>,
    ) -> Self {
        self.activity = Some(repository);
        self
    }

    /// Attach the append-only control-event log; serving `ReplayEvents`
    /// and advertising `event_replay` both derive from this wiring.
    pub fn with_control_event_repository(
        mut self,
        repository: Arc<dyn ControlEventRepository>,
    ) -> Self {
        self.control_events = Some(repository);
        self
    }

    pub fn capabilities(&self) -> &ControlPlaneCapabilities {
        &self.capabilities
    }

    /// Answer a capability negotiation request: wire compatibility plus the
    /// subset of required capabilities this deployment cannot serve.
    pub fn negotiate(
        &self,
        request: &CapabilityNegotiationRequest,
    ) -> CapabilityNegotiationResponse {
        CapabilityNegotiationResponse::evaluate(
            ProtocolVersion::CURRENT,
            self.deployment_mode,
            self.capabilities.clone(),
            request,
        )
    }

    /// Execute one framed protocol request. The protocol major version is
    /// gated first (typed `PolicyDenied` on mismatch), then the command
    /// dispatches. Domain failures are values inside the envelope — transport
    /// status codes stay reserved for transport problems.
    pub fn execute(
        &self,
        request: &ProtocolRequest<ProtocolCommand>,
    ) -> ProtocolResponse<ProtocolOutcome> {
        if !request.version.is_compatible_with(&ProtocolVersion::CURRENT) {
            return ProtocolResponse::error(
                request.id.clone(),
                ProtocolError::new(
                    ControlErrorCode::PolicyDenied,
                    format!(
                        "protocol major version {} is not supported (server: {}.{})",
                        request.version.major,
                        ProtocolVersion::CURRENT.major,
                        ProtocolVersion::CURRENT.minor
                    ),
                ),
            );
        }
        let outcome = match &request.payload {
            ProtocolCommand::NegotiateCapabilities(inner) => {
                Ok(ProtocolOutcome::Negotiated(self.negotiate(inner)))
            }
            ProtocolCommand::QueryActivity(inner) => match &self.activity {
                Some(store) => self.query_activity(store, inner),
                None => Err(self.unsupported("activity_audit")),
            },
            ProtocolCommand::ReplayEvents(inner) => match &self.control_events {
                Some(store) => self.replay_events(store, inner),
                None => Err(self.unsupported("event_replay")),
            },
            ProtocolCommand::CreateWorkItem(inner) => match &self.work_items {
                Some(store) => self.create_work_item(store, inner, &request.actor),
                None => Err(self.unsupported("work_graph")),
            },
            ProtocolCommand::TransitionWorkItem(inner) => match &self.work_items {
                Some(store) => self.transition_work_item(store, inner, &request.actor),
                None => Err(self.unsupported("work_graph")),
            },
        };
        match outcome {
            Ok(value) => ProtocolResponse::ok(request.id.clone(), value),
            Err(error) => ProtocolResponse::error(request.id.clone(), error),
        }
    }

    fn query_activity(
        &self,
        store: &Arc<dyn ActivityEventRepository>,
        request: &ActivityQueryRequest,
    ) -> Result<ProtocolOutcome, ProtocolError> {
        store
            .query(request)
            .map(ProtocolOutcome::Activity)
            .map_err(|e| self.store_failure("activity query", e))
    }

    fn replay_events(
        &self,
        store: &Arc<dyn ControlEventRepository>,
        request: &EventReplayRequest,
    ) -> Result<ProtocolOutcome, ProtocolError> {
        store
            .replay(request)
            .map(ProtocolOutcome::Replayed)
            .map_err(|e| self.store_failure("event replay", e))
    }

    /// The canonical work-item create pipeline. Birth is fixed: status
    /// `backlog`, execution phase `none` (dispatch owns that field), and
    /// revision `INITIAL` — create is not lifecycle, transitions are.
    fn create_work_item(
        &self,
        store: &Arc<dyn WorkItemRepository>,
        command: &CreateWorkItemCommand,
        actor: &altai_control_protocol::Actor,
    ) -> Result<ProtocolOutcome, ProtocolError> {
        Self::require_typed_id(&command.organization_id.kind, OrganizationId::TYPE, "organization_id")?;
        Self::require_typed_id(&command.project_id.kind, ProjectId::TYPE, "project_id")?;
        Self::require_typed_id(&command.work_item_id.kind, WorkItemId::TYPE, "work_item_id")?;
        if let Some(goal) = &command.goal_id {
            Self::require_typed_id(&goal.kind, GoalId::TYPE, "goal_id")?;
        }
        if let Some(parent) = &command.parent_work_item_id {
            Self::require_typed_id(&parent.kind, WorkItemId::TYPE, "parent_work_item_id")?;
        }
        Self::require_bounded("title", &command.title, MAX_WORK_ITEM_TITLE_BYTES)?;
        Self::require_bounded(
            "description",
            &command.description,
            MAX_WORK_ITEM_DESCRIPTION_BYTES,
        )?;
        self.require_audit_wiring()?;

        // (5) Scope resolution: the project must exist and belong to the
        // organization the command claims — a foreign organization_id never
        // becomes the audit attribution for this project's work.
        Self::require_project_organization(store, &command.organization_id, &command.project_id)?;

        let timestamp = now_timestamp();
        let item = WorkItem {
            id: command.work_item_id.clone(),
            project_id: command.project_id.clone(),
            goal_id: command.goal_id.clone(),
            parent_work_item_id: command.parent_work_item_id.clone(),
            kind: command.kind,
            title: command.title.clone(),
            description: command.description.clone(),
            status: WorkStatus::Backlog,
            execution_phase: ExecutionPhase::None,
            revision: Revision::INITIAL,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        store
            .create(item.clone())
            .map_err(|e| self.work_item_failure("create work item", e))?;
        self.record_work_item_mutation(
            actor,
            &command.organization_id,
            &item,
            EventKind::Created,
            format!("created work item: {}", item.title),
        )?;
        Ok(ProtocolOutcome::WorkItemCreated(item))
    }

    /// The canonical work-item transition pipeline: load within project
    /// scope, enforce optimistic concurrency, persist the status change,
    /// then audit and answer read-your-write. Execution phase is never
    /// written here — dispatch owns it.
    fn transition_work_item(
        &self,
        store: &Arc<dyn WorkItemRepository>,
        command: &TransitionWorkItemCommand,
        actor: &altai_control_protocol::Actor,
    ) -> Result<ProtocolOutcome, ProtocolError> {
        Self::require_typed_id(&command.organization_id.kind, OrganizationId::TYPE, "organization_id")?;
        Self::require_typed_id(&command.project_id.kind, ProjectId::TYPE, "project_id")?;
        Self::require_typed_id(&command.work_item_id.kind, WorkItemId::TYPE, "work_item_id")?;
        self.require_audit_wiring()?;
        Self::require_project_organization(store, &command.organization_id, &command.project_id)?;

        let current = store
            .get_in_project(&command.project_id, &command.work_item_id)
            .map_err(|e| self.work_item_failure("transition work item", e))?;
        if current.revision != command.expected_revision {
            return Err(ProtocolError::new(
                ControlErrorCode::StaleRevision,
                format!(
                    "transition work item: expected revision {} but found {} for {}",
                    command.expected_revision.value(),
                    current.revision.value(),
                    command.work_item_id.value
                ),
            ));
        }
        let updated = WorkItem {
            status: command.to_status,
            revision: current.revision.next(),
            updated_at: now_timestamp(),
            ..current
        };
        let stored = store
            .replace_if_revision(updated, command.expected_revision)
            .map_err(|e| self.work_item_failure("transition work item", e))?;
        self.record_work_item_mutation(
            actor,
            &command.organization_id,
            &stored,
            EventKind::StatusChanged,
            format!(
                "transitioned work item to {}",
                Self::status_name(command.to_status)
            ),
        )?;
        Ok(ProtocolOutcome::WorkItemTransitioned(stored))
    }

    /// Post-commit audit: append the activity fact and the replayable
    /// control event with the framed request actor. The activity `event_id`
    /// and the control-event per-aggregate `sequence` are both derived from
    /// the item's new revision, so every mutation lands exactly once and a
    /// retried command re-observes its own audit trail rather than
    /// duplicating it. A failed append surfaces typed — never silent.
    fn record_work_item_mutation(
        &self,
        actor: &altai_control_protocol::Actor,
        organization_id: &OrganizationId,
        item: &WorkItem,
        kind: EventKind,
        summary: String,
    ) -> Result<(), ProtocolError> {
        let timestamp = now_timestamp();
        if let Some(store) = &self.activity {
            let event = altai_control_protocol::ActivityEvent {
                event_id: format!("work_item_{}_{}", item.id.value, item.revision.value()),
                kind,
                actor: actor.clone(),
                timestamp: timestamp.clone(),
                organization_id: organization_id.clone(),
                project_id: Some(item.project_id.clone()),
                work_item_id: Some(item.id.clone()),
                attempt_id: None,
                summary,
                correlation_id: None,
                causation_id: None,
            };
            store
                .append(event)
                .map_err(|e| self.store_failure("activity append", e))?;
        }
        if let Some(store) = &self.control_events {
            let event = altai_control_protocol::ControlEvent {
                aggregate: "work_item".to_string(),
                aggregate_id: serde_json::json!({
                    "type": item.id.kind,
                    "value": item.id.value
                }),
                sequence: item.revision.value(),
                kind,
                actor: actor.clone(),
                timestamp,
                revision: item.revision,
                payload: serde_json::to_value(item)
                    .map_err(|e| self.store_failure("control event encode", e))?,
                correlation_id: None,
                causation_id: None,
            };
            store
                .append(organization_id, &event)
                .map_err(|e| self.store_failure("control event append", e))?;
        }
        Ok(())
    }

    fn require_typed_id(
        kind: &str,
        expected: &str,
        field: &str,
    ) -> Result<(), ProtocolError> {
        if kind == expected {
            Ok(())
        } else {
            Err(ProtocolError::new(
                ControlErrorCode::InvalidId,
                format!("{field} must be a {expected} id"),
            ))
        }
    }

    /// Audit attribution is structural, not conventional: a deployment that
    /// cannot record the activity fact and the replayable control event does
    /// not get to mutate canonical work. Every in-repo host wires both.
    fn require_audit_wiring(&self) -> Result<(), ProtocolError> {
        if self.activity.is_some() && self.control_events.is_some() {
            Ok(())
        } else {
            Err(ProtocolError::new(
                ControlErrorCode::PolicyDenied,
                "work-item mutations require wired activity_audit and event_replay stores",
            ))
        }
    }

    /// (5) Scope resolution shared by both mutation pipelines: the project
    /// must exist, and the organization the command claims must be the
    /// project's actual one. Cross-organization attribution fails closed.
    fn require_project_organization(
        store: &Arc<dyn WorkItemRepository>,
        claimed: &OrganizationId,
        project_id: &ProjectId,
    ) -> Result<(), ProtocolError> {
        let actual = store
            .project_organization(project_id)
            .map_err(Self::project_organization_failure)?;
        if actual == *claimed {
            Ok(())
        } else {
            Err(ProtocolError::new(
                ControlErrorCode::PolicyDenied,
                format!(
                    "organization {} does not contain project {}",
                    claimed.value, project_id.value
                ),
            ))
        }
    }

    fn project_organization_failure(error: crate::WorkItemRepositoryError) -> ProtocolError {
        match error {
            crate::WorkItemRepositoryError::ProjectNotFound { project_id } => ProtocolError::new(
                ControlErrorCode::NotFound,
                format!("project not found: {project_id}"),
            ),
            other => ProtocolError::new(
                ControlErrorCode::InternalError,
                format!("project resolution failed: {other}"),
            ),
        }
    }

    fn require_bounded(
        field: &str,
        value: &str,
        max_bytes: usize,
    ) -> Result<(), ProtocolError> {
        if value.len() <= max_bytes {
            Ok(())
        } else {
            Err(ProtocolError::new(
                ControlErrorCode::PayloadTooLarge,
                format!("{field} exceeds the {max_bytes} byte limit"),
            ))
        }
    }

    fn status_name(status: WorkStatus) -> &'static str {
        match status {
            WorkStatus::Backlog => "backlog",
            WorkStatus::Todo => "todo",
            WorkStatus::InProgress => "in_progress",
            WorkStatus::InReview => "in_review",
            WorkStatus::Done => "done",
            WorkStatus::Blocked => "blocked",
            WorkStatus::Cancelled => "cancelled",
        }
    }

    fn work_item_failure(
        &self,
        operation: &str,
        error: crate::WorkItemRepositoryError,
    ) -> ProtocolError {
        let (code, message) = match error {
            crate::WorkItemRepositoryError::AlreadyExists { work_item_id } => (
                ControlErrorCode::Conflict,
                format!("{operation}: work item already exists: {work_item_id}"),
            ),
            crate::WorkItemRepositoryError::NotFound { work_item_id } => (
                ControlErrorCode::NotFound,
                format!("{operation}: work item not found: {work_item_id}"),
            ),
            crate::WorkItemRepositoryError::ProjectNotFound { project_id } => (
                ControlErrorCode::NotFound,
                format!("{operation}: project not found: {project_id}"),
            ),
            crate::WorkItemRepositoryError::ProjectMismatch { work_item_id } => (
                ControlErrorCode::NotFound,
                format!("{operation}: work item not found in project: {work_item_id}"),
            ),
            crate::WorkItemRepositoryError::StaleRevision { work_item_id } => (
                ControlErrorCode::StaleRevision,
                format!("{operation}: stale revision for work item: {work_item_id}"),
            ),
            crate::WorkItemRepositoryError::Internal { reason } => (
                ControlErrorCode::InternalError,
                format!("{operation} failed: {reason}"),
            ),
        };
        ProtocolError::new(code, message)
    }

    fn store_failure(
        &self,
        operation: &str,
        error: impl std::fmt::Display,
    ) -> ProtocolError {
        ProtocolError::new(
            ControlErrorCode::InternalError,
            format!("{operation} failed: {error}"),
        )
    }

    fn unsupported(&self, capability: &str) -> ProtocolError {
        ProtocolError::new(
            ControlErrorCode::PolicyDenied,
            format!("capability not served by this deployment: {capability}"),
        )
    }
}

fn now_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BootstrapCredential, ControlPlane, ControlPlaneConfig, ControlPlaneStore,
        InMemoryAgentRepository, InMemoryScopeRepository, InMemoryWakeRepository,
        InMemoryWorkGraphRepository, ProtocolDispatcher, ScopeRepository, SqliteActivityEventRepository,
        SqliteAttemptRepository, SqliteApprovalRepository, SqliteControlEventRepository,
        SqliteRoutineRepository, SqliteRunBindingRepository, SqliteScopeRepository,
        SqliteWorkItemRepository, router_with_control_repositories,
    };
    use altai_control_protocol::{
        Actor, EventKind, ExecutionPhase, OrganizationId, PageRequest, Project, ProjectId,
        ProjectStatus, ProtocolCommand, Revision, WorkItem, WorkItemId, WorkItemKind, WorkStatus,
        CreateWorkItemCommand, TransitionWorkItemCommand,
    };
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode, header::AUTHORIZATION},
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    const BOOTSTRAP_TOKEN: &str = "test-bootstrap-token";

    struct Harness {
        _dir: tempfile::TempDir,
        app: Router,
        dispatcher: Arc<ProtocolDispatcher>,
        activity: Arc<SqliteActivityEventRepository>,
        control_events: Arc<SqliteControlEventRepository>,
        work_items: Arc<SqliteWorkItemRepository>,
    }

    fn seeded_project(dir: &std::path::Path) -> (Arc<SqliteWorkItemRepository>, ProjectId, OrganizationId) {
        let work_db = dir.join("work.db");
        let scope = Arc::new(SqliteScopeRepository::open(&work_db).unwrap());
        let organization_id = OrganizationId::new("org");
        scope
            .create_organization(altai_control_protocol::Organization {
                id: organization_id.clone(),
                name: "Harness org".into(),
                revision: Revision::INITIAL,
                created_at: "2026-09-01T00:00:00.000Z".into(),
                updated_at: "2026-09-01T00:00:00.000Z".into(),
            })
            .unwrap();
        let project_id = ProjectId::new("proj");
        scope
            .create_project(Project {
                id: project_id.clone(),
                organization_id: organization_id.clone(),
                goal_ids: Vec::new(),
                name: "Harness project".into(),
                description: String::new(),
                status: ProjectStatus::Active,
                revision: Revision::INITIAL,
                created_at: "2026-09-01T00:00:00.000Z".into(),
                updated_at: "2026-09-01T00:00:00.000Z".into(),
            })
            .unwrap();
        let work_items = Arc::new(SqliteWorkItemRepository::open(&work_db).unwrap());
        (work_items, project_id, organization_id)
    }

    fn harness() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let (work_items, _project_id, _organization_id) = seeded_project(dir.path());
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".to_string(),
                store: ControlPlaneStore::Sqlite {
                    database_path: dir.path().join("plane.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let activity = Arc::new(
            SqliteActivityEventRepository::open(&dir.path().join("activity.db")).unwrap(),
        );
        let control_events = Arc::new(
            SqliteControlEventRepository::open(&dir.path().join("events.db")).unwrap(),
        );
        let capabilities =
            capabilities_from_wiring(true, true, true, true, true, true, true, true, true);
        let dispatcher = Arc::new(
            ProtocolDispatcher::new(DeploymentMode::LocalDaemon, capabilities)
                .with_work_item_repository(work_items.clone())
                .with_activity_repository(activity.clone())
                .with_control_event_repository(control_events.clone()),
        );
        // The router builder constructs its own dispatcher from the same
        // wiring (all optional repositories present), so the local and
        // deployed sides below share one capability truth by construction.
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext(BOOTSTRAP_TOKEN).unwrap(),
            Some(Arc::new(InMemoryScopeRepository::default())),
            Some(Arc::new(InMemoryAgentRepository::default())),
            Some(Arc::new(InMemoryWorkGraphRepository::default())),
            Some(work_items.clone() as Arc<dyn WorkItemRepository>),
            Arc::new(InMemoryWakeRepository::default()),
            Some(Arc::new(
                SqliteRunBindingRepository::open(&dir.path().join("bindings.db")).unwrap(),
            )),
            Some(Arc::new(
                SqliteAttemptRepository::open(&dir.path().join("attempts.db")).unwrap(),
            )),
            Some(Arc::new(
                SqliteRoutineRepository::open(&dir.path().join("routines.db")).unwrap(),
            )),
            Some(Arc::new(
                SqliteApprovalRepository::open(&dir.path().join("approvals.db")).unwrap(),
            )),
            Some(activity.clone()),
            Some(control_events.clone()),
            None,
        );
        Harness {
            _dir: dir,
            app,
            dispatcher,
            activity,
            control_events,
            work_items,
        }
    }

    fn request(id: &str, version_major: u16, payload: ProtocolCommand) -> ProtocolRequest<ProtocolCommand> {
        ProtocolRequest {
            id: id.to_string(),
            version: ProtocolVersion::new(version_major, 0),
            actor: Actor::System {
                component: "conformance-test".to_string(),
            },
            payload,
        }
    }

    fn negotiate_payload() -> ProtocolCommand {
        ProtocolCommand::NegotiateCapabilities(CapabilityNegotiationRequest {
            client_version: ProtocolVersion::CURRENT,
            client_name: "altai-conformance".to_string(),
            required_capabilities: vec!["organizations".to_string()],
        })
    }

    async fn post_command(
        h: &Harness,
        body: &ProtocolRequest<ProtocolCommand>,
    ) -> ProtocolResponse<ProtocolOutcome> {
        let response = h
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/protocol/commands")
                    .header(
                        AUTHORIZATION,
                        format!("Bearer {BOOTSTRAP_TOKEN}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1_048_576)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn capabilities_reflect_wired_repositories() {
        let wired =
            capabilities_from_wiring(true, true, true, true, true, true, true, true, true);
        assert!(wired.organizations && wired.agents && wired.work_graph && wired.attempts);
        assert!(wired.activity_audit && wired.event_replay);
        assert!(!wired.budgets && !wired.evidence && !wired.workspace_scopes);

        // The work-graph capability is honest: it is advertised when either
        // work-graph repository family is wired — and the canonical
        // mutation surface only counts when its audit stores are wired too.
        let no_work = capabilities_from_wiring(true, true, false, false, true, true, true, true, true);
        assert!(!no_work.work_graph);
        let work_items_only =
            capabilities_from_wiring(true, true, false, true, true, true, true, true, true);
        assert!(work_items_only.work_graph);
        let unattributed_mutations =
            capabilities_from_wiring(true, true, false, true, true, true, true, false, true);
        assert!(!unattributed_mutations.work_graph);

        let bare =
            capabilities_from_wiring(false, false, false, false, false, false, false, false, false);
        assert!(!bare.organizations && !bare.attempts && !bare.activity_audit);
        assert!(!bare.event_replay);
    }

    #[tokio::test]
    async fn negotiation_conforms_across_local_and_deployed_transports() {
        let h = harness();
        let body = request("req-1", 1, negotiate_payload());
        let local = h.dispatcher.execute(&body);
        let deployed = post_command(&h, &body).await;
        assert_eq!(local, deployed);
        match local.result {
            Ok(ProtocolOutcome::Negotiated(response)) => {
                assert!(response.compatible);
                assert_eq!(response.deployment_mode, DeploymentMode::LocalDaemon);
            }
            other => panic!("expected negotiated outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn version_mismatch_is_denied_identically_on_both_transports() {
        let h = harness();
        let body = request("req-2", 2, negotiate_payload());
        let local = h.dispatcher.execute(&body);
        let deployed = post_command(&h, &body).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("major version 2"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
    }

    fn replay_payload() -> ProtocolCommand {
        ProtocolCommand::ReplayEvents(altai_control_protocol::EventReplayRequest::new(
            OrganizationId::new("org"),
            0,
            None,
        ))
    }

    #[tokio::test]
    async fn replay_conforms_across_local_and_deployed_transports() {
        let h = harness();
        for (aggregate, aggregate_id, sequence) in
            [("work_item", "wi_1", 1u64), ("attempt", "at_1", 1), ("work_item", "wi_1", 2)]
        {
            h.control_events
                .append(
                    &OrganizationId::new("org"),
                    &altai_control_protocol::ControlEvent {
                        aggregate: aggregate.to_string(),
                        aggregate_id: serde_json::json!({ "value": aggregate_id }),
                        sequence,
                        kind: altai_control_protocol::EventKind::Updated,
                        actor: Actor::System {
                            component: "conformance-test".into(),
                        },
                        timestamp: "2026-08-15T00:00:00Z".into(),
                        revision: altai_control_protocol::Revision::new(sequence),
                        payload: serde_json::json!({ "aggregate": aggregate }),
                        correlation_id: None,
                        causation_id: None,
                    },
                )
                .unwrap();
        }
        let body = request("req-3", 1, replay_payload());
        let local = h.dispatcher.execute(&body);
        let deployed = post_command(&h, &body).await;
        assert_eq!(local, deployed);
        match local.result {
            Ok(ProtocolOutcome::Replayed(replayed)) => {
                // Global replay order, not per-aggregate order.
                let order: Vec<(&str, u64)> = replayed
                    .events
                    .iter()
                    .map(|e| {
                        (
                            e.aggregate_id["value"].as_str().unwrap(),
                            e.sequence,
                        )
                    })
                    .collect();
                assert_eq!(
                    order,
                    vec![("wi_1", 1), ("at_1", 1), ("wi_1", 2)]
                );
                assert!(!replayed.has_more);
                assert!(replayed.next_sequence >= 3);
            }
            other => panic!("expected replayed outcome, got {other:?}"),
        }
    }

    #[test]
    fn replay_without_a_store_is_typed_denied() {
        let bare = ProtocolDispatcher::new(
            DeploymentMode::EmbeddedHost,
            capabilities_from_wiring(true, true, true, false, true, true, true, true, false),
        );
        let response = bare.execute(&request("req-3b", 1, replay_payload()));
        match response.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("event_replay"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
    }

    fn activity_payload() -> ProtocolCommand {
        ProtocolCommand::QueryActivity(altai_control_protocol::ActivityQueryRequest {
            organization_id: OrganizationId::new("org"),
            page: PageRequest::default(),
            kind: None,
            work_item_id: None,
        })
    }

    #[tokio::test]
    async fn activity_query_conforms_across_local_and_deployed_transports() {
        let h = harness();
        for index in 1..=3 {
            h.activity
                .append(altai_control_protocol::ActivityEvent {
                    event_id: format!("evt_{index}"),
                    kind: altai_control_protocol::EventKind::Created,
                    actor: Actor::System {
                        component: "conformance-test".into(),
                    },
                    timestamp: "2026-08-15T00:00:00Z".into(),
                    organization_id: OrganizationId::new("org"),
                    project_id: None,
                    work_item_id: None,
                    attempt_id: None,
                    summary: format!("event {index}"),
                    correlation_id: None,
                    causation_id: None,
                })
                .unwrap();
        }
        let body = request("req-4", 1, activity_payload());
        let local = h.dispatcher.execute(&body);
        let deployed = post_command(&h, &body).await;
        assert_eq!(local, deployed);
        match local.result {
            Ok(ProtocolOutcome::Activity(page)) => {
                let ids: Vec<&str> = page.items.iter().map(|e| e.event_id.as_str()).collect();
                assert_eq!(ids, vec!["evt_1", "evt_2", "evt_3"]);
                assert!(!page.has_more);
            }
            other => panic!("expected activity page, got {other:?}"),
        }
    }

    #[test]
    fn activity_query_without_a_store_is_typed_denied() {
        let bare = ProtocolDispatcher::new(
            DeploymentMode::EmbeddedHost,
            capabilities_from_wiring(true, true, true, false, true, true, false, true, false),
        );
        let response = bare.execute(&request("req-4b", 1, activity_payload()));
        match response.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("activity_audit"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
    }

    fn work_item_id(tag: &str) -> WorkItemId {
        WorkItemId::new(format!("01923abc-def0-7abc-8def-01234567890{tag}"))
    }

    fn create_payload(tag: &str) -> ProtocolCommand {
        ProtocolCommand::CreateWorkItem(CreateWorkItemCommand {
            organization_id: OrganizationId::new("org"),
            project_id: ProjectId::new("proj"),
            work_item_id: work_item_id(tag),
            goal_id: None,
            parent_work_item_id: None,
            kind: WorkItemKind::Task,
            title: "Harness work item".to_string(),
            description: "born through the protocol".into(),
        })
    }

    fn transition_payload(tag: &str, to_status: WorkStatus, expected_revision: Revision) -> ProtocolCommand {
        ProtocolCommand::TransitionWorkItem(TransitionWorkItemCommand {
            organization_id: OrganizationId::new("org"),
            project_id: ProjectId::new("proj"),
            work_item_id: work_item_id(tag),
            to_status,
            expected_revision,
        })
    }

    fn expect_created(result: Result<ProtocolOutcome, ProtocolError>) -> WorkItem {
        match result {
            Ok(ProtocolOutcome::WorkItemCreated(item)) => item,
            other => panic!("expected created outcome, got {other:?}"),
        }
    }

    fn expect_transitioned(result: Result<ProtocolOutcome, ProtocolError>) -> WorkItem {
        match result {
            Ok(ProtocolOutcome::WorkItemTransitioned(item)) => item,
            other => panic!("expected transitioned outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn work_item_create_conforms_across_local_and_deployed_transports() {
        let h = harness();
        // Happy path: both transports drive the same birth state (same
        // command shape, same transition) — one row per unique id, so the
        // ids differ while every invariant must match.
        let local = expect_created(
            h.dispatcher
                .execute(&request("req-w1", 1, create_payload("1")))
                .result,
        );
        let deployed = expect_created(
            post_command(&h, &request("req-w1d", 1, create_payload("1d")))
                .await
                .result,
        );
        for item in [&local, &deployed] {
            assert_eq!(item.status, WorkStatus::Backlog);
            assert_eq!(item.execution_phase, ExecutionPhase::None);
            assert_eq!(item.revision, Revision::INITIAL);
        }
        assert_eq!(local.title, deployed.title);
        // Deterministic failure: the second create of the same id answers
        // the byte-identical typed conflict on both transports.
        let replay = request("req-w1", 1, create_payload("1"));
        let local_conflict = h.dispatcher.execute(&replay);
        let deployed_conflict = post_command(&h, &replay).await;
        assert_eq!(local_conflict, deployed_conflict);
        match local_conflict.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::Conflict),
            other => panic!("expected conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn work_item_transition_conforms_across_local_and_deployed_transports() {
        let h = harness();
        h.dispatcher
            .execute(&request("req-w2", 1, create_payload("2")))
            .result
            .unwrap();
        let body = request(
            "req-w2b",
            1,
            transition_payload("2", WorkStatus::InProgress, Revision::INITIAL),
        );
        let local = expect_transitioned(h.dispatcher.execute(&body).result);
        assert_eq!(local.status, WorkStatus::InProgress);
        assert_eq!(local.revision, Revision::new(1));
        // Read-your-write through the canonical store as well.
        assert_eq!(
            h.work_items.get(&work_item_id("2")).unwrap().status,
            WorkStatus::InProgress
        );
        // Execution phase stays dispatch-owned; the protocol never writes it.
        assert_eq!(local.execution_phase, ExecutionPhase::None);

        // The deployed transport drives the same transition on a fresh item.
        h.dispatcher
            .execute(&request("req-w2c", 1, create_payload("2c")))
            .result
            .unwrap();
        let deployed = expect_transitioned(
            post_command(
                &h,
                &request(
                    "req-w2d",
                    1,
                    transition_payload("2c", WorkStatus::InProgress, Revision::INITIAL),
                ),
            )
            .await
            .result,
        );
        assert_eq!(deployed.status, local.status);
        assert_eq!(deployed.revision, local.revision);

        // Deterministic failure: replaying the already-applied transition
        // (stale revision now) answers byte-identically on both transports.
        let replay = request(
            "req-w2b",
            1,
            transition_payload("2", WorkStatus::InProgress, Revision::INITIAL),
        );
        let local_stale = h.dispatcher.execute(&replay);
        let deployed_stale = post_command(&h, &replay).await;
        assert_eq!(local_stale, deployed_stale);
        match local_stale.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::StaleRevision),
            other => panic!("expected stale revision, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn work_item_mutations_append_attributed_audit_and_replay_events() {
        let h = harness();
        h.dispatcher
            .execute(&request("req-w3", 1, create_payload("3")))
            .result
            .unwrap();
        h.dispatcher
            .execute(&request(
                "req-w3b",
                1,
                transition_payload("3", WorkStatus::Todo, Revision::INITIAL),
            ))
            .result
            .unwrap();
        let activity = h
            .activity
            .query(&altai_control_protocol::ActivityQueryRequest {
                organization_id: OrganizationId::new("org"),
                page: PageRequest::default(),
                kind: None,
                work_item_id: Some(work_item_id("3")),
            })
            .unwrap();
        assert_eq!(activity.items.len(), 2);
        assert_eq!(activity.items[0].kind, EventKind::Created);
        assert_eq!(activity.items[1].kind, EventKind::StatusChanged);
        let replayed = h
            .control_events
            .replay(&altai_control_protocol::EventReplayRequest::new(
                OrganizationId::new("org"),
                0,
                None,
            ))
            .unwrap();
        assert_eq!(replayed.events.len(), 2);
        assert_eq!(replayed.events[0].sequence, 0);
        assert_eq!(replayed.events[1].sequence, 1);
        assert_eq!(replayed.events[1].aggregate, "work_item");
    }

    #[tokio::test]
    async fn work_item_failures_are_typed_and_identical_on_both_transports() {
        let h = harness();
        h.dispatcher
            .execute(&request("req-w4", 1, create_payload("4")))
            .result
            .unwrap();

        let duplicate = request("req-w4a", 1, create_payload("4"));
        let local = h.dispatcher.execute(&duplicate);
        let deployed = post_command(&h, &duplicate).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::Conflict),
            other => panic!("expected conflict, got {other:?}"),
        }

        let stale = request(
            "req-w4b",
            1,
            transition_payload("4", WorkStatus::Todo, Revision::new(7)),
        );
        let local = h.dispatcher.execute(&stale);
        let deployed = post_command(&h, &stale).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::StaleRevision),
            other => panic!("expected stale revision, got {other:?}"),
        }

        let unknown_project = request("req-w4c", 1, {
            let mut payload = match create_payload("4") {
                ProtocolCommand::CreateWorkItem(inner) => inner,
                other => panic!("unexpected payload {other:?}"),
            };
            payload.project_id = ProjectId::new("nope");
            ProtocolCommand::CreateWorkItem(payload)
        });
        let local = h.dispatcher.execute(&unknown_project);
        let deployed = post_command(&h, &unknown_project).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::NotFound);
                assert!(error.message.contains("project not found"));
            }
            other => panic!("expected not-found, got {other:?}"),
        }

        let unknown_item = request(
            "req-w4d",
            1,
            transition_payload("404", WorkStatus::Todo, Revision::INITIAL),
        );
        let local = h.dispatcher.execute(&unknown_item);
        let deployed = post_command(&h, &unknown_item).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::NotFound),
            other => panic!("expected not-found, got {other:?}"),
        }

        let oversized = request("req-w4e", 1, {
            let mut payload = match create_payload("5") {
                ProtocolCommand::CreateWorkItem(inner) => inner,
                other => panic!("unexpected payload {other:?}"),
            };
            payload.title = "x".repeat(MAX_WORK_ITEM_TITLE_BYTES + 1);
            ProtocolCommand::CreateWorkItem(payload)
        });
        let local = h.dispatcher.execute(&oversized);
        let deployed = post_command(&h, &oversized).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::PayloadTooLarge),
            other => panic!("expected payload too large, got {other:?}"),
        }

        let foreign_id = request("req-w4f", 1, {
            let mut payload = match create_payload("6") {
                ProtocolCommand::CreateWorkItem(inner) => inner,
                other => panic!("unexpected payload {other:?}"),
            };
            payload.work_item_id = WorkItemId {
                kind: "project_id".to_string(),
                value: "not-a-work-item".to_string(),
            };
            ProtocolCommand::CreateWorkItem(payload)
        });
        let local = h.dispatcher.execute(&foreign_id);
        let deployed = post_command(&h, &foreign_id).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::InvalidId),
            other => panic!("expected invalid id, got {other:?}"),
        }

        let oversized_description = request("req-w4g", 1, {
            let mut payload = match create_payload("6b") {
                ProtocolCommand::CreateWorkItem(inner) => inner,
                other => panic!("unexpected payload {other:?}"),
            };
            payload.description = "x".repeat(MAX_WORK_ITEM_DESCRIPTION_BYTES + 1);
            ProtocolCommand::CreateWorkItem(payload)
        });
        let local = h.dispatcher.execute(&oversized_description);
        let deployed = post_command(&h, &oversized_description).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::PayloadTooLarge),
            other => panic!("expected payload too large, got {other:?}"),
        }

        let foreign_goal = request("req-w4h", 1, {
            let mut payload = match create_payload("6c") {
                ProtocolCommand::CreateWorkItem(inner) => inner,
                other => panic!("unexpected payload {other:?}"),
            };
            payload.goal_id = Some(altai_control_protocol::GoalId {
                kind: "workspace_id".to_string(),
                value: "not-a-goal".to_string(),
            });
            ProtocolCommand::CreateWorkItem(payload)
        });
        let local = h.dispatcher.execute(&foreign_goal);
        let deployed = post_command(&h, &foreign_goal).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => assert_eq!(error.code, ControlErrorCode::InvalidId),
            other => panic!("expected invalid id, got {other:?}"),
        }
    }

    #[test]
    fn work_item_commands_without_wiring_are_typed_denied() {
        let bare = ProtocolDispatcher::new(
            DeploymentMode::EmbeddedHost,
            capabilities_from_wiring(true, true, true, false, true, true, true, true, true),
        );
        let response = bare.execute(&request("req-w5", 1, create_payload("7")));
        match response.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("work_graph"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
        let response = bare.execute(&request(
            "req-w5b",
            1,
            transition_payload("7", WorkStatus::Todo, Revision::INITIAL),
        ));
        match response.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("work_graph"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
    }

    #[test]
    fn work_item_mutations_without_wired_audit_stores_are_typed_denied() {
        // The work-item store alone is not enough: audit attribution is
        // structural, so a dispatcher that cannot record the activity fact
        // and the replayable control event refuses mutations typed-closed.
        let unattributed = ProtocolDispatcher::new(
            DeploymentMode::EmbeddedHost,
            capabilities_from_wiring(true, true, false, true, true, true, true, false, false),
        )
        .with_work_item_repository(harness().work_items);
        for (id, payload) in [
            ("req-w5c", create_payload("7c")),
            (
                "req-w5d",
                transition_payload("7c", WorkStatus::Todo, Revision::INITIAL),
            ),
        ] {
            let response = unattributed.execute(&request(id, 1, payload));
            match response.result {
                Err(error) => {
                    assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                    assert!(error.message.contains("activity_audit"), "{error:?}");
                }
                other => panic!("expected typed error, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn work_item_command_organization_must_match_the_project() {
        let h = harness();
        let foreign_org = request("req-w7", 1, {
            let mut payload = match create_payload("9") {
                ProtocolCommand::CreateWorkItem(inner) => inner,
                other => panic!("unexpected payload {other:?}"),
            };
            payload.organization_id = OrganizationId::new("other");
            ProtocolCommand::CreateWorkItem(payload)
        });
        let local = h.dispatcher.execute(&foreign_org);
        let deployed = post_command(&h, &foreign_org).await;
        assert_eq!(local, deployed);
        match local.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("does not contain project"));
            }
            other => panic!("expected typed denial, got {other:?}"),
        }
        // No row and no audit event may exist for the rejected create.
        assert!(h.work_items.get(&work_item_id("9")).is_err());
        assert!(h
            .control_events
            .replay(&altai_control_protocol::EventReplayRequest::new(
                OrganizationId::new("other"),
                0,
                None
            ))
            .unwrap()
            .events
            .is_empty());
    }

    #[test]
    fn concurrent_transitions_from_the_same_revision_admit_exactly_one() {
        let h = harness();
        h.dispatcher
            .execute(&request("req-w8", 1, create_payload("10")))
            .result
            .unwrap();
        let first = request(
            "req-w8a",
            1,
            transition_payload("10", WorkStatus::InProgress, Revision::INITIAL),
        );
        let second = request(
            "req-w8b",
            1,
            transition_payload("10", WorkStatus::Blocked, Revision::INITIAL),
        );
        let dispatcher = h.dispatcher.clone();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let first_barrier = barrier.clone();
        let second_barrier = barrier.clone();
        let (first_result, second_result) = std::thread::scope(|scope| {
            let dispatcher_b = dispatcher.clone();
            let first_handle = scope.spawn(move || {
                first_barrier.wait();
                dispatcher.execute(&first)
            });
            let second_handle = scope.spawn(move || {
                second_barrier.wait();
                dispatcher_b.execute(&second)
            });
            (first_handle.join().unwrap(), second_handle.join().unwrap())
        });
        let outcomes = [first_result.result, second_result.result];
        let wins = outcomes
            .iter()
            .filter(|r| matches!(r, Ok(ProtocolOutcome::WorkItemTransitioned(_))))
            .count();
        let stale = outcomes
            .iter()
            .filter(|r| matches!(
                r,
                Err(error) if error.code == ControlErrorCode::StaleRevision
            ))
            .count();
        assert_eq!(
            (wins, stale),
            (1, 1),
            "exactly one concurrent transition may win: {outcomes:?}"
        );
        let final_item = h.work_items.get(&work_item_id("10")).unwrap();
        assert_eq!(final_item.revision, Revision::new(1));
        assert!(
            final_item.status == WorkStatus::InProgress || final_item.status == WorkStatus::Blocked,
            "final status must be exactly one of the two contenders"
        );
    }

    #[test]
    fn version_mismatch_wins_before_any_work_item_mutation() {
        let h = harness();
        let stale_version = request("req-w6", 2, create_payload("8"));
        let response = h.dispatcher.execute(&stale_version);
        match response.result {
            Err(error) => {
                assert_eq!(error.code, ControlErrorCode::PolicyDenied);
                assert!(error.message.contains("major version 2"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
        assert!(h.work_items.get(&work_item_id("8")).is_err());
    }

    #[tokio::test]
    async fn standalone_negotiate_route_matches_the_dispatched_command() {
        let h = harness();
        let negotiation = CapabilityNegotiationRequest {
            client_version: ProtocolVersion::CURRENT,
            client_name: "altai-conformance".to_string(),
            required_capabilities: vec!["organizations".to_string()],
        };
        let response = h
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/protocol/negotiate")
                    .header(
                        AUTHORIZATION,
                        format!("Bearer {BOOTSTRAP_TOKEN}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&negotiation).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1_048_576)
            .await
            .unwrap();
        let standalone: CapabilityNegotiationResponse = serde_json::from_slice(&bytes).unwrap();
        let via_command = h
            .dispatcher
            .execute(&request("req-5", 1, negotiate_payload()));
        match via_command.result {
            Ok(ProtocolOutcome::Negotiated(framed)) => assert_eq!(standalone, framed),
            other => panic!("expected negotiated outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn protocol_routes_require_bootstrap_bearer() {
        let h = harness();
        let response = h
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/protocol/commands")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&request("req-6", 1, negotiate_payload())).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
