//! Authenticated loopback HTTP transport for the control-plane bootstrap.
//!
//! The daemon binds only a loopback address in this milestone. A deployed
//! transport requires a separate TLS/proxy and credential-custody design; this
//! module deliberately refuses to make an accidental unauthenticated listener.

use crate::{
    capabilities_from_wiring, finalize_attempt, ActivityEventRepository, AgentRepository,
    AgentRepositoryError, ApprovalError, ApprovalRepository, AttemptFinalization,
    AttemptFinalizationError, AttemptRepository, ControlEventRepository, ControlPlane,
    ControlPlaneError, PluginRegistry, PluginRegistryError, ProtocolDispatcher, RegistrationGrant,
    RoutineError, RoutineRepository, WorkItemRepository,
    RunBindingError, RunBindingRepository, RunOutcome, ScopeError, ScopeRepository, WakeError,
    WakeRepository, WorkGraphError, WorkGraphRepository,
};
use altai_control_protocol::{
    AgentInstance, AgentProfileRevision, Approval, ApprovalDecision, ApprovalId, ApprovalOutcome,
    Attempt, AttemptId, CapabilityNegotiationRequest, CapabilityNegotiationResponse,
    ControlPlaneHealth, DeploymentMode, Goal, GoalId, HostRegistrationRequest, Organization,
    OrganizationId, PluginManifest, Project, ProjectWorkspace, ProtocolCommand, ProtocolOutcome,
    ProtocolRequest,
    ProtocolResponse, RegisteredHost, Routine, RoutineId, RoutineRevision, RunBinding, WakeRequest,
    WakeSource, WorkCheckoutLease, WorkComment, WorkDependency, WorkItemId,
};
use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;

/// Bootstrap bearer credential used by an administrator to inspect health and
/// issue a one-time host-registration grant. The plaintext value is never
/// retained after construction and this type intentionally does not implement
/// `Debug`.
pub struct BootstrapCredential {
    digest: [u8; 32],
}

impl BootstrapCredential {
    pub fn from_plaintext(token: &str) -> Result<Self, TransportError> {
        if token.len() < 16 {
            return Err(TransportError::UnsafeConfiguration {
                reason: "bootstrap token must contain at least 16 bytes".to_string(),
            });
        }
        Ok(Self {
            digest: digest(token),
        })
    }

    fn authorizes(&self, headers: &HeaderMap) -> bool {
        let Some(value) = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        else {
            return false;
        };
        let Some(token) = value.strip_prefix("Bearer ") else {
            return false;
        };
        self.digest.ct_eq(&digest(token)).into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    UnsafeConfiguration { reason: String },
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsafeConfiguration { reason } => {
                write!(f, "unsafe control-plane transport configuration: {reason}")
            }
        }
    }
}

impl std::error::Error for TransportError {}

#[derive(Clone)]
struct ApiState {
    plane: Arc<ControlPlane>,
    bootstrap_credential: Arc<BootstrapCredential>,
    scope_repository: Option<Arc<dyn ScopeRepository>>,
    agent_repository: Option<Arc<dyn AgentRepository>>,
    work_graph_repository: Option<Arc<dyn WorkGraphRepository>>,
    wake_repository: Option<Arc<dyn WakeRepository>>,
    run_binding_repository: Option<Arc<dyn RunBindingRepository>>,
    attempt_repository: Option<Arc<dyn AttemptRepository>>,
    routine_repository: Option<Arc<dyn RoutineRepository>>,
    approval_repository: Option<Arc<dyn ApprovalRepository>>,
    plugin_registry: Option<Arc<dyn PluginRegistry>>,
    protocol_dispatcher: Arc<ProtocolDispatcher>,
}

/// Routes are versioned from their first public exposure. The health and grant
/// routes need bootstrap authentication; host registration authenticates using
/// its one-time grant in the body and therefore does not accept the bootstrap
/// credential as a substitute.
pub fn router(plane: Arc<ControlPlane>, bootstrap_credential: BootstrapCredential) -> Router {
    router_with_all_repositories(plane, bootstrap_credential, None, None, None)
}

/// Build the authenticated transport with an optional CP-04 scope store.
/// Scope routes are absent until the daemon has a durable repository.
pub fn router_with_scope_repository(
    plane: Arc<ControlPlane>,
    bootstrap_credential: BootstrapCredential,
    scope_repository: Option<Arc<dyn ScopeRepository>>,
) -> Router {
    router_with_all_repositories(plane, bootstrap_credential, scope_repository, None, None)
}

pub fn router_with_repositories(
    plane: Arc<ControlPlane>,
    bootstrap_credential: BootstrapCredential,
    scope_repository: Option<Arc<dyn ScopeRepository>>,
    agent_repository: Option<Arc<dyn AgentRepository>>,
) -> Router {
    router_with_all_repositories(
        plane,
        bootstrap_credential,
        scope_repository,
        agent_repository,
        None,
    )
}

pub fn router_with_all_repositories(
    plane: Arc<ControlPlane>,
    bootstrap_credential: BootstrapCredential,
    scope_repository: Option<Arc<dyn ScopeRepository>>,
    agent_repository: Option<Arc<dyn AgentRepository>>,
    work_graph_repository: Option<Arc<dyn WorkGraphRepository>>,
) -> Router {
    let capabilities = capabilities_from_wiring(
        scope_repository.is_some(),
        agent_repository.is_some(),
        work_graph_repository.is_some(),
        false,
        false,
        false,
        false,
        false,
        false,
    );
    let state = ApiState {
        plane,
        bootstrap_credential: Arc::new(bootstrap_credential),
        scope_repository,
        agent_repository,
        work_graph_repository,
        wake_repository: None,
        run_binding_repository: None,
        attempt_repository: None,
        routine_repository: None,
        approval_repository: None,
        plugin_registry: None,
        protocol_dispatcher: Arc::new(ProtocolDispatcher::new(
            DeploymentMode::LocalDaemon,
            capabilities,
        )),
    };
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/registration-grants", post(issue_registration_grant))
        .route("/v1/hosts/register", post(register_host))
        .route("/v1/protocol/negotiate", post(protocol_negotiate))
        .route("/v1/protocol/commands", post(protocol_command))
        .route("/v1/organizations", post(create_organization))
        .route("/v1/goals", post(create_goal))
        .route(
            "/v1/organizations/{organization_id}/goals/{goal_id}/ancestry",
            get(goal_ancestry),
        )
        .route("/v1/projects", post(create_project))
        .route("/v1/workspaces", post(create_workspace))
        .route("/v1/agent-profile-revisions", post(append_profile_revision))
        .route("/v1/agent-instances", post(create_agent_instance))
        .route(
            "/v1/work-graph/items/{work_item_id}",
            post(register_work_item),
        )
        .route("/v1/work-graph/parents", post(set_parent))
        .route("/v1/work-graph/dependencies", post(add_dependency))
        .route("/v1/work-graph/comments", post(add_comment))
        .with_state(state)
}

/// Add CP-07 wake/checkout routes without making the earlier repository
/// constructor signatures a breaking public API.
#[allow(clippy::too_many_arguments)]
pub fn router_with_control_repositories(
    plane: Arc<ControlPlane>,
    bootstrap_credential: BootstrapCredential,
    scope_repository: Option<Arc<dyn ScopeRepository>>,
    agent_repository: Option<Arc<dyn AgentRepository>>,
    work_graph_repository: Option<Arc<dyn WorkGraphRepository>>,
    work_item_repository: Option<Arc<dyn WorkItemRepository>>,
    wake_repository: Arc<dyn WakeRepository>,
    run_binding_repository: Option<Arc<dyn RunBindingRepository>>,
    attempt_repository: Option<Arc<dyn AttemptRepository>>,
    routine_repository: Option<Arc<dyn RoutineRepository>>,
    approval_repository: Option<Arc<dyn ApprovalRepository>>,
    activity_repository: Option<Arc<dyn ActivityEventRepository>>,
    control_event_repository: Option<Arc<dyn ControlEventRepository>>,
    plugin_registry: Option<Arc<dyn PluginRegistry>>,
) -> Router {
    let capabilities = capabilities_from_wiring(
        scope_repository.is_some(),
        agent_repository.is_some(),
        work_graph_repository.is_some(),
        work_item_repository.is_some(),
        attempt_repository.is_some(),
        routine_repository.is_some(),
        approval_repository.is_some(),
        activity_repository.is_some(),
        control_event_repository.is_some(),
    );
    let mut dispatcher = ProtocolDispatcher::new(DeploymentMode::LocalDaemon, capabilities);
    if let Some(repository) = &work_item_repository {
        dispatcher = dispatcher.with_work_item_repository(repository.clone());
    }
    if let Some(repository) = &activity_repository {
        dispatcher = dispatcher.with_activity_repository(repository.clone());
    }
    if let Some(repository) = &control_event_repository {
        dispatcher = dispatcher.with_control_event_repository(repository.clone());
    }
    let dispatcher = Arc::new(dispatcher);
    let state = ApiState {
        plane,
        bootstrap_credential: Arc::new(bootstrap_credential),
        scope_repository,
        agent_repository,
        work_graph_repository,
        wake_repository: Some(wake_repository),
        run_binding_repository,
        attempt_repository,
        routine_repository,
        approval_repository,
        plugin_registry,
        protocol_dispatcher: dispatcher,
    };
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/registration-grants", post(issue_registration_grant))
        .route("/v1/hosts/register", post(register_host))
        .route("/v1/protocol/negotiate", post(protocol_negotiate))
        .route("/v1/protocol/commands", post(protocol_command))
        .route("/v1/wakes", post(enqueue_wake))
        .route("/v1/wakes/{work_item_id}/claim", post(claim_wake))
        .route("/v1/work-checkouts", post(checkout_work))
        .route("/v1/work-checkouts/release", post(release_checkout))
        .route("/v1/runtime/run-bindings", post(bind_run))
        .route(
            "/v1/runtime/attempts/{attempt_id}/finalize",
            post(finalize_attempt_route),
        )
        .route("/v1/routines", post(create_routine_route))
        .route(
            "/v1/routines/{routine_id}/revisions",
            post(append_routine_revision_route),
        )
        .route("/v1/plugins", post(install_plugin_route))
        .route(
            "/v1/organizations/{organization_id}/plugins",
            get(list_plugins_route),
        )
        .route("/v1/approvals", post(create_approval_route))
        .route(
            "/v1/approvals/{approval_id}/decisions",
            post(record_approval_decision_route),
        )
        .with_state(state)
}

#[derive(serde::Deserialize)]
struct WakeMutation {
    work_item_id: WorkItemId,
    source: WakeSource,
    requested_at: String,
}
#[derive(serde::Deserialize)]
struct ClaimMutation {
    claimed_at: String,
}
#[derive(serde::Deserialize)]
struct CheckoutMutation {
    lease: WorkCheckoutLease,
    now_unix_seconds: u64,
}
#[derive(serde::Deserialize)]
struct ReleaseMutation {
    work_item_id: WorkItemId,
    attempt_id: altai_control_protocol::AttemptId,
}
async fn enqueue_wake(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<WakeMutation>,
) -> Result<Json<WakeRequest>, ApiError> {
    require_bootstrap(&state, &headers)?;
    wake(&state)?
        .enqueue(request.work_item_id, request.source, request.requested_at)
        .map(Json)
        .map_err(ApiError::from)
}
async fn claim_wake(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(work_item_id): Path<String>,
    Json(request): Json<ClaimMutation>,
) -> Result<Json<WakeRequest>, ApiError> {
    require_bootstrap(&state, &headers)?;
    wake(&state)?
        .claim_wake(&WorkItemId::new(work_item_id), request.claimed_at)
        .map(Json)
        .map_err(ApiError::from)
}
async fn checkout_work(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<CheckoutMutation>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    wake(&state)?
        .checkout(request.lease, request.now_unix_seconds)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}
async fn release_checkout(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<ReleaseMutation>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    wake(&state)?
        .release_checkout(&request.work_item_id, &request.attempt_id)
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn bind_run(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(binding): Json<RunBinding>,
) -> Result<Json<RunBinding>, ApiError> {
    require_bootstrap(&state, &headers)?;
    run_bindings(&state)?
        .bind(binding)
        .map(Json)
        .map_err(ApiError::from)
}

/// One observed run termination submitted for durable finalization. `outcome`
/// is the executor's run observation (translated to attempt state by the
/// finalizer); the daemon never invents the completion time, it only records it.
#[derive(serde::Deserialize)]
struct FinalizeAttemptRequest {
    outcome: RunOutcome,
    observed_at_unix_seconds: u64,
}

async fn finalize_attempt_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(attempt_id): Path<String>,
    Json(request): Json<FinalizeAttemptRequest>,
) -> Result<Json<Attempt>, ApiError> {
    require_bootstrap(&state, &headers)?;
    let finalization = AttemptFinalization {
        attempt_id: AttemptId::new(attempt_id),
        outcome: request.outcome,
        observed_at_unix_seconds: request.observed_at_unix_seconds,
    };
    finalize_attempt(attempts(&state)?.as_ref(), &finalization)
        .map(Json)
        .map_err(ApiError::from)
}

async fn create_routine_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(routine): Json<Routine>,
) -> Result<Json<Routine>, ApiError> {
    require_bootstrap(&state, &headers)?;
    routines(&state)?.create(routine).map(Json).map_err(ApiError::from)
}

async fn append_routine_revision_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(routine_id): Path<String>,
    Json(revision): Json<RoutineRevision>,
) -> Result<Json<Routine>, ApiError> {
    require_bootstrap(&state, &headers)?;
    routines(&state)?
        .append_revision(&RoutineId::new(routine_id), revision)
        .map(Json)
        .map_err(ApiError::from)
}

async fn create_approval_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(approval): Json<Approval>,
) -> Result<Json<Approval>, ApiError> {
    require_bootstrap(&state, &headers)?;
    approvals(&state)?
        .create(approval)
        .map(Json)
        .map_err(ApiError::from)
}

#[derive(serde::Deserialize)]
struct PluginInstallRequest {
    organization_id: OrganizationId,
    manifest: PluginManifest,
    /// Explicit consent for capability expansion. Required only when the
    /// upgrade adds capabilities; ignored for first installs and pure upgrades.
    accept_expansion: bool,
}

async fn install_plugin_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<PluginInstallRequest>,
) -> Result<Json<crate::PluginRegistryOutcome>, ApiError> {
    require_bootstrap(&state, &headers)?;
    plugins(&state)?
        .install(&request.organization_id, request.manifest, request.accept_expansion)
        .map(Json)
        .map_err(ApiError::from)
}

async fn list_plugins_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<Vec<PluginManifest>>, ApiError> {
    require_bootstrap(&state, &headers)?;
    plugins(&state)?
        .list_in_org(&OrganizationId::new(organization_id))
        .map(Json)
        .map_err(ApiError::from)
}

/// One governance decision resolving an approval. The approval id is bound from
/// the path (not the body), mirroring how attempt finalization binds identity
/// from its path; only the outcome and audit fields come from the caller.
#[derive(serde::Deserialize)]
struct ApprovalDecisionRequest {
    outcome: ApprovalOutcome,
    decided_by: String,
    decided_at_unix_seconds: u64,
    reason: Option<String>,
}

async fn record_approval_decision_route(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(approval_id): Path<String>,
    Json(request): Json<ApprovalDecisionRequest>,
) -> Result<Json<Approval>, ApiError> {
    require_bootstrap(&state, &headers)?;
    let decision = ApprovalDecision {
        approval_id: ApprovalId::new(approval_id),
        outcome: request.outcome,
        decided_by: request.decided_by,
        decided_at_unix_seconds: request.decided_at_unix_seconds,
        reason: request.reason,
    };
    approvals(&state)?
        .record_decision(decision)
        .map(Json)
        .map_err(ApiError::from)
}

#[derive(serde::Deserialize)]
struct ParentMutation {
    work_item_id: WorkItemId,
    parent_work_item_id: Option<WorkItemId>,
}

async fn register_work_item(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(work_item_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    work_graph(&state)?
        .register_work_item(WorkItemId::new(work_item_id))
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn set_parent(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<ParentMutation>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    work_graph(&state)?
        .set_parent(request.work_item_id, request.parent_work_item_id)
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn add_dependency(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(dependency): Json<WorkDependency>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    work_graph(&state)?
        .add_dependency(dependency)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}
async fn add_comment(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(comment): Json<WorkComment>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    work_graph(&state)?
        .add_comment(comment)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn append_profile_revision(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(revision): Json<AgentProfileRevision>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    agent_repository(&state)?
        .append_profile_revision(revision)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn create_agent_instance(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(instance): Json<AgentInstance>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    agent_repository(&state)?
        .create_instance(instance)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn goal_ancestry(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path((organization_id, goal_id)): Path<(String, String)>,
) -> Result<Json<Vec<Goal>>, ApiError> {
    require_bootstrap(&state, &headers)?;
    scope(&state)?
        .goal_ancestry(&OrganizationId::new(organization_id), &GoalId::new(goal_id))
        .map(Json)
        .map_err(ApiError::from)
}

async fn create_organization(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(organization): Json<Organization>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    scope(&state)?
        .create_organization(organization)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn create_goal(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(goal): Json<Goal>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    scope(&state)?.create_goal(goal).map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn create_project(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(project): Json<Project>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    scope(&state)?
        .create_project(project)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

async fn create_workspace(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(workspace): Json<ProjectWorkspace>,
) -> Result<StatusCode, ApiError> {
    require_bootstrap(&state, &headers)?;
    scope(&state)?
        .create_workspace(workspace)
        .map_err(ApiError::from)?;
    Ok(StatusCode::CREATED)
}

fn scope(state: &ApiState) -> Result<&Arc<dyn ScopeRepository>, ApiError> {
    state.scope_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "scope_repository_unavailable",
    })
}

fn agent_repository(state: &ApiState) -> Result<&Arc<dyn AgentRepository>, ApiError> {
    state.agent_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "agent_repository_unavailable",
    })
}

fn work_graph(state: &ApiState) -> Result<&Arc<dyn WorkGraphRepository>, ApiError> {
    state.work_graph_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "work_graph_repository_unavailable",
    })
}
fn wake(state: &ApiState) -> Result<&Arc<dyn WakeRepository>, ApiError> {
    state.wake_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "wake_repository_unavailable",
    })
}
fn run_bindings(state: &ApiState) -> Result<&Arc<dyn RunBindingRepository>, ApiError> {
    state.run_binding_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "run_binding_repository_unavailable",
    })
}
fn attempts(state: &ApiState) -> Result<&Arc<dyn AttemptRepository>, ApiError> {
    state.attempt_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "attempt_repository_unavailable",
    })
}
fn routines(state: &ApiState) -> Result<&Arc<dyn RoutineRepository>, ApiError> {
    state.routine_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "routine_repository_unavailable",
    })
}
fn approvals(state: &ApiState) -> Result<&Arc<dyn ApprovalRepository>, ApiError> {
    state.approval_repository.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "approval_repository_unavailable",
    })
}
fn plugins(state: &ApiState) -> Result<&Arc<dyn PluginRegistry>, ApiError> {
    state.plugin_registry.as_ref().ok_or(ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "plugin_registry_unavailable",
    })
}

async fn health(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<ControlPlaneHealth>, ApiError> {
    require_bootstrap(&state, &headers)?;
    state.plane.health().map(Json).map_err(ApiError::from)
}

/// Bootstrap-friendly capability discovery: a client that does not yet know
/// the server's protocol version can negotiate before framing commands.
async fn protocol_negotiate(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<CapabilityNegotiationRequest>,
) -> Result<Json<CapabilityNegotiationResponse>, ApiError> {
    require_bootstrap(&state, &headers)?;
    Ok(Json(state.protocol_dispatcher.negotiate(&request)))
}

/// Uniform framed protocol entry point. The handler delegates to the same
/// dispatcher a local caller uses; domain failures are typed errors inside the
/// response envelope, so the HTTP status stays reserved for transport issues.
async fn protocol_command(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<ProtocolRequest<ProtocolCommand>>,
) -> Result<Json<ProtocolResponse<ProtocolOutcome>>, ApiError> {
    require_bootstrap(&state, &headers)?;
    Ok(Json(state.protocol_dispatcher.execute(&request)))
}

async fn issue_registration_grant(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<RegistrationGrant>, ApiError> {
    require_bootstrap(&state, &headers)?;
    state
        .plane
        .issue_registration_grant()
        .map(Json)
        .map_err(ApiError::from)
}

async fn register_host(
    State(state): State<ApiState>,
    Json(request): Json<HostRegistrationRequest>,
) -> Result<Json<RegisteredHost>, ApiError> {
    state
        .plane
        .register_host(request)
        .map(Json)
        .map_err(ApiError::from)
}

fn require_bootstrap(state: &ApiState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.bootstrap_credential.authorizes(headers) {
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
}

impl ApiError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
        }
    }
}

impl From<ControlPlaneError> for ApiError {
    fn from(value: ControlPlaneError) -> Self {
        match value {
            ControlPlaneError::InvalidGrant | ControlPlaneError::ExpiredGrant => {
                Self::unauthorized()
            }
            ControlPlaneError::UnsupportedProtocol { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "unsupported_protocol",
            },
            ControlPlaneError::DuplicateWorkspace { .. } => Self {
                status: StatusCode::BAD_REQUEST,
                code: "duplicate_workspace",
            },
            ControlPlaneError::InvalidConfig { .. } | ControlPlaneError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "control_plane_unavailable",
            },
        }
    }
}

impl From<ScopeError> for ApiError {
    fn from(value: ScopeError) -> Self {
        match value {
            ScopeError::AlreadyExists { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "already_exists",
            },
            ScopeError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
            },
            ScopeError::CrossOrganization { .. } => Self {
                status: StatusCode::FORBIDDEN,
                code: "cross_organization",
            },
            ScopeError::GoalCycle { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "goal_cycle",
            },
            ScopeError::StaleRevision { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "stale_revision",
            },
            ScopeError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "scope_repository_unavailable",
            },
        }
    }
}

impl From<AgentRepositoryError> for ApiError {
    fn from(value: AgentRepositoryError) -> Self {
        match value {
            AgentRepositoryError::AlreadyExists { .. }
            | AgentRepositoryError::ReportingCycle { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "agent_conflict",
            },
            AgentRepositoryError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
            },
            AgentRepositoryError::NotDispatchable { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "agent_not_dispatchable",
            },
            AgentRepositoryError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "agent_repository_unavailable",
            },
        }
    }
}

impl From<WorkGraphError> for ApiError {
    fn from(value: WorkGraphError) -> Self {
        match value {
            WorkGraphError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
            },
            WorkGraphError::AlreadyExists { .. } | WorkGraphError::ParentCycle { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "work_graph_conflict",
            },
            WorkGraphError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "work_graph_repository_unavailable",
            },
        }
    }
}
impl From<WakeError> for ApiError {
    fn from(value: WakeError) -> Self {
        match value {
            WakeError::ActiveCheckout { .. } | WakeError::AlreadyClaimed { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "wake_conflict",
            },
            WakeError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
            },
            WakeError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "wake_repository_unavailable",
            },
        }
    }
}

impl From<RunBindingError> for ApiError {
    fn from(value: RunBindingError) -> Self {
        match value {
            RunBindingError::Conflict { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "run_binding_conflict",
            },
            RunBindingError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "run_binding_repository_unavailable",
            },
        }
    }
}

impl From<AttemptFinalizationError> for ApiError {
    fn from(value: AttemptFinalizationError) -> Self {
        match value {
            AttemptFinalizationError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "attempt_not_found",
            },
            AttemptFinalizationError::AlreadyTerminal { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "attempt_already_terminal",
            },
            AttemptFinalizationError::InvalidTransition { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "attempt_invalid_transition",
            },
            AttemptFinalizationError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "attempt_finalization_failed",
            },
        }
    }
}

impl From<RoutineError> for ApiError {
    fn from(value: RoutineError) -> Self {
        match value {
            RoutineError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "routine_not_found",
            },
            RoutineError::RevisionNotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "routine_revision_not_found",
            },
            RoutineError::Conflict { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "routine_conflict",
            },
            RoutineError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "routine_repository_unavailable",
            },
        }
    }
}

impl From<ApprovalError> for ApiError {
    fn from(value: ApprovalError) -> Self {
        match value {
            ApprovalError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                code: "approval_not_found",
            },
            ApprovalError::Conflict { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "approval_conflict",
            },
            ApprovalError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "approval_repository_unavailable",
            },
        }
    }
}
impl From<PluginRegistryError> for ApiError {
    fn from(value: PluginRegistryError) -> Self {
        match value {
            PluginRegistryError::InvalidManifest { .. } => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_plugin_manifest",
            },
            PluginRegistryError::VersionDidNotAdvance { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "plugin_version_did_not_advance",
            },
            PluginRegistryError::ExpansionNotAccepted { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "plugin_expansion_not_accepted",
            },
            PluginRegistryError::Internal { .. } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "plugin_registry_unavailable",
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(ErrorBody { code: self.code })).into_response()
    }
}

fn digest(value: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AttemptRepository, ControlPlaneConfig, ControlPlaneStore, InMemoryAgentRepository,
        InMemoryScopeRepository, InMemoryWakeRepository, PluginRegistryOutcome, SqliteApprovalRepository,
        SqliteAttemptRepository, SqlitePluginRegistry, SqliteRoutineRepository,
        SqliteRunBindingRepository,
    };
    use altai_control_protocol::{
        AgentInstanceId, AgentProfileRevisionId, Approval, ApprovalId, ApprovalOutcome,
        ApprovalScope, Attempt, AttemptId, AttemptState, HostCapabilities, HostRegistration,
        Organization, OrganizationId, PluginCapability, PluginKind, PluginManifest, PluginVersion,
        Revision, Routine, RoutineId, RoutineRevision, RoutineRevisionId, RoutineStatus,
        RoutineTrigger, RunBinding, RunId, WorkItemId, WorkspaceId, CONTROL_PLANE_PROTOCOL_MAJOR,
    };
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    fn app() -> Router {
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".to_string(),
                store: ControlPlaneStore::Sqlite {
                    database_path: "/tmp/control-plane-test.db".to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        router_with_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            Some(Arc::new(InMemoryScopeRepository::default())),
            Some(Arc::new(InMemoryAgentRepository::default())),
        )
    }

    #[tokio::test]
    async fn health_requires_bootstrap_bearer() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn grant_registers_a_host_without_exposing_bootstrap_credential() {
        let app = app();
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/registration-grants")
                    .header(AUTHORIZATION, "Bearer test-bootstrap-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(grant_response.into_body(), 4096)
            .await
            .unwrap();
        let grant: RegistrationGrant = serde_json::from_slice(&bytes).unwrap();

        let request = HostRegistrationRequest {
            grant_token: grant.token,
            host: HostRegistration {
                agent_instance_id: AgentInstanceId::new("host-a"),
                workspaces: vec![WorkspaceId::new("workspace-a")],
                capabilities: HostCapabilities::default(),
                protocol_major: CONTROL_PLANE_PROTOCOL_MAJOR,
            },
        };
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/hosts/register")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn scope_mutations_require_bootstrap_and_reject_duplicates() {
        let organization = Organization {
            id: OrganizationId::new("transport-test"),
            name: "Transport test".to_string(),
            revision: Revision::INITIAL,
            created_at: "2026-08-13T00:00:00Z".to_string(),
            updated_at: "2026-08-13T00:00:00Z".to_string(),
        };
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/v1/organizations")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&organization).unwrap()))
                .unwrap()
        };
        assert_eq!(
            app().clone().oneshot(request()).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let authorized = |request: Request<Body>| {
            let (mut parts, body) = request.into_parts();
            parts.headers.insert(
                AUTHORIZATION,
                "Bearer test-bootstrap-token".parse().unwrap(),
            );
            Request::from_parts(parts, body)
        };
        let app = app();
        assert_eq!(
            app.clone()
                .oneshot(authorized(request()))
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            app.oneshot(authorized(request())).await.unwrap().status(),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn short_bootstrap_tokens_are_rejected() {
        assert!(BootstrapCredential::from_plaintext("too-short").is_err());
    }

    #[tokio::test]
    async fn wake_mutations_require_bearer_and_enqueue_when_authorized() {
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: "/tmp/control-plane-test.db".into(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let body = serde_json::json!({ "work_item_id": { "type": "work_item_id", "value": "work-1" }, "source": "manual", "requested_at": "now" }).to_string();
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/v1/wakes")
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let mut authorized = request();
        authorized.headers_mut().insert(
            AUTHORIZATION,
            "Bearer test-bootstrap-token".parse().unwrap(),
        );
        assert_eq!(
            app.oneshot(authorized).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn run_bindings_require_bearer_and_are_immutable() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            Some(Arc::new(
                SqliteRunBindingRepository::open(&directory.path().join("work.db")).unwrap(),
            )),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let binding = RunBinding {
            attempt_id: AttemptId::new("attempt-1"),
            work_item_id: WorkItemId::new("work-1"),
            owner_agent_instance_id: AgentInstanceId::new("agent-1"),
            run_id: RunId::new("run-1"),
            bound_at_unix_seconds: 1,
        };
        let request = |binding: RunBinding, authenticated: bool| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/v1/runtime/run-bindings")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&binding).unwrap()))
                .unwrap();
            if authenticated {
                request.headers_mut().insert(
                    AUTHORIZATION,
                    "Bearer test-bootstrap-token".parse().unwrap(),
                );
            }
            request
        };
        assert_eq!(
            app.clone()
                .oneshot(request(binding.clone(), false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.clone()
                .oneshot(request(binding.clone(), true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(request(binding.clone(), true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let mut conflicting = binding;
        conflicting.run_id = RunId::new("run-2");
        assert_eq!(
            app.oneshot(request(conflicting, true))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn finalize_attempt_route_requires_bearer_and_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let attempt_repository =
            Arc::new(SqliteAttemptRepository::open(&directory.path().join("work.db")).unwrap());
        let attempt = Attempt {
            id: AttemptId::new("attempt-1"),
            work_item_id: WorkItemId::new("work-1"),
            owner_agent_instance_id: AgentInstanceId::new("agent-1"),
            profile_revision_id: AgentProfileRevisionId::new("rev-1"),
            state: AttemptState::Created,
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        };
        attempt_repository.create(attempt).unwrap();
        attempt_repository
            .transition(&AttemptId::new("attempt-1"), AttemptState::Claimed, 10)
            .unwrap();
        attempt_repository
            .transition(&AttemptId::new("attempt-1"), AttemptState::Dispatched, 20)
            .unwrap();
        attempt_repository
            .transition(&AttemptId::new("attempt-1"), AttemptState::Running, 30)
            .unwrap();
        let attempt_read = attempt_repository.clone();
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            Some(attempt_repository),
            None,
            None,
            None,
            None,
            None,
        );
        let request = |outcome: &str, observed: u64, authenticated: bool| {
            let payload = serde_json::json!({
                "outcome": outcome,
                "observed_at_unix_seconds": observed,
            });
            let mut request = Request::builder()
                .method("POST")
                .uri("/v1/runtime/attempts/attempt-1/finalize")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();
            if authenticated {
                request.headers_mut().insert(
                    AUTHORIZATION,
                    "Bearer test-bootstrap-token".parse().unwrap(),
                );
            }
            request
        };
        // Unauthenticated: rejected before the attempt is touched.
        assert_eq!(
            app.clone()
                .oneshot(request("succeeded", 100, false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // Authenticated: finalizes Running -> Succeeded.
        assert_eq!(
            app.clone()
                .oneshot(request("succeeded", 100, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            attempt_read
                .get(&AttemptId::new("attempt-1"))
                .unwrap()
                .unwrap()
                .state,
            AttemptState::Succeeded
        );
        // Replay of the same outcome is idempotent: still OK, observed time unchanged.
        assert_eq!(
            app.clone()
                .oneshot(request("succeeded", 200, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            attempt_read
                .get(&AttemptId::new("attempt-1"))
                .unwrap()
                .unwrap()
                .updated_at_unix_seconds,
            100
        );
        // A divergent outcome after the attempt is terminal fails closed (409).
        assert_eq!(
            app.oneshot(request("failed", 300, true))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn routine_routes_require_bootstrap_are_idempotent_and_reject_conflicts() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let routine_repository =
            Arc::new(SqliteRoutineRepository::open(&directory.path().join("work.db")).unwrap());
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            Some(routine_repository.clone()),
            None,
            None,
            None,
            None,
        );

        let routine_id = RoutineId::new("rt-1");
        let routine = Routine {
            id: routine_id.clone(),
            organization_id: OrganizationId::new("org"),
            current_revision_id: None,
            status: RoutineStatus::Active,
            revision: Revision::INITIAL,
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        };
        let create_request = |routine: &Routine, authenticated: bool| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/v1/routines")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(routine).unwrap()))
                .unwrap();
            if authenticated {
                request
                    .headers_mut()
                    .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
            }
            request
        };
        // Unauthenticated create: rejected before storage is touched.
        assert_eq!(
            app.clone()
                .oneshot(create_request(&routine, false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // Authenticated create succeeds and is idempotent on replay.
        assert_eq!(
            app.clone()
                .oneshot(create_request(&routine, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(create_request(&routine, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        // A divergent routine under the same id fails closed (409).
        let mut divergent = routine.clone();
        divergent.organization_id = OrganizationId::new("other");
        assert_eq!(
            app.clone()
                .oneshot(create_request(&divergent, true))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let revision_request = |path: &str, revision: &RoutineRevision, authenticated: bool| {
            let mut request = Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(revision).unwrap()))
                .unwrap();
            if authenticated {
                request
                    .headers_mut()
                    .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
            }
            request
        };
        let rev1 = RoutineRevision {
            id: RoutineRevisionId::new("rev-1"),
            routine_id: routine_id.clone(),
            revision: Revision::new(1),
            trigger: RoutineTrigger::Recurring {
                cron_expression: "0 9 * * *".into(),
            },
            target_work_item_id: WorkItemId::new("work-1"),
            created_at_unix_seconds: 10,
        };
        let rev2 = RoutineRevision {
            id: RoutineRevisionId::new("rev-2"),
            routine_id: routine_id.clone(),
            revision: Revision::new(2),
            trigger: RoutineTrigger::Event {
                source: "pull_request".into(),
            },
            target_work_item_id: WorkItemId::new("work-1"),
            created_at_unix_seconds: 20,
        };
        // Authenticated append advances the current revision.
        assert_eq!(
            app.clone()
                .oneshot(revision_request("/v1/routines/rt-1/revisions", &rev1, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            routine_repository
                .get(&routine_id)
                .unwrap()
                .unwrap()
                .current_revision_id,
            Some(RoutineRevisionId::new("rev-1"))
        );
        // Idempotent re-append of the same revision id: still OK, no double-advance.
        assert_eq!(
            app.clone()
                .oneshot(revision_request("/v1/routines/rt-1/revisions", &rev1, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            routine_repository.get(&routine_id).unwrap().unwrap().revision,
            Revision::new(1)
        );
        // A higher revision advances again.
        assert_eq!(
            app.clone()
                .oneshot(revision_request("/v1/routines/rt-1/revisions", &rev2, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        // A non-monotonic revision (intent version <= current) fails closed (409).
        let non_monotonic = RoutineRevision {
            id: RoutineRevisionId::new("rev-3"),
            routine_id: routine_id.clone(),
            revision: Revision::new(1),
            trigger: RoutineTrigger::Recurring {
                cron_expression: "0 10 * * *".into(),
            },
            target_work_item_id: WorkItemId::new("work-1"),
            created_at_unix_seconds: 30,
        };
        assert_eq!(
            app.clone()
                .oneshot(revision_request(
                    "/v1/routines/rt-1/revisions",
                    &non_monotonic,
                    true
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        // Appending to an unknown routine fails closed (404).
        let orphan = RoutineRevision {
            id: RoutineRevisionId::new("rev-x"),
            routine_id: RoutineId::new("ghost"),
            revision: Revision::new(1),
            trigger: RoutineTrigger::Recurring {
                cron_expression: "0 9 * * *".into(),
            },
            target_work_item_id: WorkItemId::new("work-1"),
            created_at_unix_seconds: 40,
        };
        assert_eq!(
            app.oneshot(revision_request(
                "/v1/routines/ghost/revisions",
                &orphan,
                true
            ))
            .await
            .unwrap()
            .status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn approval_routes_require_bootstrap_are_idempotent_and_reject_conflicts() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let approval_repository =
            Arc::new(SqliteApprovalRepository::open(&directory.path().join("work.db")).unwrap());
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            None,
            Some(approval_repository.clone()),
            None,
            None,
            None,
        );

        let approval = Approval {
            id: ApprovalId::new("apv-1"),
            organization_id: OrganizationId::new("org"),
            scope: ApprovalScope::Plan {
                attempt_id: AttemptId::new("att-1"),
            },
            payload_revision: Revision::new(1),
            outcome: None,
            revision: Revision::INITIAL,
            created_at_unix_seconds: 1,
            resolved_at_unix_seconds: None,
        };
        let create_request = |approval: &Approval, authenticated: bool| {
            let mut request = Request::builder()
                .method("POST")
                .uri("/v1/approvals")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(approval).unwrap()))
                .unwrap();
            if authenticated {
                request
                    .headers_mut()
                    .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
            }
            request
        };
        // Unauthenticated create: rejected before storage is touched.
        assert_eq!(
            app.clone()
                .oneshot(create_request(&approval, false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // Authenticated create succeeds and is idempotent on replay.
        assert_eq!(
            app.clone()
                .oneshot(create_request(&approval, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(create_request(&approval, true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        // A divergent approval under the same id fails closed (409).
        let mut divergent = approval.clone();
        divergent.organization_id = OrganizationId::new("other");
        assert_eq!(
            app.clone()
                .oneshot(create_request(&divergent, true))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let decision_request = |approval_path: &str, outcome: &str, authenticated: bool| {
            let payload = serde_json::json!({
                "outcome": outcome,
                "decided_by": "principal",
                "decided_at_unix_seconds": 100,
                "reason": "ok"
            });
            let uri = format!("/v1/approvals/{approval_path}/decisions");
            let mut request = Request::builder()
                .method("POST")
                .uri(&uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();
            if authenticated {
                request
                    .headers_mut()
                    .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
            }
            request
        };
        // Unauthenticated resolve: rejected before storage is touched.
        assert_eq!(
            app.clone()
                .oneshot(decision_request("apv-1", "approved", false))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        // Authenticated resolve advances the outcome to Approved.
        assert_eq!(
            app.clone()
                .oneshot(decision_request("apv-1", "approved", true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let resolved = approval_repository
            .get(&ApprovalId::new("apv-1"))
            .unwrap()
            .unwrap();
        assert_eq!(resolved.outcome, Some(ApprovalOutcome::Approved));
        assert_eq!(resolved.resolved_at_unix_seconds, Some(100));
        // Idempotent re-decision: still OK, no double-advance (revision stays at 1).
        assert_eq!(
            app.clone()
                .oneshot(decision_request("apv-1", "approved", true))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            approval_repository
                .get(&ApprovalId::new("apv-1"))
                .unwrap()
                .unwrap()
                .revision,
            Revision::new(1)
        );
        // A divergent outcome after resolution fails closed (409).
        assert_eq!(
            app.clone()
                .oneshot(decision_request("apv-1", "denied", true))
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        // Resolving an unknown approval fails closed (404).
        assert_eq!(
            app.oneshot(decision_request("ghost", "approved", true))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }

    fn application_manifest(major: u32, capabilities: Vec<PluginCapability>) -> PluginManifest {
        PluginManifest {
            plugin_id: altai_control_protocol::PluginId::new("plg_http"),
            kind: PluginKind::Application,
            version: PluginVersion::new(major, 0, 0),
            display_name: "HTTP plugin".into(),
            capabilities,
            ui: None,
        }
    }

    #[tokio::test]
    async fn plugin_installs_enforce_expansion_consent_over_http() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Arc::new(
                SqlitePluginRegistry::open(&directory.path().join("work.db")).unwrap(),
            )),
        );
        let install_request = |manifest: &PluginManifest, accept: bool| {
            let body = serde_json::json!({
                "organization_id": { "type": "organization_id", "value": "org_http" },
                "manifest": manifest,
                "accept_expansion": accept,
            })
            .to_string();
            let mut request = Request::builder()
                .method("POST")
                .uri("/v1/plugins")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            request.headers_mut().insert(
                AUTHORIZATION,
                "Bearer test-bootstrap-token".parse().unwrap(),
            );
            request
        };
        // Unauthenticated installs are rejected before touching the registry.
        let unauthenticated = Request::builder()
            .method("POST")
            .uri("/v1/plugins")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "organization_id": { "type": "organization_id", "value": "org_http" },
                    "manifest": application_manifest(1, vec![PluginCapability::Jobs]),
                    "accept_expansion": false,
                })
                .to_string(),
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(unauthenticated).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        // First install commits without any consent flag.
        let response = app
            .clone()
            .oneshot(install_request(&application_manifest(1, vec![PluginCapability::Jobs]), false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let installed: PluginRegistryOutcome =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap())
            .unwrap();
        assert!(matches!(installed, PluginRegistryOutcome::Installed { .. }));
        // An upgrade that adds capabilities is refused without consent (409).
        let expanded = application_manifest(2, vec![PluginCapability::Jobs, PluginCapability::Webhooks]);
        let response = app
            .clone()
            .oneshot(install_request(&expanded, false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let refusal_body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&refusal_body).contains("plugin_expansion_not_accepted"));
        // The same upgrade with explicit consent commits and discloses.
        let response = app
            .clone()
            .oneshot(install_request(&expanded, true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let upgraded: PluginRegistryOutcome =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap())
            .unwrap();
        let PluginRegistryOutcome::Upgraded { disclosure, .. } = upgraded else {
            panic!("expected Upgraded");
        };
        assert_eq!(disclosure.added_capabilities, vec![PluginCapability::Webhooks]);
        // The org listing reflects the committed upgrade.
        let mut list = Request::builder()
            .method("GET")
            .uri("/v1/organizations/org_http/plugins")
            .body(Body::empty())
            .unwrap();
        list.headers_mut().insert(
            AUTHORIZATION,
            "Bearer test-bootstrap-token".parse().unwrap(),
        );
        let response = app.oneshot(list).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let listed: Vec<PluginManifest> =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap())
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].version.to_string(), "2.0.0");
        assert_eq!(listed[0].capabilities, vec![PluginCapability::Jobs, PluginCapability::Webhooks]);
    }

    #[tokio::test]
    async fn plugin_downgrades_are_refused_over_http() {
        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".into(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Arc::new(
                SqlitePluginRegistry::open(&directory.path().join("work.db")).unwrap(),
            )),
        );
        let send = |manifest: &PluginManifest| {
            let body = serde_json::json!({
                "organization_id": { "type": "organization_id", "value": "org_http" },
                "manifest": manifest,
                "accept_expansion": true,
            })
            .to_string();
            let mut request = Request::builder()
                .method("POST")
                .uri("/v1/plugins")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            request.headers_mut().insert(
                AUTHORIZATION,
                "Bearer test-bootstrap-token".parse().unwrap(),
            );
            request
        };
        assert_eq!(
            app.clone()
                .oneshot(send(&application_manifest(2, vec![])))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let response = app
            .oneshot(send(&application_manifest(1, vec![])))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let refusal_body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8_lossy(&refusal_body).contains("plugin_version_did_not_advance")
        );
    }

    /// Package 053's gate closes over the wire: the deployed transport's
    /// HTTP bytes must equal what the shared dispatcher produces — the same
    /// value every embedded host (CLI, desktop) returns for the same request.
    #[tokio::test]
    async fn protocol_routes_frame_the_shared_dispatcher_wire_format() {
        use altai_control_protocol::actor::UserId;
        use altai_control_protocol::{
            ActivityQueryRequest, Actor, PageRequest, ProtocolVersion,
        };

        let directory = tempfile::tempdir().unwrap();
        let work_db = directory.path().join("work.db");
        let activity = Arc::new(crate::SqliteActivityEventRepository::open(&work_db).unwrap());
        let control_events =
            Arc::new(crate::SqliteControlEventRepository::open(&work_db).unwrap());
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".to_string(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            None,
            None,
            Some(activity.clone()),
            Some(control_events.clone()),
            None,
        );
        // Reference dispatcher, built exactly the way the router builds its
        // own (LocalDaemon, honest wiring) — and the way embedded hosts build
        // theirs, modulo deployment mode.
        let capabilities =
            capabilities_from_wiring(false, false, false, false, false, false, false, true, true);
        let reference = ProtocolDispatcher::new(DeploymentMode::LocalDaemon, capabilities)
            .with_activity_repository(activity)
            .with_control_event_repository(control_events);

        let organization = OrganizationId::new("org-wire-parity");
        let request = ProtocolRequest {
            id: "req-wire-parity".into(),
            version: ProtocolVersion::CURRENT,
            actor: Actor::User {
                id: UserId::new(organization.clone(), "transport"),
                display_name: "Transport Host".into(),
            },
            payload: ProtocolCommand::QueryActivity(ActivityQueryRequest {
                organization_id: organization,
                page: PageRequest::default(),
                kind: None,
                work_item_id: None,
            }),
        };
        let command_body = serde_json::to_string(&request).unwrap();
        let post_command = |body: String| {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/protocol/commands")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let (mut parts, body) = request.into_parts();
            parts
                .headers
                .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
            Request::from_parts(parts, body)
        };

        // Transport guard first: the protocol routes sit behind bootstrap.
        let unauthenticated = Request::builder()
            .method("POST")
            .uri("/v1/protocol/commands")
            .header("content-type", "application/json")
            .body(Body::from(command_body.clone()))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(unauthenticated)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );

        let response = app
            .clone()
            .oneshot(post_command(command_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let over_the_wire: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            over_the_wire,
            serde_json::to_value(reference.execute(&request)).unwrap()
        );

        let negotiation = CapabilityNegotiationRequest {
            client_name: "wire-parity".into(),
            client_version: ProtocolVersion::CURRENT,
            required_capabilities: vec![],
        };
        let plain_negotiate = Request::builder()
            .method("POST")
            .uri("/v1/protocol/negotiate")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&negotiation).unwrap()))
            .unwrap();
        let (mut parts, body) = plain_negotiate.into_parts();
        parts
            .headers
            .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
        let response = app.oneshot(Request::from_parts(parts, body)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let over_the_wire: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            over_the_wire,
            serde_json::to_value(reference.negotiate(&negotiation)).unwrap()
        );
    }

    /// Domain failures stay inside the protocol envelope: an unwired
    /// capability answers HTTP 200 with a typed error, keeping the HTTP
    /// status reserved for transport issues.
    #[tokio::test]
    async fn unwired_capabilities_answer_typed_envelope_errors_not_http_errors() {
        use altai_control_protocol::actor::UserId;
        use altai_control_protocol::{
            ActivityQueryRequest, Actor, PageRequest, ProtocolVersion,
        };

        let directory = tempfile::tempdir().unwrap();
        let plane = Arc::new(
            ControlPlane::bootstrap(ControlPlaneConfig {
                service_version: "0.1.0".to_string(),
                store: ControlPlaneStore::Sqlite {
                    database_path: directory.path().join("control.db").display().to_string(),
                },
                registration_ttl_seconds: 60,
            })
            .unwrap(),
        );
        let app = router_with_control_repositories(
            plane,
            BootstrapCredential::from_plaintext("test-bootstrap-token").unwrap(),
            None,
            None,
            None,
            None,
            Arc::new(InMemoryWakeRepository::default()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let organization = OrganizationId::new("org-unwired");
        let request = ProtocolRequest {
            id: "req-unwired".into(),
            version: ProtocolVersion::CURRENT,
            actor: Actor::User {
                id: UserId::new(organization.clone(), "transport"),
                display_name: "Transport Host".into(),
            },
            payload: ProtocolCommand::QueryActivity(ActivityQueryRequest {
                organization_id: organization,
                page: PageRequest::default(),
                kind: None,
                work_item_id: None,
            }),
        };
        let unauthorized_request = Request::builder()
            .method("POST")
            .uri("/v1/protocol/commands")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&request).unwrap()))
            .unwrap();
        let (mut parts, body) = unauthorized_request.into_parts();
        parts
            .headers
            .insert(AUTHORIZATION, "Bearer test-bootstrap-token".parse().unwrap());
        let response = app.oneshot(Request::from_parts(parts, body)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope["result"]["Err"]["code"], "PolicyDenied");
        assert!(
            envelope["result"]["Err"]["message"]
                .as_str()
                .unwrap()
                .contains("capability not served by this deployment: activity_audit"),
            "unexpected envelope: {envelope}"
        );
    }
}
