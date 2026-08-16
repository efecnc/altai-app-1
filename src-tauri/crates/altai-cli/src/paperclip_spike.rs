//! Work OS CP-08-84/85 acceptance-spike harness (`PAPERCLIP_SPIKE_PLAN.md` §3).
//!
//! Two transports, one scenario:
//!
//! - `SpikeMode::Stub` (CP-08-84): the real control-plane axum router
//!   in-process against a stub plane (tempdir SQLite store) — no network, no
//!   daemon. The wire rehearsal for the downstream Paperclip plane: exactly
//!   the HTTP shapes the `altai-host` adapter speaks (registration chain, Work
//!   fixture, wake dispatch, transactional checkout, and the reconnect
//!   idempotency invariants of CP-08-020 through 024).
//! - `SpikeMode::Real` (CP-08-85): the same production router bound to a
//!   loopback listener; every step rides a real HTTP round trip, and the
//!   scenario grows the execution phases: the IsanAgent attempt (034), the
//!   lifecycle→event translation observed over the framed query wire (035),
//!   and the downstream Review projection (045) when a downstream plane is
//!   configured.
//!
//! The reconnect step runs twice, with the lease state asserted between runs:
//! a re-registering host must not double-fire the wake, must not steal (or
//! lose) the live lease, and a stale finalizer must not release another
//! attempt's lease. Any break exits non-zero with a phase-typed code.

use std::net::SocketAddr;
use std::sync::Arc;

use altai_agent_service::event_map::map_lifecycle_to_event;
use altai_agent_service::Event;
use altai_control_plane::{
    router_with_control_repositories, ActivityEventRepository, AttemptRepository,
    BootstrapCredential, ControlPlane, ControlPlaneConfig, ControlPlaneStore,
    InMemoryAgentRepository, InMemoryScopeRepository, InMemoryWakeRepository,
    InMemoryWorkGraphRepository, SqliteActivityEventRepository, SqliteApprovalRepository,
    SqliteAttemptRepository, SqliteControlEventRepository, SqliteRoutineRepository,
    SqliteRunBindingRepository, WorkGraphRepository,
};
use altai_control_protocol::{
    ActivityEvent, Actor, AgentInstanceId, AgentProfileRevisionId, Attempt, AttemptId,
    AttemptState, EventKind, OrganizationId, RunId, WorkItemId,
};
use axum::{
    body::Body,
    http::{header::AUTHORIZATION, Method, Request, StatusCode},
    Router,
};
use http_body_util::{BodyExt, Full};
use isanagent::bus::{RunLifecycleEvent, RunOutcome};
use serde_json::{json, Value};
use tower::ServiceExt;

/// Which transport the scenario rides.
pub enum SpikeMode {
    /// CP-08-84 wire rehearsal: the router in-process, one call per step.
    Stub,
    /// CP-08-85 end-to-end: the router on a loopback listener, real HTTP per
    /// step, plus the attempt/event/review phases. `bind` defaults to an
    /// ephemeral port; the 080 acceptance run pins the port the downstream's
    /// `altai-host` adapter is configured against. `downstream` names the
    /// Paperclip plane and the issue its projection of the spike Work lives
    /// in; without it the review phase reports a typed skip.
    Real {
        bind: Option<SocketAddr>,
        downstream: Option<DownstreamPlane>,
    },
}

/// The downstream Paperclip plane (runbook bring-up): the server's base URL
/// and the issue the runbook bound to the spike's Work item. The Review
/// probe reads Paperclip's own projection through its existing issue API —
/// no downstream route is added for the spike.
pub struct DownstreamPlane {
    pub base_url: String,
    pub issue_id: String,
}

/// Distinct from the `run` command's public exit codes (0–8, 10); each phase
/// of the spike reports its own so a red harness run names the broken link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpikePhase {
    Health,
    Registration,
    Fixture,
    Dispatch,
    Checkout,
    Attempt,
    Events,
    Review,
    Reconnect,
}

impl SpikePhase {
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Health => 30,
            Self::Registration => 31,
            Self::Fixture => 32,
            Self::Dispatch => 33,
            Self::Checkout => 34,
            Self::Attempt => 36,
            Self::Events => 37,
            Self::Review => 38,
            Self::Reconnect => 35,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Registration => "registration",
            Self::Fixture => "fixture",
            Self::Dispatch => "dispatch",
            Self::Checkout => "checkout",
            Self::Attempt => "attempt",
            Self::Events => "events",
            Self::Review => "review",
            Self::Reconnect => "reconnect",
        }
    }
}

#[derive(Debug)]
pub struct SpikeFailure {
    pub phase: SpikePhase,
    pub step: &'static str,
    pub detail: String,
}

impl SpikeFailure {
    fn new(phase: SpikePhase, step: &'static str, detail: impl Into<String>) -> Self {
        Self {
            phase,
            step,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for SpikeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.phase.label(), self.step, self.detail)
    }
}

/// The plane: production router wiring over throwaway SQLite and in-memory
/// repositories, exactly as the daemon's own dispatch tests build it
/// (`altai-control-plane` `protocol_dispatch.rs`). Three handles are exposed
/// because the wire deliberately does not serve them: the control router owns
/// no work-graph routes (the fixture is a plane precondition), attempt
/// creation is scheduler-internal (the wire covers binding and finalization),
/// and activity-event ingestion rides the plane's own repository — 035's wire
/// surface is the query side.
struct Plane {
    app: Router,
    bootstrap_token: String,
    work_graph: Arc<InMemoryWorkGraphRepository>,
    attempts: Arc<SqliteAttemptRepository>,
    activity: Arc<SqliteActivityEventRepository>,
}

fn build_plane(dir: &std::path::Path) -> Result<Plane, SpikeFailure> {
    let bootstrap_token = format!("bootstrap_{}", uuid::Uuid::new_v4());
    let plane = ControlPlane::bootstrap(ControlPlaneConfig {
        service_version: "paperclip-spike".to_string(),
        store: ControlPlaneStore::Sqlite {
            database_path: dir.join("work.db").display().to_string(),
        },
        registration_ttl_seconds: 60,
    })
    .map_err(|error| SpikeFailure::new(SpikePhase::Health, "bootstrap", error.to_string()))?;
    let sqlite = |file: &str| dir.join(file);
    let activity = Arc::new(
        SqliteActivityEventRepository::open(&sqlite("activity.db")).map_err(|error| {
            SpikeFailure::new(SpikePhase::Health, "activity-db", error.to_string())
        })?,
    );
    let control_events = Arc::new(
        SqliteControlEventRepository::open(&sqlite("events.db")).map_err(|error| {
            SpikeFailure::new(SpikePhase::Health, "events-db", error.to_string())
        })?,
    );
    let work_graph = Arc::new(InMemoryWorkGraphRepository::default());
    let attempts = Arc::new(
        SqliteAttemptRepository::open(&sqlite("attempts.db")).map_err(|error| {
            SpikeFailure::new(SpikePhase::Health, "attempts-db", error.to_string())
        })?,
    );
    let app = router_with_control_repositories(
        Arc::new(plane),
        BootstrapCredential::from_plaintext(&bootstrap_token).map_err(|error| {
            SpikeFailure::new(SpikePhase::Health, "credential", error.to_string())
        })?,
        Some(Arc::new(InMemoryScopeRepository::default())),
        Some(Arc::new(InMemoryAgentRepository::default())),
        Some(work_graph.clone()),
        Arc::new(InMemoryWakeRepository::default()),
        Some(Arc::new(
            SqliteRunBindingRepository::open(&sqlite("bindings.db")).map_err(|error| {
                SpikeFailure::new(SpikePhase::Health, "bindings-db", error.to_string())
            })?,
        )),
        Some(attempts.clone()),
        Some(Arc::new(
            SqliteRoutineRepository::open(&sqlite("routines.db")).map_err(|error| {
                SpikeFailure::new(SpikePhase::Health, "routines-db", error.to_string())
            })?,
        )),
        Some(Arc::new(
            SqliteApprovalRepository::open(&sqlite("approvals.db")).map_err(|error| {
                SpikeFailure::new(SpikePhase::Health, "approvals-db", error.to_string())
            })?,
        )),
        Some(activity.clone()),
        Some(control_events),
        None,
    );
    Ok(Plane {
        app,
        bootstrap_token,
        work_graph,
        attempts,
        activity,
    })
}

/// The transport one scenario step rides.
enum Wire {
    /// CP-08-84 rehearsal: one in-process round trip through the router.
    Oneshot { app: Router },
    /// CP-08-85 run: one real HTTP round trip against the loopback listener.
    Http {
        base: String,
        client: Box<
            hyper_util::client::legacy::Client<
                hyper_util::client::legacy::connect::HttpConnector,
                Full<bytes::Bytes>,
            >,
        >,
    },
}

impl Wire {
    async fn call(
        &self,
        method: Method,
        uri: &str,
        bearer: Option<&str>,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), SpikeFailure> {
        let (status, bytes) = match self {
            Self::Oneshot { app } => {
                let mut builder = Request::builder().method(method).uri(uri);
                if let Some(token) = bearer {
                    builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
                }
                let request = match body {
                    Some(payload) => builder
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload.to_string())),
                    None => builder.body(Body::empty()),
                }
                .map_err(|error| {
                    SpikeFailure::new(SpikePhase::Health, "request-build", error.to_string())
                })?;
                let response = app.clone().oneshot(request).await.map_err(|error| {
                    SpikeFailure::new(SpikePhase::Health, "oneshot", error.to_string())
                })?;
                let status = response.status();
                let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .map_err(|error| {
                        SpikeFailure::new(SpikePhase::Health, "body", error.to_string())
                    })?;
                (status, bytes)
            }
            Self::Http { base, client } => {
                let full_uri = format!("{base}{uri}");
                let mut builder = Request::builder().method(method).uri(&full_uri);
                if let Some(token) = bearer {
                    builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
                }
                let request = match body {
                    Some(payload) => builder
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(Full::new(bytes::Bytes::from(payload.to_string()))),
                    None => builder.body(Full::new(bytes::Bytes::new())),
                }
                .map_err(|error| {
                    SpikeFailure::new(SpikePhase::Health, "request-build", error.to_string())
                })?;
                let response = client.request(request).await.map_err(|error| {
                    SpikeFailure::new(SpikePhase::Health, "http", error.to_string())
                })?;
                let status = response.status();
                let bytes = response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|error| {
                        SpikeFailure::new(SpikePhase::Health, "http-body", error.to_string())
                    })?
                    .to_bytes();
                (status, bytes)
            }
        };
        let parsed = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        };
        Ok((status, parsed))
    }
}

/// Bind the production router to a loopback listener (real-plane mode) and
/// return the HTTP wire plus a guard that tears the server down on drop.
async fn serve_plane(
    app: Router,
    bind: Option<SocketAddr>,
) -> Result<(Wire, SocketAddr, ServerGuard), SpikeFailure> {
    let address = bind.unwrap_or_else(|| "127.0.0.1:0".parse().expect("loopback fallback"));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| SpikeFailure::new(SpikePhase::Health, "bind", error.to_string()))?;
    let bound = listener
        .local_addr()
        .map_err(|error| SpikeFailure::new(SpikePhase::Health, "bind", error.to_string()))?;
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("spike plane server");
    });
    let wire = Wire::Http {
        base: format!("http://{bound}"),
        client: Box::new(
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http(),
        ),
    };
    Ok((
        wire,
        bound,
        ServerGuard {
            task: task.abort_handle(),
        },
    ))
}

/// Tears the real-plane listener down when the scenario ends.
struct ServerGuard {
    task: tokio::task::AbortHandle,
}
impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Poll the downstream plane's projection for the spike Work (045): the
/// Paperclip issue must sit in `in_review` with at least one work product
/// attached — evidence, not just a state flag. The plane runs in its default
/// `local_trusted` mode (localhost only, downstream charter §2.8), where the
/// issue API answers the implicit board actor without a session; a
/// projection that never reaches Review within the budget is a typed
/// review-phase failure.
async fn review_projection(
    wire: &Wire,
    downstream: &DownstreamPlane,
) -> Result<String, SpikeFailure> {
    let Wire::Http { client, .. } = wire else {
        return Err(SpikeFailure::new(
            SpikePhase::Review,
            "projection",
            "review phase requires the real-plane wire",
        ));
    };
    let uri = format!(
        "{}/api/issues/{}",
        downstream.base_url.trim_end_matches('/'),
        downstream.issue_id
    );
    let mut last = String::new();
    for _ in 0..45 {
        let request = Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Full::new(bytes::Bytes::new()))
            .map_err(|error| {
                SpikeFailure::new(SpikePhase::Review, "probe-build", error.to_string())
            })?;
        match client.request(request).await {
            Ok(response) => {
                let status = response.status();
                let bytes = response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|error| {
                        SpikeFailure::new(SpikePhase::Review, "probe-body", error.to_string())
                    })?
                    .to_bytes();
                let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                last = format!(
                    "{} status={} workProducts={}",
                    status,
                    body["status"].as_str().unwrap_or("?"),
                    body["workProducts"].as_array().map(Vec::len).unwrap_or(0)
                );
                let in_review = body["status"].as_str() == Some("in_review");
                let has_evidence = body["workProducts"]
                    .as_array()
                    .is_some_and(|products| !products.is_empty());
                if status == StatusCode::OK && in_review && has_evidence {
                    return Ok(last);
                }
            }
            Err(error) => {
                last = error.to_string();
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    Err(SpikeFailure::new(
        SpikePhase::Review,
        "projection",
        format!("downstream projection never reached review with evidence: {last}"),
    ))
}

/// Assert a response landed on the expected status, returning the body.
fn expect(
    phase: SpikePhase,
    step: &'static str,
    expected: StatusCode,
    status: StatusCode,
    body: Value,
) -> Result<Value, SpikeFailure> {
    if status != expected {
        return Err(SpikeFailure::new(
            phase,
            step,
            format!("expected {expected}, got {status}: {body}"),
        ));
    }
    Ok(body)
}

fn host_registration(
    agent_instance: &AgentInstanceId,
    grant_token: &str,
    protocol_major: u16,
) -> Value {
    json!({
        "grant_token": grant_token,
        "host": {
            "agent_instance_id": agent_instance.to_json_value(),
            "workspaces": [],
            "capabilities": { "values": ["work_attempts"] },
            "protocol_major": protocol_major,
        }
    })
}

/// Run the full spike scenario. Emits one progress line per step to stdout
/// (or a single JSON report when `json`), and returns the first typed
/// failure if any invariant breaks.
pub async fn run(json: bool, mode: SpikeMode) -> Result<(), SpikeFailure> {
    let dir = tempfile::tempdir()
        .map_err(|error| SpikeFailure::new(SpikePhase::Health, "tempdir", error.to_string()))?;
    let plane = build_plane(dir.path())?;
    let token = plane.bootstrap_token.clone();
    let mut report: Vec<Value> = Vec::new();
    let record = |step: &str, detail: String, report: &mut Vec<Value>| {
        if !json {
            println!("paperclip-spike: {step}: {detail}");
        }
        report.push(json!({ "step": step, "detail": detail }));
    };
    let (wire, _server) = match &mode {
        SpikeMode::Stub => (
            Wire::Oneshot {
                app: plane.app.clone(),
            },
            None,
        ),
        SpikeMode::Real { bind, .. } => {
            let (wire, address, server) = serve_plane(plane.app.clone(), *bind).await?;
            record(
                "serve",
                format!("control plane serving real HTTP on http://{address}"),
                &mut report,
            );
            (wire, Some(server))
        }
    };

    // -- Phase 0: health is bootstrap-gated and reports the wire version ----
    let (status, _) = wire.call(Method::GET, "/v1/health", None, None).await?;
    if status != StatusCode::UNAUTHORIZED {
        return Err(SpikeFailure::new(
            SpikePhase::Health,
            "health-anonymous",
            format!("expected 401 without bootstrap bearer, got {status}"),
        ));
    }
    let (status, body) = wire
        .call(Method::GET, "/v1/health", Some(&token), None)
        .await?;
    let body = expect(SpikePhase::Health, "health", StatusCode::OK, status, body)?;
    let protocol_major = body["protocol_major"].as_u64().ok_or_else(|| {
        SpikeFailure::new(
            SpikePhase::Health,
            "health",
            "response missing protocol_major",
        )
    })? as u16;
    record(
        "health",
        format!(
            "service {} protocol v{protocol_major}",
            body["service_version"].as_str().unwrap_or("?")
        ),
        &mut report,
    );

    // -- Phase 1: registration chain ----------------------------------------
    let agent_instance = AgentInstanceId::new("ai_spike_host");
    let (status, body) = wire
        .call(Method::POST, "/v1/registration-grants", Some(&token), None)
        .await?;
    let body = expect(
        SpikePhase::Registration,
        "grant",
        StatusCode::OK,
        status,
        body,
    )?;
    let grant = body["token"]
        .as_str()
        .ok_or_else(|| {
            SpikeFailure::new(SpikePhase::Registration, "grant", "response missing token")
        })?
        .to_string();
    record(
        "grant",
        "one-time registration grant issued".to_string(),
        &mut report,
    );

    let registration = host_registration(&agent_instance, &grant, protocol_major);
    let (status, body) = wire
        .call(
            Method::POST,
            "/v1/hosts/register",
            None,
            Some(registration.clone()),
        )
        .await?;
    let body = expect(
        SpikePhase::Registration,
        "register",
        StatusCode::OK,
        status,
        body,
    )?;
    if body["agent_instance_id"]["value"].as_str() != Some(agent_instance.value.as_str()) {
        return Err(SpikeFailure::new(
            SpikePhase::Registration,
            "register",
            format!("plane did not echo agent_instance_id: {body}"),
        ));
    }
    record(
        "register",
        format!("host {agent_instance} registered"),
        &mut report,
    );

    // The grant is one-time: replaying it must not mint a second identity.
    let (status, body) = wire
        .call(Method::POST, "/v1/hosts/register", None, Some(registration))
        .await?;
    if status != StatusCode::UNAUTHORIZED {
        return Err(SpikeFailure::new(
            SpikePhase::Registration,
            "grant-replay",
            format!("expected 401 replaying consumed grant, got {status}: {body}"),
        ));
    }
    record(
        "grant-replay",
        "consumed grant rejected (one-time)".to_string(),
        &mut report,
    );

    // -- Phase 2: Work exists first ------------------------------------------
    // The control router does not serve the work-graph routes (those belong
    // to the plane builder's earlier scope), so the fixture is seeded through
    // the repository the router was wired with — the precondition of plan
    // step 2, not a wire step itself.
    let work_item = WorkItemId::new("wi_spike_fixture");
    plane
        .work_graph
        .register_work_item(work_item.clone())
        .map_err(|_| {
            SpikeFailure::new(SpikePhase::Fixture, "work-item", "registration rejected")
        })?;
    record(
        "work-item",
        format!("fixture {work_item} registered"),
        &mut report,
    );

    // -- Phase 3: dispatch (wake coalescing, 020) -----------------------------
    let enqueue = |source: &str| {
        json!({
            "work_item_id": work_item.to_json_value(),
            "source": source,
            "requested_at": "2026-08-16T00:00:00Z",
        })
    };
    let (status, body) = wire
        .call(
            Method::POST,
            "/v1/wakes",
            Some(&token),
            Some(enqueue("manual")),
        )
        .await?;
    let body = expect(
        SpikePhase::Dispatch,
        "enqueue",
        StatusCode::OK,
        status,
        body,
    )?;
    let wake_id = body["id"]
        .as_str()
        .ok_or_else(|| {
            SpikeFailure::new(SpikePhase::Dispatch, "enqueue", "wake response missing id")
        })?
        .to_string();
    record("enqueue", format!("wake {wake_id} enqueued"), &mut report);

    let (status, body) = wire
        .call(
            Method::POST,
            "/v1/wakes",
            Some(&token),
            Some(enqueue("comment")),
        )
        .await?;
    let body = expect(
        SpikePhase::Dispatch,
        "coalesce",
        StatusCode::OK,
        status,
        body,
    )?;
    if body["id"].as_str() != Some(wake_id.as_str())
        || body["sources"].as_array().is_none_or(Vec::is_empty)
        || body["sources"].as_array().map(Vec::len) != Some(2)
    {
        return Err(SpikeFailure::new(
            SpikePhase::Dispatch,
            "coalesce",
            format!("second source did not coalesce into one wake: {body}"),
        ));
    }
    record(
        "coalesce",
        "second source coalesced (020)".to_string(),
        &mut report,
    );

    // -- Phase 4: claim + exactly-one transactional checkout ------------------
    let claim_uri = format!("/v1/wakes/{}/claim", work_item.value);
    let (status, body) = wire
        .call(
            Method::POST,
            &claim_uri,
            Some(&token),
            Some(json!({ "claimed_at": "2026-08-16T00:00:01Z" })),
        )
        .await?;
    let body = expect(SpikePhase::Checkout, "claim", StatusCode::OK, status, body)?;
    if body["claimed_at"].is_null() {
        return Err(SpikeFailure::new(
            SpikePhase::Checkout,
            "claim",
            format!("claim did not stamp claimed_at: {body}"),
        ));
    }
    record("claim", "wake claimed by host".to_string(), &mut report);

    let attempt = AttemptId::new("att_spike_1");
    let checkout = |agent: &AgentInstanceId, attempt: &AttemptId| {
        json!({
            "lease": {
                "work_item_id": work_item.to_json_value(),
                "owner_agent_instance_id": agent.to_json_value(),
                "attempt_id": attempt.to_json_value(),
                "expires_at_unix_seconds": 4_102_444_800u64,
            },
            "now_unix_seconds": 1_786_060_000u64,
        })
    };
    let lease = checkout(&agent_instance, &attempt);
    let (status, body) = wire
        .call(
            Method::POST,
            "/v1/work-checkouts",
            Some(&token),
            Some(lease.clone()),
        )
        .await?;
    expect(
        SpikePhase::Checkout,
        "checkout",
        StatusCode::CREATED,
        status,
        body,
    )?;
    record("checkout", format!("lease held for {attempt}"), &mut report);

    // -- Real-plane phases (CP-08-85): attempt, translated events, review ---
    if let SpikeMode::Real { downstream, .. } = &mode {
        // The attempt record itself is scheduler-internal (no creation route
        // by design — the wire covers binding and finalization only).
        plane
            .attempts
            .create(Attempt {
                id: attempt.clone(),
                work_item_id: work_item.clone(),
                owner_agent_instance_id: agent_instance.clone(),
                profile_revision_id: AgentProfileRevisionId::new("spike"),
                state: AttemptState::Created,
                created_at_unix_seconds: 1_786_060_050,
                updated_at_unix_seconds: 1_786_060_050,
            })
            .map_err(|error| {
                SpikeFailure::new(SpikePhase::Attempt, "attempt-create", error.to_string())
            })?;

        // 052: the executor run binds to the attempt over the wire.
        let run_id = RunId::new("spike_1");
        let binding = json!({
            "attempt_id": attempt.to_json_value(),
            "work_item_id": work_item.to_json_value(),
            "owner_agent_instance_id": agent_instance.to_json_value(),
            "run_id": run_id.to_json_value(),
            "bound_at_unix_seconds": 1_786_060_100u64,
        });
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/runtime/run-bindings",
                Some(&token),
                Some(binding),
            )
            .await?;
        expect(
            SpikePhase::Attempt,
            "bind-run",
            StatusCode::OK,
            status,
            body,
        )?;
        record(
            "bind-run",
            format!("run {run_id} bound to {attempt}"),
            &mut report,
        );

        // The scheduler-internal lifecycle the finalizer builds on: a run can
        // only finalize an attempt that reached Running.
        let mut now = 1_786_060_110u64;
        for state in [
            AttemptState::Claimed,
            AttemptState::Dispatched,
            AttemptState::Running,
        ] {
            plane
                .attempts
                .transition(&attempt, state, now)
                .map_err(|error| {
                    SpikeFailure::new(SpikePhase::Attempt, "attempt-transition", error.to_string())
                })?;
            now += 10;
        }

        // 034/035: a scripted IsanAgent run — the production lifecycle→event
        // translation maps a deterministic Started/Terminated pair, so no
        // model is called (charter §2.8 review still unpassed, localhost
        // only). Ingestion rides the plane's own repository; the wire surface
        // for 035 is the framed query below.
        let chat_id = format!("chat_{attempt}");
        let lifecycle = [
            RunLifecycleEvent::Started {
                run_id: run_id.value.clone(),
                chat_id: chat_id.clone(),
            },
            RunLifecycleEvent::Terminated {
                run_id: run_id.value.clone(),
                chat_id,
                outcome: RunOutcome::Completed,
            },
        ];
        let organization = OrganizationId::new("spike");
        for (sequence, event) in lifecycle.iter().map(map_lifecycle_to_event).enumerate() {
            let summary = match &event {
                Event::RunStarted { run_id } => format!("run {run_id} started"),
                Event::RunTerminated { run_id, outcome } => {
                    format!("run {run_id} terminated: {outcome}")
                }
                other => format!("{other:?}"),
            };
            plane
                .activity
                .append(ActivityEvent {
                    event_id: format!("evt_spike_{attempt}_{sequence}"),
                    kind: EventKind::AttemptTransitioned,
                    actor: Actor::System {
                        component: "paperclip-spike".into(),
                    },
                    timestamp: "2026-08-16T00:01:00Z".into(),
                    organization_id: organization.clone(),
                    project_id: None,
                    work_item_id: Some(work_item.clone()),
                    attempt_id: Some(attempt.clone()),
                    summary,
                    correlation_id: Some(run_id.value.clone()),
                    causation_id: None,
                })
                .map_err(|error| {
                    SpikeFailure::new(SpikePhase::Attempt, "event-append", error.to_string())
                })?;
        }
        record(
            "attempt-events",
            "scripted run translated to audit events (035)".to_string(),
            &mut report,
        );

        // 034: run completion is the verification signal — the finalized
        // attempt never directly completes Work.
        let finalize_uri = format!("/v1/runtime/attempts/{}/finalize", attempt.value);
        let (status, body) = wire
            .call(
                Method::POST,
                &finalize_uri,
                Some(&token),
                Some(json!({
                    "outcome": "succeeded",
                    "observed_at_unix_seconds": 1_786_060_200u64,
                })),
            )
            .await?;
        let body = expect(
            SpikePhase::Attempt,
            "finalize",
            StatusCode::OK,
            status,
            body,
        )?;
        if body["state"].as_str() != Some("succeeded") {
            return Err(SpikeFailure::new(
                SpikePhase::Attempt,
                "finalize",
                format!("attempt did not finalize as succeeded: {body}"),
            ));
        }
        record(
            "finalize",
            format!("{attempt} finalized by run completion (034)"),
            &mut report,
        );

        // 035: the downstream observes the translated events through the
        // framed query wire, filtered to the Work item.
        let query = json!({
            "id": "spike-query-1",
            "version": { "major": protocol_major, "minor": 0 },
            "actor": { "kind": "system", "component": "paperclip-spike" },
            "payload": {
                "type": "query_activity",
                "payload": {
                    "organization_id": organization.to_json_value(),
                    "page": { "cursor": null, "limit": 50 },
                    "kind": null,
                    "work_item_id": work_item.to_json_value(),
                },
            },
        });
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/protocol/commands",
                Some(&token),
                Some(query),
            )
            .await?;
        let body = expect(
            SpikePhase::Events,
            "query-activity",
            StatusCode::OK,
            status,
            body,
        )?;
        let items = body["result"]["Ok"]["payload"]["items"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let translated = items
            .iter()
            .filter(|item| item["correlation_id"] == json!(run_id.value))
            .count();
        if translated < 2 {
            return Err(SpikeFailure::new(
                SpikePhase::Events,
                "query-activity",
                format!(
                    "expected the translated run events over the wire, found {translated}: {body}"
                ),
            ));
        }
        record(
            "query-activity",
            format!("{translated} translated events observed over the framed wire (035)"),
            &mut report,
        );

        // 045: the downstream projection reaches Review with evidence. The
        // assertion runs only against a configured downstream plane; without
        // one it records a typed skip (the 080 acceptance run sets it).
        match downstream {
            Some(plane) => {
                let projection = review_projection(&wire, plane).await?;
                record(
                    "review",
                    format!("downstream projection reached review: {projection}"),
                    &mut report,
                );
            }
            None => record(
                "review",
                "skipped: no downstream plane configured (080 acceptance run sets --downstream-url and --downstream-issue)"
                    .to_string(),
                &mut report,
            ),
        }
    }

    // -- Phase 5: reconnect, twice, with lease state asserted between runs ---
    for run in 1..=2u8 {
        // A reconnecting host re-registers through a fresh grant.
        let (status, body) = wire
            .call(Method::POST, "/v1/registration-grants", Some(&token), None)
            .await?;
        let body = expect(
            SpikePhase::Reconnect,
            "regrant",
            StatusCode::OK,
            status,
            body,
        )?;
        let fresh_grant = body["token"]
            .as_str()
            .ok_or_else(|| SpikeFailure::new(SpikePhase::Reconnect, "regrant", "missing token"))?
            .to_string();
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/hosts/register",
                None,
                Some(host_registration(
                    &agent_instance,
                    &fresh_grant,
                    protocol_major,
                )),
            )
            .await?;
        expect(
            SpikePhase::Reconnect,
            "reregister",
            StatusCode::OK,
            status,
            body,
        )?;

        // The wake does not double-fire (020): a re-claim is a typed conflict.
        let (status, body) = wire
            .call(
                Method::POST,
                &claim_uri,
                Some(&token),
                Some(json!({ "claimed_at": "2026-08-16T00:00:02Z" })),
            )
            .await?;
        if status != StatusCode::CONFLICT {
            return Err(SpikeFailure::new(
                SpikePhase::Reconnect,
                "reclaim",
                format!("run {run}: re-claim expected 409 AlreadyClaimed, got {status}: {body}"),
            ));
        }

        // The lease is not stolen and reattaches idempotently: replaying the
        // same lease is a conflict whose survivor is the original owner.
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/work-checkouts",
                Some(&token),
                Some(lease.clone()),
            )
            .await?;
        if status != StatusCode::CONFLICT {
            return Err(SpikeFailure::new(
                SpikePhase::Reconnect,
                "recheckout",
                format!("run {run}: same-lease re-checkout expected 409, got {status}: {body}"),
            ));
        }

        // A rival owner must not take the live lease (020–021).
        let rival = checkout(
            &AgentInstanceId::new("ai_spike_rival"),
            &AttemptId::new("att_spike_rival"),
        );
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/work-checkouts",
                Some(&token),
                Some(rival),
            )
            .await?;
        if status != StatusCode::CONFLICT {
            return Err(SpikeFailure::new(
                SpikePhase::Reconnect,
                "rival-checkout",
                format!("run {run}: rival lease expected 409, got {status}: {body}"),
            ));
        }

        // A stale finalizer cannot release another attempt's lease (021).
        let stale_release = json!({
            "work_item_id": work_item.to_json_value(),
            "attempt_id": AttemptId::new("att_spike_stale").to_json_value(),
        });
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/work-checkouts/release",
                Some(&token),
                Some(stale_release),
            )
            .await?;
        if status != StatusCode::CONFLICT {
            return Err(SpikeFailure::new(
                SpikePhase::Reconnect,
                "stale-release",
                format!("run {run}: stale finalizer expected 409, got {status}: {body}"),
            ));
        }

        // Between-run state check: the lease survived every attempt above —
        // the rightful owner's replay still conflicts as the live holder.
        let (status, body) = wire
            .call(
                Method::POST,
                "/v1/work-checkouts",
                Some(&token),
                Some(lease.clone()),
            )
            .await?;
        if status != StatusCode::CONFLICT {
            return Err(SpikeFailure::new(
                SpikePhase::Reconnect,
                "lease-persisted",
                format!("run {run}: lease did not persist across reconnect: {status} {body}"),
            ));
        }
        record(
            "reconnect",
            format!("run {run}: no double fire, lease intact, stale finalizer rejected"),
            &mut report,
        );
    }

    // Recovery (023): the rightful owner releases, and the checkout
    // reattaches idempotently for a fresh attempt.
    let rightful_release = json!({
        "work_item_id": work_item.to_json_value(),
        "attempt_id": attempt.to_json_value(),
    });
    let (status, body) = wire
        .call(
            Method::POST,
            "/v1/work-checkouts/release",
            Some(&token),
            Some(rightful_release),
        )
        .await?;
    expect(
        SpikePhase::Reconnect,
        "release",
        StatusCode::NO_CONTENT,
        status,
        body,
    )?;
    record(
        "release",
        "rightful owner released the lease".to_string(),
        &mut report,
    );

    let next_attempt = AttemptId::new("att_spike_2");
    let (status, body) = wire
        .call(
            Method::POST,
            "/v1/work-checkouts",
            Some(&token),
            Some(checkout(&agent_instance, &next_attempt)),
        )
        .await?;
    expect(
        SpikePhase::Reconnect,
        "reattach",
        StatusCode::CREATED,
        status,
        body,
    )?;
    record(
        "reattach",
        "fresh attempt checked out after recovery (023)".to_string(),
        &mut report,
    );

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "phase": "complete", "steps": report }))
                .expect("spike report serializes")
        );
    } else {
        let plane = match mode {
            SpikeMode::Stub => "stub plane",
            SpikeMode::Real { .. } => "real plane",
        };
        println!("paperclip-spike: PASS ({plane})");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stub_plane_scenario_passes() {
        run(false, SpikeMode::Stub)
            .await
            .expect("scenario against stub plane");
    }

    /// CP-08-85: the same scenario over real HTTP on a loopback listener,
    /// including the attempt, translated-events, and (skipped) review phases.
    #[tokio::test]
    async fn real_plane_scenario_passes_over_http() {
        run(
            false,
            SpikeMode::Real {
                bind: None,
                downstream: None,
            },
        )
        .await
        .expect("scenario over real HTTP plane");
    }

    #[tokio::test]
    async fn phases_have_distinct_exit_codes() {
        let codes = [
            SpikePhase::Health.exit_code(),
            SpikePhase::Registration.exit_code(),
            SpikePhase::Fixture.exit_code(),
            SpikePhase::Dispatch.exit_code(),
            SpikePhase::Checkout.exit_code(),
            SpikePhase::Attempt.exit_code(),
            SpikePhase::Events.exit_code(),
            SpikePhase::Review.exit_code(),
            SpikePhase::Reconnect.exit_code(),
        ];
        let mut seen = std::collections::HashSet::new();
        for code in codes {
            assert!(code > 10, "spike codes must not collide with run codes");
            assert!(seen.insert(code), "duplicate spike exit code {code}");
        }
    }
}
