//! Work OS CP-08-84 acceptance-spike harness (`PAPERCLIP_SPIKE_PLAN.md` §3).
//!
//! Drives the real control-plane axum router in-process against a stub plane
//! (tempdir SQLite store) — no network, no daemon. This is the wire rehearsal
//! for the downstream Paperclip plane: exactly the HTTP shapes the
//! `altai-host` adapter will speak in CP-08-85 (registration chain, Work
//! fixture, wake dispatch, transactional checkout, and the reconnect
//! idempotency invariants of CP-08-020 through 024).
//!
//! The reconnect step runs twice, with the lease state asserted between runs:
//! a re-registering host must not double-fire the wake, must not steal (or
//! lose) the live lease, and a stale finalizer must not release another
//! attempt's lease. Any break exits non-zero with a phase-typed code.

use std::sync::Arc;

use altai_control_plane::{
    BootstrapCredential, ControlPlane, ControlPlaneConfig, ControlPlaneStore,
    InMemoryAgentRepository, InMemoryScopeRepository, InMemoryWakeRepository,
    InMemoryWorkGraphRepository, SqliteActivityEventRepository, SqliteApprovalRepository,
    SqliteAttemptRepository, SqliteControlEventRepository, SqliteRoutineRepository,
    SqliteRunBindingRepository, WorkGraphRepository, router_with_control_repositories,
};
use altai_control_protocol::{AgentInstanceId, AttemptId, WorkItemId};
use axum::{
    Router, body::Body,
    http::{Method, Request, StatusCode, header::AUTHORIZATION},
};
use serde_json::{Value, json};
use tower::ServiceExt;

/// Distinct from the `run` command's public exit codes (0–8, 10); each phase
/// of the spike reports its own so a red harness run names the broken link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpikePhase {
    Health,
    Registration,
    Fixture,
    Dispatch,
    Checkout,
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

/// The stub plane: production router wiring over throwaway SQLite and
/// in-memory repositories, exactly as the daemon's own dispatch tests build
/// it (`altai-control-plane` `protocol_dispatch.rs`). The work-graph handle
/// is exposed because the control router does not serve work-graph routes;
/// the scenario seeds its fixture through the repository instead.
struct StubPlane {
    app: Router,
    bootstrap_token: String,
    work_graph: Arc<InMemoryWorkGraphRepository>,
}

fn stub_plane(dir: &std::path::Path) -> Result<StubPlane, SpikeFailure> {
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
        SqliteActivityEventRepository::open(&sqlite("activity.db"))
            .map_err(|error| SpikeFailure::new(SpikePhase::Health, "activity-db", error.to_string()))?,
    );
    let control_events = Arc::new(
        SqliteControlEventRepository::open(&sqlite("events.db"))
            .map_err(|error| SpikeFailure::new(SpikePhase::Health, "events-db", error.to_string()))?,
    );
    let work_graph = Arc::new(InMemoryWorkGraphRepository::default());
    let app = router_with_control_repositories(
        Arc::new(plane),
        BootstrapCredential::from_plaintext(&bootstrap_token)
            .map_err(|error| SpikeFailure::new(SpikePhase::Health, "credential", error.to_string()))?,
        Some(Arc::new(InMemoryScopeRepository::default())),
        Some(Arc::new(InMemoryAgentRepository::default())),
        Some(work_graph.clone()),
        Arc::new(InMemoryWakeRepository::default()),
        Some(Arc::new(
            SqliteRunBindingRepository::open(&sqlite("bindings.db")).map_err(|error| {
                SpikeFailure::new(SpikePhase::Health, "bindings-db", error.to_string())
            })?,
        )),
        Some(Arc::new(
            SqliteAttemptRepository::open(&sqlite("attempts.db")).map_err(|error| {
                SpikeFailure::new(SpikePhase::Health, "attempts-db", error.to_string())
            })?,
        )),
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
        Some(activity),
        Some(control_events),
        None,
    );
    Ok(StubPlane {
        app,
        bootstrap_token,
        work_graph,
    })
}

impl StubPlane {
    /// One in-process HTTP round trip through the real router.
    async fn call(
        &self,
        method: Method,
        uri: &str,
        bearer: Option<&str>,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), SpikeFailure> {
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
        .map_err(|error| SpikeFailure::new(SpikePhase::Health, "request-build", error.to_string()))?;
        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .map_err(|error| SpikeFailure::new(SpikePhase::Health, "oneshot", error.to_string()))?;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|error| SpikeFailure::new(SpikePhase::Health, "body", error.to_string()))?;
        let parsed = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::String(
                String::from_utf8_lossy(&bytes).into_owned(),
            ))
        };
        Ok((status, parsed))
    }
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

fn host_registration(agent_instance: &AgentInstanceId, grant_token: &str, protocol_major: u16) -> Value {
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
pub async fn run(json: bool) -> Result<(), SpikeFailure> {
    let dir = tempfile::tempdir()
        .map_err(|error| SpikeFailure::new(SpikePhase::Health, "tempdir", error.to_string()))?;
    let plane = stub_plane(dir.path())?;
    let token = plane.bootstrap_token.clone();
    let mut report: Vec<Value> = Vec::new();
    let record = |step: &str, detail: String, report: &mut Vec<Value>| {
        if !json {
            println!("paperclip-spike: {step}: {detail}");
        }
        report.push(json!({ "step": step, "detail": detail }));
    };

    // -- Phase 0: health is bootstrap-gated and reports the wire version ----
    let (status, _) = plane.call(Method::GET, "/v1/health", None, None).await?;
    if status != StatusCode::UNAUTHORIZED {
        return Err(SpikeFailure::new(
            SpikePhase::Health,
            "health-anonymous",
            format!("expected 401 without bootstrap bearer, got {status}"),
        ));
    }
    let (status, body) = plane.call(Method::GET, "/v1/health", Some(&token), None).await?;
    let body = expect(SpikePhase::Health, "health", StatusCode::OK, status, body)?;
    let protocol_major = body["protocol_major"].as_u64().ok_or_else(|| {
        SpikeFailure::new(SpikePhase::Health, "health", "response missing protocol_major")
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
    let (status, body) = plane
        .call(Method::POST, "/v1/registration-grants", Some(&token), None)
        .await?;
    let body = expect(
        SpikePhase::Registration,
        "grant",
        StatusCode::OK,
        status,
        body,
    )?;
    let grant = body["token"].as_str().ok_or_else(|| {
        SpikeFailure::new(SpikePhase::Registration, "grant", "response missing token")
    })?.to_string();
    record("grant", "one-time registration grant issued".to_string(), &mut report);

    let registration = host_registration(&agent_instance, &grant, protocol_major);
    let (status, body) = plane
        .call(Method::POST, "/v1/hosts/register", None, Some(registration.clone()))
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
    let (status, body) = plane
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
        .map_err(|_| SpikeFailure::new(SpikePhase::Fixture, "work-item", "registration rejected"))?;
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
    let (status, body) = plane
        .call(Method::POST, "/v1/wakes", Some(&token), Some(enqueue("manual")))
        .await?;
    let body = expect(SpikePhase::Dispatch, "enqueue", StatusCode::OK, status, body)?;
    let wake_id = body["id"].as_str().ok_or_else(|| {
        SpikeFailure::new(SpikePhase::Dispatch, "enqueue", "wake response missing id")
    })?.to_string();
    record("enqueue", format!("wake {wake_id} enqueued"), &mut report);

    let (status, body) = plane
        .call(Method::POST, "/v1/wakes", Some(&token), Some(enqueue("comment")))
        .await?;
    let body = expect(SpikePhase::Dispatch, "coalesce", StatusCode::OK, status, body)?;
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
    record("coalesce", "second source coalesced (020)".to_string(), &mut report);

    // -- Phase 4: claim + exactly-one transactional checkout ------------------
    let claim_uri = format!("/v1/wakes/{}/claim", work_item.value);
    let (status, body) = plane
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
    let (status, body) = plane
        .call(Method::POST, "/v1/work-checkouts", Some(&token), Some(lease.clone()))
        .await?;
    expect(SpikePhase::Checkout, "checkout", StatusCode::CREATED, status, body)?;
    record("checkout", format!("lease held for {attempt}"), &mut report);

    // -- Phase 5: reconnect, twice, with lease state asserted between runs ---
    for run in 1..=2u8 {
        // A reconnecting host re-registers through a fresh grant.
        let (status, body) = plane
            .call(Method::POST, "/v1/registration-grants", Some(&token), None)
            .await?;
        let body = expect(SpikePhase::Reconnect, "regrant", StatusCode::OK, status, body)?;
        let fresh_grant = body["token"].as_str().ok_or_else(|| {
            SpikeFailure::new(SpikePhase::Reconnect, "regrant", "missing token")
        })?.to_string();
        let (status, body) = plane
            .call(
                Method::POST,
                "/v1/hosts/register",
                None,
                Some(host_registration(&agent_instance, &fresh_grant, protocol_major)),
            )
            .await?;
        expect(SpikePhase::Reconnect, "reregister", StatusCode::OK, status, body)?;

        // The wake does not double-fire (020): a re-claim is a typed conflict.
        let (status, body) = plane
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
        let (status, body) = plane
            .call(Method::POST, "/v1/work-checkouts", Some(&token), Some(lease.clone()))
            .await?;
        if status != StatusCode::CONFLICT {
            return Err(SpikeFailure::new(
                SpikePhase::Reconnect,
                "recheckout",
                format!("run {run}: same-lease re-checkout expected 409, got {status}: {body}"),
            ));
        }

        // A rival owner must not take the live lease (020–021).
        let rival = checkout(&AgentInstanceId::new("ai_spike_rival"), &AttemptId::new("att_spike_rival"));
        let (status, body) = plane
            .call(Method::POST, "/v1/work-checkouts", Some(&token), Some(rival))
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
        let (status, body) = plane
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
        let (status, body) = plane
            .call(Method::POST, "/v1/work-checkouts", Some(&token), Some(lease.clone()))
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
    let (status, body) = plane
        .call(
            Method::POST,
            "/v1/work-checkouts/release",
            Some(&token),
            Some(rightful_release),
        )
        .await?;
    expect(SpikePhase::Reconnect, "release", StatusCode::NO_CONTENT, status, body)?;
    record(
        "release",
        "rightful owner released the lease".to_string(),
        &mut report,
    );

    let next_attempt = AttemptId::new("att_spike_2");
    let (status, body) = plane
        .call(
            Method::POST,
            "/v1/work-checkouts",
            Some(&token),
            Some(checkout(&agent_instance, &next_attempt)),
        )
        .await?;
    expect(SpikePhase::Reconnect, "reattach", StatusCode::CREATED, status, body)?;
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
        println!("paperclip-spike: PASS (stub plane)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stub_plane_scenario_passes() {
        run(false).await.expect("scenario against stub plane");
    }

    #[tokio::test]
    async fn phases_have_distinct_exit_codes() {
        let codes = [
            SpikePhase::Health.exit_code(),
            SpikePhase::Registration.exit_code(),
            SpikePhase::Fixture.exit_code(),
            SpikePhase::Dispatch.exit_code(),
            SpikePhase::Checkout.exit_code(),
            SpikePhase::Reconnect.exit_code(),
        ];
        let mut seen = std::collections::HashSet::new();
        for code in codes {
            assert!(code > 10, "spike codes must not collide with run codes");
            assert!(seen.insert(code), "duplicate spike exit code {code}");
        }
    }
}
