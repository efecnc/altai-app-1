//! CP-08-89 bounded OpenTag ingress contract.
//!
//! This module deliberately has no HTTP client, OpenTag dependency, database,
//! or mutation endpoint. A provider worker verifies its own webhook and passes
//! a small trusted envelope here; this seam then resolves only a pre-registered
//! ALTAI agent, projects the source actor as [`Actor::External`], and accepts a
//! bounded metadata allowlist. The caller must still use the normal versioned
//! protocol, approval, wake, Attempt, Activity and Evidence paths.

use std::collections::{BTreeMap, BTreeSet};

use altai_control_protocol::{Actor, AgentInstanceId};
use sha2::{Digest, Sha256};

use crate::{AgentRepository, AgentRepositoryError};

/// One provider event after the edge has verified its signature. The adapter
/// receives an ALTAI agent id from a local binding, never an OpenTag `agentId`
/// or workspace hint from the source thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenTagInboundEvent {
    pub verified: bool,
    pub provider: String,
    pub provider_event_id: String,
    pub external_actor_id: String,
    pub target_agent_instance_id: AgentInstanceId,
    pub source_thread_uri: String,
    pub metadata: BTreeMap<String, String>,
}

/// The provider-worker policy is explicit and small so an unknown metadata key
/// cannot grow into identity, authorization, or lifecycle authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenTagAdapterPolicy {
    pub integration: String,
    pub allowed_metadata_keys: BTreeSet<String>,
    pub max_metadata_entries: usize,
    pub max_metadata_value_bytes: usize,
}

/// The safe projection handed to a subsequent ALTAI protocol adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedOpenTagEvent {
    pub correlation_id: String,
    pub source_actor: Actor,
    pub target_agent_instance_id: AgentInstanceId,
    pub source_thread_uri: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenTagAdapterError {
    UnverifiedSource,
    InvalidField { field: &'static str },
    TargetAgent(AgentRepositoryError),
    MetadataNotAllowed { key: String },
    MetadataTooLarge { key: String },
    CredentialLikeMetadata { key: String },
}

impl std::fmt::Display for OpenTagAdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "opentag adapter error: {self:?}")
    }
}
impl std::error::Error for OpenTagAdapterError {}

fn non_empty(value: &str, field: &'static str) -> Result<(), OpenTagAdapterError> {
    (!value.trim().is_empty())
        .then_some(())
        .ok_or(OpenTagAdapterError::InvalidField { field })
}

fn credential_like(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    [
        "token",
        "secret",
        "credential",
        "password",
        "api_key",
        "apikey",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Normalize a verified source event without creating a user, agent, Work,
/// Attempt, lease, Activity or Evidence record. Equal events return an equal
/// correlation id; the durable idempotency ledger belongs to the caller's
/// canonical ALTAI path.
pub fn normalize_opentag_event(
    event: OpenTagInboundEvent,
    policy: &OpenTagAdapterPolicy,
    agents: &dyn AgentRepository,
) -> Result<NormalizedOpenTagEvent, OpenTagAdapterError> {
    if !event.verified {
        return Err(OpenTagAdapterError::UnverifiedSource);
    }
    non_empty(&policy.integration, "integration")?;
    non_empty(&event.provider, "provider")?;
    non_empty(&event.provider_event_id, "provider_event_id")?;
    non_empty(&event.external_actor_id, "external_actor_id")?;
    non_empty(&event.source_thread_uri, "source_thread_uri")?;
    if event.metadata.len() > policy.max_metadata_entries {
        return Err(OpenTagAdapterError::InvalidField { field: "metadata" });
    }
    for (key, value) in &event.metadata {
        if !policy.allowed_metadata_keys.contains(key) {
            return Err(OpenTagAdapterError::MetadataNotAllowed { key: key.clone() });
        }
        if value.len() > policy.max_metadata_value_bytes {
            return Err(OpenTagAdapterError::MetadataTooLarge { key: key.clone() });
        }
        if credential_like(key) || credential_like(value) {
            return Err(OpenTagAdapterError::CredentialLikeMetadata { key: key.clone() });
        }
    }
    agents
        .ensure_dispatchable(&event.target_agent_instance_id)
        .map_err(OpenTagAdapterError::TargetAgent)?;
    let correlation_id = format!(
        "ot_{}",
        hex::encode(Sha256::digest(
            format!(
                "{}\n{}\n{}",
                policy.integration, event.provider, event.provider_event_id
            )
            .as_bytes()
        ))
    );
    Ok(NormalizedOpenTagEvent {
        correlation_id,
        source_actor: Actor::External {
            integration: format!("{}:{}", policy.integration, event.provider),
            external_actor_id: event.external_actor_id,
        },
        target_agent_instance_id: event.target_agent_instance_id,
        source_thread_uri: event.source_thread_uri,
        metadata: event.metadata,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryAgentRepository;
    use altai_control_protocol::{
        AgentInstance, AgentProfileId, AgentProfileRevision, AgentProfileRevisionId, AgentStatus,
        OrganizationId, Revision,
    };

    fn agents(status: AgentStatus) -> InMemoryAgentRepository {
        let agents = InMemoryAgentRepository::default();
        agents
            .append_profile_revision(AgentProfileRevision {
                id: AgentProfileRevisionId::new("opentag-v1"),
                profile_id: AgentProfileId::new("opentag"),
                revision: Revision::INITIAL,
                instructions: "fixture".into(),
                model: None,
                capabilities: vec![],
                created_at: "2026-08-20T00:00:00Z".into(),
            })
            .unwrap();
        agents
            .create_instance(AgentInstance {
                id: AgentInstanceId::new("ai_opentag_target"),
                organization_id: OrganizationId::new("org"),
                profile_revision_id: AgentProfileRevisionId::new("opentag-v1"),
                reports_to_agent_id: None,
                name: "canonical target".into(),
                role: "worker".into(),
                capabilities: vec![],
                status,
                pause_reason: None,
                revision: Revision::INITIAL,
                created_at: "2026-08-20T00:00:00Z".into(),
                updated_at: "2026-08-20T00:00:00Z".into(),
            })
            .unwrap();
        agents
    }

    fn policy() -> OpenTagAdapterPolicy {
        OpenTagAdapterPolicy {
            integration: "opentag".into(),
            allowed_metadata_keys: ["intent", "source_kind"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            max_metadata_entries: 2,
            max_metadata_value_bytes: 64,
        }
    }

    fn event() -> OpenTagInboundEvent {
        OpenTagInboundEvent {
            verified: true,
            provider: "github".into(),
            provider_event_id: "evt_1".into(),
            external_actor_id: "user_1".into(),
            target_agent_instance_id: AgentInstanceId::new("ai_opentag_target"),
            source_thread_uri: "github://org/repo/issues/1#comment-2".into(),
            metadata: [("intent".into(), "review".into())].into_iter().collect(),
        }
    }

    #[test]
    fn verified_redelivery_has_one_stable_correlation_and_external_actor() {
        let agents = agents(AgentStatus::Active);
        let first = normalize_opentag_event(event(), &policy(), &agents).unwrap();
        let second = normalize_opentag_event(event(), &policy(), &agents).unwrap();
        assert_eq!(first, second);
        assert!(matches!(
            first.source_actor,
            Actor::External { ref integration, ref external_actor_id }
                if integration == "opentag:github" && external_actor_id == "user_1"
        ));
    }

    #[test]
    fn unverified_or_unregistered_source_never_resolves_a_target() {
        let agents = agents(AgentStatus::Active);
        let mut unsigned = event();
        unsigned.verified = false;
        assert_eq!(
            normalize_opentag_event(unsigned, &policy(), &agents),
            Err(OpenTagAdapterError::UnverifiedSource)
        );
        let mut unknown = event();
        unknown.target_agent_instance_id = AgentInstanceId::new("ai_from_opentag");
        assert!(matches!(
            normalize_opentag_event(unknown, &policy(), &agents),
            Err(OpenTagAdapterError::TargetAgent(
                AgentRepositoryError::NotFound { .. }
            ))
        ));
    }

    #[test]
    fn paused_target_and_untrusted_metadata_fail_closed() {
        let paused = agents(AgentStatus::Paused);
        assert!(matches!(
            normalize_opentag_event(event(), &policy(), &paused),
            Err(OpenTagAdapterError::TargetAgent(
                AgentRepositoryError::NotDispatchable { .. }
            ))
        ));
        let active = agents(AgentStatus::Active);
        let mut forbidden = event();
        forbidden
            .metadata
            .insert("workspace_hint".into(), "secret-path".into());
        assert!(matches!(
            normalize_opentag_event(forbidden, &policy(), &active),
            Err(OpenTagAdapterError::MetadataNotAllowed { .. })
        ));
    }
}
