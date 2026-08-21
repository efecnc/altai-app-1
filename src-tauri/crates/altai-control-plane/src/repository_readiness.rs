//! Read-only, bounded repository/context readiness manifest (CP-08-93).

use altai_control_protocol::{AttemptId, Evidence, OrganizationId, WorkItemId};
use serde::{Deserialize, Serialize};

use crate::ScopePermit;

pub const REPOSITORY_READINESS_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryReadinessManifest {
    pub schema_version: u16,
    pub organization_id: OrganizationId,
    pub work_item_id: WorkItemId,
    pub attempt_id: AttemptId,
    pub repository_url: String,
    pub requested_context_bytes: usize,
    pub admitted_context_bytes: usize,
    pub context_truncated: bool,
    pub evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryReadinessError {
    RepositoryDenied,
    OrganizationMismatch,
    InvalidByteMeasurement,
    EvidenceScopeMismatch { evidence_id: String },
    UnsafeEvidenceReference { evidence_id: String },
    DuplicateEvidence { evidence_id: String },
}

impl std::fmt::Display for RepositoryReadinessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "repository readiness error: {self:?}")
    }
}
impl std::error::Error for RepositoryReadinessError {}

impl RepositoryReadinessManifest {
    pub fn build(
        permit: ScopePermit,
        work_item_id: WorkItemId,
        attempt_id: AttemptId,
        requested_context_bytes: usize,
        admitted_context_bytes: usize,
        context_truncated: bool,
        evidence: &[Evidence],
    ) -> Result<Self, RepositoryReadinessError> {
        let ScopePermit::Permitted {
            organization_id,
            repository_url,
        } = permit
        else {
            return Err(RepositoryReadinessError::RepositoryDenied);
        };
        if admitted_context_bytes > requested_context_bytes {
            return Err(RepositoryReadinessError::InvalidByteMeasurement);
        }
        let mut evidence_ids = Vec::with_capacity(evidence.len());
        for item in evidence {
            if item.organization_id != organization_id
                || item.work_item_id != work_item_id
                || item.attempt_id != attempt_id
            {
                return Err(RepositoryReadinessError::EvidenceScopeMismatch {
                    evidence_id: item.id.value.clone(),
                });
            }
            let ref_lower = item.reference.to_ascii_lowercase();
            if item.reference.starts_with('/')
                || ref_lower.contains("token=")
                || ref_lower.contains("secret=")
                || ref_lower.contains("password=")
                || ref_lower.contains("api_key=")
            {
                return Err(RepositoryReadinessError::UnsafeEvidenceReference {
                    evidence_id: item.id.value.clone(),
                });
            }
            if evidence_ids.contains(&item.id.value) {
                return Err(RepositoryReadinessError::DuplicateEvidence {
                    evidence_id: item.id.value.clone(),
                });
            }
            evidence_ids.push(item.id.value.clone());
        }
        Ok(Self {
            schema_version: REPOSITORY_READINESS_SCHEMA_VERSION,
            organization_id,
            work_item_id,
            attempt_id,
            repository_url,
            requested_context_bytes,
            admitted_context_bytes,
            context_truncated,
            evidence_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use altai_control_protocol::EvidenceId;

    fn evidence(reference: &str) -> Evidence {
        Evidence {
            id: EvidenceId::new("ev_1"),
            organization_id: OrganizationId::new("org"),
            work_item_id: WorkItemId::new("wi"),
            attempt_id: AttemptId::new("att"),
            kind: "test_result".into(),
            reference: reference.into(),
            created_at_unix_seconds: 1,
        }
    }
    fn permit() -> ScopePermit {
        ScopePermit::Permitted {
            organization_id: OrganizationId::new("org"),
            repository_url: "https://github.com/efecnc/altai-app-1".into(),
        }
    }
    #[test]
    fn manifest_is_read_only_scoped_and_measured() {
        let manifest = RepositoryReadinessManifest::build(
            permit(),
            WorkItemId::new("wi"),
            AttemptId::new("att"),
            100,
            80,
            true,
            &[evidence("artifact://test")],
        )
        .unwrap();
        assert_eq!(manifest.evidence_ids, vec!["ev_1"]);
        assert_eq!(manifest.admitted_context_bytes, 80);
    }
    #[test]
    fn denied_scope_and_unsafe_reference_fail_closed() {
        assert_eq!(
            RepositoryReadinessManifest::build(
                ScopePermit::Denied(crate::DenialReason::WorkspaceNotBound),
                WorkItemId::new("wi"),
                AttemptId::new("att"),
                1,
                1,
                false,
                &[]
            ),
            Err(RepositoryReadinessError::RepositoryDenied)
        );
        assert_eq!(
            RepositoryReadinessManifest::build(
                permit(),
                WorkItemId::new("wi"),
                AttemptId::new("att"),
                1,
                1,
                false,
                &[evidence("/tmp/secret")]
            ),
            Err(RepositoryReadinessError::UnsafeEvidenceReference {
                evidence_id: "ev_1".into()
            })
        );
    }
}
