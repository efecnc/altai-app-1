//! Control-plane protocol serving from the desktop host.
//!
//! The Tauri app — including its Studio window mode, which shares this
//! command surface — is an `EmbeddedHost` deployment of the same versioned
//! control protocol the CLI and the deployed backend serve: one
//! `ProtocolDispatcher` per workspace `work.db`. Commands own framing only,
//! no domain logic, so the same command causes the same transition on every
//! host (package 053 gate).

use std::path::{Path, PathBuf};

use altai_control_plane::{
    capabilities_from_wiring, ProtocolDispatcher, SqliteActivityEventRepository,
    SqliteControlEventRepository, SqliteWorkItemRepository,
};
use altai_control_protocol::{
    CapabilityNegotiationRequest, DeploymentMode, ProtocolCommand, ProtocolRequest,
};
use altai_core::resolve_workspace_from;
use serde_json::Value;
use tauri::State;

use super::workspace::WorkspaceRegistry;

/// One dispatcher per workspace `work.db`. Built only after the registry's
/// migration gate has accepted the database (CP-08-43's single lifecycle
/// entry point), so a newer-schema database never reaches a host.
pub struct ControlProtocolHost {
    dispatcher: std::sync::Arc<ProtocolDispatcher>,
}

impl ControlProtocolHost {
    pub fn open(database: &Path) -> Result<Self, String> {
        let activity = std::sync::Arc::new(
            SqliteActivityEventRepository::open(database).map_err(|error| error.to_string())?,
        );
        let control_events = std::sync::Arc::new(
            SqliteControlEventRepository::open(database).map_err(|error| error.to_string())?,
        );
        let work_items = std::sync::Arc::new(
            SqliteWorkItemRepository::open(database).map_err(|error| error.to_string())?,
        );
        // Capabilities stay honest: only the protocol-facing repositories
        // this host wires advertise themselves. Wiring the canonical
        // work-item store is what flips `work_graph` here; unserved domains
        // answer typed dispatcher errors, never guesses.
        let capabilities = capabilities_from_wiring(
            false, false, false, true, false, false, false, true, true,
        );
        Ok(Self {
            dispatcher: std::sync::Arc::new(
                ProtocolDispatcher::new(DeploymentMode::EmbeddedHost, capabilities)
                    .with_work_item_repository(work_items)
                    .with_activity_repository(activity)
                    .with_control_event_repository(control_events),
            ),
        })
    }

    /// Serve `control/negotiate`: params are a `CapabilityNegotiationRequest`.
    pub fn negotiate(&self, params: Option<Value>) -> Result<Value, String> {
        let request: CapabilityNegotiationRequest =
            serde_json::from_value(params.unwrap_or_else(|| Value::Object(serde_json::Map::new())))
                .map_err(|error| format!("invalid negotiate request: {error}"))?;
        serde_json::to_value(self.dispatcher.negotiate(&request))
            .map_err(|error| format!("could not encode negotiation response: {error}"))
    }

    /// Serve `control/execute`: params are a `ProtocolRequest<ProtocolCommand>`.
    /// Domain failures are values inside the protocol envelope, so the only
    /// transport-level error left is a payload that does not deserialize.
    pub fn execute(&self, params: Option<Value>) -> Result<Value, String> {
        let request: ProtocolRequest<ProtocolCommand> = serde_json::from_value(
            params.ok_or("control/execute requires a protocol request payload")?,
        )
        .map_err(|error| format!("invalid protocol request: {error}"))?;
        serde_json::to_value(self.dispatcher.execute(&request))
            .map_err(|error| format!("could not encode protocol response: {error}"))
    }
}

#[tauri::command]
pub fn control_protocol_negotiate(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    params: Option<Value>,
) -> Result<Value, String> {
    let host = registry.control_protocol_host(&control_database(&workspace_path)?)?;
    host.negotiate(params)
}

#[tauri::command]
pub fn control_protocol_execute(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
    params: Option<Value>,
) -> Result<Value, String> {
    let host = registry.control_protocol_host(&control_database(&workspace_path)?)?;
    host.execute(params)
}

fn control_database(workspace_path: &str) -> Result<PathBuf, String> {
    let paths = resolve_workspace_from(Some(Path::new(workspace_path)), Path::new(workspace_path))
        .map_err(|error| error.to_string())?;
    Ok(paths.work_db())
}

#[cfg(test)]
mod tests {
    use super::*;
    use altai_control_protocol::actor::UserId;
    use altai_control_protocol::{
        ActivityQueryRequest, Actor, OrganizationId, PageRequest, ProtocolVersion,
    };

    fn workspace() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let paths = resolve_workspace_from(Some(&root), &root).unwrap();
        (directory, paths.work_db())
    }

    fn activity_request(id: &str) -> ProtocolRequest<ProtocolCommand> {
        let organization = OrganizationId::new("org-conformance");
        ProtocolRequest {
            id: id.into(),
            version: ProtocolVersion::CURRENT,
            actor: Actor::User {
                id: UserId::new(organization.clone(), "desktop"),
                display_name: "Desktop Host".into(),
            },
            payload: ProtocolCommand::QueryActivity(ActivityQueryRequest {
                organization_id: organization,
                page: PageRequest::default(),
                kind: None,
                work_item_id: None,
            }),
        }
    }

    #[test]
    fn the_registry_serves_the_protocol_after_migration() {
        let (_guard, database) = workspace();
        let registry = WorkspaceRegistry::default();
        let host = registry.control_protocol_host(&database).unwrap();
        let response = host
            .execute(Some(
                serde_json::to_value(activity_request("req-1")).unwrap(),
            ))
            .expect("query activity should execute");
        // An empty activity stream pages out cleanly — the desktop served
        // the command through the dispatcher every host shares.
        assert_eq!(response["result"]["Ok"]["type"], "activity");
    }

    #[test]
    fn one_host_is_reused_per_work_db() {
        let (_guard, database) = workspace();
        let registry = WorkspaceRegistry::default();
        let first = registry.control_protocol_host(&database).unwrap();
        let second = registry.control_protocol_host(&database).unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn a_newer_work_db_refuses_the_host_and_is_not_cached() {
        let (_guard, database) = workspace();
        if let Some(parent) = database.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE control_plane_local_migrations (
                   version INTEGER PRIMARY KEY,
                   applied_at_unix_seconds INTEGER NOT NULL
                 );
                 INSERT INTO control_plane_local_migrations VALUES (99, 0);",
            )
            .unwrap();
        drop(connection);

        let registry = WorkspaceRegistry::default();
        let error = registry
            .control_protocol_host(&database)
            .map(|_| ())
            .expect_err("newer-schema work.db must refuse the desktop host");
        assert!(
            error.contains("newer than this build"),
            "unexpected error: {error}"
        );
        // Failures stay uncached so an updated database recovers without an
        // app restart.
        assert!(registry.control_protocol_host(&database).is_err());
    }

    /// Package 053's gate: the same request must cause the same response on
    /// every host. The desktop adapter adds no logic of its own, so parity
    /// with a reference dispatcher — built the way the transports build one,
    /// on the same database — is the conformance evidence for this host.
    #[test]
    fn same_request_matches_the_shared_dispatcher_on_the_same_database() {
        let (_guard, database) = workspace();
        let registry = WorkspaceRegistry::default();
        let host = registry.control_protocol_host(&database).unwrap();

        let reference = {
            let activity =
                std::sync::Arc::new(SqliteActivityEventRepository::open(&database).unwrap());
            let control_events =
                std::sync::Arc::new(SqliteControlEventRepository::open(&database).unwrap());
            let work_items =
                std::sync::Arc::new(SqliteWorkItemRepository::open(&database).unwrap());
            let capabilities = capabilities_from_wiring(
                false, false, false, true, false, false, false, true, true,
            );
            ProtocolDispatcher::new(DeploymentMode::EmbeddedHost, capabilities)
                .with_work_item_repository(work_items)
                .with_activity_repository(activity)
                .with_control_event_repository(control_events)
        };

        let request = serde_json::to_value(activity_request("req-parity")).unwrap();
        let via_desktop = host.execute(Some(request.clone())).unwrap();
        let via_reference =
            serde_json::to_value(reference.execute(&serde_json::from_value(request).unwrap()))
                .unwrap();
        assert_eq!(via_desktop, via_reference);

        let negotiation = serde_json::to_value(CapabilityNegotiationRequest {
            client_name: "desktop-conformance".into(),
            client_version: ProtocolVersion::CURRENT,
            required_capabilities: vec![],
        })
        .unwrap();
        let negotiated_desktop = host.negotiate(Some(negotiation.clone())).unwrap();
        let negotiated_reference = serde_json::to_value(
            reference.negotiate(&serde_json::from_value(negotiation).unwrap()),
        )
        .unwrap();
        assert_eq!(negotiated_desktop, negotiated_reference);
    }

    #[test]
    fn unparsable_execute_params_surface_a_transport_error() {
        let (_guard, database) = workspace();
        let registry = WorkspaceRegistry::default();
        let host = registry.control_protocol_host(&database).unwrap();
        let error = host
            .execute(Some(Value::String("not-a-request".into())))
            .unwrap_err();
        assert!(error.contains("invalid protocol request"), "{error}");
    }
}
