//! Control-plane protocol serving over the CLI's framed stdio RPC.
//!
//! The CLI is an `EmbeddedHost` deployment of the same versioned control
//! protocol the deployed backend serves: one `ProtocolDispatcher`, one
//! `work.db`, and therefore the same transition for the same command on
//! every host (package 053 gate). This module owns no domain logic — it
//! frames requests, delegates to the dispatcher, and frames responses.

use std::sync::Arc;

use altai_control_plane::{
    capabilities_from_wiring, LocalMigrationRunner, ProtocolDispatcher,
    SqliteActivityEventRepository, SqliteControlEventRepository, SqliteWorkItemRepository,
};
use altai_control_protocol::{
    CapabilityNegotiationRequest, DeploymentMode, ProtocolCommand, ProtocolRequest,
};
use altai_core::WorkspacePaths;
use serde_json::Value;

pub struct ControlProtocolState {
    dispatcher: Arc<ProtocolDispatcher>,
}

impl ControlProtocolState {
    /// Bring `work.db` to the current schema, then serve the protocol from
    /// that database. A database written by a newer host refuses startup
    /// loudly instead of serving a schema this build cannot read.
    pub fn open(workspace: &WorkspacePaths) -> Result<Self, String> {
        let database = workspace.work_db();
        LocalMigrationRunner::migrate(&database).map_err(|error| error.to_string())?;
        let activity = Arc::new(SqliteActivityEventRepository::open(&database)?);
        let control_events = Arc::new(SqliteControlEventRepository::open(&database)?);
        let work_items = Arc::new(SqliteWorkItemRepository::open(&database)?);
        // Capabilities stay honest: only the protocol-facing repositories
        // this host actually wires advertise themselves. Wiring the
        // canonical work-item store is what flips `work_graph` here; domains
        // without serving answer typed errors from the dispatcher, never
        // guesses.
        let capabilities = capabilities_from_wiring(
            false, false, false, true, false, false, false, true, true,
        );
        let dispatcher = Arc::new(
            ProtocolDispatcher::new(DeploymentMode::EmbeddedHost, capabilities)
                .with_work_item_repository(work_items)
                .with_activity_repository(activity)
                .with_control_event_repository(control_events),
        );
        Ok(Self { dispatcher })
    }

    /// Serve `control/negotiate`: params are a `CapabilityNegotiationRequest`.
    pub fn negotiate(&self, params: Option<Value>) -> Result<Value, String> {
        let request: CapabilityNegotiationRequest = serde_json::from_value(
            params.unwrap_or_else(|| Value::Object(serde_json::Map::new())),
        )
        .map_err(|error| format!("invalid negotiate request: {error}"))?;
        serde_json::to_value(self.dispatcher.negotiate(&request))
            .map_err(|error| format!("could not encode negotiation response: {error}"))
    }

    /// Serve `control/execute`: params are a `ProtocolRequest<ProtocolCommand>`.
    /// Domain failures are values inside the protocol envelope, so the only
    /// transport-level error left is a frame that does not deserialize.
    pub fn execute(&self, params: Option<Value>) -> Result<Value, String> {
        let request: ProtocolRequest<ProtocolCommand> = serde_json::from_value(
            params.ok_or("control/execute requires a protocol request payload")?,
        )
        .map_err(|error| format!("invalid protocol request: {error}"))?;
        serde_json::to_value(self.dispatcher.execute(&request))
            .map_err(|error| format!("could not encode protocol response: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use altai_control_protocol::actor::UserId;
    use altai_control_protocol::{
        ActivityQueryRequest, Actor, OrganizationId, PageRequest, ProtocolVersion,
    };

    fn workspace() -> (tempfile::TempDir, WorkspacePaths) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let paths = altai_core::resolve_workspace_from(Some(&root), &root).unwrap();
        (directory, paths)
    }

    fn activity_request(id: &str) -> ProtocolRequest<ProtocolCommand> {
        let organization = OrganizationId::new("org-conformance");
        ProtocolRequest {
            id: id.into(),
            version: ProtocolVersion::CURRENT,
            actor: Actor::User {
                id: UserId::new(organization.clone(), "cli"),
                display_name: "CLI Host".into(),
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
    fn opening_migrates_the_workspace_work_db() {
        let (_guard, paths) = workspace();
        let state = ControlProtocolState::open(&paths).expect("embedded host should open");
        let response = state
            .execute(Some(serde_json::to_value(activity_request("req-1")).unwrap()))
            .expect("query activity should execute");
        // An empty activity stream pages out cleanly — the CLI host served
        // the command through the same dispatcher every host shares.
        assert_eq!(response["result"]["Ok"]["type"], "activity");
    }

    #[test]
    fn a_newer_work_db_refuses_startup() {
        let (_guard, paths) = workspace();
        let database = paths.work_db();
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

        let error = ControlProtocolState::open(&paths)
            .map(|_| ())
            .expect_err("newer-schema work.db must refuse embedded host startup");
        assert!(
            error.contains("newer than supported version"),
            "unexpected error: {error}"
        );
    }

    /// Package 053's gate: the same request must cause the same response on
    /// every host. The CLI adapter adds no logic of its own, so parity with
    /// the dispatcher it wraps — built the way the transport builds one — is
    /// the conformance evidence for this host.
    #[test]
    fn same_request_matches_the_shared_dispatcher_on_the_same_database() {
        let (_guard, paths) = workspace();
        let state = ControlProtocolState::open(&paths).unwrap();
        let database = paths.work_db();

        let reference = {
            let activity = Arc::new(SqliteActivityEventRepository::open(&database).unwrap());
            let control_events =
                Arc::new(SqliteControlEventRepository::open(&database).unwrap());
            let work_items = Arc::new(SqliteWorkItemRepository::open(&database).unwrap());
            let capabilities = capabilities_from_wiring(
                false, false, false, true, false, false, false, true, true,
            );
            ProtocolDispatcher::new(DeploymentMode::EmbeddedHost, capabilities)
                .with_work_item_repository(work_items)
                .with_activity_repository(activity)
                .with_control_event_repository(control_events)
        };

        let request = activity_request("req-parity");
        let via_cli = serde_json::to_value(state.dispatcher.execute(&request)).unwrap();
        let via_reference = serde_json::to_value(reference.execute(&request)).unwrap();
        assert_eq!(via_cli, via_reference);

        let negotiation = CapabilityNegotiationRequest {
            client_name: "cli-conformance".into(),
            client_version: ProtocolVersion::CURRENT,
            required_capabilities: vec![],
        };
        assert_eq!(
            serde_json::to_value(state.dispatcher.negotiate(&negotiation)).unwrap(),
            serde_json::to_value(reference.negotiate(&negotiation)).unwrap()
        );
    }

    #[test]
    fn unparsable_execute_params_surface_a_transport_error() {
        let (_guard, paths) = workspace();
        let state = ControlProtocolState::open(&paths).unwrap();
        let error = state
            .execute(Some(Value::String("not-a-request".into())))
            .unwrap_err();
        assert!(error.contains("invalid protocol request"), "{error}");
    }

    #[test]
    fn protocol_request_shape_stays_framed() {
        // Pins the framed payload shape the CLI host accepts, so it cannot
        // drift from the protocol crate's serialization.
        let value = serde_json::to_value(activity_request("req-shape")).unwrap();
        assert_eq!(value["id"], "req-shape");
        assert_eq!(value["payload"]["type"], "query_activity");
        assert_eq!(value["actor"]["kind"], "user");
    }
}
