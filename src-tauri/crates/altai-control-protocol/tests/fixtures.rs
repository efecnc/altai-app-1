//! Golden fixture round-trip tests.
//!
//! Each fixture in `shared/control-protocol/v1/fixtures/` must parse on both
//! the Rust and TypeScript sides and produce byte-identical JSON on
//! re-serialization.

use altai_control_protocol::*;
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // altai-control-protocol is at src-tauri/crates/altai-control-protocol
    // shared/ is at workspace root
    PathBuf::from(manifest_dir)
        .join("../../..")
        .join("shared/control-protocol/v1/fixtures")
}

fn read_fixture(name: &str) -> serde_json::Value {
    let path = fixtures_dir().join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture {name}: {e} at {path:?}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("invalid JSON in {name}: {e}"))
}

fn assert_round_trip<T>(name: &str, value: &serde_json::Value, parsed: T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let reserialized = serde_json::to_string(&parsed).unwrap();
    let original_compact = serde_json::to_string(value).unwrap();
    assert_eq!(
        reserialized, original_compact,
        "{name}: round-trip JSON mismatch"
    );
    // Re-parse to ensure stability.
    let reparsed: T = serde_json::from_str(&reserialized).unwrap();
    assert_eq!(parsed, reparsed, "{name}: reparsed value differs");
}

#[test]
fn work_item_id_fixture_round_trips() {
    let value = read_fixture("work-item-id.json");
    let parsed = WorkItemId::parse(&value).unwrap();
    assert_round_trip("work-item-id.json", &value, parsed);
}

#[test]
fn organization_id_fixture_round_trips() {
    let value = read_fixture("organization-id.json");
    let parsed = OrganizationId::parse(&value).unwrap();
    assert_round_trip("organization-id.json", &value, parsed);
}

#[test]
fn revision_fixture_round_trips() {
    let value = read_fixture("revision.json");
    let parsed: Revision = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("revision.json", &value, parsed);
}

#[test]
fn actor_fixture_round_trips() {
    let value = read_fixture("actor.json");
    let parsed: Actor = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("actor.json", &value, parsed);
}

#[test]
fn activity_event_fixture_round_trips() {
    let value = read_fixture("activity-event.json");
    let parsed: ActivityEvent = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("activity-event.json", &value, parsed);
}

#[test]
fn control_plane_health_fixture_round_trips() {
    let value = read_fixture("control-plane-health.json");
    let parsed: ControlPlaneHealth = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("control-plane-health.json", &value, parsed);
}

#[test]
fn host_registration_fixture_round_trips() {
    let value = read_fixture("host-registration.json");
    let parsed: HostRegistration = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("host-registration.json", &value, parsed);
}

#[test]
fn work_item_fixture_round_trips() {
    let value = read_fixture("work-item.json");
    let parsed: WorkItem = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("work-item.json", &value, parsed);
}

#[test]
fn project_workspace_fixture_round_trips() {
    let value = read_fixture("project-workspace.json");
    let parsed: ProjectWorkspace = serde_json::from_value(value.clone()).unwrap();
    assert_round_trip("project-workspace.json", &value, parsed);
}

#[test]
fn work_create_command_fixture_round_trips() {
    let value = read_fixture("work-create-command.json");
    let parsed: ProtocolCommand = serde_json::from_value(value.clone()).unwrap();
    assert!(matches!(
        parsed,
        ProtocolCommand::CreateWorkItem(CreateWorkItemCommand { .. })
    ));
    assert_round_trip("work-create-command.json", &value, parsed);
}

#[test]
fn work_transition_command_fixture_round_trips() {
    let value = read_fixture("work-transition-command.json");
    let parsed: ProtocolCommand = serde_json::from_value(value.clone()).unwrap();
    assert!(matches!(
        parsed,
        ProtocolCommand::TransitionWorkItem(TransitionWorkItemCommand { .. })
    ));
    assert_round_trip("work-transition-command.json", &value, parsed);
}

#[test]
fn work_created_outcome_fixture_round_trips() {
    let value = read_fixture("work-created-outcome.json");
    let parsed: ProtocolOutcome = serde_json::from_value(value.clone()).unwrap();
    assert!(matches!(
        parsed,
        ProtocolOutcome::WorkItemCreated(WorkItem { .. })
    ));
    assert_round_trip("work-created-outcome.json", &value, parsed);
}

#[test]
fn work_transitioned_outcome_fixture_round_trips() {
    let value = read_fixture("work-transitioned-outcome.json");
    let parsed: ProtocolOutcome = serde_json::from_value(value.clone()).unwrap();
    assert!(matches!(
        parsed,
        ProtocolOutcome::WorkItemTransitioned(WorkItem { .. })
    ));
    assert_round_trip("work-transitioned-outcome.json", &value, parsed);
}
