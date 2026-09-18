//! Read-only scheduling-authority surface (CP-08-108, package 101 slice C.a).
//!
//! Exposes the resolved scheduling-authority state of the active workspace's
//! feature-flag ledger so the renderer can gate its own dispatch behavior
//! and other surfaces can observe the cutover without guessing. Read-only
//! by design: the ledger is written through the canonical enable flow, not
//! by UI commands.

use std::path::Path;

use altai_control_plane::{resolve_schedule_authority, ScheduleAuthority};
use altai_core::resolve_workspace_from;
use serde::Serialize;
use tauri::State;

use super::workspace::WorkspaceRegistry;

/// The resolved scheduling authority for one workspace. `canonical` is the
/// single bit every consumer needs: true when the canonical scheduler owns
/// scheduling (flag on, rollback switch un-pulled) regardless of which
/// process is the named owner.
#[derive(Debug, Clone, Serialize)]
pub struct SchedulingAuthorityView {
    pub canonical: bool,
    pub enabled: bool,
    pub legacy_cron_compatibility: bool,
    pub owner: Option<String>,
}

impl SchedulingAuthorityView {
    fn from_authority(authority: ScheduleAuthority) -> Self {
        Self {
            canonical: authority.enabled && !authority.legacy_compatibility,
            enabled: authority.enabled,
            legacy_cron_compatibility: authority.legacy_compatibility,
            owner: authority.owner,
        }
    }
}

fn resolve_for_workspace(workspace_path: &str) -> Result<SchedulingAuthorityView, String> {
    let root = Path::new(workspace_path);
    let paths =
        resolve_workspace_from(Some(root), root).map_err(|error| error.to_string())?;
    let ledger = altai_control_plane::SqliteFeatureFlagRepository::open(&paths.work_db())
        .map_err(|error| error.to_string())?;
    resolve_schedule_authority(&ledger)
        .map(SchedulingAuthorityView::from_authority)
        .map_err(|error| error.to_string())
}

/// Serve `control_plane_scheduling_authority`: the resolved authority of the
/// workspace's scheduling cutover flags.
#[tauri::command]
pub fn control_plane_scheduling_authority(
    registry: State<'_, WorkspaceRegistry>,
    workspace_path: String,
) -> Result<SchedulingAuthorityView, String> {
    // The registry's migration gate must accept the database before any
    // flag surface reads it: a newer-schema work.db refuses loudly.
    let paths = resolve_workspace_from(
        Some(Path::new(&workspace_path)),
        Path::new(&workspace_path),
    )
    .map_err(|error| error.to_string())?;
    registry.ensure_work_db_migrated(&paths.work_db())?;
    resolve_for_workspace(&workspace_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_flags_resolve_to_legacy_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let paths = resolve_workspace_from(Some(root), root).unwrap();
        let work_db = paths.work_db();
        if let Some(parent) = work_db.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let view = resolve_for_workspace(&root.to_string_lossy()).unwrap();
        assert!(!view.canonical);
        assert!(!view.enabled);
        assert!(!view.legacy_cron_compatibility);
        assert_eq!(view.owner, None);
    }

    #[test]
    fn canonical_bit_tracks_enabled_and_rollback_flags() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let paths = resolve_workspace_from(Some(root), root).unwrap();
        let work_db = paths.work_db();
        if let Some(parent) = work_db.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        use altai_control_plane::FeatureFlagRepository;
        let ledger = altai_control_plane::SqliteFeatureFlagRepository::open(&work_db).unwrap();
        ledger.set("control_plane_enabled", "true").unwrap();
        let view = resolve_for_workspace(&root.to_string_lossy()).unwrap();
        assert!(view.canonical);
        ledger.set("legacy_cron_compatibility", "true").unwrap();
        let view = resolve_for_workspace(&root.to_string_lossy()).unwrap();
        assert!(!view.canonical);
    }
}
