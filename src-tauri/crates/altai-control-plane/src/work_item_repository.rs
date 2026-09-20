//! CP-06 durable canonical WorkItem repository.

use altai_control_protocol::{OrganizationId, ProjectId, Revision, WorkItem, WorkItemId};
use rusqlite::{params, Connection, OptionalExtension};
use std::{path::Path, sync::Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkItemRepositoryError {
    AlreadyExists { work_item_id: String },
    NotFound { work_item_id: String },
    ProjectMismatch { work_item_id: String },
    /// The addressed project row does not exist, so the foreign key would
    /// reject the insert; surfaced typed instead of as an internal error.
    ProjectNotFound { project_id: String },
    StaleRevision { work_item_id: String },
    Internal { reason: String },
}
impl std::fmt::Display for WorkItemRepositoryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "work item repository error: {self:?}")
    }
}
impl std::error::Error for WorkItemRepositoryError {}

pub trait WorkItemRepository: Send + Sync {
    fn create(&self, item: WorkItem) -> Result<(), WorkItemRepositoryError>;
    fn get(&self, id: &WorkItemId) -> Result<WorkItem, WorkItemRepositoryError>;
    fn get_in_project(
        &self,
        project_id: &ProjectId,
        id: &WorkItemId,
    ) -> Result<WorkItem, WorkItemRepositoryError>;
    fn replace_if_revision(
        &self,
        item: WorkItem,
        expected_revision: Revision,
    ) -> Result<WorkItem, WorkItemRepositoryError>;
    /// The organization the addressed project belongs to. Lets callers bind
    /// a command's claimed organization to the project's actual one
    /// (cross-organization attribution fails closed) without a second store.
    fn project_organization(
        &self,
        project_id: &ProjectId,
    ) -> Result<OrganizationId, WorkItemRepositoryError>;
}

pub struct SqliteWorkItemRepository {
    connection: Mutex<Connection>,
}
impl SqliteWorkItemRepository {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; CREATE TABLE IF NOT EXISTS control_plane_work_items (id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES control_plane_projects(id), payload_json TEXT NOT NULL);").map_err(|error| error.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
    /// Create a work item on a caller-owned connection, so a multi-write
    /// flow (the cron automation transfer's per-automation transaction)
    /// commits its work item and its bookkeeping atomically. Same write as
    /// [`WorkItemRepository::create`], minus the project check the caller
    /// is expected to have performed.
    pub fn create_in(
        connection: &Connection,
        item: WorkItem,
    ) -> Result<(), WorkItemRepositoryError> {
        let payload =
            serde_json::to_string(&item).map_err(|error| WorkItemRepositoryError::Internal {
                reason: error.to_string(),
            })?;
        let inserted = connection.execute("INSERT INTO control_plane_work_items (id, project_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(id) DO NOTHING", params![item.id.value, item.project_id.value, payload]).map_err(Self::db)?;
        if inserted == 1 {
            Ok(())
        } else {
            Err(WorkItemRepositoryError::AlreadyExists {
                work_item_id: item.id.value,
            })
        }
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, WorkItemRepositoryError> {
        self.connection
            .lock()
            .map_err(|_| WorkItemRepositoryError::Internal {
                reason: "work item lock poisoned".into(),
            })
    }
    fn db(error: rusqlite::Error) -> WorkItemRepositoryError {
        WorkItemRepositoryError::Internal {
            reason: error.to_string(),
        }
    }
    fn decode(payload: String) -> Result<WorkItem, WorkItemRepositoryError> {
        serde_json::from_str(&payload).map_err(|error| WorkItemRepositoryError::Internal {
            reason: error.to_string(),
        })
    }
}
impl WorkItemRepository for SqliteWorkItemRepository {
    fn create(&self, item: WorkItem) -> Result<(), WorkItemRepositoryError> {
        self.project_organization(&item.project_id)?;
        let connection = self.lock()?;
        Self::create_in(&connection, item)
    }
    fn get(&self, id: &WorkItemId) -> Result<WorkItem, WorkItemRepositoryError> {
        let payload: Option<String> = self
            .lock()?
            .query_row(
                "SELECT payload_json FROM control_plane_work_items WHERE id = ?1",
                [&id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        payload
            .map(Self::decode)
            .transpose()?
            .ok_or_else(|| WorkItemRepositoryError::NotFound {
                work_item_id: id.value.clone(),
            })
    }
    fn get_in_project(
        &self,
        project_id: &ProjectId,
        id: &WorkItemId,
    ) -> Result<WorkItem, WorkItemRepositoryError> {
        let item = self.get(id)?;
        if item.project_id == *project_id {
            Ok(item)
        } else {
            Err(WorkItemRepositoryError::ProjectMismatch {
                work_item_id: id.value.clone(),
            })
        }
    }
    fn replace_if_revision(
        &self,
        item: WorkItem,
        expected_revision: Revision,
    ) -> Result<WorkItem, WorkItemRepositoryError> {
        let existing = self.get(&item.id)?;
        if existing.project_id != item.project_id {
            return Err(WorkItemRepositoryError::ProjectMismatch {
                work_item_id: item.id.value,
            });
        }
        if existing.revision != expected_revision {
            return Err(WorkItemRepositoryError::StaleRevision {
                work_item_id: item.id.value,
            });
        }
        let payload = serde_json::to_string(&item).map_err(|error| WorkItemRepositoryError::Internal {
            reason: error.to_string(),
        })?;
        let changed = self.lock()?.execute(
            "UPDATE control_plane_work_items SET payload_json = ?2 WHERE id = ?1 AND payload_json = ?3",
            params![item.id.value, payload, serde_json::to_string(&existing).map_err(|error| WorkItemRepositoryError::Internal { reason: error.to_string() })?],
        ).map_err(Self::db)?;
        if changed == 1 { Ok(item) } else { Err(WorkItemRepositoryError::StaleRevision { work_item_id: item.id.value }) }
    }
    fn project_organization(
        &self,
        project_id: &ProjectId,
    ) -> Result<OrganizationId, WorkItemRepositoryError> {
        let stored: Option<String> = self
            .lock()?
            .query_row(
                "SELECT organization_id FROM control_plane_projects WHERE id = ?1",
                [&project_id.value],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db)?;
        stored
            .map(|value| OrganizationId {
                kind: OrganizationId::TYPE.to_string(),
                value,
            })
            .ok_or_else(|| WorkItemRepositoryError::ProjectNotFound {
                project_id: project_id.value.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ScopeRepository, SqliteScopeRepository};
    use altai_control_protocol::{
        ExecutionPhase, Organization, OrganizationId, Project, ProjectStatus, Revision,
        WorkItemKind, WorkStatus,
    };

    fn item(project_id: ProjectId) -> WorkItem {
        WorkItem {
            id: WorkItemId::new("work"),
            project_id,
            goal_id: None,
            parent_work_item_id: None,
            kind: WorkItemKind::Task,
            title: "Ship".into(),
            description: String::new(),
            status: WorkStatus::Todo,
            execution_phase: ExecutionPhase::Queued,
            revision: Revision::INITIAL,
            created_at: "now".into(),
            updated_at: "now".into(),
        }
    }
    #[test]
    fn durable_work_items_are_project_scoped() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("work.db");
        let scopes = SqliteScopeRepository::open(&database).unwrap();
        let org = Organization {
            id: OrganizationId::new("org"),
            name: "Org".into(),
            revision: Revision::INITIAL,
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        scopes.create_organization(org.clone()).unwrap();
        let project = Project {
            id: ProjectId::new("project"),
            organization_id: org.id,
            goal_ids: vec![],
            name: "Project".into(),
            description: String::new(),
            status: ProjectStatus::Active,
            revision: Revision::INITIAL,
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        scopes.create_project(project.clone()).unwrap();
        SqliteWorkItemRepository::open(&database)
            .unwrap()
            .create(item(project.id.clone()))
            .unwrap();
        let repository = SqliteWorkItemRepository::open(&database).unwrap();
        assert_eq!(
            repository
                .get_in_project(&project.id, &WorkItemId::new("work"))
                .unwrap()
                .title,
            "Ship"
        );
        assert!(matches!(
            repository.get_in_project(&ProjectId::new("other"), &WorkItemId::new("work")),
            Err(WorkItemRepositoryError::ProjectMismatch { .. })
        ));
    }
}
