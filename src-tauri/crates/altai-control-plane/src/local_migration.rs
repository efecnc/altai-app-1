//! One lifecycle entry point for the local, single-file Work OS database.
//!
//! Individual repositories retain ownership of their tables, but a host must
//! not rely on an accidental repository-open order to get a usable `work.db`.
//! This runner opens every local schema owner before recording a single
//! control-plane lifecycle checkpoint.  It never creates another database.

use crate::{
    SqliteAgentRepository, SqliteApprovalRepository, SqliteAttemptRepository,
    SqliteBudgetRepository, SqliteEvidenceRepository, SqliteExecutionSnapshotRepository,
    SqliteExternalAccountRepository, SqliteExternalObjectRepository,
    SqliteNotificationProposalRepository, SqliteRecoveryRepository, SqliteRegistrationRepository,
    SqliteRepositoryScopeRepository, SqliteRoutineRepository, SqliteRunBindingRepository,
    SqliteScheduleBackendRepository, SqliteScopeRepository, SqliteUsageRepository,
    SqliteWakeRepository, SqliteWorkGraphRepository, SqliteWorkItemRepository,
};
use altai_core::WorkStore;
use rusqlite::{params, Connection, OpenFlags, TransactionBehavior};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// The semantic lifecycle version of the complete local `work.db` topology.
pub const LOCAL_WORK_DB_SCHEMA_VERSION: i64 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalMigrationReport {
    pub schema_version: i64,
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalMigrationError {
    UnsupportedSchema { current: i64, supported: i64 },
    Database { reason: String },
}

impl std::fmt::Display for LocalMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchema { current, supported } => write!(
                f,
                "local work.db schema version {current} is newer than supported version {supported}"
            ),
            Self::Database { reason } => write!(f, "local work.db migration failed: {reason}"),
        }
    }
}

impl std::error::Error for LocalMigrationError {}

/// Migrates and verifies every table family owned by the local control plane.
pub struct LocalMigrationRunner;

impl LocalMigrationRunner {
    pub fn migrate(database: &Path) -> Result<LocalMigrationReport, LocalMigrationError> {
        if let Some(parent) = database.parent() {
            std::fs::create_dir_all(parent).map_err(|error| LocalMigrationError::Database {
                reason: error.to_string(),
            })?;
        }

        // A database written by a newer host must fail closed before even the
        // core store is allowed to migrate it. Opening without CREATE keeps
        // this preflight side-effect free for existing databases.
        if database.exists() {
            let preflight =
                Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_WRITE)
                    .map_err(database_error)?;
            let ledger_exists: bool = preflight
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'control_plane_local_migrations')",
                    [],
                    |row| row.get(0),
                )
                .map_err(database_error)?;
            if ledger_exists {
                let current = preflight
                    .query_row(
                        "SELECT MAX(version) FROM control_plane_local_migrations",
                        [],
                        |row| row.get::<_, Option<i64>>(0),
                    )
                    .map_err(database_error)?
                    .unwrap_or(0);
                if current > LOCAL_WORK_DB_SCHEMA_VERSION {
                    return Err(LocalMigrationError::UnsupportedSchema {
                        current,
                        supported: LOCAL_WORK_DB_SCHEMA_VERSION,
                    });
                }
            }
        }

        // `WorkStore::open` owns secure first creation (0600 on Unix) and the
        // core Work migrations. Do this before any raw SQLite connection can
        // create the file with process-default permissions.
        WorkStore::open(database).map_err(|error| LocalMigrationError::Database {
            reason: error.to_string(),
        })?;

        let connection = Connection::open(database).map_err(database_error)?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;
                 CREATE TABLE IF NOT EXISTS control_plane_local_migrations (
                   version INTEGER PRIMARY KEY,
                   applied_at_unix_seconds INTEGER NOT NULL
                 );",
            )
            .map_err(database_error)?;
        let current = connection
            .query_row(
                "SELECT MAX(version) FROM control_plane_local_migrations",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(database_error)?
            .unwrap_or(0);
        if current > LOCAL_WORK_DB_SCHEMA_VERSION {
            return Err(LocalMigrationError::UnsupportedSchema {
                current,
                supported: LOCAL_WORK_DB_SCHEMA_VERSION,
            });
        }
        drop(connection);

        // Each adapter owns its DDL and may safely be opened repeatedly.
        // Keep this list explicit: adding a local schema owner must update the
        // semantic lifecycle rather than quietly depending on a call site.
        SqliteScopeRepository::open(database).map_err(repository_error)?;
        SqliteAgentRepository::open(database).map_err(repository_error)?;
        SqliteWorkGraphRepository::open(database).map_err(repository_error)?;
        SqliteWorkItemRepository::open(database).map_err(repository_error)?;
        SqliteWakeRepository::open(database).map_err(repository_error)?;
        SqliteRunBindingRepository::open(database).map_err(repository_error)?;
        SqliteAttemptRepository::open(database).map_err(repository_error)?;
        SqliteExecutionSnapshotRepository::open(database).map_err(repository_error)?;
        SqliteRoutineRepository::open(database).map_err(repository_error)?;
        SqliteScheduleBackendRepository::open(database).map_err(repository_error)?;
        SqliteApprovalRepository::open(database).map_err(repository_error)?;
        SqliteEvidenceRepository::open(database).map_err(repository_error)?;
        SqliteExternalAccountRepository::open(database).map_err(repository_error)?;
        SqliteExternalObjectRepository::open(database).map_err(repository_error)?;
        SqliteUsageRepository::open(database).map_err(repository_error)?;
        SqliteBudgetRepository::open(database).map_err(repository_error)?;
        SqliteRecoveryRepository::open(database).map_err(repository_error)?;
        SqliteRepositoryScopeRepository::open(database).map_err(repository_error)?;
        SqliteRegistrationRepository::open(database).map_err(repository_error)?;
        SqliteNotificationProposalRepository::open(database).map_err(repository_error)?;

        let mut connection = Connection::open(database).map_err(database_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(database_error)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        let applied = if current < LOCAL_WORK_DB_SCHEMA_VERSION {
            transaction
                .execute(
                    "INSERT INTO control_plane_local_migrations (version, applied_at_unix_seconds) VALUES (?1, ?2)",
                    params![LOCAL_WORK_DB_SCHEMA_VERSION, now_unix_seconds() as i64],
                )
                .map_err(database_error)?;
            true
        } else {
            false
        };
        transaction.commit().map_err(database_error)?;

        Ok(LocalMigrationReport {
            schema_version: LOCAL_WORK_DB_SCHEMA_VERSION,
            applied,
        })
    }
}

fn database_error(error: rusqlite::Error) -> LocalMigrationError {
    LocalMigrationError::Database {
        reason: error.to_string(),
    }
}

fn repository_error(reason: String) -> LocalMigrationError {
    LocalMigrationError::Database { reason }
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initializes_every_local_table_family_once() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("nested/work.db");

        let first = LocalMigrationRunner::migrate(&database).unwrap();
        assert_eq!(first.schema_version, LOCAL_WORK_DB_SCHEMA_VERSION);
        assert!(first.applied);
        let second = LocalMigrationRunner::migrate(&database).unwrap();
        assert!(!second.applied);

        let connection = Connection::open(database).unwrap();
        for table in [
            "work_events",
            "control_plane_organizations",
            "control_plane_work_items",
            "control_plane_attempts",
            "control_plane_approvals",
            "control_plane_usage_records",
            "control_plane_recovery_records",
            "control_plane_registered_hosts",
            "control_plane_external_objects",
            "control_plane_notification_proposals",
            "control_plane_worker_delivery_claims",
        ] {
            let exists: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "missing {table}");
        }
    }

    #[test]
    fn rejects_a_newer_semantic_schema_before_opening_adapters() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("work.db");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE control_plane_local_migrations (version INTEGER PRIMARY KEY, applied_at_unix_seconds INTEGER NOT NULL);
                 INSERT INTO control_plane_local_migrations VALUES (99, 0);",
            )
            .unwrap();

        assert_eq!(
            LocalMigrationRunner::migrate(&database).unwrap_err(),
            LocalMigrationError::UnsupportedSchema {
                current: 99,
                supported: LOCAL_WORK_DB_SCHEMA_VERSION,
            }
        );
    }
}
