//! CP-08 cutover slice-A feature-flag ledger. Writer ownership must be a
//! recorded fact inside `work.db`, not a build property, so each cutover
//! transfer reads its flag from this table instead of from compile-time
//! configuration. The ledger seeds nothing: values are written by callers,
//! and an absent row reads as "not yet decided" (`None`), never as a default.
//! [`CONTROL_PLANE_ENABLED_FLAG`] is the first intended key.

use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

/// Whether the canonical control plane owns this workspace's transferred
/// domains. Read by every slice-C transfer decision; never defaulted here.
pub const CONTROL_PLANE_ENABLED_FLAG: &str = "control_plane_enabled";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeatureFlagError {
    InvalidFlagKey(&'static str),
    Internal { reason: String },
}

impl std::fmt::Display for FeatureFlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFlagKey(reason) => write!(f, "invalid flag key: {reason}"),
            Self::Internal { reason } => write!(f, "feature flag error: {reason}"),
        }
    }
}
impl std::error::Error for FeatureFlagError {}

pub trait FeatureFlagRepository: Send + Sync {
    /// The stored value for `flag_key`, or `None` when no caller has decided
    /// it yet. An absent flag is never interpreted as any default here.
    fn get(&self, flag_key: &str) -> Result<Option<String>, FeatureFlagError>;
    /// Record or overwrite the value for `flag_key`. Re-setting an existing
    /// key with the same value is idempotent.
    fn set(&self, flag_key: &str, flag_value: &str) -> Result<(), FeatureFlagError>;
    /// Insert-only write: returns `true` when this call recorded the value,
    /// `false` when a value was already present (which it leaves untouched).
    fn set_if_absent(&self, flag_key: &str, flag_value: &str) -> Result<bool, FeatureFlagError>;
}

pub struct SqliteFeatureFlagRepository {
    connection: Mutex<Connection>,
}

impl SqliteFeatureFlagRepository {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection.execute_batch(
            "PRAGMA busy_timeout = 5000; CREATE TABLE IF NOT EXISTS control_plane_feature_flags (flag_key TEXT PRIMARY KEY, flag_value TEXT NOT NULL);",
        )
        .map_err(|e| e.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, FeatureFlagError> {
        self.connection
            .lock()
            .map_err(|_| FeatureFlagError::Internal {
                reason: "sqlite feature flag lock poisoned".into(),
            })
    }

    fn validate(flag_key: &str, flag_value: &str) -> Result<(), FeatureFlagError> {
        if flag_key.trim().is_empty() {
            return Err(FeatureFlagError::InvalidFlagKey("flag key is required"));
        }
        if flag_value.is_empty() {
            return Err(FeatureFlagError::InvalidFlagKey("flag value is required"));
        }
        Ok(())
    }
}

impl FeatureFlagRepository for SqliteFeatureFlagRepository {
    fn get(&self, flag_key: &str) -> Result<Option<String>, FeatureFlagError> {
        Self::validate(flag_key, "present")?;
        let connection = self.lock()?;
        let value: Option<String> = connection
            .query_row(
                "SELECT flag_value FROM control_plane_feature_flags WHERE flag_key = ?1",
                params![flag_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| FeatureFlagError::Internal {
                reason: e.to_string(),
            })?;
        Ok(value)
    }

    fn set(&self, flag_key: &str, flag_value: &str) -> Result<(), FeatureFlagError> {
        Self::validate(flag_key, flag_value)?;
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO control_plane_feature_flags (flag_key, flag_value) VALUES (?1, ?2)
                 ON CONFLICT(flag_key) DO UPDATE SET flag_value = excluded.flag_value",
                params![flag_key, flag_value],
            )
            .map_err(|e| FeatureFlagError::Internal {
                reason: e.to_string(),
            })?;
        Ok(())
    }

    fn set_if_absent(&self, flag_key: &str, flag_value: &str) -> Result<bool, FeatureFlagError> {
        Self::validate(flag_key, flag_value)?;
        let connection = self.lock()?;
        let inserted = connection
            .execute(
                "INSERT OR IGNORE INTO control_plane_feature_flags (flag_key, flag_value) VALUES (?1, ?2)",
                params![flag_key, flag_value],
            )
            .map_err(|e| FeatureFlagError::Internal { reason: e.to_string() })?;
        Ok(inserted == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_flag_reads_as_none_not_a_default() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteFeatureFlagRepository::open(&dir.path().join("work.db")).unwrap();
        assert_eq!(
            repo.get(CONTROL_PLANE_ENABLED_FLAG).unwrap(),
            None,
            "an undecided flag must not read as any default value"
        );
    }

    #[test]
    fn set_then_get_round_trips_the_recorded_fact() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteFeatureFlagRepository::open(&dir.path().join("work.db")).unwrap();
        repo.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        assert_eq!(
            repo.get(CONTROL_PLANE_ENABLED_FLAG).unwrap(),
            Some("true".into())
        );
    }

    #[test]
    fn resetting_an_existing_flag_is_idempotent_and_stores_one_row() {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("work.db");
        let repo = SqliteFeatureFlagRepository::open(&database).unwrap();
        repo.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        repo.set(CONTROL_PLANE_ENABLED_FLAG, "true").unwrap();
        assert_eq!(
            repo.get(CONTROL_PLANE_ENABLED_FLAG).unwrap(),
            Some("true".into())
        );

        // Overwriting to a new value keeps exactly one row per key.
        repo.set(CONTROL_PLANE_ENABLED_FLAG, "false").unwrap();
        assert_eq!(
            repo.get(CONTROL_PLANE_ENABLED_FLAG).unwrap(),
            Some("false".into())
        );
        let connection = Connection::open(database).unwrap();
        let rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM control_plane_feature_flags WHERE flag_key = ?1",
                params![CONTROL_PLANE_ENABLED_FLAG],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn set_if_absent_writes_once_and_preserves_the_first_decision() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteFeatureFlagRepository::open(&dir.path().join("work.db")).unwrap();
        assert!(repo
            .set_if_absent(CONTROL_PLANE_ENABLED_FLAG, "true")
            .unwrap());
        assert!(!repo
            .set_if_absent(CONTROL_PLANE_ENABLED_FLAG, "false")
            .unwrap());
        assert_eq!(
            repo.get(CONTROL_PLANE_ENABLED_FLAG).unwrap(),
            Some("true".into()),
            "a lost race must never overwrite the winner's value"
        );
    }

    #[test]
    fn empty_keys_and_values_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqliteFeatureFlagRepository::open(&dir.path().join("work.db")).unwrap();
        assert!(matches!(
            repo.set("", "true"),
            Err(FeatureFlagError::InvalidFlagKey(_))
        ));
        assert!(matches!(
            repo.set(CONTROL_PLANE_ENABLED_FLAG, ""),
            Err(FeatureFlagError::InvalidFlagKey(_))
        ));
    }
}
