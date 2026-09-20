use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tauri::Manager;

use super::control_protocol::ControlProtocolHost;

// Short TTL keeps the auth-check TOCTOU window tight while still coalescing the
// burst of canonicalize calls within a single panel refresh (~100ms).
const CANONICAL_TTL: Duration = Duration::from_secs(1);
const CANONICAL_CACHE_CAP: usize = 256;

struct CanonicalEntry {
    canonical: PathBuf,
    inserted_at: Instant,
}

#[derive(Clone, PartialEq, Eq)]
struct WorkspaceRootIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume_serial: u32,
    #[cfg(windows)]
    file_index: u64,
    #[cfg(not(any(unix, windows)))]
    modified: Option<std::time::SystemTime>,
}

#[derive(Clone)]
pub struct OpenedWorkspaceGrant {
    canonical: PathBuf,
    identity: WorkspaceRootIdentity,
}

impl OpenedWorkspaceGrant {
    pub fn path(&self) -> &Path {
        &self.canonical
    }
}

impl WorkspaceRootIdentity {
    #[cfg(not(windows))]
    fn from_metadata(metadata: &std::fs::Metadata) -> Option<Self> {
        if !metadata.is_dir() {
            return None;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            Some(Self {
                modified: metadata.modified().ok(),
            })
        }
    }

    #[cfg(windows)]
    fn from_path(path: &Path) -> Option<Self> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY,
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };

        let mut options = std::fs::OpenOptions::new();
        options
            .access_mode(0)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
        let file = options.open(path).ok()?;
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) } == 0 {
            return None;
        }
        let info = unsafe { info.assume_init() };
        if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
        {
            return None;
        }
        Some(Self {
            volume_serial: info.dwVolumeSerialNumber,
            file_index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        })
    }

    fn from_path_and_metadata(path: &Path, metadata: &std::fs::Metadata) -> Option<Self> {
        #[cfg(windows)]
        {
            let _ = metadata;
            Self::from_path(path)
        }
        #[cfg(not(windows))]
        {
            let _ = path;
            Self::from_metadata(metadata)
        }
    }

    fn matches_path(&self, path: &Path, metadata: &std::fs::Metadata) -> bool {
        Self::from_path_and_metadata(path, metadata).as_ref() == Some(self)
    }
}

/// One canonical schedule driver per workspace `work.db` per app run
/// (CP-08-108, package 101 slice C.a). The thread exists only for a
/// workspace whose WorkStore this app run successfully opened and cached —
/// the store holds the workspace's single-writer lock, so spawning from
/// that success path (and from no other) keeps the driver from ticking
/// against a workspace whose lock lives elsewhere. Each tick re-reads the
/// feature-flag ledger and materializes due routines only while the ledger
/// names the desktop as the schedule's owner — an absent or foreign flag
/// keeps it idle — and requires the in-process WorkStore handle to still be
/// alive: once the registry releases it, the thread stops.
fn ensure_desktop_schedule_driver(
    work_db: &Path,
    store: &std::sync::Arc<altai_core::WorkStore>,
) {
    static DRIVERS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    let drivers = DRIVERS.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = drivers.lock().expect("schedule driver registry poisoned");
    if !guard.insert(work_db.to_path_buf()) {
        return;
    }
    drop(guard);
    let routines = match altai_control_plane::SqliteRoutineRepository::open(work_db) {
        Ok(repository) => repository,
        Err(error) => {
            log::error!(
                "Desktop schedule driver disabled for {}: routine repository unavailable: {error}",
                work_db.display()
            );
            return;
        }
    };
    let wakes = match altai_control_plane::SqliteWakeRepository::open(work_db) {
        Ok(repository) => repository,
        Err(error) => {
            log::error!(
                "Desktop schedule driver disabled for {}: wake repository unavailable: {error}",
                work_db.display()
            );
            return;
        }
    };
    let ledger = match altai_control_plane::SqliteFeatureFlagRepository::open(work_db) {
        Ok(repository) => repository,
        Err(error) => {
            log::error!(
                "Desktop schedule driver disabled for {}: flag ledger unavailable: {error}",
                work_db.display()
            );
            return;
        }
    };
    let materializer = std::sync::Arc::new(altai_control_plane::RoutineMaterializer::new(
        std::sync::Arc::new(routines),
        std::sync::Arc::new(wakes),
    ));
    let ledger = std::sync::Arc::new(ledger);
    let work_store = std::sync::Arc::downgrade(store);
    let spawn = std::thread::Builder::new().name("desktop-schedule-driver".to_string());
    let result = spawn.spawn(move || {
        let driver = altai_control_plane::SchedulerDriver::new(
            materializer,
            ledger,
            altai_control_plane::SCHEDULE_OWNER_DESKTOP,
        );
        loop {
            std::thread::sleep(Duration::from_secs(60));
            // The in-process WorkStore handle is this app run's proof that
            // the workspace single-writer lock is ours; a released handle
            // means the workspace is no longer ours to schedule.
            let Some(_work_store) = work_store.upgrade() else {
                log::info!("Desktop schedule driver stopping: workspace store released");
                break;
            };
            // A failed tick is logged and the loop continues: one bad tick
            // must not halt scheduling for every other routine.
            if let Err(error) = driver.tick(now_unix_seconds()) {
                log::error!("desktop schedule driver tick failed: {error}");
            }
        }
    });
    if let Err(error) = result {
        log::error!("failed to spawn desktop schedule driver: {error}");
    }
}

fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[derive(Default)]
pub struct WorkspaceRegistry {
    roots: Mutex<HashSet<PathBuf>>,
    opened_roots: Mutex<HashMap<PathBuf, WorkspaceRootIdentity>>,
    canonical_cache: Mutex<HashMap<PathBuf, CanonicalEntry>>,
    migrated_work_dbs: Mutex<HashSet<PathBuf>>,
    work_stores: Mutex<HashMap<PathBuf, std::sync::Arc<altai_core::WorkStore>>>,
    control_hosts: Mutex<HashMap<PathBuf, std::sync::Arc<ControlProtocolHost>>>,
}

impl WorkspaceRegistry {
    pub fn authorize<P: AsRef<Path>>(&self, path: P) -> std::io::Result<PathBuf> {
        let canonical = std::fs::canonicalize(path.as_ref())?;
        let mut set = self.roots.lock().expect("workspace registry poisoned");
        set.insert(canonical.clone());
        Ok(canonical)
    }

    /// Grant an exact workspace selected/opened by the user. Broad bootstrap
    /// roots (notably HOME) intentionally do not enter this set.
    pub fn authorize_opened<P: AsRef<Path>>(&self, path: P) -> std::io::Result<PathBuf> {
        let canonical = self.authorize(path)?;
        let metadata = canonical.symlink_metadata()?;
        if metadata.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "opened workspace must not be a symlink",
            ));
        }
        let identity = WorkspaceRootIdentity::from_path_and_metadata(&canonical, &metadata)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "opened workspace must be a directory",
                )
            })?;
        let mut opened = self
            .opened_roots
            .lock()
            .expect("workspace registry poisoned");
        // There is one active user-opened workspace. Switching replaces the
        // exact grant instead of leaving historical projects previewable.
        opened.clear();
        opened.insert(canonical.clone(), identity);
        Ok(canonical)
    }

    /// Revoke every exact user-opened workspace grant. Broad roots used by
    /// shell/filesystem compatibility remain authorized separately.
    pub fn revoke_opened(&self) {
        self.opened_roots
            .lock()
            .expect("workspace registry poisoned")
            .clear();
    }

    pub fn capture_opened_exact<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::io::Result<OpenedWorkspaceGrant> {
        let canonical = std::fs::canonicalize(path)?;
        let metadata = canonical.symlink_metadata()?;
        if metadata.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "workspace identity is not an exact opened grant",
            ));
        }
        let current = WorkspaceRootIdentity::from_path_and_metadata(&canonical, &metadata)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "workspace identity is not an exact opened grant",
                )
            })?;
        let opened = self
            .opened_roots
            .lock()
            .expect("workspace registry poisoned")
            .get(&canonical)
            .cloned();
        if opened.as_ref() != Some(&current) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "workspace identity is not an exact opened grant",
            ));
        }
        Ok(OpenedWorkspaceGrant {
            canonical,
            identity: current,
        })
    }

    pub fn is_opened_grant_current(&self, grant: &OpenedWorkspaceGrant) -> bool {
        let opened = self
            .opened_roots
            .lock()
            .expect("workspace registry poisoned");
        if opened.get(&grant.canonical) != Some(&grant.identity) {
            return false;
        }
        let Ok(metadata) = grant.canonical.symlink_metadata() else {
            return false;
        };
        #[cfg(not(windows))]
        if metadata.file_type().is_symlink() {
            return false;
        }
        // On Windows, matches_path opens the directory itself and rejects all
        // reparse points (including junctions), which is stronger than
        // FileType::is_symlink().
        grant.identity.matches_path(&grant.canonical, &metadata)
    }

    pub fn is_authorized(&self, target: &Path) -> bool {
        let set = self.roots.lock().expect("workspace registry poisoned");
        set.iter().any(|root| target.starts_with(root))
    }

    /// Bring a workspace's `work.db` to the current Work OS schema once per
    /// app run. The desktop's per-command store opens share this single
    /// lifecycle entry point, so repository-open order can never decide which
    /// tables exist. A database written by a newer host fails closed here,
    /// before any adapter opens it; failures are not cached so an updated
    /// database recovers without an app restart.
    pub fn ensure_work_db_migrated(&self, database: &Path) -> Result<(), String> {
        let mut migrated = self
            .migrated_work_dbs
            .lock()
            .expect("workspace registry poisoned");
        if migrated.contains(database) {
            return Ok(());
        }
        altai_control_plane::LocalMigrationRunner::migrate(database).map_err(
            |error| match error {
                altai_control_plane::LocalMigrationError::UnsupportedSchema {
                    current,
                    supported,
                } => format!(
                    "This workspace's work.db uses schema {current}, which is newer than this \
                     build's supported schema {supported}. Update Altai to open this workspace."
                ),
                altai_control_plane::LocalMigrationError::Database { reason } => {
                    format!("work.db migration failed: {reason}")
                }
            },
        )?;
        migrated.insert(database.to_path_buf());
        drop(migrated);
        Ok(())
    }

    /// One control-protocol host per workspace `work.db` for this app run,
    /// shared by every desktop command (Studio included). Built only after
    /// the migration gate accepts the database; failures are not cached so
    /// an updated database recovers without an app restart.
    pub fn control_protocol_host(
        &self,
        database: &Path,
    ) -> Result<std::sync::Arc<ControlProtocolHost>, String> {
        if let Some(host) = self
            .control_hosts
            .lock()
            .expect("workspace registry poisoned")
            .get(database)
        {
            return Ok(host.clone());
        }
        self.ensure_work_db_migrated(database)?;
        let host = std::sync::Arc::new(ControlProtocolHost::open(database)?);
        self.control_hosts
            .lock()
            .expect("workspace registry poisoned")
            .insert(database.to_path_buf(), host.clone());
        Ok(host)
    }

    /// One WorkStore per workspace `work.db` for this app run, shared by
    /// every desktop command. The store holds the workspace's single-writer
    /// lock (CP-08-106) for as long as it lives, so caching it keeps every
    /// command on that one lock instead of letting the app's own concurrent
    /// commands, reconcile ticks and refreshes collide into `WorkspaceHeld`
    /// failures. The map guard is held across migrate+open so a cache miss
    /// is filled exactly once even under concurrent callers. Built only
    /// after the migration gate accepts the database; failures are not
    /// cached so an updated database recovers without an app restart.
    pub fn work_store(
        &self,
        database: &Path,
    ) -> Result<std::sync::Arc<altai_core::WorkStore>, String> {
        let mut stores = self
            .work_stores
            .lock()
            .expect("workspace registry poisoned");
        if let Some(store) = stores.get(database) {
            return Ok(store.clone());
        }
        self.ensure_work_db_migrated(database)?;
        let store = std::sync::Arc::new(
            altai_core::WorkStore::open(database).map_err(|error| error.to_string())?,
        );
        stores.insert(database.to_path_buf(), store.clone());
        // Scheduling cutover (CP-08-108): the desktop schedule driver is
        // spawned only from this success path — an open that lost the
        // workspace lock (WorkspaceHeld) must not leave a driver ticking.
        ensure_desktop_schedule_driver(database, &store);
        Ok(store)
    }

    pub fn canonicalize_cached<P: AsRef<Path>>(&self, path: P) -> std::io::Result<PathBuf> {
        let key = path.as_ref().to_path_buf();
        {
            let cache = self
                .canonical_cache
                .lock()
                .expect("canonical cache poisoned");
            if let Some(entry) = cache.get(&key) {
                if entry.inserted_at.elapsed() < CANONICAL_TTL {
                    return Ok(entry.canonical.clone());
                }
            }
        }
        let canonical = std::fs::canonicalize(&key)?;
        let mut cache = self
            .canonical_cache
            .lock()
            .expect("canonical cache poisoned");
        if cache.len() >= CANONICAL_CACHE_CAP {
            cache.retain(|_, entry| entry.inserted_at.elapsed() < CANONICAL_TTL);
            if cache.len() >= CANONICAL_CACHE_CAP {
                cache.clear();
            }
        }
        cache.insert(
            key,
            CanonicalEntry {
                canonical: canonical.clone(),
                inserted_at: Instant::now(),
            },
        );
        Ok(canonical)
    }
}

// `None` means "use bootstrapped default". `Some` is canonicalized to defeat
// symlink/`..` traversal and must sit under an authorized root.
pub fn authorize_spawn_cwd(
    registry: &WorkspaceRegistry,
    cwd: Option<&str>,
    workspace: &WorkspaceEnv,
) -> Result<Option<PathBuf>, String> {
    let Some(cwd) = cwd.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let resolved = resolve_path(cwd, workspace);
    let canonical =
        std::fs::canonicalize(&resolved).map_err(|e| format!("cwd not accessible: {e}"))?;
    if !canonical.is_dir() {
        return Err(format!("cwd is not a directory: {}", canonical.display()));
    }
    if !registry.is_authorized(&canonical) {
        return Err(format!(
            "cwd is outside the authorized workspace: {}",
            canonical.display()
        ));
    }
    Ok(Some(canonical))
}

pub fn bootstrap_registry(registry: &WorkspaceRegistry) {
    let _ = registry.authorize(resolve_launch_dir());
    if let Some(home) = dirs::home_dir() {
        let _ = registry.authorize(home);
    }
}

/// Grant the webview's asset protocol (`asset:`/`convertFileSrc`) read access to
/// `dir`. Scoped deliberately to authorized workspace roots — the static config
/// scope is empty — so image preview can load files the user actually opened
/// without exposing the whole filesystem (the prior `"**"` scope did).
fn allow_asset_directory<R: tauri::Runtime>(app: &tauri::AppHandle<R>, dir: &Path) {
    if let Err(e) = app.asset_protocol_scope().allow_directory(dir, true) {
        log::warn!("asset scope grant failed for {}: {e}", dir.display());
    }
}

/// Authorize the launch directory for the asset protocol at startup so files in
/// the initially-opened project preview before any explicit workspace open.
pub fn grant_startup_asset_scope<R: tauri::Runtime>(app: &tauri::AppHandle<R>) {
    allow_asset_directory(app, &resolve_launch_dir());
}

#[tauri::command]
pub async fn workspace_authorize(
    path: String,
    workspace: Option<WorkspaceEnv>,
    registry: tauri::State<'_, WorkspaceRegistry>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    let workspace = WorkspaceEnv::from_option(workspace);
    let resolved = resolve_path(&path, &workspace);
    let canonical = registry.authorize(&resolved).map_err(|e| e.to_string())?;
    // Mirror the registry grant into the asset protocol so asset previews work
    // for files under any authorized workspace, but nothing outside it. This
    // generic API deliberately does not create an exact user-opened grant.
    allow_asset_directory(&app, &canonical);
    Ok(canonical.to_string_lossy().replace('\\', "/"))
}

/// Promote exactly one folder only after the host UI has successfully opened,
/// picked, or cloned it. Startup HOME grants, environment switches, and recent
/// probes must continue to use `workspace_authorize` instead.
#[tauri::command]
pub async fn workspace_authorize_opened(
    path: String,
    workspace: Option<WorkspaceEnv>,
    registry: tauri::State<'_, WorkspaceRegistry>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    let workspace = WorkspaceEnv::from_option(workspace);
    let resolved = resolve_path(&path, &workspace);
    let canonical = registry
        .authorize_opened(&resolved)
        .map_err(|e| e.to_string())?;
    allow_asset_directory(&app, &canonical);
    Ok(canonical.to_string_lossy().replace('\\', "/"))
}

/// Drop exact preview authority when the UI closes or changes workspace.
#[tauri::command]
pub async fn workspace_revoke_opened(
    registry: tauri::State<'_, WorkspaceRegistry>,
) -> Result<(), String> {
    registry.revoke_opened();
    Ok(())
}

#[tauri::command]
pub async fn workspace_current_dir(
    registry: tauri::State<'_, WorkspaceRegistry>,
) -> Result<String, String> {
    let launch = resolve_launch_dir();
    let canonical = registry.authorize(&launch).map_err(|e| e.to_string())?;
    Ok(canonical.to_string_lossy().replace('\\', "/"))
}

// Snapshotted once at app startup so the live `current_dir()` drifting later
// (file dialogs, plugin chdir) can't shift the value seen by IPC or spawn.
static LAUNCH_CWD: OnceLock<Option<PathBuf>> = OnceLock::new();

pub fn init_launch_cwd() {
    LAUNCH_CWD.get_or_init(|| {
        std::env::current_dir()
            .ok()
            .filter(|p| is_usable_launch_dir(p))
    });
}

pub fn launch_cwd_snapshot() -> Option<PathBuf> {
    LAUNCH_CWD.get().and_then(|o| o.clone())
}

fn resolve_launch_dir() -> PathBuf {
    if let Some(cwd) = launch_cwd_snapshot() {
        return cwd;
    }
    if let Some(cwd) = std::env::current_dir()
        .ok()
        .filter(|p| is_usable_launch_dir(p))
    {
        return cwd;
    }
    dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"))
}

fn is_usable_launch_dir(path: &Path) -> bool {
    if !path.is_dir() || path == Path::new("/") {
        return false;
    }
    let s = path.to_string_lossy();
    if s.contains(".app/Contents/") {
        return false;
    }
    if cfg!(debug_assertions) && path.file_name().and_then(|s| s.to_str()) == Some("src-tauri") {
        return false;
    }
    true
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum WorkspaceEnv {
    #[default]
    Local,
    Wsl {
        distro: String,
    },
}

impl WorkspaceEnv {
    pub fn from_option(workspace: Option<Self>) -> Self {
        workspace.unwrap_or_default()
    }

    pub fn is_wsl(&self) -> bool {
        matches!(self, Self::Wsl { .. })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct WslDistro {
    pub name: String,
    pub default: bool,
    pub running: bool,
}

#[cfg(windows)]
pub fn resolve_path(path: &str, workspace: &WorkspaceEnv) -> PathBuf {
    match workspace {
        WorkspaceEnv::Local => PathBuf::from(path),
        WorkspaceEnv::Wsl { distro } => wsl_path_to_host(distro, path),
    }
}

#[cfg(not(windows))]
pub fn resolve_path(path: &str, _workspace: &WorkspaceEnv) -> PathBuf {
    PathBuf::from(path)
}

/// True for WSL distro names safe to splice into a UNC path. Real WSL distros
/// are alphanumeric with `.`, `_`, `-` separators (e.g. `Ubuntu-22.04`). Reject
/// anything that could traverse out of the `\\wsl.localhost\<distro>\` prefix
/// (`..`, `\`, `/`, `:`, `?`, `*`, control bytes) or empty names.
#[cfg(windows)]
fn is_safe_distro_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 255 {
        return false;
    }
    if name == "." || name == ".." || name.starts_with('.') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ' '))
        && !name.contains("..")
}

#[cfg(windows)]
pub(crate) fn validate_wsl_distro_name(distro: &str) -> Result<(), String> {
    if is_safe_distro_name(distro) {
        Ok(())
    } else {
        Err(format!("unsafe WSL distro name: {distro}"))
    }
}

#[cfg(windows)]
fn wsl_drvfs_to_windows(path: &str) -> Option<PathBuf> {
    let normalized = path.replace('\\', "/");
    let rest = normalized.strip_prefix("/mnt/")?;
    let mut parts = rest.splitn(2, '/');
    let drive = parts.next()?;
    if drive.len() != 1 {
        return None;
    }
    let drive = drive.chars().next()?;
    if !drive.is_ascii_alphabetic() {
        return None;
    }
    let suffix = parts.next().unwrap_or("").replace('/', "\\");
    let mut host = format!("{}:\\", drive.to_ascii_uppercase());
    if !suffix.is_empty() {
        host.push_str(&suffix);
    }
    Some(PathBuf::from(host))
}

#[cfg(windows)]
pub fn wsl_path_to_unc(distro: &str, path: &str) -> PathBuf {
    // Defense-in-depth: refuse to construct a UNC path with a distro name that
    // could escape the WSL share root via `..`, `\`, or other path metachars.
    // Returns a clearly-invalid path that downstream `is_dir()`/`metadata()`
    // checks will reject. The webview's distro list comes from `wsl.exe --list`
    // and is normally trustworthy, but a locally-registered malicious distro
    // can name itself with traversal characters; this filter blocks that.
    if !is_safe_distro_name(distro) {
        return PathBuf::from(r"\\wsl.localhost\__altai_invalid_distro__");
    }
    let normalized = path.replace('\\', "/");
    let trimmed = normalized.trim_start_matches('/');
    let primary = PathBuf::from(format!(
        r"\\wsl.localhost\{}\{}",
        distro,
        trimmed.replace('/', r"\")
    ));
    if primary.exists() {
        return primary;
    }
    PathBuf::from(format!(r"\\wsl$\{}\{}", distro, trimmed.replace('/', r"\")))
}

#[cfg(windows)]
pub fn wsl_path_to_host(distro: &str, path: &str) -> PathBuf {
    // `/mnt/<drive>` is drvfs-backed Windows storage. Accessing it through the
    // WSL UNC share can return "Access is denied" on Windows even though the
    // same path is readable inside WSL. Use the native drive path instead.
    wsl_drvfs_to_windows(path).unwrap_or_else(|| wsl_path_to_unc(distro, path))
}

#[cfg(windows)]
pub fn decode_command_output(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xff, 0xfe]) || looks_utf16le(bytes) {
        let start = if bytes.starts_with(&[0xff, 0xfe]) {
            2
        } else {
            0
        };
        let units: Vec<u16> = bytes[start..]
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

#[cfg(windows)]
fn looks_utf16le(bytes: &[u8]) -> bool {
    if bytes.len() < 4 || !bytes.len().is_multiple_of(2) {
        return false;
    }
    let nul_odd = bytes.iter().skip(1).step_by(2).filter(|b| **b == 0).count();
    nul_odd * 2 >= bytes.len() / 2
}

#[cfg(windows)]
fn run_wsl(args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("wsl.exe")
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        let stderr = decode_command_output(&out.stderr);
        return Err(stderr.trim().to_string());
    }
    Ok(decode_command_output(&out.stdout))
}

#[cfg(windows)]
pub(crate) fn wsl_exec_capture(
    distro: &str,
    program: &str,
    args: &[&str],
) -> Result<String, String> {
    validate_wsl_distro_name(distro)?;
    let out = std::process::Command::new("wsl.exe")
        .arg("-d")
        .arg(distro)
        .arg("--exec")
        .arg(program)
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        let stderr = decode_command_output(&out.stderr);
        return Err(stderr.trim().to_string());
    }
    Ok(decode_command_output(&out.stdout))
}

#[cfg(windows)]
fn run_wsl_sh(distro: &str, script: &str) -> Result<String, String> {
    // Probe helpers must avoid login-shell startup files. User `.profile`
    // output on stdout would corrupt the parsed value (`$HOME`, login shell).
    wsl_exec_capture(distro, "sh", &["-c", script])
}

#[cfg(windows)]
pub(crate) fn normalize_wsl_value(output: String, fallback: &str) -> String {
    let value = output
        .lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

#[cfg(windows)]
fn list_distros_blocking() -> Result<Vec<WslDistro>, String> {
    let out = run_wsl(&["--list", "--verbose"])?;
    let mut distros = Vec::new();
    for raw in out.lines().skip(1) {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let default = line.starts_with('*');
        let line = line.trim_start_matches('*').trim();
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }
        let state_idx = parts.len() - 2;
        let name = parts[..state_idx].join(" ");
        let state = parts[state_idx];
        distros.push(WslDistro {
            name,
            default,
            running: state.eq_ignore_ascii_case("Running"),
        });
    }
    Ok(distros)
}

#[tauri::command]
pub async fn wsl_list_distros() -> Result<Vec<WslDistro>, String> {
    #[cfg(not(windows))]
    {
        Ok(Vec::new())
    }
    #[cfg(windows)]
    {
        tauri::async_runtime::spawn_blocking(list_distros_blocking)
            .await
            .map_err(|e| e.to_string())?
    }
}

#[tauri::command]
pub async fn wsl_default_distro() -> Result<Option<String>, String> {
    #[cfg(not(windows))]
    {
        Ok(None)
    }
    #[cfg(windows)]
    {
        tauri::async_runtime::spawn_blocking(|| {
            let distros = list_distros_blocking()?;
            Ok(distros
                .iter()
                .find(|d| d.default)
                .map(|d| d.name.clone())
                .or_else(|| distros.first().map(|d| d.name.clone())))
        })
        .await
        .map_err(|e| e.to_string())?
    }
}

#[tauri::command]
pub fn wsl_home(distro: String) -> Result<String, String> {
    #[cfg(not(windows))]
    {
        let _ = distro;
        Err("WSL is only available on Windows".into())
    }
    #[cfg(windows)]
    {
        let out = run_wsl_sh(&distro, "printf %s \"$HOME\"")?;
        let home = normalize_wsl_value(out, "");
        if home.is_empty() {
            Err(format!("could not resolve WSL home for {distro}"))
        } else {
            Ok(home)
        }
    }
}

#[cfg(windows)]
pub fn wsl_login_shell(distro: String) -> Result<String, String> {
    const SCRIPT: &str = r#"uid="$(id -u 2>/dev/null || printf '')"
entry=''
if [ -n "$uid" ] && command -v getent >/dev/null 2>&1; then
  entry="$(getent passwd "$uid" 2>/dev/null || true)"
fi
if [ -z "$entry" ] && [ -n "$uid" ] && [ -r /etc/passwd ]; then
  entry="$(awk -F: -v u="$uid" '$3 == u { print; exit }' /etc/passwd 2>/dev/null)"
fi
shell=''
if [ -n "$entry" ]; then
  shell="${entry##*:}"
fi
if [ -z "$shell" ] && [ -n "$SHELL" ]; then
  shell="$SHELL"
fi
if [ -z "$shell" ]; then
  shell=/bin/sh
fi
printf %s "$shell""#;

    let out = run_wsl_sh(&distro, SCRIPT)?;
    Ok(normalize_wsl_value(out, "/bin/sh"))
}

#[cfg(test)]
mod work_db_lifecycle_tests {
    use super::*;

    #[test]
    fn work_db_migration_runs_once_per_app_run() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("work.db");
        let registry = WorkspaceRegistry::default();

        registry.ensure_work_db_migrated(&database).unwrap();
        assert!(database.exists());

        std::fs::remove_file(&database).unwrap();
        // Cached for this run: the gate skips the runner instead of
        // recreating the database behind the caller's back.
        registry.ensure_work_db_migrated(&database).unwrap();
        assert!(!database.exists());
    }

    #[test]
    fn a_newer_work_db_fails_closed_and_is_not_cached() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("work.db");
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
        let error = registry.ensure_work_db_migrated(&database).unwrap_err();
        assert!(
            error.contains("newer than this build"),
            "unexpected error: {error}"
        );
        // Failures stay uncached so an updated database recovers without an
        // app restart — assert by looking at the still-present refusal.
        assert!(registry.ensure_work_db_migrated(&database).is_err());
    }

    #[test]
    fn one_work_store_is_reused_per_work_db() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("work.db");
        let registry = WorkspaceRegistry::default();

        let first = registry.work_store(&database).unwrap();
        let second = registry.work_store(&database).unwrap();
        // Both callers share the cached store, so both share its
        // single-writer lock instead of the second open failing against the
        // first.
        assert!(std::sync::Arc::ptr_eq(&first, &second));
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn distro_validator_accepts_real_names() {
        assert!(is_safe_distro_name("Ubuntu"));
        assert!(is_safe_distro_name("Ubuntu-22.04"));
        assert!(is_safe_distro_name("Debian"));
        assert!(is_safe_distro_name("Alpine_3.18"));
        assert!(is_safe_distro_name("openSUSE-Tumbleweed"));
    }

    #[test]
    fn distro_validator_rejects_path_traversal() {
        assert!(!is_safe_distro_name(".."));
        assert!(!is_safe_distro_name("..\\..\\Windows"));
        assert!(!is_safe_distro_name("../foo"));
        assert!(!is_safe_distro_name("foo/bar"));
        assert!(!is_safe_distro_name("foo\\bar"));
        assert!(!is_safe_distro_name("foo..bar"));
    }

    #[test]
    fn distro_validator_rejects_special_chars() {
        assert!(!is_safe_distro_name("foo:bar"));
        assert!(!is_safe_distro_name("foo?bar"));
        assert!(!is_safe_distro_name("foo*bar"));
        assert!(!is_safe_distro_name("foo\0bar"));
        assert!(!is_safe_distro_name(""));
        assert!(!is_safe_distro_name(".hidden"));
    }

    #[test]
    fn wsl_path_to_unc_blocks_traversal_distro() {
        // Malicious distro name must produce a path that is_dir() will reject,
        // never escape the WSL share root.
        let p = wsl_path_to_unc("..\\..\\..\\Windows", "/etc/passwd");
        let s = p.to_string_lossy();
        assert!(s.contains("__altai_invalid_distro__"), "got: {s}");
        assert!(!s.contains("\\..\\"), "got: {s}");
    }

    #[test]
    fn wsl_path_to_unc_accepts_valid_distro() {
        let p = wsl_path_to_unc("Ubuntu", "/etc/hosts");
        let s = p.to_string_lossy();
        assert!(!s.contains("__altai_invalid_distro__"), "got: {s}");
    }

    #[test]
    fn resolve_path_keeps_local_paths_unchanged() {
        let path = r"C:\Users\vinicios\repo";
        assert_eq!(
            resolve_path(path, &WorkspaceEnv::Local),
            PathBuf::from(path)
        );
    }

    #[test]
    fn resolve_path_maps_wsl_paths_to_host() {
        let workspace = WorkspaceEnv::Wsl {
            distro: "Ubuntu".into(),
        };
        assert_eq!(
            resolve_path("/home/vinicios/repo", &workspace),
            wsl_path_to_host("Ubuntu", "/home/vinicios/repo")
        );
    }

    #[test]
    fn wsl_drvfs_root_maps_to_windows_drive() {
        assert_eq!(wsl_drvfs_to_windows("/mnt/c"), Some(PathBuf::from(r"C:\")));
    }

    #[test]
    fn wsl_drvfs_child_maps_to_windows_drive() {
        assert_eq!(
            wsl_drvfs_to_windows("/mnt/d/Users/vinicios/repo"),
            Some(PathBuf::from(r"D:\Users\vinicios\repo"))
        );
    }

    #[test]
    fn wsl_drvfs_rejects_non_drive_mounts() {
        assert_eq!(wsl_drvfs_to_windows("/mnt/wsl"), None);
        assert_eq!(wsl_drvfs_to_windows("/home/vinicios"), None);
    }

    #[test]
    fn normalize_wsl_value_uses_last_nonempty_line() {
        assert_eq!(
            normalize_wsl_value("banner\n  /bin/zsh \n".into(), "/bin/sh"),
            "/bin/zsh"
        );
    }

    #[test]
    fn normalize_wsl_value_falls_back_when_empty() {
        assert_eq!(normalize_wsl_value(" \n".into(), "/bin/sh"), "/bin/sh");
    }
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use std::env;
    use std::fs;

    fn tempdir(label: &str) -> PathBuf {
        let mut p = env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        p.push(format!("altai-auth-{label}-{nanos}-{}", std::process::id()));
        fs::create_dir_all(&p).expect("create tempdir");
        fs::canonicalize(&p).expect("canonicalize tempdir")
    }

    #[test]
    fn authorize_spawn_cwd_accepts_none() {
        let reg = WorkspaceRegistry::default();
        assert!(authorize_spawn_cwd(&reg, None, &WorkspaceEnv::Local)
            .unwrap()
            .is_none());
    }

    #[test]
    fn authorize_spawn_cwd_accepts_empty_string() {
        let reg = WorkspaceRegistry::default();
        assert!(authorize_spawn_cwd(&reg, Some("   "), &WorkspaceEnv::Local)
            .unwrap()
            .is_none());
    }

    #[test]
    fn authorize_spawn_cwd_accepts_authorized_path() {
        let dir = tempdir("ok");
        let reg = WorkspaceRegistry::default();
        reg.authorize(&dir).expect("authorize root");
        let s = dir.to_string_lossy().into_owned();
        let resolved = authorize_spawn_cwd(&reg, Some(&s), &WorkspaceEnv::Local)
            .expect("authorized")
            .expect("returned canonical");
        assert_eq!(resolved, dir);
    }

    #[test]
    fn authorize_spawn_cwd_accepts_subdir_of_authorized_root() {
        let root = tempdir("subroot");
        let sub = root.join("inside");
        fs::create_dir_all(&sub).expect("subdir");
        let canonical_sub = fs::canonicalize(&sub).expect("canon sub");
        let reg = WorkspaceRegistry::default();
        reg.authorize(&root).expect("authorize root");
        let s = canonical_sub.to_string_lossy().into_owned();
        let resolved = authorize_spawn_cwd(&reg, Some(&s), &WorkspaceEnv::Local)
            .expect("subdir authorized")
            .expect("returned canonical");
        assert_eq!(resolved, canonical_sub);
    }

    #[test]
    fn authorize_spawn_cwd_rejects_unauthorized_path() {
        let allowed = tempdir("allowed");
        let foreign = tempdir("foreign");
        let reg = WorkspaceRegistry::default();
        reg.authorize(&allowed).expect("authorize root");
        let s = foreign.to_string_lossy().into_owned();
        let err = authorize_spawn_cwd(&reg, Some(&s), &WorkspaceEnv::Local)
            .expect_err("should reject unauthorized cwd");
        assert!(err.contains("outside"), "got: {err}");
    }

    #[test]
    fn authorize_spawn_cwd_rejects_missing_path() {
        let mut missing = env::temp_dir();
        missing.push(format!(
            "altai-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let reg = WorkspaceRegistry::default();
        let s = missing.to_string_lossy().into_owned();
        let err = authorize_spawn_cwd(&reg, Some(&s), &WorkspaceEnv::Local)
            .expect_err("should reject missing path");
        assert!(err.contains("cwd not accessible"), "got: {err}");
    }

    #[test]
    fn authorize_spawn_cwd_blocks_symlink_escape() {
        let allowed = tempdir("symroot");
        let outside = tempdir("symtarget");
        let link = allowed.join("escape");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&outside, &link).expect("symlink");
        let reg = WorkspaceRegistry::default();
        reg.authorize(&allowed).expect("authorize root");
        let s = link.to_string_lossy().into_owned();
        let err = authorize_spawn_cwd(&reg, Some(&s), &WorkspaceEnv::Local)
            .expect_err("symlink-escape must be rejected");
        assert!(err.contains("outside"), "got: {err}");
    }

    #[test]
    fn startup_home_and_recent_probe_authorization_are_not_exact_opened_grants() {
        let root = tempdir("bootstrap-only");
        let probe = tempdir("recent-probe");
        let reg = WorkspaceRegistry::default();
        let canonical = reg.authorize(&root).expect("bootstrap root");
        let canonical_probe = reg.authorize(&probe).expect("recent probe");
        assert!(reg.is_authorized(&canonical));
        assert!(reg.is_authorized(&canonical_probe));
        assert!(reg.capture_opened_exact(&canonical).is_err());
        assert!(reg.capture_opened_exact(&canonical_probe).is_err());
    }

    #[test]
    fn opened_workspace_grant_is_exact_and_does_not_cover_children() {
        let root = tempdir("opened-exact");
        let child = root.join("child");
        fs::create_dir_all(&child).expect("child");
        let canonical_child = child.canonicalize().expect("canonical child");
        let reg = WorkspaceRegistry::default();
        let canonical_root = reg.authorize_opened(&root).expect("opened root");
        assert!(reg.capture_opened_exact(&canonical_root).is_ok());
        assert!(reg.capture_opened_exact(&canonical_child).is_err());
    }

    #[test]
    fn opened_workspace_grant_rejects_a_replaced_root_identity() {
        let parent = tempdir("opened-swap-parent");
        let root = parent.join("workspace");
        let moved = parent.join("workspace-original");
        fs::create_dir(&root).expect("workspace");
        let reg = WorkspaceRegistry::default();
        let canonical = reg.authorize_opened(&root).expect("opened root");

        fs::rename(&root, &moved).expect("move original root");
        fs::create_dir(&root).expect("replacement root");

        assert!(reg.capture_opened_exact(&canonical).is_err());
        assert!(reg.capture_opened_exact(&root).is_err());
    }

    #[test]
    fn opening_or_revoking_a_workspace_invalidates_the_prior_exact_grant() {
        let first = tempdir("opened-first");
        let second = tempdir("opened-second");
        let reg = WorkspaceRegistry::default();

        let first = reg.authorize_opened(&first).expect("first opened root");
        let first_grant = reg.capture_opened_exact(&first).expect("first grant");
        assert!(reg.is_opened_grant_current(&first_grant));

        let second = reg.authorize_opened(&second).expect("second opened root");
        assert!(reg.capture_opened_exact(&first).is_err());
        assert!(!reg.is_opened_grant_current(&first_grant));
        let second_grant = reg.capture_opened_exact(&second).expect("second grant");
        assert!(reg.is_opened_grant_current(&second_grant));

        reg.revoke_opened();
        assert!(reg.capture_opened_exact(&second).is_err());
        assert!(!reg.is_opened_grant_current(&second_grant));
    }
}
