mod altai;
mod modules;

use altai::agent::commands as agent_commands;
use modules::{
    app_menu, control_plane_flags, control_protocol, external_sync, fs, git, github, gmail,
    lsp_install, mcp, net,
    notebook, orchestration, os_menu, proc, pty, routines, secrets, shell, webview, work,
    work_import, workspace,
};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::{Emitter, Manager, State, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_window_state::StateFlags;

#[cfg(any(target_os = "windows", test))]
fn windows_webview_args_for(disable_gpu: bool) -> String {
    let mut args = "--disable-features=msWebOOUI,msPdfOOUI,msSmartScreenProtection".to_string();

    // WebView2 uses the GPU by default and Microsoft recommends keeping it
    // enabled for normal application sessions. 0.6.8 forced software rendering
    // for every Windows user while investigating a driver-specific white-screen
    // report; on other machines that can leave the compositor black and makes
    // every ALTAI surface fail together. Keep software rendering as an explicit
    // troubleshooting escape hatch instead of a global application invariant.
    if disable_gpu {
        args.push_str(" --disable-gpu");
    }

    args
}

#[cfg(target_os = "windows")]
pub(crate) fn windows_webview_args() -> String {
    windows_webview_args_for(env_flag("ALTAI_DISABLE_GPU"))
}

/// WebView2 deadlocks if `WebviewWindowBuilder::build` runs inside a
/// synchronous Tauri command or menu/event handler on Windows
/// (see wry#583 / Tauri WebviewWindowBuilder docs). Async commands already
/// leave that path; sync callers must offload onto a worker thread.
pub(crate) fn create_window_off_ipc_thread(work: impl FnOnce() + Send + 'static) {
    #[cfg(target_os = "windows")]
    {
        std::thread::spawn(work);
    }
    #[cfg(not(target_os = "windows"))]
    {
        work();
    }
}

/// Run WebView window create/config on the platform-correct thread.
///
/// - macOS: `WKWebViewConfiguration` must be built on the main thread. Async
///   Tauri commands run on a tokio worker, so we hop via `run_on_main_thread`.
/// - Windows: keep off the IPC thread (caller is already async / spawned) —
///   building WebView2 inline on the IPC thread deadlocks.
fn run_webview_window_work<T>(
    app: &tauri::AppHandle,
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    #[cfg(target_os = "macos")]
    {
        let (tx, rx) = std::sync::mpsc::channel();
        app.run_on_main_thread(move || {
            let _ = tx.send(work());
        })
        .map_err(|error| error.to_string())?;
        rx.recv().map_err(|error| error.to_string())?
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = app;
        work()
    }
}

/// Per-window WebView2 profile dir. Windows requires a distinct data directory
/// when multiple webviews use `additional_browser_args`, otherwise the second
/// window can hang as a black/blank surface.
#[cfg(target_os = "windows")]
fn windows_webview_data_dir(app: &tauri::AppHandle, label: &str) -> Option<std::path::PathBuf> {
    let root = app.path().app_data_dir().ok()?;
    Some(root.join("webview-profiles").join(sanitize_webview_profile_label(label)))
}

#[cfg(any(target_os = "windows", test))]
fn sanitize_webview_profile_label(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

const WINDOW_BACKGROUND: tauri::webview::Color = tauri::webview::Color(10, 10, 10, 255);

fn restored_window_state_flags() -> StateFlags {
    // Window chrome is an application/platform invariant, not user state.
    // Never restore DECORATIONS — older snapshots may disagree with the
    // current Cursor-style frameless chrome on Windows/Linux.
    let flags = StateFlags::all() & !StateFlags::VISIBLE & !StateFlags::DECORATIONS;

    // Keep fullscreen opt-in per session on Windows so a cold start never
    // traps the user in an undecorated fullscreen frame.
    #[cfg(target_os = "windows")]
    let flags = flags & !StateFlags::FULLSCREEN;

    flags
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

/// Brings a window forward and explicitly transfers input focus into its webview.
///
/// On Windows, focusing only the outer HWND can leave WebView2 outside the
/// screen reader's active focus path. `Webview::set_focus` reaches the
/// controller-level `MoveFocus` API that NVDA, JAWS, and Narrator expect.
pub(crate) fn focus_webview_window(window: &tauri::WebviewWindow) -> tauri::Result<()> {
    window.show()?;
    window.unminimize()?;
    window.set_focus()?;
    #[cfg(target_os = "windows")]
    window.as_ref().set_focus()?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct LaunchPayload {
    #[serde(rename = "type")]
    kind: String,
    paths: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action: Option<String>,
}

/// Drained on first read so HMR / re-mounts can't replay the launch dir.
#[derive(Default)]
struct PendingLaunch(Mutex<Vec<LaunchPayload>>);

#[tauri::command]
fn get_pending_launches(state: State<'_, PendingLaunch>) -> Vec<LaunchPayload> {
    let mut pending = state.0.lock().expect("PendingLaunch mutex poisoned");
    std::mem::take(&mut *pending)
}

/// Read a process env var as a boolean flag. Returns `true` when the var is
/// set to `"1"`, `"true"`, `"yes"`, or `"on"` (case-insensitive); `false`
/// otherwise (including when unset). Used by the frontend to honor
/// `ALTAI_DISABLE_AUTOCOMPACT` / `ALTAI_DISABLE_PRUNE` overrides that
/// Vite's `import.meta.env` can't see.
#[tauri::command]
fn env_get_flag(name: String) -> bool {
    env_flag(&name)
}

/// Open a filesystem item with a user-selected application. This deliberately
/// stays in the backend so the webview never gets broad process-launch access.
#[tauri::command]
fn open_with(path: String, application: String) -> Result<(), String> {
    let application = application.trim();
    if application.is_empty() {
        return Err("An application name or executable is required.".to_string());
    }

    let path = std::fs::canonicalize(&path)
        .map_err(|e| format!("Could not access the selected item: {e}"))?;
    tauri_plugin_opener::open_path(path, Some(application))
        .map_err(|e| format!("Could not open the item with {application}: {e}"))
}

fn collect_launch_payloads(args: Vec<String>, cwd: Option<&str>) -> Vec<LaunchPayload> {
    let mut files = Vec::new();
    let mut folders = Vec::new();
    let mut action = None;

    for arg in args.into_iter().skip(1) {
        if arg == "--explain" {
            action = Some("explain".to_string());
            continue;
        }
        if arg == "--refactor" {
            action = Some("refactor".to_string());
            continue;
        }
        if arg == "--ask-project" {
            action = Some("ask-project".to_string());
            continue;
        }
        if arg.starts_with('-') {
            continue;
        }
        // Resolve relative args against the caller-provided cwd (the requesting
        // process's directory for single-instance launches), not this process's.
        let candidate = std::path::Path::new(&arg);
        let resolved = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else if let Some(base) = cwd {
            std::path::Path::new(base).join(candidate)
        } else {
            candidate.to_path_buf()
        };
        let Ok(canon) = std::fs::canonicalize(&resolved) else {
            continue;
        };
        let s = canon.to_string_lossy();
        let path = s.strip_prefix(r"\\?\").unwrap_or(&s).to_string();

        if canon.is_dir() {
            folders.push(path);
        } else if canon.is_file() {
            files.push(path);
        }
    }

    let mut payloads = Vec::new();

    if !folders.is_empty() {
        payloads.push(LaunchPayload {
            kind: "folder".to_string(),
            paths: folders,
            action: action.clone(),
        });
    }

    if !files.is_empty() {
        payloads.push(LaunchPayload {
            kind: if files.len() > 1 {
                "multi_file".to_string()
            } else {
                "file".to_string()
            },
            paths: files,
            action,
        });
    }

    payloads
}

fn handle_launch_args(app: &tauri::AppHandle, args: Vec<String>, cwd: Option<&str>) {
    let payloads = collect_launch_payloads(args, cwd);
    for payload in payloads {
        // When `main` is already up, deliver straight to it (targeting the
        // primary window so extra windows don't all switch folders) and do NOT
        // queue: a queued payload would be drained by the next "New Window" on
        // mount, hijacking its welcome screen. Only queue when no window exists
        // yet (cold start / startup race), for the first window to pick up.
        match app.get_webview_window("main") {
            Some(main) => {
                let _ = main.emit("altai:launch", &payload);
            }
            None => {
                let _ = app.emit("altai:launch", &payload);
                let state = app.state::<PendingLaunch>();
                state.0.lock().unwrap().push(payload);
            }
        }
    }
}

fn parse_initial_launch(state: &PendingLaunch) {
    let args = std::env::args().collect();
    let payloads = collect_launch_payloads(args, None);
    let mut pending = state.0.lock().expect("PendingLaunch mutex poisoned");
    pending.extend(payloads);
}

pub(crate) fn show_or_create_settings_window(
    app: &tauri::AppHandle,
    label: &str,
    title: &str,
    surface: &str,
    tab: Option<&str>,
) -> Result<(), String> {
    if let Some(window) = app.get_webview_window(label) {
        let _ = focus_webview_window(&window);
        if let Some(t) = tab.filter(|s| !s.is_empty()) {
            // emit() serializes via JSON — no string-escape footgun, unlike
            // eval() with format!(). Frontend listens via Tauri event API.
            let _ = window.emit("altai:settings-tab", t);
        }
        return Ok(());
    }

    let mut url_path = format!("settings.html?surface={surface}");
    if let Some(t) = tab.filter(|s| !s.is_empty()) {
        url_path.push_str("&tab=");
        url_path.push_str(t);
    }

    let builder = WebviewWindowBuilder::new(app, label, WebviewUrl::App(url_path.into()))
        .title(title)
        .inner_size(860.0, 620.0)
        .min_inner_size(760.0, 520.0)
        .resizable(true)
        .focused(true)
        .visible(true)
        .background_color(WINDOW_BACKGROUND);

    // Settings header is h-11 (44px); inset lights to the vertical center.
    // Opt out of Apple Intelligence Writing Tools / Siri AI selection popover.
    #[cfg(target_os = "macos")]
    let builder = builder
        .title_bar_style(tauri::TitleBarStyle::Overlay)
        .hidden_title(true)
        .traffic_light_position(tauri::LogicalPosition::new(16.0, 16.0))
        .with_webview_configuration(modules::macos_webview::config_without_writing_tools());

    // App-owned chrome (Cursor-style): no classic OS title bar on Linux/Windows.
    // Windows stays opaque — WebView2 + transparent frames are unreliable.
    #[cfg(target_os = "linux")]
    let builder = builder.decorations(false).transparent(true);
    #[cfg(target_os = "windows")]
    let builder = {
        let builder = builder
            .decorations(false)
            .transparent(false)
            .additional_browser_args(&windows_webview_args());
        match windows_webview_data_dir(app, label) {
            Some(dir) => builder.data_directory(dir),
            None => builder,
        }
    };

    let window = builder.build().map_err(|e| e.to_string())?;
    // Some WMs ignore builder-time decorations; re-assert frameless chrome.
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    {
        let _ = window.set_decorations(false);
    }
    let _ = focus_webview_window(&window);
    Ok(())
}

#[tauri::command]
async fn open_settings_window(app: tauri::AppHandle, tab: Option<String>) -> Result<(), String> {
    // async: WebView2 deadlocks if build() runs in a sync command on Windows.
    // macOS still needs the main thread for WKWebViewConfiguration.
    let handle = app.clone();
    run_webview_window_work(&app, move || {
        show_or_create_settings_window(
            &handle,
            "settings",
            "ALTAI Studio Settings",
            "app",
            tab.as_deref(),
        )
    })
}

/// Percent-encode a filesystem path for use as a query parameter value.
fn encode_query_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len().saturating_mul(3));
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            // Keep path separators readable in logs / DevTools.
            b'/' => out.push('/'),
            _ => {
                out.push('%');
                out.push_str(&format!("{byte:02X}"));
            }
        }
    }
    out
}

fn studio_window_url(folder: Option<&str>) -> String {
    match folder.map(str::trim).filter(|path| !path.is_empty()) {
        Some(path) => format!(
            "index.html?mode=studio&folder={}",
            encode_query_component(path)
        ),
        None => "index.html?mode=studio".to_string(),
    }
}

fn emit_studio_folder(window: &tauri::WebviewWindow, folder: Option<&str>) {
    let Some(path) = folder.map(str::trim).filter(|path| !path.is_empty()) else {
        return;
    };
    let _ = window.emit(
        "altai:launch",
        &LaunchPayload {
            kind: "folder".to_string(),
            paths: vec![path.replace('\\', "/")],
            action: None,
        },
    );
}

/// Build an IDE webview (`studio` singleton or Dock "New Window").
pub(crate) fn build_ide_window(
    app: &tauri::AppHandle,
    label: &str,
    folder: Option<&str>,
) -> Result<tauri::WebviewWindow, String> {
    let builder = WebviewWindowBuilder::new(
        app,
        label,
        WebviewUrl::App(studio_window_url(folder).into()),
    )
    .title("ALTAI IDE")
    .inner_size(1280.0, 800.0)
    .min_inner_size(900.0, 600.0)
    .resizable(true)
    .focused(true)
    .visible(true)
    .background_color(WINDOW_BACKGROUND);

    #[cfg(target_os = "macos")]
    let builder = builder
        .title_bar_style(tauri::TitleBarStyle::Overlay)
        .hidden_title(true)
        // Tauri's macOS Y value includes the native titlebar inset. `22`
        // places the 16px traffic lights at y=12 inside our 40px IDE header,
        // so both centers land exactly on y=20.
        .traffic_light_position(tauri::LogicalPosition::new(16.0, 22.0))
        .with_webview_configuration(modules::macos_webview::config_without_writing_tools());

    // App-owned chrome (Cursor-style): no classic OS title bar on Linux/Windows.
    // Windows stays opaque — WebView2 + transparent frames are unreliable.
    #[cfg(target_os = "linux")]
    let builder = builder.decorations(false).transparent(true);
    #[cfg(target_os = "windows")]
    let builder = {
        let builder = builder
            .decorations(false)
            .transparent(false)
            .additional_browser_args(&windows_webview_args());
        match windows_webview_data_dir(app, label) {
            Some(dir) => builder.data_directory(dir),
            None => builder,
        }
    };

    let window = builder.build().map_err(|e| e.to_string())?;
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    {
        let _ = window.set_decorations(false);
    }
    Ok(window)
}

pub(crate) fn show_or_create_studio_window(
    app: &tauri::AppHandle,
    folder: Option<&str>,
) -> Result<(), String> {
    if let Some(window) = app.get_webview_window("studio") {
        let _ = focus_webview_window(&window);
        emit_studio_folder(&window, folder);
        return Ok(());
    }

    let window = build_ide_window(app, "studio", folder)?;
    let _ = focus_webview_window(&window);
    // The new webview may not have subscribed to events yet; the URL also
    // carries `folder` so the first mount can apply it without a race.
    emit_studio_folder(&window, folder);
    Ok(())
}

#[tauri::command]
async fn open_studio_window(app: tauri::AppHandle, folder: Option<String>) -> Result<(), String> {
    // async: WebView2 deadlocks if build() runs in a sync command on Windows.
    // That is the root cause of the black IDE window + app freeze on Windows.
    // macOS still needs the main thread for WKWebViewConfiguration — without
    // the hop below, Open IDE panics on a tokio worker (`MainThreadMarker`).
    let handle = app.clone();
    run_webview_window_work(&app, move || {
        show_or_create_studio_window(&handle, folder.as_deref())
    })
}

/// Renderer-to-native startup checkpoint used by logs and the Windows release
/// smoke test. The frontend invokes this from a React effect, after the real
/// application shell commits; no synthetic overlay is painted over the UI.
#[tauri::command]
fn renderer_ready(window: tauri::WebviewWindow) -> bool {
    log::info!("renderer ready: {}", window.label());
    let smoke = cfg!(target_os = "windows") && env_flag("ALTAI_GUI_SMOKE");
    if smoke {
        let _ = window.set_title("ALTAI [renderer-ready]");
    }
    smoke
}

#[tauri::command]
async fn focus_agent_window(app: tauri::AppHandle) -> Result<(), String> {
    let window = app
        .get_webview_window("main")
        .ok_or_else(|| "Agent window is not available".to_string())?;
    focus_webview_window(&window).map_err(|error| error.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    workspace::init_launch_cwd();
    let _ = modules::os_integration::register_context_menus();

    tauri::Builder::default()
        .on_page_load(|webview, payload| {
            log::info!(
                "page load {:?}: {} ({})",
                payload.event(),
                webview.label(),
                payload.url()
            );
        })
        .plugin(tauri_plugin_single_instance::init(|app, args, cwd| {
            // A `--new-window` relaunch (from the Dock/Jump List/.desktop action
            // or `altai --new-window`) opens a fresh window instead of focusing
            // the existing one. Any folder/file args alongside it are still
            // honored (delivered to the primary window by handle_launch_args).
            if args.iter().any(|a| a == "--new-window") {
                // Menu / single-instance callbacks are sync event handlers —
                // creating the WebView inline deadlocks WebView2 on Windows.
                let handle = app.clone();
                create_window_off_ipc_thread(move || {
                    os_menu::spawn_new_window(&handle);
                });
                let rest: Vec<String> = args.into_iter().filter(|a| a != "--new-window").collect();
                handle_launch_args(app, rest, Some(&cwd));
                return;
            }
            if let Some(main) = app.get_webview_window("main") {
                let _ = focus_webview_window(&main);
            }
            handle_launch_args(app, args, Some(&cwd));
        }))
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_cli::init())
        .plugin(tauri_plugin_process::init())
        // TODO: Re-enable updater once ALTAI has its own update endpoint
        // .plugin(tauri_plugin_updater::Builder::new().build())
        // Skip restoring VISIBLE so a previously hidden window never comes
        // back hidden — screen readers (VoiceOver/NVDA/JAWS) need the window
        // in the accessibility tree at launch.
        .plugin(
            tauri_plugin_window_state::Builder::new()
                .with_state_flags(restored_window_state_flags())
                .build(),
        )
        .plugin(tauri_plugin_autostart::Builder::new().build())
        .plugin(tauri_plugin_store::Builder::new().build())
        .plugin(tauri_plugin_os::init())
        .plugin(
            tauri_plugin_log::Builder::new()
                .level(tauri_plugin_log::log::LevelFilter::Info)
                .build(),
        )
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(pty::PtyState::default())
        .manage(proc::ProcState::default())
        .manage(shell::ShellState::default())
        .manage(secrets::SecretsState::default())
        .manage(lsp_install::LspInstallState::default())
        .manage(fs::watch::WatcherState::default())
        .manage({
            let registry = workspace::WorkspaceRegistry::default();
            workspace::bootstrap_registry(&registry);
            registry
        })
        .manage({
            let state = PendingLaunch::default();
            parse_initial_launch(&state);
            state
        })
        .manage(os_menu::RecentFolders::default())
        .manage(mcp::McpStatusRegistry::new())
        .manage(orchestration::commands::OrchestrationCommandState::default())
        .manage(orchestration::OrchestrationState::default())
        .manage(orchestration::hooks::HookRegistry::new())
        .on_menu_event(app_menu::handle_event)
        .setup(|app| {
            #[cfg(target_os = "windows")]
            log::info!("Windows WebView2 args: {}", windows_webview_args());
            altai::agent::runtime::init(app.handle().clone())?;
            // We use workspaceFallbackPath in frontend which depends on this
            workspace::grant_startup_asset_scope(app.handle());
            // Build the Dock/Jump List menu (recents fill in once the frontend
            // mirrors them via set_recent_folders).
            os_menu::init(app.handle());
            app_menu::install(app.handle(), &[])?;

            // Main window is `create: false` in tauri.conf so we can attach a
            // WKWebViewConfiguration that opts out of Apple Intelligence
            // Writing Tools / the Siri AI selection popover (must be set at
            // construction time — cannot be changed later).
            let window_cfg = app
                .config()
                .app
                .windows
                .iter()
                .find(|w| w.label == "main")
                .cloned()
                .ok_or("missing main window config in tauri.conf.json")?;
            #[cfg(target_os = "macos")]
            let builder = WebviewWindowBuilder::from_config(app.handle(), &window_cfg)?
                .with_webview_configuration(modules::macos_webview::config_without_writing_tools());
            #[cfg(not(target_os = "macos"))]
            let builder = WebviewWindowBuilder::from_config(app.handle(), &window_cfg)?;

            // Cursor-style frameless chrome on Windows. Opaque surface keeps
            // WebView2 compositing stable; users with a confirmed GPU-driver
            // issue can opt into software rendering with ALTAI_DISABLE_GPU=1.
            #[cfg(target_os = "windows")]
            let builder = {
                let builder = builder
                    .decorations(false)
                    .transparent(false)
                    .additional_browser_args(&windows_webview_args());
                // Pin the primary window to an explicit profile so secondary
                // IDE/settings webviews (which also set browser args) never
                // share WebView2's default user-data folder.
                match windows_webview_data_dir(app.handle(), "main") {
                    Some(dir) => builder.data_directory(dir),
                    None => builder,
                }
            };

            let window = builder.build()?;
            #[cfg(target_os = "windows")]
            {
                // Re-assert after WebView2 creates the HWND; window-state must
                // not restore an older decorated snapshot over frameless chrome.
                let _ = window.set_decorations(false);
            }
            let _ = focus_webview_window(&window);

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            pty::pty_open,
            pty::pty_write,
            pty::pty_resize,
            pty::pty_close,
            fs::tree::list_subdirs,
            fs::tree::fs_read_dir,
            fs::file::fs_read_file,
            fs::file::fs_extract_pdf,
            fs::file::fs_extract_pdf_path,
            fs::file::fs_write_file,
            fs::file::fs_stat,
            fs::file::fs_canonicalize,
            fs::mutate::fs_create_file,
            fs::mutate::fs_create_dir,
            fs::mutate::fs_rename,
            fs::mutate::fs_delete,
            fs::search::fs_search,
            fs::search::fs_list_files,
            fs::grep::fs_grep,
            fs::grep::fs_glob,
            fs::watch::fs_watch_start,
            fs::watch::fs_watch_stop,
            fs::isanagentignore::fs_get_isanagentignore,
            fs::isanagentignore::fs_set_isanagentignore,
            git::commands::git_resolve_repo,
            git::commands::git_panel_snapshot,
            git::commands::git_status,
            git::commands::git_diff,
            git::commands::git_diff_content,
            git::commands::git_stage,
            git::commands::git_unstage,
            git::commands::git_discard,
            git::commands::git_commit,
            git::commands::git_clone,
            git::commands::git_fetch,
            git::commands::git_pull_ff_only,
            git::commands::git_branches,
            git::commands::git_worktree_create,
            git::commands::git_worktree_remove,
            git::commands::git_worktree_apply,
            git::commands::git_checkout_branch,
            git::commands::git_create_branch,
            git::commands::git_push,
            git::commands::git_publish,
            git::commands::git_log,
            git::commands::git_show_commit,
            git::commands::git_commit_files,
            git::commands::git_commit_file_diff,
            git::commands::git_remote_url,
            // ALTAI — GitHub connect / identity / API proxy
            github::commands::github_device_start,
            github::commands::github_poll_token,
            github::commands::github_status,
            github::commands::github_disconnect,
            github::commands::github_api_request,
            github::commands::github_create_repo,
            github::external_sync::external_sync_github_issues,
            // ALTAI — Gmail multi-account adapter (package 074)
            gmail::commands::gmail_connect_account,
            gmail::commands::gmail_accounts_list,
            gmail::commands::gmail_disconnect_account,
            gmail::commands::gmail_sync_account,
            external_sync::external_object_resolve_conflict,
            // ALTAI — local project orchestration (ALTAI/IsanAgent runtime)
            orchestration::orchestration_snapshot,
            orchestration::orchestration_start,
            orchestration::orchestration_configure,
            orchestration::orchestration_pause,
            orchestration::orchestration_stop,
            orchestration::orchestration_reconcile,
            orchestration::orchestration_dispatch_result,
            orchestration::orchestration_record_terminal,
            orchestration::workflow::orchestration_workflow_load,
            orchestration::workflow::orchestration_workflow_save,
            orchestration::hooks::orchestration_hooks_inspect,
            orchestration::gardening::orchestration_gardening_tick,
            // ALTAI — orchestration v2 command wiring
            orchestration::commands::orchestration_quality_metrics,
            orchestration::commands::orchestration_readiness_scan,
            orchestration::commands::orchestration_context_pack,
            orchestration::commands::orchestration_plan_parse,
            orchestration::commands::orchestration_decision_record,
            orchestration::commands::orchestration_decisions_for_task,
            orchestration::commands::orchestration_graph_add_dependency,
            orchestration::commands::orchestration_graph_eligible,
            orchestration::commands::orchestration_graph_blocked_reason,
            orchestration::commands::orchestration_graph_topological_order,
            orchestration::commands::orchestration_profile_register,
            orchestration::commands::orchestration_profile_resolve,
            orchestration::commands::orchestration_profile_select,
            orchestration::commands::orchestration_profile_names,
            orchestration::commands::orchestration_hierarchy_add_child,
            orchestration::commands::orchestration_hierarchy_children,
            orchestration::commands::orchestration_hierarchy_descendants,
            orchestration::commands::orchestration_mailbox_post,
            orchestration::commands::orchestration_mailbox_deliver,
            orchestration::commands::orchestration_detect_file_conflicts,
            orchestration::commands::orchestration_notify,
            orchestration::commands::orchestration_notifications_drain,
            orchestration::commands::orchestration_credential_store,
            orchestration::commands::orchestration_credential_status,
            orchestration::commands::orchestration_credential_revoke,
            orchestration::commands::orchestration_check_gate,
            orchestration::commands::orchestration_review_evaluate,
            orchestration::commands::orchestration_usage_process,
            orchestration::commands::orchestration_usage_should_stop,
            orchestration::commands::orchestration_detect_overlaps,
            orchestration::commands::orchestration_gardening_scan,
            orchestration::commands::orchestration_session_analyze,
            orchestration::commands::orchestration_playbook_propose,
            orchestration::commands::orchestration_support_bundle,
            orchestration::commands::orchestration_schema_version,
            shell::shell_run_command,
            shell::shell_session_open,
            shell::shell_session_run,
            shell::shell_session_close,
            shell::shell_bg_spawn,
            shell::shell_bg_logs,
            shell::shell_bg_kill,
            shell::shell_bg_list,
            workspace::wsl_list_distros,
            workspace::wsl_default_distro,
            workspace::wsl_home,
            workspace::workspace_authorize,
            workspace::workspace_authorize_opened,
            workspace::workspace_revoke_opened,
            workspace::workspace_current_dir,
            get_pending_launches,
            env_get_flag,
            renderer_ready,
            open_with,
            open_settings_window,
            open_studio_window,
            focus_agent_window,
            // ALTAI — OS taskbar/Dock menu: new window + recent folders
            os_menu::open_new_window,
            os_menu::set_recent_folders,
            // ALTAI — native child-webview tabs
            webview::webview_create,
            webview::webview_set_bounds,
            webview::webview_close,
            secrets::secrets_get,
            secrets::secrets_set,
            secrets::secrets_delete,
            secrets::secrets_get_all,
            // ALTAI — Work OS (Milestone 1)
            work::work_create,
            work::work_list,
            work::work_children,
            work::work_inbox_list,
            work::work_get,
            work::work_transition,
            work::work_start,
            work::work_start_attempt,
            work::work_attempts,
            work::work_events,
            work::work_runs,
            work::work_events_recent,
            work::work_usage_recent,
            routines::routines_list,
            work::agent_list,
            work::agent_create,
            work::agent_transition,
            work::agent_set_reporting,
            work::work_attempt_bind,
            work::work_attempt_finish,
            work::work_attempt_reconcile,
            work::work_ready_for_review,
            work::work_review,
            work_import::work_legacy_import_preview,
            // ALTAI — Work OS control protocol (CP-08-45)
            control_plane_flags::control_plane_scheduling_authority,
            control_protocol::control_protocol_negotiate,
            control_protocol::control_protocol_execute,
            net::lm_ping,
            net::ai_http_request,
            net::ai_http_stream,
            // ALTAI — notebook execution
            notebook::notebook_execute_cell,
            // ALTAI — generic stdio process (LSP/MCP servers)
            proc::proc_spawn,
            proc::proc_stdin_write,
            proc::proc_kill,
            proc::proc_home_dir,
            proc::proc_which,
            // ALTAI — MCP server configuration and agent tool bridge
            mcp::mcp_get_servers,
            mcp::mcp_save_servers,
            mcp::mcp_probe_server,
            mcp::mcp_server_status,
            // ALTAI — managed LSP installer (Phase 1: rust-analyzer working;
            // TS/Python/Go stubbed until Phase 4 lands bundled Node + Go detect)
            lsp_install::lsp_registry_list,
            lsp_install::lsp_registry_get,
            lsp_install::lsp_install_status,
            lsp_install::lsp_install_run,
            lsp_install::lsp_install_cancel,
            lsp_install::lsp_install_uninstall,
            // ALTAI — İsanAgent commands
            agent_commands::agent_start,
            agent_commands::agent_send,
            agent_commands::agent_compact,
            agent_commands::agent_approve,
            agent_commands::agent_cancel,
            agent_commands::agent_steer,
            agent_commands::agent_list_sessions,
            agent_commands::agent_get_session_messages,
            agent_commands::agent_replay_events,
            agent_commands::agent_latest_run_replay_cursor,
            agent_commands::agent_truncate_after_user_message,
            agent_commands::agent_list_notifications,
            agent_commands::agent_notification_mark_seen,
            agent_commands::agent_notification_resolve,
            agent_commands::agent_list_background_jobs,
            agent_commands::agent_background_job_dismiss,
            agent_commands::agent_list_clarification_tickets,
            agent_commands::agent_clarification_ticket_dismiss,
            agent_commands::agent_clarification_ticket_reply,
            agent_commands::agent_list_automations,
            agent_commands::agent_automation_create,
            agent_commands::agent_automation_remove,
            agent_commands::agent_fetch_paper,
            agent_commands::checkpoint_list,
            agent_commands::checkpoint_restore,
            agent_commands::agent_install_skill,
            agent_commands::agent_list_skills,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn persisted_window_state_never_owns_platform_chrome() {
        let flags = restored_window_state_flags();

        assert!(!flags.contains(StateFlags::VISIBLE));
        assert!(!flags.contains(StateFlags::DECORATIONS));
        #[cfg(target_os = "windows")]
        assert!(!flags.contains(StateFlags::FULLSCREEN));
        assert!(flags.contains(StateFlags::SIZE));
        assert!(flags.contains(StateFlags::POSITION));
        assert!(flags.contains(StateFlags::MAXIMIZED));
    }

    #[test]
    fn studio_window_url_encodes_folder_paths() {
        assert_eq!(studio_window_url(None), "index.html?mode=studio");
        assert_eq!(studio_window_url(Some("")), "index.html?mode=studio");
        assert_eq!(
            studio_window_url(Some("/Users/me/My Project")),
            "index.html?mode=studio&folder=/Users/me/My%20Project"
        );
        assert_eq!(
            studio_window_url(Some(r"C:\Users\me\repo")),
            "index.html?mode=studio&folder=C%3A%5CUsers%5Cme%5Crepo"
        );
    }

    #[test]
    fn webview_profile_labels_stay_path_safe() {
        assert_eq!(sanitize_webview_profile_label("studio"), "studio");
        assert_eq!(
            sanitize_webview_profile_label("main-abc123"),
            "main-abc123"
        );
        assert_eq!(
            sanitize_webview_profile_label(r"evil\..\path"),
            "evil____path"
        );
    }

    #[test]
    fn windows_webview_keeps_hardware_rendering_by_default() {
        let args = windows_webview_args_for(false);

        assert!(!args.contains("--disable-gpu"));
        assert!(!args.contains("--force-renderer-accessibility"));
    }

    #[test]
    fn windows_webview_software_rendering_is_opt_in() {
        assert!(windows_webview_args_for(true).contains("--disable-gpu"));
    }

    #[test]
    fn test_collect_launch_payloads() {
        let dir = tempdir().unwrap();
        let folder_path = dir.path().join("test_folder");
        fs::create_dir(&folder_path).unwrap();
        let file_path = dir.path().join("test_file.txt");
        fs::write(&file_path, "test").unwrap();

        let args = vec![
            "altai".to_string(),
            folder_path.to_string_lossy().to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        let payloads = collect_launch_payloads(args, None);
        // Canonicalization might fail in some CI environments if paths don't exist,
        // but here we created them.
        assert_eq!(payloads.len(), 2);

        let folder_payload = payloads.iter().find(|p| p.kind == "folder").unwrap();
        assert_eq!(folder_payload.paths.len(), 1);
        assert!(folder_payload.paths[0]
            .replace("\\\\", "/")
            .contains("test_folder"));

        let file_payload = payloads.iter().find(|p| p.kind == "file").unwrap();
        assert_eq!(file_payload.paths.len(), 1);
        assert!(file_payload.paths[0]
            .replace("\\\\", "/")
            .contains("test_file.txt"));
    }

    #[test]
    fn test_collect_multi_file_payloads() {
        let dir = tempdir().unwrap();
        let file1 = dir.path().join("file1.txt");
        let file2 = dir.path().join("file2.txt");
        fs::write(&file1, "1").unwrap();
        fs::write(&file2, "2").unwrap();

        let args = vec![
            "altai".to_string(),
            file1.to_string_lossy().to_string(),
            file2.to_string_lossy().to_string(),
        ];

        let payloads = collect_launch_payloads(args, None);
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].kind, "multi_file");
        assert_eq!(payloads[0].paths.len(), 2);
    }
}
