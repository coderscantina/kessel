// Release builds on Windows are GUI apps: no console window flashes up behind
// the app. Debug builds keep the console so `cargo run` still shows the
// tracing output that the fmt layer writes to stdout.
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod app;
mod assets;
mod aws;
mod bookmarks;
mod config;
mod db;
mod dragout;
mod edit;
mod errors;
mod i18n;
mod keymap;
mod localfs;
mod logs;
mod mcp;
mod menu;
mod oauth;
mod provider;
mod queue;
mod s3;
mod secrets;
// The sftp transport is unix-only (see Cargo.toml); elsewhere a stub module
// with the same signatures stands in, so call sites stay `cfg`-free.
#[cfg_attr(not(unix), path = "sftp_unsupported.rs")]
mod sftp;
mod sso;
mod sync;
mod theme;
mod ui;
mod update;
mod zed;

use anyhow::Result;
use gpui::{
    px, size, App, AppContext as _, Bounds, TitlebarOptions, WindowBounds,
    WindowOptions,
};
use gpui_component::{Root, WindowExt as _};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::EnvFilter;

use app::{AppState, KesselApp};

fn main() -> Result<()> {
    let data_dir = or_fatal(config::data_dir(), "kessel could not open its data directory.")?;
    // A damaged config.json5 must not keep the app from opening: fall back to
    // the defaults and tell the user in the window once it is up.
    let (config, config_warning) = match config::load() {
        Ok(config) => (config, None),
        Err(error) => (
            config::defaults(),
            Some(format!(
                "config.json5 could not be read, so the defaults are in use: {error:#}"
            )),
        ),
    };
    i18n::init(&config.ui.locale);

    // Reloadable filter so config.json5 edits to `logging.filter` apply
    // live (see app::watch_config); RUST_LOG still wins at startup.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.filter));
    let (filter, filter_handle) = tracing_subscriber::reload::Layer::new(filter);
    let log_file = logs::RotatingFile::open(&data_dir.join("logs"))
        .inspect_err(|error| eprintln!("kessel: cannot open the log file: {error}"))
        .ok()
        .map(|file| {
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(file))
        });
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(log_file)
        // In-app capture for the bottom log panel.
        .with(logs::layer())
        .init();
    config::LOG_FILTER.set(filter_handle).ok();
    logs::install_panic_hook();
    if let Some(warning) = &config_warning {
        tracing::warn!("{warning}");
    }

    // Clear anything a previous in-app update had to leave behind (Windows
    // can't delete a running executable, so the old one is swept here).
    update::sweep_leftovers();

    // Bootstrap async state (DB, migrations, crash recovery) on the app-owned
    // tokio runtime; it stays alive in AppState for all later I/O work.
    let runtime = tokio::runtime::Runtime::new()?;
    let booted = runtime.block_on(async {
        let pool = db::connect(&data_dir).await?;
        let requeued_jobs = db::requeue_interrupted(&pool).await?;
        let sites = db::list_sites(&pool).await?;
        let folders = db::list_folders(&pool).await?;
        let spaces = db::list_spaces(&pool).await?;
        let tags = db::list_tags(&pool).await?;
        let open_tabs = db::list_open_tabs(&pool).await?;
        anyhow::Ok((pool, sites, folders, spaces, tags, open_tabs, requeued_jobs))
    });
    let (db, sites, folders, spaces, tags, open_tabs, requeued_jobs) = match booted {
        Ok(booted) => booted,
        Err(error) => {
            tracing::error!(error = format!("{error:#}"), "startup failed");
            show_fatal(&format!(
                "kessel could not open its database.\n\n{error:#}\n\nDatabase: {}\nBackup from before the last upgrade: {}\nLog: {}",
                data_dir.join("kessel.db").display(),
                data_dir.join("kessel.db.bak").display(),
                data_dir.join("logs").join("kessel.log").display(),
            ));
            return Err(error);
        }
    };
    let queue = queue::start(
        &runtime,
        db.clone(),
        config.transfers.clone(),
        config.layers.clone(),
    );
    // Optional embedded MCP server (off unless config.mcp.enabled); shares the
    // same pool and queue handle as the GUI. Config changes apply on restart.
    mcp::start(
        &runtime,
        &config.mcp,
        db.clone(),
        queue.clone(),
        config.layers.clone(),
    );
    let state = AppState {
        config,
        config_epoch: 0,
        db,
        sites,
        folders,
        active_space: spaces.first().map(|space| space.id).unwrap_or(1),
        spaces,
        tags,
        open_tabs,
        requeued_jobs,
        queue,
        runtime,
    };
    tracing::info!(
        sites = state.sites.len(),
        requeued = state.requeued_jobs,
        "kessel state loaded"
    );

    let application = gpui_platform::application().with_assets(assets::Assets);
    // macOS keeps the app alive without windows: a Dock click brings one back.
    application.on_reopen(|cx| {
        if cx.windows().is_empty() {
            open_main_window(cx, None);
            cx.activate(true);
        }
    });
    application.run(move |cx: &mut App| {
        app::init(cx);
        // Graceful queue shutdown: running workers checkpoint their offsets
        // and settle partial writes before the process exits (bounded so a
        // hung backend can't block quitting; crash recovery covers the rest).
        cx.on_app_quit({
            let queue = state.queue.clone();
            move |cx| {
                edit::end_sessions();
                let ack = queue.shutdown();
                let deadline = cx
                    .background_executor()
                    .timer(std::time::Duration::from_secs(5));
                async move {
                    smol::future::race(async { ack.await.ok(); }, deadline).await;
                }
            }
        })
        .detach();
        theme::init(&state.config.ui, cx);
        ui::file_icons::FileIcons::init(&state.config.ui, cx);
        keymap::init(cx);
        menu::install(cx);
        cx.set_global(state);
        app::watch_config(cx);

        // Quit without a window to receive it (the app menu on macOS).
        cx.on_action(|_: &app::Quit, cx| cx.quit());
        open_main_window(cx, config_warning);
        cx.activate(true);
    });

    Ok(())
}

fn open_main_window(cx: &mut App, warning: Option<String>) {
    let bounds = Bounds::centered(None, size(px(1200.), px(800.)), cx);
    let window = cx
        .open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                // Transparent titlebar: the app draws its own header row
                // (gpui_component::TitleBar) while macOS keeps the native
                // traffic lights, positioned by title_bar_options().
                titlebar: Some(TitlebarOptions {
                    title: Some("kessel".into()),
                    ..gpui_component::TitleBar::title_bar_options()
                }),
                window_min_size: Some(size(px(640.), px(400.))),
                ..Default::default()
            },
            |window, cx| {
                let view = cx.new(|cx| KesselApp::new(window, cx));
                // gpui-component requires its Root as the window's first layer.
                cx.new(|cx| Root::new(view, window, cx))
            },
        )
        .expect("failed to open window");
    if let Some(warning) = warning {
        window
            .update(cx, |_, window, cx| window.push_notification(warning, cx))
            .ok();
    }
}

/// Unwrap a startup result, showing the failure in a dialog first: release
/// builds on Windows have no console to print it to.
fn or_fatal<T>(result: Result<T>, headline: &str) -> Result<T> {
    result.inspect_err(|error| show_fatal(&format!("{headline}\n\n{error:#}")))
}

/// Blocking native error dialog for failures before any window exists.
fn show_fatal(message: &str) {
    eprintln!("kessel: {message}");
    #[cfg(target_os = "macos")]
    let status = std::process::Command::new("osascript")
        .args([
            "-e",
            "display alert \"kessel failed to start\" message (system attribute \"KESSEL_FATAL\") as critical",
        ])
        .env("KESSEL_FATAL", message)
        .status();
    #[cfg(target_os = "windows")]
    let status = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-WindowStyle",
            "Hidden",
            "-Command",
            "Add-Type -AssemblyName PresentationFramework; \
             [void][System.Windows.MessageBox]::Show($env:KESSEL_FATAL, 'kessel failed to start', 'OK', 'Error')",
        ])
        .env("KESSEL_FATAL", message)
        .status();
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let status: std::io::Result<()> = Ok(());
    let _ = status;
}
