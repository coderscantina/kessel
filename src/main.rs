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
    px, size, App, AppContext as _, Application, Bounds, TitlebarOptions, WindowBounds,
    WindowOptions,
};
use gpui_component::Root;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::EnvFilter;

use app::{AppState, KesselApp};

fn main() -> Result<()> {
    let config = config::load()?;
    i18n::init(&config.ui.locale);

    // Reloadable filter so config.json5 edits to `logging.filter` apply
    // live (see app::watch_config); RUST_LOG still wins at startup.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.filter));
    let (filter, filter_handle) = tracing_subscriber::reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        // In-app capture for the bottom log panel.
        .with(logs::layer())
        .init();
    config::LOG_FILTER.set(filter_handle).ok();

    // Clear anything a previous in-app update had to leave behind (Windows
    // can't delete a running executable, so the old one is swept here).
    update::sweep_leftovers();

    // Bootstrap async state (DB, migrations, crash recovery) on the app-owned
    // tokio runtime; it stays alive in AppState for all later I/O work.
    let runtime = tokio::runtime::Runtime::new()?;
    let (db, sites, folders, spaces, tags, open_tabs, requeued_jobs) = runtime.block_on(async {
        let data_dir = config::data_dir()?;
        let pool = db::connect(&data_dir).await?;
        let requeued_jobs = db::requeue_interrupted(&pool).await?;
        let sites = db::list_sites(&pool).await?;
        let folders = db::list_folders(&pool).await?;
        let spaces = db::list_spaces(&pool).await?;
        let tags = db::list_tags(&pool).await?;
        let open_tabs = db::list_open_tabs(&pool).await?;
        anyhow::Ok((pool, sites, folders, spaces, tags, open_tabs, requeued_jobs))
    })?;
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

    Application::new().with_assets(assets::Assets).run(move |cx: &mut App| {
        app::init(cx);
        // Graceful queue shutdown: running workers checkpoint their offsets
        // and settle partial writes before the process exits (bounded so a
        // hung backend can't block quitting; crash recovery covers the rest).
        cx.on_app_quit({
            let queue = state.queue.clone();
            move |cx| {
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

        let bounds = Bounds::centered(None, size(px(1200.), px(800.)), cx);
        cx.open_window(
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
        cx.activate(true);
    });

    Ok(())
}
