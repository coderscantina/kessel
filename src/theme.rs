//! Theme wiring, Zed-compatible: themes are collected from
//! `<data_dir>/themes/*.json` (native gpui-component `ThemeSet` *or* Zed theme
//! files — e.g. anything from a Zed theme extension like `material-dark`) and
//! from unpacked Zed extensions in `<data_dir>/extensions/*/themes/*.json`.
//! The config picks a mode (light / dark / system) plus optional named themes
//! per appearance via `ui.light_theme` / `ui.dark_theme`.
//!
//! Both directories are watched; edits hot-reload themes *and* icon themes
//! (see `ui::file_icons`).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use gpui::{App, SharedString};
use gpui_component::{Theme, ThemeConfig, ThemeMode};
use notify::Watcher as _;

use crate::config::{self, UiConfig};
use crate::ui::file_icons::FileIcons;
use crate::zed;

pub fn init(ui: &UiConfig, cx: &mut App) {
    apply(ui, cx);
    watch(ui.clone(), cx);
}

/// All available user themes by name. Later sources win on name collision:
/// bare files in `themes/`, then extensions.
fn load_themes(ui: &UiConfig) -> HashMap<SharedString, Rc<ThemeConfig>> {
    let mut themes = HashMap::new();
    let Ok(data_dir) = config::data_dir() else {
        return themes;
    };

    let mut files = json_files(&data_dir.join("themes"));
    if let Ok(extensions) = fs::read_dir(data_dir.join("extensions")) {
        for extension in extensions.flatten() {
            files.extend(json_files(&extension.path().join("themes")));
        }
    }

    for file in files {
        let parsed = fs::read_to_string(&file)
            .map_err(anyhow::Error::from)
            .and_then(|json| zed::parse_themes(&json, &ui.theme_overrides));
        match parsed {
            Ok(configs) => {
                for config in configs {
                    themes.insert(config.name.clone(), Rc::new(config));
                }
            }
            Err(error) => {
                tracing::warn!(%error, file = %file.display(), "ignoring invalid theme file")
            }
        }
    }
    themes
}

/// Names of all installed user themes (bare `themes/` files plus extensions),
/// sorted, for the Preferences theme pickers.
pub fn installed_theme_names(ui: &UiConfig) -> Vec<SharedString> {
    let mut names: Vec<SharedString> = load_themes(ui).into_keys().collect();
    names.sort();
    names
}

fn json_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect()
}

pub fn apply(ui: &UiConfig, cx: &mut App) {
    let themes = load_themes(ui);
    let theme = Theme::global_mut(cx);
    if let Some(config) = ui.light_theme.as_deref().and_then(|name| themes.get(name)) {
        theme.light_theme = Rc::clone(config);
    }
    if let Some(config) = ui.dark_theme.as_deref().and_then(|name| themes.get(name)) {
        theme.dark_theme = Rc::clone(config);
    }

    match ui.theme.as_str() {
        "light" => Theme::change(ThemeMode::Light, None, cx),
        "dark" => Theme::change(ThemeMode::Dark, None, cx),
        // "system" (and anything else): follow the OS appearance.
        _ => Theme::sync_system_appearance(None, cx),
    }
}

/// Watch `themes/` and `extensions/` and hot-reload themes + icon themes on
/// any change.
fn watch(ui: UiConfig, cx: &mut App) {
    let Ok(data_dir) = config::data_dir() else {
        return;
    };
    let dirs = [data_dir.join("themes"), data_dir.join("extensions")];
    for dir in &dirs {
        fs::create_dir_all(dir).ok();
    }

    let (tx, rx) = smol::channel::bounded::<()>(16);
    let mut watcher = match notify::recommended_watcher(
        move |result: notify::Result<notify::Event>| {
            if let Ok(event) = result {
                if matches!(
                    event.kind,
                    notify::EventKind::Create(_)
                        | notify::EventKind::Modify(_)
                        | notify::EventKind::Remove(_)
                ) {
                    tx.try_send(()).ok();
                }
            }
        },
    ) {
        Ok(watcher) => watcher,
        Err(error) => {
            tracing::warn!(%error, "theme hot reload disabled: cannot create watcher");
            return;
        }
    };
    for dir in &dirs {
        if let Err(error) = watcher.watch(dir, notify::RecursiveMode::Recursive) {
            tracing::warn!(%error, dir = %dir.display(), "failed to watch");
        }
    }

    cx.spawn(async move |cx| {
        let _watcher = watcher; // keep watching for the app's lifetime
        while rx.recv().await.is_ok() {
            // Let the write burst settle, then coalesce queued events.
            cx.background_executor()
                .timer(Duration::from_millis(150))
                .await;
            while rx.try_recv().is_ok() {}

            cx.update(|cx| {
                tracing::info!("reloading themes and icon themes");
                // Read the live config so a hot-reloaded theme/icon-theme
                // selection isn't overridden by the startup snapshot.
                let ui = cx
                    .try_global::<crate::app::AppState>()
                    .map(|state| state.config.ui.clone())
                    .unwrap_or_else(|| ui.clone());
                apply(&ui, cx);
                FileIcons::reload(&ui, cx);
                cx.refresh_windows();
            });
        }
    })
    .detach();
}
