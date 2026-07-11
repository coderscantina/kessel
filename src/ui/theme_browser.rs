//! Theme & icon-theme browser: search the public Zed extensions registry,
//! then install a theme with one click. Installed extensions unpack into
//! `<data_dir>/extensions/<id>/`, exactly where `crate::theme` and
//! `crate::ui::file_icons` look — and the notify watcher there hot-loads them,
//! so a fresh download applies without a restart. Users then pick the theme in
//! Preferences → Appearance.
//!
//! Networking runs on the app's tokio runtime (`AppState.runtime`); results
//! marshal back onto the gpui executor via `cx.spawn` + `Entity::update`, the
//! same pattern the workspace uses for its SQLite work.

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result};
use gpui::prelude::*;
use gpui::{
    div, px, uniform_list, AnyElement, App, Context, Entity, FocusHandle, Focusable, SharedString,
    UniformListScrollHandle, Window,
};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{h_flex, v_flex, ActiveTheme as _, Disableable as _, Sizable as _};

use crate::app::AppState;
use crate::config;
use crate::i18n::{t, tf};

/// Which kind of extension the browser is showing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Themes,
    IconThemes,
}

impl Kind {
    /// The value the Zed API reports in an extension's `provides` array.
    fn provides(self) -> &'static str {
        match self {
            Kind::Themes => "themes",
            Kind::IconThemes => "icon-themes",
        }
    }
}

/// One registry entry, ready to render.
#[derive(Clone)]
struct ExtRow {
    id: String,
    name: SharedString,
    authors: SharedString,
    downloads: u64,
}

pub struct ThemeBrowser {
    input: Entity<InputState>,
    kind: Kind,
    rows: Vec<ExtRow>,
    /// Extension ids present under `<data_dir>/extensions/`.
    installed: HashSet<String>,
    /// Ids with an install/uninstall in flight (button shows a spinner).
    busy: HashSet<String>,
    loading: bool,
    error: Option<SharedString>,
    /// Bumped on every new query/tab switch; async results whose generation is
    /// stale are dropped, giving debounce + last-write-wins for free.
    generation: u64,
    scroll_handle: UniformListScrollHandle,
}

impl Focusable for ThemeBrowser {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.input.focus_handle(cx)
    }
}

impl ThemeBrowser {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input =
            cx.new(|cx| InputState::new(window, cx).placeholder(t("themes.search_placeholder")));
        cx.subscribe_in(&input, window, |this, _, event: &InputEvent, _, cx| {
            if let InputEvent::Change = event {
                this.reload(cx);
            }
        })
        .detach();

        let mut this = Self {
            input,
            kind: Kind::Themes,
            rows: Vec::new(),
            installed: HashSet::new(),
            busy: HashSet::new(),
            loading: false,
            error: None,
            generation: 0,
            scroll_handle: UniformListScrollHandle::new(),
        };
        this.refresh_installed();
        this.reload(cx);
        this
    }

    fn set_kind(&mut self, kind: Kind, cx: &mut Context<Self>) {
        if self.kind != kind {
            self.kind = kind;
            self.reload(cx);
        }
    }

    /// Rescan the extensions dir so install/uninstall state is accurate.
    fn refresh_installed(&mut self) {
        self.installed.clear();
        if let Ok(dir) = config::data_dir() {
            if let Ok(entries) = fs::read_dir(dir.join("extensions")) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            self.installed.insert(name.to_string());
                        }
                    }
                }
            }
        }
    }

    /// Kick off a debounced fetch for the current query + tab.
    fn reload(&mut self, cx: &mut Context<Self>) {
        self.generation += 1;
        let generation = self.generation;
        let query = self.input.read(cx).value().trim().to_string();
        let kind = self.kind;
        self.loading = true;
        self.error = None;
        cx.notify();

        cx.spawn(async move |this, cx| {
            // Debounce: let typing settle before hitting the network.
            cx.background_executor()
                .timer(Duration::from_millis(250))
                .await;
            // Spawn the request on the app runtime, but only if this is still
            // the newest query (a later keystroke would have bumped it).
            let task = this
                .update(cx, |this, cx| {
                    if this.generation != generation {
                        return None;
                    }
                    Some(cx.global::<AppState>().runtime.spawn(fetch(query, kind)))
                })
                .ok()
                .flatten();
            let Some(task) = task else { return };

            let result = match task.await {
                Ok(result) => result,
                Err(join) => Err(anyhow::anyhow!("theme fetch task failed: {join}")),
            };
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                this.loading = false;
                match result {
                    Ok(rows) => {
                        this.rows = rows;
                        this.error = None;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "theme registry fetch failed");
                        this.rows.clear();
                        this.error = Some(t("themes.error_network"));
                    }
                }
                this.refresh_installed();
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn install(&mut self, id: String, cx: &mut Context<Self>) {
        if self.busy.contains(&id) {
            return;
        }
        self.busy.insert(id.clone());
        cx.notify();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(install_extension(id.clone()));
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join) => Err(anyhow::anyhow!("install task failed: {join}")),
            };
            this.update(cx, |this, cx| {
                this.busy.remove(&id);
                match result {
                    Ok(()) => this.error = None,
                    Err(error) => {
                        tracing::warn!(%error, %id, "theme install failed");
                        this.error = Some(t("themes.error_install"));
                    }
                }
                this.refresh_installed();
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn uninstall(&mut self, id: String, cx: &mut Context<Self>) {
        if self.busy.contains(&id) {
            return;
        }
        self.busy.insert(id.clone());
        cx.notify();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(uninstall_extension(id.clone()));
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join) => Err(anyhow::anyhow!("uninstall task failed: {join}")),
            };
            this.update(cx, |this, cx| {
                this.busy.remove(&id);
                if let Err(error) = result {
                    tracing::warn!(%error, %id, "theme uninstall failed");
                    this.error = Some(t("themes.error_install"));
                }
                this.refresh_installed();
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn tab_button(&self, kind: Kind, label: SharedString, cx: &mut Context<Self>) -> AnyElement {
        let active = self.kind == kind;
        let id = match kind {
            Kind::Themes => "tab-themes",
            Kind::IconThemes => "tab-icon-themes",
        };
        let mut button = Button::new(id).label(label).small();
        button = if active { button.primary() } else { button.ghost() };
        button
            .on_click(cx.listener(move |this, _, _, cx| this.set_kind(kind, cx)))
            .into_any_element()
    }

    fn render_row(&self, ix: usize, cx: &mut Context<Self>) -> AnyElement {
        let row = self.rows[ix].clone();
        let muted = cx.theme().muted_foreground;
        let installed = self.installed.contains(&row.id);
        let busy = self.busy.contains(&row.id);

        let action: AnyElement = if installed {
            h_flex()
                .gap_2()
                .items_center()
                .child(
                    div()
                        .text_xs()
                        .text_color(cx.theme().success)
                        .child(t("themes.installed")),
                )
                .child({
                    let id = row.id.clone();
                    Button::new(("uninstall", ix))
                        .label(t("themes.uninstall"))
                        .ghost()
                        .danger()
                        .xsmall()
                        .loading(busy)
                        .disabled(busy)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.uninstall(id.clone(), cx)
                        }))
                })
                .into_any_element()
        } else {
            let id = row.id.clone();
            Button::new(("install", ix))
                .label(t("themes.install"))
                .primary()
                .xsmall()
                .loading(busy)
                .disabled(busy)
                .on_click(cx.listener(move |this, _, _, cx| this.install(id.clone(), cx)))
                .into_any_element()
        };

        h_flex()
            .id(("theme-row", ix))
            .py_1p5()
            .px_2()
            .gap_3()
            .items_center()
            .justify_between()
            .rounded_md()
            .hover(|style| style.bg(cx.theme().list_hover))
            .child(
                v_flex()
                    .gap_0p5()
                    .flex_1()
                    .min_w_0()
                    .child(div().text_sm().truncate().child(row.name.clone()))
                    .child(
                        div()
                            .text_xs()
                            .text_color(muted)
                            .truncate()
                            .child(tf(
                                "themes.row_meta",
                                &[
                                    ("authors", &row.authors),
                                    ("downloads", &format_count(row.downloads)),
                                ],
                            )),
                    ),
            )
            .child(div().flex_none().child(action))
            .into_any_element()
    }
}

impl Render for ThemeBrowser {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;
        let row_count = self.rows.len();

        v_flex()
            .w_full()
            .gap_3()
            .child(
                h_flex()
                    .gap_2()
                    .child(self.tab_button(Kind::Themes, t("themes.tab_themes"), cx))
                    .child(self.tab_button(Kind::IconThemes, t("themes.tab_icon_themes"), cx)),
            )
            .child(Input::new(&self.input).small())
            .child(
                v_flex()
                    .h(px(360.))
                    .child(if self.loading && row_count == 0 {
                        div()
                            .py_4()
                            .text_sm()
                            .text_color(muted)
                            .child(t("themes.loading"))
                            .into_any_element()
                    } else if let Some(error) = self.error.clone() {
                        div()
                            .py_4()
                            .text_sm()
                            .text_color(muted)
                            .child(error)
                            .into_any_element()
                    } else if row_count == 0 {
                        div()
                            .py_4()
                            .text_sm()
                            .text_color(muted)
                            .child(t("themes.empty"))
                            .into_any_element()
                    } else {
                        uniform_list(
                            "theme-list",
                            row_count,
                            cx.processor(|this, range: std::ops::Range<usize>, _, cx| {
                                range.map(|ix| this.render_row(ix, cx)).collect()
                            }),
                        )
                        .track_scroll(self.scroll_handle.clone())
                        .flex_1()
                        .min_h_0()
                        .into_any_element()
                    }),
            )
            .child(
                div()
                    .pt_2()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .text_xs()
                    .text_color(muted)
                    .child(t("themes.hint")),
            )
    }
}

/// Human-friendly download count (e.g. `12.3k`).
fn format_count(count: u64) -> SharedString {
    if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0).into()
    } else if count >= 1_000 {
        format!("{:.1}k", count as f64 / 1_000.0).into()
    } else {
        count.to_string().into()
    }
}

// --- Networking (runs on the app runtime) ---------------------------------

const API_BASE: &str = "https://api.zed.dev/extensions";
const NET_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(serde::Deserialize)]
struct ApiResponse {
    #[serde(default)]
    data: Vec<ApiExtension>,
}

#[derive(serde::Deserialize)]
struct ApiExtension {
    id: String,
    name: String,
    #[serde(default)]
    authors: Vec<String>,
    #[serde(default)]
    provides: Vec<String>,
    #[serde(default)]
    download_count: u64,
}

/// Query the registry and keep only extensions of the wanted kind, sorted by
/// popularity. An empty query returns the registry's popular default set.
async fn fetch(query: String, kind: Kind) -> Result<Vec<ExtRow>> {
    let client = reqwest::Client::builder()
        .timeout(NET_TIMEOUT)
        .build()
        .context("building http client")?;
    // `max_schema_version` is required — without it the API omits `provides`.
    let mut url = url::Url::parse(API_BASE).context("parsing registry url")?;
    url.query_pairs_mut()
        .append_pair("filter", &query)
        .append_pair("max_schema_version", "1");
    let response = client
        .get(url)
        .send()
        .await
        .context("requesting extension list")?
        .error_for_status()
        .context("extension list request rejected")?;
    let body = response.text().await.context("reading extension list")?;
    // The codebase parses JSON via serde_json from text (see oauth.rs).
    let parsed: ApiResponse = serde_json::from_str(&body).context("parsing extension list")?;

    let want = kind.provides();
    let mut rows: Vec<ExtRow> = parsed
        .data
        .into_iter()
        .filter(|ext| ext.provides.iter().any(|p| p == want))
        .map(|ext| ExtRow {
            name: ext.name.into(),
            authors: ext.authors.join(", ").into(),
            downloads: ext.download_count,
            id: ext.id,
        })
        .collect();
    rows.sort_by(|a, b| b.downloads.cmp(&a.downloads).then(a.name.cmp(&b.name)));
    Ok(rows)
}

/// Download `<id>` as a gzipped tar and unpack it into
/// `<data_dir>/extensions/<id>/`, dropping any entry whose path escapes the
/// target directory.
async fn install_extension(id: String) -> Result<()> {
    let dest = config::data_dir()?.join("extensions").join(&id);
    let url = format!("{API_BASE}/{id}/download");
    let client = reqwest::Client::builder()
        .timeout(NET_TIMEOUT)
        .build()
        .context("building http client")?;
    let bytes = client
        .get(&url)
        .send()
        .await
        .context("requesting extension archive")?
        .error_for_status()
        .context("extension archive request rejected")?
        .bytes()
        .await
        .context("reading extension archive")?;

    // Unpack into a fresh dir so a reinstall can't leave stale files behind.
    let _ = fs::remove_dir_all(&dest);
    fs::create_dir_all(&dest).context("creating extension dir")?;
    let decoder = flate2::read::GzDecoder::new(&bytes[..]);
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().context("reading tar")? {
        let mut entry = entry.context("reading tar entry")?;
        let entry_path = entry.path().context("tar entry path")?.into_owned();
        let Some(target) = sanitized_dest(&dest, &entry_path) else {
            tracing::warn!(path = %entry_path.display(), "skipping unsafe tar entry");
            continue;
        };
        if entry.header().entry_type().is_dir() {
            fs::create_dir_all(&target).context("creating entry dir")?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).context("creating entry parent")?;
            }
            entry.unpack(&target).context("unpacking entry")?;
        }
    }
    Ok(())
}

async fn uninstall_extension(id: String) -> Result<()> {
    let dest = config::data_dir()?.join("extensions").join(&id);
    if dest.exists() {
        fs::remove_dir_all(&dest).context("removing extension dir")?;
    }
    Ok(())
}

/// Resolve a tar entry path to a destination under `root`, or `None` if it
/// would escape (absolute path, `..`, or a drive prefix). The Zed archive
/// prefixes every entry with `./`, which normalizes away here.
fn sanitized_dest(root: &Path, entry: &Path) -> Option<PathBuf> {
    let mut cleaned = PathBuf::new();
    for component in entry.components() {
        match component {
            Component::Normal(part) => cleaned.push(part),
            Component::CurDir => {}
            // ParentDir / RootDir / Prefix all escape the target.
            _ => return None,
        }
    }
    if cleaned.as_os_str().is_empty() {
        return None;
    }
    Some(root.join(cleaned))
}

#[cfg(test)]
mod tests {
    use super::sanitized_dest;
    use std::path::{Path, PathBuf};

    #[test]
    fn strips_leading_dot_and_joins_under_root() {
        let root = Path::new("/data/extensions/my-theme");
        assert_eq!(
            sanitized_dest(root, Path::new("./themes/one.json")),
            Some(PathBuf::from("/data/extensions/my-theme/themes/one.json"))
        );
        assert_eq!(
            sanitized_dest(root, Path::new("extension.toml")),
            Some(PathBuf::from("/data/extensions/my-theme/extension.toml"))
        );
    }

    #[test]
    fn rejects_traversal_and_absolute_paths() {
        let root = Path::new("/data/extensions/my-theme");
        assert_eq!(sanitized_dest(root, Path::new("../evil")), None);
        assert_eq!(sanitized_dest(root, Path::new("./a/../../evil")), None);
        assert_eq!(sanitized_dest(root, Path::new("/etc/passwd")), None);
        // A bare "./" normalizes to nothing and is rejected.
        assert_eq!(sanitized_dest(root, Path::new("./")), None);
    }
}
