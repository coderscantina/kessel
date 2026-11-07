//! Command bar (cmd-k): a fuzzy palette over every app command, bookmark and
//! space. Commands dispatch the same gpui actions the keymap and menu bar
//! use; bookmarks open like a sidebar click; spaces switch like ctrl-N.

use gpui::prelude::*;
use gpui::{
    div, px, uniform_list, Action, AnyElement, App, Context, Entity, EventEmitter, FocusHandle,
    Focusable, KeyDownEvent, ScrollStrategy, SharedString, UniformListScrollHandle, Window,
};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{h_flex, v_flex, ActiveTheme as _, Icon, IconName, Sizable as _};

use crate::app::AppState;
use crate::i18n::{t, tf};
use crate::bookmarks::{parse_quick_url, QUICK_CONNECT_SPACE};
use crate::queue::LOCAL_SITE_ID;
use crate::ui::{bookmark_color, bookmark_icon};

/// What the host should do when an entry is picked.
#[derive(Clone)]
pub enum RunRequest {
    /// Dispatch a registered action by name through the focus chain.
    Action(&'static str),
    /// Open a bookmark (pinned tab semantics).
    Bookmark(i64),
    /// Switch to a space.
    Space(i64),
    /// Quick connect to a URL typed into the bar.
    QuickConnect(String),
}

pub enum CommandBarEvent {
    Dismiss,
    Run(RunRequest),
}

struct Entry {
    label: SharedString,
    /// Right-aligned keystroke hint (first keymap binding, if any).
    keystroke: Option<SharedString>,
    icon: Icon,
    run: RunRequest,
}

/// (i18n label key, action name) for every palette command. The action is
/// dispatched through the focused view, exactly like its keyboard shortcut.
const COMMANDS: &[(&str, &str)] = &[
    ("cmdbar.command.quick_connect", "workspace::QuickConnect"),
    ("cmdbar.command.new_tab", "workspace::NewTab"),
    ("cmdbar.command.close_tab", "workspace::CloseTab"),
    ("cmdbar.command.next_tab", "workspace::NextTab"),
    ("cmdbar.command.prev_tab", "workspace::PrevTab"),
    ("cmdbar.command.toggle_sidebar", "workspace::ToggleSidebar"),
    (
        "cmdbar.command.toggle_transfers",
        "workspace::ToggleTransfers",
    ),
    ("cmdbar.command.toggle_logs", "workspace::ToggleLogs"),
    ("cmdbar.command.layout_single", "workspace::LayoutSingle"),
    ("cmdbar.command.layout_split_h", "workspace::LayoutSplitH"),
    ("cmdbar.command.layout_split_v", "workspace::LayoutSplitV"),
    (
        "cmdbar.command.focus_other_pane",
        "workspace::FocusOtherPane",
    ),
    ("cmdbar.command.bookmarks", "workspace::OpenBookmarks"),
    ("cmdbar.command.save_as_site", "workspace::SaveAsSite"),
    ("cmdbar.command.preferences", "workspace::OpenPreferences"),
    (
        "cmdbar.command.theme_browser",
        "workspace::OpenThemeBrowser",
    ),
    ("cmdbar.command.synchronize", "workspace::SyncPanes"),
    ("cmdbar.command.about", "workspace::About"),
    (
        "cmdbar.command.check_updates",
        "workspace::CheckForUpdates",
    ),
    ("cmdbar.command.refresh", "file_pane::Refresh"),
    ("cmdbar.command.go_up", "file_pane::GoUp"),
    ("cmdbar.command.go_back", "file_pane::GoBack"),
    ("cmdbar.command.go_forward", "file_pane::GoForward"),
    ("cmdbar.command.new_folder", "file_pane::NewFolder"),
    ("cmdbar.command.toggle_hidden", "file_pane::ToggleHidden"),
    ("cmdbar.command.toggle_tree", "file_pane::ToggleTree"),
    ("cmdbar.command.search", "file_pane::Search"),
    ("cmdbar.command.quick_look", "file_pane::QuickLook"),
    ("cmdbar.command.edit_path", "file_pane::EditPath"),
];

pub struct CommandBar {
    input: Entity<InputState>,
    entries: Vec<Entry>,
    /// Entries built at open; anything past this index is the dynamic
    /// "Connect to <url>" entry for a query that parses as a URL.
    base_len: usize,
    /// Indices into `entries`, filtered + ranked by the current query.
    filtered: Vec<usize>,
    selected: usize,
    scroll_handle: UniformListScrollHandle,
}

impl EventEmitter<CommandBarEvent> for CommandBar {}

impl Focusable for CommandBar {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.input.focus_handle(cx)
    }
}

impl CommandBar {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| InputState::new(window, cx).placeholder(t("cmdbar.placeholder")));
        cx.subscribe_in(
            &input,
            window,
            |this, _, event: &InputEvent, _, cx| match event {
                InputEvent::Change => this.refilter(cx),
                InputEvent::PressEnter { .. } => this.run_selected(cx),
                _ => {}
            },
        )
        .detach();

        let mut entries = Vec::new();
        for (key, action_name) in COMMANDS {
            let keystroke = cx
                .build_action(action_name, None)
                .ok()
                .and_then(|action| first_binding(action.as_ref(), window));
            entries.push(Entry {
                label: t(key),
                keystroke,
                icon: Icon::new(IconName::SquareTerminal),
                run: RunRequest::Action(action_name),
            });
        }
        let state = cx.global::<AppState>();
        for site in &state.sites {
            if site.id == LOCAL_SITE_ID || site.space_id == QUICK_CONNECT_SPACE {
                continue;
            }
            let mut icon = Icon::new(bookmark_icon(site.icon.as_deref(), IconName::Globe));
            if let Some(color) = bookmark_color(site.color.as_deref()) {
                icon = icon.text_color(color);
            }
            entries.push(Entry {
                label: tf("cmdbar.connect_prefix", &[("name", &site.name)]),
                keystroke: None,
                icon,
                run: RunRequest::Bookmark(site.id),
            });
        }
        for space in &state.spaces {
            let mut icon = Icon::new(bookmark_icon(space.icon.as_deref(), IconName::Frame));
            if let Some(color) = bookmark_color(space.color.as_deref()) {
                icon = icon.text_color(color);
            }
            entries.push(Entry {
                label: tf("cmdbar.space_prefix", &[("name", &space.name)]),
                keystroke: None,
                icon,
                run: RunRequest::Space(space.id),
            });
        }

        let filtered = (0..entries.len()).collect();
        Self {
            input,
            base_len: entries.len(),
            entries,
            filtered,
            selected: 0,
            scroll_handle: UniformListScrollHandle::new(),
        }
    }

    fn refilter(&mut self, cx: &mut Context<Self>) {
        let raw = self.input.read(cx).value().trim().to_string();
        self.entries.truncate(self.base_len);
        let query = raw.to_lowercase();
        if query.is_empty() {
            self.filtered = (0..self.entries.len()).collect();
        } else {
            let mut scored: Vec<(i64, usize)> = self
                .entries
                .iter()
                .enumerate()
                .filter_map(|(ix, entry)| {
                    fuzzy_score(&entry.label.to_lowercase(), &query).map(|score| (score, ix))
                })
                .collect();
            scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            self.filtered = scored.into_iter().map(|(_, ix)| ix).collect();
        }
        // A query that parses as a quick-connect URL ranks first.
        if let Ok(target) = parse_quick_url(&raw) {
            self.entries.push(Entry {
                label: tf("cmdbar.connect_url", &[("url", &target.name)]),
                keystroke: None,
                icon: Icon::default().path("icons/zap.svg"),
                run: RunRequest::QuickConnect(raw),
            });
            self.filtered.insert(0, self.entries.len() - 1);
        }
        self.selected = 0;
        self.scroll_handle
            .scroll_to_item(self.selected, ScrollStrategy::Center);
        cx.notify();
    }

    fn run_selected(&mut self, cx: &mut Context<Self>) {
        if let Some(&ix) = self.filtered.get(self.selected) {
            cx.emit(CommandBarEvent::Run(self.entries[ix].run.clone()));
        }
    }

    fn move_selection(&mut self, delta: i64, cx: &mut Context<Self>) {
        if self.filtered.is_empty() {
            return;
        }
        let len = self.filtered.len() as i64;
        self.selected = ((self.selected as i64 + delta).rem_euclid(len)) as usize;
        self.scroll_handle
            .scroll_to_item(self.selected, ScrollStrategy::Center);
        cx.notify();
    }

    fn on_key_down(&mut self, event: &KeyDownEvent, _: &mut Window, cx: &mut Context<Self>) {
        match event.keystroke.key.as_str() {
            "escape" => {
                cx.stop_propagation();
                cx.emit(CommandBarEvent::Dismiss);
            }
            "down" => {
                cx.stop_propagation();
                self.move_selection(1, cx);
            }
            "up" => {
                cx.stop_propagation();
                self.move_selection(-1, cx);
            }
            _ => {}
        }
    }

    fn render_row(&self, position: usize, entry_ix: usize, cx: &mut Context<Self>) -> AnyElement {
        let entry = &self.entries[entry_ix];
        let is_selected = position == self.selected;
        h_flex()
            .id(("cmd-row", position))
            .h(px(ROW_HEIGHT))
            .gap_2()
            .px_2()
            .rounded_md()
            .cursor_pointer()
            .text_sm()
            .when(is_selected, |el| el.bg(cx.theme().list_active))
            .when(!is_selected, |el| {
                el.hover(|style| style.bg(cx.theme().list_hover))
            })
            .on_mouse_move(cx.listener(move |this, _, _, cx| {
                if this.selected != position {
                    this.selected = position;
                    cx.notify();
                }
            }))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.selected = position;
                this.run_selected(cx);
            }))
            .child(entry.icon.clone().small())
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(entry.label.clone()),
            )
            .children(entry.keystroke.clone().map(|keys| {
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(keys)
            }))
            .into_any_element()
    }
}

/// Row height of the result list. The list gets an explicit height from it:
/// a uniform list inside an auto-height popover collapses to zero otherwise.
const ROW_HEIGHT: f32 = 28.;
const VISIBLE_ROWS: usize = 12;

impl Render for CommandBar {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let row_count = self.filtered.len();

        v_flex()
            .key_context("CommandBar")
            .on_key_down(cx.listener(Self::on_key_down))
            .w(px(560.))
            .max_h(px(420.))
            .p_2()
            .gap_2()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().popover)
            .text_color(cx.theme().popover_foreground)
            .shadow_lg()
            .child(Input::new(&self.input))
            .when(row_count > 0, |el| {
                el.child(
                    uniform_list(
                        "cmd-list",
                        row_count,
                        cx.processor(|this, range: std::ops::Range<usize>, _, cx| {
                            range
                                .map(|position| {
                                    this.render_row(position, this.filtered[position], cx)
                                })
                                .collect()
                        }),
                    )
                    .track_scroll(&self.scroll_handle)
                    .h(px(ROW_HEIGHT * row_count.min(VISIBLE_ROWS) as f32)),
                )
            })
            .when(row_count == 0, |el| {
                el.child(
                    div()
                        .px_2()
                        .py_1()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child(t("cmdbar.no_results")),
                )
            })
    }
}

/// First keymap binding for an action, rendered as "cmd-t"-style text.
fn first_binding(action: &dyn Action, window: &Window) -> Option<SharedString> {
    let binding = window.bindings_for_action(action).into_iter().next()?;
    let keys = binding
        .keystrokes()
        .iter()
        .map(|keystroke| keystroke.inner().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    (!keys.is_empty()).then(|| keys.into())
}

/// Subsequence fuzzy match: all query chars must appear in order; scoring
/// prefers consecutive runs and word starts. `None` = no match.
fn fuzzy_score(haystack: &str, query: &str) -> Option<i64> {
    let hay: Vec<char> = haystack.chars().collect();
    let mut score = 0i64;
    let mut hay_ix = 0usize;
    let mut previous_hit: Option<usize> = None;
    for ch in query.chars() {
        if ch == ' ' {
            continue;
        }
        let found = (hay_ix..hay.len()).find(|&ix| hay[ix] == ch)?;
        score += 1;
        if previous_hit == Some(found.wrapping_sub(1)) {
            score += 3; // consecutive
        }
        if found == 0 || !hay[found - 1].is_alphanumeric() {
            score += 2; // word start
        }
        previous_hit = Some(found);
        hay_ix = found + 1;
    }
    // Shorter labels rank higher on equal hits.
    Some(score * 100 - hay.len() as i64)
}

#[cfg(test)]
mod tests {
    use super::fuzzy_score;

    #[test]
    fn fuzzy_prefers_word_starts_and_runs() {
        assert!(fuzzy_score("toggle sidebar", "tsb").is_some());
        assert!(fuzzy_score("toggle sidebar", "xyz").is_none());
        let exact = fuzzy_score("new tab", "new").unwrap();
        let scattered = fuzzy_score("novel web", "new").unwrap();
        assert!(exact > scattered);
    }
}
