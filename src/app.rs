//! Root GPUI view: an Arc-style vertical tabs sidebar next to the active
//! tab's workspace (one or two file panes in single / split-horizontal /
//! split-vertical layout).
//!
//! Sidebar model, following Arc/Zen:
//! - **Bookmarks on top** act as pinned tabs: opening one lives on its
//!   bookmark row (highlight + close affordance), never in the tab list.
//! - **Ephemeral tabs** below a separator, then new-tab / new-connection.
//! - **Spaces** switch whole tab sets; the icons-only switcher sits at the
//!   very bottom. The session (spaces × tabs) persists to SQLite.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use gpui::prelude::*;
use gpui::{
    actions, canvas, div, px, AnyElement, App, Context, Corner, DispatchPhase, Entity, EntityId,
    FocusHandle, Focusable as _, Global, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent,
    NavigationDirection, SharedString, Window,
};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::menu::{ContextMenuExt as _, PopupMenuItem};
use gpui_component::popover::Popover;
use gpui_component::progress::Progress;
use gpui_component::resizable::{h_resizable, resizable_panel, v_resizable, ResizableState};
use gpui_component::slider::{Slider, SliderEvent, SliderState};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Disableable as _, Icon, IconName, Root, Selectable as _,
    Sizable as _, StyledExt as _, TitleBar, WindowExt as _,
};
use sqlx::SqlitePool;

use crate::config::KesselConfig;
use crate::db::{self, BookmarkFolder, OpenTabRow, Space};
use crate::provider::{LayerConfig, SiteProfile};
use crate::queue::{ConflictPolicy, JobState, NewJob, LOCAL_SITE_ID};
use crate::i18n::{t, tf};
use crate::ui::bookmarks::{BookmarkEvent, BookmarkManager};
use crate::ui::command_bar::{CommandBar, CommandBarEvent, RunRequest};
use crate::ui::file_pane::{DraggedEntries as DraggedRowEntries, FilePane, FsEntry, PaneEvent, PendingDragOut};
use crate::ui::logs::LogsPanel;
use crate::ui::transfers::{TransfersEvent, TransfersPanel};
use crate::ui::{bookmark_color, bookmark_icon};

actions!(
    workspace,
    [
        NewTab,
        CloseTab,
        NextTab,
        PrevTab,
        ToggleSidebar,
        ToggleTransfers,
        ToggleLogs,
        LayoutSingle,
        LayoutSplitH,
        LayoutSplitV,
        FocusOtherPane,
        OpenBookmarks,
        About,
        OpenPreferences,
        OpenThemeBrowser,
        ToggleCommandBar,
        SyncPanes,
        Help,
        CheckForUpdates,
        Quit
    ]
);

/// Switch to the nth space (1-based); `ctrl-1…9` by default.
#[derive(Clone, PartialEq, serde::Deserialize, schemars::JsonSchema, gpui::Action)]
#[action(namespace = workspace)]
pub struct SwitchSpace {
    pub index: usize,
}

/// Activate the nth tab of the current space (1-based); `cmd-1…9` by default.
#[derive(Clone, PartialEq, serde::Deserialize, schemars::JsonSchema, gpui::Action)]
#[action(namespace = workspace)]
pub struct ActivateTab {
    pub index: usize,
}

/// App-wide state, built in `main` before the UI starts.
pub struct AppState {
    pub config: KesselConfig,
    /// Bumped whenever `config` is replaced by the config.json5 hot-reload
    /// watcher; views observing the global compare it to re-apply values.
    pub config_epoch: u64,
    pub db: SqlitePool,
    pub sites: Vec<SiteProfile>,
    pub folders: Vec<BookmarkFolder>,
    pub spaces: Vec<Space>,
    /// Known connection tags (lowercase names + accent colors).
    pub tags: Vec<db::Tag>,
    /// The space currently shown, kept in sync by [`KesselApp`] so detached
    /// views (e.g. the bookmark editor) know where new bookmarks belong.
    pub active_space: i64,
    /// Persisted tab session from the previous run.
    pub open_tabs: Vec<OpenTabRow>,
    pub requeued_jobs: u64,
    /// Handle to the persistent transfer queue engine (see `crate::queue`).
    pub queue: crate::queue::QueueHandle,
    /// Dedicated I/O runtime — gpui has its own executor, so all OpenDAL and
    /// sqlx work is spawned here and results are sent back to the UI.
    pub runtime: tokio::runtime::Runtime,
}

impl Global for AppState {}

/// Connection profile for the machine's own filesystem, rooted at `/` (the
/// system drive on Windows — see `localfs`).
pub fn local_profile() -> SiteProfile {
    SiteProfile {
        id: LOCAL_SITE_ID,
        name: "Local".into(),
        scheme: "fs".into(),
        options: HashMap::from([("root".to_string(), crate::localfs::root())]),
        layers: LayerConfig::default(),
        ..Default::default()
    }
}

fn home_path() -> String {
    crate::localfs::home_browse_path()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PaneLayout {
    Single,
    SplitH,
    SplitV,
}

impl PaneLayout {
    fn as_str(self) -> &'static str {
        match self {
            PaneLayout::Single => "single",
            PaneLayout::SplitH => "split_h",
            PaneLayout::SplitV => "split_v",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "split_h" => PaneLayout::SplitH,
            "split_v" => PaneLayout::SplitV,
            _ => PaneLayout::Single,
        }
    }
}

/// One tab's browsing state: two panes (the second hidden in single layout),
/// the split orientation, and which pane the user touched last.
struct Workspace {
    panes: [Entity<FilePane>; 2],
    layout: PaneLayout,
    active: usize,
    split_h: Entity<ResizableState>,
    split_v: Entity<ResizableState>,
    linked_roots: Option<[(i64, String); 2]>,
}

fn linked_directory(root: &str, other_root: &str, path: &str) -> Option<String> {
    if path.split('/').any(|part| matches!(part, "." | "..")) {
        return None;
    }
    let relative = path.strip_prefix(root)?;
    Some(format!("{other_root}{relative}"))
}

enum TabContent {
    Panes(Workspace),
    /// "New connection" tab: the bookmark form as the full main pane.
    Connect(Entity<BookmarkManager>),
}

struct Tab {
    /// Stable in-session identity (indices shift on close/reorder).
    uid: u64,
    space_id: i64,
    /// Set when this is a bookmark ("pinned") tab: it renders on its
    /// bookmark row in the sidebar, not in the ephemeral tab list.
    bookmark_id: Option<i64>,
    /// User-given title (inline rename); `None` derives it live.
    custom_name: Option<String>,
    content: TabContent,
}

/// Which sidebar row is being renamed inline.
#[derive(Clone, Copy, PartialEq)]
enum RenameTarget {
    Tab(u64),
    Bookmark(i64),
}

/// Which fixed-pixel panel a divider drag is resizing.
#[derive(Clone, Copy, PartialEq)]
enum PanelKind {
    Sidebar,
    /// The shared bottom dock (Transfers / Logs), one height for both.
    Dock,
}

/// Which panel the shared bottom dock is showing. `None` = dock closed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BottomPanel {
    Transfers,
    Logs,
}

impl BottomPanel {
    fn label(self) -> SharedString {
        match self {
            BottomPanel::Transfers => t("dock.transfers"),
            BottomPanel::Logs => t("dock.logs"),
        }
    }

    fn icon_path(self) -> &'static str {
        match self {
            BottomPanel::Transfers => "icons/list-checks.svg",
            BottomPanel::Logs => "icons/square-terminal.svg",
        }
    }
}

/// An in-progress panel divider drag: pointer start (x for the sidebar,
/// y for the bottom panels) and the panel's size at drag start.
#[derive(Clone, Copy)]
struct PanelResize {
    kind: PanelKind,
    start_pos: f32,
    start_size: f32,
}

/// Drag payload for reordering tabs in the sidebar.
#[derive(Clone)]
struct DraggedTab(u64);

/// Drag payload for reordering bookmarks / moving them into folders.
#[derive(Clone)]
struct DraggedBookmark(i64);

struct SidebarDragGhost(SharedString);

impl Render for SidebarDragGhost {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .px_2()
            .py_0p5()
            .rounded_full()
            .bg(cx.theme().primary)
            .text_color(cx.theme().primary_foreground)
            .text_xs()
            .whitespace_nowrap()
            .child(self.0.clone())
    }
}

pub struct KesselApp {
    tabs: Vec<Tab>,
    /// uid of the active tab.
    active: u64,
    next_uid: u64,
    active_space: i64,
    /// Last active tab per space, so switching spaces restores position.
    space_active: HashMap<i64, u64>,
    /// Shared manager backing the "Bookmarks" dialog (list + editor).
    bookmarks: Entity<BookmarkManager>,
    transfers: Entity<TransfersPanel>,
    /// The global log bottom pane (see `crate::logs`).
    logs: Entity<LogsPanel>,
    /// Which panel the shared bottom dock shows, or `None` when closed. Both
    /// panels are global (across tabs and spaces) and mutually exclusive.
    bottom_dock: Option<BottomPanel>,
    /// Footer popover controls for the queue's global limits.
    parallel_slider: Entity<SliderState>,
    throttle_slider: Entity<SliderState>,
    collapsed_folders: HashSet<i64>,
    sidebar_open: bool,
    /// Sidebar / transfers / logs sizes are fixed pixel values from the
    /// config; a divider drag updates them directly.
    panel_resize: Option<PanelResize>,
    /// Inline rename of a sidebar row (tab or bookmark).
    rename: Option<(RenameTarget, Entity<InputState>)>,
    /// Fallback focus so Workspace-context keybindings (quit, tabs, panels)
    /// fire even when no pane or input holds focus.
    focus_handle: FocusHandle,
    /// Cmd-Q while transfers run: (first press, latest repeat) — key repeat
    /// keeps re-firing the action, and a sustained hold quits anyway.
    quit_hold: Option<(Instant, Instant)>,
    quit_dialog_open: bool,
    /// Set by `activate_tab`; the next render focuses the active pane.
    pending_pane_focus: bool,
    /// Debounces writes of panel sizes back into config.json5.
    ui_save_scheduled: bool,
    /// Last `AppState::config_epoch` this view applied (config hot reload).
    config_epoch_seen: u64,
    // "New space" popover form state.
    space_name_input: Entity<InputState>,
    space_icon: Option<String>,
    space_color: Option<String>,
    /// Command bar (cmd-k) overlay; rebuilt on every open so bookmarks and
    /// spaces are fresh.
    command_bar: Option<Entity<CommandBar>>,
    /// Newest release from the periodic update check, when it's newer than the
    /// running build; drives the footer indicator and the update dialog.
    available_update: Option<crate::update::UpdateInfo>,
    /// True while an update is downloading/installing (spinner + disabled button).
    update_installing: bool,
    /// Inline feedback shown in the update dialog (manual-install hint or error).
    update_error: Option<SharedString>,
    /// In-app menu bar, drawn in the title bar where the OS has no menu bar of
    /// its own (Windows, Linux). `None` on macOS, which owns the main menu.
    /// It snapshots the menus when built, so it is rebuilt when the menu epoch
    /// moves (a locale change reinstalls them).
    menu_bar: Option<(u64, Entity<gpui_component::menu::AppMenuBar>)>,
}

/// Bandwidth slider unit: MB/s ↔ bytes/sec.
const MB: f32 = (1024 * 1024) as f32;

impl KesselApp {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let bookmarks = cx.new(BookmarkManager::new);
        cx.subscribe_in(&bookmarks, window, Self::on_bookmark_event)
            .detach();

        let transfers = cx.new(TransfersPanel::new);
        // Re-render the status bar's transfer summary as snapshots arrive.
        cx.observe(&transfers, |_, _, cx| cx.notify()).detach();
        cx.subscribe_in(&transfers, window, Self::on_transfers_event)
            .detach();

        let logs = cx.new(LogsPanel::new);

        // Global queue limits, seeded from the engine's current values
        // (config defaults until changed at runtime).
        let queue_snap = cx.global::<AppState>().queue.latest();
        let parallel_slider = cx.new(|_| {
            SliderState::new()
                .min(1.)
                .max(16.)
                .step(1.)
                .default_value(queue_snap.max_parallel as f32)
        });
        cx.subscribe(&parallel_slider, |_, _, event: &SliderEvent, cx| {
            let SliderEvent::Change(value) = event;
            cx.global::<AppState>()
                .queue
                .set_max_parallel(value.start() as usize);
            cx.notify();
        })
        .detach();
        let throttle_slider = cx.new(|_| {
            SliderState::new()
                .min(0.)
                .max(200.)
                .step(1.)
                .default_value(queue_snap.bandwidth_limit as f32 / MB)
        });
        cx.subscribe(&throttle_slider, |_, _, event: &SliderEvent, cx| {
            let SliderEvent::Change(value) = event;
            cx.global::<AppState>()
                .queue
                .set_bandwidth((value.start() * MB) as u64);
            cx.notify();
        })
        .detach();

        let state = cx.global::<AppState>();
        let session = state.open_tabs.clone();
        let first_space = state.spaces.first().map(|space| space.id).unwrap_or(1);
        let space_ids: HashSet<i64> = state.spaces.iter().map(|space| space.id).collect();

        let mut this = Self {
            tabs: Vec::new(),
            active: 0,
            next_uid: 1,
            active_space: first_space,
            space_active: HashMap::new(),
            bookmarks,
            transfers,
            logs,
            bottom_dock: None,
            parallel_slider,
            throttle_slider,
            collapsed_folders: HashSet::new(),
            sidebar_open: true,
            panel_resize: None,
            rename: None,
            focus_handle: cx.focus_handle(),
            quit_hold: None,
            quit_dialog_open: false,
            pending_pane_focus: false,
            ui_save_scheduled: false,
            config_epoch_seen: 0,
            menu_bar: None,
            space_name_input: cx.new(|cx| InputState::new(window, cx).placeholder("space name")),
            space_icon: None,
            space_color: None,
            command_bar: None,
            available_update: None,
            update_installing: false,
            update_error: None,
        };

        // Background auto-update: check at startup and once a day.
        this.start_update_checks(cx);

        // Conflicts raised by external-edit sessions (the server copy
        // changed underneath an edit) surface as a dialog here.
        let edit_conflicts = crate::edit::subscribe();
        cx.spawn_in(window, async move |this, cx| {
            while let Ok(conflict) = edit_conflicts.recv().await {
                let alive = this.update_in(cx, |_, window, cx| {
                    crate::edit::open_conflict_dialog(conflict, window, cx);
                });
                if alive.is_err() {
                    break;
                }
            }
        })
        .detach();

        // config.json5 hot reload (see `watch_config`): when the epoch bumps,
        // re-seed panel layout from the freshly loaded values.
        cx.observe_global::<AppState>(|this, cx| {
            let epoch = cx.global::<AppState>().config_epoch;
            if epoch != this.config_epoch_seen {
                this.config_epoch_seen = epoch;
                this.reset_split_states(cx);
            }
        })
        .detach();

        // Restore the persisted tab session; fall back to one local tab.
        for row in &session {
            let a = this.profile_for(row.a_site_id, cx);
            let b = this.profile_for(row.b_site_id, cx);
            let workspace = this.make_workspace(
                a,
                row.a_path.clone(),
                b,
                row.b_path.clone(),
                PaneLayout::parse(&row.layout),
                window,
                cx,
            );
            let space_id = if space_ids.contains(&row.space_id) {
                row.space_id
            } else {
                first_space
            };
            let uid = this.next_uid;
            this.next_uid += 1;
            this.tabs.push(Tab {
                uid,
                space_id,
                bookmark_id: row.bookmark_site_id,
                custom_name: row.name.clone(),
                content: TabContent::Panes(workspace),
            });
        }
        // No session → no tabs: the workspace shows its empty state instead
        // of a synthesized local tab.
        if let Some(first) = this.tabs.first() {
            this.active_space = first.space_id;
            this.active = first.uid;
        }
        cx.global_mut::<AppState>().active_space = this.active_space;
        this
    }

    // --- tab/pane construction ---

    /// Resolve a persisted site id to a profile; missing sites (deleted
    /// bookmarks) fall back to the local filesystem.
    fn profile_for(&self, site_id: Option<i64>, cx: &App) -> SiteProfile {
        match site_id {
            None => local_profile(),
            Some(id) if id == LOCAL_SITE_ID => local_profile(),
            Some(id) => cx
                .global::<AppState>()
                .sites
                .iter()
                .find(|site| site.id == id)
                .cloned()
                .unwrap_or_else(local_profile),
        }
    }

    fn make_pane(
        &mut self,
        profile: SiteProfile,
        path: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<FilePane> {
        let pane = cx.new(|cx| FilePane::for_profile(profile, path, window, cx));
        cx.subscribe_in(&pane, window, Self::on_pane_event).detach();
        pane
    }

    fn make_workspace(
        &mut self,
        profile_a: SiteProfile,
        path_a: String,
        profile_b: SiteProfile,
        path_b: String,
        layout: PaneLayout,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Workspace {
        let split_h = cx.new(|_| ResizableState::default());
        let split_v = cx.new(|_| ResizableState::default());
        for split in [&split_h, &split_v] {
            cx.observe(split, |this, _, cx| this.on_split_resized(cx))
                .detach();
        }
        let workspace = Workspace {
            panes: [
                self.make_pane(profile_a, path_a, window, cx),
                self.make_pane(profile_b, path_b, window, cx),
            ],
            layout,
            active: 0,
            split_h,
            split_v,
            linked_roots: None,
        };
        // Seed each pane's tree orientation from the initial layout.
        for pane in &workspace.panes {
            pane.update(cx, |pane, cx| {
                pane.set_tree_on_top(layout == PaneLayout::SplitH, cx)
            });
        }
        workspace
    }

    fn push_tab(&mut self, bookmark_id: Option<i64>, content: TabContent, cx: &mut Context<Self>) {
        let uid = self.next_uid;
        self.next_uid += 1;
        self.tabs.push(Tab {
            uid,
            space_id: self.active_space,
            bookmark_id,
            custom_name: None,
            content,
        });
        self.activate_tab(uid, cx);
        self.persist_session(cx);
    }

    /// Open `path` on `profile` in a fresh tab. `bookmark_id` pins the tab to
    /// its bookmark row.
    fn open_tab_with(
        &mut self,
        profile: SiteProfile,
        path: String,
        bookmark_id: Option<i64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let workspace = self.make_workspace(
            profile,
            path,
            local_profile(),
            home_path(),
            PaneLayout::Single,
            window,
            cx,
        );
        self.push_tab(bookmark_id, TabContent::Panes(workspace), cx);
    }

    fn push_connection_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let manager = cx.new(BookmarkManager::new);
        manager.update(cx, |manager, cx| manager.open_new(window, cx));
        cx.subscribe_in(&manager, window, Self::on_bookmark_event)
            .detach();
        self.push_tab(None, TabContent::Connect(manager), cx);
    }

    fn tab_ix(&self, uid: u64) -> Option<usize> {
        self.tabs.iter().position(|tab| tab.uid == uid)
    }

    /// Deselect any tab: `active` points at nothing (uids start at 1) and
    /// the workspace renders its empty state — never a synthesized local tab.
    fn clear_active_tab(&mut self, space_id: i64, cx: &mut Context<Self>) {
        self.active = 0;
        self.active_space = space_id;
        cx.global_mut::<AppState>().active_space = space_id;
        cx.notify();
    }

    fn activate_tab(&mut self, uid: u64, cx: &mut Context<Self>) {
        let Some(ix) = self.tab_ix(uid) else { return };
        self.active = uid;
        self.active_space = self.tabs[ix].space_id;
        self.space_active.insert(self.active_space, uid);
        cx.global_mut::<AppState>().active_space = self.active_space;
        // Keyboard lands in the pane right away; the next render (which has
        // window access) applies the focus.
        self.pending_pane_focus = true;
        cx.notify();
    }

    fn close_tab(&mut self, uid: u64, cx: &mut Context<Self>) {
        let Some(ix) = self.tab_ix(uid) else { return };
        let space_id = self.tabs[ix].space_id;
        self.tabs.remove(ix);
        self.space_active.retain(|_, active| *active != uid);
        if self.active == uid {
            // Prefer the neighbor in the same space.
            let neighbor = self
                .tabs
                .iter()
                .enumerate()
                .filter(|(_, tab)| tab.space_id == space_id)
                .min_by_key(|(tab_ix, _)| tab_ix.abs_diff(ix))
                .map(|(_, tab)| tab.uid);
            match neighbor {
                Some(next) => self.activate_tab(next, cx),
                None => self.clear_active_tab(space_id, cx),
            }
        }
        self.persist_session(cx);
        cx.notify();
    }

    fn move_tab(&mut self, from_uid: u64, to_uid: u64, cx: &mut Context<Self>) {
        let (Some(from), Some(to)) = (self.tab_ix(from_uid), self.tab_ix(to_uid)) else {
            return;
        };
        if from == to {
            return;
        }
        let tab = self.tabs.remove(from);
        let to = self
            .tab_ix(to_uid)
            .unwrap_or(if from < to { to - 1 } else { to });
        self.tabs.insert(to, tab);
        self.persist_session(cx);
        cx.notify();
    }

    fn set_layout(&mut self, layout: PaneLayout, cx: &mut Context<Self>) {
        let Some(ix) = self.tab_ix(self.active) else {
            return;
        };
        if let TabContent::Panes(workspace) = &mut self.tabs[ix].content {
            workspace.layout = layout;
            if layout == PaneLayout::Single {
                workspace.active = 0;
                workspace.linked_roots = None;
                for pane in &workspace.panes {
                    pane.update(cx, |pane, _| pane.cancel_linked_navigation());
                }
            }
            let panes = workspace.panes.clone();
            self.persist_session(cx);
            for pane in &panes {
                pane.update(cx, |pane, cx| {
                    pane.set_tree_on_top(layout == PaneLayout::SplitH, cx)
                });
            }
            cx.notify();
        }
    }

    fn active_tab(&self) -> Option<&Tab> {
        self.tab_ix(self.active).map(|ix| &self.tabs[ix])
    }

    /// Locate a pane entity across all tabs → (tab index, pane index).
    fn find_pane(&self, id: EntityId) -> Option<(usize, usize)> {
        self.tabs.iter().enumerate().find_map(|(tab_ix, tab)| {
            let TabContent::Panes(workspace) = &tab.content else {
                return None;
            };
            workspace
                .panes
                .iter()
                .position(|pane| pane.entity_id() == id)
                .map(|pane_ix| (tab_ix, pane_ix))
        })
    }

    // --- spaces ---

    fn switch_space(&mut self, space_id: i64, cx: &mut Context<Self>) {
        if self.active_space == space_id {
            return;
        }
        self.active_space = space_id;
        // Restore the space's last active tab, else its first; an empty
        // space just shows the workspace empty state.
        let target = self
            .space_active
            .get(&space_id)
            .copied()
            .filter(|uid| self.tab_ix(*uid).is_some())
            .or_else(|| {
                self.tabs
                    .iter()
                    .find(|tab| tab.space_id == space_id)
                    .map(|tab| tab.uid)
            });
        match target {
            Some(uid) => self.activate_tab(uid, cx),
            None => self.clear_active_tab(space_id, cx),
        }
    }

    fn create_space(
        &mut self,
        name: String,
        icon: Option<String>,
        color: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            db::insert_space(&pool, &name, icon.as_deref(), color.as_deref()).await?;
            db::list_spaces(&pool).await
        });
        self.sync_spaces(task, cx);
    }

    fn delete_space(&mut self, space_id: i64, window: &mut Window, cx: &mut Context<Self>) {
        if cx.global::<AppState>().spaces.len() <= 1 {
            window.push_notification("The last space cannot be deleted.", cx);
            return;
        }
        self.tabs.retain(|tab| tab.space_id != space_id);
        self.space_active.remove(&space_id);
        let state = cx.global_mut::<AppState>();
        // Mirror what db::delete_space persists: the deleted space's
        // bookmarks and folders move to the first surviving space.
        let adopter = state
            .spaces
            .iter()
            .map(|space| space.id)
            .filter(|id| *id != space_id)
            .min()
            .unwrap_or(space_id);
        for site in &mut state.sites {
            if site.space_id == space_id {
                site.space_id = adopter;
            }
        }
        for folder in &mut state.folders {
            if folder.space_id == space_id {
                folder.space_id = adopter;
            }
        }
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            db::delete_space(&pool, space_id).await?;
            db::list_spaces(&pool).await
        });
        self.sync_spaces(task, cx);
        if self.active_space == space_id {
            let fallback = cx
                .global::<AppState>()
                .spaces
                .iter()
                .map(|space| space.id)
                .find(|id| *id != space_id)
                .unwrap_or(space_id);
            self.switch_space_fallback(fallback, cx);
        }
        self.persist_session(cx);
    }

    /// Step to the previous/next space in switcher order (mouse back/forward
    /// buttons over the sidebar).
    fn cycle_space(&mut self, delta: i64, cx: &mut Context<Self>) {
        let spaces: Vec<i64> = cx
            .global::<AppState>()
            .spaces
            .iter()
            .map(|space| space.id)
            .collect();
        if spaces.is_empty() {
            return;
        }
        let current = spaces
            .iter()
            .position(|id| *id == self.active_space)
            .unwrap_or(0) as i64;
        let next = (current + delta).rem_euclid(spaces.len() as i64) as usize;
        self.switch_space(spaces[next], cx);
    }

    /// Like `switch_space`, but unconditional (used after deleting the
    /// active space).
    fn switch_space_fallback(&mut self, space_id: i64, cx: &mut Context<Self>) {
        self.active_space = space_id;
        let target = self
            .tabs
            .iter()
            .find(|tab| tab.space_id == space_id)
            .map(|tab| tab.uid);
        match target {
            Some(uid) => self.activate_tab(uid, cx),
            None => self.clear_active_tab(space_id, cx),
        }
    }

    // --- global actions (see assets/config/keybindings.json5) ---

    /// uids of the active space's tabs, in sidebar order.
    fn space_tabs(&self) -> Vec<u64> {
        self.tabs
            .iter()
            .filter(|tab| tab.space_id == self.active_space)
            .map(|tab| tab.uid)
            .collect()
    }

    fn cycle_tab(&mut self, delta: i64, cx: &mut Context<Self>) {
        let tabs = self.space_tabs();
        if tabs.is_empty() {
            return;
        }
        let current = tabs.iter().position(|uid| *uid == self.active).unwrap_or(0) as i64;
        let next = (current + delta).rem_euclid(tabs.len() as i64) as usize;
        self.activate_tab(tabs[next], cx);
    }

    fn on_new_tab(&mut self, _: &NewTab, window: &mut Window, cx: &mut Context<Self>) {
        self.push_connection_tab(window, cx);
    }

    fn on_close_tab(&mut self, _: &CloseTab, _: &mut Window, cx: &mut Context<Self>) {
        if self.tab_ix(self.active).is_some() {
            self.close_tab(self.active, cx);
        }
    }

    fn on_next_tab(&mut self, _: &NextTab, _: &mut Window, cx: &mut Context<Self>) {
        self.cycle_tab(1, cx);
    }

    fn on_prev_tab(&mut self, _: &PrevTab, _: &mut Window, cx: &mut Context<Self>) {
        self.cycle_tab(-1, cx);
    }

    fn on_activate_tab(&mut self, action: &ActivateTab, _: &mut Window, cx: &mut Context<Self>) {
        let tabs = self.space_tabs();
        if let Some(uid) = action.index.checked_sub(1).and_then(|ix| tabs.get(ix)) {
            self.activate_tab(*uid, cx);
        }
    }

    fn on_switch_space(&mut self, action: &SwitchSpace, _: &mut Window, cx: &mut Context<Self>) {
        let target = action
            .index
            .checked_sub(1)
            .and_then(|ix| cx.global::<AppState>().spaces.get(ix))
            .map(|space| space.id);
        if let Some(space_id) = target {
            self.switch_space(space_id, cx);
        }
    }

    /// Tab (in a split layout): move focus to the other pane.
    fn on_focus_other_pane(
        &mut self,
        _: &FocusOtherPane,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(ix) = self.tab_ix(self.active) else {
            return;
        };
        if let TabContent::Panes(workspace) = &mut self.tabs[ix].content {
            if workspace.layout == PaneLayout::Single {
                return;
            }
            workspace.active = 1 - workspace.active;
            let focus = workspace.panes[workspace.active].focus_handle(cx);
            window.focus(&focus);
            cx.notify();
        }
    }

    fn on_layout_single(&mut self, _: &LayoutSingle, _: &mut Window, cx: &mut Context<Self>) {
        self.set_layout(PaneLayout::Single, cx);
    }

    fn on_layout_split_h(&mut self, _: &LayoutSplitH, _: &mut Window, cx: &mut Context<Self>) {
        self.set_layout(PaneLayout::SplitH, cx);
    }

    fn on_layout_split_v(&mut self, _: &LayoutSplitV, _: &mut Window, cx: &mut Context<Self>) {
        self.set_layout(PaneLayout::SplitV, cx);
    }

    fn on_toggle_sidebar(&mut self, _: &ToggleSidebar, _: &mut Window, cx: &mut Context<Self>) {
        self.toggle_sidebar(cx);
    }

    fn on_toggle_transfers(&mut self, _: &ToggleTransfers, _: &mut Window, cx: &mut Context<Self>) {
        self.toggle_bottom(BottomPanel::Transfers, cx);
    }

    fn on_toggle_logs(&mut self, _: &ToggleLogs, _: &mut Window, cx: &mut Context<Self>) {
        self.toggle_bottom(BottomPanel::Logs, cx);
    }

    fn on_open_bookmarks(
        &mut self,
        _: &OpenBookmarks,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_bookmarks_dialog(None, window, cx);
    }

    fn on_about(&mut self, _: &About, window: &mut Window, cx: &mut Context<Self>) {
        window.open_dialog(cx, move |dialog, _, cx| {
            dialog.w(px(380.)).child(
                gpui_component::v_flex()
                    .items_center()
                    .gap_2()
                    .py_2()
                    .child(Icon::default().path("icons/rocket.svg"))
                    .child(div().text_lg().font_semibold().child(t("about.title")))
                    .child(
                        div()
                            .text_sm()
                            .text_color(cx.theme().muted_foreground)
                            .child(crate::ui::preferences::app_version()),
                    )
                    .child(div().text_sm().child(t("about.tagline")))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(t("about.credits")),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(t("about.licenses")),
                    )
                    .child(
                        Button::new("about-website")
                            .label(t("about.website"))
                            .outline()
                            .small()
                            .on_click(|_, _, _| {
                                crate::ui::open_path_or_url("https://www.coderscantina.com");
                            }),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(t("about.copyright")),
                    ),
            )
        });
    }

    fn on_open_preferences(
        &mut self,
        _: &OpenPreferences,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let panel = cx.new(|cx| crate::ui::preferences::PreferencesPanel::new(window, cx));
        window.open_dialog(cx, move |dialog, _, _| {
            dialog
                .title(t("prefs.title"))
                .w(px(720.))
                .child(panel.clone())
        });
    }

    fn on_open_theme_browser(
        &mut self,
        _: &OpenThemeBrowser,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let panel = cx.new(|cx| crate::ui::theme_browser::ThemeBrowser::new(window, cx));
        window.open_dialog(cx, move |dialog, _, _| {
            dialog
                .title(t("themes.title"))
                .w(px(560.))
                .child(panel.clone())
        });
    }

    fn on_help(&mut self, _: &Help, _: &mut Window, _: &mut Context<Self>) {
        crate::ui::open_path_or_url("https://github.com/coderscantina/kessel#readme");
    }

    // --- auto-update ---

    /// Background check at startup, then once every 24h. Network work runs on
    /// the io runtime; the result is marshalled back to set `available_update`.
    fn start_update_checks(&self, cx: &mut Context<Self>) {
        let cfg = cx.global::<AppState>().config.update.clone();
        if !cfg.check {
            return;
        }
        cx.spawn(async move |this, cx| loop {
            let cfg = cfg.clone();
            let Ok(task) = this.update(cx, |_, cx| {
                cx.global::<AppState>()
                    .runtime
                    .spawn(async move { crate::update::check_for_update(&cfg).await })
            }) else {
                break;
            };
            if let Ok(Some(info)) = task.await {
                let applied = this.update(cx, |this, cx| {
                    this.available_update = Some(info);
                    cx.notify();
                });
                if applied.is_err() {
                    break;
                }
            }
            cx.background_executor()
                .timer(Duration::from_secs(24 * 60 * 60))
                .await;
        })
        .detach();
    }

    /// Manual "Check for updates…": if one is already known, open the dialog;
    /// otherwise fetch now and either open the dialog or notify "up to date".
    fn on_check_for_updates(
        &mut self,
        _: &CheckForUpdates,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.available_update.is_some() {
            self.open_update_dialog(window, cx);
            return;
        }
        let cfg = cx.global::<AppState>().config.update.clone();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { crate::update::fetch_latest(&cfg).await });
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(inner) => inner,
                Err(join) => Err(anyhow::anyhow!("update task failed: {join}")),
            };
            this.update_in(cx, |this, window, cx| match result {
                Ok(Some(info)) => {
                    this.available_update = Some(info);
                    this.open_update_dialog(window, cx);
                }
                Ok(None) => window.push_notification(t("update.up_to_date").to_string(), cx),
                Err(error) => {
                    tracing::debug!(%error, "manual update check failed");
                    window.push_notification(t("update.check_failed").to_string(), cx);
                }
            })
            .ok();
        })
        .detach();
    }

    /// Confirm dialog for an available update: version, release date, and
    /// "Install & relaunch" / "Later". Re-renders from `self` as the install
    /// progresses so the spinner and any error show inline.
    fn open_update_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(info) = self.available_update.clone() else {
            return;
        };
        self.update_error = None;
        self.update_installing = false;
        let app = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog, _, cx| {
            let (installing, error) = app
                .upgrade()
                .map(|app| {
                    let app = app.read(cx);
                    (app.update_installing, app.update_error.clone())
                })
                .unwrap_or((false, None));
            let date = info.pub_date_display();
            let body = v_flex()
                .gap_1()
                .py_1()
                .child(
                    div()
                        .text_sm()
                        .child(tf("update.dialog.available", &[("version", &info.version)])),
                )
                .when(!date.is_empty(), |el| {
                    el.child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(tf("update.dialog.released", &[("date", &date)])),
                    )
                })
                .when_some(error, |el, err| {
                    el.child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().danger)
                            .child(err),
                    )
                });
            let footer_app = app.clone();
            let footer_info = info.clone();
            dialog
                .title(t("update.dialog.title"))
                .w(px(420.))
                .overlay_closable(!installing)
                .child(body)
                .footer(move |_ok, _cancel, _window, _cx| {
                    let install_app = footer_app.clone();
                    let install_info = footer_info.clone();
                    vec![
                        Button::new("update-later")
                            .label(t("update.dialog.later"))
                            .ghost()
                            .disabled(installing)
                            .on_click(|_, window, cx| window.close_dialog(cx)),
                        Button::new("update-install")
                            .label(t("update.dialog.install"))
                            .primary()
                            .loading(installing)
                            .disabled(installing)
                            .on_click(move |_, _, cx| {
                                let info = install_info.clone();
                                install_app
                                    .update(cx, |this, cx| this.start_install(info, cx))
                                    .ok();
                            }),
                    ]
                })
        });
    }

    /// Kick off the download/install on the io runtime; on success relaunch and
    /// quit, on manual-install show a hint, on error show it inline.
    fn start_install(&mut self, info: crate::update::UpdateInfo, cx: &mut Context<Self>) {
        if self.update_installing {
            return;
        }
        self.update_installing = true;
        self.update_error = None;
        cx.notify();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { crate::update::download_and_install(&info).await });
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(inner) => inner,
                Err(join) => Err(anyhow::anyhow!("update install task failed: {join}")),
            };
            this.update(cx, |this, cx| {
                this.update_installing = false;
                match result {
                    Ok(crate::update::InstallOutcome::ReadyToRelaunch { app_path }) => {
                        crate::update::relaunch(&app_path);
                        cx.quit();
                    }
                    Ok(crate::update::InstallOutcome::ManualInstall) => {
                        this.update_error = Some(t("update.manual_hint"));
                    }
                    Err(error) => {
                        this.update_error = Some(SharedString::from(format!("{error}")));
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    // --- command bar ---

    fn on_toggle_command_bar(
        &mut self,
        _: &ToggleCommandBar,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.command_bar.take().is_some() {
            self.pending_pane_focus = true;
            cx.notify();
            return;
        }
        let bar = cx.new(|cx| CommandBar::new(window, cx));
        cx.subscribe_in(&bar, window, Self::on_command_bar_event)
            .detach();
        let focus = bar.read(cx).focus_handle(cx);
        window.focus(&focus);
        self.command_bar = Some(bar);
        cx.notify();
    }

    fn close_command_bar(&mut self, cx: &mut Context<Self>) {
        if self.command_bar.take().is_some() {
            self.pending_pane_focus = true;
            cx.notify();
        }
    }

    fn on_command_bar_event(
        &mut self,
        _: &Entity<CommandBar>,
        event: &CommandBarEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            CommandBarEvent::Dismiss => self.close_command_bar(cx),
            CommandBarEvent::Run(request) => {
                let request = request.clone();
                self.close_command_bar(cx);
                match request {
                    RunRequest::Action(name) => {
                        // Route through the focus chain like the keymap: put
                        // focus back on the active pane first so pane actions
                        // land there.
                        if let Some(tab) = self.active_tab() {
                            if let TabContent::Panes(workspace) = &tab.content {
                                let focus = workspace.panes[workspace.active].focus_handle(cx);
                                window.focus(&focus);
                            }
                        }
                        match cx.build_action(name, None) {
                            Ok(action) => window.dispatch_action(action, cx),
                            Err(error) => {
                                tracing::error!(%error, name, "command bar action failed")
                            }
                        }
                    }
                    RunRequest::Bookmark(site_id) => self.open_bookmark(site_id, window, cx),
                    RunRequest::Space(space_id) => self.switch_space(space_id, cx),
                }
            }
        }
    }

    // --- directory synchronization ---

    /// Open the Synchronize dialog over the active tab's two panes.
    fn on_sync_panes(&mut self, _: &SyncPanes, window: &mut Window, cx: &mut Context<Self>) {
        let Some(tab) = self.active_tab() else {
            window.push_notification(t("sync.needs_split").to_string(), cx);
            return;
        };
        let TabContent::Panes(workspace) = &tab.content else {
            window.push_notification(t("sync.needs_split").to_string(), cx);
            return;
        };
        if workspace.layout == PaneLayout::Single {
            window.push_notification(t("sync.needs_split").to_string(), cx);
            return;
        }
        let side = |pane: &Entity<FilePane>| {
            let pane = pane.read(cx);
            crate::ui::sync::SyncSide {
                profile: pane.profile().clone(),
                dir: pane.current_dir(),
                label: pane.label(),
            }
        };
        let (a, b) = (side(&workspace.panes[0]), side(&workspace.panes[1]));
        let dialog_view = cx.new(|cx| crate::ui::sync::SyncDialog::new(a, b, window, cx));
        window.open_dialog(cx, move |dialog, _, _| {
            dialog
                .title(t("sync.title"))
                .w(px(560.))
                .child(dialog_view.clone())
        });
    }

    /// Cmd-Q: quit at once when the queue is idle. With transfers running,
    /// ask first (Chrome-style) — but a sustained hold quits anyway, riding
    /// on key repeat re-firing this action while cmd-q stays down. The
    /// `on_app_quit` hook pauses workers and flushes offsets either way.
    fn on_quit(&mut self, _: &Quit, window: &mut Window, cx: &mut Context<Self>) {
        let busy = self
            .transfers
            .read(cx)
            .stats()
            .is_some_and(|stats| stats.active > 0);
        if !busy {
            cx.quit();
            return;
        }
        let now = Instant::now();
        match self.quit_hold {
            // Repeats arrive well under 400ms apart while the key is held.
            Some((first, last)) if now.duration_since(last) < Duration::from_millis(400) => {
                self.quit_hold = Some((first, now));
                if now.duration_since(first) >= Duration::from_millis(1200) {
                    cx.quit();
                }
            }
            _ => {
                self.quit_hold = Some((now, now));
                if !self.quit_dialog_open {
                    self.confirm_quit_dialog(window, cx);
                }
            }
        }
    }

    fn confirm_quit_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.quit_dialog_open = true;
        let app = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog, _, _| {
            let app = app.clone();
            let reset = move |cx: &mut App| {
                app.update(cx, |this, _| this.quit_dialog_open = false).ok();
            };
            let on_cancel_reset = reset.clone();
            dialog
                .title("Transfers are running")
                .w(px(440.))
                .child(format!(
                    "Quitting pauses running transfers and resumes them the next \
                     time kessel starts. Keep holding {} to quit without asking.",
                    if cfg!(target_os = "macos") { "⌘Q" } else { "Ctrl+Q" }
                ))
                .confirm()
                .button_props(
                    gpui_component::dialog::DialogButtonProps::default()
                        .ok_text("Pause and Quit")
                        .cancel_text("Keep Transferring"),
                )
                .on_ok(move |_, _, cx| {
                    reset(cx);
                    cx.quit();
                    true
                })
                .on_cancel(move |_, _, cx| {
                    on_cancel_reset(cx);
                    true
                })
        });
    }

    fn sync_spaces(
        &mut self,
        task: tokio::task::JoinHandle<anyhow::Result<Vec<Space>>>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("space task failed: {join_error}")),
            };
            this.update(cx, |_, cx| {
                match result {
                    Ok(spaces) => cx.global_mut::<AppState>().spaces = spaces,
                    Err(error) => tracing::error!(%error, "space change failed"),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn toggle_linked_browsing(&mut self, cx: &mut Context<Self>) {
        let Some(ix) = self.tab_ix(self.active) else {
            return;
        };
        let TabContent::Panes(workspace) = &mut self.tabs[ix].content else {
            return;
        };
        if workspace.layout == PaneLayout::Single {
            return;
        }
        workspace.linked_roots = if workspace.linked_roots.is_some() {
            None
        } else {
            Some(workspace.panes.each_ref().map(|pane| {
                let pane = pane.read(cx);
                (pane.site_id(), pane.current_dir())
            }))
        };
        for pane in &workspace.panes {
            pane.update(cx, |pane, _| pane.cancel_linked_navigation());
        }
        cx.notify();
    }

    fn follow_linked_pane(
        &mut self,
        pane: &Entity<FilePane>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some((tab_ix, pane_ix)) = self.find_pane(pane.entity_id()) else {
            return;
        };
        let TabContent::Panes(workspace) = &mut self.tabs[tab_ix].content else {
            return;
        };
        let Some(roots) = &workspace.linked_roots else {
            return;
        };
        let path = pane.read(cx).current_dir();
        let sites_match = workspace
            .panes
            .iter()
            .zip(roots)
            .all(|(pane, (site, _))| pane.read(cx).site_id() == *site);
        let target = sites_match
            .then(|| linked_directory(&roots[pane_ix].1, &roots[1 - pane_ix].1, &path))
            .flatten();
        let Some(target) = target else {
            workspace.linked_roots = None;
            for pane in &workspace.panes {
                pane.update(cx, |pane, _| pane.cancel_linked_navigation());
            }
            window.push_notification("Linked browsing stopped: you left the paired folders.", cx);
            cx.notify();
            return;
        };
        let other = &workspace.panes[1 - pane_ix];
        if other.read(cx).current_dir() != target {
            other.update(cx, |pane, cx| pane.navigate_linked(target, window, cx));
        }
    }

    // --- event plumbing ---

    fn on_pane_event(
        &mut self,
        pane: &Entity<FilePane>,
        event: &PaneEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            PaneEvent::CopyRequested(entries) => {
                let Some((tab_ix, pane_ix)) = self.find_pane(pane.entity_id()) else {
                    return;
                };
                self.enqueue_entries(tab_ix, pane_ix, entries.clone(), false, window, cx);
            }
            PaneEvent::MoveRequested(entries) => {
                let Some((tab_ix, pane_ix)) = self.find_pane(pane.entity_id()) else {
                    return;
                };
                self.enqueue_entries(tab_ix, pane_ix, entries.clone(), true, window, cx);
            }
            PaneEvent::OpenInNewTab { profile, path } => {
                self.open_tab_with(profile.clone(), path.clone(), None, window, cx);
            }
            PaneEvent::DirectoryLoaded { linked } => {
                if !linked { self.follow_linked_pane(pane, window, cx); }
            }
            PaneEvent::LinkedNavigationFailed => {
                if let Some((tab_ix, _)) = self.find_pane(pane.entity_id()) {
                    if let TabContent::Panes(workspace) = &mut self.tabs[tab_ix].content {
                        workspace.linked_roots = None;
                        for pane in &workspace.panes {
                            pane.update(cx, |pane, _| pane.cancel_linked_navigation());
                        }
                        cx.notify();
                    }
                }
            }
            PaneEvent::Navigated => {
                if let Some((tab_ix, pane_ix)) = self.find_pane(pane.entity_id()) {
                    if let TabContent::Panes(workspace) = &self.tabs[tab_ix].content {
                        workspace.panes[1 - pane_ix].update(cx, |pane, _| pane.cancel_linked_navigation());
                    }
                }
                self.persist_session(cx);
                cx.notify();
            }
            PaneEvent::Focused => {
                if let Some((tab_ix, pane_ix)) = self.find_pane(pane.entity_id()) {
                    if let TabContent::Panes(workspace) = &mut self.tabs[tab_ix].content {
                        if workspace.active != pane_ix {
                            workspace.active = pane_ix;
                            cx.notify();
                        }
                    }
                }
            }
        }
    }

    fn on_transfers_event(
        &mut self,
        _: &Entity<TransfersPanel>,
        event: &TransfersEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            // A finished transfer refreshes any pane currently showing its
            // destination directory. Local panes are skipped — their fs
            // watcher already picks up the change.
            TransfersEvent::JobFinished(job) => {
                if job.dst_site_id == LOCAL_SITE_ID {
                    return;
                }
                for tab in &self.tabs {
                    let TabContent::Panes(workspace) = &tab.content else {
                        continue;
                    };
                    for pane in &workspace.panes {
                        let showing_dst = {
                            let pane = pane.read(cx);
                            pane.site_id() == job.dst_site_id && pane.current_dir() == job.dst
                        };
                        if showing_dst {
                            pane.update(cx, |pane, cx| pane.refresh(window, cx));
                        }
                    }
                }
            }
        }
    }

    fn on_bookmark_event(
        &mut self,
        manager: &Entity<BookmarkManager>,
        event: &BookmarkEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            BookmarkEvent::Connect(profile) => {
                let profile = profile.clone();
                // A connection tab turns into a pinned bookmark tab in
                // place; a connect from the bookmarks dialog behaves like a
                // bookmark click.
                let connect_tab = self.tabs.iter().position(|tab| {
                    matches!(&tab.content, TabContent::Connect(m) if m.entity_id() == manager.entity_id())
                });
                match connect_tab {
                    Some(ix) => {
                        let workspace = self.make_workspace(
                            profile.clone(),
                            String::new(),
                            local_profile(),
                            home_path(),
                            PaneLayout::Single,
                            window,
                            cx,
                        );
                        let tab = &mut self.tabs[ix];
                        tab.content = TabContent::Panes(workspace);
                        tab.bookmark_id = (profile.id > 0).then_some(profile.id);
                        let uid = tab.uid;
                        self.activate_tab(uid, cx);
                        self.persist_session(cx);
                    }
                    None => self.open_bookmark(profile.id, window, cx),
                }
            }
            BookmarkEvent::SitesChanged => {
                self.refresh_tags(cx);
                cx.notify();
            }
        }
    }

    /// Enqueue entries from one pane into the other pane of the same tab.
    fn enqueue_entries(
        &mut self,
        tab_ix: usize,
        src_ix: usize,
        entries: Vec<FsEntry>,
        move_source: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(TabContent::Panes(workspace)) = self.tabs.get(tab_ix).map(|tab| &tab.content)
        else {
            return;
        };
        if workspace.layout == PaneLayout::Single {
            window.push_notification("Switch to split view to copy between panes.", cx);
            return;
        }
        let src_pane = &workspace.panes[src_ix];
        let dst_pane = &workspace.panes[1 - src_ix];
        let src_site_id = src_pane.read(cx).site_id();
        let dst_site_id = dst_pane.read(cx).site_id();
        let dst_dir = dst_pane.read(cx).current_dir();
        let count = entries.len();
        let queue = cx.global::<AppState>().queue.clone();
        for entry in entries {
            queue.enqueue(NewJob {
                src_site_id,
                dst_site_id,
                src: entry.path,
                dst_dir: dst_dir.clone(),
                dst_name: None,
                // Existing files park as conflicts; the Transfers panel prompts.
                conflict_policy: ConflictPolicy::Ask,
                move_source,
            });
        }
        let verb = if move_source { "move" } else { "item" };
        window.push_notification(format!("Queued {count} {verb}(s)."), cx);
        // Surface the queue right where the work was kicked off.
        self.bottom_dock = Some(BottomPanel::Transfers);
        cx.notify();
    }

    // --- panel size persistence ---

    /// A workspace pane split moved: mirror current px sizes into the config
    /// and debounce a write back to config.json5. (Sidebar / transfers /
    /// logs sizes are fixed-pixel and written directly by the divider drag.)
    fn on_split_resized(&mut self, cx: &mut Context<Self>) {
        // Persisted panel sizes are rounded to whole pixels. The resizable
        // reports full sub-pixel precision, but `config::fmt_f32` writes one
        // decimal, so an unrounded value never survives the write→reload
        // round-trip: `new != old`, the watcher bumps the config epoch and
        // `reset_split_states` re-seeds the state, which re-measures and writes
        // again — an endless reload/flicker loop. Rounding makes a self-write
        // reload back byte-for-byte equal, so it's a no-op.
        let read = |state: &Entity<ResizableState>, ix: usize| {
            state
                .read(cx)
                .sizes()
                .get(ix)
                .map(|size| f32::from(*size).round())
                .filter(|size| *size > 1.)
        };
        let workspace_splits = self.active_tab().and_then(|tab| match &tab.content {
            TabContent::Panes(workspace) => {
                Some((workspace.split_h.clone(), workspace.split_v.clone()))
            }
            TabContent::Connect(_) => None,
        });
        let split_h = workspace_splits.as_ref().and_then(|(h, _)| read(h, 0));
        let split_v = workspace_splits.as_ref().and_then(|(_, v)| read(v, 0));

        let ui = &mut cx.global_mut::<AppState>().config.ui;
        let mut changed = false;
        if let Some(width) = split_h {
            if ui.split_h_width != width {
                ui.split_h_width = width;
                changed = true;
            }
        }
        if let Some(height) = split_v {
            if ui.split_v_height != height {
                ui.split_v_height = height;
                changed = true;
            }
        }
        // Only persist on a real change: an unchanged self-write would still
        // reload cleanly, but skipping it avoids the churn entirely.
        if changed {
            self.schedule_ui_save(cx);
        }
    }

    /// Re-seed pane split layout from the (re)loaded config: fresh
    /// `ResizableState`s make the resizable panels lay out from the
    /// configured pixel sizes again. The fixed-pixel panels (sidebar,
    /// transfers, logs) read the config directly on every render.
    fn reset_split_states(&mut self, cx: &mut Context<Self>) {
        for ix in 0..self.tabs.len() {
            if let TabContent::Panes(workspace) = &mut self.tabs[ix].content {
                workspace.split_h = cx.new(|_| ResizableState::default());
                workspace.split_v = cx.new(|_| ResizableState::default());
                let splits = [workspace.split_h.clone(), workspace.split_v.clone()];
                for split in splits {
                    cx.observe(&split, |this, _, cx| this.on_split_resized(cx))
                        .detach();
                }
            }
        }
        cx.notify();
    }

    fn schedule_ui_save(&mut self, cx: &mut Context<Self>) {
        if self.ui_save_scheduled {
            return;
        }
        self.ui_save_scheduled = true;
        cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_millis(800))
                .await;
            this.update(cx, |this, cx| {
                this.ui_save_scheduled = false;
                let ui = cx.global::<AppState>().config.ui.clone();
                if let Err(error) = crate::config::save_ui_state(&ui) {
                    tracing::error!(%error, "persisting ui state failed");
                }
            })
            .ok();
        })
        .detach();
    }

    // --- session persistence ---

    /// Rewrite the persisted tab session (fire-and-forget; the session is
    /// tiny). Connection tabs are transient and not persisted.
    fn persist_session(&self, cx: &mut Context<Self>) {
        let rows: Vec<OpenTabRow> = self
            .tabs
            .iter()
            .filter_map(|tab| {
                let TabContent::Panes(workspace) = &tab.content else {
                    return None;
                };
                let a = workspace.panes[0].read(cx);
                let b = workspace.panes[1].read(cx);
                Some(OpenTabRow {
                    space_id: tab.space_id,
                    bookmark_site_id: tab.bookmark_id,
                    name: tab.custom_name.clone(),
                    layout: workspace.layout.as_str().to_string(),
                    a_site_id: (a.site_id() != LOCAL_SITE_ID).then(|| a.site_id()),
                    a_path: a.current_dir(),
                    b_site_id: (b.site_id() != LOCAL_SITE_ID).then(|| b.site_id()),
                    b_path: b.current_dir(),
                })
            })
            .collect();
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        state.runtime.spawn(async move {
            if let Err(error) = db::save_open_tabs(&pool, &rows).await {
                tracing::error!(%error, "persisting tab session failed");
            }
        });
    }

    // --- bookmarks (sidebar actions) ---

    /// The bookmark's pinned tab in the current space, if it's open.
    fn bookmark_tab(&self, site_id: i64) -> Option<u64> {
        self.tabs
            .iter()
            .find(|tab| tab.space_id == self.active_space && tab.bookmark_id == Some(site_id))
            .map(|tab| tab.uid)
    }

    /// Bookmark click: focus its pinned tab in the current space, opening it
    /// first if needed. Never adds a row to the ephemeral tab list.
    fn open_bookmark(&mut self, site_id: i64, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(uid) = self.bookmark_tab(site_id) {
            self.activate_tab(uid, cx);
            return;
        }
        let profile = self.profile_for(Some(site_id), cx);
        self.open_tab_with(profile, String::new(), Some(site_id), window, cx);
    }

    /// Await a DB task returning the fresh site list and sync it into the
    /// global state.
    fn sync_sites(
        &mut self,
        task: tokio::task::JoinHandle<anyhow::Result<Vec<SiteProfile>>>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("bookmark task failed: {join_error}")),
            };
            this.update(cx, |_, cx| {
                match result {
                    Ok(sites) => cx.global_mut::<AppState>().sites = sites,
                    Err(error) => tracing::error!(%error, "bookmark change failed"),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Context menu "Duplicate": insert a copy of the bookmark (including
    /// its keychain secrets, under the new site's own handle).
    fn duplicate_bookmark(&mut self, site_id: i64, cx: &mut Context<Self>) {
        let state = cx.global::<AppState>();
        let Some(site) = state.sites.iter().find(|site| site.id == site_id).cloned() else {
            return;
        };
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            let name = format!("{} copy", site.name);
            let meta = db::BookmarkMeta {
                icon: site.icon.clone(),
                color: site.color.clone(),
                tags: site.tags.clone(),
                folder_id: site.folder_id,
                space_id: site.space_id,
            };
            let new_id = db::insert_site(
                &pool,
                &name,
                &site.scheme,
                &site.options,
                &site.layers,
                &meta,
            )
            .await?;
            if let Some(handle) = &site.secret_handle {
                if let Ok(secrets) = crate::secrets::SecretStore::load(handle) {
                    let new_handle = crate::secrets::SecretStore::handle_for_site(new_id);
                    crate::secrets::SecretStore::store(&new_handle, &secrets)?;
                    db::set_site_secret_handle(&pool, new_id, Some(&new_handle)).await?;
                }
            }
            db::list_sites(&pool).await
        });
        self.sync_sites(task, cx);
    }

    /// Context menu "Delete…": confirm, close its pinned tabs, then remove
    /// the site row and its keychain secrets.
    fn confirm_delete_bookmark(
        &mut self,
        site_id: i64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(name) = cx
            .global::<AppState>()
            .sites
            .iter()
            .find(|site| site.id == site_id)
            .map(|site| site.name.clone())
        else {
            return;
        };
        let app = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog, _, _| {
            let app = app.clone();
            dialog
                .title(format!("Delete bookmark `{name}`?"))
                .w(px(380.))
                .child("Its saved connection settings and secrets are removed for good.")
                .confirm()
                .on_ok(move |_, _, cx| {
                    app.update(cx, |this, cx| this.delete_bookmark(site_id, cx))
                        .ok();
                    true
                })
        });
    }

    fn delete_bookmark(&mut self, site_id: i64, cx: &mut Context<Self>) {
        // Close any pinned tabs of this bookmark (across spaces).
        let open: Vec<u64> = self
            .tabs
            .iter()
            .filter(|tab| tab.bookmark_id == Some(site_id))
            .map(|tab| tab.uid)
            .collect();
        for uid in open {
            self.close_tab(uid, cx);
        }
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            let _ = crate::secrets::SecretStore::delete(
                &crate::secrets::SecretStore::handle_for_site(site_id),
            );
            db::delete_site(&pool, site_id).await?;
            db::list_sites(&pool).await
        });
        self.sync_sites(task, cx);
    }

    /// Drop bookmark `src` onto bookmark `dst`: adopt dst's folder and slot
    /// in before it.
    fn drop_bookmark_on(&mut self, src_id: i64, dst_id: i64, cx: &mut Context<Self>) {
        if src_id == dst_id {
            return;
        }
        let state = cx.global_mut::<AppState>();
        let Some(src_ix) = state.sites.iter().position(|site| site.id == src_id) else {
            return;
        };
        let mut src = state.sites.remove(src_ix);
        let Some(dst_ix) = state.sites.iter().position(|site| site.id == dst_id) else {
            state.sites.insert(src_ix, src);
            return;
        };
        let folder_id = state.sites[dst_ix].folder_id;
        src.folder_id = folder_id;
        state.sites.insert(dst_ix, src);
        let ids: Vec<i64> = state.sites.iter().map(|site| site.id).collect();
        let pool = state.db.clone();
        state.runtime.spawn(async move {
            let _ = db::set_site_folder(&pool, src_id, folder_id).await;
            let _ = db::set_site_order(&pool, &ids).await;
        });
        cx.notify();
    }

    /// Drop a bookmark onto a folder header (or the section header for
    /// "no folder").
    fn drop_bookmark_into_folder(
        &mut self,
        src_id: i64,
        folder_id: Option<i64>,
        cx: &mut Context<Self>,
    ) {
        let state = cx.global_mut::<AppState>();
        let Some(site) = state.sites.iter_mut().find(|site| site.id == src_id) else {
            return;
        };
        site.folder_id = folder_id;
        let pool = state.db.clone();
        state.runtime.spawn(async move {
            let _ = db::set_site_folder(&pool, src_id, folder_id).await;
        });
        cx.notify();
    }

    fn new_folder_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| InputState::new(window, cx).placeholder("folder name"));
        let app = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog, _, _| {
            let app = app.clone();
            let input = input.clone();
            dialog
                .title("New folder")
                .w(px(380.))
                .child(Input::new(&input))
                .confirm()
                .on_ok(move |_, _, cx| {
                    let name = input.read(cx).value().trim().to_string();
                    if name.is_empty() {
                        return false;
                    }
                    app.update(cx, |this, cx| this.create_folder(name, cx)).ok();
                    true
                })
        });
    }

    fn create_folder(&mut self, name: String, cx: &mut Context<Self>) {
        let space_id = self.active_space;
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            db::insert_folder(&pool, &name, None, None, space_id).await?;
            db::list_folders(&pool).await
        });
        self.sync_folders(task, cx);
    }

    fn delete_folder(&mut self, folder_id: i64, cx: &mut Context<Self>) {
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            db::delete_folder(&pool, folder_id).await?;
            db::list_folders(&pool).await
        });
        // Folder members fell back to top-level (FK sets folder_id NULL).
        for site in &mut cx.global_mut::<AppState>().sites {
            if site.folder_id == Some(folder_id) {
                site.folder_id = None;
            }
        }
        self.sync_folders(task, cx);
    }

    fn sync_folders(
        &mut self,
        task: tokio::task::JoinHandle<anyhow::Result<Vec<BookmarkFolder>>>,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("folder task failed: {join_error}")),
            };
            this.update(cx, |_, cx| {
                match result {
                    Ok(folders) => cx.global_mut::<AppState>().folders = folders,
                    Err(error) => tracing::error!(%error, "folder change failed"),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn open_bookmarks_dialog(
        &mut self,
        edit_site: Option<i64>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.bookmarks.update(cx, |manager, cx| match edit_site {
            Some(id) => manager.open_edit(id, window, cx),
            None => manager.refresh(cx),
        });
        let manager = self.bookmarks.clone();
        window.open_dialog(cx, move |dialog, _, _| {
            dialog.title("Bookmarks").w(px(560.)).child(manager.clone())
        });
    }

    /// Toggle the shared bottom dock: pressing a panel's key closes the dock
    /// if that panel is already showing, otherwise switches to (or opens) it.
    fn toggle_bottom(&mut self, panel: BottomPanel, cx: &mut Context<Self>) {
        self.bottom_dock = if self.bottom_dock == Some(panel) {
            None
        } else {
            Some(panel)
        };
        cx.notify();
    }

    /// The shared bottom dock: a tab strip (Transfers / Logs, with live
    /// badges) over the active panel. Panel identity + the single close button
    /// live here, so the panels themselves render only their own toolbars.
    fn render_bottom_dock(&self, active: BottomPanel, cx: &mut Context<Self>) -> AnyElement {
        // Transfers badge: in-flight jobs. Logs badge: warnings + errors.
        let transfers_badge = self
            .transfers
            .read(cx)
            .stats()
            .map(|stats| stats.active)
            .filter(|active| *active > 0)
            .map(|active| (SharedString::from(active.to_string()), false));
        let problems = self.logs.read(cx).problem_count();
        let logs_badge =
            (problems > 0).then(|| (SharedString::from(problems.to_string()), true));

        let content: AnyElement = match active {
            BottomPanel::Transfers => self.transfers.clone().into_any_element(),
            BottomPanel::Logs => self.logs.clone().into_any_element(),
        };

        v_flex()
            .size_full()
            .overflow_hidden()
            .bg(cx.theme().sidebar)
            .child(
                h_flex()
                    .px_2()
                    .py_0p5()
                    .gap_1()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(self.render_dock_tab(BottomPanel::Transfers, active, transfers_badge, cx))
                    .child(self.render_dock_tab(BottomPanel::Logs, active, logs_badge, cx))
                    .child(div().flex_1())
                    .child(
                        Button::new("close-dock")
                            .icon(Icon::new(IconName::Close))
                            .ghost()
                            .xsmall()
                            .tooltip(t("dock.close"))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.bottom_dock = None;
                                cx.notify();
                            })),
                    ),
            )
            .child(div().flex_1().min_h_0().w_full().child(content))
            .into_any_element()
    }

    fn render_dock_tab(
        &self,
        panel: BottomPanel,
        active: BottomPanel,
        badge: Option<(SharedString, bool)>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let is_active = panel == active;
        let id = match panel {
            BottomPanel::Transfers => "dock-tab-transfers",
            BottomPanel::Logs => "dock-tab-logs",
        };
        h_flex()
            .id(id)
            .gap_1p5()
            .items_center()
            .px_2()
            .py_0p5()
            .rounded_md()
            .cursor_pointer()
            .text_sm()
            .when(is_active, |el| {
                el.bg(cx.theme().list_active)
                    .text_color(cx.theme().foreground)
            })
            .when(!is_active, |el| {
                el.text_color(cx.theme().muted_foreground)
                    .hover(|style| style.bg(cx.theme().list_hover))
            })
            .child(Icon::default().path(panel.icon_path()).small())
            .child(panel.label())
            .children(badge.map(|(text, warn)| {
                let color = if warn {
                    cx.theme().warning
                } else {
                    cx.theme().muted_foreground
                };
                div()
                    .px_1()
                    .rounded_full()
                    .text_xs()
                    .bg(color.opacity(0.18))
                    .text_color(color)
                    .child(text)
            }))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.bottom_dock = Some(panel);
                cx.notify();
            }))
            .into_any_element()
    }

    fn toggle_sidebar(&mut self, cx: &mut Context<Self>) {
        self.sidebar_open = !self.sidebar_open;
        cx.notify();
    }

    // --- inline rename (sidebar rows) ---

    fn start_rename(
        &mut self,
        target: RenameTarget,
        current: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let input = cx.new(|cx| InputState::new(window, cx).default_value(current));
        cx.subscribe_in(&input, window, move |this, input, event: &InputEvent, window, cx| {
            if let InputEvent::PressEnter { .. } = event {
                let name = input.read(cx).value().trim().to_string();
                this.commit_rename(name, window, cx);
            }
        })
        .detach();
        let focus = input.focus_handle(cx);
        window.focus(&focus);
        self.rename = Some((target, input));
        cx.notify();
    }

    fn commit_rename(&mut self, name: String, _window: &mut Window, cx: &mut Context<Self>) {
        let Some((target, _)) = self.rename.take() else {
            return;
        };
        if name.is_empty() {
            cx.notify();
            return;
        }
        match target {
            RenameTarget::Tab(uid) => {
                if let Some(ix) = self.tab_ix(uid) {
                    self.tabs[ix].custom_name = Some(name);
                    self.persist_session(cx);
                }
            }
            RenameTarget::Bookmark(site_id) => {
                let state = cx.global_mut::<AppState>();
                if let Some(site) = state.sites.iter_mut().find(|site| site.id == site_id) {
                    site.name = name.clone();
                }
                let pool = state.db.clone();
                state.runtime.spawn(async move {
                    if let Err(error) = db::set_site_name(&pool, site_id, &name).await {
                        tracing::error!(%error, "renaming bookmark failed");
                    }
                });
            }
        }
        cx.notify();
    }

    fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        if self.rename.take().is_some() {
            cx.notify();
        }
    }

    /// The inline rename input for `target`, if that row is being renamed.
    fn rename_input(&self, target: RenameTarget) -> Option<Entity<InputState>> {
        self.rename
            .as_ref()
            .filter(|(current, _)| *current == target)
            .map(|(_, input)| input.clone())
    }

    /// Wrap a rename input so Escape cancels without committing.
    fn render_rename_input(&self, input: Entity<InputState>, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex_1()
            .min_w_0()
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, _, cx| {
                if event.keystroke.key == "escape" {
                    cx.stop_propagation();
                    this.cancel_rename(cx);
                }
            }))
            .child(Input::new(&input).xsmall())
            .into_any_element()
    }

    /// Reload the tag list (names + colors) after bookmark edits.
    fn refresh_tags(&mut self, cx: &mut Context<Self>) {
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move { db::list_tags(&pool).await });
        cx.spawn(async move |this, cx| {
            let Ok(Ok(tags)) = task.await else { return };
            this.update(cx, |_, cx| {
                cx.global_mut::<AppState>().tags = tags;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    // --- fixed-pixel panel dividers ---

    /// Drag handle between a fixed-pixel panel and the workspace. The drag
    /// itself is tracked by the root element's mouse-move/up handlers.
    fn panel_divider(&self, kind: PanelKind, cx: &mut Context<Self>) -> AnyElement {
        let id = match kind {
            PanelKind::Sidebar => "divider-sidebar",
            PanelKind::Dock => "divider-dock",
        };
        let base = div()
            .id(id)
            .flex_shrink_0()
            .bg(cx.theme().border)
            .hover(|style| style.bg(cx.theme().primary.opacity(0.6)))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, event: &MouseDownEvent, _, cx| {
                    cx.stop_propagation();
                    let ui = &cx.global::<AppState>().config.ui;
                    let (start_pos, start_size) = match kind {
                        PanelKind::Sidebar => (f32::from(event.position.x), ui.sidebar_width),
                        PanelKind::Dock => (f32::from(event.position.y), ui.dock_height),
                    };
                    this.panel_resize = Some(PanelResize {
                        kind,
                        start_pos,
                        start_size,
                    });
                }),
            );
        match kind {
            PanelKind::Sidebar => base.w(px(3.)).h_full().cursor_col_resize(),
            PanelKind::Dock => base.h(px(3.)).w_full().cursor_row_resize(),
        }
        .into_any_element()
    }

    fn on_panel_resize_move(&mut self, event: &MouseMoveEvent, cx: &mut Context<Self>) {
        let Some(resize) = self.panel_resize else {
            return;
        };
        if event.pressed_button != Some(MouseButton::Left) {
            self.panel_resize = None;
            return;
        }
        let sidebar_right = cx.global::<AppState>().config.ui.sidebar_position == "right";
        let value = match resize.kind {
            PanelKind::Sidebar => {
                let delta = f32::from(event.position.x) - resize.start_pos;
                let delta = if sidebar_right { -delta } else { delta };
                (resize.start_size + delta).clamp(160., 420.)
            }
            // The bottom dock grows as the divider moves up.
            PanelKind::Dock => {
                (resize.start_size + resize.start_pos - f32::from(event.position.y))
                    .clamp(120., 600.)
            }
        };
        // Whole pixels: the config writer rounds to one decimal, so an
        // unrounded value wouldn't survive the write→reload round-trip and the
        // watcher would churn (see `on_split_resized`).
        let value = value.round();
        let ui = &mut cx.global_mut::<AppState>().config.ui;
        let changed = match resize.kind {
            PanelKind::Sidebar => std::mem::replace(&mut ui.sidebar_width, value) != value,
            PanelKind::Dock => std::mem::replace(&mut ui.dock_height, value) != value,
        };
        if changed {
            self.schedule_ui_save(cx);
            cx.notify();
        }
    }

    // --- title bar ---

    /// Custom app header replacing the native titlebar; the window keeps the
    /// native traffic lights (transparent titlebar), the bar carries the
    /// sidebar toggle on the sidebar's side.
    fn render_title_bar(&self, sidebar_right: bool, cx: &mut Context<Self>) -> AnyElement {
        let toggle_icon = match (sidebar_right, self.sidebar_open) {
            (false, true) => IconName::PanelLeftClose,
            (false, false) => IconName::PanelLeftOpen,
            (true, true) => IconName::PanelRightClose,
            (true, false) => IconName::PanelRightOpen,
        };
        let toggle = Button::new("toggle-sidebar")
            .icon(Icon::new(toggle_icon))
            .ghost()
            .small()
            .tooltip(if self.sidebar_open {
                "Hide sidebar"
            } else {
                "Show sidebar"
            })
            .on_click(cx.listener(|this, _, _, cx| this.toggle_sidebar(cx)));

        // Swallow mouse-downs before they bubble to the TitleBar: fast
        // repeated clicks on these buttons must not read as a titlebar
        // double-click (which zooms/maximizes the window).
        let mut left = h_flex().gap_1().items_center().on_mouse_down(
            MouseButton::Left,
            |_: &MouseDownEvent, _, cx| cx.stop_propagation(),
        );
        // Windows / Linux: the app menus, in the title bar (see `crate::menu`).
        if let Some((_, menu_bar)) = &self.menu_bar {
            left = left.child(menu_bar.clone());
        }
        let mut right = h_flex().gap_1().items_center().on_mouse_down(
            MouseButton::Left,
            |_: &MouseDownEvent, _, cx| cx.stop_propagation(),
        );
        // Split-layout switcher lives in the app header, right side.
        if let Some(layout) = self.active_tab().and_then(|tab| match &tab.content {
            TabContent::Panes(workspace) => Some(workspace.layout),
            TabContent::Connect(_) => None,
        }) {
            right = right
                .child(self.layout_button(
                    "layout-single",
                    "icons/square.svg",
                    PaneLayout::Single,
                    layout,
                    "Single pane",
                    cx,
                ))
                .child(self.layout_button(
                    "layout-split-h",
                    "icons/columns-2.svg",
                    PaneLayout::SplitH,
                    layout,
                    "Split horizontally",
                    cx,
                ))
                .child(self.layout_button(
                    "layout-split-v",
                    "icons/rows-2.svg",
                    PaneLayout::SplitV,
                    layout,
                    "Split vertically",
                    cx,
                ));
        }
        if let Some(Tab { content: TabContent::Panes(workspace), .. }) = self.active_tab() {
            if workspace.layout != PaneLayout::Single {
                let linked = workspace.linked_roots.is_some();
                right = right.child(
                    Button::new("linked-browsing")
                        .label(if linked { "Unlink panes" } else { "Link panes" })
                        .selected(linked)
                        .ghost()
                        .small()
                        .tooltip("Follow matching subfolders from the two current directories")
                        .on_click(cx.listener(|this, _, _, cx| this.toggle_linked_browsing(cx))),
                );
            }
        }
        if sidebar_right {
            right = right.child(toggle);
        } else {
            left = left.child(toggle);
        }
        // Workspace context (like Zed's project name): the active Space's icon
        // and name, falling back to the product name if no space is active.
        let space = cx
            .global::<AppState>()
            .spaces
            .iter()
            .find(|space| space.id == self.active_space)
            .cloned();
        let space_chip = match space {
            Some(space) => {
                let mut icon =
                    Icon::new(bookmark_icon(space.icon.as_deref(), IconName::Frame)).small();
                if let Some(color) = bookmark_color(space.color.as_deref()) {
                    icon = icon.text_color(color);
                }
                h_flex()
                    .gap_1p5()
                    .items_center()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(icon)
                    .child(SharedString::from(space.name))
                    .into_any_element()
            }
            None => div()
                .text_sm()
                .text_color(cx.theme().muted_foreground)
                .child("kessel")
                .into_any_element(),
        };
        left = left.child(space_chip);

        TitleBar::new()
            .child(
                h_flex()
                    .flex_1()
                    .items_center()
                    .justify_between()
                    .pr_2()
                    .child(left)
                    .child(right),
            )
            .into_any_element()
    }

    // --- sidebar rendering ---

    fn tab_title(&self, tab: &Tab, cx: &App) -> (Icon, SharedString) {
        match &tab.content {
            TabContent::Connect(_) => (
                Icon::new(IconName::Plus),
                tab.custom_name
                    .clone()
                    .map(SharedString::from)
                    .unwrap_or_else(|| "New connection".into()),
            ),
            TabContent::Panes(workspace) => {
                let pane = workspace.panes[workspace.active].read(cx);
                let profile = pane.profile();
                let icon = if profile.id == LOCAL_SITE_ID {
                    Icon::new(IconName::Folder)
                } else {
                    let mut icon =
                        Icon::new(bookmark_icon(profile.icon.as_deref(), IconName::Globe));
                    if let Some(color) = bookmark_color(profile.color.as_deref()) {
                        icon = icon.text_color(color);
                    }
                    icon
                };
                if let Some(name) = &tab.custom_name {
                    return (icon, name.clone().into());
                }
                let dir = pane.current_dir();
                let leaf = dir
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .filter(|leaf| !leaf.is_empty());
                let title = match leaf {
                    Some(leaf) => format!("{} — {leaf}", pane.label()),
                    None => pane.label().to_string(),
                };
                (icon, title.into())
            }
        }
    }

    fn render_bookmark_row(&self, site: &SiteProfile, cx: &mut Context<Self>) -> AnyElement {
        let site_id = site.id;
        let open_tab = self.bookmark_tab(site_id);
        let is_active = open_tab == Some(self.active);
        let name: SharedString = site.name.clone().into();
        let ghost = name.clone();
        let mut icon = Icon::new(bookmark_icon(site.icon.as_deref(), IconName::Globe)).small();
        if let Some(color) = bookmark_color(site.color.as_deref()) {
            icon = icon.text_color(color);
        }
        let first_tag: Option<(SharedString, Option<gpui::Hsla>)> = site.tags.first().map(|tag| {
            let color = cx
                .global::<AppState>()
                .tags
                .iter()
                .find(|known| known.name == *tag)
                .and_then(|known| bookmark_color(known.color.as_deref()));
            (SharedString::from(tag.clone()), color)
        });
        let in_folder = site.folder_id.is_some();
        let rename_input = self.rename_input(RenameTarget::Bookmark(site_id));
        let renaming = rename_input.is_some();
        let name_for_rename = site.name.clone();

        h_flex()
            .id(("bookmark", site_id as usize))
            .group("sidebar-row")
            .gap_2()
            .mx_1()
            .px_2()
            .py_1()
            .when(in_folder, |el| el.pl_6())
            .text_sm()
            .cursor_pointer()
            .rounded_md()
            .when(is_active, |el| el.bg(cx.theme().list_active))
            .when(!is_active, |el| {
                el.hover(|style| style.bg(cx.theme().list_hover).opacity(1.))
            })
            // Bookmarks without a live tab sit dimmed until opened.
            .when(open_tab.is_none() && !renaming, |el| el.opacity(0.55))
            .when(!renaming, |el| {
                el.on_click(cx.listener(move |this, _, window, cx| {
                    this.open_bookmark(site_id, window, cx);
                }))
            })
            .on_drag(DraggedBookmark(site_id), move |_, _, _, cx| {
                let ghost = ghost.clone();
                cx.new(|_| SidebarDragGhost(ghost))
            })
            .drag_over::<DraggedBookmark>(|style, _, _, cx| {
                style.bg(cx.theme().primary.opacity(0.15))
            })
            .on_drop(cx.listener(move |this, drag: &DraggedBookmark, _, cx| {
                this.drop_bookmark_on(drag.0, site_id, cx);
            }))
            .context_menu({
                let weak = cx.entity().downgrade();
                let name_for_rename = name_for_rename.clone();
                move |menu, _, _| {
                    menu.item(PopupMenuItem::new("Rename").on_click({
                        let weak = weak.clone();
                        let name = name_for_rename.clone();
                        move |_, window, cx| {
                            weak.update(cx, |this, cx| {
                                this.start_rename(
                                    RenameTarget::Bookmark(site_id),
                                    name.clone(),
                                    window,
                                    cx,
                                );
                            })
                            .ok();
                        }
                    }))
                    .item(PopupMenuItem::new("Edit…").on_click({
                        let weak = weak.clone();
                        move |_, window, cx| {
                            weak.update(cx, |this, cx| {
                                this.open_bookmarks_dialog(Some(site_id), window, cx);
                            })
                            .ok();
                        }
                    }))
                    .item(PopupMenuItem::new("Duplicate").on_click({
                        let weak = weak.clone();
                        move |_, _, cx| {
                            weak.update(cx, |this, cx| this.duplicate_bookmark(site_id, cx))
                                .ok();
                        }
                    }))
                    .separator()
                    .item(PopupMenuItem::new("Delete…").on_click({
                        let weak = weak.clone();
                        move |_, window, cx| {
                            weak.update(cx, |this, cx| {
                                this.confirm_delete_bookmark(site_id, window, cx);
                            })
                            .ok();
                        }
                    }))
                }
            })
            .child(icon)
            .child(match rename_input {
                Some(input) => self.render_rename_input(input, cx),
                None => div().flex_1().min_w_0().truncate().child(name).into_any_element(),
            })
            .children(first_tag.filter(|_| !renaming).map(|(tag, color)| {
                let chip = div().px_1().rounded_sm().text_xs();
                match color {
                    Some(color) => chip.bg(color.opacity(0.18)).text_color(color),
                    None => chip
                        .bg(cx.theme().muted)
                        .text_color(cx.theme().muted_foreground),
                }
                .child(tag)
            }))
            .children(open_tab.map(|uid| {
                // Open bookmark = pinned tab: the close control lives right
                // on the bookmark row, revealed on hover.
                div()
                    .opacity(0.)
                    .group_hover("sidebar-row", |style| style.opacity(1.))
                    .child(
                        Button::new(("close-bookmark-tab", site_id as usize))
                            .icon(Icon::new(IconName::Close))
                            .ghost()
                            .xsmall()
                            .on_click(cx.listener(move |this, _, _, cx| {
                                // Button doesn't stop propagation; without
                                // this the row's own click handler re-opens
                                // the bookmark.
                                cx.stop_propagation();
                                this.close_tab(uid, cx);
                            })),
                    )
            }))
            .into_any_element()
    }

    fn render_folder_row(&self, folder: &BookmarkFolder, cx: &mut Context<Self>) -> AnyElement {
        let folder_id = folder.id;
        let collapsed = self.collapsed_folders.contains(&folder_id);
        let mut icon = Icon::new(bookmark_icon(folder.icon.as_deref(), IconName::Folder)).small();
        if let Some(color) = bookmark_color(folder.color.as_deref()) {
            icon = icon.text_color(color);
        }
        h_flex()
            .id(("folder", folder_id as usize))
            .gap_2()
            .mx_1()
            .px_2()
            .py_1()
            .text_sm()
            .cursor_pointer()
            .rounded_md()
            .hover(|style| style.bg(cx.theme().list_hover))
            .on_click(cx.listener(move |this, _, _, cx| {
                if !this.collapsed_folders.remove(&folder_id) {
                    this.collapsed_folders.insert(folder_id);
                }
                cx.notify();
            }))
            .drag_over::<DraggedBookmark>(|style, _, _, cx| {
                style.bg(cx.theme().primary.opacity(0.15))
            })
            .on_drop(cx.listener(move |this, drag: &DraggedBookmark, _, cx| {
                this.drop_bookmark_into_folder(drag.0, Some(folder_id), cx);
            }))
            .context_menu({
                let weak = cx.entity().downgrade();
                move |menu, _, _| {
                    menu.item(PopupMenuItem::new("Delete folder").on_click({
                        let weak = weak.clone();
                        move |_, _, cx| {
                            weak.update(cx, |this, cx| this.delete_folder(folder_id, cx))
                                .ok();
                        }
                    }))
                }
            })
            .child(
                Icon::new(if collapsed {
                    IconName::ChevronRight
                } else {
                    IconName::ChevronDown
                })
                .small()
                .text_color(cx.theme().muted_foreground),
            )
            .child(icon)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(SharedString::from(folder.name.clone())),
            )
            .into_any_element()
    }

    fn render_tab_row(&self, tab: &Tab, cx: &mut Context<Self>) -> AnyElement {
        let uid = tab.uid;
        let (icon, title) = self.tab_title(tab, cx);
        let is_active = uid == self.active;
        let ghost = title.clone();
        let rename_input = self.rename_input(RenameTarget::Tab(uid));
        let renaming = rename_input.is_some();
        let title_for_rename = title.to_string();
        h_flex()
            .id(("tab", uid as usize))
            .group("sidebar-row")
            .gap_2()
            .mx_1()
            .px_2()
            .py_1()
            .text_sm()
            .cursor_pointer()
            .rounded_md()
            .when(is_active, |el| el.bg(cx.theme().list_active))
            .when(!is_active && !renaming, |el| {
                // Background tabs sit dimmed; hover restores them.
                el.opacity(0.55)
                    .hover(|style| style.bg(cx.theme().list_hover).opacity(1.))
            })
            .when(!renaming, |el| {
                el.on_click(cx.listener(move |this, _, _, cx| {
                    this.activate_tab(uid, cx);
                }))
            })
            .on_drag(DraggedTab(uid), move |_, _, _, cx| {
                let ghost = ghost.clone();
                cx.new(|_| SidebarDragGhost(ghost))
            })
            .drag_over::<DraggedTab>(|style, _, _, cx| style.bg(cx.theme().primary.opacity(0.15)))
            .on_drop(cx.listener(move |this, drag: &DraggedTab, _, cx| {
                this.move_tab(drag.0, uid, cx);
            }))
            .context_menu({
                let weak = cx.entity().downgrade();
                move |menu, _, _| {
                    menu.item(PopupMenuItem::new("Rename").on_click({
                        let weak = weak.clone();
                        let title = title_for_rename.clone();
                        move |_, window, cx| {
                            weak.update(cx, |this, cx| {
                                this.start_rename(
                                    RenameTarget::Tab(uid),
                                    title.clone(),
                                    window,
                                    cx,
                                );
                            })
                            .ok();
                        }
                    }))
                    .item(PopupMenuItem::new("Close tab").on_click({
                        let weak = weak.clone();
                        move |_, _, cx| {
                            weak.update(cx, |this, cx| this.close_tab(uid, cx)).ok();
                        }
                    }))
                }
            })
            .child(icon.small())
            .child(match rename_input {
                Some(input) => self.render_rename_input(input, cx),
                None => div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(title)
                    .into_any_element(),
            })
            .child(
                // Close affordance appears on row hover only.
                div()
                    .opacity(0.)
                    .group_hover("sidebar-row", |style| style.opacity(1.))
                    .child(
                        Button::new(("close-tab", uid as usize))
                            .icon(Icon::new(IconName::Close))
                            .ghost()
                            .xsmall()
                            .on_click(cx.listener(move |this, _, _, cx| {
                                cx.stop_propagation();
                                this.close_tab(uid, cx);
                            })),
                    ),
            )
            .into_any_element()
    }

    fn render_space_switcher(&self, cx: &mut Context<Self>) -> AnyElement {
        let spaces = cx.global::<AppState>().spaces.clone();
        h_flex()
            .relative()
            .p_2()
            .gap_1()
            .justify_center()
            .border_t_1()
            .border_color(cx.theme().border)
            .children(spaces.iter().map(|space| {
                let space_id = space.id;
                let is_active = space_id == self.active_space;
                let mut icon = Icon::new(bookmark_icon(space.icon.as_deref(), IconName::Frame));
                if let Some(color) = bookmark_color(space.color.as_deref()) {
                    icon = icon.text_color(color);
                }
                let button = Button::new(("space", space_id as usize))
                    .icon(icon)
                    .small()
                    .ghost()
                    .selected(is_active)
                    .tooltip(SharedString::from(space.name.clone()));
                div()
                    .id(("space-slot", space_id as usize))
                    .context_menu({
                        let weak = cx.entity().downgrade();
                        move |menu, _, _| {
                            menu.item(PopupMenuItem::new("Delete space").on_click({
                                let weak = weak.clone();
                                move |_, window, cx| {
                                    weak.update(cx, |this, cx| {
                                        this.delete_space(space_id, window, cx)
                                    })
                                    .ok();
                                }
                            }))
                        }
                    })
                    .child(button.on_click(cx.listener(move |this, _, _, cx| {
                        this.switch_space(space_id, cx);
                    })))
            }))
            .child(
                // Pinned right while the space icons stay centered.
                div()
                    .absolute()
                    .right_2()
                    .child(self.render_new_space_popover(cx)),
            )
            .into_any_element()
    }

    /// "+" popover with the new-space form: name, icon grid, color swatches.
    fn render_new_space_popover(&self, cx: &mut Context<Self>) -> Popover {
        let app = cx.entity().downgrade();
        let name_input = self.space_name_input.clone();
        Popover::new("new-space")
            .anchor(Corner::BottomRight)
            .trigger(
                Button::new("add-space")
                    .icon(Icon::new(IconName::Plus))
                    .ghost()
                    .small()
                    .tooltip("New space"),
            )
            .content(move |_, _, cx| {
                let popover = cx.entity();
                let (icon, color) = app
                    .upgrade()
                    .map(|app| {
                        let app = app.read(cx);
                        (app.space_icon.clone(), app.space_color.clone())
                    })
                    .unwrap_or_default();

                let pick_icon: std::rc::Rc<dyn Fn(Option<String>, &mut Window, &mut App)> = {
                    let app = app.clone();
                    std::rc::Rc::new(move |value, _, cx| {
                        app.update(cx, |this, cx| {
                            this.space_icon = value;
                            cx.notify();
                        })
                        .ok();
                    })
                };
                let pick_color: std::rc::Rc<dyn Fn(Option<String>, &mut Window, &mut App)> = {
                    let app = app.clone();
                    std::rc::Rc::new(move |value, _, cx| {
                        app.update(cx, |this, cx| {
                            this.space_color = value;
                            cx.notify();
                        })
                        .ok();
                    })
                };

                let create = {
                    let app = app.clone();
                    let name_input = name_input.clone();
                    let popover = popover.clone();
                    move |_: &gpui::ClickEvent, window: &mut Window, cx: &mut App| {
                        let name = name_input.read(cx).value().trim().to_string();
                        if name.is_empty() {
                            return;
                        }
                        app.update(cx, |this, cx| {
                            let icon = this.space_icon.take();
                            let color = this.space_color.take();
                            this.create_space(name, icon, color, cx);
                        })
                        .ok();
                        name_input.update(cx, |input, cx| input.set_value("", window, cx));
                        popover.update(cx, |state, cx| state.dismiss(window, cx));
                    }
                };

                v_flex()
                    .w(px(240.))
                    .gap_2()
                    .p_1()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .child("New space"),
                    )
                    .child(Input::new(&name_input).small())
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child("Icon"),
                    )
                    .child(crate::ui::pickers::icon_grid(
                        icon,
                        IconName::Frame,
                        pick_icon,
                    ))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child("Color"),
                    )
                    .child(crate::ui::pickers::color_grid(color, pick_color, cx))
                    .child(
                        h_flex().justify_end().child(
                            Button::new("create-space")
                                .label("Create")
                                .primary()
                                .small()
                                .on_click(create),
                        ),
                    )
            })
    }

    fn render_sidebar(&mut self, cx: &mut Context<Self>) -> AnyElement {
        // Bookmarks and folders belong to the active space, like tabs.
        let state = cx.global::<AppState>();
        let sites: Vec<SiteProfile> = state
            .sites
            .iter()
            .filter(|site| site.space_id == self.active_space)
            .cloned()
            .collect();
        let folders: Vec<BookmarkFolder> = state
            .folders
            .iter()
            .filter(|folder| folder.space_id == self.active_space)
            .cloned()
            .collect();
        let space = state
            .spaces
            .iter()
            .find(|space| space.id == self.active_space)
            .cloned();

        let mut bookmark_rows: Vec<AnyElement> = Vec::new();
        // Folders first, each with its members (unless collapsed) …
        for folder in &folders {
            bookmark_rows.push(self.render_folder_row(folder, cx));
            if !self.collapsed_folders.contains(&folder.id) {
                for site in sites
                    .iter()
                    .filter(|site| site.folder_id == Some(folder.id))
                {
                    bookmark_rows.push(self.render_bookmark_row(site, cx));
                }
            }
        }
        // … then loose bookmarks.
        for site in sites.iter().filter(|site| site.folder_id.is_none()) {
            bookmark_rows.push(self.render_bookmark_row(site, cx));
        }

        // Ephemeral tabs only — bookmark (pinned) tabs live on their rows.
        let tab_rows: Vec<AnyElement> = self
            .tabs
            .iter()
            .filter(|tab| tab.space_id == self.active_space && tab.bookmark_id.is_none())
            .map(|tab| self.render_tab_row(tab, cx))
            .collect();

        v_flex()
            // Fill the column via flex-grow, not a percentage height: with a
            // vertical split the ancestor height resolves late, so `size_full`
            // would collapse to content height and unpin the space switcher.
            .flex_1()
            .min_h_0()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().sidebar)
            .text_color(cx.theme().sidebar_foreground)
            // Mouse back/forward buttons over the sidebar step through spaces.
            .on_mouse_down(
                MouseButton::Navigate(NavigationDirection::Back),
                cx.listener(|this, _: &MouseDownEvent, _, cx| this.cycle_space(-1, cx)),
            )
            .on_mouse_down(
                MouseButton::Navigate(NavigationDirection::Forward),
                cx.listener(|this, _: &MouseDownEvent, _, cx| this.cycle_space(1, cx)),
            )
            .child(
                // Space header (icon + name) doubles as the "remove from
                // folder" target for its bookmarks.
                h_flex()
                    .id("bookmarks-header")
                    .px_4()
                    .pt_3()
                    .pb_3()
                    .justify_between()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .drag_over::<DraggedBookmark>(|style, _, _, cx| {
                        style.bg(cx.theme().primary.opacity(0.15))
                    })
                    .on_drop(cx.listener(|this, drag: &DraggedBookmark, _, cx| {
                        this.drop_bookmark_into_folder(drag.0, None, cx);
                    }))
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .children(space.as_ref().map(|space| {
                                let mut icon = Icon::new(bookmark_icon(
                                    space.icon.as_deref(),
                                    IconName::Frame,
                                ))
                                .small();
                                if let Some(color) = bookmark_color(space.color.as_deref()) {
                                    icon = icon.text_color(color);
                                }
                                icon
                            }))
                            .child(
                                div()
                                    .text_color(cx.theme().primary)
                                    .font_weight(gpui::FontWeight::MEDIUM)
                                    .child(SharedString::from(
                                        space
                                            .as_ref()
                                            .map(|space| space.name.clone())
                                            .unwrap_or_else(|| "Bookmarks".to_string()),
                                    )),
                            ),
                    )
                    .child(
                        Button::new("new-folder")
                            .icon(Icon::default().path("icons/folder-plus.svg"))
                            .ghost()
                            .xsmall()
                            .tooltip("New folder")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.new_folder_dialog(window, cx);
                            })),
                    ),
            )
            .child(
                v_flex()
                    .id("bookmark-list")
                    .px_1()
                    .gap_0p5()
                    .max_h(px(320.))
                    // Never cede height to the rest of the sidebar (e.g.
                    // when the transfers pane re-layouts the window); long
                    // lists scroll instead.
                    .flex_shrink_0()
                    .overflow_y_scroll()
                    .children(bookmark_rows)
                    .when(sites.is_empty(), |el| {
                        el.child(
                            div()
                                .px_2()
                                .py_1()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child("No bookmarks yet."),
                        )
                    }),
            )
            .child(
                // Separator between persisted bookmarks and ephemeral tabs.
                // `border` instead of `sidebar_border`: several themes leave
                // the latter identical to the sidebar background.
                div()
                    .mx_2()
                    .my_2()
                    .h(px(1.))
                    .flex_shrink_0()
                    .bg(cx.theme().border),
            )
            .child(
                v_flex()
                    .px_1()
                    .gap_0p5()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(
                        // A new tab *is* a new connection: the empty tab
                        // opens on the connect form. Styled like a tab row,
                        // pinned above the ephemeral tabs.
                        h_flex()
                            .id("new-tab")
                            .gap_2()
                            .px_2()
                            .py_1()
                            .text_sm()
                            .cursor_pointer()
                            .rounded_md()
                            .opacity(0.55)
                            .hover(|style| style.bg(cx.theme().list_hover).opacity(1.))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.push_connection_tab(window, cx);
                            }))
                            .child(Icon::new(IconName::Plus).small())
                            .child(div().flex_1().min_w_0().truncate().child("New tab")),
                    )
                    .children(tab_rows),
            )
            .child(self.render_space_switcher(cx))
            .into_any_element()
    }

    // --- footer ---

    /// Gauge popover: runtime controls for the queue's global limits.
    fn render_limits_popover(&self) -> impl IntoElement {
        let parallel = self.parallel_slider.clone();
        let throttle = self.throttle_slider.clone();
        Popover::new("queue-limits")
            .anchor(Corner::BottomRight)
            .trigger(
                Button::new("queue-limits-trigger")
                    .icon(Icon::default().path("icons/gauge.svg"))
                    .ghost()
                    .small()
                    .tooltip("Transfer limits"),
            )
            .content(move |_, _, cx| {
                let par = parallel.read(cx).value().start() as usize;
                let bw = throttle.read(cx).value().start();
                v_flex()
                    .w(px(280.))
                    .gap_2()
                    .p_2()
                    .child(
                        h_flex()
                            .justify_between()
                            .text_sm()
                            .child("Parallel transfers")
                            .child(format!("{par}")),
                    )
                    .child(Slider::new(&parallel))
                    .child(
                        h_flex()
                            .justify_between()
                            .text_sm()
                            .child("Bandwidth limit")
                            .child(if bw < 1.0 {
                                "Unlimited".to_string()
                            } else {
                                format!("{bw:.0} MB/s")
                            }),
                    )
                    .child(Slider::new(&throttle))
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child("The bandwidth limit applies to transfers started from now on."),
                    )
            })
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let stats = self.transfers.read(cx).stats();
        let problems = self.logs.read(cx).problem_count();

        // Dock toggles: a ghost icon whose active state is a subtle filled box
        // (Selectable), not a bold accent fill.
        let transfers_toggle = Button::new("transfers")
            .icon(Icon::default().path("icons/list-checks.svg"))
            .small()
            .ghost()
            .selected(self.bottom_dock == Some(BottomPanel::Transfers))
            .tooltip(t("dock.transfers"));
        let logs_toggle = Button::new("logs")
            .icon(Icon::default().path("icons/square-terminal.svg"))
            .small()
            .ghost()
            .selected(self.bottom_dock == Some(BottomPanel::Logs))
            .tooltip(t("menu.view.toggle_logs"));

        // Focused-pane context: which connection is active and its item count.
        let active_pane = self.active_tab().and_then(|tab| match &tab.content {
            TabContent::Panes(workspace) => Some(workspace.panes[workspace.active].clone()),
            TabContent::Connect(_) => None,
        });
        let (site_chip, pane_summary) = match &active_pane {
            Some(pane) => {
                let pane = pane.read(cx);
                let profile = pane.profile();
                let icon = if profile.id == LOCAL_SITE_ID {
                    Icon::new(IconName::Folder)
                } else {
                    let mut icon =
                        Icon::new(bookmark_icon(profile.icon.as_deref(), IconName::Globe));
                    if let Some(color) = bookmark_color(profile.color.as_deref()) {
                        icon = icon.text_color(color);
                    }
                    icon
                };
                let chip = h_flex()
                    .gap_1()
                    .items_center()
                    .child(icon.small())
                    .child(div().text_xs().child(pane.label()));
                (Some(chip), Some(pane.item_summary()))
            }
            None => (None, None),
        };

        // Left cluster: dock toggles · diagnostics · active-site chip.
        let left = h_flex()
            .gap_1()
            .items_center()
            .child(transfers_toggle.on_click(cx.listener(|this, _, _, cx| {
                this.toggle_bottom(BottomPanel::Transfers, cx);
            })))
            .child(logs_toggle.on_click(cx.listener(|this, _, _, cx| {
                this.toggle_bottom(BottomPanel::Logs, cx);
            })))
            .when(problems > 0, |el| {
                // Warning/error tally; click opens the Logs panel.
                el.child(
                    h_flex()
                        .id("footer-problems")
                        .gap_1()
                        .items_center()
                        .cursor_pointer()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.bottom_dock = Some(BottomPanel::Logs);
                            cx.notify();
                        }))
                        .child(
                            Icon::new(IconName::TriangleAlert)
                                .small()
                                .text_color(cx.theme().warning),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().warning)
                                .child(problems.to_string()),
                        ),
                )
            })
            .children(site_chip);

        h_flex()
            .px_2()
            .py_0p5()
            .gap_2()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .border_t_1()
            .border_color(cx.theme().border)
            .child(left)
            .child(div().flex_1())
            // Right cluster: focused-pane summary · queue · update · limits.
            .children(
                pane_summary.map(|summary| div().text_xs().whitespace_nowrap().child(summary)),
            )
            .when_some(stats, |this, stats| {
                // Live queue summary; clicking it reveals the transfers pane.
                this.child(
                    h_flex()
                        .id("footer-transfer-stats")
                        .gap_2()
                        .items_center()
                        .cursor_pointer()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.bottom_dock = Some(BottomPanel::Transfers);
                            cx.notify();
                        }))
                        .when(stats.conflicts > 0, |el| {
                            el.child(
                                Icon::new(IconName::TriangleAlert)
                                    .small()
                                    .text_color(cx.theme().warning),
                            )
                        })
                        .child(div().text_xs().child(stats.summary()))
                        .child(
                            div()
                                .w(px(120.))
                                .child(Progress::new().value(stats.percent())),
                        ),
                )
            })
            .when_some(self.available_update.clone(), |this, info| {
                // Unobtrusive "update ready" affordance; opens the update dialog.
                this.child(
                    Button::new("update-available")
                        .label(tf("update.footer", &[("version", &info.version)]))
                        .small()
                        .primary()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_update_dialog(window, cx);
                        })),
                )
            })
            .child(self.render_limits_popover())
            .into_any_element()
    }

    // --- main content rendering ---

    fn render_workspace(&self, workspace: &Workspace, cx: &App) -> AnyElement {
        let ui = &cx.global::<AppState>().config.ui;
        match workspace.layout {
            // Single pane: the global status bar carries its item summary, so
            // the pane draws no status line of its own.
            PaneLayout::Single => div()
                .size_full()
                .child(workspace.panes[workspace.active].clone())
                .into_any_element(),
            PaneLayout::SplitH => h_resizable("panes-h")
                .with_state(&workspace.split_h)
                .child(
                    resizable_panel()
                        .size(px(ui.split_h_width.clamp(240., 1600.)))
                        .size_range(px(240.)..px(1600.))
                        .child(self.pane_with_status(&workspace.panes[0], cx)),
                )
                .child(resizable_panel().child(self.pane_with_status(&workspace.panes[1], cx)))
                .into_any_element(),
            PaneLayout::SplitV => v_resizable("panes-v")
                .with_state(&workspace.split_v)
                .child(
                    resizable_panel()
                        .size(px(ui.split_v_height.clamp(150., 1200.)))
                        .size_range(px(150.)..px(1200.))
                        .child(self.pane_with_status(&workspace.panes[0], cx)),
                )
                .child(resizable_panel().child(self.pane_with_status(&workspace.panes[1], cx)))
                .into_any_element(),
        }
    }

    /// A split pane wrapped with its own thin status line (site label + item
    /// summary) — in split view each pane shows its own counts, since the
    /// global status bar can only reflect the focused one.
    fn pane_with_status(&self, pane: &Entity<FilePane>, cx: &App) -> AnyElement {
        let pane_ref = pane.read(cx);
        let label = pane_ref.label();
        let summary = pane_ref.item_summary();
        v_flex()
            .size_full()
            .overflow_hidden()
            .child(div().flex_1().min_h_0().w_full().child(pane.clone()))
            .child(
                h_flex()
                    .px_2()
                    .py_0p5()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .justify_between()
                    .child(label)
                    .child(summary),
            )
            .into_any_element()
    }

    fn layout_button(
        &self,
        id: &'static str,
        icon_path: &'static str,
        layout: PaneLayout,
        current: PaneLayout,
        tooltip: &'static str,
        cx: &mut Context<Self>,
    ) -> Button {
        Button::new(id)
            .icon(Icon::default().path(icon_path))
            .small()
            .ghost()
            .selected(layout == current)
            .tooltip(tooltip)
            .on_click(cx.listener(move |this, _, _, cx| this.set_layout(layout, cx)))
    }

    fn render_tab_content(&mut self, cx: &mut Context<Self>) -> AnyElement {
        // No tab selected (fresh space, last tab closed): an inviting blank,
        // never a synthesized local browser.
        let Some(tab) = self.active_tab() else {
            return v_flex()
                .size_full()
                .items_center()
                .justify_center()
                .gap_3()
                .text_color(cx.theme().muted_foreground)
                .child(Icon::new(IconName::Globe))
                .child(div().text_sm().child("No open tabs in this space."))
                .child(
                    Button::new("empty-new-tab")
                        .label("New tab")
                        .icon(Icon::new(IconName::Plus))
                        .outline()
                        .small()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.push_connection_tab(window, cx);
                        })),
                )
                .into_any_element();
        };
        match &tab.content {
            // Centered column that scrolls inside the pane, so a tall form
            // never pushes into the footer. Column flow (not a row with
            // stretch): the child must keep its natural height, otherwise
            // the scroll extent never exceeds the container.
            TabContent::Connect(manager) => v_flex()
                .id("connect-scroll")
                .size_full()
                .min_h_0()
                .overflow_y_scroll()
                .overflow_x_hidden()
                .items_center()
                .child(
                    v_flex()
                        .w_full()
                        .max_w(px(560.))
                        .p_4()
                        .pb_8()
                        .child(div().text_lg().mb_2().child("New connection"))
                        .child(manager.clone()),
                )
                .into_any_element(),
            // The layout switcher lives in the app header now; the panes
            // get the full tab height.
            TabContent::Panes(workspace) => {
                let panes = self.render_workspace(workspace, cx);
                div()
                    .size_full()
                    .overflow_hidden()
                    .child(panes)
                    .into_any_element()
            }
        }
    }
}

/// Promote an in-flight internal row drag to a native OS drag session —
/// gpui has no outbound drag support, so once the pointer leaves the window
/// the gpui drag is stopped and the still-held gesture is handed to the
/// platform (see `crate::dragout`). Local entries travel as real file URLs;
/// remote ones as file promises that run through the transfer queue when
/// Finder (or any other app) accepts the drop.
fn promote_drag_out(drag: DraggedRowEntries, window: &mut Window, cx: &mut App) {
    let state = cx.global::<AppState>();
    let queue = state.queue.clone();
    let runtime = state.runtime.handle().clone();
    let local = drag.site_id == LOCAL_SITE_ID;

    let items: Vec<crate::dragout::DragItem> = drag
        .entries
        .iter()
        .map(|entry| crate::dragout::DragItem {
            name: entry.name.to_string(),
            is_dir: entry.is_dir,
            // Local-site browse paths are root-relative (dirs keep a trailing
            // slash) — see `localfs`.
            local_path: local
                .then(|| crate::localfs::full_path(&crate::localfs::root(), &entry.path)),
        })
        .collect();

    let site_id = drag.site_id;
    let sources: Vec<String> = drag.entries.iter().map(|entry| entry.path.clone()).collect();
    let fulfill: crate::dragout::FulfillFn = Box::new(move |ix, dest| {
        use anyhow::Context as _;
        let src = sources.get(ix).context("unknown drag item")?.clone();
        let dir = dest.parent().context("promise destination has no parent")?;
        let name = dest.file_name().context("promise destination has no name")?;
        let job = NewJob {
            src_site_id: site_id,
            dst_site_id: LOCAL_SITE_ID,
            src,
            dst_dir: crate::ui::file_pane::local_dir_path(dir),
            dst_name: Some(name.to_string_lossy().to_string()),
            // The destination already chose the final (unique) name; never
            // park a promise on a conflict prompt.
            conflict_policy: ConflictPolicy::Overwrite,
            move_source: false,
        };
        let queue = queue.clone();
        runtime.block_on(async move {
            let job_id = queue
                .enqueue_tracked(job)
                .await
                .map_err(|_| anyhow::anyhow!("transfer was rejected"))?;
            match queue.wait_terminal(job_id).await {
                JobState::Done => Ok(()),
                state => {
                    let detail = queue
                        .latest()
                        .jobs
                        .iter()
                        .find(|job| job.id == job_id)
                        .and_then(|job| job.error.clone());
                    Err(match detail {
                        Some(error) => anyhow::anyhow!(error),
                        None => anyhow::anyhow!("transfer {}", state.as_str()),
                    })
                }
            }
        })
    });

    if crate::dragout::start_drag(window, items, fulfill) {
        cx.stop_active_drag(window);
    }
}

impl Render for KesselApp {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let ui = cx.global::<AppState>().config.ui.clone();
        let sidebar_right = ui.sidebar_position == "right";

        // Deferred focus from activate_tab (which has no window access).
        if std::mem::take(&mut self.pending_pane_focus) {
            if let Some(tab) = self.active_tab() {
                if let TabContent::Panes(workspace) = &tab.content {
                    let focus = workspace.panes[workspace.active].focus_handle(cx);
                    window.focus(&focus);
                }
            }
        }
        // Keyboard shortcuts must work everywhere: when nothing holds focus
        // (empty space, freshly closed dialog), park it on the workspace so
        // Workspace-context bindings (quit, tabs, panels) keep firing.
        if window.focused(cx).is_none() && self.rename.is_none() {
            window.focus(&self.focus_handle);
        }

        // Where the OS draws no menu bar, kessel draws one in its title bar.
        // Rebuilt when `menu::install` bumps the epoch (locale change), since
        // AppMenuBar snapshots the menus at construction.
        if !cfg!(target_os = "macos") {
            let epoch = crate::menu::epoch();
            if self.menu_bar.as_ref().is_none_or(|(seen, _)| *seen != epoch) {
                let bar = gpui_component::menu::AppMenuBar::new(window, cx);
                self.menu_bar = Some((epoch, bar));
            }
        }

        let title_bar = self.render_title_bar(sidebar_right, cx);
        let tab_content = self.render_tab_content(cx);
        let footer = self.render_footer(cx);

        // The shared bottom dock (Transfers / Logs) sits below the tab
        // content, global across all tabs, spaces and connections. Its height
        // is a fixed pixel value from the config — it doesn't scale with the
        // window; the divider drag updates the config value directly.
        let mut main: AnyElement = tab_content;
        if let Some(panel) = self.bottom_dock {
            main = v_flex()
                .size_full()
                .overflow_hidden()
                .child(div().flex_1().min_h_0().w_full().child(main))
                .child(self.panel_divider(PanelKind::Dock, cx))
                .child(
                    div()
                        .h(px(ui.dock_height.clamp(120., 600.)))
                        .flex_shrink_0()
                        .w_full()
                        .overflow_hidden()
                        .child(self.render_bottom_dock(panel, cx)),
                )
                .into_any_element();
        }

        let workspace = div().size_full().min_w_0().overflow_hidden().child(
            v_flex()
                .size_full()
                .overflow_hidden()
                .child(div().flex_1().min_h_0().w_full().child(main))
                .child(footer),
        );

        // Fixed-pixel sidebar with a drag divider; hidden entirely when
        // toggled off.
        let body: AnyElement = if !self.sidebar_open {
            workspace.into_any_element()
        } else {
            let sidebar = div()
                .flex()
                .flex_col()
                .w(px(ui.sidebar_width.clamp(160., 420.)))
                .flex_shrink_0()
                .h_full()
                .min_h_0()
                .overflow_hidden()
                .child(self.render_sidebar(cx));
            let divider = self.panel_divider(PanelKind::Sidebar, cx);
            let workspace = div().flex_1().min_w_0().min_h_0().h_full().child(workspace);
            // `h_flex()` centers children on the cross axis; with a vertical
            // split the workspace column reports a tall intrinsic height, which
            // leaves the sidebar's `h_full` resolving to content height and
            // floating centered. Use a plain flex row (default `stretch`) so
            // both columns fill top to bottom, and clamp the row height so the
            // split can't push it.
            let row = div().flex().flex_row().size_full().min_h_0();
            if sidebar_right {
                row.child(workspace).child(divider).child(sidebar)
            } else {
                row.child(sidebar).child(divider).child(workspace)
            }
            .into_any_element()
        };

        // Root only renders our view; the dialog / sheet / notification
        // overlays are drawn by the top-level view — without these the
        // Bookmarks dialog is registered but never painted.
        div()
            .relative()
            .size_full()
            .key_context("Workspace")
            .track_focus(&self.focus_handle)
            // Panel divider drags are tracked window-wide so they survive
            // the pointer leaving the 3px handle.
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                this.on_panel_resize_move(event, cx);
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _: &MouseUpEvent, _, cx| {
                    if this.panel_resize.take().is_some() {
                        cx.notify();
                    }
                }),
            )
            .on_action(cx.listener(Self::on_new_tab))
            .on_action(cx.listener(Self::on_close_tab))
            .on_action(cx.listener(Self::on_next_tab))
            .on_action(cx.listener(Self::on_prev_tab))
            .on_action(cx.listener(Self::on_activate_tab))
            .on_action(cx.listener(Self::on_switch_space))
            .on_action(cx.listener(Self::on_focus_other_pane))
            .on_action(cx.listener(Self::on_layout_single))
            .on_action(cx.listener(Self::on_layout_split_h))
            .on_action(cx.listener(Self::on_layout_split_v))
            .on_action(cx.listener(Self::on_toggle_sidebar))
            .on_action(cx.listener(Self::on_toggle_transfers))
            .on_action(cx.listener(Self::on_toggle_logs))
            .on_action(cx.listener(Self::on_open_bookmarks))
            .on_action(cx.listener(Self::on_about))
            .on_action(cx.listener(Self::on_open_preferences))
            .on_action(cx.listener(Self::on_open_theme_browser))
            .on_action(cx.listener(Self::on_toggle_command_bar))
            .on_action(cx.listener(Self::on_sync_panes))
            .on_action(cx.listener(Self::on_help))
            .on_action(cx.listener(Self::on_check_for_updates))
            .on_action(cx.listener(Self::on_quit))
            .child(
                v_flex()
                    .size_full()
                    .child(title_bar)
                    .child(div().flex_1().min_h_0().w_full().child(body)),
            )
            // Invisible per-frame hook: window-level mouse listeners (they
            // fire even outside the window while the button is held, which
            // element listeners never do) that promote an internal row drag
            // to a native OS drag session the moment it crosses the window
            // edge — the only path for dropping entries onto Finder.
            .child(
                canvas(
                    |_, _, _| (),
                    |_, _, window, _| {
                        window.on_mouse_event(
                            move |event: &MouseMoveEvent, phase, window, cx| {
                                if phase != DispatchPhase::Bubble
                                    || event.pressed_button != Some(MouseButton::Left)
                                    || !cx.has_active_drag()
                                {
                                    return;
                                }
                                let size = window.viewport_size();
                                let position = event.position;
                                let inside = position.x >= px(0.)
                                    && position.y >= px(0.)
                                    && position.x <= size.width
                                    && position.y <= size.height;
                                if inside {
                                    return;
                                }
                                let Some(drag) = cx
                                    .try_global::<PendingDragOut>()
                                    .and_then(|pending| pending.0.clone())
                                else {
                                    return;
                                };
                                cx.global_mut::<PendingDragOut>().0 = None;
                                promote_drag_out(drag, window, cx);
                            },
                        );
                        window.on_mouse_event(move |_: &MouseUpEvent, phase, _, cx| {
                            if phase == DispatchPhase::Bubble
                                && cx
                                    .try_global::<PendingDragOut>()
                                    .is_some_and(|pending| pending.0.is_some())
                            {
                                cx.global_mut::<PendingDragOut>().0 = None;
                            }
                        });
                    },
                )
                .absolute()
                .size_0(),
            )
            // Command bar overlay: centered near the top, backdrop click
            // dismisses.
            .children(self.command_bar.clone().map(|bar| {
                div()
                    .absolute()
                    .inset_0()
                    .flex()
                    .items_start()
                    .justify_center()
                    .pt_20()
                    .bg(gpui::black().opacity(0.2))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _: &MouseDownEvent, _, cx| {
                            this.close_command_bar(cx);
                        }),
                    )
                    .child(div().on_mouse_down(
                        MouseButton::Left,
                        |_: &MouseDownEvent, _, cx| cx.stop_propagation(),
                    ).child(bar))
            }))
            .children(Root::render_sheet_layer(window, cx))
            .children(Root::render_dialog_layer(window, cx))
            .children(Root::render_notification_layer(window, cx))
    }
}

pub fn init(cx: &mut App) {
    gpui_component::init(cx);
}

/// Hot reload for `config.json5`: watch the file, re-parse on change, and
/// apply every value live — theme + icon theme, logging filter, transfer
/// engine knobs, and the UI config (panel sizes and layout are re-applied by
/// `KesselApp`, which observes the bumped `config_epoch`).
pub fn watch_config(cx: &mut App) {
    let Ok(data_dir) = crate::config::data_dir() else {
        return;
    };
    let config_path = data_dir.join("config.json5");

    let (tx, rx) = smol::channel::bounded::<()>(16);
    let watched = config_path.clone();
    let mut watcher = match notify::recommended_watcher(
        move |result: notify::Result<notify::Event>| {
            let Ok(event) = result else { return };
            if !matches!(
                event.kind,
                notify::EventKind::Create(_)
                    | notify::EventKind::Modify(_)
                    | notify::EventKind::Remove(_)
            ) {
                return;
            }
            // The data dir also holds SQLite files that churn constantly —
            // only config.json5 changes matter here.
            if event.paths.iter().any(|path| path.ends_with("config.json5") || *path == watched) {
                tx.try_send(()).ok();
            }
        },
    ) {
        Ok(watcher) => watcher,
        Err(error) => {
            tracing::warn!(%error, "config hot reload disabled: cannot create watcher");
            return;
        }
    };
    // Watch the directory, not the file: editors replace files on save,
    // which breaks single-file watches.
    if let Err(error) = notify::Watcher::watch(&mut watcher, &data_dir, notify::RecursiveMode::NonRecursive) {
        tracing::warn!(%error, "config hot reload disabled: cannot watch data dir");
        return;
    }

    cx.spawn(async move |cx| {
        let _watcher = watcher; // keep watching for the app's lifetime
        while rx.recv().await.is_ok() {
            // Let the write burst settle, then coalesce queued events.
            cx.background_executor()
                .timer(Duration::from_millis(300))
                .await;
            while rx.try_recv().is_ok() {}

            let applied = cx.update(|cx| {
                let new = match crate::config::load() {
                    Ok(new) => new,
                    Err(error) => {
                        tracing::warn!(%error, "config.json5 is invalid; keeping current config");
                        return;
                    }
                };
                let old = cx.global::<AppState>().config.clone();
                if new == old {
                    return;
                }
                tracing::info!("reloading config.json5");
                if new.ui != old.ui {
                    crate::theme::apply(&new.ui, cx);
                    crate::ui::file_icons::FileIcons::reload(&new.ui, cx);
                }
                if new.ui.locale != old.ui.locale {
                    crate::i18n::init(&new.ui.locale);
                    crate::menu::install(cx);
                }
                if new.logging.filter != old.logging.filter {
                    if let Some(handle) = crate::config::LOG_FILTER.get() {
                        match new.logging.filter.parse::<tracing_subscriber::EnvFilter>() {
                            Ok(filter) => {
                                if let Err(error) = handle.reload(filter) {
                                    tracing::warn!(%error, "applying new logging filter failed");
                                }
                            }
                            Err(error) => {
                                tracing::warn!(%error, filter = %new.logging.filter, "invalid logging filter")
                            }
                        }
                    }
                }
                let queue = cx.global::<AppState>().queue.clone();
                if new.transfers.max_parallel != old.transfers.max_parallel {
                    queue.set_max_parallel(new.transfers.max_parallel);
                }
                if new.transfers.bandwidth_limit != old.transfers.bandwidth_limit {
                    queue.set_bandwidth(new.transfers.bandwidth_limit);
                }
                if new.transfers != old.transfers {
                    queue.set_transfer_settings(&new.transfers);
                }
                // New default layer stack applies to jobs prepared from now
                // on and to panes opened from now on.
                if new.layers != old.layers {
                    queue.set_default_layers(new.layers.clone());
                }
                let state = cx.global_mut::<AppState>();
                state.config = new;
                state.config_epoch += 1;
                cx.refresh_windows();
            });
            if applied.is_err() {
                break;
            }
        }
    })
    .detach();
}

#[cfg(test)]
mod linked_browsing_tests {
    use super::linked_directory;

    #[test]
    fn follows_relative_paths_in_both_directions() {
        assert_eq!(
            linked_directory(
                "local/project/",
                "remote/www/",
                "local/project/assets/icons/"
            ),
            Some("remote/www/assets/icons/".into())
        );
        assert_eq!(
            linked_directory("remote/www/", "local/project/", "remote/www/assets/"),
            Some("local/project/assets/".into())
        );
        assert_eq!(
            linked_directory("local/project/", "remote/www/", "local/project/"),
            Some("remote/www/".into())
        );
        assert_eq!(
            linked_directory("", "backup/", "photos/"),
            Some("backup/photos/".into())
        );
    }

    #[test]
    fn stops_at_the_paired_root_boundary() {
        assert_eq!(linked_directory("project/", "www/", ""), None);
        assert_eq!(linked_directory("project/", "www/", "project-other/"), None);
        assert_eq!(
            linked_directory("project/", "www/", "project/../private/"),
            None
        );
    }
}
