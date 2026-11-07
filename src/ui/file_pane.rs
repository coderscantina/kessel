//! One browser pane: toolbar (up / path / new-folder / refresh) over a
//! custom virtualized file list.
//!
//! The list owns a Finder-grade selection model — click, cmd-click toggle,
//! shift-click range, rubber-band over empty space (cmd/shift keeps the
//! existing selection), and keyboard navigation (arrows, shift-arrows,
//! cmd-a, enter, backspace) — plus drag & drop: rows are drag sources
//! (dragging a selected row carries the whole selection), directory rows
//! and the pane body are drop targets, and files dragged in from the OS
//! (Finder/Explorer) enqueue as local-site transfers.
//!
//! Paths follow OpenDAL's model: relative to the operator root, `""` is the
//! root, directories end with `/`. The path bar displays them with a leading
//! slash for familiarity.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify::Watcher as _;

use chrono::{DateTime, Utc};
use gpui::prelude::*;
use gpui::{
    actions, canvas, div, px, uniform_list, App, Bounds, ClickEvent, Context, Entity,
    EventEmitter, ExternalPaths, FocusHandle, Focusable, KeyDownEvent, MouseButton,
    Modifiers, MouseDownEvent, MouseMoveEvent, MouseUpEvent, NavigationDirection,
    PathPromptOptions, Pixels, Point, ScrollStrategy, SharedString, UniformListScrollHandle,
    Window,
};
use gpui_component::button::{Button, ButtonVariant, ButtonVariants as _};
use gpui_component::dialog::DialogButtonProps;
use gpui_component::notification::Notification;
use gpui_component::spinner::Spinner;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::menu::{ContextMenuExt as _, PopupMenu, PopupMenuItem};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Disableable as _, Icon, IconName, Selectable as _,
    Sizable as _, WindowExt as _,
};

use crate::app::AppState;
use crate::errors::{humanize, Humanized};
use crate::i18n::{t, tf};
use crate::provider::{Provider, SiteProfile};
use crate::queue::{ConflictPolicy, NewJob};
use crate::ui::file_icons::{self, FileIcons};
use crate::ui::{human_size, human_time};

actions!(
    file_pane,
    [
        SelectNext,
        SelectPrev,
        ExtendNext,
        ExtendPrev,
        SelectFirst,
        SelectLast,
        SelectAll,
        ClearSelection,
        Activate,
        GoUp,
        GoBack,
        GoForward,
        ToggleHidden,
        Refresh,
        NewFolder,
        RenameSelected,
        DeleteSelected,
        QuickLook,
        Search,
        ToggleTree,
        EditPath,
        CopyToOtherPane,
        MoveToOtherPane
    ]
);

const KEY_CONTEXT: &str = "FilePane";
/// Fixed row height; the rubber band's hit math depends on it.
const ROW_HEIGHT: f32 = 26.0;

#[derive(Clone)]
pub struct FsEntry {
    pub name: SharedString,
    /// Full OpenDAL path (dirs end with `/`).
    pub path: String,
    pub is_dir: bool,
    /// Symlink (resolved: `is_dir` reflects the target).
    pub is_symlink: bool,
    /// Bucket row in an account-root S3 listing.
    pub is_bucket: bool,
    /// Synthetic `..` row.
    pub is_parent: bool,
    /// Dotfile — only present when the pane shows hidden files; rendered
    /// dimmed.
    pub is_hidden: bool,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
    /// Unix mode bits (local fs inline; sftp via lazy detail fetch).
    pub permissions: Option<u32>,
    /// S3 storage class (lazy detail fetch).
    pub storage_class: Option<String>,
    /// Bucket region on account-root S3 listings (lazy detail fetch).
    pub region: Option<String>,
}

/// Optional file-list columns (Name is always shown). Availability depends
/// on the backend scheme; *defaults* come from `ui.columns` per scheme, and
/// the header's context menu toggles them per pane. Header clicks sort, the
/// header dividers resize.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Column {
    Size,
    Modified,
    /// Unix mode, `rwxr-xr-x` style (local fs and sftp).
    Permissions,
    /// S3 storage class.
    StorageClass,
    /// Bucket region on account-root S3 listings.
    Region,
}

impl Column {
    const ALL: [Column; 5] = [
        Column::Size,
        Column::Modified,
        Column::Permissions,
        Column::StorageClass,
        Column::Region,
    ];

    fn parse(key: &str) -> Option<Self> {
        match key {
            "size" => Some(Column::Size),
            "modified" => Some(Column::Modified),
            "permissions" => Some(Column::Permissions),
            "storage_class" => Some(Column::StorageClass),
            "region" => Some(Column::Region),
            _ => None,
        }
    }

    /// Which columns a backend can actually fill.
    fn available_for(self, scheme: &str) -> bool {
        match self {
            Column::Size | Column::Modified => true,
            Column::Permissions => supports_permissions(scheme),
            Column::StorageClass | Column::Region => scheme == "s3",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Column::Size => "Size",
            Column::Modified => "Modified",
            Column::Permissions => "Permissions",
            Column::StorageClass => "Class",
            Column::Region => "Region",
        }
    }

    fn default_width(self) -> f32 {
        match self {
            Column::Size => 90.,
            Column::Modified => 150.,
            Column::Permissions => 100.,
            Column::StorageClass => 110.,
            Column::Region => 100.,
        }
    }

    fn right_aligned(self) -> bool {
        matches!(self, Column::Size)
    }

    fn text_for(self, entry: &FsEntry) -> String {
        match self {
            Column::Size => {
                if entry.is_dir && entry.size == 0 {
                    // Folder sizes show up once calculated (context menu).
                    "—".to_string()
                } else {
                    human_size(entry.size)
                }
            }
            Column::Modified => human_time(entry.modified),
            Column::Permissions => entry
                .permissions
                .map(|mode| mode_string(mode, entry.is_dir))
                .unwrap_or_default(),
            Column::StorageClass => entry.storage_class.clone().unwrap_or_default(),
            Column::Region => entry.region.clone().unwrap_or_default(),
        }
    }
}

/// `0o755` → `rwxr-xr-x` (with a leading `d` for directories; setuid/setgid/
/// sticky fold into the execute slots the way `ls -l` shows them).
fn mode_string(mode: u32, is_dir: bool) -> String {
    let mut out = String::with_capacity(10);
    out.push(if is_dir { 'd' } else { '-' });
    for (shift, special, special_char) in [
        (6, 0o4000, 's'),
        (3, 0o2000, 's'),
        (0, 0o1000, 't'),
    ] {
        let bits = (mode >> shift) & 0o7;
        out.push(if bits & 0o4 != 0 { 'r' } else { '-' });
        out.push(if bits & 0o2 != 0 { 'w' } else { '-' });
        out.push(match (bits & 0o1 != 0, mode & special != 0) {
            (true, true) => special_char,
            (true, false) => 'x',
            (false, true) => special_char.to_ascii_uppercase(),
            (false, false) => '-',
        });
    }
    out
}

/// Current sort: `column: None` sorts by name. Direction applies to the
/// sort key; directories always group first.
#[derive(Clone, Copy, PartialEq)]
struct Sort {
    column: Option<Column>,
    ascending: bool,
}

impl Default for Sort {
    fn default() -> Self {
        Sort {
            column: None,
            ascending: true,
        }
    }
}

impl Sort {
    fn compare(&self, a: &FsEntry, b: &FsEntry) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let group = b
            .is_parent
            .cmp(&a.is_parent)
            .then(b.is_dir.cmp(&a.is_dir));
        if group != Ordering::Equal {
            return group;
        }
        let keyed = match self.column {
            None => Ordering::Equal,
            Some(Column::Size) => a.size.cmp(&b.size),
            Some(Column::Modified) => a.modified.cmp(&b.modified),
            Some(Column::Permissions) => a.permissions.cmp(&b.permissions),
            Some(Column::StorageClass) => a.storage_class.cmp(&b.storage_class),
            Some(Column::Region) => a.region.cmp(&b.region),
        };
        let name = a.name.to_lowercase().cmp(&b.name.to_lowercase());
        let keyed = keyed.then(name);
        if self.ascending {
            keyed
        } else {
            keyed.reverse()
        }
    }
}

/// Events the pane raises for its parent (the tabbed workspace) to act on.
#[derive(Clone)]
pub enum PaneEvent {
    /// "Copy to other pane" was chosen for these entries.
    CopyRequested(Vec<FsEntry>),
    /// "Move to other pane": transfer, then delete the sources.
    MoveRequested(Vec<FsEntry>),
    /// "Open in new tab" on a folder: open `path` on this pane's site.
    OpenInNewTab {
        profile: crate::provider::SiteProfile,
        path: String,
    },
    /// The pane's site or directory changed — refresh titles, persist tabs.
    Navigated,
    DirectoryLoaded { linked: bool },
    LinkedNavigationFailed,
    /// The user interacted with this pane; it becomes the tab's active pane.
    Focused,
    /// "Edit site" on a failed connection: open this site's settings.
    EditSite(i64),
}

impl EventEmitter<PaneEvent> for FilePane {}

/// Drag payload for rows: the dragged entries plus the site they came from,
/// so any pane (or dir row) can accept the drop and enqueue transfers.
/// Dropping onto the directory an entry already lives in is filtered out by
/// path at the drop site, so same-pane drops are naturally no-ops.
#[derive(Clone)]
pub struct DraggedEntries {
    pub site_id: i64,
    pub entries: Vec<FsEntry>,
}

/// The row drag currently in flight, if any. Set when a row drag starts and
/// cleared on mouse-up; the workspace watches the pointer and promotes the
/// internal drag to a native OS drag session (Finder drop target) the
/// moment it leaves the window. See [`crate::dragout`].
#[derive(Default)]
pub struct PendingDragOut(pub Option<DraggedEntries>);

impl gpui::Global for PendingDragOut {}

/// The little chip that follows the pointer during a drag. Kept to a small
/// solid pill — a large translucent box under the pointer reads as a
/// misplaced shadow.
///
/// gpui paints the drag view at `mouse - grab_offset` (the grab point within
/// the dragged element), so a row grabbed far from its left edge would leave
/// the pill way off to the side. The stored offset pads the pill back under
/// the pointer, just below-right so the cursor doesn't cover it.
struct DragGhost {
    /// What is dragged ("report.pdf", "3 items"); the operation is prefixed
    /// live from the held modifiers.
    what: SharedString,
    grab_offset: Point<Pixels>,
}

impl Render for DragGhost {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let key = if is_move_drop(&window.modifiers()) {
            "file_pane.drag.move"
        } else {
            "file_pane.drag.copy"
        };
        let label = tf(key, &[("what", &self.what)]);
        div()
            .pl(self.grab_offset.x + px(12.))
            .pt(self.grab_offset.y + px(12.))
            .child(
                div()
                    .px_2()
                    .py_0p5()
                    .rounded_full()
                    .bg(cx.theme().primary)
                    .text_color(cx.theme().primary_foreground)
                    .text_xs()
                    .whitespace_nowrap()
                    .child(label),
            )
    }
}

/// Holding Shift (or Cmd on macOS, as in Finder) turns a drop into a move.
fn is_move_drop(modifiers: &Modifiers) -> bool {
    modifiers.shift || (cfg!(target_os = "macos") && modifiers.platform)
}

/// Where the pane's backend connection stands. Connecting runs on the app
/// runtime because it reads the keychain and may call AWS STS; a failure
/// keeps the requested site so the pane can retry or open its settings.
enum Connection {
    Connecting,
    Failed(Humanized),
    Ready(Provider),
}

/// An in-progress rubber-band selection, in content coordinates (y = 0 is
/// the top of row 0), so it stays anchored while the list scrolls.
struct RubberBand {
    origin: Point<Pixels>,
    current: Point<Pixels>,
    /// Selection at drag start; kept when cmd/shift was held.
    base: BTreeSet<usize>,
}

pub struct FilePane {
    label: SharedString,
    connection: Connection,
    /// Profile the provider was built from — cloned into "open in new tab"
    /// requests and used for the URL-style path display.
    profile: SiteProfile,
    /// Current directory, OpenDAL-style: `""` or `some/dir/`.
    path: String,
    /// Per-pane navigation history (paths within the current site).
    back_stack: Vec<String>,
    forward_stack: Vec<String>,
    path_input: Entity<InputState>,
    /// True while the path bar shows the editable input instead of the
    /// clickable breadcrumb (toggled by `EditPath` / cmd-l).
    editing_path: bool,
    /// Why the last path-bar entry was rejected (a URL for another site).
    path_error: Option<SharedString>,
    /// Rendered listing — `all_entries` narrowed by the type-to-search
    /// filter (or a plain copy when no search is active). All row/selection
    /// indices refer to this view.
    entries: Vec<FsEntry>,
    /// Full listing for the current directory, in sort order.
    all_entries: Vec<FsEntry>,
    /// Type-to-search: the filter bar input, shown while `search_active`.
    search_input: Entity<InputState>,
    search_active: bool,
    /// Mirrors `search_input`'s value so filtering doesn't need the input
    /// entity (updated from its change events).
    search_query: String,
    loading: bool,
    /// A listing has been in flight for longer than a blink: shows the
    /// path-bar spinner.
    slow_loading: bool,
    /// Directory the rows in `all_entries` belong to. While a listing for
    /// another path loads, the old rows stay visible but dimmed and inert.
    loaded_path: Option<String>,
    /// Failed listing of the current directory.
    error: Option<Humanized>,
    /// The error state's "Details" disclosure is open.
    details_open: bool,
    selection: BTreeSet<usize>,
    /// Keyboard focus row.
    cursor: Option<usize>,
    /// Fixed end of shift-ranges.
    anchor: Option<usize>,
    rubber: Option<RubberBand>,
    /// List viewport in window coordinates, captured during paint.
    list_bounds: Option<Bounds<Pixels>>,
    focus_handle: FocusHandle,
    scroll_handle: UniformListScrollHandle,
    /// Bumped per reload so stale results are dropped.
    load_seq: usize,
    linked_seq: usize,
    linked_navigation: bool,
    /// Visible optional columns, seeded from `ui.columns` by scheme.
    columns: Vec<Column>,
    /// Current sort; entries are kept in this order.
    sort: Sort,
    /// Per-pane column width overrides (header dividers drag-resize).
    col_widths: std::collections::HashMap<Column, f32>,
    /// An in-progress header resize: (column, pointer start x, start width).
    resizing: Option<(Column, f32, f32)>,
    /// Show dotfiles (dimmed); seeded from `ui.show_hidden`.
    show_hidden: bool,
    /// Entry names to re-select once the next listing arrives (set by
    /// selection-preserving refreshes: watcher events, the refresh button).
    pending_selection: Option<Vec<SharedString>>,
    /// Filesystem watcher for local panes plus the directory it currently
    /// watches; retargeted on every navigation.
    watcher: Option<(notify::RecommendedWatcher, PathBuf)>,
    /// Optional folder tree beside the file list.
    tree_visible: bool,
    /// Lazily loaded tree children: dir path → its subdirectories.
    tree_children: HashMap<String, Vec<(SharedString, String)>>,
    tree_expanded: HashSet<String>,
    tree_loading: HashSet<String>,
    /// An in-progress tree divider drag: (pointer start coordinate, start
    /// size). The coordinate is x/width when the tree sits on the left,
    /// y/height when it sits on top (`tree_on_top`).
    tree_resize: Option<(f32, f32)>,
    /// Place the folder tree above the file list (true) instead of to its
    /// left (false). Set by the workspace from its pane layout: side-by-side
    /// (narrow) panes stack tree-over-list; stacked / single panes keep it
    /// on the left.
    tree_on_top: bool,
    /// Last `AppState::config_epoch` applied (config hot reload re-seeds
    /// show_hidden and columns).
    config_epoch_seen: u64,
}

impl Focusable for FilePane {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl FilePane {
    /// A pane browsing any site profile (local is just the reserved fs
    /// profile). Remote sites connect in the background; a failed connect
    /// stays on the site and shows the error with Retry and Edit site.
    pub fn for_profile(
        profile: SiteProfile,
        start_path: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        // A plain local site builds without any I/O, so it connects inline
        // and never flashes a connecting state.
        let connection = if profile.scheme == "fs" && profile.secret_handle.is_none() {
            let defaults = &cx.global::<AppState>().config.layers;
            match Provider::connect(&profile, defaults) {
                Ok(provider) => Connection::Ready(provider),
                Err(error) => Connection::Failed(humanize(&error)),
            }
        } else {
            Connection::Connecting
        };

        let path_input = cx.new(|cx| InputState::new(window, cx).placeholder("/path/to/dir"));

        cx.subscribe_in(
            &path_input,
            window,
            |this, input, event: &InputEvent, window, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    let raw = input.read(cx).value();
                    match parse_path_input(&raw, &this.profile) {
                        PathInput::Here(path) => {
                            this.editing_path = false;
                            this.navigate_to(path, window, cx);
                        }
                        PathInput::Elsewhere(url) => {
                            this.path_error =
                                Some(tf("file_pane.path_elsewhere", &[("url", &url)]));
                            cx.notify();
                        }
                    }
                }
            },
        )
        .detach();

        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Search…"));

        cx.subscribe_in(
            &search_input,
            window,
            |this, input, event: &InputEvent, window, cx| match event {
                InputEvent::Change { .. } => {
                    this.search_query = input.read(cx).value().to_string();
                    this.rebuild_view();
                    cx.notify();
                }
                InputEvent::PressEnter { .. } => {
                    this.activate(&Activate, window, cx);
                }
                _ => {}
            },
        )
        .detach();

        let config = &cx.global::<AppState>().config.ui;
        let show_hidden = config.show_hidden;
        let columns: Vec<Column> = config
            .columns
            .get(&profile.scheme)
            .or_else(|| config.columns.get("default"))
            .map(|keys| keys.iter().filter_map(|key| Column::parse(key)).collect())
            .unwrap_or_else(|| vec![Column::Size, Column::Modified])
            .into_iter()
            .filter(|column| column.available_for(&profile.scheme))
            .collect();
        let tree_visible = config.tree_visible;
        let config_epoch = cx.global::<AppState>().config_epoch;

        let mut this = Self {
            label: profile.name.clone().into(),
            connection,
            profile,
            path: start_path,
            back_stack: Vec::new(),
            forward_stack: Vec::new(),
            path_input,
            editing_path: false,
            path_error: None,
            entries: Vec::new(),
            all_entries: Vec::new(),
            search_input,
            search_active: false,
            search_query: String::new(),
            loading: false,
            slow_loading: false,
            loaded_path: None,
            error: None,
            details_open: false,
            selection: BTreeSet::new(),
            cursor: None,
            anchor: None,
            rubber: None,
            list_bounds: None,
            focus_handle: cx.focus_handle(),
            scroll_handle: UniformListScrollHandle::new(),
            load_seq: 0,
            linked_seq: 0,
            linked_navigation: false,
            columns,
            sort: Sort::default(),
            col_widths: std::collections::HashMap::new(),
            resizing: None,
            show_hidden,
            pending_selection: None,
            watcher: None,
            tree_visible,
            tree_children: HashMap::new(),
            tree_expanded: HashSet::new(),
            tree_loading: HashSet::new(),
            tree_resize: None,
            tree_on_top: false,
            config_epoch_seen: config_epoch,
        };
        this.init_watcher(window, cx);
        match this.connection {
            Connection::Ready(_) => this.on_connected(window, cx),
            Connection::Connecting => this.connect(window, cx),
            Connection::Failed(_) => {}
        }
        this
    }

    fn provider(&self) -> Option<&Provider> {
        match &self.connection {
            Connection::Ready(provider) => Some(provider),
            Connection::Connecting | Connection::Failed(_) => None,
        }
    }

    /// Build the site's provider on the app runtime (keychain reads and AWS
    /// STS block), then load the listing.
    fn connect(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.connection = Connection::Connecting;
        self.details_open = false;
        let profile = self.profile.clone();
        let state = cx.global::<AppState>();
        let defaults = state.config.layers.clone();
        let task = state
            .runtime
            .spawn_blocking(move || Provider::connect(&profile, &defaults));
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("connect task failed: {join_error}")),
            };
            this.update_in(cx, |this, window, cx| {
                match result {
                    Ok(provider) => {
                        this.connection = Connection::Ready(provider);
                        this.on_connected(window, cx);
                    }
                    Err(error) => {
                        tracing::error!(
                            site = %this.profile.name,
                            error = %format!("{error:#}"),
                            "connecting site failed"
                        );
                        this.connection = Connection::Failed(humanize(&error));
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn on_connected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.reload(window, cx);
        if self.tree_visible {
            self.tree_load(String::new(), cx);
        }
    }

    /// Config hot reload: re-seed the per-pane values that come from
    /// `ui.*` (show_hidden, columns) and reload so they take effect.
    fn apply_config(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let config = &cx.global::<AppState>().config.ui;
        let show_hidden = config.show_hidden;
        let columns: Vec<Column> = config
            .columns
            .get(&self.profile.scheme)
            .or_else(|| config.columns.get("default"))
            .map(|keys| keys.iter().filter_map(|key| Column::parse(key)).collect())
            .unwrap_or_else(|| vec![Column::Size, Column::Modified])
            .into_iter()
            .filter(|column| column.available_for(&self.profile.scheme))
            .collect();
        let mut changed = false;
        if show_hidden != self.show_hidden {
            self.show_hidden = show_hidden;
            changed = true;
        }
        if columns != self.columns {
            self.columns = columns;
            self.fetch_details(cx);
            changed = true;
        }
        if changed {
            self.refresh(window, cx);
        }
    }

    fn toggle_column(&mut self, column: Column, cx: &mut Context<Self>) {
        if self.columns.contains(&column) {
            self.columns.retain(|c| *c != column);
        } else {
            // Keep the canonical order regardless of toggle order.
            self.columns = Column::ALL
                .into_iter()
                .filter(|c| *c == column || self.columns.contains(c))
                .collect();
            // Lazily-filled columns get their data on first show.
            self.fetch_details(cx);
        }
        cx.notify();
    }

    fn column_width(&self, column: Column) -> Pixels {
        px(self
            .col_widths
            .get(&column)
            .copied()
            .unwrap_or_else(|| column.default_width()))
    }

    /// Re-sort in place; the selection follows its entries.
    fn set_sort(&mut self, column: Option<Column>, cx: &mut Context<Self>) {
        if self.sort.column == column {
            self.sort.ascending = !self.sort.ascending;
        } else {
            self.sort = Sort {
                column,
                ascending: true,
            };
        }
        self.apply_sort();
        cx.notify();
    }

    fn apply_sort(&mut self) {
        let sort = self.sort;
        self.all_entries.sort_by(|a, b| sort.compare(a, b));
        self.rebuild_view();
    }

    // --- type-to-search ---

    /// Rebuild the rendered `entries` view from `all_entries` and the fuzzy
    /// query. Row indices change, so selection and cursor are carried over
    /// by path; entries the filter hides drop out of the selection.
    fn rebuild_view(&mut self) {
        let selected: HashSet<String> = self
            .selected_entries()
            .into_iter()
            .map(|entry| entry.path)
            .collect();
        let cursor_path = self
            .cursor
            .and_then(|ix| self.entries.get(ix))
            .map(|entry| entry.path.clone());
        self.rubber = None;
        let query = self.search_query.trim();
        self.entries = if !self.search_active || query.is_empty() {
            self.all_entries.clone()
        } else {
            self.all_entries
                .iter()
                .filter(|entry| entry.is_parent || fuzzy_match(&entry.name, query))
                .cloned()
                .collect()
        };
        self.selection = select_paths(&self.entries, &selected);
        self.cursor = cursor_path
            .and_then(|path| self.entries.iter().position(|entry| entry.path == path));
        self.anchor = self.cursor;
        // Preselect the first match so Enter opens it right away.
        if self.search_active && !query.is_empty() && self.selection.is_empty() {
            if let Some(first) = self.entries.iter().position(|entry| !entry.is_parent) {
                self.selection.insert(first);
                self.cursor = Some(first);
                self.anchor = Some(first);
            }
        }
    }

    fn start_search(&mut self, seed: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.search_active = true;
        self.search_query = seed.to_string();
        let focus = self.search_input.focus_handle(cx);
        let seed = self.search_query.clone();
        self.search_input.update(cx, |input, cx| {
            input.set_value(seed, window, cx);
        });
        window.focus(&focus, cx);
        self.rebuild_view();
        cx.notify();
    }

    fn close_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.search_active {
            return;
        }
        self.search_active = false;
        self.search_query.clear();
        self.search_input
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.rebuild_view();
        window.focus(&self.focus_handle, cx);
        cx.notify();
    }

    fn search_action(&mut self, _: &Search, window: &mut Window, cx: &mut Context<Self>) {
        if self.search_active {
            // Already open: just refocus the input.
            let focus = self.search_input.focus_handle(cx);
            window.focus(&focus, cx);
        } else {
            self.start_search("", window, cx);
        }
    }

    /// Auto-start search when a printable key is typed into the focused
    /// list (Finder-style type-to-search).
    fn on_pane_key_down(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.search_active {
            return;
        }
        // Only when the list itself is focused — not the path bar or any
        // dialog input that happens to live inside the pane.
        if !self.focus_handle.is_focused(window) {
            return;
        }
        let keystroke = &event.keystroke;
        if keystroke.modifiers.control
            || keystroke.modifiers.alt
            || keystroke.modifiers.platform
            || keystroke.modifiers.function
        {
            return;
        }
        let Some(typed) = keystroke.key_char.as_deref() else {
            return;
        };
        if typed.chars().count() != 1
            || typed.chars().all(char::is_whitespace)
            || typed.chars().any(char::is_control)
        {
            return;
        }
        self.start_search(typed, window, cx);
        cx.stop_propagation();
    }

    fn sort_marker(&self, column: Option<Column>) -> &'static str {
        if self.sort.column != column {
            ""
        } else if self.sort.ascending {
            " ↑"
        } else {
            " ↓"
        }
    }

    fn toggle_hidden(&mut self, _: &ToggleHidden, window: &mut Window, cx: &mut Context<Self>) {
        self.show_hidden = !self.show_hidden;
        self.refresh(window, cx);
    }

    // --- folder tree ---

    fn toggle_tree(&mut self, _: &ToggleTree, _: &mut Window, cx: &mut Context<Self>) {
        self.tree_visible = !self.tree_visible;
        if self.tree_visible && !self.tree_children.contains_key("") {
            self.tree_load(String::new(), cx);
        }
        cx.notify();
    }

    /// Orient the folder tree: above the file list (`on_top`) for narrow,
    /// side-by-side panes, or to its left otherwise. Driven by the workspace
    /// whenever its pane layout changes.
    pub fn set_tree_on_top(&mut self, on_top: bool, cx: &mut Context<Self>) {
        if self.tree_on_top != on_top {
            self.tree_on_top = on_top;
            self.tree_resize = None;
            cx.notify();
        }
    }

    /// Load a directory's subdirectories into the tree cache.
    fn tree_load(&mut self, dir: String, cx: &mut Context<Self>) {
        let Some(provider) = self.provider().cloned() else { return };
        if !self.tree_loading.insert(dir.clone()) {
            return;
        }
        let show_hidden = self.show_hidden;
        let list_dir = dir.clone();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { provider.list(&list_dir).await });
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("tree listing failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                this.tree_loading.remove(&dir);
                match result {
                    Ok(entries) => {
                        let mut dirs: Vec<(SharedString, String)> = entries
                            .into_iter()
                            .filter(|entry| {
                                entry.is_dir
                                    && !entry.name.is_empty()
                                    && (show_hidden || !entry.name.starts_with('.'))
                            })
                            .map(|entry| (SharedString::from(entry.name), entry.path))
                            .collect();
                        dirs.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));
                        this.tree_children.insert(dir.clone(), dirs);
                    }
                    Err(error) => {
                        tracing::debug!(%error, dir = %dir, "tree listing failed");
                        this.tree_children.entry(dir.clone()).or_default();
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn tree_toggle_expand(&mut self, dir: String, cx: &mut Context<Self>) {
        if !self.tree_expanded.remove(&dir) {
            self.tree_expanded.insert(dir.clone());
            if !self.tree_children.contains_key(&dir) {
                self.tree_load(dir, cx);
            }
        }
        cx.notify();
    }

    /// Rows of the folder tree under `dir`, depth-first over expanded dirs.
    fn render_tree_rows(
        &self,
        dir: &str,
        depth: usize,
        rows: &mut Vec<gpui::AnyElement>,
        cx: &mut Context<Self>,
    ) {
        let Some(children) = self.tree_children.get(dir) else {
            return;
        };
        for (name, path) in children.clone() {
            let expanded = self.tree_expanded.contains(&path);
            let is_current = self.path == path;
            let icons = FileIcons::get(cx);
            let toggle_path = path.clone();
            let nav_path = path.clone();
            let has_loaded_empty = self
                .tree_children
                .get(&path)
                .is_some_and(|children| children.is_empty());
            rows.push(
                h_flex()
                    .id(SharedString::from(format!("tree-{path}")))
                    .w_full()
                    .h(px(24.))
                    // Fixed row height: never let a short tree panel compress
                    // rows via flex shrink — the panel scrolls instead.
                    .flex_shrink_0()
                    .pl(px(6. + depth as f32 * 14.))
                    .pr_1()
                    .gap_1()
                    .items_center()
                    .text_sm()
                    .cursor_pointer()
                    .overflow_hidden()
                    .when(is_current, |el| el.bg(cx.theme().list_active))
                    .when(!is_current, |el| {
                        el.hover(|style| style.bg(cx.theme().list_hover))
                    })
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.navigate_to(nav_path.clone(), window, cx);
                    }))
                    .child(
                        div()
                            .id(SharedString::from(format!("tree-chevron-{path}")))
                            .flex_shrink_0()
                            .w(px(16.))
                            .cursor_pointer()
                            .when(!has_loaded_empty, |el| {
                                el.child(
                                    Icon::new(if expanded {
                                        IconName::ChevronDown
                                    } else {
                                        IconName::ChevronRight
                                    })
                                    .small()
                                    .text_color(cx.theme().muted_foreground),
                                )
                            })
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _: &MouseDownEvent, _, cx| {
                                    cx.stop_propagation();
                                    this.tree_toggle_expand(toggle_path.clone(), cx);
                                }),
                            ),
                    )
                    .child(file_icons::icon_element(
                        icons.folder_named(&name, expanded),
                        px(14.),
                        cx.theme().accent_foreground,
                    ))
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .child(name.clone()),
                    )
                    .into_any_element(),
            );
            if expanded {
                self.render_tree_rows(&path, depth + 1, rows, cx);
            }
        }
    }

    /// The tree panel plus its drag divider (empty when hidden). The tree
    /// sits above the list (`tree_on_top`, height-sized, row-resize divider)
    /// or to its left (width-sized, column-resize divider).
    fn render_tree_panel(&mut self, cx: &mut Context<Self>) -> Vec<gpui::AnyElement> {
        if !self.tree_visible {
            return Vec::new();
        }
        let on_top = self.tree_on_top;
        let mut rows: Vec<gpui::AnyElement> = Vec::new();
        // Root row navigates back to the site root.
        let is_root = self.path.is_empty();
        rows.push(
            h_flex()
                .id("tree-root")
                .w_full()
                .h(px(24.))
                .flex_shrink_0()
                .pl(px(6.))
                .gap_1()
                .items_center()
                .text_sm()
                .cursor_pointer()
                .when(is_root, |el| el.bg(cx.theme().list_active))
                .when(!is_root, |el| el.hover(|style| style.bg(cx.theme().list_hover)))
                .on_click(cx.listener(|this, _, window, cx| {
                    this.navigate_to(String::new(), window, cx);
                }))
                .child(div().w(px(16.)).flex_shrink_0())
                .child(
                    Icon::new(IconName::Folder)
                        .small()
                        .text_color(cx.theme().accent_foreground),
                )
                .child("/")
                .into_any_element(),
        );
        self.render_tree_rows("", 1, &mut rows, cx);
        if self.tree_loading.contains("") {
            rows.push(
                div()
                    .px_2()
                    .py_1()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child("Loading…")
                    .into_any_element(),
            );
        }

        let mut panel = v_flex()
            .id("folder-tree")
            .flex_shrink_0()
            .overflow_hidden()
            .overflow_y_scroll()
            .py_1()
            .children(rows);
        let mut divider = div()
            .id("tree-divider")
            .flex_shrink_0()
            .bg(cx.theme().border)
            .hover(|style| style.bg(cx.theme().primary.opacity(0.6)));
        if on_top {
            let height = cx
                .global::<AppState>()
                .config
                .ui
                .tree_height
                .clamp(100., 400.);
            panel = panel.w_full().h(px(height));
            divider = divider
                .h(px(3.))
                .w_full()
                .cursor_row_resize()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, event: &MouseDownEvent, _, cx| {
                        cx.stop_propagation();
                        let height = cx.global::<AppState>().config.ui.tree_height;
                        this.tree_resize = Some((f32::from(event.position.y), height));
                    }),
                );
        } else {
            let width = cx
                .global::<AppState>()
                .config
                .ui
                .tree_width
                .clamp(120., 400.);
            panel = panel.w(px(width)).h_full();
            divider = divider
                .w(px(3.))
                .h_full()
                .cursor_col_resize()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, event: &MouseDownEvent, _, cx| {
                        cx.stop_propagation();
                        let width = cx.global::<AppState>().config.ui.tree_width;
                        this.tree_resize = Some((f32::from(event.position.x), width));
                    }),
                );
        }
        vec![panel.into_any_element(), divider.into_any_element()]
    }

    /// Fill lazily-fetched per-entry details (sftp permissions, S3 storage
    /// classes, bucket regions) for the current listing, if the matching
    /// column is visible. Merges by entry name; stale results are dropped
    /// via `load_seq`.
    fn fetch_details(&mut self, cx: &mut Context<Self>) {
        let seq = self.load_seq;
        let Some(provider) = self.provider().cloned() else { return };
        let path = self.path.clone();
        let scheme = self.profile.scheme.clone();
        let runtime = cx.global::<AppState>().runtime.handle().clone();

        enum Detail {
            Permissions(std::collections::HashMap<String, u32>),
            StorageClasses(std::collections::HashMap<String, String>),
            Regions(std::collections::HashMap<String, String>),
        }

        let mut fetches = Vec::new();
        if scheme == "sftp" && self.columns.contains(&Column::Permissions) {
            let provider = provider.clone();
            let dir = path.clone();
            fetches.push(runtime.spawn(async move {
                provider.sftp_permissions(&dir).await.map(Detail::Permissions)
            }));
        }
        if scheme == "s3" && self.columns.contains(&Column::StorageClass) {
            let provider = provider.clone();
            let dir = path.clone();
            fetches.push(runtime.spawn(async move {
                provider.storage_classes(&dir).await.map(Detail::StorageClasses)
            }));
        }
        if scheme == "s3" && path.is_empty() && self.columns.contains(&Column::Region) {
            let buckets: Vec<String> = self
                .entries
                .iter()
                .filter(|entry| entry.is_bucket)
                .map(|entry| entry.name.to_string())
                .collect();
            if !buckets.is_empty() {
                let provider = provider.clone();
                fetches.push(runtime.spawn(async move {
                    Ok(Detail::Regions(provider.bucket_regions(buckets).await))
                }));
            }
        }

        for task in fetches {
            cx.spawn(async move |this, cx| {
                let Ok(result) = task.await else { return };
                let detail = match result {
                    Ok(detail) => detail,
                    Err(error) => {
                        tracing::debug!(%error, "detail fetch failed");
                        return;
                    }
                };
                this.update(cx, |this, cx| {
                    if this.load_seq != seq {
                        return;
                    }
                    let sorted_by = match &detail {
                        Detail::Permissions(_) => Column::Permissions,
                        Detail::StorageClasses(_) => Column::StorageClass,
                        Detail::Regions(_) => Column::Region,
                    };
                    // `entries` holds clones of `all_entries`, so merge into
                    // both the source listing and the rendered view.
                    for entry in this.all_entries.iter_mut().chain(this.entries.iter_mut()) {
                        let name = entry.name.as_ref();
                        match &detail {
                            Detail::Permissions(modes) => {
                                if let Some(mode) = modes.get(name) {
                                    entry.permissions = Some(*mode);
                                }
                            }
                            Detail::StorageClasses(classes) => {
                                if let Some(class) = classes.get(name) {
                                    entry.storage_class = Some(class.clone());
                                }
                            }
                            Detail::Regions(regions) => {
                                if let Some(region) = regions.get(name) {
                                    entry.region = Some(region.clone());
                                }
                            }
                        }
                    }
                    if this.sort.column == Some(sorted_by) {
                        this.apply_sort();
                    }
                    cx.notify();
                })
                .ok();
            })
            .detach();
        }
    }

    pub fn profile(&self) -> &crate::provider::SiteProfile {
        &self.profile
    }

    pub fn label(&self) -> SharedString {
        self.label.clone()
    }

    /// Site backing this pane (0 = the reserved local-fs site), used to
    /// address transfer jobs.
    pub fn site_id(&self) -> i64 {
        self.profile.id
    }

    /// Current directory as a browse path (`""` = root, otherwise ends `/`).
    pub fn current_dir(&self) -> String {
        self.path.clone()
    }

    pub fn selected_entries(&self) -> Vec<FsEntry> {
        self.selection
            .iter()
            .filter_map(|&ix| self.entries.get(ix))
            .filter(|entry| !entry.is_parent)
            .cloned()
            .collect()
    }

    /// "N items", or "N items · M selected · SIZE" — surfaced both in the
    /// global status bar (active pane) and, in split view, per pane.
    pub fn item_summary(&self) -> String {
        let item_count = self
            .entries
            .iter()
            .filter(|entry| !entry.is_parent)
            .count();
        let selected = self.selected_entries();
        if selected.is_empty() {
            return format!("{item_count} items");
        }
        // Folder sizes aren't known from a listing; sum files only.
        let bytes: u64 = selected
            .iter()
            .filter(|entry| !entry.is_dir)
            .map(|entry| entry.size)
            .sum();
        let mut status = format!("{item_count} items · {} selected", selected.len());
        if bytes > 0 {
            status.push_str(&format!(" · {}", human_size(bytes)));
        }
        status
    }

    /// A listing for another directory is in flight, so the visible rows
    /// belong to the previous one and must not be acted on.
    fn is_stale(&self) -> bool {
        self.loading && self.loaded_path.as_deref() != Some(self.path.as_str())
    }

    // --- selection model ---

    fn selectable(&self, ix: usize) -> bool {
        self.entries.get(ix).is_some_and(|entry| !entry.is_parent)
    }

    fn select_single(&mut self, ix: usize) {
        self.selection = BTreeSet::from([ix]);
        self.cursor = Some(ix);
        self.anchor = Some(ix);
    }

    fn select_range(&mut self, a: usize, b: usize) {
        let (lo, hi) = (a.min(b), a.max(b));
        self.selection = (lo..=hi).filter(|&ix| self.selectable(ix)).collect();
    }

    fn on_row_click(
        &mut self,
        ix: usize,
        event: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.focus_handle, cx);
        cx.emit(PaneEvent::Focused);
        if self.is_stale() {
            return;
        }
        let Some(entry) = self.entries.get(ix).cloned() else {
            return;
        };

        if event.click_count() >= 2 {
            if entry.is_parent {
                self.go_up(window, cx);
            } else if entry.is_dir {
                self.navigate_to(entry.path, window, cx);
            } else {
                self.open_files(vec![entry], window, cx);
            }
            return;
        }
        if entry.is_parent {
            return;
        }

        let modifiers = event.modifiers();
        if modifiers.shift {
            let anchor = self.anchor.or(self.cursor).unwrap_or(ix);
            self.anchor = Some(anchor);
            self.select_range(anchor, ix);
            self.cursor = Some(ix);
        } else if modifiers.secondary() {
            if !self.selection.remove(&ix) {
                self.selection.insert(ix);
            }
            self.cursor = Some(ix);
            self.anchor = Some(ix);
        } else {
            self.select_single(ix);
        }
        cx.notify();
    }

    /// Right-click on an unselected row moves the selection there before the
    /// context menu opens (Finder behavior).
    fn on_row_right_down(&mut self, ix: usize, window: &mut Window, cx: &mut Context<Self>) {
        window.focus(&self.focus_handle, cx);
        if !self.is_stale() && self.selectable(ix) && !self.selection.contains(&ix) {
            self.select_single(ix);
            cx.notify();
        }
    }

    // --- keyboard ---

    fn step_cursor(&mut self, delta: i64, extend: bool, cx: &mut Context<Self>) {
        let len = self.entries.len();
        let Some(first) = self.entries.iter().position(|entry| !entry.is_parent) else {
            return;
        };
        let next = match self.cursor {
            Some(cursor) => {
                (cursor as i64 + delta).clamp(first as i64, len as i64 - 1) as usize
            }
            None if delta > 0 => first,
            None => len - 1,
        };
        if extend {
            let anchor = self.anchor.unwrap_or(next);
            self.anchor = Some(anchor);
            self.select_range(anchor, next);
        } else {
            self.select_single(next);
        }
        self.cursor = Some(next);
        self.scroll_handle.scroll_to_item(next, ScrollStrategy::Center);
        cx.notify();
    }

    fn select_next(&mut self, _: &SelectNext, _: &mut Window, cx: &mut Context<Self>) {
        self.step_cursor(1, false, cx);
    }
    fn select_prev(&mut self, _: &SelectPrev, _: &mut Window, cx: &mut Context<Self>) {
        self.step_cursor(-1, false, cx);
    }
    fn extend_next(&mut self, _: &ExtendNext, _: &mut Window, cx: &mut Context<Self>) {
        self.step_cursor(1, true, cx);
    }
    fn extend_prev(&mut self, _: &ExtendPrev, _: &mut Window, cx: &mut Context<Self>) {
        self.step_cursor(-1, true, cx);
    }

    fn select_all(&mut self, _: &SelectAll, _: &mut Window, cx: &mut Context<Self>) {
        self.selection = (0..self.entries.len())
            .filter(|&ix| self.selectable(ix))
            .collect();
        cx.notify();
    }

    fn clear_selection(&mut self, _: &ClearSelection, _: &mut Window, cx: &mut Context<Self>) {
        self.selection.clear();
        self.rubber = None;
        cx.notify();
    }

    /// Enter / right arrow / double-click: a single folder is entered;
    /// files open in their default app, extra folders in new tabs.
    fn activate(&mut self, _: &Activate, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_stale() {
            return;
        }
        let selected = self.selected_entries();
        if let [entry] = selected.as_slice() {
            if entry.is_dir {
                self.navigate_to(entry.path.clone(), window, cx);
                return;
            }
        }
        let (dirs, files): (Vec<FsEntry>, Vec<FsEntry>) =
            selected.into_iter().partition(|entry| entry.is_dir);
        for dir in dirs {
            cx.emit(PaneEvent::OpenInNewTab {
                profile: self.profile.clone(),
                path: dir.path,
            });
        }
        self.open_files(files, window, cx);
    }

    /// Open files in their default app. Local files open in place; remote
    /// ones go through the external-edit path, which downloads them and
    /// uploads saves back.
    fn open_files(&mut self, files: Vec<FsEntry>, window: &mut Window, cx: &mut Context<Self>) {
        if self.profile.scheme == "fs" {
            let root = self.local_root();
            for file in files {
                let full = crate::localfs::full_path(&root, &file.path);
                crate::ui::open_path_or_url(&full.to_string_lossy());
            }
        } else {
            for file in files {
                self.edit_external(file, window, cx);
            }
        }
    }

    /// Filesystem root of a local (fs) site.
    fn local_root(&self) -> String {
        self.profile
            .options
            .get("root")
            .cloned()
            .unwrap_or_else(crate::localfs::root)
    }

    fn copy_to_other_pane(&mut self, _: &CopyToOtherPane, _: &mut Window, cx: &mut Context<Self>) {
        let selected = self.selected_entries();
        if !selected.is_empty() && !self.is_stale() {
            cx.emit(PaneEvent::CopyRequested(selected));
        }
    }

    fn move_to_other_pane(&mut self, _: &MoveToOtherPane, _: &mut Window, cx: &mut Context<Self>) {
        let selected = self.selected_entries();
        if !selected.is_empty() && !self.is_stale() {
            cx.emit(PaneEvent::MoveRequested(selected));
        }
    }

    fn go_up_action(&mut self, _: &GoUp, window: &mut Window, cx: &mut Context<Self>) {
        self.go_up(window, cx);
    }

    fn go_back_action(&mut self, _: &GoBack, window: &mut Window, cx: &mut Context<Self>) {
        self.go_back(window, cx);
    }

    fn go_forward_action(&mut self, _: &GoForward, window: &mut Window, cx: &mut Context<Self>) {
        self.go_forward(window, cx);
    }

    fn jump_to(&mut self, ix: usize, cx: &mut Context<Self>) {
        self.select_single(ix);
        self.scroll_handle.scroll_to_item(ix, ScrollStrategy::Center);
        cx.notify();
    }

    fn select_first(&mut self, _: &SelectFirst, _: &mut Window, cx: &mut Context<Self>) {
        if let Some(first) = self.entries.iter().position(|entry| !entry.is_parent) {
            self.jump_to(first, cx);
        }
    }

    fn select_last(&mut self, _: &SelectLast, _: &mut Window, cx: &mut Context<Self>) {
        if !self.entries.is_empty() {
            self.jump_to(self.entries.len() - 1, cx);
        }
    }

    fn refresh_action(&mut self, _: &Refresh, window: &mut Window, cx: &mut Context<Self>) {
        self.refresh(window, cx);
    }

    fn new_folder_action(&mut self, _: &NewFolder, window: &mut Window, cx: &mut Context<Self>) {
        self.new_folder_dialog(window, cx);
    }

    fn rename_selected(&mut self, _: &RenameSelected, window: &mut Window, cx: &mut Context<Self>) {
        if let [entry] = self.selected_entries().as_slice() {
            if self.can_rename(entry) {
                self.rename_dialog(entry.clone(), window, cx);
            }
        }
    }

    fn delete_selected(&mut self, _: &DeleteSelected, window: &mut Window, cx: &mut Context<Self>) {
        self.confirm_delete(self.selected_entries(), window, cx);
    }

    /// Whether one entry can be renamed here: server-side rename, or the
    /// copy + delete fallback for file entries where only copy exists.
    fn can_rename(&self, entry: &FsEntry) -> bool {
        if entry.is_bucket {
            return false;
        }
        let Some(caps) = self.provider().map(|provider| provider.caps) else {
            return false;
        };
        if entry.is_dir {
            // On virtual-dir backends a directory is only a prefix, so
            // renaming it isn't a single server-side op.
            caps.rename && !caps.virtual_dirs
        } else {
            caps.rename || caps.copy
        }
    }

    // --- Quick Look ---

    /// Space: preview the selection. Local entries open in Quick Look
    /// directly; remote files download to a temp folder first.
    fn quick_look_action(&mut self, _: &QuickLook, window: &mut Window, cx: &mut Context<Self>) {
        let entries = self.selected_entries();
        if entries.is_empty() {
            return;
        }
        if self.profile.scheme == "fs" {
            let root = self
                .profile
                .options
                .get("root")
                .cloned()
                .unwrap_or_else(crate::localfs::root);
            let paths: Vec<PathBuf> = entries
                .iter()
                .map(|entry| crate::localfs::full_path(&root, &entry.path))
                .collect();
            preview_files(paths);
            return;
        }

        let files: Vec<FsEntry> = entries.into_iter().filter(|entry| !entry.is_dir).collect();
        if files.is_empty() {
            window.push_notification("Quick Look works on remote files, not folders.", cx);
            return;
        }
        let Some(provider) = self.provider().cloned() else { return };
        let tmp = quicklook_dir(self.site_id(), &self.path);
        let task = cx.global::<AppState>().runtime.spawn(async move {
            tokio::fs::create_dir_all(&tmp).await?;
            let mut paths = Vec::new();
            for entry in files {
                let dest = tmp.join(entry.name.as_ref());
                provider.read_to_file(&entry.path, &dest).await?;
                paths.push(dest);
            }
            anyhow::Ok(paths)
        });
        window.push_notification("Preparing Quick Look…", cx);
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("quick look task failed: {join_error}")),
            };
            this.update_in(cx, |_, window, cx| match result {
                Ok(paths) => preview_files(paths),
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), "quick look failed");
                    notify_error(&error, window, cx);
                }
            })
            .ok();
        })
        .detach();
    }

    // --- rubber band ---

    /// Window position → content coordinates (y = 0 at the top of row 0).
    fn content_point(&self, position: Point<Pixels>) -> Option<Point<Pixels>> {
        let bounds = self.list_bounds?;
        let offset = self.scroll_handle.0.borrow().base_handle.offset();
        Some(Point {
            x: position.x - bounds.origin.x,
            y: position.y - bounds.origin.y - offset.y,
        })
    }

    fn on_list_mouse_down(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.focus_handle, cx);
        cx.emit(PaneEvent::Focused);
        let Some(point) = self.content_point(event.position) else {
            return;
        };
        // Presses on a row are clicks or drags, not rubber bands.
        let row = (f32::from(point.y) / ROW_HEIGHT).floor();
        if f32::from(point.y) >= 0.0 && row >= 0.0 && (row as usize) < self.entries.len() {
            return;
        }
        let keep = event.modifiers.secondary() || event.modifiers.shift;
        let base = if keep {
            self.selection.clone()
        } else {
            BTreeSet::new()
        };
        self.selection = base.clone();
        self.rubber = Some(RubberBand {
            origin: point,
            current: point,
            base,
        });
        cx.notify();
    }

    fn on_list_mouse_move(
        &mut self,
        event: &MouseMoveEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.rubber.is_none() {
            return;
        }
        if event.pressed_button != Some(MouseButton::Left) {
            self.rubber = None;
            cx.notify();
            return;
        }
        // Dragging past the viewport edge scrolls the list under the band
        // (movement-driven, proportional to the overshoot).
        if let Some(bounds) = self.list_bounds {
            let y = f32::from(event.position.y);
            let top = f32::from(bounds.top());
            let bottom = f32::from(bounds.bottom());
            let overshoot = if y < top {
                y - top
            } else if y > bottom {
                y - bottom
            } else {
                0.0
            };
            if overshoot != 0.0 {
                let content = self.entries.len() as f32 * ROW_HEIGHT;
                let min_offset = (f32::from(bounds.size.height) - content).min(0.0);
                let handle = self.scroll_handle.0.borrow();
                let mut offset = handle.base_handle.offset();
                offset.y = px((f32::from(offset.y) - overshoot * 0.3).clamp(min_offset, 0.0));
                handle.base_handle.set_offset(offset);
            }
        }
        let Some(point) = self.content_point(event.position) else {
            return;
        };
        let Some(rubber) = &mut self.rubber else {
            return;
        };
        rubber.current = point;
        let y0 = f32::from(rubber.origin.y).min(f32::from(point.y));
        let y1 = f32::from(rubber.origin.y).max(f32::from(point.y));
        let mut selection = rubber.base.clone();
        if y1 >= 0.0 && !self.entries.is_empty() {
            let first = (y0.max(0.0) / ROW_HEIGHT).floor() as usize;
            let last = ((y1 / ROW_HEIGHT).floor() as usize).min(self.entries.len() - 1);
            for ix in first..=last {
                if self
                    .entries
                    .get(ix)
                    .is_some_and(|entry| !entry.is_parent)
                {
                    selection.insert(ix);
                }
            }
        }
        self.selection = selection;
        cx.notify();
    }

    fn end_rubber(&mut self, cx: &mut Context<Self>) {
        if self.rubber.take().is_some() {
            cx.notify();
        }
    }

    // --- drag & drop ---

    /// Entries an internal drag drops here: enqueue transfers into `dst_dir`,
    /// moving when Shift (macOS: also Cmd) is held. Dropping something onto
    /// the directory it's already in is a no-op.
    fn accept_entries(
        &mut self,
        drag: &DraggedEntries,
        dst_dir: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let dst_site = self.site_id();
        let move_source = is_move_drop(&window.modifiers());
        let queue = cx.global::<AppState>().queue.clone();
        let mut queued = 0;
        for entry in &drag.entries {
            if drag.site_id == dst_site
                && (parent_of(&entry.path) == dst_dir || entry.path == dst_dir)
            {
                continue;
            }
            queue.enqueue(NewJob {
                src_site_id: drag.site_id,
                dst_site_id: dst_site,
                src: entry.path.clone(),
                dst_dir: dst_dir.clone(),
                dst_name: None,
                conflict_policy: ConflictPolicy::Ask,
                move_source,
            });
            queued += 1;
        }
        if queued > 0 {
            window.push_notification(queued_message(queued), cx);
        }
    }

    /// Files dragged in from Finder/Explorer: they're paths on the reserved
    /// local site, so they enqueue like any other transfer.
    fn accept_external(
        &mut self,
        paths: &ExternalPaths,
        dst_dir: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.enqueue_local_paths(paths.paths(), dst_dir, window, cx);
    }

    fn upload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let dst_dir = self.path.clone();
        let site = self.site_id();
        let picker = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: true,
            multiple: true,
            prompt: Some("Upload files or folders".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = picker.await else {
                return;
            };
            this.update_in(cx, |this, window, cx| {
                if this.site_id() == site {
                    this.enqueue_local_paths(&paths, dst_dir, window, cx);
                }
            })
            .ok();
        })
        .detach();
    }

    fn enqueue_local_paths(
        &mut self,
        paths: &[PathBuf],
        dst_dir: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let dst_site = self.site_id();
        let queue = cx.global::<AppState>().queue.clone();
        let mut queued = 0;
        for path in paths {
            let src = crate::localfs::browse_path(path, path.is_dir());
            if dst_site == crate::queue::LOCAL_SITE_ID && parent_of(&src) == dst_dir {
                continue;
            }
            queue.enqueue(NewJob {
                src_site_id: crate::queue::LOCAL_SITE_ID,
                dst_site_id: dst_site,
                src,
                dst_dir: dst_dir.clone(),
                dst_name: None,
                conflict_policy: ConflictPolicy::Ask,
                move_source: false,
            });
            queued += 1;
        }
        if queued > 0 {
            window.push_notification(queued_message(queued), cx);
        }
    }

    // --- file operations (context menu) ---

    /// Run a provider operation on the app runtime, then reload the listing;
    /// failures surface as a notification instead of replacing the listing.
    fn spawn_op(
        &mut self,
        op: impl Future<Output = anyhow::Result<()>> + Send + 'static,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let task = cx.global::<AppState>().runtime.spawn(op);
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("operation task failed: {join_error}")),
            };
            this.update_in(cx, |this, window, cx| {
                if let Err(error) = result {
                    tracing::error!(error = %format!("{error:#}"), "file operation failed");
                    notify_error(&error, window, cx);
                }
                this.reload(window, cx);
            })
            .ok();
        })
        .detach();
    }

    /// Confirm, then delete. Local entries go to the OS trash; remote
    /// deletes are permanent and the dialog says so.
    fn confirm_delete(
        &mut self,
        entries: Vec<FsEntry>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if entries.is_empty() || self.is_stale() {
            return;
        }
        let pane = cx.entity().downgrade();
        let to_trash = self.profile.scheme == "fs";
        let bin = t(if cfg!(windows) {
            "file_pane.trash.bin_windows"
        } else {
            "file_pane.trash.bin"
        });
        let count = entries.len().to_string();
        let (title_one, title_other) = if to_trash {
            ("file_pane.trash.title_one", "file_pane.trash.title_other")
        } else {
            ("file_pane.delete.title_one", "file_pane.delete.title_other")
        };
        let title = match entries.as_slice() {
            [entry] => tf(title_one, &[("name", &entry.name), ("bin", &bin)]),
            _ => tf(title_other, &[("count", &count), ("bin", &bin)]),
        };
        let body = if to_trash {
            tf("file_pane.trash.body", &[("bin", &bin)])
        } else if entries.iter().any(|entry| entry.is_dir) {
            t("file_pane.delete.body_folders")
        } else {
            t("file_pane.delete.body")
        };
        let ok_text = if to_trash {
            tf("file_pane.trash.ok", &[("bin", &bin)])
        } else {
            t("file_pane.delete.ok")
        };
        window.open_alert_dialog(cx, move |dialog, _, _| {
            let pane = pane.clone();
            let entries = entries.clone();
            dialog
                .title(title.clone())
                .width(px(380.))
                .child(body.clone())
                .confirm()
                .button_props(
                    DialogButtonProps::default()
                        .ok_text(ok_text.clone())
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, window, cx| {
                    let entries = entries.clone();
                    pane.update(cx, |this, cx| {
                        let Some(provider) = this.provider().cloned() else { return };
                        this.spawn_op(
                            async move {
                                for entry in entries {
                                    if to_trash {
                                        provider.trash(&entry.path).await?;
                                    } else {
                                        provider.delete(&entry.path, entry.is_dir).await?;
                                    }
                                }
                                Ok(())
                            },
                            window,
                            cx,
                        );
                    })
                    .ok();
                    true
                })
        });
    }

    fn rename_dialog(&mut self, entry: FsEntry, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| InputState::new(window, cx).default_value(entry.name.clone()));
        let pane = cx.entity().downgrade();
        window.open_alert_dialog(cx, move |dialog, _, _| {
            let pane = pane.clone();
            let entry = entry.clone();
            let input = input.clone();
            dialog
                .title("Rename")
                .width(px(380.))
                .child(Input::new(&input))
                .confirm()
                .on_ok(move |_, window, cx| {
                    let new_name = input.read(cx).value().trim().to_string();
                    if new_name.is_empty() || new_name.contains('/') {
                        return false;
                    }
                    pane.update(cx, |this, cx| {
                        this.rename_entry(&entry, &new_name, window, cx)
                    })
                    .ok();
                    true
                })
        });
    }

    fn rename_entry(
        &mut self,
        entry: &FsEntry,
        new_name: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let mut to = format!("{}{new_name}", parent_of(&entry.path));
        if entry.is_dir {
            to.push('/');
        }
        if to == entry.path {
            return;
        }
        let Some(provider) = self.provider().cloned() else { return };
        let from = entry.path.clone();
        self.spawn_op(async move { provider.rename(&from, &to).await }, window, cx);
    }

    fn new_folder_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| InputState::new(window, cx).placeholder("folder name"));
        let pane = cx.entity().downgrade();
        window.open_alert_dialog(cx, move |dialog, _, _| {
            let pane = pane.clone();
            let input = input.clone();
            dialog
                .title("New folder")
                .width(px(380.))
                .child(Input::new(&input))
                .confirm()
                .on_ok(move |_, window, cx| {
                    let name = input.read(cx).value().trim().trim_matches('/').to_string();
                    if name.is_empty() {
                        return false;
                    }
                    pane.update(cx, |this, cx| {
                        let path = format!("{}{name}/", this.path);
                        let Some(provider) = this.provider().cloned() else { return };
                        this.spawn_op(async move { provider.create_dir(&path).await }, window, cx);
                    })
                    .ok();
                    true
                })
        });
    }

    // --- downloads ---

    fn enqueue_downloads(
        &mut self,
        entries: Vec<FsEntry>,
        dst_dir: String,
        dst_name: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let queue = cx.global::<AppState>().queue.clone();
        let count = entries.len();
        for entry in entries {
            queue.enqueue(NewJob {
                src_site_id: self.site_id(),
                dst_site_id: crate::queue::LOCAL_SITE_ID,
                src: entry.path,
                dst_dir: dst_dir.clone(),
                dst_name: dst_name.clone(),
                conflict_policy: ConflictPolicy::Ask,
                move_source: false,
            });
        }
        window.push_notification(queued_message(count), cx);
    }

    /// Download into the platform Downloads folder (falls back to the
    /// folder picker when the platform has none).
    fn download(&mut self, entries: Vec<FsEntry>, window: &mut Window, cx: &mut Context<Self>) {
        match dirs::download_dir() {
            Some(dir) => self.enqueue_downloads(entries, local_dir_path(&dir), None, window, cx),
            None => self.download_to(entries, window, cx),
        }
    }

    /// "Download To…": pick a destination folder.
    fn download_to(
        &mut self,
        entries: Vec<FsEntry>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let rx = cx.prompt_for_paths(PathPromptOptions {
            files: false,
            directories: true,
            multiple: false,
            prompt: Some("Download".into()),
        });
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(paths))) = rx.await else {
                return;
            };
            let Some(dir) = paths.into_iter().next() else {
                return;
            };
            this.update_in(cx, |this, window, cx| {
                this.enqueue_downloads(entries, local_dir_path(&dir), None, window, cx);
            })
            .ok();
        })
        .detach();
    }

    /// "Download As…": pick a destination path; the file name becomes the
    /// job's destination-name override.
    fn download_as(&mut self, entry: FsEntry, window: &mut Window, cx: &mut Context<Self>) {
        let start = dirs::download_dir().unwrap_or_else(|| PathBuf::from(crate::localfs::root()));
        let rx = cx.prompt_for_new_path(&start, Some(&entry.name));
        cx.spawn_in(window, async move |this, cx| {
            let Ok(Ok(Some(path))) = rx.await else {
                return;
            };
            let Some(name) = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
            else {
                return;
            };
            let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
            this.update_in(cx, |this, window, cx| {
                this.enqueue_downloads(
                    vec![entry],
                    local_dir_path(&dir),
                    Some(name),
                    window,
                    cx,
                );
            })
            .ok();
        })
        .detach();
    }

    // --- provider-specific operations ---

    /// Server-side copy beside the original under a `name copy` name.
    fn duplicate_entry(&mut self, entry: FsEntry, window: &mut Window, cx: &mut Context<Self>) {
        let new_name = duplicate_name(&entry.name, &self.all_entries);
        let to = format!("{}{new_name}", parent_of(&entry.path));
        let Some(provider) = self.provider().cloned() else { return };
        let from = entry.path.clone();
        self.spawn_op(async move { provider.copy(&from, &to).await }, window, cx);
    }

    /// Presign a GET and put the URL on the clipboard.
    fn copy_presigned(
        &mut self,
        entry: FsEntry,
        expire: Duration,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(provider) = self.provider().cloned() else { return };
        let path = entry.path.clone();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { provider.presign_read(&path, expire).await });
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("presign task failed: {join_error}")),
            };
            this.update_in(cx, |_, window, cx| match result {
                Ok(url) => {
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(url));
                    window.push_notification("Presigned URL copied to clipboard.", cx);
                }
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), "presign failed");
                    notify_error(&error, window, cx);
                }
            })
            .ok();
        })
        .detach();
    }

    /// Presign a GET and open it in the browser.
    fn open_presigned(
        &mut self,
        entry: FsEntry,
        expire: Duration,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(provider) = self.provider().cloned() else { return };
        let path = entry.path.clone();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { provider.presign_read(&path, expire).await });
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("presign task failed: {join_error}")),
            };
            this.update_in(cx, |_, window, cx| match result {
                Ok(url) => crate::ui::open_path_or_url(&url),
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), "presign failed");
                    notify_error(&error, window, cx);
                }
            })
            .ok();
        })
        .detach();
    }

    /// Recursively sum a folder and show the result right in the listing's
    /// Size column (the entry keeps it until the next reload).
    fn calc_folder_size(&mut self, entry: FsEntry, window: &mut Window, cx: &mut Context<Self>) {
        let Some(provider) = self.provider().cloned() else { return };
        let path = entry.path.clone();
        let name = entry.name.clone();
        let seq = self.load_seq;
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { provider.dir_size(&path).await });
        window.push_notification(format!("Calculating size of `{name}`…"), cx);
        let path = entry.path.clone();
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("size task failed: {join_error}")),
            };
            this.update_in(cx, |this, window, cx| match result {
                Ok((_files, bytes)) => {
                    if this.load_seq != seq {
                        return;
                    }
                    // `entries` holds clones of `all_entries`; write both.
                    for entry in this.all_entries.iter_mut().chain(this.entries.iter_mut()) {
                        if entry.path == path {
                            entry.size = bytes;
                        }
                    }
                    cx.notify();
                }
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), "folder size failed");
                    notify_error(&error, window, cx);
                }
            })
            .ok();
        })
        .detach();
    }

    /// Download to temp, open in the default app, and re-upload on save
    /// (see `crate::edit`).
    fn edit_external(&mut self, entry: FsEntry, window: &mut Window, cx: &mut Context<Self>) {
        let Some(provider) = self.provider().cloned() else { return };
        let queue = cx.global::<AppState>().queue.clone();
        let path = entry.path.clone();
        let name = entry.name.to_string();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { crate::edit::start(provider, path, name, queue).await });
        window.push_notification("Opening in external editor — saves upload back automatically.", cx);
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("edit task failed: {join_error}")),
            };
            if let Err(error) = result {
                tracing::error!(error = %format!("{error:#}"), "external edit failed");
                this.update_in(cx, |_, window, cx| {
                    notify_error(&error, window, cx);
                })
                .ok();
            }
        })
        .detach();
    }

    fn apply_storage_class(
        &mut self,
        entry: &FsEntry,
        class: &'static str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(provider) = self.provider().cloned() else { return };
        let path = entry.path.clone();
        self.spawn_op(
            async move { provider.set_storage_class(&path, class).await },
            window,
            cx,
        );
    }

    fn apply_acl(
        &mut self,
        entry: &FsEntry,
        acl: &'static str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(provider) = self.provider().cloned() else { return };
        let path = entry.path.clone();
        self.spawn_op(async move { provider.set_acl(&path, acl).await }, window, cx);
    }

    /// chmod dialog: stat the current mode first so the input starts from
    /// what's actually on the server.
    fn permissions_dialog(&mut self, entry: FsEntry, window: &mut Window, cx: &mut Context<Self>) {
        let Some(provider) = self.provider().cloned() else { return };
        let path = entry.path.clone();
        let known = entry.permissions;
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { provider.stat_mode(&path).await });
        cx.spawn_in(window, async move |this, cx| {
            let mode = match task.await {
                Ok(Ok(mode)) => mode.or(known),
                _ => known,
            };
            this.update_in(cx, |this, window, cx| {
                this.open_permissions_dialog(entry, mode, window, cx);
            })
            .ok();
        })
        .detach();
    }

    fn open_permissions_dialog(
        &mut self,
        entry: FsEntry,
        mode: Option<u32>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let initial = format!("{:03o}", mode.unwrap_or(0o644));
        let input = cx.new(|cx| InputState::new(window, cx).default_value(initial));
        let pane = cx.entity().downgrade();
        window.open_alert_dialog(cx, move |dialog, _, _| {
            let pane = pane.clone();
            let entry = entry.clone();
            let input = input.clone();
            dialog
                .title(format!("Permissions for `{}`", entry.name))
                .width(px(380.))
                .child("Octal mode, e.g. 644 or 755.")
                .child(Input::new(&input))
                .confirm()
                .on_ok(move |_, window, cx| {
                    let raw = input.read(cx).value().trim().to_string();
                    let Ok(mode) = u32::from_str_radix(&raw, 8) else {
                        return false;
                    };
                    if mode > 0o7777 {
                        return false;
                    }
                    pane.update(cx, |this, cx| {
                        let Some(provider) = this.provider().cloned() else { return };
                        let path = entry.path.clone();
                        this.spawn_op(
                            async move { provider.chmod(&path, mode).await },
                            window,
                            cx,
                        );
                    })
                    .ok();
                    true
                })
        });
    }

    // --- navigation / loading ---

    pub fn cancel_linked_navigation(&mut self) {
        self.linked_seq += 1;
    }

    pub fn navigate_linked(&mut self, path: String, window: &mut Window, cx: &mut Context<Self>) {
        self.linked_seq += 1;
        let seq = self.linked_seq;
        let Some(provider) = self.provider().cloned() else { return };
        let target = path.clone();
        let task = cx.global::<AppState>().runtime.spawn(async move {
            if target.is_empty() {
                provider.list(&target).await?;
            } else if provider.caps.virtual_dirs {
                let siblings = provider.list(&parent_of(&target)).await?;
                anyhow::ensure!(
                    siblings
                        .iter()
                        .any(|entry| entry.is_dir && entry.path == target),
                    "matching folder does not exist"
                );
            } else {
                let (op, rel) = provider.op_for(&target)?;
                let meta = op.stat(&rel).await?;
                anyhow::ensure!(meta.is_dir(), "matching path is not a folder");
            }
            Ok::<_, anyhow::Error>(())
        });
        cx.spawn_in(window, async move |this, cx| {
            let result = task.await;
            this.update_in(cx, |this, window, cx| {
                if this.linked_seq != seq {
                    return;
                }
                match result {
                    Ok(Ok(())) => {
                        this.navigate_to(path, window, cx);
                        this.linked_navigation = true;
                    }
                    _ => {
                        window.push_notification(
                            format!("Linked browsing stopped: cannot open matching folder /{path}"),
                            cx,
                        );
                        cx.emit(PaneEvent::LinkedNavigationFailed);
                    }
                }
            })
            .ok();
        })
        .detach();
    }

    pub fn navigate_to(&mut self, path: String, window: &mut Window, cx: &mut Context<Self>) {
        self.path_error = None;
        if path != self.path {
            self.back_stack.push(std::mem::replace(&mut self.path, path));
            self.forward_stack.clear();
        }
        self.close_search(window, cx);
        self.reload(window, cx);
        cx.emit(PaneEvent::Navigated);
    }

    pub fn go_back(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(path) = self.back_stack.pop() {
            self.forward_stack
                .push(std::mem::replace(&mut self.path, path));
            self.close_search(window, cx);
            self.reload(window, cx);
            cx.emit(PaneEvent::Navigated);
        }
    }

    pub fn go_forward(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(path) = self.forward_stack.pop() {
            self.back_stack
                .push(std::mem::replace(&mut self.path, path));
            self.close_search(window, cx);
            self.reload(window, cx);
            cx.emit(PaneEvent::Navigated);
        }
    }

    fn go_up(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.path.is_empty() {
            self.navigate_to(parent_of(&self.path), window, cx);
        }
    }

    /// Navigate to the breadcrumb ancestor made of the first `depth` path
    /// segments (`depth == 0` is the site root).
    fn navigate_to_depth(&mut self, depth: usize, window: &mut Window, cx: &mut Context<Self>) {
        let target = if depth == 0 {
            String::new()
        } else {
            let mut path: String = self
                .path
                .trim_matches('/')
                .split('/')
                .take(depth)
                .collect::<Vec<_>>()
                .join("/");
            path.push('/');
            path
        };
        self.navigate_to(target, window, cx);
    }

    /// Swap the breadcrumb for the editable path input (cmd-l), seeded with the
    /// current URL and focused for typing.
    fn edit_path(&mut self, _: &EditPath, window: &mut Window, cx: &mut Context<Self>) {
        self.begin_edit_path(window, cx);
    }

    fn begin_edit_path(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_path = true;
        self.path_error = None;
        let display = self.display_url();
        let focus = self.path_input.focus_handle(cx);
        self.path_input.update(cx, |input, cx| {
            input.set_value(display, window, cx);
        });
        window.focus(&focus, cx);
        cx.notify();
    }

    fn cancel_edit_path(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.editing_path {
            self.editing_path = false;
            self.path_error = None;
            window.focus(&self.focus_handle, cx);
            cx.notify();
        }
    }

    // --- local dir watcher ---

    /// Local panes watch their current directory so external changes
    /// (Finder, transfers, shells) show up without a manual refresh.
    fn init_watcher(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.profile.scheme != "fs" {
            return;
        }
        let (tx, rx) = smol::channel::bounded::<()>(16);
        let watcher = notify::recommended_watcher(
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
        );
        match watcher {
            Ok(watcher) => self.watcher = Some((watcher, PathBuf::new())),
            Err(error) => {
                tracing::warn!(%error, "dir auto-refresh disabled: cannot create watcher");
                return;
            }
        }
        cx.spawn_in(window, async move |this, cx| {
            while rx.recv().await.is_ok() {
                // Let write bursts settle, then coalesce queued events.
                cx.background_executor()
                    .timer(Duration::from_millis(250))
                    .await;
                while rx.try_recv().is_ok() {}
                let refreshed = this.update_in(cx, |this, window, cx| {
                    this.refresh(window, cx);
                });
                if refreshed.is_err() {
                    break;
                }
            }
        })
        .detach();
    }

    /// Point the watcher at the current directory (no-op for remote panes).
    fn update_watch(&mut self) {
        let Some((watcher, watched)) = &mut self.watcher else {
            return;
        };
        let root = self
            .profile
            .options
            .get("root")
            .cloned()
            .unwrap_or_else(crate::localfs::root);
        let dir = crate::localfs::full_path(&root, &self.path);
        if *watched == dir {
            return;
        }
        if watched.as_os_str().len() > 0 {
            watcher.unwatch(watched).ok();
        }
        *watched = match watcher.watch(&dir, notify::RecursiveMode::NonRecursive) {
            Ok(()) => dir,
            Err(error) => {
                tracing::debug!(%error, dir = %dir.display(), "watching dir failed");
                PathBuf::new()
            }
        };
    }

    /// Reload the listing but keep the current selection (matched by name).
    /// On a failed connection this retries connecting instead.
    pub fn refresh(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.connection {
            Connection::Ready(_) => {}
            Connection::Failed(_) => return self.connect(window, cx),
            Connection::Connecting => return,
        }
        self.pending_selection = Some(
            self.selected_entries()
                .into_iter()
                .map(|entry| entry.name)
                .collect(),
        );
        self.reload(window, cx);
        // Keep the folder tree in sync with what's visible in it.
        if self.tree_visible {
            let dirs: Vec<String> = std::iter::once(String::new())
                .chain(self.tree_expanded.iter().cloned())
                .collect();
            for dir in dirs {
                self.tree_load(dir, cx);
            }
        }
    }

    fn reload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(provider) = self.provider().cloned() else { return };
        self.update_watch();
        self.linked_navigation = false;
        self.linked_seq += 1;
        self.load_seq += 1;
        let seq = self.load_seq;
        let path = self.path.clone();

        let state = cx.global::<AppState>();
        let show_hidden = self.show_hidden;
        let list_task = state
            .runtime
            .spawn(async move { list_dir(provider, path, show_hidden).await });

        // Only rewrite the path bar when it actually changed, and never while
        // the user is editing it: a background refresh (fs watch, details
        // fetch) calling set_value on every reload otherwise resets the
        // input's rope + cursor, which reads as a flicker.
        let display = self.display_url();
        let editing = self.path_input.focus_handle(cx).is_focused(window);
        if !editing {
            self.path_input.update(cx, |input, cx| {
                if input.value().as_ref() != display.as_str() {
                    input.set_value(display, window, cx);
                }
            });
        }
        self.loading = true;
        self.slow_loading = false;
        self.error = None;
        self.details_open = false;
        self.selection.clear();
        self.cursor = None;
        self.anchor = None;
        self.rubber = None;
        cx.notify();

        // Fast listings never show the spinner.
        cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_millis(150))
                .await;
            this.update(cx, |this, cx| {
                if this.load_seq == seq && this.loading {
                    this.slow_loading = true;
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();

        cx.spawn_in(window, async move |this, cx| {
            let result = match list_task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("listing task failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                if this.load_seq != seq {
                    return;
                }
                this.loading = false;
                this.slow_loading = false;
                let pending = this.pending_selection.take();
                match result {
                    Ok(mut entries) => {
                        if !this.path.is_empty() {
                            entries.insert(
                                0,
                                FsEntry {
                                    name: "..".into(),
                                    path: parent_of(&this.path),
                                    is_dir: true,
                                    is_symlink: false,
                                    is_bucket: false,
                                    is_parent: true,
                                    is_hidden: false,
                                    size: 0,
                                    modified: None,
                                    permissions: None,
                                    storage_class: None,
                                    region: None,
                                },
                            );
                        }
                        this.all_entries = entries;
                        this.loaded_path = Some(this.path.clone());
                        this.apply_sort();
                        cx.emit(PaneEvent::DirectoryLoaded { linked: this.linked_navigation });
                        this.fetch_details(cx);
                        // Selection-preserving refresh: re-select by name.
                        if let Some(names) = pending {
                            this.selection = this
                                .entries
                                .iter()
                                .enumerate()
                                .filter(|(_, entry)| {
                                    !entry.is_parent && names.contains(&entry.name)
                                })
                                .map(|(ix, _)| ix)
                                .collect();
                        }
                    }
                    Err(error) => {
                        tracing::error!(path = %this.path, error = %format!("{error:#}"), "listing failed");
                        this.all_entries = Vec::new();
                        this.entries = Vec::new();
                        this.loaded_path = None;
                        this.error = Some(humanize(&error));
                        cx.emit(PaneEvent::LinkedNavigationFailed);
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// URL shown in the path bar: plain `/path` for the local pane,
    /// `scheme://site/path` for remote sites.
    fn display_url(&self) -> String {
        if self.profile.id == crate::queue::LOCAL_SITE_ID {
            format!("/{}", self.path)
        } else {
            format!("{}://{}/{}", self.profile.scheme, self.profile.name, self.path)
        }
    }

    /// The path bar: an editable input while `editing_path` (Escape cancels),
    /// otherwise the clickable breadcrumb.
    fn render_path_bar(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        if self.editing_path {
            return div()
                .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                    if event.keystroke.key.as_str() == "escape" {
                        cx.stop_propagation();
                        this.cancel_edit_path(window, cx);
                    }
                }))
                .child(Input::new(&self.path_input).small())
                .into_any_element();
        }
        self.render_breadcrumbs(cx)
    }

    /// Clickable breadcrumb over the current directory: the site root followed
    /// by one crumb per path segment. Deep paths collapse the leading segments
    /// behind an `…` crumb. Clicking the trailing space edits the raw path.
    fn render_breadcrumbs(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let segments: Vec<&str> = self
            .path
            .trim_matches('/')
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        let total = segments.len();
        // Keep the last MAX segments; collapse anything earlier behind an `…`.
        const MAX: usize = 4;
        let start = total.saturating_sub(MAX);

        let separator = |cx: &mut Context<Self>| {
            div()
                .px_0p5()
                .text_color(cx.theme().muted_foreground)
                .child("›")
        };

        let mut row = h_flex()
            .id("breadcrumbs")
            .gap_0p5()
            .items_center()
            .overflow_hidden()
            .text_sm()
            .child(self.breadcrumb_crumb("crumb-root".into(), self.label(), 0, total == 0, cx));
        if start > 0 {
            row = row
                .child(separator(cx))
                .child(self.breadcrumb_crumb("crumb-ellipsis".into(), "…".into(), start, false, cx));
        }
        for (ix, segment) in segments.iter().enumerate().skip(start) {
            let depth = ix + 1;
            let is_last = depth == total;
            row = row.child(separator(cx)).child(self.breadcrumb_crumb(
                SharedString::from(format!("crumb-{depth}")),
                SharedString::from(segment.to_string()),
                depth,
                is_last,
                cx,
            ));
        }
        // Trailing hit area: click the empty run to edit the raw path.
        row.child(
            div()
                .id("crumb-edit-zone")
                .flex_1()
                .min_w_0()
                .h_full()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, window, cx| this.begin_edit_path(window, cx)),
                ),
        )
        .into_any_element()
    }

    fn breadcrumb_crumb(
        &self,
        id: SharedString,
        label: SharedString,
        depth: usize,
        is_last: bool,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        div()
            .id(id)
            .px_1()
            .rounded_sm()
            .cursor_pointer()
            .whitespace_nowrap()
            .when(is_last, |el| el.text_color(cx.theme().foreground))
            .when(!is_last, |el| {
                el.text_color(cx.theme().muted_foreground)
                    .hover(|style| style.bg(cx.theme().list_hover))
            })
            .child(label)
            .on_click(cx.listener(move |this, _, window, cx| {
                this.navigate_to_depth(depth, window, cx)
            }))
            .into_any_element()
    }

    // --- rendering ---

    fn render_row(&self, ix: usize, cx: &mut Context<Self>) -> impl IntoElement {
        let entry = &self.entries[ix];
        let is_selected = self.selection.contains(&ix);
        let icons = FileIcons::get(cx);

        let icon = if entry.is_parent {
            Icon::new(IconName::ArrowUp)
                .text_color(cx.theme().muted_foreground)
                .small()
                .into_any_element()
        } else if entry.is_bucket {
            file_icons::icon_element(icons.bucket(), px(14.), cx.theme().accent_foreground)
        } else if entry.is_symlink {
            let color = if entry.is_dir {
                cx.theme().accent_foreground
            } else {
                cx.theme().muted_foreground
            };
            file_icons::icon_element(icons.symlink(entry.is_dir), px(14.), color)
        } else if entry.is_dir {
            file_icons::icon_element(
                icons.folder_named(&entry.name, false),
                px(14.),
                cx.theme().accent_foreground,
            )
        } else {
            file_icons::icon_element(icons.icon(&entry.name), px(14.), cx.theme().muted_foreground)
        };

        // Dragging a selected row carries the whole selection.
        let drag_entries = if is_selected && !entry.is_parent {
            self.selected_entries()
        } else if entry.is_parent {
            Vec::new()
        } else {
            vec![entry.clone()]
        };
        let ghost_what: SharedString = if let [single] = drag_entries.as_slice() {
            single.name.clone()
        } else {
            items_label(drag_entries.len())
        };
        let payload = DraggedEntries {
            site_id: self.site_id(),
            entries: drag_entries,
        };
        // Rows of a directory that is being replaced are inert.
        // They dim only once the load is slow, so fast navigation doesn't
        // flash.
        let stale = self.is_stale();
        let dimmed = stale && self.slow_loading;
        let is_drag_source = !payload.entries.is_empty() && !stale;

        // Dirs (and the `..` row) accept drops into themselves.
        let drop_dir = (entry.is_dir && !stale).then(|| entry.path.clone());

        let weak = cx.entity().downgrade();
        let entry_clone = entry.clone();

        h_flex()
            .id(ix)
            .w_full()
            .h(px(ROW_HEIGHT))
            .px_2()
            .gap_2()
            .text_sm()
            .when(!stale, |el| el.cursor_pointer())
            .overflow_hidden()
            .when(entry.is_hidden && !dimmed, |style| style.opacity(0.55))
            .when(dimmed, |style| style.opacity(0.4))
            // A constant left border keeps content from shifting when the
            // selection accent appears.
            .border_l_2()
            .when(is_selected, |style| {
                style
                    .bg(cx.theme().list_active)
                    .border_color(cx.theme().list_active_border)
            })
            .when(!is_selected, |style| {
                style
                    .border_color(gpui::transparent_black())
                    .hover(|style| style.bg(cx.theme().list_hover))
            })
            .on_click(cx.listener(move |this, event: &ClickEvent, window, cx| {
                this.on_row_click(ix, event, window, cx);
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, _: &MouseDownEvent, window, cx| {
                    this.on_row_right_down(ix, window, cx);
                }),
            )
            .when(is_drag_source, |el| {
                el.on_drag(payload, move |payload, grab_offset, _window, cx| {
                    cx.set_global(PendingDragOut(Some(payload.clone())));
                    let what = ghost_what.clone();
                    cx.new(|_| DragGhost { what, grab_offset })
                })
            })
            .when_some(drop_dir, |el, dir| {
                let dir_for_entries = dir.clone();
                // Drop-into-folder target: filled row with a primary accent
                // on the constant left border, so the target is unambiguous.
                el.drag_over::<DraggedEntries>(|style, _, _, cx| {
                    style
                        .bg(cx.theme().primary.opacity(0.2))
                        .border_color(cx.theme().primary)
                })
                .drag_over::<ExternalPaths>(|style, _, _, cx| {
                    style
                        .bg(cx.theme().primary.opacity(0.2))
                        .border_color(cx.theme().primary)
                })
                .on_drop(cx.listener(move |this, drag: &DraggedEntries, window, cx| {
                    this.accept_entries(drag, dir_for_entries.clone(), window, cx);
                }))
                .on_drop(cx.listener(move |this, paths: &ExternalPaths, window, cx| {
                    this.accept_external(paths, dir.clone(), window, cx);
                }))
            })
            .context_menu(move |menu, window, cx| {
                let Some(pane) = weak.upgrade() else {
                    return menu;
                };
                build_context_menu(menu, &pane, &entry_clone, window, cx)
            })
            .child(icon)
            .child(div().flex_1().min_w_0().truncate().child(entry.name.clone()))
            .children(self.columns.iter().map(|column| {
                div()
                    .w(self.column_width(*column))
                    .flex_shrink_0()
                    .overflow_hidden()
                    .when(column.right_aligned(), |el| el.text_right())
                    .text_color(cx.theme().muted_foreground)
                    .child(if entry.is_parent {
                        String::new()
                    } else {
                        column.text_for(entry)
                    })
            }))
    }

    /// The centered state over the file list: connecting, a failed connect
    /// or listing, a slow first load, an empty folder, or a filter with no
    /// matches. `None` while rows tell the story themselves.
    fn render_list_state(&self, cx: &mut Context<Self>) -> Option<gpui::AnyElement> {
        let spinner = |cx: &mut Context<Self>| {
            Spinner::new()
                .color(cx.theme().muted_foreground)
                .into_any_element()
        };
        let failure = |human: &Humanized| {
            (human.title.clone().into(), human.hint.clone().map(SharedString::from), Some(human.details.clone()))
        };
        match &self.connection {
            Connection::Connecting => {
                let title = tf("file_pane.connecting", &[("site", &self.profile.name)]);
                return Some(self.render_state(Some(spinner(cx)), title, None, None, Vec::new(), cx));
            }
            Connection::Failed(human) => {
                let (title, hint, details) = failure(human);
                let mut actions = vec![Button::new("connect-retry")
                    .label(t("file_pane.retry"))
                    .small()
                    .primary()
                    .on_click(cx.listener(|this, _, window, cx| this.connect(window, cx)))];
                if self.profile.id != crate::queue::LOCAL_SITE_ID {
                    let site_id = self.profile.id;
                    actions.push(
                        Button::new("connect-edit-site")
                            .label(t("file_pane.edit_site"))
                            .small()
                            .outline()
                            .on_click(cx.listener(move |_, _, _, cx| {
                                cx.emit(PaneEvent::EditSite(site_id))
                            })),
                    );
                }
                let icon = Icon::new(IconName::CircleX)
                    .text_color(cx.theme().danger)
                    .into_any_element();
                return Some(self.render_state(Some(icon), title, hint, details, actions, cx));
            }
            Connection::Ready(_) => {}
        }
        if let Some(human) = &self.error {
            let (title, hint, details) = failure(human);
            let actions = vec![
                Button::new("list-retry")
                    .label(t("file_pane.retry"))
                    .small()
                    .primary()
                    .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
                Button::new("list-go-up")
                    .label(t("file_pane.go_up"))
                    .small()
                    .outline()
                    .disabled(self.path.is_empty())
                    .on_click(cx.listener(|this, _, window, cx| this.go_up(window, cx))),
            ];
            let icon = Icon::new(IconName::TriangleAlert)
                .text_color(cx.theme().danger)
                .into_any_element();
            return Some(self.render_state(Some(icon), title, hint, details, actions, cx));
        }
        if self.loading {
            if !(self.slow_loading && self.entries.is_empty()) {
                return None;
            }
            let title = t("file_pane.loading");
            return Some(self.render_state(Some(spinner(cx)), title, None, None, Vec::new(), cx));
        }
        if self.entries.iter().any(|entry| !entry.is_parent) {
            return None;
        }
        let query = self.search_query.trim();
        if self.search_active && !query.is_empty() {
            let title = tf("file_pane.no_matches", &[("query", query)]);
            let actions = vec![Button::new("clear-filter")
                .label(t("file_pane.clear_filter"))
                .small()
                .outline()
                .on_click(cx.listener(|this, _, window, cx| this.close_search(window, cx)))];
            let icon = Icon::new(IconName::Search)
                .text_color(cx.theme().muted_foreground)
                .into_any_element();
            return Some(self.render_state(Some(icon), title, None, None, actions, cx));
        }
        let icon = Icon::new(IconName::FolderOpen)
            .text_color(cx.theme().muted_foreground)
            .into_any_element();
        let hint = Some(t("file_pane.empty_hint"));
        Some(self.render_state(Some(icon), t("file_pane.empty"), hint, None, Vec::new(), cx))
    }

    /// One centered list state: icon, title, hint, an optional "Details"
    /// disclosure for the raw error chain, and actions.
    fn render_state(
        &self,
        icon: Option<gpui::AnyElement>,
        title: SharedString,
        hint: Option<SharedString>,
        details: Option<String>,
        actions: Vec<Button>,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        let details_open = self.details_open;
        v_flex()
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .items_center()
            .justify_center()
            .gap_2()
            .px_4()
            .text_sm()
            .text_center()
            .children(icon)
            .child(
                div()
                    .max_w(px(420.))
                    .font_weight(gpui::FontWeight::MEDIUM)
                    .text_color(cx.theme().foreground)
                    .child(title),
            )
            .children(hint.map(|hint| {
                div()
                    .max_w(px(420.))
                    .text_color(cx.theme().muted_foreground)
                    .child(hint)
            }))
            .when(!actions.is_empty(), |el| {
                el.child(h_flex().pt_1().gap_2().children(actions))
            })
            .children(details.map(|details| {
                v_flex()
                    .items_center()
                    .gap_1()
                    .child(
                        Button::new("toggle-details")
                            .label(if details_open {
                                t("file_pane.hide_details")
                            } else {
                                t("file_pane.show_details")
                            })
                            .xsmall()
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.details_open = !this.details_open;
                                cx.notify();
                            })),
                    )
                    .when(details_open, |el| {
                        el.child(
                            div()
                                .id("error-details")
                                .max_w(px(420.))
                                .max_h(px(140.))
                                .overflow_y_scroll()
                                .px_2()
                                .py_1()
                                .rounded_md()
                                .border_1()
                                .border_color(cx.theme().border)
                                .text_xs()
                                .text_left()
                                .text_color(cx.theme().muted_foreground)
                                .child(details),
                        )
                    })
            }))
            .into_any_element()
    }

    /// The translucent rubber-band rectangle, converted back from content to
    /// container coordinates.
    fn render_rubber_band(&self, cx: &Context<Self>) -> Option<gpui::Div> {
        let rubber = self.rubber.as_ref()?;
        let offset = self.scroll_handle.0.borrow().base_handle.offset();
        let x0 = f32::from(rubber.origin.x).min(f32::from(rubber.current.x));
        let x1 = f32::from(rubber.origin.x).max(f32::from(rubber.current.x));
        let y0 = f32::from(rubber.origin.y).min(f32::from(rubber.current.y)) + f32::from(offset.y);
        let y1 = f32::from(rubber.origin.y).max(f32::from(rubber.current.y)) + f32::from(offset.y);
        Some(
            div()
                .absolute()
                .left(px(x0))
                .top(px(y0))
                .w(px((x1 - x0).max(1.0)))
                .h(px((y1 - y0).max(1.0)))
                .bg(cx.theme().primary.opacity(0.1))
                .border_1()
                .border_color(cx.theme().primary.opacity(0.4)),
        )
    }
}

/// S3 storage classes offered in the context menu.
const S3_STORAGE_CLASSES: [&str; 7] = [
    "STANDARD",
    "INTELLIGENT_TIERING",
    "STANDARD_IA",
    "ONEZONE_IA",
    "GLACIER_IR",
    "GLACIER",
    "DEEP_ARCHIVE",
];

/// Canned ACLs offered in the context menu.
const S3_ACLS: [&str; 4] = [
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
];

/// Presigned-URL lifetimes offered in the context menu.
const PRESIGN_EXPIRIES: [(&str, u64); 4] = [
    ("15 minutes", 900),
    ("1 hour", 3600),
    ("1 day", 86400),
    ("1 week", 604800),
];

/// Right-click menu, acting on the pane's current selection.
fn build_context_menu(
    menu: PopupMenu,
    pane: &Entity<FilePane>,
    row_entry: &FsEntry,
    window: &mut Window,
    cx: &mut Context<PopupMenu>,
) -> PopupMenu {
    if row_entry.is_parent {
        return menu;
    }
    let (selected, caps, scheme, is_local) = {
        let pane = pane.read(cx);
        let Some(caps) = pane.provider().map(|provider| provider.caps) else {
            return menu;
        };
        if pane.is_stale() {
            return menu;
        }
        (
            pane.selected_entries(),
            caps,
            pane.profile.scheme.clone(),
            pane.site_id() == crate::queue::LOCAL_SITE_ID,
        )
    };
    let selected = if selected.is_empty() {
        vec![row_entry.clone()]
    } else {
        selected
    };
    let count = selected.len();
    let single = (count == 1).then(|| selected[0].clone());
    let plural = |verb: &str| {
        if count == 1 {
            format!("{verb} “{}”", selected[0].name)
        } else {
            format!("{verb} {count} Items")
        }
    };
    let weak = pane.downgrade();

    // -- transfer --

    // The pane-to-pane transfer is a download or an upload depending on
    // which side of the split it starts from.
    let mut menu = menu;
    if !is_local {
        menu = menu
            .item(PopupMenuItem::new(plural("Download")).on_click({
                let pane = weak.clone();
                let selected = selected.clone();
                move |_, window, cx| {
                    pane.update(cx, |this, cx| {
                        this.download(selected.clone(), window, cx)
                    })
                    .ok();
                }
            }));
        if let Some(entry) = single.clone() {
            menu = menu.item(PopupMenuItem::new("Download As…").on_click({
                let pane = weak.clone();
                move |_, window, cx| {
                    pane.update(cx, |this, cx| {
                        this.download_as(entry.clone(), window, cx)
                    })
                    .ok();
                }
            }));
        }
        menu = menu.item(PopupMenuItem::new("Download To…").on_click({
            let pane = weak.clone();
            let selected = selected.clone();
            move |_, window, cx| {
                pane.update(cx, |this, cx| {
                    this.download_to(selected.clone(), window, cx)
                })
                .ok();
            }
        }));
    }
    // Pane-to-pane transfers; a move deletes each source once it arrived
    // safely. The actions only label the shortcuts, the click handlers act
    // on the selection captured here.
    menu = menu
        .item(
            PopupMenuItem::new(t("file_pane.menu.copy_to_other"))
                .action(Box::new(CopyToOtherPane))
                .on_click({
                    let pane = weak.clone();
                    let selected = selected.clone();
                    move |_, _, cx| {
                        pane.update(cx, |_, cx| {
                            cx.emit(PaneEvent::CopyRequested(selected.clone()))
                        })
                        .ok();
                    }
                }),
        )
        .item(
            PopupMenuItem::new(t("file_pane.menu.move_to_other"))
                .action(Box::new(MoveToOtherPane))
                .on_click({
                    let pane = weak.clone();
                    let selected = selected.clone();
                    move |_, _, cx| {
                        pane.update(cx, |_, cx| {
                            cx.emit(PaneEvent::MoveRequested(selected.clone()))
                        })
                        .ok();
                    }
                }),
        );
    if row_entry.is_dir {
        menu = menu.item(PopupMenuItem::new("Open in new tab").on_click({
            let pane = weak.clone();
            let path = row_entry.path.clone();
            move |_, _, cx| {
                pane.update(cx, |this, cx| {
                    cx.emit(PaneEvent::OpenInNewTab {
                        profile: this.profile.clone(),
                        path: path.clone(),
                    })
                })
                .ok();
            }
        }));
    }

    // -- edit --

    let can_rename = single
        .as_ref()
        .is_some_and(|entry| pane.read(cx).can_rename(entry));
    // Server-side copy is file-level only.
    let can_duplicate = single
        .as_ref()
        .is_some_and(|entry| caps.copy && !entry.is_dir && !entry.is_bucket);
    menu = menu
        .separator()
        .item(
            PopupMenuItem::new("Rename…")
                .disabled(!can_rename)
                .on_click({
                    let pane = weak.clone();
                    let entry = selected[0].clone();
                    move |_, window, cx| {
                        pane.update(cx, |this, cx| {
                            this.rename_dialog(entry.clone(), window, cx)
                        })
                        .ok();
                    }
                }),
        )
        .item(
            PopupMenuItem::new("Duplicate")
                .disabled(!can_duplicate)
                .on_click({
                    let pane = weak.clone();
                    let entry = selected[0].clone();
                    move |_, window, cx| {
                        pane.update(cx, |this, cx| {
                            this.duplicate_entry(entry.clone(), window, cx)
                        })
                        .ok();
                    }
                }),
        );
    if !is_local {
        if let Some(entry) = single
            .clone()
            .filter(|entry| !entry.is_dir && !entry.is_bucket)
        {
            menu = menu.item(PopupMenuItem::new("Edit in External Editor").on_click({
                let pane = weak.clone();
                move |_, window, cx| {
                    pane.update(cx, |this, cx| {
                        this.edit_external(entry.clone(), window, cx)
                    })
                    .ok();
                }
            }));
        }
    }
    if let Some(entry) = single.clone().filter(|entry| entry.is_dir && !entry.is_bucket) {
        menu = menu.item(PopupMenuItem::new("Calculate Folder Size").on_click({
            let pane = weak.clone();
            move |_, window, cx| {
                pane.update(cx, |this, cx| {
                    this.calc_folder_size(entry.clone(), window, cx)
                })
                .ok();
            }
        }));
    }

    // -- provider-specific --

    let presignable = single
        .as_ref()
        .is_some_and(|entry| caps.presign && !entry.is_dir && !entry.is_bucket);
    if presignable {
        let entry = single.clone().expect("presignable implies single");
        menu = menu.separator();
        {
            let pane = weak.clone();
            let entry = entry.clone();
            menu = menu.item(PopupMenuItem::new("Open Presigned URL").on_click(
                move |_, window, cx| {
                    pane.update(cx, |this, cx| {
                        this.open_presigned(
                            entry.clone(),
                            Duration::from_secs(3600),
                            window,
                            cx,
                        )
                    })
                    .ok();
                },
            ));
        }
        let pane = weak.clone();
        menu = menu.submenu("Copy Presigned URL", window, cx, move |menu, _, _| {
            PRESIGN_EXPIRIES.into_iter().fold(menu, |menu, (label, secs)| {
                let pane = pane.clone();
                let entry = entry.clone();
                menu.item(PopupMenuItem::new(label).on_click(move |_, window, cx| {
                    pane.update(cx, |this, cx| {
                        this.copy_presigned(
                            entry.clone(),
                            Duration::from_secs(secs),
                            window,
                            cx,
                        )
                    })
                    .ok();
                }))
            })
        });
    }

    if scheme == "s3" {
        if let Some(entry) = single
            .clone()
            .filter(|entry| !entry.is_dir && !entry.is_bucket)
        {
            if !presignable {
                menu = menu.separator();
            }
            let current = entry.storage_class.clone();
            let pane = weak.clone();
            let class_entry = entry.clone();
            menu = menu.submenu("Storage Class", window, cx, move |menu, _, _| {
                S3_STORAGE_CLASSES.into_iter().fold(menu, |menu, class| {
                    let pane = pane.clone();
                    let entry = class_entry.clone();
                    menu.item(
                        PopupMenuItem::new(class)
                            .checked(current.as_deref() == Some(class))
                            .on_click(move |_, window, cx| {
                                pane.update(cx, |this, cx| {
                                    this.apply_storage_class(&entry, class, window, cx)
                                })
                                .ok();
                            }),
                    )
                })
            });
            let pane = weak.clone();
            menu = menu.submenu("ACL", window, cx, move |menu, _, _| {
                S3_ACLS.into_iter().fold(menu, |menu, acl| {
                    let pane = pane.clone();
                    let entry = entry.clone();
                    menu.item(PopupMenuItem::new(acl).on_click(move |_, window, cx| {
                        pane.update(cx, |this, cx| this.apply_acl(&entry, acl, window, cx))
                            .ok();
                    }))
                })
            });
        }
    }

    if supports_permissions(&scheme) {
        if let Some(entry) = single.clone() {
            let pane = weak.clone();
            menu = menu.separator().item(PopupMenuItem::new("Permissions…").on_click(
                move |_, window, cx| {
                    pane.update(cx, |this, cx| {
                        this.permissions_dialog(entry.clone(), window, cx)
                    })
                    .ok();
                },
            ));
        }
    }

    menu.separator()
        .item(PopupMenuItem::new(format!("{}…", plural("Delete"))).on_click({
            let pane = weak;
            move |_, window, cx| {
                pane.update(cx, |this, cx| {
                    this.confirm_delete(selected.clone(), window, cx)
                })
                .ok();
            }
        }))
}

impl Render for FilePane {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Config hot reload: re-seed ui.* derived state once per epoch.
        let epoch = cx.global::<AppState>().config_epoch;
        if epoch != self.config_epoch_seen {
            self.config_epoch_seen = epoch;
            self.apply_config(_window, cx);
        }

        let entry_rows = self.entries.len();

        let weak = cx.entity().downgrade();
        let bounds_probe = canvas(
            move |bounds, _, cx| {
                weak.update(cx, |this, _| this.list_bounds = Some(bounds)).ok();
            },
            |_, _, _, _| {},
        )
        .absolute()
        .size_full();

        let pane_dir = self.path.clone();
        let pane_dir_ext = pane_dir.clone();
        let tree_panel = self.render_tree_panel(cx);
        let list_state = self.render_list_state(cx);
        let connected = self.provider().is_some();

        v_flex()
            .size_full()
            .overflow_hidden()
            // Sidebar surface across the file tree + list, matching the docked
            // transfers / log panels.
            .bg(cx.theme().sidebar)
            .key_context(KEY_CONTEXT)
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(Self::select_next))
            .on_action(cx.listener(Self::select_prev))
            .on_action(cx.listener(Self::extend_next))
            .on_action(cx.listener(Self::extend_prev))
            .on_action(cx.listener(Self::select_first))
            .on_action(cx.listener(Self::select_last))
            .on_action(cx.listener(Self::select_all))
            .on_action(cx.listener(Self::clear_selection))
            .on_action(cx.listener(Self::activate))
            .on_action(cx.listener(Self::go_up_action))
            .on_action(cx.listener(Self::go_back_action))
            .on_action(cx.listener(Self::go_forward_action))
            .on_action(cx.listener(Self::toggle_hidden))
            .on_action(cx.listener(Self::refresh_action))
            .on_action(cx.listener(Self::new_folder_action))
            .on_action(cx.listener(Self::rename_selected))
            .on_action(cx.listener(Self::delete_selected))
            .on_action(cx.listener(Self::quick_look_action))
            .on_action(cx.listener(Self::search_action))
            .on_action(cx.listener(Self::toggle_tree))
            .on_action(cx.listener(Self::edit_path))
            .on_action(cx.listener(Self::copy_to_other_pane))
            .on_action(cx.listener(Self::move_to_other_pane))
            // Typing a printable character starts type-to-search.
            .on_key_down(cx.listener(Self::on_pane_key_down))
            // Column resize: the header handle sets `resizing`; the pane
            // tracks the pointer so the drag survives leaving the header.
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                if let Some((start, size0)) = this.tree_resize {
                    if event.pressed_button != Some(MouseButton::Left) {
                        this.tree_resize = None;
                        return;
                    }
                    // Whole pixels so the persisted value survives the
                    // config write→reload round-trip without churn (the writer
                    // rounds to one decimal; see `KesselApp::on_split_resized`).
                    if this.tree_on_top {
                        let height =
                            (size0 + (f32::from(event.position.y) - start)).clamp(100., 400.).round();
                        cx.global_mut::<AppState>().config.ui.tree_height = height;
                    } else {
                        let width =
                            (size0 + (f32::from(event.position.x) - start)).clamp(120., 400.).round();
                        cx.global_mut::<AppState>().config.ui.tree_width = width;
                    }
                    cx.notify();
                    return;
                }
                let Some((column, x0, w0)) = this.resizing else {
                    return;
                };
                if event.pressed_button != Some(MouseButton::Left) {
                    this.resizing = None;
                    return;
                }
                let width = (w0 + (x0 - f32::from(event.position.x))).clamp(50., 600.);
                this.col_widths.insert(column, width);
                cx.notify();
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _: &MouseUpEvent, _, cx| {
                    if this.resizing.take().is_some() {
                        cx.notify();
                    }
                    if this.tree_resize.take().is_some() {
                        // Persist the final width back into config.json5.
                        let ui = cx.global::<AppState>().config.ui.clone();
                        if let Err(error) = crate::config::save_ui_state(&ui) {
                            tracing::error!(%error, "persisting tree width failed");
                        }
                        cx.notify();
                    }
                }),
            )
            // Mouse back/forward buttons walk the pane's history.
            .on_mouse_down(
                MouseButton::Navigate(NavigationDirection::Back),
                cx.listener(|this, _: &MouseDownEvent, window, cx| this.go_back(window, cx)),
            )
            .on_mouse_down(
                MouseButton::Navigate(NavigationDirection::Forward),
                cx.listener(|this, _: &MouseDownEvent, window, cx| this.go_forward(window, cx)),
            )
            .child(
                h_flex()
                    .gap_1()
                    .px_2()
                    .py_1()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child({
                        Button::new("toggle-tree")
                            .icon(Icon::default().path("icons/list-tree.svg"))
                            .small()
                            .ghost()
                            .selected(self.tree_visible)
                            .tooltip(if self.tree_visible {
                                "Hide folder tree"
                            } else {
                                "Show folder tree"
                            })
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.toggle_tree(&ToggleTree, window, cx);
                            }))
                    })
                    .child(
                        Button::new("back")
                            .icon(Icon::new(IconName::ChevronLeft))
                            .ghost()
                            .small()
                            .disabled(self.back_stack.is_empty())
                            .tooltip("Back")
                            .on_click(cx.listener(|this, _, window, cx| this.go_back(window, cx))),
                    )
                    .child(
                        Button::new("forward")
                            .icon(Icon::new(IconName::ChevronRight))
                            .ghost()
                            .small()
                            .disabled(self.forward_stack.is_empty())
                            .tooltip("Forward")
                            .on_click(
                                cx.listener(|this, _, window, cx| this.go_forward(window, cx)),
                            ),
                    )
                    .child(
                        Button::new("up")
                            .icon(Icon::new(IconName::ArrowUp))
                            .ghost()
                            .small()
                            .on_click(cx.listener(|this, _, window, cx| this.go_up(window, cx))),
                    )
                    .child(div().flex_1().min_w_0().child(self.render_path_bar(cx)))
                    .when(self.slow_loading, |el| {
                        el.child(
                            Spinner::new()
                                .small()
                                .color(cx.theme().muted_foreground),
                        )
                    })
                    .child(
                        Button::new("upload")
                            .icon(Icon::new(IconName::ArrowUp))
                            .label("Upload…")
                            .ghost()
                            .small()
                            .disabled(!connected)
                            .tooltip("Upload files or full folders into this directory")
                            .on_click(cx.listener(|this, _, window, cx| this.upload(window, cx))),
                    )
                    .child(
                        Button::new("new-folder")
                            .icon(Icon::default().path("icons/folder-plus.svg"))
                            .ghost()
                            .small()
                            .disabled(!connected)
                            .tooltip("New folder")
                            .on_click(
                                cx.listener(|this, _, window, cx| {
                                    this.new_folder_dialog(window, cx)
                                }),
                            ),
                    )
                    .child(
                        Button::new("refresh")
                            .icon(Icon::default().path("icons/rotate-cw.svg"))
                            .ghost()
                            .small()
                            .on_click(cx.listener(|this, _, window, cx| this.refresh(window, cx))),
                    ),
            )
            .children(self.path_error.clone().map(|message| {
                div()
                    .px_2()
                    .py_1()
                    .text_sm()
                    .text_color(cx.theme().danger)
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(message)
            }))
            // Folder tree + header/list/status column. Side-by-side (narrow)
            // panes stack the tree above the list (v_flex); otherwise it sits
            // to the left (h_flex).
            .child({
                let tree_on_top = self.tree_on_top;
                let container = if tree_on_top { v_flex() } else { h_flex() };
                container
                .flex_1()
                .min_h_0()
                .w_full()
                .children(tree_panel)
                .child(v_flex()
                    .flex_1()
                    .when(tree_on_top, |el| el.w_full().min_h_0())
                    .when(!tree_on_top, |el| el.h_full().min_w_0())
                    .overflow_hidden()
            .child(
                h_flex()
                    .id("list-header")
                    .w_full()
                    // 2px extra left padding matches the rows' constant
                    // selection border so the header stays aligned.
                    .pl(px(10.))
                    .pr_2()
                    .py_0p5()
                    .gap_2()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .context_menu({
                        let weak = cx.entity().downgrade();
                        let visible = self.columns.clone();
                        let scheme = self.profile.scheme.clone();
                        let show_hidden = self.show_hidden;
                        move |menu, _, _| {
                            let menu = Column::ALL
                                .into_iter()
                                .filter(|column| column.available_for(&scheme))
                                .fold(menu, |menu, column| {
                                    let weak = weak.clone();
                                    menu.item(
                                        PopupMenuItem::new(column.title())
                                            .checked(visible.contains(&column))
                                            .on_click(move |_, _, cx| {
                                                weak.update(cx, |this, cx| {
                                                    this.toggle_column(column, cx)
                                                })
                                                .ok();
                                            }),
                                    )
                                });
                            let weak = weak.clone();
                            menu.separator().item(
                                PopupMenuItem::new("Show hidden files")
                                    .checked(show_hidden)
                                    .on_click(move |_, window, cx| {
                                        weak.update(cx, |this, cx| {
                                            this.toggle_hidden(&ToggleHidden, window, cx)
                                        })
                                        .ok();
                                    }),
                            )
                        }
                    })
                    .child(div().w(px(14.)).flex_shrink_0())
                    .child(
                        div()
                            .id("hdr-name")
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .cursor_pointer()
                            .on_click(cx.listener(|this, _, _, cx| this.set_sort(None, cx)))
                            .child(format!("Name{}", self.sort_marker(None))),
                    )
                    .children(self.columns.iter().enumerate().map(|(ix, column)| {
                        let column = *column;
                        div()
                            .id(("hdr-col", ix))
                            .relative()
                            .w(self.column_width(column))
                            .flex_shrink_0()
                            .overflow_hidden()
                            .cursor_pointer()
                            .when(column.right_aligned(), |el| el.text_right())
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.set_sort(Some(column), cx)
                            }))
                            .child(format!(
                                "{}{}",
                                column.title(),
                                self.sort_marker(Some(column))
                            ))
                            // Grab zone on the cell's left edge: drag to
                            // resize (the pane's mouse-move does the math).
                            .child(
                                div()
                                    .absolute()
                                    .left_0()
                                    .top_0()
                                    .bottom_0()
                                    .w(px(6.))
                                    .cursor_col_resize()
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(move |this, event: &MouseDownEvent, _, cx| {
                                            cx.stop_propagation();
                                            let width =
                                                f32::from(this.column_width(column));
                                            this.resizing = Some((
                                                column,
                                                f32::from(event.position.x),
                                                width,
                                            ));
                                        }),
                                    ),
                            )
                    })),
            )
            .child(
                div()
                    .id("file-list")
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, event: &MouseDownEvent, window, cx| {
                            this.on_list_mouse_down(event, window, cx);
                        }),
                    )
                    .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, window, cx| {
                        this.on_list_mouse_move(event, window, cx);
                    }))
                    .on_mouse_up(
                        MouseButton::Left,
                        cx.listener(|this, _: &MouseUpEvent, _, cx| this.end_rubber(cx)),
                    )
                    .on_mouse_up_out(
                        MouseButton::Left,
                        cx.listener(|this, _: &MouseUpEvent, _, cx| this.end_rubber(cx)),
                    )
                    // Drop-into-current-directory target: soft fill plus an
                    // inset primary ring around the whole list.
                    .drag_over::<DraggedEntries>(|style, _, _, cx| {
                        style
                            .bg(cx.theme().primary.opacity(0.08))
                            .border_2()
                            .border_color(cx.theme().primary.opacity(0.6))
                    })
                    .drag_over::<ExternalPaths>(|style, _, _, cx| {
                        style
                            .bg(cx.theme().primary.opacity(0.08))
                            .border_2()
                            .border_color(cx.theme().primary.opacity(0.6))
                    })
                    .on_drop(cx.listener(move |this, drag: &DraggedEntries, window, cx| {
                        this.accept_entries(drag, pane_dir.clone(), window, cx);
                    }))
                    .on_drop(cx.listener(move |this, paths: &ExternalPaths, window, cx| {
                        this.accept_external(paths, pane_dir_ext.clone(), window, cx);
                    }))
                    .child(
                        uniform_list(
                            "file-entries",
                            entry_rows,
                            cx.processor(|this, range: Range<usize>, _window, cx| {
                                range.map(|ix| this.render_row(ix, cx)).collect()
                            }),
                        )
                        .track_scroll(&self.scroll_handle)
                        .size_full(),
                    )
                    .child(bounds_probe)
                    .children(self.render_rubber_band(cx))
                    .children(list_state),
            )
            .when(self.search_active, |el| {
                let matches = self
                    .entries
                    .iter()
                    .filter(|entry| !entry.is_parent)
                    .count();
                el.child(
                    h_flex()
                        .px_2()
                        .py_1()
                        .gap_2()
                        .items_center()
                        .border_t_1()
                        .border_color(cx.theme().border)
                        // Steer list navigation from inside the input.
                        .on_key_down(cx.listener(
                            |this, event: &KeyDownEvent, window, cx| {
                                match event.keystroke.key.as_str() {
                                    "escape" => this.close_search(window, cx),
                                    "up" => this.step_cursor(-1, false, cx),
                                    "down" => this.step_cursor(1, false, cx),
                                    _ => return,
                                }
                                cx.stop_propagation();
                            },
                        ))
                        .child(
                            Icon::new(IconName::Search)
                                .small()
                                .text_color(cx.theme().muted_foreground),
                        )
                        .child(div().flex_1().child(Input::new(&self.search_input).small()))
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .whitespace_nowrap()
                                .child(if matches == 1 {
                                    "1 match".to_string()
                                } else {
                                    format!("{matches} matches")
                                }),
                        )
                        .child(
                            Button::new("close-search")
                                .icon(Icon::new(IconName::Close))
                                .ghost()
                                .xsmall()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.close_search(window, cx)
                                })),
                        ),
                )
            })
            )
            })
    }
}

/// Case-insensitive fuzzy (subsequence) match: every query character must
/// appear in `name`, in order — "rlib" hits "railib.rs".
fn fuzzy_match(name: &str, query: &str) -> bool {
    let mut name_chars = name.chars().flat_map(char::to_lowercase);
    query
        .chars()
        .flat_map(char::to_lowercase)
        .all(|q| name_chars.any(|c| c == q))
}

async fn list_dir(
    provider: Provider,
    path: String,
    show_hidden: bool,
) -> anyhow::Result<Vec<FsEntry>> {
    let raw = provider.list(&path).await?;
    let entries: Vec<FsEntry> = raw
        .into_iter()
        .filter_map(|entry| {
            let is_hidden = entry.name.starts_with('.');
            if entry.name.is_empty() || (!show_hidden && is_hidden) {
                return None;
            }
            Some(FsEntry {
                name: entry.name.into(),
                is_dir: entry.is_dir,
                is_symlink: entry.is_symlink,
                is_bucket: entry.is_bucket,
                is_parent: false,
                is_hidden,
                size: entry.size,
                modified: entry.modified,
                permissions: entry.permissions,
                storage_class: entry.storage_class,
                region: None,
                path: entry.path,
            })
        })
        .collect();
    // Ordering is the pane's job (`apply_sort`), not the loader's.
    Ok(entries)
}

/// Temp folder that remote Quick Look previews download into, stable per
/// (site, directory) so repeated previews reuse it.
fn quicklook_dir(site_id: i64, dir: &str) -> PathBuf {
    let safe: String = dir
        .chars()
        .map(|c| if c == '/' { '_' } else { c })
        .collect();
    std::env::temp_dir()
        .join("kessel-quicklook")
        .join(format!("{site_id}-{safe}"))
}

/// Open the platform previewer on a set of local paths: Quick Look on
/// macOS, the default opener elsewhere.
fn preview_files(paths: Vec<PathBuf>) {
    if paths.is_empty() {
        return;
    }
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("qlmanage")
            .arg("-p")
            .args(&paths)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok();
    }
    #[cfg(not(target_os = "macos"))]
    {
        crate::ui::open_path_or_url(paths[0].to_string_lossy().as_ref());
    }
}

/// Whether a backend has unix permission bits to show and edit. The local
/// filesystem only does on unix; sftp always does, but only exists at all in
/// a unix build (see `sftp_unsupported.rs`).
fn supports_permissions(scheme: &str) -> bool {
    scheme == "sftp" || (cfg!(unix) && scheme == "fs")
}

/// Local directory → browse path on the reserved local site (root-relative,
/// trailing `/` unless it *is* the root).
pub(crate) fn local_dir_path(dir: &Path) -> String {
    crate::localfs::dir_browse_path(dir)
}

/// `report.pdf` → `report copy.pdf`, then `report copy 2.pdf`, … — first
/// name not already taken in the listing.
fn duplicate_name(name: &str, entries: &[FsEntry]) -> String {
    let (stem, ext) = match name.rfind('.').filter(|&ix| ix > 0) {
        Some(ix) => name.split_at(ix),
        None => (name, ""),
    };
    let taken = |candidate: &str| entries.iter().any(|entry| entry.name.as_ref() == candidate);
    let mut candidate = format!("{stem} copy{ext}");
    let mut n = 2;
    while taken(&candidate) {
        candidate = format!("{stem} copy {n}{ext}");
        n += 1;
    }
    candidate
}

/// `"a/b/c/"` → `"a/b/"`, `"a/"` → `""`.
fn parent_of(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(ix) => trimmed[..=ix].to_string(),
        None => String::new(),
    }
}

/// What the path bar's text points at.
#[derive(Debug, PartialEq)]
enum PathInput {
    /// A directory on this pane's site (OpenDAL dir path).
    Here(String),
    /// A URL on another site or server, as `scheme://authority`.
    Elsewhere(String),
}

/// Path-bar input → target. Plain paths browse this site. URLs browse it
/// only when their authority is this site: the name the bar displays
/// (`scheme://site/path`), the server host, or (S3) the bucket.
fn parse_path_input(input: &str, profile: &SiteProfile) -> PathInput {
    let input = input.trim();
    let Some((scheme, rest)) = input.split_once("://") else {
        return PathInput::Here(normalize_path(input));
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let elsewhere = || PathInput::Elsewhere(format!("{scheme}://{authority}"));
    let is_fs = profile.scheme == "fs";
    if is_fs && scheme.eq_ignore_ascii_case("file") {
        return if authority.is_empty() || authority.eq_ignore_ascii_case("localhost") {
            PathInput::Here(normalize_path(path))
        } else {
            elsewhere()
        };
    }
    if !scheme.eq_ignore_ascii_case(&profile.scheme) {
        return elsewhere();
    }
    // Exact, as the bar displays it; a case-insensitive match could shadow
    // an S3 bucket of the same name.
    if authority == profile.name {
        return PathInput::Here(normalize_path(path));
    }
    if profile.scheme == "s3" {
        let root = profile
            .options
            .get("root")
            .map(|root| root.trim_matches('/'))
            .unwrap_or("");
        return match profile.options.get("bucket").filter(|bucket| !bucket.is_empty()) {
            // Bucket-root site: the bucket is the first browse segment.
            None => PathInput::Here(normalize_path(&format!("{authority}/{path}"))),
            Some(bucket) if bucket == authority => {
                let path = path.trim_start_matches('/');
                if root.is_empty() {
                    PathInput::Here(normalize_path(path))
                } else if let Some(rest) = path.strip_prefix(root).filter(|rest| {
                    rest.is_empty() || rest.starts_with('/')
                }) {
                    PathInput::Here(normalize_path(rest))
                } else {
                    elsewhere()
                }
            }
            Some(_) => elsewhere(),
        };
    }
    let site_host = profile.options.get("endpoint").map(|endpoint| url_host(endpoint));
    if site_host.is_some_and(|host| !host.is_empty() && host.eq_ignore_ascii_case(url_host(authority))) {
        PathInput::Here(normalize_path(path))
    } else {
        elsewhere()
    }
}

/// Host part of a URL or authority: `ssh://user@host:22/x` → `host`.
fn url_host(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    host.split(':').next().unwrap_or(host)
}

/// Indices of the non-`..` rows whose paths are in `paths`.
fn select_paths(entries: &[FsEntry], paths: &HashSet<String>) -> BTreeSet<usize> {
    entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| !entry.is_parent && paths.contains(&entry.path))
        .map(|(ix, _)| ix)
        .collect()
}

/// "1 item" / "N items".
fn items_label(count: usize) -> SharedString {
    if count == 1 {
        t("file_pane.items.one")
    } else {
        tf("file_pane.items.other", &[("count", &count.to_string())])
    }
}

fn queued_message(count: usize) -> SharedString {
    if count == 1 {
        t("file_pane.queued.one")
    } else {
        tf("file_pane.queued.other", &[("count", &count.to_string())])
    }
}

/// Show a failed operation in plain words: the humanized title, with the
/// next step as the message when there is one.
fn notify_error(error: &anyhow::Error, window: &mut Window, cx: &mut App) {
    let human = humanize(error);
    let note = match human.hint {
        Some(hint) => Notification::error(hint).title(human.title),
        None => Notification::error(human.title),
    };
    window.push_notification(note, cx);
}

/// User input → OpenDAL dir path: no leading `/`, trailing `/` unless root.
fn normalize_path(input: &str) -> String {
    let path = input.trim().trim_start_matches('/');
    if path.is_empty() {
        String::new()
    } else if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str) -> FsEntry {
        FsEntry {
            name: name.to_string().into(),
            path: name.to_string(),
            is_dir: false,
            is_symlink: false,
            is_bucket: false,
            is_parent: false,
            is_hidden: false,
            size: 0,
            modified: None,
            permissions: None,
            storage_class: None,
            region: None,
        }
    }

    #[test]
    fn mode_strings_match_ls() {
        assert_eq!(mode_string(0o755, false), "-rwxr-xr-x");
        assert_eq!(mode_string(0o644, false), "-rw-r--r--");
        assert_eq!(mode_string(0o700, true), "drwx------");
        assert_eq!(mode_string(0o4755, false), "-rwsr-xr-x");
        assert_eq!(mode_string(0o1777, true), "drwxrwxrwt");
    }

    #[test]
    fn duplicate_names_avoid_collisions() {
        let entries = vec![entry("a.txt"), entry("a copy.txt"), entry("b")];
        assert_eq!(duplicate_name("a.txt", &entries), "a copy 2.txt");
        assert_eq!(duplicate_name("b", &entries), "b copy");
        // A leading dot is a hidden file, not an extension.
        assert_eq!(duplicate_name(".env", &entries), ".env copy");
    }

    #[test]
    fn sort_groups_dirs_first_and_respects_direction() {
        let mut dir = entry("zdir");
        dir.is_dir = true;
        let mut small = entry("small");
        small.size = 1;
        let mut big = entry("big");
        big.size = 100;

        let mut entries = vec![small.clone(), dir.clone(), big.clone()];
        let sort = Sort {
            column: Some(Column::Size),
            ascending: false,
        };
        entries.sort_by(|a, b| sort.compare(a, b));
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_ref()).collect();
        assert_eq!(names, ["zdir", "big", "small"]);
    }

    fn site(scheme: &str, name: &str, options: &[(&str, &str)]) -> SiteProfile {
        SiteProfile {
            id: 7,
            name: name.to_string(),
            scheme: scheme.to_string(),
            options: options
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn path_input_only_browses_its_own_site() {
        let prod = site("sftp", "Prod", &[("endpoint", "ssh://prod.example.com:22")]);
        let here = |path: &str| PathInput::Here(path.to_string());
        assert_eq!(parse_path_input("/var/www", &prod), here("var/www/"));
        assert_eq!(parse_path_input("sftp://Prod/var/", &prod), here("var/"));
        assert_eq!(parse_path_input("sftp://me@prod.example.com/srv", &prod), here("srv/"));
        assert_eq!(
            parse_path_input("sftp://staging.example.com/srv", &prod),
            PathInput::Elsewhere("sftp://staging.example.com".to_string())
        );
        assert_eq!(
            parse_path_input("s3://Prod/x", &prod),
            PathInput::Elsewhere("s3://Prod".to_string())
        );

        let bucket = site("s3", "Assets", &[("bucket", "assets"), ("root", "/public/")]);
        assert_eq!(parse_path_input("s3://assets/public/img", &bucket), here("img/"));
        assert!(matches!(parse_path_input("s3://assets/private", &bucket), PathInput::Elsewhere(_)));
        let account = site("s3", "AWS", &[]);
        assert_eq!(parse_path_input("s3://logs/2026/", &account), here("logs/2026/"));

        let local = site("fs", "Local", &[]);
        assert_eq!(parse_path_input("file:///Users/ada", &local), here("Users/ada/"));
    }

    #[test]
    fn selection_follows_paths_across_rebuilds() {
        let mut parent = entry("..");
        parent.is_parent = true;
        let entries = vec![parent, entry("b"), entry("a"), entry("c")];
        let paths: HashSet<String> = ["a", "c", "..", "gone"].map(String::from).into();
        assert_eq!(select_paths(&entries, &paths), BTreeSet::from([2, 3]));
    }
}
