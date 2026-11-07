//! Transfers pane: a live view of the persistent queue with per-job
//! pause / resume / cancel / retry, rendered as a global bottom pane. It
//! subscribes to the engine's watch channel, so SQLite stays the source of
//! truth and this view is pure presentation.
//!
//! Beyond the live queue it offers a per-job file drill-down (chevron or
//! context menu), a failed-files list with "Retry failed", queue
//! priorities, a global pause, and a History tab reading the
//! `transfer_history` audit trail. Both lists are virtualized.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use gpui::prelude::*;
use gpui::{
    div, list, px, AnyElement, App, Context, EventEmitter, ListAlignment, ListState,
    SharedString, WeakEntity, Window,
};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::checkbox::Checkbox;
use gpui_component::menu::{ContextMenuExt as _, PopupMenuItem};
use gpui_component::notification::Notification;
use gpui_component::progress::Progress;
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Icon, IconName, Sizable as _, WindowExt as _,
};

use crate::app::AppState;
use crate::errors::humanize_text;
use crate::i18n::{t, tf};
use crate::queue::{
    ConflictInfo, ConflictPolicy, HistoryEntry, JobSnapshot, JobState, QueueHandle,
    QueueSnapshot, TaskSnapshot, LOCAL_SITE_ID,
};
use crate::ui::{human_duration, human_size, human_time};

/// Rows fetched per page for the per-job drill-down and the history list.
const PAGE: i64 = 200;

/// ETAs beyond this are noise from a rate that hasn't settled yet.
const MAX_ETA_SECS: u64 = 100 * 3600;

/// "1 file" / "3 files": `{key}.one` or `{key}.other` with `{count}` filled in.
pub fn plural(key: &str, count: u64) -> SharedString {
    let form = if count == 1 { "one" } else { "other" };
    tf(&format!("{key}.{form}"), &[("count", &count.to_string())])
}

/// Seconds until `total - done` bytes are through at `bps`; `None` when the
/// rate is unknown, nothing is left, or the estimate is too far out to mean
/// anything.
pub fn eta_secs(bps: u64, done: u64, total: u64) -> Option<u64> {
    if bps == 0 || total <= done {
        return None;
    }
    Some((total - done).div_ceil(bps)).filter(|secs| *secs <= MAX_ETA_SECS)
}

/// "less than a minute left", "about 3 min left", "about 2 h 5 min left".
pub fn eta_text(secs: u64) -> SharedString {
    if secs < 60 {
        return t("transfers.eta.under_minute");
    }
    let minutes = (secs + 30) / 60;
    if minutes < 60 {
        return tf("transfers.eta.minutes", &[("count", &minutes.to_string())]);
    }
    let (hours, minutes) = ((minutes / 60).to_string(), minutes % 60);
    if minutes == 0 {
        tf("transfers.eta.hours", &[("hours", &hours)])
    } else {
        tf(
            "transfers.eta.hours_minutes",
            &[("hours", &hours), ("minutes", &minutes.to_string())],
        )
    }
}

/// "4.2 MB/s, about 3 min left", or just the speed when the ETA is unknown.
pub fn rate_text(bps: u64, done: u64, total: u64) -> String {
    let speed = format!("{}/s", human_size(bps));
    match eta_secs(bps, done, total) {
        Some(eta) => format!("{speed}, {}", eta_text(eta)),
        None => speed,
    }
}

/// Aggregate over all non-terminal jobs, for the pane footer and the global
/// status bar.
#[derive(Debug, Clone, Copy)]
pub struct TransferStats {
    pub active: usize,
    pub conflicts: u64,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub bps: u64,
}

impl TransferStats {
    pub fn percent(&self) -> f32 {
        if self.bytes_total == 0 {
            return 0.0;
        }
        (self.bytes_done as f32 / self.bytes_total as f32) * 100.0
    }

    /// One line: "2 transfers · 45.1 MB of 1.2 GB · 12.3 MB/s, about 2 min left"
    pub fn summary(&self) -> String {
        let mut parts = vec![
            plural("transfers.count", self.active as u64).to_string(),
            tf(
                "transfers.bytes_of",
                &[
                    ("done", &human_size(self.bytes_done)),
                    ("total", &human_size(self.bytes_total)),
                ],
            )
            .to_string(),
        ];
        if self.bps > 0 {
            parts.push(rate_text(self.bps, self.bytes_done, self.bytes_total));
        }
        if self.conflicts > 0 {
            parts.push(plural("transfers.conflicts", self.conflicts).to_string());
        }
        parts.join(" · ")
    }
}

pub enum TransfersEvent {
    /// A job just completed — panes showing its destination refresh.
    JobFinished(JobSnapshot),
}

/// Things the panel asks the app to do; a separate event type so the app's
/// existing `TransfersEvent` match stays exhaustive.
#[derive(Debug, Clone)]
pub enum TransfersRequest {
    /// A job just parked its first conflict. The app shows the transfers
    /// dock and calls [`TransfersPanel::prompt_conflicts`].
    ConflictParked { job_id: i64 },
    /// Show a finished job's destination folder in a file pane.
    ShowInPane { site_id: i64, dir: String },
}

/// A fetched list and whether it's still loading or failed.
enum Fetched<T> {
    Loading,
    Ready(T),
    Failed(String),
}

/// An unfolded per-file list of one job.
struct Drill {
    /// Only the failed files, opened from "Done, N failed".
    failed_only: bool,
    limit: i64,
    tasks: Fetched<Vec<TaskSnapshot>>,
}

pub struct TransfersPanel {
    snapshot: QueueSnapshot,
    jobs_list: ListState,
    drills: HashMap<i64, Drill>,
    /// Jobs whose failed files show their raw error text.
    details: HashSet<i64>,
    /// Jobs that already asked for a decision on their parked conflicts.
    conflict_prompted: HashSet<i64>,
    /// History tab instead of the live queue.
    show_history: bool,
    history: Fetched<Vec<HistoryEntry>>,
    history_limit: i64,
    history_list: ListState,
}

impl EventEmitter<TransfersEvent> for TransfersPanel {}
impl EventEmitter<TransfersRequest> for TransfersPanel {}

/// Show the next conflict with a per-file or whole-transfer decision.
fn open_resolve_dialog(job_id: i64, window: &mut Window, cx: &mut App) {
    let state = cx.global::<AppState>();
    let pool = state.db.clone();
    let queue = state.queue.clone();
    let fetch = state
        .runtime
        .spawn(async move { crate::queue::store::job_conflicts(&pool, job_id, 100).await });
    window
        .spawn(cx, async move |cx| {
            let conflicts = match fetch.await {
                Ok(Ok(conflicts)) => conflicts,
                Ok(Err(error)) => return notify_failure("transfers.conflict.load_failed", &format!("{error:#}"), cx),
                Err(error) => return notify_failure("transfers.conflict.load_failed", &error.to_string(), cx),
            };
            cx.update(|window, cx| open_resolve_dialog_with(job_id, conflicts, queue, window, cx))
                .ok();
        })
        .detach();
}

fn notify_failure(title_key: &str, raw: &str, cx: &mut gpui::AsyncWindowContext) {
    let human = humanize_text(raw);
    let title = t(title_key);
    cx.update(|window, cx| {
        window.push_notification(Notification::error(human.title).title(title), cx);
    })
    .ok();
}

/// "4.2 MB, modified 2026-11-07 10:12" for one side of a conflict.
fn conflict_side(size: Option<u64>, modified: Option<&str>) -> String {
    let size = size.map(human_size).unwrap_or_else(|| t("transfers.conflict.unknown_size").to_string());
    match parse_rfc3339(modified) {
        Some(time) => tf("transfers.conflict.side", &[("size", &size), ("modified", &human_time(Some(time)))]).to_string(),
        None => size,
    }
}

fn parse_rfc3339(raw: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    raw.and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|time| time.with_timezone(&chrono::Utc))
}

fn open_resolve_dialog_with(
    job_id: i64,
    conflicts: Vec<ConflictInfo>,
    queue: QueueHandle,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(conflict) = conflicts.first().cloned() else {
        window.push_notification(t("transfers.conflict.none_left"), cx);
        return;
    };
    let remaining: Vec<ConflictInfo> = conflicts[1..].to_vec();
    let apply_all = Rc::new(Cell::new(false));
    let show_more = Rc::new(Cell::new(false));

    let decide: Rc<dyn Fn(ConflictPolicy, &mut Window, &mut App)> = {
        let apply_all = apply_all.clone();
        Rc::new(move |policy, window, cx| {
            window.close_dialog(cx);
            if apply_all.get() {
                queue.resolve(job_id, policy);
                return;
            }
            queue.resolve_task(conflict.id, policy);
            if !remaining.is_empty() {
                open_resolve_dialog_with(job_id, remaining.clone(), queue.clone(), window, cx);
            }
        })
    };

    let name = conflict.dst.trim_end_matches('/').rsplit('/').next().unwrap_or(&conflict.dst).to_string();
    let src_time = parse_rfc3339(conflict.src_modified.as_deref());
    let dst_time = parse_rfc3339(conflict.dst_modified.as_deref());
    let newer = match (src_time, dst_time) {
        (Some(src), Some(dst)) if src > dst => Some(true),
        (Some(src), Some(dst)) if src < dst => Some(false),
        _ => None,
    };
    let incoming = conflict_side(conflict.src_size, conflict.src_modified.as_deref());
    let existing = conflict_side(conflict.dst_size, conflict.dst_modified.as_deref());
    let others = conflicts.len() as u64 - 1;

    window.open_dialog(cx, move |dialog, _, cx| {
        let side = |label: SharedString, value: &str, is_newer: bool| {
            h_flex()
                .gap_2()
                .child(div().w(px(72.)).flex_shrink_0().text_color(cx.theme().muted_foreground).child(label))
                .child(div().flex_1().child(value.to_string()))
                .when(is_newer, |this| this.child(div().text_color(cx.theme().muted_foreground).child(t("transfers.conflict.newer"))))
        };
        let choice = |id: &'static str, key: &str, policy: ConflictPolicy| {
            let decide = decide.clone();
            Button::new(id)
                .label(t(key))
                .small()
                .on_click(move |_, window, cx| decide(policy, window, cx))
        };
        let more = show_more.get();
        dialog
            .title(t("transfers.conflict.title"))
            .w(px(480.))
            .child(
                v_flex()
                    .gap_3()
                    .child(tf("transfers.conflict.body", &[("name", &name)]))
                    .child(
                        v_flex()
                            .p_2()
                            .gap_1()
                            .text_sm()
                            .rounded(cx.theme().radius)
                            .bg(cx.theme().muted)
                            .child(side(t("transfers.conflict.incoming"), &incoming, newer == Some(true)))
                            .child(side(t("transfers.conflict.existing"), &existing, newer == Some(false))),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(choice("conflict-replace", "transfers.conflict.replace", ConflictPolicy::Overwrite).primary())
                            .child(choice("conflict-skip", "transfers.conflict.skip", ConflictPolicy::Skip).outline())
                            .child(choice("conflict-keep-both", "transfers.conflict.keep_both", ConflictPolicy::Rename).outline()),
                    )
                    .child({
                        let apply_all = apply_all.clone();
                        let label = if others > 0 {
                            tf("transfers.conflict.apply_all_count", &[("count", &others.to_string())])
                        } else {
                            t("transfers.conflict.apply_all")
                        };
                        Checkbox::new("conflict-apply-all")
                            .label(label)
                            .checked(apply_all.get())
                            .on_click(move |checked, window, _| {
                                apply_all.set(*checked);
                                window.refresh();
                            })
                    })
                    .child({
                        let show_more = show_more.clone();
                        Button::new("conflict-more")
                            .label(t("transfers.conflict.more"))
                            .icon(Icon::new(if more { IconName::ChevronDown } else { IconName::ChevronRight }))
                            .ghost()
                            .xsmall()
                            .on_click(move |_, window, _| {
                                show_more.set(!show_more.get());
                                window.refresh();
                            })
                    })
                    .when(more, |this| {
                        this.child(
                            v_flex()
                                .gap_2()
                                .children([
                                    ("conflict-newer", "transfers.conflict.replace_newer", "transfers.conflict.replace_newer_hint", ConflictPolicy::OverwriteNewer),
                                    ("conflict-resume", "transfers.conflict.resume", "transfers.conflict.resume_hint", ConflictPolicy::Resume),
                                ]
                                .map(|(id, key, hint, policy)| {
                                    h_flex()
                                        .gap_2()
                                        .child(choice(id, key, policy).outline().flex_shrink_0())
                                        .child(div().text_xs().text_color(cx.theme().muted_foreground).child(t(hint)))
                                })),
                        )
                    }),
            )
    });
}

/// Bring a parked conflict to the user's attention: open the dialog, or
/// when another dialog is up, a notification with a Resolve action.
fn prompt_conflict(job: &JobSnapshot, window: &mut Window, cx: &mut App) {
    let job_id = job.id;
    if !window.has_active_dialog(cx) {
        open_resolve_dialog(job_id, window, cx);
        return;
    }
    let note = Notification::warning(tf("transfers.conflict.note", &[("name", file_name(&job.src))]))
        .id1::<TransfersRequest>(job_id as usize)
        .title(t("transfers.conflict.note_title"))
        .action(move |_, _, cx| {
            let note = cx.entity().downgrade();
            Button::new("resolve-conflict")
                .label(t("transfers.conflict.resolve"))
                .primary()
                .small()
                .on_click(move |_, window, cx| {
                    note.update(cx, |note, cx| note.dismiss(window, cx)).ok();
                    open_resolve_dialog(job_id, window, cx);
                })
        });
    window.push_notification(note, cx);
}

/// Last segment of a browse path, without the trailing `/` of a folder.
fn file_name(path: &str) -> &str {
    let path = path.trim_end_matches('/');
    path.rsplit('/').next().unwrap_or(path)
}

/// Where a finished download to the local site landed. Falls back to the
/// destination folder when the item isn't there under its source name
/// (written as "file (1).txt", say).
fn local_target(job: &JobSnapshot) -> PathBuf {
    let root = crate::localfs::root();
    let item = crate::localfs::full_path(&root, &format!("{}{}", job.dst, file_name(&job.src)));
    if item.exists() {
        item
    } else {
        crate::localfs::full_path(&root, &job.dst)
    }
}

/// Show `path` selected in Finder / Explorer; elsewhere open its folder.
fn reveal(path: &Path) {
    #[cfg(target_os = "macos")]
    let result = std::process::Command::new("open").arg("-R").arg(path).spawn();
    #[cfg(target_os = "windows")]
    let result = std::process::Command::new("explorer")
        .arg(format!("/select,{}", path.display()))
        .spawn();
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let result = std::process::Command::new("xdg-open")
        .arg(path.parent().unwrap_or(path))
        .spawn();
    if let Err(error) = result {
        tracing::error!(%error, path = %path.display(), "revealing in the file manager failed");
    }
}

fn reveal_label() -> SharedString {
    t(if cfg!(target_os = "macos") {
        "transfers.reveal_finder"
    } else if cfg!(target_os = "windows") {
        "transfers.reveal_explorer"
    } else {
        "transfers.reveal_folder"
    })
}

/// A finished download sitting on this machine.
fn is_local_download(job: &JobSnapshot) -> bool {
    job.state == JobState::Done && job.dst_site_id == LOCAL_SITE_ID
}

/// SQLite `datetime('now')` (UTC) in local time.
fn history_time(raw: &str) -> String {
    chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
        .map(|time| human_time(Some(time.and_utc())))
        .unwrap_or_else(|_| raw.to_string())
}

impl TransfersPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        let handle = cx.global::<AppState>().queue.clone();
        let mut rx = handle.subscribe();
        // tokio's watch primitives are runtime-agnostic, so awaiting the
        // change signal on gpui's executor is fine.
        cx.spawn(async move |this, cx| {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let snapshot = rx.borrow_and_update().clone();
                if this.update(cx, |panel, cx| panel.apply_snapshot(snapshot, cx)).is_err() {
                    break;
                }
            }
        })
        .detach();

        let snapshot = handle.latest();
        let jobs_list = ListState::new(snapshot.jobs.len(), ListAlignment::Top, px(200.));
        Self {
            snapshot,
            jobs_list,
            drills: HashMap::new(),
            details: HashSet::new(),
            conflict_prompted: HashSet::new(),
            show_history: false,
            history: Fetched::Loading,
            history_limit: PAGE,
            history_list: ListState::new(0, ListAlignment::Top, px(200.)),
        }
    }

    fn apply_snapshot(&mut self, snapshot: QueueSnapshot, cx: &mut Context<Self>) {
        // Jobs that just crossed into Done: emit so open folders showing
        // their destination pick up the new files.
        let finished: Vec<JobSnapshot> = snapshot
            .jobs
            .iter()
            .filter(|job| {
                job.state == JobState::Done
                    && self
                        .snapshot
                        .jobs
                        .iter()
                        .find(|old| old.id == job.id)
                        .is_none_or(|old| old.state != JobState::Done)
            })
            .cloned()
            .collect();
        let mut parked = Vec::new();
        for job in &snapshot.jobs {
            if job.files_conflict == 0 {
                self.conflict_prompted.remove(&job.id);
            } else if self.conflict_prompted.insert(job.id) {
                parked.push(job.id);
            }
        }
        let old_count = self.snapshot.jobs.len();
        self.snapshot = snapshot;
        let alive: HashSet<i64> = self.snapshot.jobs.iter().map(|job| job.id).collect();
        self.drills.retain(|id, _| alive.contains(id));
        self.details.retain(|id| alive.contains(id));
        self.conflict_prompted.retain(|id| alive.contains(id));
        self.sync_list(old_count);

        // Keep unfolded drill-downs and the history tab fresh.
        let drilled: Vec<i64> = self.drills.keys().copied().collect();
        for job_id in drilled {
            self.fetch_tasks(job_id, cx);
        }
        if self.show_history && !finished.is_empty() {
            self.fetch_history(cx);
        }
        for job in finished {
            cx.emit(TransfersEvent::JobFinished(job));
        }
        for job_id in parked {
            cx.emit(TransfersRequest::ConflictParked { job_id });
        }
        cx.notify();
    }

    /// Tell the job list its rows changed; heights vary, so they remeasure.
    fn sync_list(&self, old_count: usize) {
        let count = self.snapshot.jobs.len();
        if count == old_count {
            self.jobs_list.remeasure_items(0..count);
        } else {
            self.jobs_list.splice(0..old_count, count);
        }
    }

    /// Ask for a decision on a job's parked conflicts: opens the resolve
    /// dialog, or a notification with a Resolve action while another dialog
    /// is open. Called by the app on [`TransfersRequest::ConflictParked`].
    pub fn prompt_conflicts(&mut self, job_id: i64, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(job) = self.snapshot.jobs.iter().find(|job| job.id == job_id && job.files_conflict > 0) {
            prompt_conflict(job, window, cx);
        }
    }

    /// Aggregate stats over non-terminal jobs; `None` while the queue is idle.
    pub fn stats(&self) -> Option<TransferStats> {
        let jobs: Vec<&JobSnapshot> = self
            .snapshot
            .jobs
            .iter()
            .filter(|j| !j.state.is_terminal())
            .collect();
        if jobs.is_empty() {
            return None;
        }
        Some(TransferStats {
            active: jobs.len(),
            conflicts: jobs.iter().map(|j| j.files_conflict).sum(),
            bytes_done: jobs.iter().map(|j| j.bytes_done).sum(),
            bytes_total: jobs.iter().map(|j| j.bytes_total).sum(),
            bps: self.snapshot.total_bps,
        })
    }

    /// Fold or unfold a job's file list; `failed_only` opens just the
    /// failed files (switching an open list over instead of closing it).
    fn toggle_drill(&mut self, job_id: i64, failed_only: bool, cx: &mut Context<Self>) {
        match self.drills.get_mut(&job_id) {
            Some(drill) if drill.failed_only == failed_only => {
                self.drills.remove(&job_id);
            }
            Some(drill) => drill.failed_only = failed_only,
            None => {
                self.drills.insert(
                    job_id,
                    Drill {
                        failed_only,
                        limit: PAGE,
                        tasks: Fetched::Loading,
                    },
                );
                self.fetch_tasks(job_id, cx);
            }
        }
        self.sync_list(self.snapshot.jobs.len());
        cx.notify();
    }

    fn show_more_tasks(&mut self, job_id: i64, cx: &mut Context<Self>) {
        if let Some(drill) = self.drills.get_mut(&job_id) {
            drill.limit += PAGE;
            self.fetch_tasks(job_id, cx);
        }
    }

    fn toggle_details(&mut self, job_id: i64, cx: &mut Context<Self>) {
        if !self.details.remove(&job_id) {
            self.details.insert(job_id);
        }
        self.sync_list(self.snapshot.jobs.len());
        cx.notify();
    }

    fn fetch_tasks(&mut self, job_id: i64, cx: &mut Context<Self>) {
        let Some(limit) = self.drills.get(&job_id).map(|drill| drill.limit) else {
            return;
        };
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let fetch = state
            .runtime
            .spawn(async move { crate::queue::store::job_tasks(&pool, job_id, limit).await });
        cx.spawn(async move |this, cx| {
            let tasks = match fetch.await {
                Ok(Ok(tasks)) => Fetched::Ready(tasks),
                Ok(Err(error)) => Fetched::Failed(humanize_text(&format!("{error:#}")).title),
                Err(error) => Fetched::Failed(error.to_string()),
            };
            this.update(cx, |this, cx| {
                let Some(drill) = this.drills.get_mut(&job_id) else { return };
                // A failed refresh keeps the rows already on screen.
                if matches!(tasks, Fetched::Ready(_)) || !matches!(drill.tasks, Fetched::Ready(_)) {
                    drill.tasks = tasks;
                    this.sync_list(this.snapshot.jobs.len());
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
    }

    fn fetch_history(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.history, Fetched::Ready(_)) {
            self.history = Fetched::Loading;
        }
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let limit = self.history_limit;
        let fetch = state
            .runtime
            .spawn(async move { crate::queue::store::history(&pool, limit).await });
        cx.spawn(async move |this, cx| {
            let history = match fetch.await {
                Ok(Ok(history)) => Fetched::Ready(history),
                Ok(Err(error)) => Fetched::Failed(humanize_text(&format!("{error:#}")).title),
                Err(error) => Fetched::Failed(error.to_string()),
            };
            this.update(cx, |this, cx| {
                let old = this.history_rows();
                this.history = history;
                this.history_list.splice(0..old, this.history_rows());
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// History list rows: the entries plus a "Load more" row when the last
    /// page came back full.
    fn history_rows(&self) -> usize {
        match &self.history {
            Fetched::Ready(entries) => entries.len() + usize::from(entries.len() as i64 >= self.history_limit),
            _ => 0,
        }
    }

    fn toggle_history(&mut self, cx: &mut Context<Self>) {
        self.show_history = !self.show_history;
        if self.show_history {
            self.fetch_history(cx);
        }
        cx.notify();
    }

    /// Status word with its tone: "Done, 2 failed" in warning, "Failed" in
    /// danger; a healthy row stays quiet.
    fn status(&self, job: &JobSnapshot, cx: &App) -> (SharedString, gpui::Hsla) {
        let theme = cx.theme();
        if job.files_conflict > 0 {
            return (t("transfers.state.waiting_decision"), theme.warning);
        }
        match job.state {
            JobState::Active => (t("transfers.state.active"), theme.muted_foreground),
            JobState::Queued => (t("transfers.state.queued"), theme.muted_foreground),
            JobState::Paused => (t("transfers.state.paused"), theme.muted_foreground),
            JobState::Cancelled => (t("transfers.state.cancelled"), theme.muted_foreground),
            JobState::Failed => (t("transfers.state.failed"), theme.danger),
            JobState::Done if job.files_failed > 0 => (
                tf("transfers.state.done_failed", &[("count", &job.files_failed.to_string())]),
                theme.warning,
            ),
            JobState::Done => (t("transfers.state.done"), theme.success),
        }
    }

    /// The muted line under a job's title: kind, sites, counts and rate.
    fn detail_line(job: &JobSnapshot) -> String {
        let kind = match (job.direction(), job.move_source) {
            ("upload", false) => "transfers.kind.upload",
            ("upload", true) => "transfers.kind.move_upload",
            ("download", false) => "transfers.kind.download",
            ("download", true) => "transfers.kind.move_download",
            (_, false) => "transfers.kind.copy",
            (_, true) => "transfers.kind.move",
        };
        let mut parts = vec![
            t(kind).to_string(),
            format!("{} → {}", job.src_site, job.dst_site),
        ];
        if job.state.is_terminal() {
            parts.push(plural("transfers.files", job.files_done).to_string());
            parts.push(human_size(job.bytes_done));
        } else {
            parts.push(
                tf(
                    "transfers.files_of",
                    &[("done", &job.files_done.to_string()), ("total", &plural("transfers.files", job.files_total))],
                )
                .to_string(),
            );
            parts.push(
                tf(
                    "transfers.bytes_of",
                    &[("done", &human_size(job.bytes_done)), ("total", &human_size(job.bytes_total))],
                )
                .to_string(),
            );
        }
        if job.files_skipped > 0 {
            parts.push(tf("transfers.skipped", &[("count", &job.files_skipped.to_string())]).to_string());
        }
        if job.state == JobState::Active && job.bps > 0 {
            parts.push(rate_text(job.bps, job.bytes_done, job.bytes_total));
        }
        if job.priority != 0 {
            parts.push(tf("transfers.priority", &[("priority", &job.priority.to_string())]).to_string());
        }
        parts.join(" · ")
    }

    fn render_job_at(&mut self, ix: usize, cx: &mut Context<Self>) -> AnyElement {
        match self.snapshot.jobs.get(ix).cloned() {
            Some(job) => self.render_job(&job, cx).into_any_element(),
            None => div().into_any_element(),
        }
    }

    fn render_job(&self, job: &JobSnapshot, cx: &mut Context<Self>) -> impl IntoElement {
        let queue = cx.global::<AppState>().queue.clone();
        let id = job.id;
        let key = id as usize;
        let icon = match job.direction() {
            "upload" => IconName::ArrowUp,
            "download" => IconName::ArrowDown,
            _ => IconName::ArrowRight,
        };
        let percent = if job.bytes_total > 0 {
            (job.bytes_done as f32 / job.bytes_total as f32) * 100.0
        } else {
            0.0
        };
        let (status, tone) = self.status(job, cx);
        let drill = self.drills.get(&id);
        let is_expanded = drill.is_some();
        let failed_open = drill.is_some_and(|drill| drill.failed_only);
        let local_download = is_local_download(job);

        let controls = h_flex()
            .gap_1()
            .when(job.files_conflict > 0, |this| {
                this.child(
                    Button::new(("resolve", key))
                        .label(plural("transfers.resolve_conflicts", job.files_conflict))
                        .warning()
                        .xsmall()
                        .on_click(move |_, window, cx| open_resolve_dialog(id, window, cx)),
                )
            })
            .when(local_download, |this| {
                let open = local_target(job);
                let shown = open.clone();
                this.child(
                    Button::new(("open", key))
                        .label(t("transfers.open"))
                        .ghost()
                        .xsmall()
                        .on_click(move |_, _, _| crate::ui::open_path_or_url(&open.to_string_lossy())),
                )
                .child(
                    Button::new(("reveal", key))
                        .icon(Icon::new(IconName::FolderOpen))
                        .ghost()
                        .xsmall()
                        .tooltip(reveal_label())
                        .on_click(move |_, _, _| reveal(&shown)),
                )
            })
            .when(job.state == JobState::Done && job.files_failed > 0, |this| {
                let queue = queue.clone();
                this.child(
                    Button::new(("retry-failed", key))
                        .label(t("transfers.retry_failed"))
                        .ghost()
                        .xsmall()
                        .on_click(move |_, _, _| queue.retry_failed(id)),
                )
            })
            .when(matches!(job.state, JobState::Failed | JobState::Cancelled), |this| {
                let queue = queue.clone();
                this.child(
                    Button::new(("retry", key))
                        .label(t("transfers.retry"))
                        .ghost()
                        .xsmall()
                        .on_click(move |_, _, _| queue.retry(id)),
                )
            })
            .when(matches!(job.state, JobState::Active | JobState::Queued), |this| {
                let queue = queue.clone();
                this.child(
                    Button::new(("pause", key))
                        .label(t("transfers.pause"))
                        .ghost()
                        .xsmall()
                        .on_click(move |_, _, _| queue.pause(id)),
                )
            })
            .when(job.state == JobState::Paused, |this| {
                let queue = queue.clone();
                this.child(
                    Button::new(("resume", key))
                        .label(t("transfers.resume"))
                        .ghost()
                        .xsmall()
                        .on_click(move |_, _, _| queue.resume(id)),
                )
            })
            .child({
                let queue = queue.clone();
                let terminal = job.state.is_terminal();
                Button::new(("dismiss", key))
                    .icon(Icon::new(if terminal { IconName::Close } else { IconName::CircleX }))
                    .ghost()
                    .xsmall()
                    .tooltip(t(if terminal { "transfers.remove" } else { "transfers.cancel" }))
                    .on_click(move |_, _, _| {
                        if terminal {
                            queue.remove(id)
                        } else {
                            queue.cancel(id)
                        }
                    })
            });

        let weak = cx.entity().downgrade();
        let menu_job = job.clone();
        let has_problem = job.error.is_some() || job.files_failed > 0;

        v_flex()
            .id(("job", key))
            .gap_1()
            .px_3()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .context_menu(move |menu, _, cx| build_job_menu(menu, &menu_job, is_expanded, weak.clone(), cx))
            .child(
                h_flex()
                    .gap_2()
                    .justify_between()
                    .child(
                        h_flex()
                            .gap_1()
                            .min_w_0()
                            .overflow_hidden()
                            .child(
                                Button::new(("expand", key))
                                    .icon(Icon::new(if is_expanded { IconName::ChevronDown } else { IconName::ChevronRight }))
                                    .ghost()
                                    .xsmall()
                                    .tooltip(t(if is_expanded { "transfers.hide_files" } else { "transfers.show_files" }))
                                    .on_click(cx.listener(move |this, _, _, cx| this.toggle_drill(id, false, cx))),
                            )
                            .child(Icon::new(icon).small().text_color(cx.theme().muted_foreground))
                            .child(div().truncate().text_sm().child(format!("/{} → /{}", job.src, job.dst))),
                    )
                    .child(controls),
            )
            .when(!job.state.is_terminal(), |this| {
                this.child(Progress::new(("job-progress", key)).value(percent))
            })
            .child(
                h_flex()
                    .gap_2()
                    .text_xs()
                    .child(div().flex_shrink_0().text_color(tone).child(status))
                    .child(div().min_w_0().truncate().text_color(cx.theme().muted_foreground).child(Self::detail_line(job))),
            )
            .when(has_problem, |this| {
                this.child(
                    h_flex()
                        .gap_2()
                        .text_xs()
                        .when_some(job.error.clone(), |this, error| {
                            this.child(div().min_w_0().text_color(tone).child(error))
                        })
                        .when(job.files_failed > 0, |this| {
                            this.child(
                                Button::new(("failed-files", key))
                                    .label(t(if failed_open { "transfers.hide_failed" } else { "transfers.show_failed" }))
                                    .ghost()
                                    .xsmall()
                                    .on_click(cx.listener(move |this, _, _, cx| this.toggle_drill(id, true, cx))),
                            )
                        }),
                )
            })
            .when_some(drill, |this, drill| this.child(self.render_tasks(job, drill, cx)))
    }

    /// The unfolded per-file list of one job: loading, failed and empty
    /// states, the rows, and a "Show more" row when there are more.
    fn render_tasks(&self, job: &JobSnapshot, drill: &Drill, cx: &mut Context<Self>) -> impl IntoElement {
        let id = job.id;
        let key = id as usize;
        let details = self.details.contains(&id);
        let muted = cx.theme().muted_foreground;
        let body = match &drill.tasks {
            Fetched::Loading => div().text_color(muted).child(t("transfers.files_loading")).into_any_element(),
            Fetched::Failed(reason) => h_flex()
                .gap_2()
                .child(div().text_color(cx.theme().danger).child(tf("transfers.files_failed_load", &[("reason", reason)])))
                .child(
                    Button::new(("tasks-retry", key))
                        .label(t("transfers.retry"))
                        .ghost()
                        .xsmall()
                        .on_click(cx.listener(move |this, _, _, cx| this.fetch_tasks(id, cx))),
                )
                .into_any_element(),
            Fetched::Ready(tasks) => {
                let rows: Vec<&TaskSnapshot> = tasks
                    .iter()
                    .filter(|task| !drill.failed_only || task.state == "failed")
                    .collect();
                let expected = if drill.failed_only { job.files_failed } else { job.files_total };
                let fetched_all = (tasks.len() as i64) < drill.limit;
                let remaining = if fetched_all { 0 } else { expected.saturating_sub(rows.len() as u64) };
                let any_errors = rows.iter().any(|task| task.error.is_some());
                v_flex()
                    .gap_0p5()
                    .when(rows.is_empty(), |this| this.child(div().text_color(muted).child(t("transfers.files_empty"))))
                    .children(rows.into_iter().map(|task| self.render_task(task, details, cx)))
                    .when(remaining > 0, |this| {
                        this.child(
                            h_flex()
                                .gap_2()
                                .child(div().text_color(muted).child(plural("transfers.more_files", remaining)))
                                .child(
                                    Button::new(("tasks-more", key))
                                        .label(t("transfers.show_more"))
                                        .ghost()
                                        .xsmall()
                                        .on_click(cx.listener(move |this, _, _, cx| this.show_more_tasks(id, cx))),
                                ),
                        )
                    })
                    .when(any_errors, |this| {
                        this.child(
                            div().child(
                                Button::new(("details", key))
                                    .label(t(if details { "transfers.hide_details" } else { "transfers.show_details" }))
                                    .ghost()
                                    .xsmall()
                                    .on_click(cx.listener(move |this, _, _, cx| this.toggle_details(id, cx))),
                            ),
                        )
                    })
                    .into_any_element()
            }
        };
        v_flex().pl_6().pt_1().text_xs().child(body)
    }

    fn render_task(&self, task: &TaskSnapshot, details: bool, cx: &App) -> impl IntoElement {
        let theme = cx.theme();
        let (state, tone) = match task.state.as_str() {
            "failed" => (t("transfers.task.failed"), theme.danger),
            "conflict" => (t("transfers.task.conflict"), theme.warning),
            "done" => (t("transfers.task.done"), theme.success),
            "active" => (t("transfers.task.active"), theme.muted_foreground),
            "paused" => (t("transfers.task.paused"), theme.muted_foreground),
            "skipped" => (t("transfers.task.skipped"), theme.muted_foreground),
            "cancelled" => (t("transfers.task.cancelled"), theme.muted_foreground),
            _ => (t("transfers.task.queued"), theme.muted_foreground),
        };
        let size = match task.total_bytes {
            Some(total) if task.state == "active" => tf(
                "transfers.bytes_of",
                &[("done", &human_size(task.bytes_done)), ("total", &human_size(total))],
            )
            .to_string(),
            Some(total) => human_size(total),
            None => String::new(),
        };
        let human = task.error.as_deref().map(humanize_text);
        v_flex()
            .child(
                h_flex()
                    .gap_2()
                    .child(div().w(px(88.)).flex_shrink_0().text_color(tone).child(state))
                    .child(div().flex_1().min_w_0().truncate().child(file_name(&task.src).to_string()))
                    .child(div().text_color(theme.muted_foreground).whitespace_nowrap().child(size))
                    .when_some(human.as_ref().map(|human| human.title.clone()), |this, reason| {
                        this.child(div().max_w(px(320.)).truncate().text_color(tone).child(reason))
                    }),
            )
            .when_some(human.filter(|_| details), |this, human| {
                this.child(
                    div()
                        .pl(px(96.))
                        .text_color(theme.muted_foreground)
                        .font_family("monospace")
                        .child(human.details),
                )
            })
    }

    fn render_history_row(&mut self, ix: usize, cx: &mut Context<Self>) -> AnyElement {
        let Fetched::Ready(entries) = &self.history else {
            return div().into_any_element();
        };
        let Some(entry) = entries.get(ix) else {
            return div()
                .px_3()
                .py_2()
                .child(
                    Button::new("history-more")
                        .label(t("transfers.load_more"))
                        .ghost()
                        .xsmall()
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.history_limit += PAGE;
                            this.fetch_history(cx);
                        })),
                )
                .into_any_element();
        };
        let theme = cx.theme();
        let (outcome, tone) = match entry.outcome.as_str() {
            "done" if entry.files_failed > 0 => (
                tf("transfers.state.done_failed", &[("count", &entry.files_failed.to_string())]),
                theme.warning,
            ),
            "done" => (t("transfers.state.done"), theme.success),
            "failed" => (t("transfers.state.failed"), theme.danger),
            _ => (t("transfers.state.cancelled"), theme.muted_foreground),
        };
        let mut parts = vec![
            history_time(&entry.finished_at),
            format!("{} → {}", entry.src_site, entry.dst_site),
            plural("transfers.files", entry.files.max(0) as u64).to_string(),
            human_size(entry.bytes),
        ];
        if let Some(ms) = entry.duration_ms {
            parts.push(human_duration((ms / 1000).max(0) as u64));
        }
        v_flex()
            .gap_0p5()
            .px_3()
            .py_2()
            .border_b_1()
            .border_color(theme.border)
            .child(
                h_flex()
                    .gap_2()
                    .text_sm()
                    .child(div().flex_shrink_0().text_color(tone).child(outcome))
                    .child(div().min_w_0().truncate().child(format!("/{} → /{}", entry.src, entry.dst))),
            )
            .child(div().text_xs().text_color(theme.muted_foreground).child(parts.join(" · ")))
            .when_some(entry.error.clone(), |this, error| {
                this.child(div().text_xs().text_color(tone).child(error))
            })
            .into_any_element()
    }

    /// A centered one-line state (loading, empty, failed) for a whole tab.
    fn render_placeholder(&self, text: SharedString, retry: bool, cx: &mut Context<Self>) -> AnyElement {
        v_flex()
            .flex_1()
            .py_8()
            .gap_2()
            .items_center()
            .text_sm()
            .text_color(cx.theme().muted_foreground)
            .child(text)
            .when(retry, |this| {
                this.child(
                    Button::new("history-retry")
                        .label(t("transfers.retry"))
                        .ghost()
                        .xsmall()
                        .on_click(cx.listener(|this, _, _, cx| this.fetch_history(cx))),
                )
            })
            .into_any_element()
    }

    fn render_history(&self, cx: &mut Context<Self>) -> AnyElement {
        match &self.history {
            Fetched::Loading => self.render_placeholder(t("transfers.history_loading"), false, cx),
            Fetched::Failed(reason) => self.render_placeholder(
                tf("transfers.history_failed", &[("reason", reason)]),
                true,
                cx,
            ),
            Fetched::Ready(entries) if entries.is_empty() => {
                self.render_placeholder(t("transfers.history_empty"), false, cx)
            }
            Fetched::Ready(_) => list(
                self.history_list.clone(),
                cx.processor(|this, ix: usize, _, cx| this.render_history_row(ix, cx)),
            )
            .flex_1()
            .size_full()
            .into_any_element(),
        }
    }
}

/// Right-click menu on one job row, most frequent actions first.
fn build_job_menu(
    menu: gpui_component::menu::PopupMenu,
    job: &JobSnapshot,
    is_expanded: bool,
    panel: WeakEntity<TransfersPanel>,
    cx: &App,
) -> gpui_component::menu::PopupMenu {
    let queue = cx.global::<AppState>().queue.clone();
    let id = job.id;
    let mut menu = menu;

    if job.files_conflict > 0 {
        menu = menu.item(
            PopupMenuItem::new(plural("transfers.resolve_conflicts", job.files_conflict))
                .on_click(move |_, window, cx| open_resolve_dialog(id, window, cx)),
        );
    }
    if is_local_download(job) {
        let open = local_target(job);
        let shown = open.clone();
        menu = menu
            .item(
                PopupMenuItem::new(t("transfers.open"))
                    .on_click(move |_, _, _| crate::ui::open_path_or_url(&open.to_string_lossy())),
            )
            .item(PopupMenuItem::new(reveal_label()).on_click(move |_, _, _| reveal(&shown)));
    }
    if job.state == JobState::Done {
        let panel = panel.clone();
        let request = TransfersRequest::ShowInPane {
            site_id: job.dst_site_id,
            dir: job.dst.clone(),
        };
        menu = menu.item(PopupMenuItem::new(t("transfers.show_in_pane")).on_click(move |_, _, cx| {
            panel.update(cx, |_, cx| cx.emit(request.clone())).ok();
        }));
    }
    if job.state == JobState::Done && job.files_failed > 0 {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new(t("transfers.retry_failed")).on_click(move |_, _, _| queue.retry_failed(id)));
    }
    if matches!(job.state, JobState::Failed | JobState::Cancelled) {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new(t("transfers.retry")).on_click(move |_, _, _| queue.retry(id)));
    }
    if matches!(job.state, JobState::Active | JobState::Queued) {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new(t("transfers.pause")).on_click(move |_, _, _| queue.pause(id)));
    }
    if job.state == JobState::Paused {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new(t("transfers.resume")).on_click(move |_, _, _| queue.resume(id)));
    }
    if !job.state.is_terminal() {
        let top = queue.latest().jobs.iter().map(|job| job.priority).max().unwrap_or(0);
        let first = queue.clone();
        menu = menu.item(
            PopupMenuItem::new(t("transfers.run_first"))
                .disabled(job.priority > top || (job.priority == top && top != 0))
                .on_click(move |_, _, _| first.set_priority(id, top + 1)),
        );
        if job.priority != 0 {
            let queue = queue.clone();
            menu = menu.item(
                PopupMenuItem::new(t("transfers.reset_priority"))
                    .on_click(move |_, _, _| queue.set_priority(id, 0)),
            );
        }
    }

    menu = menu.separator();
    {
        let panel = panel.clone();
        let label = t(if is_expanded { "transfers.hide_files" } else { "transfers.show_files" });
        menu = menu.item(PopupMenuItem::new(label).on_click(move |_, _, cx| {
            panel.update(cx, |this, cx| this.toggle_drill(id, false, cx)).ok();
        }));
    }
    {
        let path = format!("/{}", job.dst);
        menu = menu.item(PopupMenuItem::new(t("transfers.copy_destination")).on_click(move |_, _, cx| {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(path.clone()));
        }));
    }

    menu = menu.separator();
    let queue = queue.clone();
    if job.state.is_terminal() {
        menu.item(PopupMenuItem::new(t("transfers.remove")).on_click(move |_, _, _| queue.remove(id)))
    } else {
        menu.item(PopupMenuItem::new(t("transfers.cancel")).on_click(move |_, _, _| queue.cancel(id)))
    }
}

impl Render for TransfersPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let jobs = &self.snapshot.jobs;
        let has_finished = jobs.iter().any(|j| j.state.is_terminal());
        let any_pausable = jobs.iter().any(|j| matches!(j.state, JobState::Active | JobState::Queued));
        let any_paused = jobs.iter().any(|j| j.state == JobState::Paused);
        let no_jobs = jobs.is_empty();
        let queue = cx.global::<AppState>().queue.clone();
        let stats = self.stats();
        let live = !self.show_history;

        v_flex()
            .size_full()
            .overflow_hidden()
            .bg(cx.theme().sidebar)
            .child(
                h_flex()
                    .px_3()
                    .py_1()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(div().flex_1())
                    .when(live && any_pausable, |this| {
                        let queue = queue.clone();
                        this.child(
                            Button::new("pause-all")
                                .label(t("transfers.pause_all"))
                                .ghost()
                                .xsmall()
                                .on_click(move |_, _, _| queue.pause_all()),
                        )
                    })
                    .when(live && any_paused, |this| {
                        let queue = queue.clone();
                        this.child(
                            Button::new("resume-all")
                                .label(t("transfers.resume_all"))
                                .ghost()
                                .xsmall()
                                .on_click(move |_, _, _| queue.resume_all()),
                        )
                    })
                    .when(live && has_finished, |this| {
                        let queue = queue.clone();
                        this.child(
                            Button::new("clear-finished")
                                .label(t("transfers.clear_finished"))
                                .ghost()
                                .xsmall()
                                .on_click(move |_, _, _| queue.clear_finished()),
                        )
                    })
                    .child(
                        Button::new("toggle-history")
                            .label(t(if self.show_history { "transfers.queue" } else { "transfers.history" }))
                            .ghost()
                            .xsmall()
                            .on_click(cx.listener(|this, _, _, cx| this.toggle_history(cx))),
                    ),
            )
            .child(if self.show_history {
                self.render_history(cx)
            } else if no_jobs {
                self.render_placeholder(t("transfers.empty"), false, cx)
            } else {
                list(
                    self.jobs_list.clone(),
                    cx.processor(|this, ix: usize, _, cx| this.render_job_at(ix, cx)),
                )
                .flex_1()
                .size_full()
                .into_any_element()
            })
            .when_some(stats, |this, stats| {
                this.child(
                    h_flex()
                        .px_3()
                        .py_1()
                        .gap_3()
                        .border_t_1()
                        .border_color(cx.theme().border)
                        .child(div().w(px(160.)).child(Progress::new("queue-progress").value(stats.percent())))
                        .child(div().text_xs().text_color(cx.theme().muted_foreground).child(stats.summary())),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eta_is_hidden_when_unknown_or_absurd() {
        assert_eq!(eta_secs(0, 0, 100), None);
        assert_eq!(eta_secs(10, 100, 100), None);
        assert_eq!(eta_secs(1, 0, 1 << 40), None);
        assert_eq!(eta_secs(10, 0, 95), Some(10));
    }

    #[test]
    fn eta_and_plurals_read_plainly() {
        crate::i18n::init("en");
        assert_eq!(eta_text(20), "less than a minute left");
        assert_eq!(eta_text(170), "about 3 min left");
        assert_eq!(eta_text(7200), "about 2 h left");
        assert_eq!(eta_text(7500), "about 2 h 5 min left");
        assert_eq!(plural("transfers.files", 1), "1 file");
        assert_eq!(plural("transfers.files", 3), "3 files");
        let mb = 1024 * 1024;
        assert_eq!(rate_text(4 * mb, 0, 4 * mb * 180), "4.0 MB/s, about 3 min left");
        assert_eq!(rate_text(4 * mb, 0, 0), "4.0 MB/s");
    }
}
