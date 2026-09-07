//! Transfers pane: a live view of the persistent queue with per-job
//! pause / resume / cancel / retry, rendered as a global bottom pane. It
//! subscribes to the engine's watch channel, so SQLite stays the source of
//! truth and this view is pure presentation.
//!
//! Beyond the live queue it offers a per-job file drill-down (chevron or
//! context menu), queue priorities, a global pause, and a History tab
//! reading the `transfer_history` audit trail.

use std::collections::{HashMap, HashSet};

use gpui::prelude::*;
use gpui::{div, px, App, Context, EventEmitter, WeakEntity, Window};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::menu::{ContextMenuExt as _, PopupMenuItem};
use gpui_component::progress::Progress;
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Icon, IconName, Sizable as _, WindowExt as _,
};

use crate::app::AppState;
use crate::queue::{
    ConflictInfo, ConflictPolicy, HistoryEntry, JobSnapshot, JobState, QueueSnapshot,
    TaskSnapshot,
};
use crate::ui::{human_duration, human_size, human_time};

/// Cap on rows fetched for the per-job drill-down and the history list.
const TASK_LIMIT: i64 = 200;
const HISTORY_LIMIT: i64 = 200;

/// Aggregate over all non-terminal jobs, for the pane footer and the global
/// status bar.
#[derive(Debug, Clone, Copy)]
pub struct TransferStats {
    pub active: usize,
    pub conflicts: u64,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub bps: u64,
    /// Seconds until the remaining bytes are through at the current rate.
    pub eta_secs: Option<u64>,
}

impl TransferStats {
    pub fn percent(&self) -> f32 {
        if self.bytes_total == 0 {
            return 0.0;
        }
        (self.bytes_done as f32 / self.bytes_total as f32) * 100.0
    }

    /// One line: "2 transfers · 45.1 MB / 1.2 GB · 12.3 MB/s · ETA 1m 20s"
    pub fn summary(&self) -> String {
        let mut text = format!(
            "{} transfer{} · {} / {}",
            self.active,
            if self.active == 1 { "" } else { "s" },
            human_size(self.bytes_done),
            human_size(self.bytes_total),
        );
        if self.bps > 0 {
            text.push_str(&format!(" · {}/s", human_size(self.bps)));
        }
        if let Some(eta) = self.eta_secs {
            text.push_str(&format!(" · ETA {}", human_duration(eta)));
        }
        if self.conflicts > 0 {
            text.push_str(&format!(" · {} conflict(s)", self.conflicts));
        }
        text
    }
}

pub enum TransfersEvent {
    /// A job just completed — panes showing its destination refresh.
    JobFinished(JobSnapshot),
}

pub struct TransfersPanel {
    snapshot: QueueSnapshot,
    /// Jobs whose per-file task list is unfolded.
    expanded: HashSet<i64>,
    /// Fetched drill-down rows per expanded job.
    tasks: HashMap<i64, Vec<TaskSnapshot>>,
    /// History tab instead of the live queue.
    show_history: bool,
    history: Vec<HistoryEntry>,
}

impl EventEmitter<TransfersEvent> for TransfersPanel {}

/// Show the next conflict with an explicit per-file or whole-transfer decision.
fn open_resolve_dialog(job_id: i64, window: &mut Window, cx: &mut Context<TransfersPanel>) {
    let state = cx.global::<AppState>();
    let pool = state.db.clone();
    let queue = state.queue.clone();
    let fetch = state
        .runtime
        .spawn(async move { crate::queue::store::job_conflicts(&pool, job_id, 100).await });
    cx.spawn_in(window, async move |_, cx| {
        let conflicts = match fetch.await {
            Ok(Ok(conflicts)) => conflicts,
            error => {
                cx.update(|window, cx| {
                    window.push_notification(format!("Could not load conflicts: {error:?}"), cx);
                })
                .ok();
                return;
            }
        };
        cx.update(|window, cx| {
            open_resolve_dialog_with(job_id, conflicts, queue, window, cx);
        })
        .ok();
    })
    .detach();
}

fn open_resolve_dialog_with(
    job_id: i64,
    conflicts: Vec<ConflictInfo>,
    queue: crate::queue::QueueHandle,
    window: &mut Window,
    cx: &mut App,
) {
    if conflicts.is_empty() {
        window.push_notification("No conflicts waiting in this transfer.", cx);
        return;
    }
    let task_id = conflicts[0].id;
    const CHOICES: &[(&str, &str, ConflictPolicy)] = &[
        (
            "Overwrite",
            "Replace the existing files with the source files.",
            ConflictPolicy::Overwrite,
        ),
        (
            "Overwrite if newer",
            "Replace only when the source is newer. Missing dates require a decision.",
            ConflictPolicy::OverwriteNewer,
        ),
        (
            "Skip",
            "Leave the existing files as they are.",
            ConflictPolicy::Skip,
        ),
        (
            "Resume",
            "Use only for a known partial copy. Appends when supported; otherwise replaces. Equal or larger files are skipped.",
            ConflictPolicy::Resume,
        ),
        (
            "Keep both",
            "Write the new files under a unique name, e.g. \"file (1).txt\".",
            ConflictPolicy::Rename,
        ),
    ];
    window.open_dialog(cx, move |dialog, _, cx| {
        let queue = queue.clone();
        let conflict = &conflicts[0];
        let details = |label: &str, size: Option<u64>, modified: Option<&str>| {
            let size = size.map(human_size).unwrap_or_else(|| "Unknown size".into());
            let modified = modified
                .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
                .map(|time| human_time(Some(time.with_timezone(&chrono::Utc))))
                .unwrap_or_else(|| "Unknown date".into());
            format!("{label}: {size} · {modified}")
        };
        let comparison = v_flex().gap_1().text_xs()
            .child(div().child(conflict.dst.clone()))
            .child(details("Incoming", conflict.src_size, conflict.src_modified.as_deref()))
            .child(details("Existing", conflict.dst_size, conflict.dst_modified.as_deref()));
        dialog
            .title("File already exists")
            .w(px(580.))
            .child(
                v_flex()
                    .gap_2()
                    .child("Choose for this file, or apply a rule to every conflict in this transfer, including future conflicts.")
                    .when(!conflicts.is_empty(), |this| {
                        this.child(
                            v_flex()
                                .p_2()
                                .gap_1()
                                .rounded(cx.theme().radius)
                                .bg(cx.theme().muted)
                                .child(comparison),
                        )
                    })
                    .children(CHOICES.iter().map(|(label, explain, policy)| {
                        let queue = queue.clone();
                        let policy = *policy;
                        let remaining = conflicts[1..].to_vec();
                        v_flex()
                            .gap_0p5()
                            .child({
                                let all_queue = queue.clone();
                                h_flex().gap_2()
                                    .child(Button::new(*label)
                                        .label(format!("{label}: this file"))
                                        .outline().small()
                                        .on_click(move |_, window, cx| {
                                            queue.resolve_task(task_id, policy);
                                            window.close_dialog(cx);
                                            if !remaining.is_empty() {
                                                open_resolve_dialog_with(job_id, remaining.clone(), queue.clone(), window, cx);
                                            }
                                        }))
                                    .child(Button::new(gpui::SharedString::from(format!("all-{label}")))
                                        .label("Entire transfer")
                                        .ghost().small()
                                        .on_click(move |_, window, cx| {
                                            all_queue.resolve(job_id, policy);
                                            window.close_dialog(cx);
                                        }))
                            })
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(*explain),
                            )
                    })),
            )
    });
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
                let alive = this.update(cx, |panel: &mut TransfersPanel, cx| {
                    // Jobs that just crossed into Done: emit so open folders
                    // showing their destination pick up the new files.
                    let finished: Vec<JobSnapshot> = snapshot
                        .jobs
                        .iter()
                        .filter(|job| {
                            job.state == JobState::Done
                                && panel
                                    .snapshot
                                    .jobs
                                    .iter()
                                    .find(|old| old.id == job.id)
                                    .is_none_or(|old| old.state != JobState::Done)
                        })
                        .cloned()
                        .collect();
                    panel.snapshot = snapshot;
                    // Keep unfolded drill-downs and the history tab fresh.
                    let expanded: Vec<i64> = panel.expanded.iter().copied().collect();
                    for job_id in expanded {
                        panel.fetch_tasks(job_id, cx);
                    }
                    if panel.show_history && !finished.is_empty() {
                        panel.fetch_history(cx);
                    }
                    for job in finished {
                        cx.emit(TransfersEvent::JobFinished(job));
                    }
                    cx.notify();
                });
                if alive.is_err() {
                    break;
                }
            }
        })
        .detach();

        Self {
            snapshot: handle.latest(),
            expanded: HashSet::new(),
            tasks: HashMap::new(),
            show_history: false,
            history: Vec::new(),
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
        let bytes_done: u64 = jobs.iter().map(|j| j.bytes_done).sum();
        let bytes_total: u64 = jobs.iter().map(|j| j.bytes_total).sum();
        let bps = self.snapshot.total_bps;
        Some(TransferStats {
            active: jobs.len(),
            conflicts: jobs.iter().map(|j| j.files_conflict).sum(),
            bytes_done,
            bytes_total,
            bps,
            eta_secs: (bps > 0 && bytes_total > bytes_done)
                .then(|| (bytes_total - bytes_done).div_ceil(bps)),
        })
    }

    fn toggle_expanded(&mut self, job_id: i64, cx: &mut Context<Self>) {
        if self.expanded.remove(&job_id) {
            self.tasks.remove(&job_id);
        } else {
            self.expanded.insert(job_id);
            self.fetch_tasks(job_id, cx);
        }
        cx.notify();
    }

    fn fetch_tasks(&mut self, job_id: i64, cx: &mut Context<Self>) {
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let fetch = state
            .runtime
            .spawn(async move { crate::queue::store::job_tasks(&pool, job_id, TASK_LIMIT).await });
        cx.spawn(async move |this, cx| {
            let Ok(Ok(tasks)) = fetch.await else { return };
            this.update(cx, |this, cx| {
                if this.expanded.contains(&job_id) {
                    this.tasks.insert(job_id, tasks);
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
    }

    fn fetch_history(&mut self, cx: &mut Context<Self>) {
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let fetch = state
            .runtime
            .spawn(async move { crate::queue::store::history(&pool, HISTORY_LIMIT).await });
        cx.spawn(async move |this, cx| {
            let Ok(Ok(history)) = fetch.await else { return };
            this.update(cx, |this, cx| {
                this.history = history;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn toggle_history(&mut self, cx: &mut Context<Self>) {
        self.show_history = !self.show_history;
        if self.show_history {
            self.fetch_history(cx);
        }
        cx.notify();
    }

    fn render_job(&self, job: &JobSnapshot, cx: &mut Context<Self>) -> impl IntoElement {
        let queue = cx.global::<AppState>().queue.clone();
        let id = job.id;

        let (icon, label) = match (job.direction(), job.move_source) {
            ("upload", false) => (IconName::ArrowUp, "Upload"),
            ("upload", true) => (IconName::ArrowUp, "Move (upload)"),
            ("download", false) => (IconName::ArrowDown, "Download"),
            ("download", true) => (IconName::ArrowDown, "Move (download)"),
            (_, false) => (IconName::ArrowRight, "Copy"),
            (_, true) => (IconName::ArrowRight, "Move"),
        };
        let percent = if job.bytes_total > 0 {
            (job.bytes_done as f32 / job.bytes_total as f32) * 100.0
        } else if job.state == JobState::Done {
            100.0
        } else {
            0.0
        };

        let mut detail = format!(
            "{label} · {} → {} · {}/{} file(s) · {} / {}",
            job.src_site,
            job.dst_site,
            job.files_done,
            job.files_total,
            human_size(job.bytes_done),
            human_size(job.bytes_total),
        );
        if job.files_skipped > 0 {
            detail.push_str(&format!(" · {} skipped", job.files_skipped));
        }
        if job.state == JobState::Active && job.bps > 0 {
            detail.push_str(&format!(" · {}/s", human_size(job.bps)));
            if job.bytes_total > job.bytes_done {
                let eta = (job.bytes_total - job.bytes_done).div_ceil(job.bps);
                detail.push_str(&format!(" · ETA {}", human_duration(eta)));
            }
        }
        if job.priority != 0 {
            detail.push_str(&format!(" · priority {}", job.priority));
        }
        if job.state != JobState::Active {
            detail.push_str(&format!(" · {}", job.state.as_str()));
        }

        let controls = h_flex()
            .gap_1()
            .when(job.files_conflict > 0, |this| {
                this.child(
                    Button::new(("resolve", id as usize))
                        .label(format!("Resolve {} conflict(s)…", job.files_conflict))
                        .warning()
                        .xsmall()
                        .on_click(cx.listener(move |_, _, window, cx| {
                            open_resolve_dialog(id, window, cx);
                        })),
                )
            })
            .when(
                job.state == JobState::Active || job.state == JobState::Queued,
                |this| {
                    this.child(
                        Button::new(("pause", id as usize))
                            .label("Pause")
                            .ghost()
                            .xsmall()
                            .on_click({
                                let queue = queue.clone();
                                move |_, _, _| queue.pause(id)
                            }),
                    )
                },
            )
            .when(job.state == JobState::Paused, |this| {
                this.child(
                    Button::new(("resume", id as usize))
                        .label("Resume")
                        .ghost()
                        .xsmall()
                        .on_click({
                            let queue = queue.clone();
                            move |_, _, _| queue.resume(id)
                        }),
                )
            })
            .when(
                matches!(job.state, JobState::Failed | JobState::Cancelled),
                |this| {
                    this.child(
                        Button::new(("retry", id as usize))
                            .label("Retry")
                            .ghost()
                            .xsmall()
                            .on_click({
                                let queue = queue.clone();
                                move |_, _, _| queue.retry(id)
                            }),
                    )
                },
            )
            .when(!job.state.is_terminal(), |this| {
                this.child(
                    Button::new(("cancel", id as usize))
                        .icon(Icon::new(IconName::CircleX))
                        .ghost()
                        .xsmall()
                        .tooltip("Cancel")
                        .on_click({
                            let queue = queue.clone();
                            move |_, _, _| queue.cancel(id)
                        }),
                )
            })
            .when(job.state.is_terminal(), |this| {
                this.child(
                    Button::new(("remove", id as usize))
                        .icon(Icon::new(IconName::Close))
                        .ghost()
                        .xsmall()
                        .tooltip("Remove from list")
                        .on_click({
                            let queue = queue.clone();
                            move |_, _, _| queue.remove(id)
                        }),
                )
            });

        let is_expanded = self.expanded.contains(&id);
        let weak = cx.entity().downgrade();
        let menu_job = job.clone();

        v_flex()
            .id(("job", id as usize))
            .gap_1()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .context_menu(move |menu, _, cx| {
                build_job_menu(menu, &menu_job, is_expanded, weak.clone(), cx)
            })
            .child(
                h_flex()
                    .gap_2()
                    .justify_between()
                    .child(
                        h_flex()
                            .gap_1()
                            .overflow_hidden()
                            .child(
                                Button::new(("expand", id as usize))
                                    .icon(Icon::new(if is_expanded {
                                        IconName::ChevronDown
                                    } else {
                                        IconName::ChevronRight
                                    }))
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Show files")
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.toggle_expanded(id, cx);
                                    })),
                            )
                            .child(Icon::new(icon).small().text_color(
                                if job.state == JobState::Failed {
                                    cx.theme().danger
                                } else {
                                    cx.theme().muted_foreground
                                },
                            ))
                            .child(div().truncate().text_sm().child(format!(
                                "/{} → /{}",
                                job.src, job.dst
                            ))),
                    )
                    .child(controls),
            )
            .child(Progress::new().value(percent))
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(detail),
            )
            .when_some(job.error.clone(), |this, error| {
                this.child(
                    div()
                        .text_xs()
                        .text_color(cx.theme().danger)
                        .child(error),
                )
            })
            .when(is_expanded, |this| this.child(self.render_tasks(job, cx)))
    }

    /// The unfolded per-file task list of one job.
    fn render_tasks(&self, job: &JobSnapshot, cx: &Context<Self>) -> impl IntoElement {
        let tasks = self.tasks.get(&job.id);
        let shown = tasks.map(|tasks| tasks.len()).unwrap_or(0);
        let remaining = (job.files_total as usize).saturating_sub(shown);
        v_flex()
            .pl_6()
            .pt_1()
            .gap_0p5()
            .when(tasks.is_none(), |this| {
                this.child(
                    div()
                        .text_xs()
                        .text_color(cx.theme().muted_foreground)
                        .child("Loading files…"),
                )
            })
            .children(tasks.into_iter().flatten().map(|task| {
                let name = task.src.rsplit('/').next().unwrap_or(&task.src).to_string();
                let progress = match task.total_bytes {
                    Some(total) if task.state == "active" => {
                        format!("{} / {}", human_size(task.bytes_done), human_size(total))
                    }
                    Some(total) => human_size(total),
                    None => String::new(),
                };
                h_flex()
                    .gap_2()
                    .text_xs()
                    .child(
                        div()
                            .w(px(64.))
                            .flex_shrink_0()
                            .text_color(match task.state.as_str() {
                                "failed" => cx.theme().danger,
                                "conflict" => cx.theme().warning,
                                "done" => cx.theme().success,
                                _ => cx.theme().muted_foreground,
                            })
                            .child(task.state.clone()),
                    )
                    .child(div().flex_1().min_w_0().truncate().child(name))
                    .child(
                        div()
                            .text_color(cx.theme().muted_foreground)
                            .whitespace_nowrap()
                            .child(progress),
                    )
                    .when_some(task.error.clone(), |this, error| {
                        this.child(
                            div()
                                .max_w(px(320.))
                                .truncate()
                                .text_color(cx.theme().danger)
                                .child(error),
                        )
                    })
            }))
            .when(remaining > 0, |this| {
                this.child(
                    div()
                        .text_xs()
                        .text_color(cx.theme().muted_foreground)
                        .child(format!("…and {remaining} more")),
                )
            })
    }

    fn render_history(&self, cx: &Context<Self>) -> impl IntoElement {
        v_flex()
            .id("history-scroll")
            .flex_1()
            .min_h_0()
            .px_3()
            .overflow_y_scroll()
            .when(self.history.is_empty(), |this| {
                this.child(
                    div()
                        .py_8()
                        .text_center()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child("No finished transfers yet."),
                )
            })
            .children(self.history.iter().map(|entry| {
                let outcome_color = match entry.outcome.as_str() {
                    "done" => cx.theme().success,
                    "failed" => cx.theme().danger,
                    _ => cx.theme().muted_foreground,
                };
                let mut detail = format!(
                    "{} · {} → {} · {} file(s) · {}",
                    entry.finished_at,
                    entry.src_site,
                    entry.dst_site,
                    entry.files,
                    human_size(entry.bytes),
                );
                if let Some(ms) = entry.duration_ms {
                    detail.push_str(&format!(" · {}", human_duration((ms / 1000) as u64)));
                }
                v_flex()
                    .gap_0p5()
                    .py_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                div()
                                    .w(px(72.))
                                    .flex_shrink_0()
                                    .text_sm()
                                    .text_color(outcome_color)
                                    .child(entry.outcome.clone()),
                            )
                            .child(div().truncate().text_sm().child(format!(
                                "/{} → /{}",
                                entry.src, entry.dst
                            ))),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(detail),
                    )
                    .when_some(entry.error.clone(), |this, error| {
                        this.child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().danger)
                                .child(error),
                        )
                    })
            }))
    }
}

/// Right-click menu on one job row.
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
        let panel = panel.clone();
        menu = menu.item(
            PopupMenuItem::new(format!("Resolve {} Conflict(s)…", job.files_conflict)).on_click(
                move |_, window, cx| {
                    panel
                        .update(cx, |_, cx| open_resolve_dialog(id, window, cx))
                        .ok();
                },
            ),
        );
    }
    if matches!(job.state, JobState::Active | JobState::Queued) {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new("Pause").on_click(move |_, _, _| queue.pause(id)));
    }
    if job.state == JobState::Paused {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new("Resume").on_click(move |_, _, _| queue.resume(id)));
    }
    if matches!(job.state, JobState::Failed | JobState::Cancelled) {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new("Retry").on_click(move |_, _, _| queue.retry(id)));
    }
    if !job.state.is_terminal() {
        let queue = queue.clone();
        menu = menu.item(PopupMenuItem::new("Cancel").on_click(move |_, _, _| queue.cancel(id)));
    }

    menu = menu.separator();
    {
        let panel = panel.clone();
        let label = if is_expanded { "Hide Files" } else { "Show Files" };
        menu = menu.item(PopupMenuItem::new(label).on_click(move |_, _, cx| {
            panel
                .update(cx, |this, cx| this.toggle_expanded(id, cx))
                .ok();
        }));
    }
    if !job.state.is_terminal() {
        let top = cx
            .global::<AppState>()
            .queue
            .latest()
            .jobs
            .iter()
            .map(|job| job.priority)
            .max()
            .unwrap_or(0);
        {
            let queue = queue.clone();
            menu = menu.item(
                PopupMenuItem::new("Run First")
                    .disabled(job.priority > top || (job.priority == top && top != 0))
                    .on_click(move |_, _, _| queue.set_priority(id, top + 1)),
            );
        }
        if job.priority != 0 {
            let queue = queue.clone();
            menu = menu.item(
                PopupMenuItem::new("Reset Priority")
                    .on_click(move |_, _, _| queue.set_priority(id, 0)),
            );
        }
    }

    menu = menu.separator();
    {
        let src = format!("/{}", job.src);
        menu = menu.item(PopupMenuItem::new("Copy Source Path").on_click(move |_, _, cx| {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(src.clone()));
        }));
    }
    {
        let dst = format!("/{}", job.dst);
        menu = menu.item(
            PopupMenuItem::new("Copy Destination Path").on_click(move |_, _, cx| {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(dst.clone()));
            }),
        );
    }

    if job.state.is_terminal() {
        let queue = queue.clone();
        menu = menu.separator().item(
            PopupMenuItem::new("Remove from List").on_click(move |_, _, _| queue.remove(id)),
        );
    }
    menu
}

impl Render for TransfersPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let jobs = self.snapshot.jobs.clone();
        let has_finished = jobs.iter().any(|j| j.state.is_terminal());
        let any_pausable = jobs
            .iter()
            .any(|j| matches!(j.state, JobState::Active | JobState::Queued));
        let any_paused = jobs.iter().any(|j| j.state == JobState::Paused);
        let queue = cx.global::<AppState>().queue.clone();
        let stats = self.stats();

        v_flex()
            .size_full()
            .overflow_hidden()
            // Match the sidebar so the docked panels read as one surface.
            .bg(cx.theme().sidebar)
            .child(
                // Panel toolbar: identity + close live in the dock's tab strip
                // above; this row carries only the queue's actions.
                h_flex()
                    .px_3()
                    .py_1()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(div().flex_1())
                    .when(!self.show_history && any_pausable, |this| {
                        let queue = queue.clone();
                        this.child(
                            Button::new("pause-all")
                                .label("Pause all")
                                .ghost()
                                .xsmall()
                                .on_click(move |_, _, _| queue.pause_all()),
                        )
                    })
                    .when(!self.show_history && any_paused, |this| {
                        let queue = queue.clone();
                        this.child(
                            Button::new("resume-all")
                                .label("Resume all")
                                .ghost()
                                .xsmall()
                                .on_click(move |_, _, _| queue.resume_all()),
                        )
                    })
                    .when(!self.show_history && has_finished, |this| {
                        let queue = queue.clone();
                        this.child(
                            Button::new("clear-finished")
                                .label("Clear finished")
                                .ghost()
                                .xsmall()
                                .on_click(move |_, _, _| queue.clear_finished()),
                        )
                    })
                    .child(
                        Button::new("toggle-history")
                            .label(if self.show_history { "Queue" } else { "History" })
                            .ghost()
                            .xsmall()
                            .on_click(cx.listener(|this, _, _, cx| this.toggle_history(cx))),
                    ),
            )
            .child(if self.show_history {
                self.render_history(cx).into_any_element()
            } else if jobs.is_empty() {
                div()
                    .flex_1()
                    .py_8()
                    .text_center()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("No transfers yet — drop files onto a pane to get started.")
                    .into_any_element()
            } else {
                v_flex()
                    .id("transfers-scroll")
                    .flex_1()
                    .min_h_0()
                    .px_3()
                    .overflow_y_scroll()
                    .children(jobs.iter().map(|job| self.render_job(job, cx)))
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
                        .child(
                            div()
                                .w(px(160.))
                                .child(Progress::new().value(stats.percent())),
                        )
                        .child(
                            div()
                                .text_xs()
                                .text_color(cx.theme().muted_foreground)
                                .child(stats.summary()),
                        ),
                )
            })
    }
}
