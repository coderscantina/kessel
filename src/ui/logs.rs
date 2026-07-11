//! Bottom log panel: a live view over the in-app log buffer
//! (`crate::logs`), which captures the whole tracing stack — kessel's own
//! events plus OpenDAL's per-operation logging layer.

use std::collections::BTreeSet;
use std::time::Duration;

use gpui::prelude::*;
use gpui::{div, px, ClickEvent, ClipboardItem, Context, Window};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Sizable as _,
};

use crate::logs::{self, LogEntry};

pub struct LogsPanel {
    entries: Vec<LogEntry>,
    seen_version: u64,
    /// Selected rows, as indices into `entries` (oldest-first). New log lines
    /// append at the back, so existing indices stay valid; a shrink (clear or
    /// ring eviction) drops the now-invalid ones on refresh.
    selected: BTreeSet<usize>,
    /// Anchor for shift-click range selection.
    anchor: Option<usize>,
}

impl LogsPanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        // Poll the buffer's change counter; log events arrive from arbitrary
        // threads, so there is no direct way to wake this view.
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(500))
                    .await;
                let alive = this.update(cx, |this: &mut LogsPanel, cx| {
                    let version = logs::version();
                    if version != this.seen_version {
                        this.seen_version = version;
                        this.adopt(logs::entries(), cx);
                    }
                });
                if alive.is_err() {
                    break;
                }
            }
        })
        .detach();

        Self {
            entries: logs::entries(),
            seen_version: logs::version(),
            selected: BTreeSet::new(),
            anchor: None,
        }
    }

    /// Count of WARN/ERROR entries — surfaced as the dock's Logs tab badge.
    pub fn problem_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| {
                matches!(entry.level, tracing::Level::WARN | tracing::Level::ERROR)
            })
            .count()
    }

    /// Swap in a fresh snapshot, keeping the selection valid: a shrunk buffer
    /// (clear / ring eviction) can no longer be indexed, so drop stale rows.
    fn adopt(&mut self, entries: Vec<LogEntry>, cx: &mut Context<Self>) {
        if entries.len() < self.entries.len() {
            self.selected.clear();
            self.anchor = None;
        }
        self.entries = entries;
        cx.notify();
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        self.seen_version = logs::version();
        self.adopt(logs::entries(), cx);
    }

    fn clear(&mut self, cx: &mut Context<Self>) {
        logs::clear();
        self.selected.clear();
        self.anchor = None;
        self.refresh(cx);
    }

    fn on_row_click(&mut self, ix: usize, event: &ClickEvent, cx: &mut Context<Self>) {
        let modifiers = event.modifiers();
        if modifiers.shift {
            let anchor = self.anchor.unwrap_or(ix);
            self.anchor = Some(anchor);
            let (lo, hi) = (anchor.min(ix), anchor.max(ix));
            self.selected = (lo..=hi).collect();
        } else if modifiers.secondary() {
            // Cmd/Ctrl-click toggles a single row.
            if !self.selected.remove(&ix) {
                self.selected.insert(ix);
            }
            self.anchor = Some(ix);
        } else {
            self.selected = BTreeSet::from([ix]);
            self.anchor = Some(ix);
        }
        cx.notify();
    }

    fn copy(&self, indices: impl Iterator<Item = usize>, cx: &mut Context<Self>) {
        let text = indices
            .filter_map(|ix| self.entries.get(ix))
            .map(entry_line)
            .collect::<Vec<_>>()
            .join("\n");
        if !text.is_empty() {
            cx.write_to_clipboard(ClipboardItem::new_string(text));
        }
    }
}

/// One log line as plain text, matching the panel's column order.
fn entry_line(entry: &LogEntry) -> String {
    format!(
        "{} {:5} {} {}",
        entry.time.format("%H:%M:%S%.3f"),
        entry.level.as_str(),
        entry.target,
        entry.message,
    )
}

impl Render for LogsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let selected_count = self.selected.len();
        v_flex()
            .size_full()
            .overflow_hidden()
            // Match the sidebar so the docked panels read as one surface.
            .bg(cx.theme().sidebar)
            .child(
                // Panel toolbar: identity + close live in the dock's tab strip
                // above; this row carries only the log's meta + actions.
                h_flex()
                    .px_3()
                    .py_1()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        div()
                            .text_xs()
                            .text_color(cx.theme().muted_foreground)
                            .child(if selected_count > 0 {
                                format!("{selected_count} of {} selected", self.entries.len())
                            } else {
                                format!("{} entries", self.entries.len())
                            }),
                    )
                    .child(div().flex_1())
                    .when(selected_count > 0, |this| {
                        this.child(
                            Button::new("copy-selected")
                                .label(format!("Copy ({selected_count})"))
                                .ghost()
                                .xsmall()
                                .tooltip("Copy selected lines")
                                .on_click(cx.listener(|this, _, _, cx| {
                                    let indices: Vec<usize> =
                                        this.selected.iter().copied().collect();
                                    this.copy(indices.into_iter(), cx);
                                })),
                        )
                    })
                    .when(!self.entries.is_empty(), |this| {
                        this.child(
                            Button::new("copy-all")
                                .label("Copy all")
                                .ghost()
                                .xsmall()
                                .tooltip("Copy every line")
                                .on_click(cx.listener(|this, _, _, cx| {
                                    let count = this.entries.len();
                                    this.copy(0..count, cx);
                                })),
                        )
                    })
                    .child(
                        Button::new("clear-logs")
                            .label("Clear")
                            .ghost()
                            .xsmall()
                            .on_click(cx.listener(|this, _, _, cx| this.clear(cx))),
                    ),
            )
            .child(if self.entries.is_empty() {
                div()
                    .flex_1()
                    .py_8()
                    .text_center()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child("Nothing logged yet — tune `logging.filter` in config.json5.")
                    .into_any_element()
            } else {
                v_flex()
                    .id("logs-scroll")
                    .flex_1()
                    .min_h_0()
                    .px_1()
                    .py_1()
                    .overflow_y_scroll()
                    .font_family("monospace")
                    .text_xs()
                    // Newest first, so fresh output is visible without
                    // chasing the scroll position.
                    .children(self.entries.iter().enumerate().rev().map(|(ix, entry)| {
                        let level_color = match entry.level {
                            tracing::Level::ERROR => cx.theme().danger,
                            tracing::Level::WARN => cx.theme().warning,
                            tracing::Level::INFO => cx.theme().info,
                            _ => cx.theme().muted_foreground,
                        };
                        let is_selected = self.selected.contains(&ix);
                        h_flex()
                            .id(("log", ix))
                            .gap_2()
                            .px_2()
                            .rounded_sm()
                            .items_start()
                            .cursor_pointer()
                            .when(is_selected, |el| el.bg(cx.theme().list_active))
                            .when(!is_selected, |el| {
                                el.hover(|style| style.bg(cx.theme().list_hover))
                            })
                            .on_click(cx.listener(move |this, event: &ClickEvent, _, cx| {
                                this.on_row_click(ix, event, cx);
                            }))
                            .child(
                                div()
                                    .flex_shrink_0()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(entry.time.format("%H:%M:%S%.3f").to_string()),
                            )
                            .child(
                                div()
                                    .w(px(44.))
                                    .flex_shrink_0()
                                    .text_color(level_color)
                                    .child(entry.level.as_str().to_string()),
                            )
                            .child(
                                div()
                                    .max_w(px(220.))
                                    .flex_shrink_0()
                                    .truncate()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(entry.target.clone()),
                            )
                            .child(div().flex_1().min_w_0().child(entry.message.clone()))
                    }))
                    .into_any_element()
            })
    }
}
