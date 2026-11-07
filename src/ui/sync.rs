//! The Synchronize dialog: pick direction and mode, review every planned
//! action (per row and per kind), then run the selection: one queue job for
//! the copies and, in mirror mode, deletes once that job succeeded.

use gpui::prelude::*;
use gpui::{div, px, uniform_list, AnyElement, Context, Entity, Hsla, SharedString, Window};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::checkbox::Checkbox;
use gpui_component::select::{SearchableVec, Select, SelectEvent, SelectState};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Disableable as _, Icon, IconName, Sizable as _,
    StyledExt as _, Theme, WindowExt as _,
};

use crate::app::AppState;
use crate::i18n::{t, tf};
use crate::provider::{Provider, SiteProfile};
use crate::sync::{plan, PlanKind, SyncMode, SyncPlan, SyncReport, SyncWork};
use crate::ui::human_size;

/// One side of the comparison, captured from a pane when the dialog opens.
#[derive(Clone)]
pub struct SyncSide {
    pub profile: SiteProfile,
    pub dir: String,
    pub label: SharedString,
}

enum State {
    Idle,
    Planning,
    Planned(Review),
    Error(SharedString),
}

/// A computed plan, the sides it was computed for, and which items the user
/// keeps (parallel to `plan.items`).
struct Review {
    plan: SyncPlan,
    selected: Vec<bool>,
    src: SyncSide,
    dst: SyncSide,
}

impl Review {
    fn new(plan: SyncPlan, src: SyncSide, dst: SyncSide) -> Self {
        let selected = plan
            .items
            .iter()
            .map(|item| item.kind.selected_by_default())
            .collect();
        Self {
            plan,
            selected,
            src,
            dst,
        }
    }

    /// (selected, total) items of one kind.
    fn tally(&self, kind: PlanKind) -> (usize, usize) {
        self.plan
            .items
            .iter()
            .zip(&self.selected)
            .filter(|(item, _)| item.kind == kind)
            .fold((0, 0), |(on, total), (_, selected)| (on + usize::from(*selected), total + 1))
    }

    fn set_kind(&mut self, kind: PlanKind, on: bool) {
        for (item, selected) in self.plan.items.iter().zip(&mut self.selected) {
            if item.kind == kind {
                *selected = on;
            }
        }
    }

    /// Bytes the selected copies will transfer.
    fn selected_bytes(&self) -> u64 {
        self.plan
            .items
            .iter()
            .zip(&self.selected)
            .filter(|(item, on)| **on && item.kind != PlanKind::Delete && !item.is_dir)
            .map(|(item, _)| item.size)
            .sum()
    }

    fn any_selected(&self) -> bool {
        self.selected.iter().any(|on| *on)
    }
}

type StringSelect = Entity<SelectState<SearchableVec<SharedString>>>;

pub struct SyncDialog {
    a: SyncSide,
    b: SyncSide,
    direction: StringSelect,
    mode: StringSelect,
    state: State,
    /// Bumped per Analyze click so a stale plan task can't overwrite a newer
    /// one.
    epoch: u64,
}

impl SyncDialog {
    pub fn new(a: SyncSide, b: SyncSide, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let arrow = |from: &SyncSide, to: &SyncSide| {
            SharedString::from(format!("{} → {}", side_name(from), side_name(to)))
        };
        let direction_items = vec![arrow(&a, &b), arrow(&b, &a)];
        let direction = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(direction_items),
                Some(gpui_component::IndexPath::new(0)),
                window,
                cx,
            )
        });
        let mode_items: Vec<SharedString> = vec![t("sync.mode.update"), t("sync.mode.mirror")];
        let mode = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(mode_items),
                Some(gpui_component::IndexPath::new(0)),
                window,
                cx,
            )
        });
        // Direction / mode changes invalidate a computed plan.
        for select in [&direction, &mode] {
            cx.subscribe(
                select,
                |this, _, event: &SelectEvent<SearchableVec<SharedString>>, cx| {
                    if matches!(event, SelectEvent::Confirm(_)) {
                        this.epoch += 1;
                        this.state = State::Idle;
                        cx.notify();
                    }
                },
            )
            .detach();
        }

        Self {
            a,
            b,
            direction,
            mode,
            state: State::Idle,
            epoch: 0,
        }
    }

    fn a_to_b(&self, cx: &Context<Self>) -> bool {
        self.direction
            .read(cx)
            .selected_index(cx)
            .map(|ix| ix.row == 0)
            .unwrap_or(true)
    }

    fn mode_value(&self, cx: &Context<Self>) -> SyncMode {
        match self.mode.read(cx).selected_index(cx).map(|ix| ix.row) {
            Some(1) => SyncMode::Mirror,
            _ => SyncMode::Update,
        }
    }

    fn oriented(&self, cx: &Context<Self>) -> (SyncSide, SyncSide) {
        if self.a_to_b(cx) {
            (self.a.clone(), self.b.clone())
        } else {
            (self.b.clone(), self.a.clone())
        }
    }

    fn analyze(&mut self, cx: &mut Context<Self>) {
        let (src, dst) = self.oriented(cx);
        let mode = self.mode_value(cx);
        self.epoch += 1;
        let epoch = self.epoch;
        self.state = State::Planning;
        cx.notify();

        let state = cx.global::<AppState>();
        let defaults = state.config.layers.clone();
        let (src_profile, dst_profile) = (src.profile.clone(), dst.profile.clone());
        let (src_dir, dst_dir) = (src.dir.clone(), dst.dir.clone());
        let task = state.runtime.spawn(async move {
            let src_provider = Provider::connect(&src_profile, &defaults)?;
            let dst_provider = Provider::connect(&dst_profile, &defaults)?;
            plan(&src_provider, &src_dir, &dst_provider, &dst_dir, mode).await
        });
        cx.spawn(async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("sync planning failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                if this.epoch != epoch {
                    return; // superseded by a newer Analyze / settings change
                }
                this.state = match result {
                    Ok(plan) => State::Planned(Review::new(plan, src, dst)),
                    Err(error) => {
                        tracing::error!(error = %format!("{error:#}"), "sync planning failed");
                        State::Error(crate::errors::humanize(&error).title.into())
                    }
                };
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Run the reviewed selection in the background and report the outcome
    /// as a notification once the copies settled and deletes ran.
    fn apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let State::Planned(review) = &self.state else {
            return;
        };
        let work = SyncWork::from_plan(&review.plan, &review.selected, &review.src.dir, &review.dst.dir);
        let src_site_id = review.src.profile.id;
        let dst_profile = review.dst.profile.clone();
        let src_label = side_name(&review.src);
        let dst_label = side_name(&review.dst);
        let files = work.copies.len();

        let state = cx.global::<AppState>();
        let queue = state.queue.clone();
        let defaults = state.config.layers.clone();
        let task = state.runtime.spawn(async move {
            let dst = Provider::connect(&dst_profile, &defaults)?;
            anyhow::Ok(crate::sync::run(&queue, src_site_id, &dst, src_label, dst_label, work).await)
        });
        let handle = window.window_handle();
        cx.spawn(async move |_, cx| {
            let message = match task.await {
                Ok(Ok(report)) => report_message(&report),
                Ok(Err(error)) => tf(
                    "sync.report.failed",
                    &[("reason", &crate::errors::humanize(&error).title)],
                )
                .to_string(),
                Err(join_error) => format!("{join_error}"),
            };
            cx.update_window(handle, |_, window, cx| window.push_notification(message, cx))
                .ok();
        })
        .detach();

        if files > 0 {
            window.push_notification(tf("sync.started", &[("count", &files.to_string())]).to_string(), cx);
        }
        window.close_dialog(cx);
    }

    fn render_review(&self, review: &Review, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();
        if review.plan.is_empty() {
            return h_flex()
                .gap_2()
                .items_center()
                .text_sm()
                .child(Icon::new(IconName::Check).small().text_color(theme.success))
                .child(t("sync.in_sync"))
                .into_any_element();
        }

        let kinds = PlanKind::ALL.into_iter().filter_map(|kind| {
            let (on, total) = review.tally(kind);
            (total > 0).then(|| {
                let color = kind_color(kind, theme);
                h_flex()
                    .gap_2()
                    .items_center()
                    .text_sm()
                    .child(
                        Checkbox::new(("sync-kind", kind as usize))
                            .checked(on == total)
                            .on_click(cx.listener(move |this, checked: &bool, _, cx| {
                                if let State::Planned(review) = &mut this.state {
                                    review.set_kind(kind, *checked);
                                    cx.notify();
                                }
                            })),
                    )
                    .child(div().flex_1().text_color(color).child(kind_label(kind)))
                    .child(
                        div()
                            .text_color(if kind == PlanKind::Delete && on > 0 {
                                theme.danger
                            } else {
                                theme.muted_foreground
                            })
                            .child(tf(
                                "sync.kind.count",
                                &[("selected", &on.to_string()), ("total", &total.to_string())],
                            )),
                    )
            })
        });

        let rows = uniform_list(
            "sync-plan",
            review.plan.items.len(),
            cx.processor(|this, range: std::ops::Range<usize>, _, cx| {
                let State::Planned(review) = &this.state else {
                    return Vec::new();
                };
                range.map(|ix| render_row(review, ix, cx)).collect()
            }),
        )
        .h(px(240.));

        v_flex()
            .gap_2()
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(t("sync.rule")),
            )
            .child(v_flex().gap_1().children(kinds))
            .child(
                div()
                    .border_1()
                    .border_color(theme.border)
                    .rounded_md()
                    .p_1()
                    .child(rows),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(tf(
                        "sync.transfer_size",
                        &[("bytes", &human_size(review.selected_bytes()))],
                    )),
            )
            .into_any_element()
    }
}

fn render_row(review: &Review, ix: usize, cx: &mut Context<SyncDialog>) -> AnyElement {
    let item = &review.plan.items[ix];
    let theme = cx.theme();
    let muted = theme.muted_foreground;
    h_flex()
        .id(("sync-row", ix))
        .gap_2()
        .px_1()
        .h(px(24.))
        .items_center()
        .text_xs()
        .child(
            Checkbox::new(("sync-item", ix))
                .checked(review.selected[ix])
                .on_click(cx.listener(move |this, checked: &bool, _, cx| {
                    if let State::Planned(review) = &mut this.state {
                        review.selected[ix] = *checked;
                        cx.notify();
                    }
                })),
        )
        .child(
            div()
                .w(px(88.))
                .flex_shrink_0()
                .text_color(kind_color(item.kind, theme))
                .child(row_tag(item.kind)),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .child(SharedString::from(item.rel.clone())),
        )
        .when(!item.is_dir && item.kind != PlanKind::Delete, |el| {
            el.child(div().text_color(muted).child(human_size(item.size)))
        })
        .into_any_element()
}

fn kind_color(kind: PlanKind, theme: &Theme) -> Hsla {
    match kind {
        PlanKind::Copy => theme.success,
        PlanKind::Update => theme.info,
        PlanKind::DestNewer => theme.warning,
        PlanKind::Unsure => theme.muted_foreground,
        PlanKind::Delete => theme.danger,
    }
}

fn kind_label(kind: PlanKind) -> SharedString {
    t(match kind {
        PlanKind::Copy => "sync.kind.copy",
        PlanKind::Update => "sync.kind.update",
        PlanKind::DestNewer => "sync.kind.dest_newer",
        PlanKind::Unsure => "sync.kind.unsure",
        PlanKind::Delete => "sync.kind.delete",
    })
}

fn row_tag(kind: PlanKind) -> SharedString {
    t(match kind {
        PlanKind::Copy => "sync.plan.copy",
        PlanKind::Update => "sync.plan.update",
        PlanKind::DestNewer => "sync.plan.dest_newer",
        PlanKind::Unsure => "sync.plan.unsure",
        PlanKind::Delete => "sync.plan.delete",
    })
}

/// The closing notification: what was copied and deleted, or what failed
/// and whether deletes were held back because of it.
fn report_message(report: &SyncReport) -> String {
    let counts = [
        ("copied", report.copied.to_string()),
        ("deleted", report.deleted.to_string()),
        ("failed", report.failed.to_string()),
        ("reason", report.reason.clone().unwrap_or_default()),
    ];
    let args: Vec<(&str, &str)> = counts.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let mut message = match (report.failed, &report.reason) {
        (0, None) => tf("sync.report.done", &args).to_string(),
        // Stopped without a failed file, e.g. the copy job was cancelled.
        (0, Some(_)) => tf("sync.report.failed", &args).to_string(),
        _ => tf("sync.report.partial", &args).to_string(),
    };
    if report.deletes_skipped {
        message.push(' ');
        message.push_str(&t("sync.report.deletes_skipped"));
    }
    message
}

/// "label — /path" for the direction picker.
fn side_name(side: &SyncSide) -> String {
    let dir = if side.dir.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", side.dir.trim_end_matches('/'))
    };
    format!("{} {dir}", side.label)
}

impl Render for SyncDialog {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;
        let labeled = |label: SharedString, control: AnyElement| {
            v_flex()
                .gap_1()
                .child(div().text_xs().font_medium().text_color(muted).child(label))
                .child(control)
        };

        let body: AnyElement = match &self.state {
            State::Idle => div().into_any_element(),
            State::Planning => h_flex()
                .gap_2()
                .items_center()
                .text_sm()
                .text_color(muted)
                .child(Icon::new(IconName::LoaderCircle).small())
                .child(t("sync.analyzing"))
                .into_any_element(),
            State::Planned(review) => self.render_review(review, cx),
            State::Error(error) => div()
                .text_sm()
                .text_color(cx.theme().danger)
                .child(error.clone())
                .into_any_element(),
        };

        let can_start = matches!(&self.state, State::Planned(review) if review.any_selected());
        let planning = matches!(self.state, State::Planning);

        v_flex()
            .gap_3()
            .child(labeled(
                t("sync.direction"),
                Select::new(&self.direction).small().into_any_element(),
            ))
            .child(labeled(
                t("sync.mode"),
                Select::new(&self.mode).small().into_any_element(),
            ))
            .child(body)
            .child(
                h_flex()
                    .gap_2()
                    .justify_end()
                    .child(
                        Button::new("sync-analyze")
                            .label(t("sync.analyze"))
                            .outline()
                            .small()
                            .loading(planning)
                            .disabled(planning)
                            .on_click(cx.listener(|this, _, _, cx| this.analyze(cx))),
                    )
                    .child(
                        Button::new("sync-start")
                            .label(t("sync.start"))
                            .primary()
                            .small()
                            .disabled(!can_start)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.apply(window, cx);
                            })),
                    ),
            )
    }
}
