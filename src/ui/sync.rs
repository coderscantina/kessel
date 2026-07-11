//! The Synchronize dialog: pick direction and mode, review the planned
//! actions, then hand the copies to the transfer queue (and, in mirror mode,
//! delete extraneous destination entries).

use gpui::prelude::*;
use gpui::{div, px, AnyElement, Context, Entity, SharedString, Window};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::select::{SearchableVec, Select, SelectEvent, SelectState};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Disableable as _, Icon, IconName, Sizable as _,
    StyledExt as _, WindowExt as _,
};

use crate::app::AppState;
use crate::i18n::{t, tf};
use crate::provider::{Provider, SiteProfile};
use crate::queue::{ConflictPolicy, NewJob};
use crate::sync::{dst_dir_for, plan, PlanKind, SyncMode, SyncPlan};
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
    Planned(SyncPlan),
    Error(SharedString),
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
        let task = state.runtime.spawn(async move {
            let src_provider = Provider::connect(&src.profile, &defaults)?;
            let dst_provider = Provider::connect(&dst.profile, &defaults)?;
            plan(&src_provider, &src.dir, &dst_provider, &dst.dir, mode).await
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
                    Ok(plan) => State::Planned(plan),
                    Err(error) => State::Error(format!("{error:#}").into()),
                };
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Enqueue the plan's copies on the transfer queue and run its deletes on
    /// the runtime. Returns how many transfers were queued.
    fn apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let State::Planned(sync_plan) = &self.state else {
            return;
        };
        let sync_plan = sync_plan.clone();
        let (src, dst) = self.oriented(cx);
        let state = cx.global::<AppState>();
        let queue = state.queue.clone();

        let mut queued = 0usize;
        let mut deletes: Vec<(String, bool)> = Vec::new();
        let mut empty_dirs: Vec<String> = Vec::new();
        for item in &sync_plan.items {
            match item.kind {
                PlanKind::Copy | PlanKind::Update if item.is_dir => {
                    empty_dirs.push(format!("{}{}", dst.dir, item.rel));
                }
                PlanKind::Copy | PlanKind::Update => {
                    queue.enqueue(NewJob {
                        src_site_id: src.profile.id,
                        dst_site_id: dst.profile.id,
                        src: format!("{}{}", src.dir, item.rel),
                        dst_dir: dst_dir_for(&dst.dir, &item.rel),
                        dst_name: None,
                        conflict_policy: ConflictPolicy::Overwrite,
                        move_source: false,
                    });
                    queued += 1;
                }
                PlanKind::Delete => {
                    deletes.push((format!("{}{}", dst.dir, item.rel), item.is_dir));
                }
            }
        }

        if !deletes.is_empty() || !empty_dirs.is_empty() {
            let defaults = state.config.layers.clone();
            let dst_profile = dst.profile.clone();
            state.runtime.spawn(async move {
                let provider = match Provider::connect(&dst_profile, &defaults) {
                    Ok(provider) => provider,
                    Err(error) => {
                        tracing::error!(%error, "sync: connecting for deletes failed");
                        return;
                    }
                };
                for dir in empty_dirs {
                    if let Err(error) = provider.create_dir(&dir).await {
                        tracing::warn!(%error, dir, "sync: creating directory failed");
                    }
                }
                let mut deleted = 0usize;
                for (path, is_dir) in deletes {
                    match provider.delete(&path, is_dir).await {
                        Ok(()) => deleted += 1,
                        Err(error) => tracing::error!(%error, path, "sync: delete failed"),
                    }
                }
                tracing::info!(deleted, "sync: extraneous entries removed");
            });
        }

        if queued > 0 {
            window.push_notification(
                tf("sync.queued", &[("count", &queued.to_string())]).to_string(),
                cx,
            );
        }
        gpui_component::WindowExt::close_dialog(window, cx);
    }

    fn render_plan(&self, sync_plan: &SyncPlan, cx: &Context<Self>) -> AnyElement {
        let muted = cx.theme().muted_foreground;
        if sync_plan.is_empty() {
            return div()
                .text_sm()
                .text_color(cx.theme().success)
                .child(t("sync.in_sync"))
                .into_any_element();
        }
        let (copies, updates, deletes) = sync_plan.counts();
        let summary = tf(
            "sync.plan.summary",
            &[
                ("copies", &copies.to_string()),
                ("updates", &updates.to_string()),
                ("deletes", &deletes.to_string()),
                ("bytes", &human_size(sync_plan.transfer_bytes())),
            ],
        );

        const MAX_ROWS: usize = 400;
        let rows: Vec<AnyElement> = sync_plan
            .items
            .iter()
            .take(MAX_ROWS)
            .enumerate()
            .map(|(ix, item)| {
                let (tag, color) = match item.kind {
                    PlanKind::Copy => (t("sync.plan.copy"), cx.theme().success),
                    PlanKind::Update => (t("sync.plan.update"), cx.theme().warning),
                    PlanKind::Delete => (t("sync.plan.delete"), cx.theme().danger),
                };
                h_flex()
                    .id(("sync-row", ix))
                    .gap_2()
                    .px_1()
                    .text_xs()
                    .child(
                        div()
                            .w(px(64.))
                            .flex_shrink_0()
                            .text_color(color)
                            .child(tag),
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
            })
            .collect();

        v_flex()
            .gap_2()
            .child(div().text_sm().child(summary))
            .child(
                v_flex()
                    .id("sync-plan")
                    .max_h(px(260.))
                    .overflow_y_scroll()
                    .gap_0p5()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded_md()
                    .p_1()
                    .children(rows)
                    .when(sync_plan.items.len() > MAX_ROWS, |el| {
                        el.child(
                            div()
                                .px_1()
                                .text_xs()
                                .text_color(muted)
                                .child(format!("+ {}", sync_plan.items.len() - MAX_ROWS)),
                        )
                    }),
            )
            .into_any_element()
    }
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
            State::Planned(sync_plan) => self.render_plan(&sync_plan.clone(), cx),
            State::Error(error) => div()
                .text_sm()
                .text_color(cx.theme().danger)
                .child(error.clone())
                .into_any_element(),
        };

        let can_start = matches!(&self.state, State::Planned(plan) if !plan.is_empty());
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
