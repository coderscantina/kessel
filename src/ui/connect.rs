//! Connecting: the quick-connect URL field, the ssh host key trust dialog
//! and the shared error block that words connection failures for people.
//!
//! Quick connect never adds a bookmark. It creates a site row in
//! [`QUICK_CONNECT_SPACE`], which the sidebar never shows, so the transfer
//! queue and tab restore can still resolve the site by id. "Save as site"
//! moves the row into a real space (see `ui::bookmarks`).

use std::collections::HashMap;
use std::rc::Rc;

use anyhow::{Context as _, Result};
use gpui::prelude::*;
use gpui::{
    div, px, AnyElement, App, Context, Entity, EventEmitter, Focusable as _, SharedString, Window,
};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Disableable as _, Icon, IconName, Sizable as _,
    StyledExt as _, WindowExt as _,
};
use sqlx::SqlitePool;

use crate::app::AppState;
use crate::bookmarks::{
    parse_quick_url, quick_prompt_fields, FieldSpec, QuickTarget, QUICK_CONNECT_SPACE,
};
use crate::db;
use crate::errors::{humanize, Humanized};
use crate::i18n::{t, tf};
use crate::provider::{LayerConfig, SiteProfile};
use crate::secrets::SecretStore;
use crate::sftp::{self, HostKey};

/// Runs after the user trusted a host key (retry the connection).
pub type OnTrusted = Rc<dyn Fn(&mut Window, &mut App)>;

/// An error as shown in a form: wording from [`humanize`], an optional raw
/// chain for the details disclosure, and the host to offer trust for when
/// the failure was an unknown ssh host key.
#[derive(Clone, Debug)]
pub struct FormError {
    pub title: SharedString,
    pub hint: Option<SharedString>,
    pub details: Option<SharedString>,
    pub untrusted_host: Option<(String, u16)>,
}

impl FormError {
    /// A validation message with nothing behind it.
    pub fn plain(message: impl Into<SharedString>) -> Self {
        Self {
            title: message.into(),
            hint: None,
            details: None,
            untrusted_host: None,
        }
    }

    /// A failure from the backend. `sftp_endpoint` is the endpoint that was
    /// tried, so an unknown host key can offer the trust dialog.
    pub fn from_error(error: &anyhow::Error, sftp_endpoint: Option<&str>) -> Self {
        let Humanized {
            title,
            hint,
            details,
            class,
        } = humanize(error);
        let untrusted_host = (class == crate::errors::Class::HostKeyUnknown)
            .then(|| sftp_endpoint.and_then(sftp::host_port))
            .flatten();
        Self {
            details: (details != title).then(|| details.into()),
            title: title.into(),
            hint: hint.map(Into::into),
            untrusted_host,
        }
    }
}

/// The danger-toned error block: title, next step, and a "Details"
/// disclosure for the raw chain. `on_review_key` adds the host key button.
pub fn error_block(
    id: &'static str,
    error: &FormError,
    details_open: bool,
    on_toggle_details: impl Fn(&mut Window, &mut App) + 'static,
    on_review_key: Option<Box<dyn Fn(&mut Window, &mut App)>>,
    cx: &App,
) -> AnyElement {
    let danger = cx.theme().danger;
    let fg = cx.theme().foreground;
    v_flex()
        .id(id)
        .gap_1()
        .p_3()
        .rounded_md()
        .border_1()
        .border_color(danger.opacity(0.3))
        .bg(danger.opacity(0.08))
        .child(div().text_sm().font_medium().text_color(fg).child(error.title.clone()))
        .children(
            error
                .hint
                .clone()
                .map(|hint| div().text_sm().text_color(fg.opacity(0.72)).child(hint)),
        )
        .when(on_review_key.is_some() || error.details.is_some(), |block| {
            block.child(
                h_flex()
                    .mt_1()
                    .gap_2()
                    .items_center()
                    .children(on_review_key.map(|review| {
                        Button::new((id, 0usize))
                            .label(t("connect.trust.review"))
                            .outline()
                            .small()
                            .on_click(move |_, window, cx| review(window, cx))
                    }))
                    .children(error.details.as_ref().map(|_| {
                        Button::new((id, 1usize))
                            .label(t("connect.details"))
                            .icon(Icon::new(if details_open {
                                IconName::ChevronDown
                            } else {
                                IconName::ChevronRight
                            }))
                            .ghost()
                            .xsmall()
                            .on_click(move |_, window, cx| on_toggle_details(window, cx))
                    })),
            )
        })
        .when_some(error.details.clone().filter(|_| details_open), |block, details| {
            block.child(
                div()
                    .text_xs()
                    .font_family(cx.theme().mono_font_family.clone())
                    .text_color(fg.opacity(0.72))
                    .child(details),
            )
        })
        .into_any_element()
}

// --- host key trust ---

enum Scan {
    Loading,
    Ready(Vec<HostKey>),
    Failed(FormError),
}

/// Body of the trust dialog: scans the host's keys, shows their
/// fingerprints, and on "Trust and connect" writes them to known_hosts.
pub struct HostKeyPrompt {
    host: String,
    port: u16,
    scan: Scan,
    trusting: bool,
    error: Option<FormError>,
    details_open: bool,
    on_trusted: OnTrusted,
}

/// Open the trust dialog for an ssh server whose host key isn't known yet.
/// `on_trusted` runs after the key landed in `~/.ssh/known_hosts`, with the
/// dialog already closed: retry the connection there. A *changed* key never
/// comes here; that's a blocking error (`Class::HostKeyChanged`).
pub fn open_host_key_dialog(
    host: String,
    port: u16,
    on_trusted: OnTrusted,
    window: &mut Window,
    cx: &mut App,
) {
    let prompt = cx.new(|cx| HostKeyPrompt::new(host, port, on_trusted, cx));
    window.open_dialog(cx, move |dialog, _, _| {
        dialog
            .title(t("connect.trust.title"))
            .w(px(520.))
            .child(prompt.clone())
    });
}

impl HostKeyPrompt {
    fn new(host: String, port: u16, on_trusted: OnTrusted, cx: &mut Context<Self>) -> Self {
        let task = {
            let host = host.clone();
            cx.global::<AppState>()
                .runtime
                .spawn(async move { sftp::scan_host_keys(&host, port).await })
        };
        cx.spawn(async move |this, cx| {
            let result = task
                .await
                .unwrap_or_else(|join| Err(anyhow::anyhow!("host key scan failed: {join}")));
            this.update(cx, |this, cx| {
                this.scan = match result {
                    Ok(keys) => Scan::Ready(keys),
                    Err(error) => Scan::Failed(FormError::from_error(&error, None)),
                };
                cx.notify();
            })
            .ok();
        })
        .detach();
        Self {
            host,
            port,
            scan: Scan::Loading,
            trusting: false,
            error: None,
            details_open: false,
            on_trusted,
        }
    }

    fn trust(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Scan::Ready(keys) = &self.scan else { return };
        if self.trusting {
            return;
        }
        let (keys, host, port) = (keys.clone(), self.host.clone(), self.port);
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state
            .runtime
            .spawn(async move { sftp::trust_host_keys(&pool, &host, port, &keys).await });
        self.trusting = true;
        self.error = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = task
                .await
                .unwrap_or_else(|join| Err(anyhow::anyhow!("saving the host key failed: {join}")));
            this.update_in(cx, |this, window, cx| {
                this.trusting = false;
                match result {
                    Ok(()) => {
                        window.close_dialog(cx);
                        (this.on_trusted.clone())(window, cx);
                    }
                    Err(error) => this.error = Some(FormError::from_error(&error, None)),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// "ssh-ed25519" → "ED25519", "ecdsa-sha2-nistp256" → "ECDSA".
    fn key_label(key_type: &str) -> String {
        let name = key_type.trim_start_matches("ssh-");
        let name = name.split('-').next().unwrap_or(name);
        name.to_uppercase()
    }
}

impl Render for HostKeyPrompt {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;
        let address = if self.port == 22 {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        };
        let body: AnyElement = match &self.scan {
            Scan::Loading => div()
                .text_sm()
                .text_color(muted)
                .child(t("connect.trust.scanning"))
                .into_any_element(),
            Scan::Ready(keys) => v_flex()
                .rounded_md()
                .border_1()
                .border_color(cx.theme().border)
                .children(keys.iter().enumerate().map(|(ix, key)| {
                    v_flex()
                        .px_3()
                        .py_2()
                        .gap_0p5()
                        .when(ix > 0, |row| row.border_t_1().border_color(cx.theme().border))
                        .child(
                            div()
                                .text_xs()
                                .text_color(muted)
                                .child(Self::key_label(&key.key_type)),
                        )
                        .child(
                            div()
                                .text_sm()
                                .font_family(cx.theme().mono_font_family.clone())
                                .child(key.fingerprint.clone()),
                        )
                }))
                .into_any_element(),
            Scan::Failed(error) => {
                let weak = cx.entity().downgrade();
                error_block(
                    "host-key-scan-error",
                    error,
                    self.details_open,
                    move |_, cx| {
                        weak.update(cx, |this, cx| {
                            this.details_open = !this.details_open;
                            cx.notify();
                        })
                        .ok();
                    },
                    None,
                    cx,
                )
            }
        };
        let ready = matches!(self.scan, Scan::Ready(_));
        v_flex()
            .gap_3()
            .child(
                div()
                    .text_sm()
                    .child(tf("connect.trust.intro", &[("host", &address)])),
            )
            .child(body)
            .children(self.error.clone().map(|error| {
                let weak = cx.entity().downgrade();
                error_block(
                    "host-key-trust-error",
                    &error,
                    self.details_open,
                    move |_, cx| {
                        weak.update(cx, |this, cx| {
                            this.details_open = !this.details_open;
                            cx.notify();
                        })
                        .ok();
                    },
                    None,
                    cx,
                )
            }))
            .child(
                h_flex()
                    .gap_2()
                    .justify_end()
                    .child(
                        Button::new("host-key-cancel")
                            .label(t("connect.cancel"))
                            .ghost()
                            .small()
                            .on_click(|_, window, cx| window.close_dialog(cx)),
                    )
                    .child(
                        Button::new("host-key-trust")
                            .label(t("connect.trust.confirm"))
                            .primary()
                            .small()
                            .loading(self.trusting)
                            .disabled(!ready || self.trusting)
                            .on_click(cx.listener(|this, _, window, cx| this.trust(window, cx))),
                    ),
            )
    }
}

// --- quick connect ---

pub enum QuickConnectEvent {
    /// A quick-connect site was created; open a tab on it.
    Connect(SiteProfile),
}

/// The quick-connect URL field plus a small prompt for missing secrets.
pub struct QuickConnectForm {
    url: Entity<InputState>,
    /// Parsed target waiting for the secrets in `secret_inputs`.
    pending: Option<QuickTarget>,
    secret_inputs: Vec<(&'static FieldSpec, Entity<InputState>)>,
    busy: bool,
    error: Option<FormError>,
    details_open: bool,
}

impl EventEmitter<QuickConnectEvent> for QuickConnectForm {}

impl gpui::Focusable for QuickConnectForm {
    fn focus_handle(&self, cx: &App) -> gpui::FocusHandle {
        self.url.focus_handle(cx)
    }
}

impl QuickConnectForm {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let url = cx.new(|cx| InputState::new(window, cx).placeholder(t("connect.quick.placeholder")));
        cx.subscribe_in(&url, window, |this, _, event: &InputEvent, window, cx| match event {
            InputEvent::PressEnter { .. } => this.submit(window, cx),
            InputEvent::Change => {
                // Editing the URL drops the secret prompt for the old one.
                this.pending = None;
                this.secret_inputs.clear();
                this.error = None;
                cx.notify();
            }
            _ => {}
        })
        .detach();
        Self {
            url,
            pending: None,
            secret_inputs: Vec::new(),
            busy: false,
            error: None,
            details_open: false,
        }
    }

    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        let focus = self.url.focus_handle(cx);
        window.focus(&focus, cx);
    }

    /// Pre-fill the field (a URL typed into the command bar) and connect.
    pub fn connect_to(&mut self, url: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.url
            .update(cx, |input, cx| input.set_value(url.to_string(), window, cx));
        self.submit(window, cx);
    }

    fn submit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let target = match &self.pending {
            Some(target) => target.clone(),
            None => match parse_quick_url(&self.url.read(cx).value()) {
                Ok(target) => target,
                Err(error) => {
                    self.error = Some(FormError::plain(error.message()));
                    cx.notify();
                    return;
                }
            },
        };
        // First Enter on a URL that lacks a secret: ask for it.
        if self.pending.is_none() {
            let fields = quick_prompt_fields(&target);
            if !fields.is_empty() {
                self.secret_inputs = fields
                    .into_iter()
                    .map(|field| {
                        let input = cx.new(|cx| InputState::new(window, cx).masked(true));
                        cx.subscribe_in(&input, window, |this, _, event: &InputEvent, window, cx| {
                            if let InputEvent::PressEnter { .. } = event {
                                this.submit(window, cx);
                            }
                        })
                        .detach();
                        (field, input)
                    })
                    .collect();
                if let Some((_, first)) = self.secret_inputs.first() {
                    let focus = first.focus_handle(cx);
                    window.focus(&focus, cx);
                }
                self.pending = Some(target);
                self.error = None;
                cx.notify();
                return;
            }
        }

        // Secrets are passed exactly as typed: never trimmed.
        let mut secrets = target.secrets.clone();
        for (field, input) in &self.secret_inputs {
            let value = input.read(cx).value().to_string();
            if !value.is_empty() {
                secrets.insert(field.key.to_string(), value);
            }
        }
        let pool = cx.global::<AppState>().db.clone();
        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { create_quick_site(pool, target, secrets).await });
        self.busy = true;
        self.error = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = task
                .await
                .unwrap_or_else(|join| Err(anyhow::anyhow!("quick connect failed: {join}")));
            this.update_in(cx, |this, window, cx| {
                this.busy = false;
                match result {
                    Ok((profile, sites)) => {
                        cx.global_mut::<AppState>().sites = sites;
                        this.pending = None;
                        this.secret_inputs.clear();
                        this.url
                            .update(cx, |input, cx| input.set_value("", window, cx));
                        cx.emit(QuickConnectEvent::Connect(profile));
                    }
                    Err(error) => this.error = Some(FormError::from_error(&error, None)),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }
}

impl Render for QuickConnectForm {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;
        let is_s3 = self.pending.as_ref().is_some_and(|target| target.scheme == "s3");
        v_flex()
            .w_full()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(div().flex_1().child(Input::new(&self.url)))
                    .child(
                        Button::new("quick-connect-go")
                            .label(t("connect.quick.connect"))
                            .primary()
                            .loading(self.busy)
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _, window, cx| this.submit(window, cx))),
                    ),
            )
            .children(self.secret_inputs.iter().map(|(field, input)| {
                v_flex()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .font_medium()
                            .text_color(muted)
                            .child(field.label),
                    )
                    .child(Input::new(input))
            }))
            .when(is_s3, |el| {
                el.child(div().text_xs().text_color(muted).child(t("connect.quick.s3_hint")))
            })
            .children(self.error.clone().map(|error| {
                let weak = cx.entity().downgrade();
                error_block(
                    "quick-connect-error",
                    &error,
                    self.details_open,
                    move |_, cx| {
                        weak.update(cx, |this, cx| {
                            this.details_open = !this.details_open;
                            cx.notify();
                        })
                        .ok();
                    },
                    None,
                    cx,
                )
            }))
    }
}

/// Persist a quick-connect target as a hidden site (keychain for secrets)
/// and return it with the fresh site list.
async fn create_quick_site(
    pool: SqlitePool,
    target: QuickTarget,
    secrets: HashMap<String, String>,
) -> Result<(SiteProfile, Vec<SiteProfile>)> {
    let meta = db::BookmarkMeta {
        space_id: QUICK_CONNECT_SPACE,
        ..Default::default()
    };
    let id = db::insert_site(
        &pool,
        &target.name,
        target.scheme,
        &target.options,
        &LayerConfig::default(),
        &meta,
    )
    .await?;
    if !secrets.is_empty() {
        let handle = SecretStore::handle_for_site(id);
        SecretStore::store(&handle, &secrets).context("saving the password to the keychain")?;
        db::set_site_secret_handle(&pool, id, Some(&handle)).await?;
    }
    let sites = db::list_sites(&pool).await?;
    let profile = sites
        .iter()
        .find(|site| site.id == id)
        .cloned()
        .context("the new site vanished")?;
    Ok((profile, sites))
}

/// Drop quick-connect sites nothing needs anymore: no open tab shows them
/// and no unfinished transfer uses them. Runs once at startup; returns the
/// removed ids.
pub async fn sweep_quick_sites(pool: &SqlitePool) -> Result<Vec<i64>> {
    let rows: Vec<(i64, Option<String>)> = sqlx::query_as(
        "SELECT id, secret_handle FROM sites WHERE space_id = ? AND id != 0 \
           AND id NOT IN (SELECT a_site_id FROM open_tabs WHERE a_site_id IS NOT NULL) \
           AND id NOT IN (SELECT b_site_id FROM open_tabs WHERE b_site_id IS NOT NULL) \
           AND id NOT IN (SELECT src_site_id FROM transfer_jobs \
                          WHERE state NOT IN ('done', 'cancelled')) \
           AND id NOT IN (SELECT dst_site_id FROM transfer_jobs \
                          WHERE state NOT IN ('done', 'cancelled'))",
    )
    .bind(QUICK_CONNECT_SPACE)
    .fetch_all(pool)
    .await?;
    for (id, handle) in &rows {
        if let Some(handle) = handle {
            if let Err(error) = SecretStore::delete(handle) {
                tracing::warn!(%error, site = id, "dropping quick-connect secret failed");
            }
        }
        db::delete_site(pool, *id).await?;
    }
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}
