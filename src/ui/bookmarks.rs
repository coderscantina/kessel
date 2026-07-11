//! Bookmark manager: a list of saved sites plus an add/edit form whose fields
//! come from the driver registry (`src/bookmarks.rs`). Secrets go straight to
//! the OS keychain; the DB row only ever sees non-secret options.
//!
//! S3 supports two credential sources: directly entered access keys
//! (keychain-backed) or a named profile from `~/.aws`, stored as the
//! `aws_profile` option and resolved fresh on every connect.

use std::collections::HashMap;
use std::rc::Rc;

use anyhow::{Context as _, Result};
use gpui::prelude::*;
use gpui::{div, AnyElement, Context, Entity, EventEmitter, Hsla, SharedString, Window};
use gpui_component::button::{Button, ButtonVariants as _};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::popover::Popover;
use gpui_component::select::{SearchableVec, Select, SelectEvent, SelectState};
use gpui_component::{
    h_flex, v_flex, ActiveTheme as _, Disableable as _, Icon, IconName, IndexPath, Sizable as _,
    StyledExt as _,
};
use sqlx::SqlitePool;

use crate::app::AppState;
use crate::aws;
use crate::bookmarks::{
    driver_by_scheme, is_oauth_scheme, secret_keys, DriverSpec, FieldSpec, AWS_PROFILE_OPT,
    DRIVERS,
};
use crate::i18n::t;
use crate::db;
use crate::provider::{LayerConfig, Provider, SiteProfile};
use crate::secrets::SecretStore;
use crate::ui::pickers;

const CRED_MODE_KEYS: &str = "Access keys";
const CRED_MODE_PROFILE: &str = "AWS profile (~/.aws)";

pub enum BookmarkEvent {
    /// User hit "Connect" on a bookmark.
    Connect(SiteProfile),
    /// Bookmarks were added / edited / removed; `AppState.sites` is updated.
    SitesChanged,
}

type StringSelect = Entity<SelectState<SearchableVec<SharedString>>>;

struct Editor {
    site_id: Option<i64>,
    had_secret: bool,
    /// Scheme the site had when the editor opened; a mismatch with `scheme`
    /// at save time means the old keychain entry is stale and gets dropped.
    original_scheme: Option<String>,
    name: Entity<InputState>,
    driver: StringSelect,
    scheme: &'static str,
    inputs: Vec<(&'static FieldSpec, Entity<InputState>)>,
    // S3 credential source: direct keys or an ~/.aws profile.
    cred_mode: StringSelect,
    use_profile: bool,
    profile: StringSelect,
    profile_count: usize,
    /// SSO profiles from `~/.aws`, mapped to `(start_url, region)`. Non-empty
    /// entries gate the interactive "Sign in with AWS SSO" button and feed the
    /// device-authorization flow when it's clicked.
    sso_profiles: HashMap<String, (String, String)>,
    sso_busy: bool,
    // Sidebar presentation: icon / color / tags / folder.
    icon: Option<String>,
    color: Option<String>,
    tags: Entity<InputState>,
    folder: StringSelect,
    /// Folder id per `folder` select index (index 0 = "No folder").
    folder_ids: Vec<Option<i64>>,
    /// Refresh token freshly obtained via the browser flow this session
    /// (cloud-drive schemes); persisted to the keychain on save.
    oauth_token: Option<String>,
    oauth_busy: bool,
    /// Whether the (optional) appearance & organization block is expanded.
    /// Collapsed by default for a new connection so the form is just
    /// "pick a type → fill the connection"; expanded when editing a site.
    appearance_expanded: bool,
}

pub struct BookmarkManager {
    sites: Vec<SiteProfile>,
    editor: Option<Editor>,
    error: Option<SharedString>,
    /// Success feedback ("Connection OK").
    notice: Option<SharedString>,
    /// Site armed for deletion; the second click actually deletes.
    confirm_delete: Option<i64>,
    busy: bool,
}

/// Validated form values, shared by save and test-connection.
struct FormData {
    name: String,
    scheme: String,
    options: HashMap<String, String>,
    secrets: HashMap<String, String>,
    profile_mode: bool,
    meta: db::BookmarkMeta,
}

impl EventEmitter<BookmarkEvent> for BookmarkManager {}

impl BookmarkManager {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            sites: cx.global::<AppState>().sites.clone(),
            editor: None,
            error: None,
            notice: None,
            confirm_delete: None,
            busy: false,
        }
    }

    /// Jump straight into a blank "new connection" form (used by the
    /// full-pane connection tab).
    pub fn open_new(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_editor(None, window, cx);
    }

    /// Open the editor for one saved site (sidebar context menu).
    pub fn open_edit(&mut self, site_id: i64, window: &mut Window, cx: &mut Context<Self>) {
        self.sites = cx.global::<AppState>().sites.clone();
        let site = self.sites.iter().find(|site| site.id == site_id).cloned();
        self.open_editor(site.as_ref(), window, cx);
    }

    /// Called every time the dialog opens: back to the list, fresh data.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.sites = cx.global::<AppState>().sites.clone();
        self.editor = None;
        self.error = None;
        self.notice = None;
        self.confirm_delete = None;
        self.busy = false;
        cx.notify();
    }

    fn open_editor(
        &mut self,
        site: Option<&SiteProfile>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let spec = site
            .and_then(|site| driver_by_scheme(&site.scheme))
            .unwrap_or(&DRIVERS[0]);
        let options = site.map(|site| site.options.clone()).unwrap_or_default();
        let stored_profile = options.get(AWS_PROFILE_OPT).cloned();
        let had_secret = site.is_some_and(|site| site.secret_handle.is_some());

        let name = cx.new(|cx| {
            InputState::new(window, cx).placeholder("Optional — derived from the connection")
        });
        if let Some(site) = site {
            name.update(cx, |input, cx| {
                input.set_value(site.name.clone(), window, cx)
            });
        }

        let driver_ix = DRIVERS
            .iter()
            .position(|driver| driver.scheme == spec.scheme)
            .unwrap_or(0);
        let driver_items: Vec<SharedString> =
            DRIVERS.iter().map(|driver| driver.label.into()).collect();
        let driver = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(driver_items),
                Some(IndexPath::new(driver_ix)),
                window,
                cx,
            )
        });
        cx.subscribe_in(&driver, window, Self::on_driver_selected).detach();

        let cred_items: Vec<SharedString> =
            vec![CRED_MODE_KEYS.into(), CRED_MODE_PROFILE.into()];
        let use_profile = stored_profile.is_some();
        let cred_mode = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(cred_items),
                Some(IndexPath::new(use_profile as usize)),
                window,
                cx,
            )
        });
        cx.subscribe_in(&cred_mode, window, Self::on_cred_mode_selected)
            .detach();

        let profiles: Vec<SharedString> =
            aws::list_profiles().into_iter().map(Into::into).collect();
        let profile_count = profiles.len();
        let profile_ix = stored_profile
            .and_then(|stored| profiles.iter().position(|p| p.as_ref() == stored))
            .map(IndexPath::new);
        let profile = cx.new(|cx| {
            SelectState::new(SearchableVec::new(profiles), profile_ix, window, cx)
                .searchable(true)
        });
        // The interactive-SSO button appears only for SSO profiles, so the form
        // must re-render when the picked profile changes.
        cx.subscribe_in(
            &profile,
            window,
            |_, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| cx.notify(),
        )
        .detach();

        // Presentation: icon / color / tags / folder.
        let icon = site.and_then(|site| site.icon.clone());
        let color = site.and_then(|site| site.color.clone());

        let tags = cx.new(|cx| InputState::new(window, cx).placeholder("prod, media, …"));
        if let Some(site) = site.filter(|site| !site.tags.is_empty()) {
            tags.update(cx, |input, cx| {
                input.set_value(site.tags.join(", "), window, cx)
            });
        }
        // The per-tag color chips below the input mirror its value live.
        cx.subscribe_in(&tags, window, |_, _, event: &InputEvent, _, cx| {
            if let InputEvent::Change { .. } = event {
                cx.notify();
            }
        })
        .detach();

        // Only the active space's folders — bookmarks are per space.
        let state = cx.global::<AppState>();
        let app_folders: Vec<crate::db::BookmarkFolder> = state
            .folders
            .iter()
            .filter(|folder| folder.space_id == state.active_space)
            .cloned()
            .collect();
        let folder_items: Vec<SharedString> = std::iter::once("No folder".into())
            .chain(app_folders.iter().map(|folder| folder.name.clone().into()))
            .collect();
        let folder_ids: Vec<Option<i64>> = std::iter::once(None)
            .chain(app_folders.iter().map(|folder| Some(folder.id)))
            .collect();
        let folder_ix = site
            .and_then(|site| site.folder_id)
            .and_then(|id| folder_ids.iter().position(|f| *f == Some(id)))
            .unwrap_or(0);
        let folder = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(folder_items),
                Some(IndexPath::new(folder_ix)),
                window,
                cx,
            )
        });

        let mut editor = Editor {
            site_id: site.map(|site| site.id),
            had_secret,
            original_scheme: site.map(|site| site.scheme.clone()),
            name,
            driver,
            scheme: spec.scheme,
            inputs: Vec::new(),
            cred_mode,
            use_profile,
            profile,
            profile_count,
            sso_profiles: aws::sso_profiles(),
            sso_busy: false,
            icon,
            color,
            tags,
            folder,
            folder_ids,
            oauth_token: None,
            oauth_busy: false,
            appearance_expanded: site.is_some(),
        };
        editor.inputs = Self::build_inputs(spec, &options, had_secret, window, cx);
        self.editor = Some(editor);
        self.error = None;
        self.notice = None;
        self.confirm_delete = None;
        cx.notify();
    }

    fn build_inputs(
        spec: &'static DriverSpec,
        existing: &HashMap<String, String>,
        had_secret: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<(&'static FieldSpec, Entity<InputState>)> {
        spec.fields
            .iter()
            .map(|field| {
                let placeholder = if field.secret && had_secret {
                    "•••••• (leave blank to keep)"
                } else {
                    field.placeholder
                };
                let input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder(placeholder);
                    if field.secret {
                        state = state.masked(true);
                    }
                    state
                });
                if let Some(value) = existing.get(field.key) {
                    input.update(cx, |input, cx| {
                        input.set_value(value.clone(), window, cx)
                    });
                }
                (field, input)
            })
            .collect()
    }

    fn on_driver_selected(
        &mut self,
        _: &StringSelect,
        event: &SelectEvent<SearchableVec<SharedString>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let SelectEvent::Confirm(Some(label)) = event else {
            return;
        };
        let Some(spec) = DRIVERS.iter().find(|driver| driver.label == label.as_ref()) else {
            return;
        };
        let Some(editor) = &self.editor else { return };
        if editor.scheme == spec.scheme {
            return;
        }
        // Stored secrets are only kept while the scheme stays the original
        // one; on a scheme change they'll be dropped at save time, so don't
        // show the "leave blank to keep" placeholder.
        let keeps_secret = editor.had_secret
            && editor.original_scheme.as_deref() == Some(spec.scheme);
        let inputs = Self::build_inputs(spec, &HashMap::new(), keeps_secret, window, cx);
        if let Some(editor) = &mut self.editor {
            editor.scheme = spec.scheme;
            editor.inputs = inputs;
            editor.oauth_token = None;
        }
        cx.notify();
    }

    fn on_cred_mode_selected(
        &mut self,
        _: &StringSelect,
        event: &SelectEvent<SearchableVec<SharedString>>,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let SelectEvent::Confirm(Some(label)) = event else {
            return;
        };
        if let Some(editor) = &mut self.editor {
            editor.use_profile = label.as_ref() == CRED_MODE_PROFILE;
            cx.notify();
        }
    }

    fn toggle_appearance(&mut self, cx: &mut Context<Self>) {
        if let Some(editor) = &mut self.editor {
            editor.appearance_expanded = !editor.appearance_expanded;
            cx.notify();
        }
    }

    /// Read and validate the editor form. `Err` is a user-facing message.
    fn collect(&self, cx: &Context<Self>) -> Result<FormData, SharedString> {
        let Some(editor) = &self.editor else {
            return Err("No editor open".into());
        };

        let profile_mode = editor.scheme == "s3" && editor.use_profile;
        let mut options = HashMap::new();
        let mut secrets = HashMap::new();
        for (field, input) in &editor.inputs {
            if field.secret && profile_mode {
                continue;
            }
            let value = input.read(cx).value().trim().to_string();
            if value.is_empty() {
                if field.required {
                    return Err(format!("{} is required", field.label).into());
                }
                continue;
            }
            if field.secret {
                secrets.insert(field.key.to_string(), value);
            } else {
                options.insert(field.key.to_string(), value);
            }
        }
        if profile_mode {
            let Some(profile) = editor.profile.read(cx).selected_value().cloned() else {
                return Err("Choose an AWS profile".into());
            };
            options.insert(AWS_PROFILE_OPT.to_string(), profile.to_string());
        }

        // Cloud drives: the refresh token from the browser flow is the real
        // credential; a bookmark without one (fresh or stored) can't connect.
        if is_oauth_scheme(editor.scheme) {
            match &editor.oauth_token {
                Some(token) => {
                    secrets.insert("refresh_token".to_string(), token.clone());
                }
                None if !Self::keeps_stored_secret(editor) => {
                    return Err(t("oauth.needed"));
                }
                None => {}
            }
        }

        // Names are optional: an empty field derives one from the connection.
        let mut name = editor.name.read(cx).value().trim().to_string();
        if name.is_empty() {
            name = derive_name(editor.scheme, &options);
        }

        let icon = editor.icon.clone();
        let color = editor.color.clone();
        // Tags are normalized to lowercase everywhere (rows in the `tags`
        // table are lowercase-unique).
        let tags: Vec<String> = editor
            .tags
            .read(cx)
            .value()
            .split(',')
            .map(|tag| tag.trim().to_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect();
        let folder_id = editor
            .folder
            .read(cx)
            .selected_index(cx)
            .and_then(|ix| editor.folder_ids.get(ix.row).copied())
            .flatten();

        Ok(FormData {
            name,
            scheme: editor.scheme.to_string(),
            options,
            secrets,
            profile_mode,
            meta: db::BookmarkMeta {
                icon,
                color,
                tags,
                folder_id,
                // Only used on insert; edits never move a bookmark's space.
                space_id: cx.global::<AppState>().active_space,
            },
        })
    }

    /// Whether the keychain entry from before this edit still applies: it
    /// exists and the scheme hasn't changed under it.
    fn keeps_stored_secret(editor: &Editor) -> bool {
        editor.had_secret && editor.original_scheme.as_deref() == Some(editor.scheme)
    }

    /// Persist the bookmark. When `connect` is set (the "Connect" action on a
    /// new-connection tab), the freshly saved site is opened right after — the
    /// tab turns into a live two-pane browser instead of dropping back to the
    /// bookmark list.
    fn save(&mut self, connect: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let form = match self.collect(cx) {
            Ok(form) => form,
            Err(message) => {
                self.error = Some(message);
                cx.notify();
                return;
            }
        };
        let editor = self.editor.as_ref().expect("collect ensured editor");
        let site_id = editor.site_id;
        let had_secret = editor.had_secret;
        let keep_stored = Self::keeps_stored_secret(editor);

        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state
            .runtime
            .spawn(async move { save_site(pool, site_id, form, had_secret, keep_stored).await });
        self.finish_with(task, true, connect, window, cx);
    }

    /// Build the layered operator with the form's current values (stored
    /// secrets filled in for blank fields) and probe the backend, without
    /// persisting anything.
    fn test_connection(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let form = match self.collect(cx) {
            Ok(form) => form,
            Err(message) => {
                self.error = Some(message);
                cx.notify();
                return;
            }
        };
        let editor = self.editor.as_ref().expect("collect ensured editor");
        let stored_handle = editor
            .site_id
            .filter(|_| Self::keeps_stored_secret(editor))
            .map(SecretStore::handle_for_site);

        let state = cx.global::<AppState>();
        let defaults = state.config.layers.clone();
        let task = state
            .runtime
            .spawn(async move { test_site(form, stored_handle, defaults).await });

        self.busy = true;
        self.error = None;
        self.notice = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("connection test failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                this.busy = false;
                match result {
                    Ok(()) => this.notice = Some("Connection OK".into()),
                    Err(error) => this.error = Some(format!("{error:#}").into()),
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Run the browser OAuth flow for the editor's cloud-drive scheme and
    /// park the resulting refresh token on the editor until save.
    fn oauth_sign_in(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(editor) = &self.editor else { return };
        if editor.oauth_busy || self.busy {
            return;
        }
        let Some(provider) = crate::oauth::CloudProvider::from_scheme(editor.scheme) else {
            return;
        };
        let mut client_id = String::new();
        let mut client_secret = String::new();
        for (field, input) in &editor.inputs {
            let value = input.read(cx).value().trim().to_string();
            match field.key {
                "client_id" => client_id = value,
                "client_secret" => client_secret = value,
                _ => {}
            }
        }
        if client_id.is_empty() {
            self.error = Some("Enter the OAuth client ID first".into());
            cx.notify();
            return;
        }
        // A blank secret while editing means "use the stored one".
        let stored_handle = editor
            .site_id
            .filter(|_| Self::keeps_stored_secret(editor))
            .map(SecretStore::handle_for_site);

        let task = cx.global::<AppState>().runtime.spawn(async move {
            let mut secret = client_secret;
            if secret.is_empty() {
                if let Some(handle) = stored_handle {
                    if let Some(stored) = SecretStore::load(&handle)
                        .ok()
                        .and_then(|mut secrets| secrets.remove("client_secret"))
                    {
                        secret = stored;
                    }
                }
            }
            crate::oauth::authorize(
                provider,
                &client_id,
                (!secret.is_empty()).then_some(secret.as_str()),
            )
            .await
        });

        if let Some(editor) = &mut self.editor {
            editor.oauth_busy = true;
        }
        self.error = None;
        self.notice = Some(t("oauth.waiting"));
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("sign-in task failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                if let Some(editor) = &mut this.editor {
                    editor.oauth_busy = false;
                }
                match result {
                    Ok(token) => {
                        if let Some(editor) = &mut this.editor {
                            editor.oauth_token = Some(token);
                        }
                        this.notice = Some(t("oauth.signed_in_new"));
                        this.error = None;
                    }
                    Err(error) => {
                        this.error = Some(format!("{error:#}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Run the interactive AWS IAM Identity Center device-authorization flow
    /// for the selected profile. Unlike the OAuth flow this parks no secret on
    /// the editor: success just means `~/.aws/sso/cache` now holds a valid
    /// token, which `aws::profile_options` resolves at connect time.
    fn sso_sign_in(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(editor) = &self.editor else { return };
        if editor.sso_busy || self.busy {
            return;
        }
        let Some(profile) = editor.profile.read(cx).selected_value().cloned() else {
            self.error = Some("Choose an AWS profile first".into());
            cx.notify();
            return;
        };
        let Some((start_url, region)) = editor.sso_profiles.get(profile.as_ref()).cloned() else {
            self.error = Some("The selected profile does not use AWS SSO".into());
            cx.notify();
            return;
        };

        let task = cx
            .global::<AppState>()
            .runtime
            .spawn(async move { crate::sso::sso_login(&start_url, &region).await });

        if let Some(editor) = &mut self.editor {
            editor.sso_busy = true;
        }
        self.error = None;
        self.notice = Some(t("sso.waiting"));
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("SSO sign-in task failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                if let Some(editor) = &mut this.editor {
                    editor.sso_busy = false;
                }
                match result {
                    Ok(()) => {
                        this.notice = Some(t("sso.signed_in"));
                        this.error = None;
                    }
                    Err(error) => {
                        this.error = Some(format!("{error:#}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    /// Persist a tag's accent color (row is created if the tag is new) and
    /// mirror it into the in-memory tag list.
    fn set_tag_color(&mut self, tag: String, color: Option<String>, cx: &mut Context<Self>) {
        let state = cx.global_mut::<AppState>();
        match state.tags.iter_mut().find(|known| known.name == tag) {
            Some(known) => known.color = color.clone(),
            None => state.tags.push(db::Tag {
                id: 0,
                name: tag.clone(),
                color: color.clone(),
            }),
        }
        let pool = state.db.clone();
        state.runtime.spawn(async move {
            if let Err(error) = db::set_tag_color(&pool, &tag, color.as_deref()).await {
                tracing::error!(%error, "saving tag color failed");
            }
        });
        cx.notify();
    }

    fn delete(&mut self, site_id: i64, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.confirm_delete = None;
        let state = cx.global::<AppState>();
        let pool = state.db.clone();
        let task = state.runtime.spawn(async move {
            let _ = SecretStore::delete(&SecretStore::handle_for_site(site_id));
            db::delete_site(&pool, site_id).await?;
            db::list_sites(&pool).await
        });
        self.finish_with(task, false, false, window, cx);
    }

    /// Await a DB task that returns the fresh site list, then sync it into
    /// the global state and this view. `close_editor` on success.
    fn finish_with(
        &mut self,
        task: tokio::task::JoinHandle<Result<Vec<SiteProfile>>>,
        close_editor: bool,
        connect: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.busy = true;
        self.error = None;
        self.notice = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let result = match task.await {
                Ok(result) => result,
                Err(join_error) => Err(anyhow::anyhow!("db task failed: {join_error}")),
            };
            this.update(cx, |this, cx| {
                this.busy = false;
                match result {
                    Ok(sites) => {
                        // The just-saved site: the id present now but not in
                        // the manager's pre-save snapshot. Used to connect
                        // straight after a "Connect" save.
                        let added = connect
                            .then(|| {
                                let before: std::collections::HashSet<i64> =
                                    this.sites.iter().map(|site| site.id).collect();
                                sites.iter().find(|site| !before.contains(&site.id)).cloned()
                            })
                            .flatten();
                        cx.global_mut::<AppState>().sites = sites.clone();
                        this.sites = sites;
                        if close_editor {
                            this.editor = None;
                        }
                        cx.emit(BookmarkEvent::SitesChanged);
                        if let Some(profile) = added {
                            cx.emit(BookmarkEvent::Connect(profile));
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "bookmark save/delete failed");
                        this.error = Some(format!("{error:#}").into());
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn render_list(&self, cx: &Context<Self>) -> AnyElement {
        let muted = cx.theme().muted_foreground;
        let rows: Vec<AnyElement> = self
            .sites
            .iter()
            .enumerate()
            .map(|(ix, site)| {
                let driver_label = driver_by_scheme(&site.scheme)
                    .map(|spec| spec.label)
                    .unwrap_or(site.scheme.as_str());
                let site_for_connect = site.clone();
                let site_id = site.id;
                let edit_ix = ix;
                let armed = self.confirm_delete == Some(site_id);
                h_flex()
                    .gap_2()
                    .px_2()
                    .py_1()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        v_flex()
                            .flex_1()
                            .overflow_hidden()
                            .child(div().truncate().child(site.name.clone()))
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(muted)
                                    .child(driver_label.to_string()),
                            ),
                    )
                    .child(
                        Button::new(("connect", ix))
                            .label("Connect")
                            .primary()
                            .small()
                            .on_click(cx.listener(move |_, _, window, cx| {
                                cx.emit(BookmarkEvent::Connect(site_for_connect.clone()));
                                gpui_component::WindowExt::close_dialog(window, cx);
                            })),
                    )
                    .child(
                        Button::new(("edit", ix))
                            .icon(Icon::new(IconName::Settings2))
                            .ghost()
                            .small()
                            .on_click(cx.listener(move |this, _, window, cx| {
                                let site = this.sites.get(edit_ix).cloned();
                                this.open_editor(site.as_ref(), window, cx);
                            })),
                    )
                    .when(armed, |row| {
                        // Second, explicit click deletes the bookmark and its
                        // keychain secrets for good.
                        row.child(
                            Button::new(("confirm-delete", ix))
                                .label("Delete?")
                                .danger()
                                .small()
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.delete(site_id, window, cx);
                                })),
                        )
                        .child(
                            Button::new(("cancel-delete", ix))
                                .label("Keep")
                                .ghost()
                                .small()
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.confirm_delete = None;
                                    cx.notify();
                                })),
                        )
                    })
                    .when(!armed, |row| {
                        row.child(
                            Button::new(("delete", ix))
                                .icon(Icon::new(IconName::Delete))
                                .ghost()
                                .small()
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.confirm_delete = Some(site_id);
                                    cx.notify();
                                })),
                        )
                    })
                    .into_any_element()
            })
            .collect();

        v_flex()
            .gap_2()
            .when(rows.is_empty(), |this| {
                this.child(
                    div()
                        .text_sm()
                        .text_color(muted)
                        .child("No bookmarks yet — add one to connect to a remote."),
                )
            })
            .children(rows)
            .child(
                h_flex().justify_end().child(
                    Button::new("add-bookmark")
                        .label("Add bookmark")
                        .icon(Icon::new(IconName::Plus))
                        .small()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.open_editor(None, window, cx);
                        })),
                ),
            )
            .into_any_element()
    }

    fn render_editor(&self, cx: &Context<Self>) -> AnyElement {
        let Some(editor) = &self.editor else {
            return div().into_any_element();
        };
        let muted = cx.theme().muted_foreground;
        let profile_mode = editor.scheme == "s3" && editor.use_profile;

        // Type first: it decides which fields follow. Name comes after and is
        // optional (a blank name is derived from the connection at save time).
        let mut fields: Vec<AnyElement> = vec![
            section("CONNECTION", muted),
            labeled(
                "Type",
                muted,
                Select::new(&editor.driver).into_any_element(),
            ),
            labeled("Name", muted, Input::new(&editor.name).into_any_element()),
        ];
        for (field, input) in &editor.inputs {
            if field.secret {
                continue;
            }
            let label = if field.required {
                format!("{} *", field.label)
            } else {
                field.label.to_string()
            };
            fields.push(labeled(label, muted, Input::new(input).into_any_element()));
        }
        if editor.scheme == "s3" {
            fields.push(labeled(
                "Credentials",
                muted,
                Select::new(&editor.cred_mode).into_any_element(),
            ));
        }
        if profile_mode {
            if editor.profile_count == 0 {
                fields.push(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child("No profiles found in ~/.aws/credentials or ~/.aws/config.")
                        .into_any_element(),
                );
            } else {
                fields.push(labeled(
                    "AWS profile *",
                    muted,
                    Select::new(&editor.profile).into_any_element(),
                ));
                // SSO profiles get an interactive login so the temporary
                // session can be refreshed without leaving the app.
                let is_sso = editor
                    .profile
                    .read(cx)
                    .selected_value()
                    .is_some_and(|profile| editor.sso_profiles.contains_key(profile.as_ref()));
                if is_sso {
                    fields.push(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Button::new("sso-sign-in")
                                    .label(t("sso.sign_in"))
                                    .outline()
                                    .small()
                                    .loading(editor.sso_busy)
                                    .disabled(editor.sso_busy || self.busy)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.sso_sign_in(window, cx);
                                    })),
                            )
                            .child(div().text_xs().text_color(muted).child(t("sso.hint")))
                            .into_any_element(),
                    );
                }
            }
        } else {
            for (field, input) in &editor.inputs {
                if field.secret {
                    fields.push(labeled(
                        field.label.to_string(),
                        muted,
                        Input::new(input).into_any_element(),
                    ));
                }
            }
        }

        // Cloud drives: credentials come from the browser flow, not typing.
        if is_oauth_scheme(editor.scheme) {
            let (status, color) = if editor.oauth_token.is_some() {
                (t("oauth.signed_in_new"), cx.theme().success)
            } else if Self::keeps_stored_secret(editor) {
                (t("oauth.signed_in"), cx.theme().success)
            } else {
                (t("oauth.needed"), muted)
            };
            fields.push(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        Button::new("oauth-sign-in")
                            .label(t("oauth.sign_in"))
                            .outline()
                            .small()
                            .loading(editor.oauth_busy)
                            .disabled(editor.oauth_busy || self.busy)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.oauth_sign_in(window, cx);
                            })),
                    )
                    .child(div().text_xs().text_color(color).child(status))
                    .into_any_element(),
            );
            fields.push(
                div()
                    .text_xs()
                    .text_color(muted)
                    .child(t("oauth.redirect_hint"))
                    .into_any_element(),
            );
        }

        // Sidebar presentation: icon, color, tags, folder. Optional and
        // secondary, so it lives behind a disclosure — collapsed by default
        // for a new connection, expanded when editing a saved site.
        let expanded = editor.appearance_expanded;
        fields.push(
            h_flex()
                .id("appearance-disclosure")
                .mt_1()
                .gap_1()
                .items_center()
                .cursor_pointer()
                .text_xs()
                .text_color(muted)
                .on_click(cx.listener(|this, _, _, cx| this.toggle_appearance(cx)))
                .child(
                    Icon::new(if expanded {
                        IconName::ChevronDown
                    } else {
                        IconName::ChevronRight
                    })
                    .small(),
                )
                .child("APPEARANCE & ORGANIZATION")
                .into_any_element(),
        );
        if !expanded {
            // Nothing else in this section when collapsed.
            return self.editor_frame(fields, cx);
        }
        let weak_icon = cx.entity().downgrade();
        let weak_color = cx.entity().downgrade();
        fields.push(
            h_flex()
                .gap_2()
                .child(div().flex_1().child(labeled(
                    "Icon",
                    muted,
                    pickers::icon_picker(
                        "bookmark-icon",
                        editor.icon.clone(),
                        IconName::Globe,
                        Rc::new(move |value, _, cx| {
                            weak_icon
                                .update(cx, |this, cx| {
                                    if let Some(editor) = &mut this.editor {
                                        editor.icon = value;
                                    }
                                    cx.notify();
                                })
                                .ok();
                        }),
                    )
                    .into_any_element(),
                )))
                .child(div().flex_1().child(labeled(
                    "Color",
                    muted,
                    pickers::color_picker(
                        "bookmark-color",
                        editor.color.clone(),
                        Rc::new(move |value, _, cx| {
                            weak_color
                                .update(cx, |this, cx| {
                                    if let Some(editor) = &mut this.editor {
                                        editor.color = value;
                                    }
                                    cx.notify();
                                })
                                .ok();
                        }),
                    )
                    .into_any_element(),
                )))
                .into_any_element(),
        );
        fields.push(labeled(
            "Tags (comma-separated)",
            muted,
            Input::new(&editor.tags).into_any_element(),
        ));
        // Per-tag color: chips for the tags currently entered, each with a
        // swatch popover writing straight to the `tags` table.
        let entered: Vec<String> = editor
            .tags
            .read(cx)
            .value()
            .split(',')
            .map(|tag| tag.trim().to_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect();
        if !entered.is_empty() {
            let known = cx.global::<AppState>().tags.clone();
            fields.push(
                h_flex()
                    .flex_wrap()
                    .gap_1()
                    .children(entered.into_iter().enumerate().map(|(ix, tag)| {
                        let color_name = known
                            .iter()
                            .find(|known| known.name == tag)
                            .and_then(|known| known.color.clone());
                        let color = crate::ui::bookmark_color(color_name.as_deref());
                        let weak = cx.entity().downgrade();
                        let tag_for_pick = tag.clone();
                        Popover::new(("tag-color", ix))
                            .trigger(
                                Button::new(("tag-chip", ix))
                                    .label(tag.clone())
                                    .icon(match color {
                                        Some(color) => Icon::default()
                                            .path("icons/circle.svg")
                                            .text_color(color),
                                        None => Icon::default().path("icons/circle-off.svg"),
                                    })
                                    .outline()
                                    .xsmall(),
                            )
                            .content(move |_, _, cx| {
                                let popover = cx.entity();
                                let weak = weak.clone();
                                let tag = tag_for_pick.clone();
                                let pick: pickers::PickFn = Rc::new(move |value, window, cx| {
                                    let tag = tag.clone();
                                    weak.update(cx, |this, cx| {
                                        this.set_tag_color(tag, value, cx);
                                    })
                                    .ok();
                                    popover.update(cx, |state, cx| state.dismiss(window, cx));
                                });
                                div()
                                    .p_1()
                                    .child(pickers::color_grid(color_name.clone(), pick, cx))
                            })
                    }))
                    .into_any_element(),
            );
        }
        fields.push(labeled(
            "Folder",
            muted,
            Select::new(&editor.folder).into_any_element(),
        ));

        self.editor_frame(fields, cx)
    }

    /// Wrap the editor fields with the shared action row. A new connection
    /// offers "Save" (bookmark only) plus a primary "Connect" (save + open);
    /// editing a saved site keeps a single primary "Save".
    fn editor_frame(&self, fields: Vec<AnyElement>, cx: &Context<Self>) -> AnyElement {
        let is_edit = self
            .editor
            .as_ref()
            .and_then(|editor| editor.site_id)
            .is_some();
        let busy = self.busy;

        let mut actions = h_flex()
            .gap_2()
            .justify_end()
            .mt_2()
            .child(
                Button::new("test-connection")
                    .label("Test connection")
                    .outline()
                    .small()
                    .loading(busy)
                    .disabled(busy)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.test_connection(window, cx);
                    })),
            )
            .child(div().flex_1())
            .child(
                Button::new("cancel-edit")
                    .label("Cancel")
                    .ghost()
                    .small()
                    .disabled(busy)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.editor = None;
                        this.error = None;
                        this.notice = None;
                        cx.notify();
                    })),
            );
        if is_edit {
            actions = actions.child(
                Button::new("save-bookmark")
                    .label("Save")
                    .primary()
                    .small()
                    .disabled(busy)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save(false, window, cx);
                    })),
            );
        } else {
            actions = actions
                .child(
                    Button::new("save-bookmark")
                        .label("Save")
                        .outline()
                        .small()
                        .disabled(busy)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.save(false, window, cx);
                        })),
                )
                .child(
                    Button::new("save-and-connect")
                        .label("Connect")
                        .primary()
                        .small()
                        .disabled(busy)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.save(true, window, cx);
                        })),
                );
        }

        v_flex()
            .gap_3()
            .children(fields)
            .child(actions)
            .into_any_element()
    }
}

impl Render for BookmarkManager {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let body = if self.editor.is_some() {
            self.render_editor(cx)
        } else {
            self.render_list(cx)
        };
        v_flex()
            .gap_2()
            .child(body)
            .children(self.error.clone().map(|error| {
                div()
                    .text_sm()
                    .text_color(cx.theme().danger)
                    .child(error)
            }))
            .children(self.notice.clone().map(|notice| {
                div()
                    .text_sm()
                    .text_color(cx.theme().success)
                    .child(notice)
            }))
    }
}

fn labeled(label: impl Into<SharedString>, muted: Hsla, control: AnyElement) -> AnyElement {
    v_flex()
        .gap_1()
        .child(
            div()
                .text_xs()
                .font_medium()
                .text_color(muted)
                .child(label.into()),
        )
        .child(control)
        .into_any_element()
}

/// Small uppercase heading that opens a form section.
fn section(label: &'static str, muted: Hsla) -> AnyElement {
    div()
        .mt_2()
        .text_xs()
        .font_semibold()
        .text_color(muted)
        .child(label)
        .into_any_element()
}

/// Runs on the app tokio runtime: persist the row, sync keychain secrets,
/// return the fresh site list.
///
/// Keychain rules: profile mode owns no entry; a scheme change invalidates
/// the stored entry (`keep_stored` false); otherwise blank secret fields mean
/// "keep the stored value" and typed ones overwrite it.
async fn save_site(
    pool: SqlitePool,
    site_id: Option<i64>,
    form: FormData,
    had_secret: bool,
    keep_stored: bool,
) -> Result<Vec<SiteProfile>> {
    let id = match site_id {
        Some(id) => {
            db::update_site(&pool, id, &form.name, &form.scheme, &form.options, &form.meta)
                .await?;
            id
        }
        None => {
            db::insert_site(
                &pool,
                &form.name,
                &form.scheme,
                &form.options,
                &LayerConfig::default(),
                &form.meta,
            )
            .await?
        }
    };

    // Tags live as first-class rows too (lowercase, optional color).
    db::upsert_tags(&pool, &form.meta.tags).await?;

    let handle = SecretStore::handle_for_site(id);
    if form.profile_mode || (form.secrets.is_empty() && had_secret && !keep_stored) {
        // No secrets belong to this site anymore; drop a stale entry.
        if had_secret {
            let _ = SecretStore::delete(&handle);
            db::set_site_secret_handle(&pool, id, None).await?;
        }
    } else if !form.secrets.is_empty() {
        let mut merged = stored_secrets(&handle, &form.scheme, keep_stored);
        merged.extend(form.secrets);
        SecretStore::store(&handle, &merged)?;
        db::set_site_secret_handle(&pool, id, Some(&handle)).await?;
    }

    db::list_sites(&pool).await
}

/// A display name for a connection when the user left the field blank:
/// prefer the most identifying option the driver has (host / bucket /
/// container / endpoint / root), falling back to the driver label.
fn derive_name(scheme: &str, options: &HashMap<String, String>) -> String {
    for key in ["bucket", "container", "endpoint", "host", "root"] {
        if let Some(value) = options.get(key) {
            let value = value
                .trim()
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_start_matches("ssh://")
                .trim_start_matches("ftp://")
                .trim_end_matches('/');
            if !value.is_empty() && value != "/" {
                return value.to_string();
            }
        }
    }
    driver_by_scheme(scheme)
        .map(|spec| spec.label.to_string())
        .unwrap_or_else(|| scheme.to_string())
}

/// The stored keychain map, restricted to the secret keys the current scheme
/// actually declares (stale keys from an earlier scheme are dropped).
fn stored_secrets(handle: &str, scheme: &str, keep_stored: bool) -> HashMap<String, String> {
    if !keep_stored {
        return HashMap::new();
    }
    let allowed = secret_keys(scheme);
    SecretStore::load(handle)
        .unwrap_or_default()
        .into_iter()
        .filter(|(key, _)| allowed.contains(&key.as_str()))
        .collect()
}

/// Probe a backend with the form's values without persisting anything:
/// stored secrets fill blank fields, then a layered operator (with a bounded
/// timeout) lists the root.
async fn test_site(
    form: FormData,
    stored_handle: Option<String>,
    defaults: LayerConfig,
) -> Result<()> {
    let mut options = form.options;
    if let Some(handle) = stored_handle {
        for (key, value) in stored_secrets(&handle, &form.scheme, true) {
            options.entry(key).or_insert(value);
        }
    }
    // Typed secrets win over stored ones.
    options.extend(form.secrets);

    // Cap the probe so a dead endpoint reports instead of hanging.
    let defaults = LayerConfig {
        timeout_secs: Some(defaults.timeout_secs.unwrap_or(15).min(30)),
        retry: None,
        throttle: None,
        concurrent_limit: None,
    };
    let profile = SiteProfile {
        id: -1,
        name: form.name,
        scheme: form.scheme,
        options,
        ..Default::default()
    };
    let provider = Provider::connect(&profile, &defaults)?;
    provider.check().await.context("backend not reachable")?;
    Ok(())
}
