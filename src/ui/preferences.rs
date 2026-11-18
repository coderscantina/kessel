//! Preferences dialog: a Zed-style settings editor over the most-used
//! config.json5 values. A left category rail and a top search box filter a
//! single data-driven settings table (`SETTINGS`); each row shows the setting
//! name, a one-line description and its control.
//!
//! Every control writes straight back into the user's config file
//! (comment-preserving, see `config::save_values`); the hot-reload watcher
//! applies the change, so hand-edits and the dialog share one code path and
//! can't drift apart.

use gpui::prelude::*;
use gpui::{div, px, AnyElement, Context, Entity, SharedString, Window};
use gpui_component::button::Button;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::select::{SearchableVec, Select, SelectEvent, SelectState};
use gpui_component::slider::{Slider, SliderEvent, SliderState};
use gpui_component::switch::Switch;
use gpui_component::{h_flex, v_flex, ActiveTheme as _, IndexPath, Sizable as _, StyledExt as _};

use crate::app::AppState;
use crate::config::{self, fmt_str, KesselConfig};
use crate::i18n::{t, tf};

const MB: f32 = (1024 * 1024) as f32;

type StringSelect = Entity<SelectState<SearchableVec<SharedString>>>;

/// The category rail, top to bottom.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Category {
    General,
    Appearance,
    Transfers,
    Advanced,
}

impl Category {
    const ALL: [Category; 4] = [
        Category::General,
        Category::Appearance,
        Category::Transfers,
        Category::Advanced,
    ];

    fn label(self) -> SharedString {
        match self {
            Category::General => t("prefs.cat.general"),
            Category::Appearance => t("prefs.cat.appearance"),
            Category::Transfers => t("prefs.cat.transfers"),
            Category::Advanced => t("prefs.cat.advanced"),
        }
    }
}

/// Identifies which control a settings row renders.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SettingId {
    Language,
    Sidebar,
    ShowHidden,
    Theme,
    LightTheme,
    DarkTheme,
    IconTheme,
    MaxParallel,
    Bandwidth,
    Retries,
    Verify,
    PreserveTimestamps,
    SkipHidden,
    ConflictPolicy,
    LogFilter,
}

/// One entry in the settings catalog: which category it lives under, plus the
/// i18n keys for its name and one-line description. Adding a setting is one row
/// here plus its arm in [`PreferencesPanel::control`].
struct Setting {
    id: SettingId,
    category: Category,
    name: &'static str,
    desc: &'static str,
}

const SETTINGS: &[Setting] = &[
    Setting {
        id: SettingId::Language,
        category: Category::General,
        name: "prefs.language",
        desc: "prefs.language.desc",
    },
    Setting {
        id: SettingId::Sidebar,
        category: Category::General,
        name: "prefs.sidebar_position",
        desc: "prefs.sidebar_position.desc",
    },
    Setting {
        id: SettingId::ShowHidden,
        category: Category::General,
        name: "prefs.show_hidden",
        desc: "prefs.show_hidden.desc",
    },
    Setting {
        id: SettingId::Theme,
        category: Category::Appearance,
        name: "prefs.theme",
        desc: "prefs.theme.desc",
    },
    Setting {
        id: SettingId::LightTheme,
        category: Category::Appearance,
        name: "prefs.light_theme",
        desc: "prefs.light_theme.desc",
    },
    Setting {
        id: SettingId::DarkTheme,
        category: Category::Appearance,
        name: "prefs.dark_theme",
        desc: "prefs.dark_theme.desc",
    },
    Setting {
        id: SettingId::IconTheme,
        category: Category::Appearance,
        name: "prefs.icon_theme",
        desc: "prefs.icon_theme.desc",
    },
    Setting {
        id: SettingId::MaxParallel,
        category: Category::Transfers,
        name: "prefs.max_parallel",
        desc: "prefs.max_parallel.desc",
    },
    Setting {
        id: SettingId::Bandwidth,
        category: Category::Transfers,
        name: "prefs.bandwidth",
        desc: "prefs.bandwidth.desc",
    },
    Setting {
        id: SettingId::Retries,
        category: Category::Transfers,
        name: "prefs.retries",
        desc: "prefs.retries.desc",
    },
    Setting {
        id: SettingId::Verify,
        category: Category::Transfers,
        name: "prefs.verify",
        desc: "prefs.verify.desc",
    },
    Setting {
        id: SettingId::PreserveTimestamps,
        category: Category::Transfers,
        name: "prefs.preserve_timestamps",
        desc: "prefs.preserve_timestamps.desc",
    },
    Setting {
        id: SettingId::SkipHidden,
        category: Category::Transfers,
        name: "prefs.skip_hidden",
        desc: "prefs.skip_hidden.desc",
    },
    Setting {
        id: SettingId::ConflictPolicy,
        category: Category::Transfers,
        name: "prefs.conflict_policy",
        desc: "prefs.conflict_policy.desc",
    },
    Setting {
        id: SettingId::LogFilter,
        category: Category::Advanced,
        name: "prefs.log_filter",
        desc: "prefs.log_filter.desc",
    },
];

/// One dropdown backed by (config value, display label) pairs.
struct Choice {
    select: StringSelect,
    values: Vec<String>,
}

impl Choice {
    fn new(
        pairs: &[(String, SharedString)],
        current: &str,
        window: &mut Window,
        cx: &mut Context<PreferencesPanel>,
    ) -> Self {
        let items: Vec<SharedString> = pairs.iter().map(|(_, label)| label.clone()).collect();
        let values: Vec<String> = pairs.iter().map(|(value, _)| value.clone()).collect();
        let ix = values
            .iter()
            .position(|value| value == current)
            .unwrap_or(0);
        let select = cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(items),
                Some(IndexPath::new(ix)),
                window,
                cx,
            )
        });
        Self { select, values }
    }

    fn value(&self, cx: &Context<PreferencesPanel>) -> Option<String> {
        self.select
            .read(cx)
            .selected_index(cx)
            .and_then(|ix| self.values.get(ix.row).cloned())
    }
}

pub struct PreferencesPanel {
    search: Entity<InputState>,
    query: String,
    selected: Category,
    theme: Choice,
    light_theme: Choice,
    dark_theme: Choice,
    icon_theme: Choice,
    locale: Choice,
    sidebar: Choice,
    conflict_policy: Choice,
    parallel: Entity<SliderState>,
    bandwidth: Entity<SliderState>,
    retries: Entity<InputState>,
    log_filter: Entity<InputState>,
}

impl PreferencesPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let cfg = cx.global::<AppState>().config.clone();

        let search = cx.new(|cx| {
            InputState::new(window, cx).placeholder(t("prefs.search").to_string())
        });
        cx.subscribe_in(&search, window, |this, _, event: &InputEvent, _, cx| {
            if let InputEvent::Change = event {
                this.query = this.search.read(cx).value().trim().to_string();
                cx.notify();
            }
        })
        .detach();

        let theme = Choice::new(
            &[
                ("system".into(), t("prefs.theme.system")),
                ("light".into(), t("prefs.theme.light")),
                ("dark".into(), t("prefs.theme.dark")),
            ],
            &cfg.ui.theme,
            window,
            cx,
        );
        cx.subscribe_in(
            &theme.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("ui", "theme", &this.theme, cx);
            },
        )
        .detach();

        // Installed named themes for the light/dark pickers, with a "Default"
        // (built-in) option first (empty value → falls back to the built-in).
        let theme_pairs: Vec<(String, SharedString)> =
            std::iter::once(("".to_string(), t("prefs.theme.default")))
                .chain(
                    crate::theme::installed_theme_names(&cfg.ui)
                        .into_iter()
                        .map(|name| (name.to_string(), name)),
                )
                .collect();

        let light_theme = Choice::new(
            &theme_pairs,
            cfg.ui.light_theme.as_deref().unwrap_or(""),
            window,
            cx,
        );
        cx.subscribe_in(
            &light_theme.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("ui", "light_theme", &this.light_theme, cx);
            },
        )
        .detach();

        let dark_theme = Choice::new(
            &theme_pairs,
            cfg.ui.dark_theme.as_deref().unwrap_or(""),
            window,
            cx,
        );
        cx.subscribe_in(
            &dark_theme.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("ui", "dark_theme", &this.dark_theme, cx);
            },
        )
        .detach();

        let icon_pairs: Vec<(String, SharedString)> =
            std::iter::once(("".to_string(), t("prefs.theme.default")))
                .chain(
                    crate::ui::file_icons::installed_icon_theme_names()
                        .into_iter()
                        .map(|name| (name.clone(), SharedString::from(name))),
                )
                .collect();
        let icon_theme = Choice::new(
            &icon_pairs,
            cfg.ui.icon_theme.as_deref().unwrap_or(""),
            window,
            cx,
        );
        cx.subscribe_in(
            &icon_theme.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("ui", "icon_theme", &this.icon_theme, cx);
            },
        )
        .detach();

        let locales: Vec<(String, SharedString)> =
            std::iter::once(("system".to_string(), t("prefs.language.system")))
                .chain(
                    crate::i18n::available_locales()
                        .into_iter()
                        .map(|tag| (tag.clone(), SharedString::from(tag))),
                )
                .collect();
        let locale = Choice::new(&locales, &cfg.ui.locale, window, cx);
        cx.subscribe_in(
            &locale.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("ui", "locale", &this.locale, cx);
            },
        )
        .detach();

        let sidebar = Choice::new(
            &[
                ("left".into(), t("prefs.sidebar.left")),
                ("right".into(), t("prefs.sidebar.right")),
            ],
            &cfg.ui.sidebar_position,
            window,
            cx,
        );
        cx.subscribe_in(
            &sidebar.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("ui", "sidebar_position", &this.sidebar, cx);
            },
        )
        .detach();

        let conflict_policy = Choice::new(
            &[
                ("ask".into(), t("prefs.conflict_policy.ask")),
                ("replace".into(), t("transfers.conflict.replace")),
                ("skip".into(), t("transfers.conflict.skip")),
                ("keep_both".into(), t("transfers.conflict.keep_both")),
                ("replace_if_newer".into(), t("transfers.conflict.replace_newer")),
            ],
            cfg.transfers.default_conflict_policy.as_str(),
            window,
            cx,
        );
        cx.subscribe_in(
            &conflict_policy.select,
            window,
            |this, _, _: &SelectEvent<SearchableVec<SharedString>>, _, cx| {
                this.save_choice("transfers", "default_conflict_policy", &this.conflict_policy, cx);
            },
        )
        .detach();

        let parallel = cx.new(|_| {
            SliderState::new()
                .min(1.)
                .max(16.)
                .step(1.)
                .default_value(cfg.transfers.max_parallel as f32)
        });
        cx.subscribe(&parallel, |_, _, event: &SliderEvent, cx| {
            let (SliderEvent::Change(value) | SliderEvent::Release(value)) = event;
            save(
                cx,
                "transfers",
                "max_parallel",
                format!("{}", value.start() as usize),
            );
            // Refresh the inline value label next to the slider.
            cx.notify();
        })
        .detach();

        let bandwidth = cx.new(|_| {
            SliderState::new()
                .min(0.)
                .max(200.)
                .step(1.)
                .default_value(cfg.transfers.bandwidth_limit as f32 / MB)
        });
        cx.subscribe(&bandwidth, |_, _, event: &SliderEvent, cx| {
            let (SliderEvent::Change(value) | SliderEvent::Release(value)) = event;
            save(
                cx,
                "transfers",
                "bandwidth_limit",
                format!("{}", (value.start() * MB) as u64),
            );
            // Refresh the inline value label next to the slider.
            cx.notify();
        })
        .detach();

        let retries = cx
            .new(|cx| InputState::new(window, cx).default_value(cfg.transfers.retries.to_string()));
        cx.subscribe_in(&retries, window, |this, _, event: &InputEvent, _, cx| {
            if let InputEvent::Change = event {
                let value = this.retries.read(cx).value().trim().to_string();
                if let Ok(n) = value.parse::<u32>() {
                    save(cx, "transfers", "retries", n.to_string());
                }
            }
        })
        .detach();

        let log_filter = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("kessel=info,opendal=warn")
                .default_value(cfg.logging.filter.clone())
        });
        cx.subscribe_in(&log_filter, window, |this, _, event: &InputEvent, _, cx| {
            if let InputEvent::PressEnter { .. } = event {
                let value = this.log_filter.read(cx).value().trim().to_string();
                if !value.is_empty() {
                    save(cx, "logging", "filter", fmt_str(&value));
                }
            }
        })
        .detach();

        Self {
            search,
            query: String::new(),
            selected: Category::General,
            theme,
            light_theme,
            dark_theme,
            icon_theme,
            locale,
            sidebar,
            conflict_policy,
            parallel,
            bandwidth,
            retries,
            log_filter,
        }
    }

    fn save_choice(&self, section: &str, key: &str, choice: &Choice, cx: &mut Context<Self>) {
        if let Some(value) = choice.value(cx) {
            save(cx, section, key, fmt_str(&value));
        }
    }

    fn toggle(
        &self,
        id: &'static str,
        checked: bool,
        section: &'static str,
        key: &'static str,
    ) -> AnyElement {
        Switch::new(id)
            .checked(checked)
            .on_click(move |checked: &bool, _, cx| {
                save(cx, section, key, checked.to_string());
            })
            .into_any_element()
    }

    /// The control for one setting, ready to drop into the row's right column.
    fn control(&self, id: SettingId, cfg: &KesselConfig, cx: &Context<Self>) -> AnyElement {
        let muted = cx.theme().muted_foreground;
        match id {
            SettingId::Language => Select::new(&self.locale.select).small().into_any_element(),
            SettingId::Sidebar => Select::new(&self.sidebar.select).small().into_any_element(),
            SettingId::ShowHidden => self.toggle("show-hidden", cfg.ui.show_hidden, "ui", "show_hidden"),
            SettingId::Theme => Select::new(&self.theme.select).small().into_any_element(),
            SettingId::LightTheme => {
                Select::new(&self.light_theme.select).small().into_any_element()
            }
            SettingId::DarkTheme => Select::new(&self.dark_theme.select).small().into_any_element(),
            SettingId::IconTheme => Select::new(&self.icon_theme.select).small().into_any_element(),
            SettingId::MaxParallel => {
                let value = self.parallel.read(cx).value().start() as usize;
                v_flex()
                    .gap_1()
                    .child(div().text_xs().text_color(muted).child(value.to_string()))
                    .child(Slider::new(&self.parallel))
                    .into_any_element()
            }
            SettingId::Bandwidth => {
                let mb = self.bandwidth.read(cx).value().start();
                let label = if mb < 1.0 {
                    t("prefs.bandwidth.unlimited").to_string()
                } else {
                    format!("{mb:.0} MB/s")
                };
                v_flex()
                    .gap_1()
                    .child(div().text_xs().text_color(muted).child(label))
                    .child(Slider::new(&self.bandwidth))
                    .into_any_element()
            }
            SettingId::Retries => Input::new(&self.retries).small().into_any_element(),
            SettingId::Verify => self.toggle("verify", cfg.transfers.verify, "transfers", "verify"),
            SettingId::PreserveTimestamps => self.toggle(
                "preserve-timestamps",
                cfg.transfers.preserve_timestamps,
                "transfers",
                "preserve_timestamps",
            ),
            SettingId::SkipHidden => self.toggle(
                "skip-hidden",
                cfg.transfers.skip_hidden,
                "transfers",
                "skip_hidden",
            ),
            SettingId::ConflictPolicy => {
                Select::new(&self.conflict_policy.select).small().into_any_element()
            }
            SettingId::LogFilter => Input::new(&self.log_filter).small().into_any_element(),
        }
    }

    /// One titled settings block: name, muted description, control on the right.
    fn row(&self, setting: &Setting, cfg: &KesselConfig, cx: &Context<Self>) -> AnyElement {
        let muted = cx.theme().muted_foreground;
        h_flex()
            .py_2()
            .gap_4()
            .items_start()
            .justify_between()
            .child(
                v_flex()
                    .gap_0p5()
                    .flex_1()
                    .child(div().text_sm().child(t(setting.name)))
                    .child(
                        div()
                            .text_xs()
                            .text_color(muted)
                            .child(t(setting.desc)),
                    ),
            )
            .child(div().w(px(220.)).flex_none().child(self.control(setting.id, cfg, cx)))
            .into_any_element()
    }

    /// True while the search box is non-empty; category selection is ignored.
    fn searching(&self) -> bool {
        !self.query.is_empty()
    }

    /// Whether a setting matches the current search (name or description).
    fn matches(&self, setting: &Setting) -> bool {
        let needle = self.query.to_lowercase();
        t(setting.name).to_lowercase().contains(&needle)
            || t(setting.desc).to_lowercase().contains(&needle)
    }
}

/// Write one value into config.json5; the file watcher applies it live.
fn save(cx: &mut gpui::App, section: &str, key: &str, value: String) {
    if let Err(error) = config::save_values(section, &[(key, value)]) {
        tracing::error!(%error, section, key, "saving preference failed");
    }
    // The watcher will also refresh, but reflect immediately where possible.
    if section == "ui" && key == "locale" {
        if let Ok(cfg) = config::load() {
            crate::i18n::init(&cfg.ui.locale);
            crate::menu::install(cx);
        }
    }
}

impl Render for PreferencesPanel {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let cfg = cx.global::<AppState>().config.clone();
        let muted = cx.theme().muted_foreground;
        let accent = cx.theme().accent;
        let accent_fg = cx.theme().accent_foreground;
        let searching = self.searching();

        // Category rail.
        let rail = v_flex().gap_0p5().w(px(150.)).flex_none().children(
            Category::ALL.into_iter().map(|category| {
                let active = !searching && category == self.selected;
                div()
                    .px_2()
                    .py_1()
                    .rounded_md()
                    .text_sm()
                    .cursor_pointer()
                    .when(active, |this| {
                        this.bg(accent).text_color(accent_fg)
                    })
                    .when(!active, |this| this.text_color(muted).hover(|s| s.bg(cx.theme().muted)))
                    .child(category.label())
                    .on_mouse_down(
                        gpui::MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.selected = category;
                            if !this.query.is_empty() {
                                this.query.clear();
                                this.search.update(cx, |state, cx| {
                                    state.set_value("", window, cx);
                                });
                            }
                            cx.notify();
                        }),
                    )
            }),
        );

        // Visible settings: everything matching the query, else the selected
        // category. When searching, show a small category caption per group.
        let mut content = v_flex()
            .id("prefs-content")
            .gap_0()
            .flex_1()
            .overflow_y_scroll();
        if searching {
            let mut any = false;
            for category in Category::ALL {
                let rows: Vec<AnyElement> = SETTINGS
                    .iter()
                    .filter(|s| s.category == category && self.matches(s))
                    .map(|s| self.row(s, &cfg, cx))
                    .collect();
                if rows.is_empty() {
                    continue;
                }
                any = true;
                content = content
                    .child(
                        div()
                            .mt_2()
                            .text_xs()
                            .font_semibold()
                            .text_color(muted)
                            .child(category.label()),
                    )
                    .children(rows);
            }
            if !any {
                content = content.child(
                    div()
                        .py_4()
                        .text_sm()
                        .text_color(muted)
                        .child(t("prefs.no_results")),
                );
            }
        } else {
            content = content.children(
                SETTINGS
                    .iter()
                    .filter(|s| s.category == self.selected)
                    .map(|s| self.row(s, &cfg, cx)),
            );
            // Appearance gets a shortcut into the theme/icon-theme downloader.
            if self.selected == Category::Appearance {
                content = content.child(
                    h_flex().pt_2().child(
                        Button::new("browse-themes")
                            .label(t("prefs.browse_themes"))
                            .outline()
                            .small()
                            .on_click(|_, window, cx| {
                                window.dispatch_action(
                                    Box::new(crate::app::OpenThemeBrowser),
                                    cx,
                                );
                            }),
                    ),
                );
            }
        }

        v_flex()
            .gap_3()
            .w_full()
            .child(Input::new(&self.search).small())
            .child(h_flex().gap_4().h(px(420.)).items_start().child(rail).child(content))
            .child(
                h_flex()
                    .pt_2()
                    .gap_2()
                    .items_center()
                    .justify_between()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .child(
                        div()
                            .flex_1()
                            .text_xs()
                            .text_color(muted)
                            .child(t("prefs.config_hint")),
                    )
                    .child(
                        Button::new("open-config")
                            .label(t("prefs.open_config"))
                            .outline()
                            .small()
                            .on_click(|_, _, _| {
                                if let Ok(dir) = config::data_dir() {
                                    let path = dir.join("config.json5");
                                    // Make sure the file exists before opening it.
                                    if !path.exists() {
                                        let _ = std::fs::write(&path, "{\n}\n");
                                    }
                                    crate::ui::open_path_or_url(&path.display().to_string());
                                }
                            }),
                    ),
            )
    }
}

/// Version string for the About dialog.
pub fn app_version() -> SharedString {
    tf("about.version", &[("version", env!("CARGO_PKG_VERSION"))])
}
