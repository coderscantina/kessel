//! Zed extension compatibility. Parses the two Zed extension asset formats so
//! published extensions like `material-dark` (themes) and `material-icon-theme`
//! (icon themes) work unmodified:
//!
//! - Themes: <https://zed.dev/schema/themes/v0.2.0.json>
//! - Icon themes: <https://zed.dev/schema/icon_themes/v0.2.0.json>
//!
//! Drop an unpacked extension into `<data_dir>/extensions/<id>/` — its
//! `themes/*.json` and `icon_themes/*.json` are discovered, and icon paths
//! (`./icons/foo.svg`) resolve relative to the extension root, exactly as in
//! Zed. Zed theme styles are mapped onto gpui-component's `ThemeConfig`; the
//! `style` object doubles as the syntax-highlight theme, which gpui-component
//! already reads in Zed's format.

use std::collections::HashMap;

use anyhow::{Context as _, Result};
use gpui_component::highlighter::HighlightThemeStyle;
use gpui_component::{ThemeConfig, ThemeMode, ThemeSet};
use serde::Deserialize;
use serde_json::{Map, Value};

/// A Zed theme family file (`themes/*.json` in an extension).
#[derive(Debug, Deserialize)]
pub struct ThemeFamily {
    pub themes: Vec<ThemeContent>,
}

#[derive(Debug, Deserialize)]
pub struct ThemeContent {
    pub name: gpui::SharedString,
    /// "light" | "dark" — same wire format as gpui-component's `ThemeMode`.
    #[serde(default)]
    pub appearance: ThemeMode,
    pub style: Map<String, Value>,
}

/// A Zed icon theme family file (`icon_themes/*.json` in an extension).
#[derive(Debug, Deserialize)]
pub struct IconThemeFamily {
    pub themes: Vec<IconTheme>,
}

#[derive(Debug, Deserialize)]
pub struct IconTheme {
    pub name: String,
    #[serde(default)]
    pub directory_icons: Option<PairedIcons>,
    /// Folder-name-specific icons (schema v0.3.0), e.g. `"src"`,
    /// `"node_modules"` → their own collapsed/expanded pair.
    #[serde(default)]
    pub named_directory_icons: HashMap<String, PairedIcons>,
    #[serde(default)]
    pub file_stems: HashMap<String, String>,
    #[serde(default)]
    pub file_suffixes: HashMap<String, String>,
    #[serde(default)]
    pub file_icons: HashMap<String, IconDefinition>,
}

#[derive(Debug, Deserialize)]
pub struct PairedIcons {
    pub collapsed: Option<String>,
    pub expanded: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IconDefinition {
    pub path: String,
}

/// Zed-style `theme_overrides` from the config: theme name → style keys
/// merged over that theme's style.
pub type ThemeOverrides = HashMap<String, Map<String, Value>>;

/// Parse a theme file in either format: a Zed theme family (entries carry a
/// `style` object, with any matching `overrides` merged on top, Zed-style) or
/// a native gpui-component `ThemeSet`.
pub fn parse_themes(json: &str, overrides: &ThemeOverrides) -> Result<Vec<ThemeConfig>> {
    let value: Value = serde_json::from_str(json).context("invalid JSON")?;
    let is_zed = value["themes"]
        .as_array()
        .is_some_and(|themes| themes.iter().any(|theme| theme["style"].is_object()));
    if is_zed {
        let mut family: ThemeFamily =
            serde_json::from_value(value).context("invalid Zed theme family")?;
        for theme in &mut family.themes {
            // Case-insensitive fallback: older configs (config crate 0.14
            // lowercased map keys) keep working.
            if let Some(patch) = overrides
                .get(theme.name.as_ref())
                .or_else(|| overrides.get(&theme.name.to_lowercase()))
            {
                theme
                    .style
                    .extend(patch.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
        }
        Ok(family.themes.iter().map(to_theme_config).collect())
    } else {
        let set: ThemeSet = serde_json::from_value(value).context("invalid theme set")?;
        Ok(set.themes)
    }
}

/// gpui-component color key ← Zed style keys, first present wins. Keys absent
/// here (or in the theme) fall back to gpui-component's built-in defaults for
/// the theme's mode.
const COLOR_MAP: &[(&str, &[&str])] = &[
    ("background", &["background"]),
    ("foreground", &["text", "editor.foreground"]),
    ("border", &["border"]),
    ("input.border", &["border.variant", "border"]),
    ("ring", &["border.focused"]),
    ("muted.background", &["element.background", "surface.background"]),
    ("muted.foreground", &["text.muted"]),
    ("accent.background", &["element.hover"]),
    ("accent.foreground", &["text"]),
    ("primary.background", &["text.accent"]),
    ("primary.foreground", &["background"]),
    ("secondary.background", &["element.background", "surface.background"]),
    ("secondary.foreground", &["text"]),
    ("group_box.background", &["surface.background"]),
    ("group_box.foreground", &["text"]),
    ("list.active.background", &["element.selected"]),
    ("list.active.border", &["border.focused"]),
    ("list.hover.background", &["element.hover"]),
    ("table.background", &["editor.background"]),
    ("table.active.background", &["element.selected"]),
    ("table.active.border", &["border.focused"]),
    ("table.hover.background", &["element.hover"]),
    ("table.head.background", &["toolbar.background", "tab_bar.background"]),
    ("table.head.foreground", &["text.muted"]),
    ("table.row.border", &["border.variant"]),
    ("sidebar.background", &["panel.background", "surface.background"]),
    ("sidebar.foreground", &["text"]),
    ("sidebar.border", &["border"]),
    ("sidebar.accent.background", &["element.selected", "element.hover"]),
    ("sidebar.accent.foreground", &["text"]),
    ("sidebar.primary.background", &["text.accent"]),
    ("sidebar.primary.foreground", &["background"]),
    ("popover.background", &["elevated_surface.background", "surface.background"]),
    ("popover.foreground", &["text"]),
    ("title_bar.background", &["title_bar.background"]),
    ("title_bar.border", &["border"]),
    ("tab_bar.background", &["tab_bar.background"]),
    ("tab.background", &["tab.inactive_background"]),
    ("tab.active.background", &["tab.active_background"]),
    ("tab.foreground", &["text.muted"]),
    ("tab.active.foreground", &["text"]),
    ("scrollbar.background", &["scrollbar.track.background"]),
    ("scrollbar.thumb.background", &["scrollbar.thumb.background"]),
    ("scrollbar.thumb.hover.background", &["scrollbar.thumb.hover_background"]),
    ("link", &["link_text.hover"]),
    ("drop_target.background", &["drop_target.background"]),
    ("drag.border", &["border.focused"]),
    ("danger.background", &["error"]),
    ("danger.foreground", &["background"]),
    ("success.background", &["success", "created"]),
    ("success.foreground", &["background"]),
    ("warning.background", &["warning"]),
    ("warning.foreground", &["background"]),
    ("info.background", &["info"]),
    ("info.foreground", &["background"]),
    ("window.border", &["border"]),
    ("base.red", &["terminal.ansi.red"]),
    ("base.red.light", &["terminal.ansi.bright_red"]),
    ("base.green", &["terminal.ansi.green"]),
    ("base.green.light", &["terminal.ansi.bright_green"]),
    ("base.blue", &["terminal.ansi.blue"]),
    ("base.blue.light", &["terminal.ansi.bright_blue"]),
    ("base.yellow", &["terminal.ansi.yellow"]),
    ("base.yellow.light", &["terminal.ansi.bright_yellow"]),
    ("base.magenta", &["terminal.ansi.magenta"]),
    ("base.magenta.light", &["terminal.ansi.bright_magenta"]),
    ("base.cyan", &["terminal.ansi.cyan"]),
    ("base.cyan.light", &["terminal.ansi.bright_cyan"]),
];

/// Drop the alpha channel from a `#RRGGBBAA` / `#RGBA` hex color.
fn strip_alpha(hex: &str) -> String {
    match (hex.starts_with('#'), hex.len()) {
        (true, 9) => hex[..7].to_string(),
        (true, 5) => hex[..4].to_string(),
        _ => hex.to_string(),
    }
}

fn to_theme_config(theme: &ThemeContent) -> ThemeConfig {
    let style = &theme.style;
    let color = |key: &str| {
        style
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    // The first player slot carries the classic cursor / selection colors.
    let player = |field: &str| {
        style
            .get("players")?
            .as_array()?
            .first()?
            .get(field)?
            .as_str()
            .map(str::to_owned)
    };

    let mut colors = Map::new();
    for (target, sources) in COLOR_MAP {
        if let Some(mut value) = sources.iter().find_map(|key| color(key)) {
            // Popovers (context menus) draw a box shadow behind themselves;
            // Zed's elevated surfaces are often translucent, which lets the
            // shadow bleed through the menu and read as a smudge.
            if *target == "popover.background" {
                value = strip_alpha(&value);
            }
            colors.insert((*target).into(), value.into());
        }
    }
    if let Some(cursor) = player("cursor") {
        colors.insert("caret".into(), cursor.clone().into());
        colors
            .entry("primary.background")
            .or_insert_with(|| cursor.into());
    }
    if let Some(selection) = player("selection") {
        colors.insert("selection.background".into(), selection.into());
    }

    // ThemeConfigColors has private fields (the base palette), so it is built
    // through its own deserializer rather than a struct literal.
    let colors = serde_json::from_value(Value::Object(colors)).unwrap_or_default();
    // gpui-component's highlight style *is* Zed's `style` object (editor.*,
    // status colors, `syntax`) — feed it through directly.
    let highlight = serde_json::from_value::<HighlightThemeStyle>(Value::Object(style.clone()))
        .map_err(|error| {
            tracing::warn!(%error, theme = %theme.name, "unusable syntax style in Zed theme")
        })
        .ok();

    ThemeConfig {
        name: theme.name.clone(),
        mode: theme.appearance,
        colors,
        highlight,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZED_THEME: &str = r##"{
        "$schema": "https://zed.dev/schema/themes/v0.2.0.json",
        "name": "Test Family",
        "author": "test",
        "themes": [{
            "name": "Test Dark",
            "appearance": "dark",
            "style": {
                "background": "#263238",
                "text": "#EEFFFF",
                "border": "#37474F",
                "element.hover": "#37474F80",
                "element.selected": "#546E7A",
                "text.accent": "#82AAFF",
                "panel.background": "#263238",
                "error": "#F07178",
                "players": [{"cursor": "#FFCC00", "selection": "#546E7A66"}],
                "syntax": {
                    "keyword": {"color": "#C792EA", "font_style": null, "font_weight": null},
                    "comment": {"color": "#546E7A", "font_style": "italic", "font_weight": null}
                }
            }
        }]
    }"##;

    #[test]
    fn parses_zed_theme_family() {
        let themes = parse_themes(ZED_THEME, &ThemeOverrides::new()).unwrap();
        assert_eq!(themes.len(), 1);
        let theme = &themes[0];
        assert_eq!(theme.name.as_ref(), "Test Dark");
        assert_eq!(theme.mode, ThemeMode::Dark);
        assert_eq!(theme.colors.background.as_ref().map(|s| s.as_ref() as &str), Some("#263238"));
        assert_eq!(theme.colors.foreground.as_ref().map(|s| s.as_ref() as &str), Some("#EEFFFF"));
        assert_eq!(theme.colors.accent.as_ref().map(|s| s.as_ref() as &str), Some("#37474F80"));
        assert_eq!(theme.colors.primary.as_ref().map(|s| s.as_ref() as &str), Some("#82AAFF"));
        assert_eq!(theme.colors.caret.as_ref().map(|s| s.as_ref() as &str), Some("#FFCC00"));
        assert_eq!(theme.colors.selection.as_ref().map(|s| s.as_ref() as &str), Some("#546E7A66"));
        assert_eq!(theme.colors.sidebar.as_ref().map(|s| s.as_ref() as &str), Some("#263238"));
        assert_eq!(theme.colors.danger.as_ref().map(|s| s.as_ref() as &str), Some("#F07178"));
        assert!(theme.highlight.is_some(), "syntax style should parse");
    }

    #[test]
    fn applies_theme_overrides() {
        let mut overrides = ThemeOverrides::new();
        overrides.insert(
            "Test Dark".into(),
            serde_json::from_str(r##"{"element.selected": "#212121", "border": "#1A1A1A"}"##)
                .unwrap(),
        );
        let themes = parse_themes(ZED_THEME, &overrides).unwrap();
        let theme = &themes[0];
        assert_eq!(
            theme.colors.list_active.as_ref().map(|s| s.as_ref() as &str),
            Some("#212121")
        );
        assert_eq!(
            theme.colors.border.as_ref().map(|s| s.as_ref() as &str),
            Some("#1A1A1A")
        );
        // Untouched keys keep the theme's own values.
        assert_eq!(
            theme.colors.background.as_ref().map(|s| s.as_ref() as &str),
            Some("#263238")
        );
    }

    #[test]
    fn parses_native_theme_set() {
        let native = r##"{
            "name": "Native",
            "themes": [{
                "name": "Native Light",
                "mode": "light",
                "colors": {"background": "#ffffff"}
            }]
        }"##;
        let themes = parse_themes(native, &ThemeOverrides::new()).unwrap();
        assert_eq!(themes[0].name.as_ref(), "Native Light");
        assert_eq!(themes[0].mode, ThemeMode::Light);
        assert_eq!(themes[0].colors.background.as_ref().map(|s| s.as_ref() as &str), Some("#ffffff"));
    }

    #[test]
    fn parses_icon_theme_family() {
        let json = r##"{
            "$schema": "https://zed.dev/schema/icon_themes/v0.2.0.json",
            "name": "Icons",
            "author": "test",
            "themes": [{
                "name": "Icons Dark",
                "appearance": "dark",
                "directory_icons": {"collapsed": "./icons/folder.svg", "expanded": "./icons/folder-open.svg"},
                "file_stems": {"Makefile": "makefile"},
                "file_suffixes": {"rs": "rust"},
                "file_icons": {"rust": {"path": "./icons/rust.svg"}}
            }]
        }"##;
        let family: IconThemeFamily = serde_json::from_str(json).unwrap();
        let theme = &family.themes[0];
        assert_eq!(theme.name, "Icons Dark");
        assert_eq!(theme.file_suffixes["rs"], "rust");
        assert_eq!(theme.file_icons["rust"].path, "./icons/rust.svg");
        assert_eq!(
            theme.directory_icons.as_ref().unwrap().collapsed.as_deref(),
            Some("./icons/folder.svg")
        );
    }

    /// Gate on env vars to verify against real extension files, e.g.
    /// `KESSEL_ZED_THEME_FIXTURE=material-dark.json cargo test`.
    #[test]
    fn parses_real_extension_files_if_provided() {
        if let Ok(path) = std::env::var("KESSEL_ZED_THEME_FIXTURE") {
            let json = std::fs::read_to_string(path).unwrap();
            let themes = parse_themes(&json, &ThemeOverrides::new()).unwrap();
            assert!(!themes.is_empty());
            for theme in &themes {
                assert!(theme.colors.background.is_some(), "{}: no background", theme.name);
                assert!(theme.highlight.is_some(), "{}: no syntax style", theme.name);
            }
        }
        if let Ok(path) = std::env::var("KESSEL_ZED_ICON_THEME_FIXTURE") {
            let json = std::fs::read_to_string(path).unwrap();
            let family: IconThemeFamily = serde_json::from_str(&json).unwrap();
            assert!(!family.themes.is_empty());
            assert!(!family.themes[0].file_icons.is_empty());
        }
    }
}
