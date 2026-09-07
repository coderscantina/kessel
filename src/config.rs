use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use serde::Deserialize;

use crate::provider::LayerConfig;

/// Compiled-in defaults; user overrides live in `<data_dir>/kessel/config.json5`.
const DEFAULTS: &str = include_str!("../assets/config/default.json5");

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KesselConfig {
    pub ui: UiConfig,
    pub transfers: TransferConfig,
    /// Default OpenDAL layer stack, overridable per site.
    pub layers: LayerConfig,
    pub logging: LoggingConfig,
    /// Embedded MCP server (disabled by default).
    #[serde(default)]
    pub mcp: McpConfig,
    /// In-app auto-update against GitHub releases.
    #[serde(default)]
    pub update: UpdateConfig,
}

/// In-app auto-update. kessel checks a stable release manifest (`latest.json`)
/// on GitHub at startup and once a day, and — when a newer build is available
/// and it is running from a writable `.app` bundle — can download, verify and
/// swap it in place.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateConfig {
    /// Master switch for the background check (manual "Check for updates…"
    /// still works regardless).
    #[serde(default = "default_true")]
    pub check: bool,
    /// The release manifest URL; the default resolves to the newest release.
    #[serde(default = "default_update_url")]
    pub url: String,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        UpdateConfig {
            check: true,
            url: default_update_url(),
        }
    }
}

/// The release manifest for *this* platform. Each release publishes one per
/// platform beside its artifact; `latest.json` stays the macOS one so builds
/// that predate the Windows port keep updating.
fn default_update_url() -> String {
    let manifest = if cfg!(target_os = "windows") {
        "latest-windows.json"
    } else {
        "latest.json"
    };
    format!("https://github.com/coderscantina/kessel/releases/latest/download/{manifest}")
}

/// Embedded Model Context Protocol server. Exposes kessel's sites, listings
/// and transfer queue to MCP clients over a local streamable-HTTP endpoint
/// (`http://<bind>:<port>/mcp`). Off by default; changes apply on restart.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct McpConfig {
    /// Master switch. When false the server never binds a socket.
    #[serde(default)]
    pub enabled: bool,
    /// TCP port for the streamable-HTTP endpoint.
    #[serde(default = "default_mcp_port")]
    pub port: u16,
    /// Bind address; keep on loopback — there is no authentication (v1).
    #[serde(default = "default_mcp_bind")]
    pub bind: String,
    /// Gate for mutating tools (enqueue/pause/resume/cancel transfers). When
    /// false only read-only tools are exposed.
    #[serde(default)]
    pub expose_write: bool,
}

impl Default for McpConfig {
    fn default() -> Self {
        McpConfig {
            enabled: false,
            port: default_mcp_port(),
            bind: default_mcp_bind(),
            expose_write: false,
        }
    }
}

fn default_mcp_port() -> u16 {
    39100
}

fn default_mcp_bind() -> String {
    "127.0.0.1".to_string()
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UiConfig {
    /// "light" | "dark" | "system"
    pub theme: String,
    /// UI language: "system" resolves from the environment; otherwise a
    /// locale tag matching a table in `assets/i18n/` or `<data_dir>/i18n/`.
    #[serde(default = "default_locale")]
    pub locale: String,
    /// Named theme (from `<data_dir>/themes/*.json`) per appearance.
    #[serde(default)]
    pub light_theme: Option<String>,
    #[serde(default)]
    pub dark_theme: Option<String>,
    /// Zed-compatible icon theme, by theme name, from an unpacked extension
    /// in `<data_dir>/extensions/*/icon_themes/*.json`.
    #[serde(default)]
    pub icon_theme: Option<String>,
    /// Zed-style `theme_overrides`: theme name → Zed style keys merged over
    /// that theme's style before conversion (Zed-format themes only).
    #[serde(default)]
    pub theme_overrides: HashMap<String, serde_json::Map<String, serde_json::Value>>,
    pub show_hidden: bool,
    /// Which side the vertical tabs sidebar docks to: "left" | "right".
    #[serde(default = "default_sidebar_position")]
    pub sidebar_position: String,
    /// Default file-list columns per backend scheme ("default" is the
    /// fallback). Known columns: "size", "modified", "permissions" (fs,
    /// sftp), "storage_class" (s3), "region" (s3). Name always shows.
    #[serde(default = "default_columns")]
    pub columns: HashMap<String, Vec<String>>,
    /// Panel sizes in px. Live values are written back into
    /// `<data_dir>/config.json5` itself (comment-preserving in-place edits).
    #[serde(default = "default_sidebar_width")]
    pub sidebar_width: f32,
    /// Height of the shared bottom dock (Transfers / Logs). The old per-panel
    /// `transfers_height` / `logs_height` keys are folded into this on load
    /// (see [`migrate_dock_height`]); an alias can't do it because the merged
    /// defaults always carry `dock_height`, so serde would see the field twice.
    #[serde(default = "default_dock_height")]
    pub dock_height: f32,
    /// First-pane size in a horizontal / vertical split.
    #[serde(default = "default_split_h_width")]
    pub split_h_width: f32,
    #[serde(default = "default_split_v_height")]
    pub split_v_height: f32,
    /// Per-pane folder tree: shown by default for new panes, and its width.
    #[serde(default)]
    pub tree_visible: bool,
    #[serde(default = "default_tree_width")]
    pub tree_width: f32,
    /// Per-pane folder tree height when placed above the file list (panes
    /// side-by-side / narrow layout).
    #[serde(default = "default_tree_height")]
    pub tree_height: f32,
}

fn default_tree_width() -> f32 {
    180.
}

fn default_tree_height() -> f32 {
    180.
}

fn default_sidebar_width() -> f32 {
    220.
}

fn default_dock_height() -> f32 {
    240.
}

fn default_split_h_width() -> f32 {
    500.
}

fn default_split_v_height() -> f32 {
    300.
}

/// Live UI state (panel sizes) is stored in `config.json5` itself. Writes
/// are surgical, line-based edits so hand-authored comments and formatting
/// survive: an existing `key: value` line keeps its indentation and trailing
/// comment; missing keys are inserted into the `ui: {` block (which is
/// appended if absent).
pub fn save_ui_state(ui: &UiConfig) -> Result<()> {
    let updates: Vec<(&str, String)> = [
        ("sidebar_width", ui.sidebar_width),
        ("dock_height", ui.dock_height),
        ("split_h_width", ui.split_h_width),
        ("split_v_height", ui.split_v_height),
        ("tree_width", ui.tree_width),
        ("tree_height", ui.tree_height),
    ]
    .into_iter()
    .map(|(key, value)| (key, fmt_f32(value)))
    .collect();
    save_values("ui", &updates)
}

/// Write `section.key = value` pairs into the user config.json5 in place
/// (values are pre-formatted JSON5 literals, see [`fmt_str`] / [`fmt_f32`]).
/// The Preferences UI writes through here; the hot-reload watcher applies
/// the result, so this is the single mutation path for settings.
pub fn save_values(section: &str, updates: &[(&str, String)]) -> Result<()> {
    let path = data_dir()?.join("config.json5");
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let text = upsert_section_values(&text, section, updates);
    std::fs::write(&path, text).with_context(|| format!("writing `{}`", path.display()))
}

/// A JSON5 string literal.
pub fn fmt_str(value: &str) -> String {
    format!("{:?}", value)
}

fn fmt_f32(value: f32) -> String {
    if value.fract() == 0.0 {
        format!("{}", value as i64)
    } else {
        format!("{value:.1}")
    }
}

/// The line range of `section: { … }` in the file: (open line, close line).
/// Brace counting is naive (comments/strings with braces can fool it), which
/// is fine for the config files kessel itself writes and documents.
fn section_bounds(lines: &[String], section: &str) -> Option<(usize, usize)> {
    let open = lines.iter().position(|line| {
        let t = line.trim_start();
        !t.starts_with("//")
            && (t.starts_with(&format!("{section}:")) || t.starts_with(&format!("\"{section}\":")))
            && t.contains('{')
    })?;
    let mut depth = 0i32;
    for (ix, line) in lines.iter().enumerate().skip(open) {
        for ch in line.chars() {
            match ch {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
        if depth <= 0 {
            return Some((open, ix));
        }
    }
    Some((open, lines.len().saturating_sub(1)))
}

/// The character length of the JSON5 value at the start of `rest`: a quoted
/// string runs to its closing quote, anything else to the trailing comma /
/// line comment / end of line.
fn value_len(rest: &str) -> usize {
    if let Some(tail) = rest.strip_prefix('"') {
        let mut escaped = false;
        for (ix, ch) in tail.char_indices() {
            match ch {
                '\\' if !escaped => escaped = true,
                '"' if !escaped => return ix + 2,
                _ => escaped = false,
            }
        }
        rest.len()
    } else {
        let end = rest
            .find(',')
            .unwrap_or(rest.len())
            .min(rest.find("//").unwrap_or(rest.len()));
        rest[..end].trim_end().len()
    }
}

/// Replace (or insert) values inside one `section: { … }` block of
/// config.json5 text while leaving every other byte untouched.
fn upsert_section_values(text: &str, section: &str, updates: &[(&str, String)]) -> String {
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let mut missing: Vec<(&str, &String)> = Vec::new();

    'update: for (key, value) in updates {
        if let Some((open, close)) = section_bounds(&lines, section) {
            for line in &mut lines[open + 1..close] {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }
                // Match `key:` or `"key":` at the start of the line.
                let after_key = trimmed
                    .strip_prefix(&format!("{key}:"))
                    .or_else(|| trimmed.strip_prefix(&format!("\"{key}\":")));
                let Some(rest) = after_key else { continue };
                // Preserve everything after the value (comma, comments).
                let len = value_len(rest.trim_start());
                let leading_ws = &line[..line.len() - trimmed.len()];
                let key_part = &trimmed[..trimmed.len() - rest.len()];
                let tail = &rest.trim_start()[len..];
                *line = format!("{leading_ws}{key_part} {value}{tail}");
                continue 'update;
            }
        }
        missing.push((key, value));
    }

    if !missing.is_empty() {
        // Insert missing keys right after the section's block opener; create
        // the block (and the root object) if the file doesn't have one.
        match section_bounds(&lines, section) {
            Some((open, _)) => {
                let indent = format!(
                    "{}  ",
                    &lines[open][..lines[open].len() - lines[open].trim_start().len()]
                );
                for (key, value) in missing.iter().rev() {
                    lines.insert(open + 1, format!("{indent}{key}: {value},"));
                }
            }
            None => {
                let block: Vec<String> = std::iter::once(format!("  {section}: {{"))
                    .chain(
                        missing
                            .iter()
                            .map(|(key, value)| format!("    {key}: {value},")),
                    )
                    .chain(std::iter::once("  },".to_string()))
                    .collect();
                // Insert before the root object's closing brace, or create
                // the root object from scratch.
                let close = lines
                    .iter()
                    .rposition(|line| line.trim_start().starts_with('}'));
                match close {
                    Some(ix) => {
                        for line in block.into_iter().rev() {
                            lines.insert(ix, line);
                        }
                    }
                    None => {
                        lines = std::iter::once("{".to_string())
                            .chain(block)
                            .chain(std::iter::once("}".to_string()))
                            .collect();
                    }
                }
            }
        }
    }

    let mut out = lines.join("\n");
    if text.ends_with('\n') || text.is_empty() {
        out.push('\n');
    }
    out
}

/// Drop `key: value` lines from one `section: { … }` block, leaving every
/// other byte untouched.
fn remove_section_values(text: &str, section: &str, keys: &[&str]) -> String {
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    if let Some((open, close)) = section_bounds(&lines, section) {
        for ix in (open + 1..close).rev() {
            let t = lines[ix].trim_start();
            if !t.starts_with("//")
                && keys.iter().any(|key| {
                    t.starts_with(&format!("{key}:")) || t.starts_with(&format!("\"{key}\":"))
                })
            {
                lines.remove(ix);
            }
        }
    }
    let mut out = lines.join("\n");
    if text.ends_with('\n') {
        out.push('\n');
    }
    out
}

fn default_sidebar_position() -> String {
    "left".to_string()
}

fn default_locale() -> String {
    "system".to_string()
}

fn default_columns() -> HashMap<String, Vec<String>> {
    HashMap::from([(
        "default".to_string(),
        vec!["size".to_string(), "modified".to_string()],
    )])
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TransferConfig {
    pub max_parallel: usize,
    pub bandwidth_limit: u64,
    /// Extra attempts per file after its first failure (on top of the
    /// per-operation retries OpenDAL's RetryLayer already does).
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Post-transfer verification: re-stat the destination and compare sizes
    /// (and etags where they are comparable).
    #[serde(default = "default_true")]
    pub verify: bool,
    /// Copy the source's modified time onto the destination where the
    /// backend allows it (local fs, sftp).
    #[serde(default = "default_true")]
    pub preserve_timestamps: bool,
    /// Leave hidden files (dotfiles) out when a directory is expanded into
    /// per-file transfer tasks.
    #[serde(default)]
    pub skip_hidden: bool,
}

fn default_retries() -> u32 {
    2
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LoggingConfig {
    pub filter: String,
}

/// Reload handle for the tracing filter, set once at startup so the
/// config.json5 watcher can apply `logging.filter` changes live.
pub static LOG_FILTER: std::sync::OnceLock<
    tracing_subscriber::reload::Handle<
        tracing_subscriber::EnvFilter,
        tracing_subscriber::Registry,
    >,
> = std::sync::OnceLock::new();

/// `~/Library/Application Support/kessel` (macOS) or platform equivalent.
pub fn data_dir() -> Result<PathBuf> {
    let dir = dirs::data_dir()
        .context("no platform data directory")?
        .join("kessel");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-place UI-state writes must keep comments/formatting, update
    /// existing keys, and insert missing ones into the ui block.
    #[test]
    fn upsert_preserves_comments_and_inserts() {
        let existing = "{\n  // my config\n  ui: {\n    theme: \"dark\",\n    sidebar_width: 200, // tuned by hand\n  },\n}\n";
        let out = upsert_section_values(
            existing,
            "ui",
            &[
                ("sidebar_width", "260".to_string()),
                ("transfers_height", "300".to_string()),
            ],
        );
        assert!(out.contains("// my config"));
        assert!(out.contains("sidebar_width: 260, // tuned by hand"));
        assert!(out.contains("transfers_height: 300,"));
        assert!(out.contains("theme: \"dark\""));
        // Still valid JSON5 with the expected values.
        let parsed: serde_json::Value = json5::from_str(&out).unwrap();
        assert_eq!(parsed["ui"]["sidebar_width"], 260);
        assert_eq!(parsed["ui"]["transfers_height"], 300);
    }

    /// Writing into an empty / missing config file creates a valid document.
    #[test]
    fn upsert_creates_document() {
        let out = upsert_section_values("", "ui", &[("sidebar_width", "220".to_string())]);
        let parsed: serde_json::Value = json5::from_str(&out).unwrap();
        assert_eq!(parsed["ui"]["sidebar_width"], 220);
    }

    /// A config without a ui block gets one appended inside the root object.
    #[test]
    fn upsert_appends_ui_block() {
        let existing = "{\n  transfers: {\n    max_parallel: 4,\n  },\n}\n";
        let out = upsert_section_values(existing, "ui", &[("split_h_width", "512.5".to_string())]);
        let parsed: serde_json::Value = json5::from_str(&out).unwrap();
        assert_eq!(parsed["transfers"]["max_parallel"], 4);
        assert_eq!(parsed["ui"]["split_h_width"], 512.5);
    }

    /// Legacy `transfers_height` / `logs_height` keys fold into the shared
    /// `dock_height` and are removed from the text; an explicit `dock_height`
    /// wins over legacy values; a bare config still gets the default.
    #[test]
    fn dock_height_migrates_from_legacy_keys() {
        let text = "{\n  ui: {\n    logs_height: 228.1, // keep\n    transfers_height: 283.8,\n  },\n}\n";
        let (dock, legacy) = legacy_dock_heights(text);
        assert_eq!(dock, None);
        let out = remove_section_values(text, "ui", &["transfers_height", "logs_height"]);
        let out = upsert_section_values(&out, "ui", &[("dock_height", fmt_f32(legacy.unwrap()))]);
        let parsed: serde_json::Value = json5::from_str(&out).unwrap();
        assert_eq!(parsed["ui"]["dock_height"], 283.8);
        assert!(!out.contains("transfers_height") && !out.contains("logs_height"));

        // Explicit dock_height is left alone; legacy keys are still dropped.
        let text = "{\n  ui: {\n    dock_height: 100,\n    logs_height: 228.1,\n  },\n}\n";
        let (dock, legacy) = legacy_dock_heights(text);
        assert_eq!((dock, legacy), (Some(100.), Some(228.1)));

        let bare: UiConfig =
            json5::from_str(r#"{ "theme": "dark", "show_hidden": false }"#).unwrap();
        assert_eq!(bare.dock_height, default_dock_height());
    }

    /// The MCP server is off by default and its knobs fall back to the
    /// documented defaults (loopback bind, port 39100, no write tools).
    #[test]
    fn mcp_defaults() {
        let cfg: KesselConfig = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Json5))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert!(!cfg.mcp.enabled);
        assert_eq!(cfg.mcp.port, 39100);
        assert_eq!(cfg.mcp.bind, "127.0.0.1");
        assert!(!cfg.mcp.expose_write);
    }

    /// Auto-update is on by default and points at the stable release manifest.
    #[test]
    fn update_defaults() {
        let cfg: KesselConfig = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Json5))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert!(cfg.update.check);
        assert!(cfg
            .update
            .url
            .ends_with("releases/latest/download/latest.json"));
    }

    /// Zed-style theme_overrides use dotted style keys ("element.selected");
    /// they must survive the config crate as literal map keys, not nested tables.
    #[test]
    fn theme_overrides_keep_dotted_keys() {
        let json5 = r##"{
            ui: {
                theme_overrides: {
                    "My Theme": { "element.selected": "#212121", "border": "#1A1A1A" }
                }
            }
        }"##;
        let cfg = config::Config::builder()
            .add_source(config::File::from_str(DEFAULTS, config::FileFormat::Json5))
            .add_source(config::File::from_str(json5, config::FileFormat::Json5))
            .build()
            .unwrap();
        let cfg: KesselConfig = cfg.try_deserialize().unwrap();
        // config 0.15 preserves map-key case (0.14 lowercased them);
        // zed::parse_themes matches theme names case-insensitively either
        // way. Dotted style keys must survive as literal keys, not nested
        // tables.
        let patch = &cfg.ui.theme_overrides["My Theme"];
        assert_eq!(patch["element.selected"], "#212121");
        assert_eq!(patch["border"], "#1A1A1A");
    }
}

pub fn load() -> Result<KesselConfig> {
    migrate_ui_state()?;
    migrate_dock_height()?;
    let user_config = data_dir()?.join("config.json5");
    let cfg = config::Config::builder()
        .add_source(config::File::from_str(
            DEFAULTS,
            config::FileFormat::Json5,
        ))
        .add_source(config::File::from(user_config).required(false))
        .build()
        .context("building config")?;
    cfg.try_deserialize().context("deserializing config")
}

/// One-time migration: UI state used to live in a separate machine-written
/// `ui_state.json5` overlay; fold it into `config.json5` and remove it.
fn migrate_ui_state() -> Result<()> {
    let legacy = data_dir()?.join("ui_state.json5");
    let Ok(raw) = std::fs::read_to_string(&legacy) else {
        return Ok(());
    };
    #[derive(Deserialize)]
    struct Legacy {
        ui: LegacyUi,
    }
    #[derive(Deserialize)]
    struct LegacyUi {
        sidebar_width: Option<f32>,
        transfers_height: Option<f32>,
        split_h_width: Option<f32>,
        split_v_height: Option<f32>,
    }
    if let Ok(legacy_cfg) = json5::from_str::<Legacy>(&raw) {
        let updates: Vec<(&str, String)> = [
            ("sidebar_width", legacy_cfg.ui.sidebar_width),
            ("dock_height", legacy_cfg.ui.transfers_height),
            ("split_h_width", legacy_cfg.ui.split_h_width),
            ("split_v_height", legacy_cfg.ui.split_v_height),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.map(|v| (key, fmt_f32(v))))
        .collect();
        if !updates.is_empty() {
            save_values("ui", &updates)?;
        }
    }
    std::fs::remove_file(&legacy).ok();
    Ok(())
}

/// One-time migration: the shared bottom dock replaced the per-panel
/// `transfers_height` / `logs_height` keys. Fold them into `dock_height`
/// (unless the user config already sets it) and drop the legacy lines —
/// they'd otherwise shadow-conflict with the compiled-in `dock_height`
/// default at deserialize time.
fn migrate_dock_height() -> Result<()> {
    let path = data_dir()?.join("config.json5");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(());
    };
    let (dock, legacy) = legacy_dock_heights(&text);
    let Some(height) = legacy else {
        return Ok(());
    };
    let mut out = remove_section_values(&text, "ui", &["transfers_height", "logs_height"]);
    if dock.is_none() {
        out = upsert_section_values(&out, "ui", &[("dock_height", fmt_f32(height))]);
    }
    std::fs::write(&path, out).with_context(|| format!("writing `{}`", path.display()))
}

/// (`dock_height`, legacy height) as present in the user config text.
fn legacy_dock_heights(text: &str) -> (Option<f32>, Option<f32>) {
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Ui {
        dock_height: Option<f32>,
        transfers_height: Option<f32>,
        logs_height: Option<f32>,
    }
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Root {
        ui: Ui,
    }
    let root: Root = json5::from_str(text).unwrap_or_default();
    let legacy = root.ui.transfers_height.or(root.ui.logs_height);
    (root.ui.dock_height, legacy)
}
