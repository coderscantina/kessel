//! Configurable keyboard shortcuts.
//!
//! Defaults ship compiled-in (`assets/config/keybindings.json5`); the user
//! overrides individual bindings in `<data_dir>/keybindings.json5` (same
//! shape). A binding maps a keystroke to an action name, to
//! `["action", { …args }]` for actions with arguments, or to `null` to
//! unbind a default. Top-level keys are gpui key contexts.

use std::collections::HashMap;
use std::rc::Rc;

use gpui::{App, KeyBinding, KeyBindingContextPredicate};
use serde::Deserialize;

const DEFAULTS: &str = include_str!("../assets/config/keybindings.json5");

/// A single binding value in the keymap file.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum BindingSpec {
    /// `"file_pane::SelectAll"`
    Action(String),
    /// `["workspace::SwitchSpace", { index: 1 }]`
    WithArgs(String, serde_json::Value),
}

/// context → keystroke → action (`None` unbinds a default). Section names may
/// carry a platform selector (see [`resolve_platforms`]).
type KeymapFile = HashMap<String, HashMap<String, Option<BindingSpec>>>;

/// This build's name in a keymap platform selector.
const PLATFORM: &str = if cfg!(target_os = "macos") {
    "macos"
} else if cfg!(target_os = "windows") {
    "windows"
} else {
    "linux"
};

const PLATFORMS: &[&str] = &["macos", "windows", "linux"];

/// Collapse platform-selected sections into plain contexts.
///
/// A section name may be prefixed with a comma-separated platform list —
/// `"windows,linux:Workspace"` — and only applies on those platforms. The
/// selected sections are folded over the plain ones, so a platform section can
/// override a shared binding or drop it with `null`. Names without a
/// recognized selector are contexts as-is (`"Workspace"`, and gpui context
/// predicates like `"FilePane && !editing"`).
fn resolve_platforms(raw: KeymapFile) -> KeymapFile {
    let mut out = KeymapFile::new();
    // Shared sections first, then the ones selected for this platform, so
    // platform-specific bindings win over the shared ones.
    for platform_pass in [false, true] {
        for (name, binds) in &raw {
            let (selector, context) = split_selector(name);
            if selector.is_some() != platform_pass || selector == Some(false) {
                continue;
            }
            out.entry(context.to_string())
                .or_default()
                .extend(binds.clone());
        }
    }
    out
}

/// Split a section name into its platform selector and context. The selector
/// is `Some(applies_here)` for `"windows,linux:Workspace"`, and `None` when
/// the name is a plain context — including gpui predicates that contain no
/// colon at all, like `"FilePane && !editing"`.
fn split_selector(name: &str) -> (Option<bool>, &str) {
    let Some((platforms, context)) = name.split_once(':') else {
        return (None, name);
    };
    let mut parts = platforms.split(',').map(str::trim);
    let first = parts.clone().next().unwrap_or_default();
    if !PLATFORMS.contains(&first) {
        return (None, name);
    }
    (
        Some(parts.any(|platform| platform == PLATFORM)),
        context.trim(),
    )
}

/// Parse defaults, merge the user file over them per (context, keystroke),
/// and register everything with gpui. Called once at startup, after the
/// action types (via `actions!` / `#[derive(Action)]`) are registered.
pub fn init(cx: &mut App) {
    let mut keymap = resolve_platforms(
        json5::from_str(DEFAULTS).expect("compiled-in keybindings.json5 must parse"),
    );

    match user_keymap() {
        Ok(Some(user)) => {
            for (context, binds) in resolve_platforms(user) {
                keymap.entry(context).or_default().extend(binds);
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::error!(%error, "keybindings.json5 is invalid; using defaults");
        }
    }

    let mut bindings = Vec::new();
    for (context, binds) in keymap {
        let predicate = match KeyBindingContextPredicate::parse(&context) {
            Ok(predicate) => Rc::new(predicate),
            Err(error) => {
                tracing::error!(%context, %error, "invalid key context in keymap");
                continue;
            }
        };
        for (keystroke, spec) in binds {
            let Some(spec) = spec else { continue };
            let (name, args) = match spec {
                BindingSpec::Action(name) => (name, None),
                BindingSpec::WithArgs(name, args) => (name, Some(args)),
            };
            let action = match cx.build_action(&name, args) {
                Ok(action) => action,
                Err(error) => {
                    tracing::error!(%name, %error, "unknown action in keymap");
                    continue;
                }
            };
            match KeyBinding::load(
                &keystroke,
                action,
                Some(predicate.clone()),
                false,
                None,
                cx.keyboard_mapper().as_ref(),
            ) {
                Ok(binding) => bindings.push(binding),
                Err(error) => {
                    tracing::error!(%keystroke, %error, "invalid keystroke in keymap");
                }
            }
        }
    }
    cx.bind_keys(bindings);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_in_keymap_parses() {
        let keymap: KeymapFile = json5::from_str(DEFAULTS).expect("defaults must parse");
        assert!(keymap.contains_key("FilePane"));
        assert!(keymap.contains_key("Workspace"));
        match keymap["Workspace"]["ctrl-1"].as_ref().expect("bound") {
            BindingSpec::WithArgs(name, args) => {
                assert_eq!(name, "workspace::SwitchSpace");
                assert_eq!(args["index"], 1);
            }
            BindingSpec::Action(_) => panic!("ctrl-1 should carry args"),
        }
        match keymap["FilePane"]["space"].as_ref().expect("bound") {
            BindingSpec::Action(name) => assert_eq!(name, "file_pane::QuickLook"),
            BindingSpec::WithArgs(..) => panic!("space is a plain action"),
        }
    }

    #[test]
    fn platform_sections_fold_into_their_context() {
        let raw: KeymapFile = json5::from_str(DEFAULTS).expect("defaults must parse");
        let resolved = resolve_platforms(raw);
        // Selectors are consumed: only real contexts survive.
        assert!(resolved.keys().all(|context| !context.contains(':')));

        // ctrl-1…9 switches spaces on macOS, where the command modifier is Cmd;
        // off macOS it belongs to the tabs, so the platform section drops it and
        // spaces move to alt-1…9.
        let workspace = &resolved["Workspace"];
        if cfg!(target_os = "macos") {
            assert!(workspace["ctrl-1"].is_some());
            assert!(!workspace.contains_key("alt-1"));
        } else {
            assert!(workspace["ctrl-1"].is_none(), "unbound by the platform section");
            assert!(workspace["alt-1"].is_some());
        }
        // Shared bindings survive either way.
        assert!(workspace["secondary-t"].is_some());
    }

    #[test]
    fn unselected_sections_are_dropped_and_plain_contexts_kept() {
        let other = if cfg!(target_os = "macos") { "windows" } else { "macos" };
        let raw: KeymapFile = json5::from_str(&format!(
            r#"{{
                "FilePane && !editing": {{ "enter": "file_pane::Activate" }},
                "{other}: FilePane": {{ "enter": null }},
                "{PLATFORM}: Workspace": {{ "f1": "workspace::Help" }}
            }}"#
        ))
        .expect("fixture must parse");
        let resolved = resolve_platforms(raw);

        // A predicate context with no platform selector is left alone…
        assert!(resolved["FilePane && !editing"]["enter"].is_some());
        // …a section for another platform contributes nothing…
        assert!(!resolved.contains_key("FilePane"));
        // …and one for this platform lands on its context.
        assert!(resolved["Workspace"]["f1"].is_some());
    }
}

fn user_keymap() -> anyhow::Result<Option<KeymapFile>> {
    let path = crate::config::data_dir()?.join("keybindings.json5");
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)?;
    Ok(Some(json5::from_str(&raw)?))
}
