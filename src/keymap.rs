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

/// context → keystroke → action (`None` unbinds a default).
type KeymapFile = HashMap<String, HashMap<String, Option<BindingSpec>>>;

/// Parse defaults, merge the user file over them per (context, keystroke),
/// and register everything with gpui. Called once at startup, after the
/// action types (via `actions!` / `#[derive(Action)]`) are registered.
pub fn init(cx: &mut App) {
    let mut keymap: KeymapFile =
        json5::from_str(DEFAULTS).expect("compiled-in keybindings.json5 must parse");

    match user_keymap() {
        Ok(Some(user)) => {
            for (context, binds) in user {
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
}

fn user_keymap() -> anyhow::Result<Option<KeymapFile>> {
    let path = crate::config::data_dir()?.join("keybindings.json5");
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)?;
    Ok(Some(json5::from_str(&raw)?))
}
