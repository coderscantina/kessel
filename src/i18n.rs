//! Localization: flat key → string tables in JSON5, one file per locale.
//!
//! Built-in locales ship compiled-in (`assets/i18n/*.json5`); users can add
//! or override locales in `<data_dir>/i18n/<locale>.json5`. Lookup falls back
//! to English, then to the key itself, so partial translations are safe.
//! `ui.locale` in config.json5 picks the locale ("system" resolves from the
//! environment) and hot-reloads like every other config value.

use std::collections::HashMap;
use std::sync::RwLock;

use gpui::SharedString;

const EN: &str = include_str!("../assets/i18n/en.json5");
const BUILTIN: &[(&str, &str)] = &[("en", EN), ("de", include_str!("../assets/i18n/de.json5"))];

static STRINGS: RwLock<Option<HashMap<String, String>>> = RwLock::new(None);

/// Locales the preferences UI can offer (built-in plus user-provided).
pub fn available_locales() -> Vec<String> {
    let mut locales: Vec<String> = BUILTIN.iter().map(|(name, _)| name.to_string()).collect();
    if let Ok(dir) = crate::config::data_dir() {
        if let Ok(entries) = std::fs::read_dir(dir.join("i18n")) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "json5") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if !locales.iter().any(|known| known == stem) {
                            locales.push(stem.to_string());
                        }
                    }
                }
            }
        }
    }
    locales.sort();
    locales
}

/// Resolve "system" to a concrete locale tag from the environment
/// (`LC_ALL` / `LC_MESSAGES` / `LANG`, e.g. "de_AT.UTF-8" → "de").
fn system_locale() -> String {
    for var in ["LC_ALL", "LC_MESSAGES", "LANG"] {
        if let Ok(value) = std::env::var(var) {
            let tag = value.split(['.', '@']).next().unwrap_or("");
            let lang = tag.split(['_', '-']).next().unwrap_or("");
            if !lang.is_empty() && lang != "C" && lang != "POSIX" {
                return lang.to_lowercase();
            }
        }
    }
    "en".to_string()
}

fn parse(source: &str, origin: &str) -> HashMap<String, String> {
    match json5::from_str::<HashMap<String, String>>(source) {
        Ok(map) => map,
        Err(error) => {
            tracing::error!(%error, origin, "invalid i18n table; ignoring");
            HashMap::new()
        }
    }
}

/// (Re)load the string table for `locale` ("system" | "en" | "de" | …).
/// English is always the base; the chosen locale (built-in, then the user
/// file) merges over it.
pub fn init(locale: &str) {
    let locale = if locale == "system" {
        system_locale()
    } else {
        locale.to_lowercase()
    };

    let mut table = parse(EN, "builtin en");
    if locale != "en" {
        if let Some((_, source)) = BUILTIN.iter().find(|(name, _)| *name == locale) {
            table.extend(parse(source, "builtin"));
        }
    }
    if let Ok(dir) = crate::config::data_dir() {
        let path = dir.join("i18n").join(format!("{locale}.json5"));
        if let Ok(raw) = std::fs::read_to_string(&path) {
            table.extend(parse(&raw, &path.display().to_string()));
        }
    }
    *STRINGS.write().unwrap() = Some(table);
}

/// Look up a UI string. Unknown keys return the key itself (and log once at
/// debug level), so a missing translation never breaks rendering.
pub fn t(key: &str) -> SharedString {
    let strings = STRINGS.read().unwrap();
    match strings.as_ref().and_then(|table| table.get(key)) {
        Some(value) => value.clone().into(),
        None => {
            tracing::debug!(key, "missing i18n key");
            key.to_string().into()
        }
    }
}

/// `t(key)` with `{placeholder}` substitution:
/// `tf("about.version", &[("version", "0.1.0")])`.
pub fn tf(key: &str, args: &[(&str, &str)]) -> SharedString {
    let mut out = t(key).to_string();
    for (name, value) in args {
        out = out.replace(&format!("{{{name}}}"), value);
    }
    out.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_tables_parse_and_share_keys() {
        let en = parse(EN, "en");
        assert!(!en.is_empty());
        for (name, source) in BUILTIN {
            let table = json5::from_str::<HashMap<String, String>>(source)
                .unwrap_or_else(|e| panic!("{name}.json5 must parse: {e}"));
            for key in table.keys() {
                assert!(en.contains_key(key), "`{key}` in {name} missing from en");
            }
        }
    }

    #[test]
    fn lookup_falls_back_to_key() {
        init("en");
        assert_eq!(t("menu.file"), "File");
        assert_eq!(t("no.such.key"), "no.such.key");
        assert!(tf("about.version", &[("version", "1.0")]).contains("1.0"));
    }
}
