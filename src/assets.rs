//! Asset bundle. Embedded assets ship in the binary; anything not embedded
//! falls back to `<data_dir>/assets/<path>` on disk so user icon packs and
//! theme extras work without recompiling (see `ui::file_icons`).
//!
//! gpui-component ships no SVGs of its own — the app must embed every icon
//! its `IconName` enum can reference, or icons render blank. `build.rs`
//! generates the table from all of `assets/icons/` (see
//! `scripts/sync-icons.sh` to refresh the set from Lucide).
//!
//! Lucide icons, ISC license — see assets/icons/LICENSE.

use std::borrow::Cow;

use gpui::{AssetSource, Result, SharedString};

include!(concat!(env!("OUT_DIR"), "/embedded_icons.rs"));

const EMBEDDED: &[(&str, &[u8])] = EMBEDDED_ICONS;

pub struct Assets;

impl AssetSource for Assets {
    fn load(&self, path: &str) -> Result<Option<Cow<'static, [u8]>>> {
        if let Some((_, bytes)) = EMBEDDED.iter().find(|(p, _)| *p == path) {
            return Ok(Some(Cow::Borrowed(*bytes)));
        }
        // Absolute paths: SVGs from unpacked Zed icon-theme extensions
        // (`<data_dir>/extensions/...`, see ui::file_icons).
        if std::path::Path::new(path).is_absolute() {
            return Ok(std::fs::read(path).ok().map(Cow::Owned));
        }
        // On-disk fallback for user-provided assets (icon packs etc.).
        if let Ok(dir) = crate::config::data_dir() {
            let file = dir.join("assets").join(path);
            if file.starts_with(dir.join("assets")) && file.is_file() {
                return Ok(Some(Cow::Owned(std::fs::read(file)?)));
            }
        }
        Ok(None)
    }

    fn list(&self, path: &str) -> Result<Vec<SharedString>> {
        Ok(EMBEDDED
            .iter()
            .filter(|(p, _)| p.starts_with(path))
            .map(|(p, _)| (*p).into())
            .collect())
    }
}
