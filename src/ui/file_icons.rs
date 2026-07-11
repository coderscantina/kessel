//! File-type icons, Zed icon-theme compatible: filename/suffix maps to an
//! icon *key*, keys map to SVG paths.
//!
//! The built-in set uses generic Lucide category glyphs (ISC-licensed — brand
//! logos are deliberately not vendored), rendered as monochrome masks
//! recolored by the theme. Unpack a Zed icon-theme extension (e.g.
//! `material-icon-theme`) into `<data_dir>/extensions/<id>/` and select one of
//! its themes by name via `ui.icon_theme` in config.json5; its mappings merge
//! over the built-ins, and its SVGs render in full color.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use gpui::{
    img, svg, AnyElement, App, Global, Hsla, IntoElement as _, Pixels, SharedString, Styled as _,
};

use crate::config::UiConfig;
use crate::zed;

/// Full-filename matches (checked before suffixes), Zed's `file_stems`.
const STEMS: &[(&str, &str)] = &[
    ("Dockerfile", "code"),
    ("Containerfile", "code"),
    ("Makefile", "code"),
    ("Justfile", "code"),
    ("Cargo.lock", "lock"),
    ("yarn.lock", "lock"),
    ("bun.lockb", "lock"),
    ("Gemfile.lock", "lock"),
    ("composer.lock", "lock"),
    ("LICENSE", "document"),
];

/// suffix → icon key. Compound suffixes (`test.ts`) work via the progressive
/// dot-stripping in [`FileIcons::icon`].
const SUFFIXES: &[(&str, &[&str])] = &[
    (
        "code",
        &[
            "rs", "py", "go", "rb", "php", "java", "c", "h", "cpp", "cc", "cxx", "hpp", "swift",
            "zig", "kt", "cs", "ex", "exs", "erl", "hs", "lua", "pl", "r", "scala", "dart", "m",
            "mm", "ts", "tsx", "js", "jsx", "cjs", "mjs", "vue", "svelte", "astro", "html", "htm",
            "css", "scss", "sass", "less",
        ],
    ),
    (
        "data",
        &["json", "jsonc", "json5", "yaml", "yml", "toml", "xml", "plist", "graphql"],
    ),
    ("document", &["md", "markdown", "mdx", "rst", "adoc", "txt", "rtf", "doc", "docx", "odt", "pdf"]),
    (
        "image",
        &["png", "jpg", "jpeg", "gif", "webp", "svg", "ico", "bmp", "tiff", "heic", "avif", "psd"],
    ),
    ("audio", &["mp3", "wav", "flac", "ogg", "m4a", "aac", "opus", "wma"]),
    ("video", &["mp4", "mkv", "mov", "avi", "webm", "m4v", "wmv"]),
    (
        "archive",
        &["zip", "tar", "gz", "bz2", "xz", "7z", "rar", "tgz", "zst", "dmg", "iso"],
    ),
    ("font", &["ttf", "otf", "woff", "woff2"]),
    ("lock", &["lock"]),
    (
        "settings",
        &["conf", "ini", "cfg", "env", "editorconfig", "properties", "gitconfig"],
    ),
    ("database", &["db", "sqlite", "sqlite3", "sql", "csv", "tsv", "parquet"]),
    (
        "terminal",
        &["sh", "bash", "zsh", "fish", "ps1", "nu", "bat", "cmd", "zshrc", "bashrc", "profile"],
    ),
    ("vcs", &["gitignore", "gitattributes", "gitmodules", "gitkeep"]),
];

/// icon key → embedded SVG path.
const ICONS: &[(&str, &str)] = &[
    ("code", "icons/file_icons/file-code.svg"),
    ("data", "icons/file_icons/braces.svg"),
    ("document", "icons/file_icons/book.svg"),
    ("text", "icons/file_icons/file-text.svg"),
    ("image", "icons/file_icons/image.svg"),
    ("audio", "icons/file_icons/music.svg"),
    ("video", "icons/file_icons/film.svg"),
    ("archive", "icons/file_icons/archive.svg"),
    ("font", "icons/file_icons/type.svg"),
    ("lock", "icons/file_icons/lock.svg"),
    ("settings", "icons/file_icons/sliders.svg"),
    ("database", "icons/file_icons/database.svg"),
    ("terminal", "icons/file_icons/terminal.svg"),
    ("vcs", "icons/file_icons/git-branch.svg"),
    ("default", "icons/file_icons/file.svg"),
    ("bucket", "icons/file_icons/bucket.svg"),
    ("file-symlink", "icons/file_icons/file-symlink.svg"),
    ("folder-symlink", "icons/file_icons/folder-symlink.svg"),
];

/// Render an icon path: built-in icons (asset-relative paths) are monochrome
/// masks tinted by the theme; icon-theme SVGs (absolute paths) keep their own
/// colors.
pub fn icon_element(path: SharedString, size: Pixels, color: Hsla) -> AnyElement {
    if Path::new(path.as_ref()).is_absolute() {
        img(path).flex_none().w(size).h(size).into_any_element()
    } else {
        svg()
            .path(path)
            .flex_none()
            .size(size)
            .text_color(color)
            .into_any_element()
    }
}

pub struct FileIcons {
    stems: HashMap<String, String>,
    suffixes: HashMap<String, String>,
    icons: HashMap<String, SharedString>,
    dir_collapsed: SharedString,
    dir_expanded: SharedString,
    /// Folder-name-specific icons from the icon theme
    /// (`named_directory_icons`, schema v0.3.0): name → (collapsed, expanded).
    named_dirs: HashMap<String, (Option<SharedString>, Option<SharedString>)>,
}

impl Global for FileIcons {}

impl FileIcons {
    pub fn init(ui: &UiConfig, cx: &mut App) {
        Self::reload(ui, cx);
    }

    /// (Re)build the icon maps: built-ins, plus the selected Zed icon theme
    /// merged on top.
    pub fn reload(ui: &UiConfig, cx: &mut App) {
        let mut this = Self::builtin();
        if let Some(name) = ui.icon_theme.as_deref() {
            match find_icon_theme(name) {
                Some((root, theme)) => this.merge(&root, theme),
                None => tracing::warn!(
                    name,
                    "icon theme not found in <data_dir>/extensions/*/icon_themes"
                ),
            }
        }
        cx.set_global(this);
    }

    pub fn get(cx: &App) -> &Self {
        cx.global::<Self>()
    }

    fn builtin() -> Self {
        let stems = STEMS
            .iter()
            .map(|(stem, key)| (stem.to_string(), key.to_string()))
            .collect();
        let suffixes = SUFFIXES
            .iter()
            .flat_map(|(key, suffixes)| {
                suffixes.iter().map(|s| (s.to_string(), key.to_string()))
            })
            .collect();
        let icons = ICONS
            .iter()
            .map(|(key, path)| (key.to_string(), SharedString::from(*path)))
            .collect();
        Self {
            stems,
            suffixes,
            icons,
            dir_collapsed: "icons/file_icons/folder.svg".into(),
            dir_expanded: "icons/file_icons/folder-open.svg".into(),
            named_dirs: HashMap::new(),
        }
    }

    fn merge(&mut self, extension_root: &Path, theme: zed::IconTheme) {
        // Zed resolves icon paths (`./icons/rust.svg`) against the extension root.
        let resolve = |path: &str| -> SharedString {
            let path = path.strip_prefix("./").unwrap_or(path);
            extension_root.join(path).to_string_lossy().into_owned().into()
        };
        self.stems.extend(theme.file_stems);
        self.suffixes.extend(theme.file_suffixes);
        self.icons.extend(
            theme
                .file_icons
                .into_iter()
                .map(|(key, def)| (key, resolve(&def.path))),
        );
        if let Some(dirs) = theme.directory_icons {
            if let Some(p) = dirs.collapsed {
                self.dir_collapsed = resolve(&p);
            }
            if let Some(p) = dirs.expanded {
                self.dir_expanded = resolve(&p);
            }
        }
        self.named_dirs.extend(
            theme.named_directory_icons.into_iter().map(|(name, pair)| {
                (
                    name,
                    (
                        pair.collapsed.as_deref().map(&resolve),
                        pair.expanded.as_deref().map(&resolve),
                    ),
                )
            }),
        );
    }

    fn resolve(&self, key: &str) -> SharedString {
        self.icons
            .get(key)
            .or_else(|| self.icons.get("default"))
            .cloned()
            .unwrap_or_else(|| "icons/file_icons/file.svg".into())
    }

    /// Resolve an icon for a file name, Zed-style: full-name match first,
    /// then progressively stripped compound suffixes (`a.test.ts` tries
    /// `test.ts`, then `ts`; `.gitignore` tries `gitignore`), then default.
    pub fn icon(&self, file_name: &str) -> SharedString {
        if let Some(key) = self.stems.get(file_name) {
            return self.resolve(key);
        }
        let mut rest = file_name;
        while let Some((_, suffix)) = rest.split_once('.') {
            if let Some(key) = self.suffixes.get(suffix) {
                return self.resolve(key);
            }
            rest = suffix;
        }
        self.resolve("default")
    }

    /// Bucket rows (account-root S3 listings). Icon themes can override the
    /// `bucket` key like any other.
    pub fn bucket(&self) -> SharedString {
        self.resolve("bucket")
    }

    /// Symlink rows, by resolved target kind (`file-symlink` /
    /// `folder-symlink` keys).
    pub fn symlink(&self, is_dir: bool) -> SharedString {
        self.resolve(if is_dir { "folder-symlink" } else { "file-symlink" })
    }

    pub fn folder(&self, expanded: bool) -> SharedString {
        if expanded {
            self.dir_expanded.clone()
        } else {
            self.dir_collapsed.clone()
        }
    }

    /// Folder icon for a specific directory name: the theme's
    /// `named_directory_icons` entry (exact, then lowercased) when present,
    /// else the generic folder pair.
    pub fn folder_named(&self, name: &str, expanded: bool) -> SharedString {
        let named = self
            .named_dirs
            .get(name)
            .or_else(|| self.named_dirs.get(&name.to_lowercase()));
        named
            .and_then(|(collapsed, expanded_icon)| {
                if expanded {
                    expanded_icon.clone().or_else(|| collapsed.clone())
                } else {
                    collapsed.clone()
                }
            })
            .unwrap_or_else(|| self.folder(expanded))
    }
}

/// Names of all installed Zed icon themes across unpacked extensions in
/// `<data_dir>/extensions/*/icon_themes/*.json`, sorted, for the Preferences
/// icon-theme picker.
pub fn installed_icon_theme_names() -> Vec<String> {
    let mut names = Vec::new();
    let Ok(extensions) = crate::config::data_dir().map(|dir| dir.join("extensions")) else {
        return names;
    };
    let Ok(entries) = fs::read_dir(extensions) else {
        return names;
    };
    for extension in entries.flatten() {
        let Ok(files) = fs::read_dir(extension.path().join("icon_themes")) else {
            continue;
        };
        for file in files.flatten() {
            let path = file.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            if let Ok(family) = fs::read_to_string(&path)
                .map_err(anyhow::Error::from)
                .and_then(|json| {
                    serde_json::from_str::<zed::IconThemeFamily>(&json).map_err(Into::into)
                })
            {
                names.extend(family.themes.into_iter().map(|theme| theme.name));
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

/// Find a Zed icon theme by entry name across all unpacked extensions in
/// `<data_dir>/extensions/*/icon_themes/*.json`. Returns the extension root
/// (base for icon paths) and the parsed theme.
fn find_icon_theme(name: &str) -> Option<(PathBuf, zed::IconTheme)> {
    let extensions = crate::config::data_dir().ok()?.join("extensions");
    for extension in fs::read_dir(extensions).ok()?.flatten() {
        let root = extension.path();
        let Ok(files) = fs::read_dir(root.join("icon_themes")) else {
            continue;
        };
        for file in files.flatten() {
            let path = file.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let family = fs::read_to_string(&path)
                .map_err(anyhow::Error::from)
                .and_then(|json| {
                    serde_json::from_str::<zed::IconThemeFamily>(&json).map_err(Into::into)
                });
            match family {
                Ok(family) => {
                    if let Some(theme) = family.themes.into_iter().find(|t| t.name == name) {
                        return Some((root, theme));
                    }
                }
                Err(error) => tracing::warn!(
                    %error,
                    file = %path.display(),
                    "ignoring invalid icon theme file"
                ),
            }
        }
    }
    None
}
