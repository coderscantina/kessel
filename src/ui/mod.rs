pub mod bookmarks;
pub mod command_bar;
pub mod connect;
pub mod file_icons;
pub mod file_pane;
pub mod logs;
pub mod pickers;
pub mod preferences;
pub mod sync;
pub mod theme_browser;
pub mod transfers;

use chrono::{DateTime, Local, Utc};
use gpui::Hsla;
use gpui_component::{Icon, IconName};

/// Named icons a bookmark or folder can pick from (stored by name in SQLite,
/// rendered from the embedded Lucide SVGs in `assets/icons/`).
pub const BOOKMARK_ICONS: &[(&str, &str)] = &[
    // Places & structure.
    ("globe", "icons/globe.svg"),
    ("folder", "icons/folder.svg"),
    ("home", "icons/home.svg"),
    ("building", "icons/building-2.svg"),
    ("map", "icons/map.svg"),
    ("briefcase", "icons/briefcase.svg"),
    // Infrastructure.
    ("server", "icons/server.svg"),
    ("database", "icons/database.svg"),
    ("cloud", "icons/cloud.svg"),
    ("drive", "icons/hard-drive.svg"),
    ("cpu", "icons/cpu.svg"),
    ("terminal", "icons/square-terminal.svg"),
    ("dashboard", "icons/layout-dashboard.svg"),
    // Content.
    ("archive", "icons/archive.svg"),
    ("package", "icons/package.svg"),
    ("inbox", "icons/inbox.svg"),
    ("book", "icons/book-open.svg"),
    ("newspaper", "icons/newspaper.svg"),
    ("code", "icons/code.svg"),
    ("image", "icons/image.svg"),
    ("camera", "icons/camera.svg"),
    ("music", "icons/music.svg"),
    ("film", "icons/film.svg"),
    ("gamepad", "icons/gamepad.svg"),
    // Security.
    ("lock", "icons/lock.svg"),
    ("key", "icons/key.svg"),
    ("shield", "icons/shield.svg"),
    // Flair.
    ("star", "icons/star.svg"),
    ("heart", "icons/heart.svg"),
    ("rocket", "icons/rocket.svg"),
    ("zap", "icons/zap.svg"),
    ("wrench", "icons/wrench.svg"),
    ("flask", "icons/flask.svg"),
];

/// Named accent colors for bookmarks and folders (stored by name in SQLite).
pub const BOOKMARK_COLORS: &[(&str, u32)] = &[
    ("red", 0xef4444),
    ("rose", 0xf43f5e),
    ("orange", 0xf97316),
    ("amber", 0xf59e0b),
    ("yellow", 0xeab308),
    ("lime", 0x84cc16),
    ("green", 0x22c55e),
    ("emerald", 0x10b981),
    ("teal", 0x14b8a6),
    ("cyan", 0x06b6d4),
    ("sky", 0x0ea5e9),
    ("blue", 0x3b82f6),
    ("indigo", 0x6366f1),
    ("violet", 0x8b5cf6),
    ("purple", 0xa855f7),
    ("fuchsia", 0xd946ef),
    ("pink", 0xec4899),
    ("slate", 0x64748b),
    ("gray", 0x6b7280),
    ("stone", 0x78716c),
];

pub fn bookmark_icon(name: Option<&str>, fallback: IconName) -> Icon {
    name.and_then(|name| {
        BOOKMARK_ICONS
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, path)| Icon::default().path(*path))
    })
    .unwrap_or_else(|| Icon::new(fallback))
}

pub fn bookmark_color(name: Option<&str>) -> Option<Hsla> {
    let name = name?;
    BOOKMARK_COLORS
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, hex)| gpui::rgb(*hex).into())
}

/// Open a URL or local path with the platform's default handler.
pub fn open_path_or_url(target: &str) {
    #[cfg(target_os = "macos")]
    let opener = "open";
    #[cfg(target_os = "linux")]
    let opener = "xdg-open";
    #[cfg(target_os = "windows")]
    let opener = "explorer";
    if let Err(error) = std::process::Command::new(opener).arg(target).spawn() {
        tracing::error!(%error, %target, "opening with the system handler failed");
    }
}

pub fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Compact duration for ETAs: "12s", "4m 05s", "3h 12m".
pub fn human_duration(secs: u64) -> String {
    match secs {
        0..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m {:02}s", secs / 60, secs % 60),
        _ => format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60),
    }
}

pub fn human_time(time: Option<DateTime<Utc>>) -> String {
    match time {
        Some(t) => t.with_timezone(&Local).format("%Y-%m-%d %H:%M").to_string(),
        None => String::new(),
    }
}
