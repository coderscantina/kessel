//! The path model for the machine's own filesystem.
//!
//! kessel addresses every site the same way: a *browse path* is relative to
//! the site's root, uses `/` separators, and carries a trailing `/` on
//! directories. For the built-in "Local" site that root is the filesystem
//! root, so a browse path is just an absolute path with the root chopped off
//! (`/Users/ada/src/` → `Users/ada/src/`).
//!
//! POSIX has exactly one root. Windows has one per volume, so the built-in
//! Local site is rooted at the system drive (`C:\`) and browse paths are
//! drive-relative with forward slashes (`C:\Users\ada` → `Users/ada/`). Other
//! volumes are reachable by adding a "Local filesystem" bookmark rooted at
//! `D:\` — it behaves exactly like the built-in one.

use std::path::{Path, PathBuf};

/// Root of the built-in Local site: `/`, or the system drive on Windows.
#[cfg(not(windows))]
pub fn root() -> String {
    "/".to_string()
}

#[cfg(windows)]
pub fn root() -> String {
    // `SystemDrive` is `C:` (no separator) on a stock install.
    match std::env::var("SystemDrive") {
        Ok(drive) if !drive.is_empty() => format!("{}\\", drive.trim_end_matches(['\\', '/'])),
        _ => "C:\\".to_string(),
    }
}

/// Absolute local path → browse path against `root`: separators normalized to
/// `/`, the root prefix removed, and a trailing `/` when `is_dir`.
///
/// A path outside `root` (another volume, say) keeps its full spelling minus
/// separators — the resulting browse path won't resolve, which is what the
/// caller wants: nothing silently lands on the wrong volume.
pub fn browse_path_in(root: &str, path: &Path, is_dir: bool) -> String {
    let full = path.to_string_lossy().replace('\\', "/");
    let root = root.replace('\\', "/");
    // Drive letters are case-insensitive; the rest of the comparison is a
    // plain prefix match, so lowercase both sides only for the check.
    let rest = if full.to_lowercase().starts_with(&root.to_lowercase()) {
        &full[root.len()..]
    } else {
        full.as_str()
    };
    let mut out = rest.trim_start_matches('/').to_string();
    if is_dir && !out.is_empty() && !out.ends_with('/') {
        out.push('/');
    }
    out
}

/// [`browse_path_in`] against the built-in Local site's root.
pub fn browse_path(path: &Path, is_dir: bool) -> String {
    browse_path_in(&root(), path, is_dir)
}

/// A local directory as a browse path on the built-in Local site.
pub fn dir_browse_path(dir: &Path) -> String {
    browse_path(dir, true)
}

/// Browse path → absolute local path under `root` (the inverse of
/// [`browse_path_in`]).
///
/// Joined by hand rather than with `Path::join`, whose notion of "already
/// ends in a separator" is platform-specific: a `C:\` root would grow a
/// second separator when the joining happens to run on unix (tests, say).
pub fn full_path(root: &str, browse: &str) -> PathBuf {
    let browse = browse.trim_matches('/');
    if browse.is_empty() {
        return PathBuf::from(root);
    }
    let mut out = root.to_string();
    if !out.ends_with(['/', '\\']) {
        out.push(std::path::MAIN_SEPARATOR);
    }
    out.push_str(browse);
    PathBuf::from(out)
}

/// The user's home directory as a browse path on the Local site (empty when
/// the platform won't say where home is).
pub fn home_browse_path() -> String {
    dirs::home_dir().map(|home| dir_browse_path(&home)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_the_root_and_marks_directories() {
        let dir = browse_path_in("/", Path::new("/Users/ada"), true);
        assert_eq!(dir, "Users/ada/");
        let file = browse_path_in("/", Path::new("/Users/ada/a.txt"), false);
        assert_eq!(file, "Users/ada/a.txt");
        // The root itself is the empty browse path, with or without is_dir.
        assert_eq!(browse_path_in("/", Path::new("/"), true), "");
    }

    #[test]
    fn normalizes_windows_paths() {
        let dir = browse_path_in("C:\\", Path::new(r"C:\Users\ada"), true);
        assert_eq!(dir, "Users/ada/");
        // Drive letters compare case-insensitively.
        let file = browse_path_in("C:\\", Path::new(r"c:\Users\ada\a.txt"), false);
        assert_eq!(file, "Users/ada/a.txt");
        // A path on another volume is left whole rather than silently rerooted.
        assert_eq!(browse_path_in("C:\\", Path::new(r"D:\data"), true), "D:/data/");
    }

    #[test]
    fn full_path_inverts_browse_path() {
        assert_eq!(full_path("/", "Users/ada/"), PathBuf::from("/Users/ada"));
        assert_eq!(full_path("C:\\", "Users/ada/"), PathBuf::from("C:\\Users/ada"));
    }
}
