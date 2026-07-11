//! In-app auto-update against GitHub releases.
//!
//! The release CI publishes a stable manifest — `latest.json` — next to each
//! DMG. kessel fetches it (a cheap redirect to the newest release), compares
//! the advertised version to the running build, and — when newer and running
//! from a writable `.app` bundle — downloads the DMG, verifies its SHA-256,
//! and swaps the new bundle in place. When not installed as a writable bundle
//! (e.g. `cargo run`, or a read-only mount) it falls back to revealing the
//! downloaded DMG in Finder for a manual drag-install.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context as _, Result};
use tokio::io::AsyncWriteExt as _;

use crate::config::UpdateConfig;

/// One release, as advertised by the manifest (`latest.json`). Shape:
/// `{"version","tag","url","sha256","pub_date"}`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpdateInfo {
    pub version: String,
    #[allow(dead_code)]
    pub tag: String,
    pub url: String,
    pub sha256: String,
    #[serde(default)]
    pub pub_date: String,
}

impl UpdateInfo {
    /// The `pub_date` reduced to its calendar day (`2026-07-10T…` → `2026-07-10`),
    /// or empty when absent — used for the "Released …" line in the dialog.
    pub fn pub_date_display(&self) -> String {
        self.pub_date
            .split('T')
            .next()
            .unwrap_or("")
            .to_string()
    }
}

/// Outcome of an install attempt.
pub enum InstallOutcome {
    /// The running bundle was replaced; the caller should relaunch and quit.
    ReadyToRelaunch { app_path: PathBuf },
    /// Not installed as a writable bundle — the DMG was revealed in Finder for
    /// a manual drag-install.
    ManualInstall,
}

/// Parse a dotted-numeric version ("26.7.10") into a comparable tuple. Dev
/// versions like "0.1.0" parse too and simply sort below any calendar release.
fn parse_version(text: &str) -> Option<(u64, u64, u64)> {
    let mut parts = text.trim().split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    Some((major, minor, patch))
}

/// Whether `candidate` is a strictly newer release than `current`. Anything
/// that doesn't parse as a dotted-numeric version is treated as "not newer".
fn is_newer(candidate: &str, current: &str) -> bool {
    match (parse_version(candidate), parse_version(current)) {
        (Some(new), Some(cur)) => new > cur,
        _ => false,
    }
}

/// Fetch and parse the manifest, returning the release only when it is newer
/// than the running build. `Ok(None)` means "up to date"; `Err` means the
/// manifest couldn't be reached or parsed.
pub async fn fetch_latest(cfg: &UpdateConfig) -> Result<Option<UpdateInfo>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building the update http client")?;
    // Follow the convention in oauth.rs: read the body as text, then serde_json.
    let body = client
        .get(&cfg.url)
        .send()
        .await
        .context("fetching the update manifest")?
        .error_for_status()
        .context("update manifest request failed")?
        .text()
        .await
        .context("reading the update manifest")?;
    let info: UpdateInfo =
        serde_json::from_str(&body).context("parsing the update manifest (latest.json)")?;
    Ok(is_newer(&info.version, env!("CARGO_PKG_VERSION")).then_some(info))
}

/// Background-check wrapper: swallows errors (being offline is normal) and
/// logs them at debug. Returns `Some` only when a newer release is available.
pub async fn check_for_update(cfg: &UpdateConfig) -> Option<UpdateInfo> {
    if !cfg.check {
        return None;
    }
    match fetch_latest(cfg).await {
        Ok(info) => info,
        Err(error) => {
            tracing::debug!(%error, "update check failed (offline?)");
            None
        }
    }
}

/// Download the release DMG, verify its SHA-256, and try to swap the running
/// bundle in place. Falls back to revealing the DMG in Finder when the app
/// isn't installed as a writable `.app` bundle. Runs on the tokio runtime.
pub async fn download_and_install(info: &UpdateInfo) -> Result<InstallOutcome> {
    let dir = std::env::temp_dir().join(format!("kessel-update-{}", info.version));
    std::fs::create_dir_all(&dir).context("creating the update temp dir")?;
    let dmg = dir.join(format!("Kessel-{}.dmg", info.version));

    download(&info.url, &dmg).await?;
    verify_sha256(&dmg, &info.sha256)?;

    match running_app_bundle() {
        Some(app) if bundle_parent_writable(&app) => {
            install_in_place(&dmg, &app)?;
            tracing::info!(version = %info.version, app = %app.display(), "update installed");
            Ok(InstallOutcome::ReadyToRelaunch { app_path: app })
        }
        _ => {
            tracing::info!("not a writable .app bundle; revealing the DMG for manual install");
            reveal_in_finder(&dmg);
            Ok(InstallOutcome::ManualInstall)
        }
    }
}

/// Relaunch the app from `app_path` in a fresh process. The caller quits the
/// current process right after.
pub fn relaunch(app_path: &Path) {
    if let Err(error) = Command::new("open").arg("-n").arg(app_path).spawn() {
        tracing::error!(%error, app = %app_path.display(), "relaunch failed");
    }
}

/// Stream the DMG to `dest`, logging coarse progress.
async fn download(url: &str, dest: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
        .context("building the download http client")?;
    let mut response = client
        .get(url)
        .send()
        .await
        .context("starting the DMG download")?
        .error_for_status()
        .context("DMG download request failed")?;
    let total = response.content_length();
    let mut file = tokio::fs::File::create(dest)
        .await
        .with_context(|| format!("creating `{}`", dest.display()))?;
    let mut downloaded: u64 = 0;
    let mut next_log: u64 = 8 * 1024 * 1024;
    while let Some(chunk) = response.chunk().await.context("reading the DMG stream")? {
        file.write_all(&chunk).await.context("writing the DMG")?;
        downloaded += chunk.len() as u64;
        if downloaded >= next_log {
            tracing::debug!(downloaded, ?total, "downloading update");
            next_log += 8 * 1024 * 1024;
        }
    }
    file.flush().await.context("flushing the DMG")?;
    tracing::info!(bytes = downloaded, "update DMG downloaded");
    Ok(())
}

/// Verify the file's SHA-256 against the manifest's hex digest.
fn verify_sha256(path: &Path, expected: &str) -> Result<()> {
    use sha2::{Digest as _, Sha256};
    let bytes = std::fs::read(path).with_context(|| format!("reading `{}`", path.display()))?;
    let digest = Sha256::digest(&bytes);
    let actual: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    if !actual.eq_ignore_ascii_case(expected.trim()) {
        bail!("SHA-256 mismatch: manifest {expected}, downloaded {actual}");
    }
    Ok(())
}

/// The running app's `.app` bundle: `current_exe` is `…/Kessel.app/Contents/
/// MacOS/kessel`, so three parents up is the bundle. `None` when that path
/// doesn't end in `.app` (e.g. `cargo run`).
fn running_app_bundle() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let app = exe.parent()?.parent()?.parent()?.to_path_buf();
    (app.extension().is_some_and(|ext| ext == "app")).then_some(app)
}

/// Probe whether the bundle's parent directory is writable (can we swap the
/// bundle for a fresh copy?) by creating and deleting a temp file there.
fn bundle_parent_writable(app: &Path) -> bool {
    let Some(parent) = app.parent() else {
        return false;
    };
    let probe = parent.join(format!(".kessel-write-probe-{}", std::process::id()));
    match std::fs::File::create(&probe) {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

/// Mount the DMG, copy the new bundle over `target_app` with an atomic-ish
/// swap, and detach the mount.
fn install_in_place(dmg: &Path, target_app: &Path) -> Result<()> {
    let mount = attach_dmg(dmg)?;
    let result = swap_bundle(&mount, target_app);
    detach_dmg(&mount);
    result
}

/// `hdiutil attach` the DMG read-only and return its `/Volumes/…` mount point.
fn attach_dmg(dmg: &Path) -> Result<PathBuf> {
    let output = Command::new("hdiutil")
        .args(["attach", "-nobrowse", "-readonly"])
        .arg(dmg)
        .output()
        .context("running hdiutil attach")?;
    if !output.status.success() {
        bail!(
            "hdiutil attach failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    // Columns are tab-separated; the mount point is the trailing /Volumes/… one.
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| line.rsplit('\t').next().map(str::trim))
        .filter(|token| token.starts_with("/Volumes/"))
        .next_back()
        .map(PathBuf::from)
        .context("no mount point in hdiutil output")
}

fn detach_dmg(mount: &Path) {
    let _ = Command::new("hdiutil")
        .arg("detach")
        .arg(mount)
        .arg("-quiet")
        .status();
}

/// Copy the `.app` out of the read-only mount to a staging path, then swap it
/// over the current bundle (old moved aside, new moved in, old removed). On
/// failure the previous bundle is restored.
fn swap_bundle(mount: &Path, target_app: &Path) -> Result<()> {
    let new_app = find_app(mount)?;
    let parent = target_app.parent().context("app bundle has no parent")?;
    let pid = std::process::id();
    let staging = parent.join(format!(".kessel-new-{pid}.app"));
    let backup = parent.join(format!(".kessel-old-{pid}.app"));
    let _ = std::fs::remove_dir_all(&staging);
    let _ = std::fs::remove_dir_all(&backup);

    // ditto preserves bundle metadata/extended attributes and dereferences the
    // read-only DMG into a writable copy we own.
    let status = Command::new("ditto")
        .arg(&new_app)
        .arg(&staging)
        .status()
        .context("running ditto")?;
    if !status.success() {
        let _ = std::fs::remove_dir_all(&staging);
        bail!("ditto failed to copy the new app bundle");
    }

    std::fs::rename(target_app, &backup).context("moving the current app aside")?;
    match std::fs::rename(&staging, target_app) {
        Ok(()) => {
            let _ = std::fs::remove_dir_all(&backup);
            Ok(())
        }
        Err(error) => {
            // Restore the previous bundle so the app stays launchable.
            let _ = std::fs::rename(&backup, target_app);
            let _ = std::fs::remove_dir_all(&staging);
            Err(anyhow::Error::new(error).context("installing the new app bundle"))
        }
    }
}

/// The first `.app` directory at the root of the mounted DMG.
fn find_app(mount: &Path) -> Result<PathBuf> {
    for entry in std::fs::read_dir(mount).context("reading the mounted DMG")? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "app") {
            return Ok(path);
        }
    }
    bail!("no .app bundle found in the mounted DMG");
}

fn reveal_in_finder(path: &Path) {
    let _ = Command::new("open").arg("-R").arg(path).status();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_compare_versions() {
        assert_eq!(parse_version("26.7.10"), Some((26, 7, 10)));
        assert_eq!(parse_version("0.1.0"), Some((0, 1, 0)));
        assert_eq!(parse_version(" 1.2.3 "), Some((1, 2, 3)));
        assert_eq!(parse_version("nope"), None);
        assert_eq!(parse_version("1.2"), None);

        // A calendar release is newer than the dev version and than an older tag.
        assert!(is_newer("26.7.10", "0.1.0"));
        assert!(is_newer("26.7.10", "26.7.9"));
        assert!(is_newer("26.8.1", "26.7.30"));
        // Same or older is not newer, and unparseable candidates never win.
        assert!(!is_newer("26.7.10", "26.7.10"));
        assert!(!is_newer("26.7.9", "26.7.10"));
        assert!(!is_newer("garbage", "0.1.0"));
    }

    #[test]
    fn parse_latest_json_fixture() {
        let fixture = r#"{
            "version": "26.7.10",
            "tag": "v26.7.10",
            "url": "https://github.com/coderscantina/kessel/releases/download/v26.7.10/Kessel-26.7.10.dmg",
            "sha256": "abc123",
            "pub_date": "2026-07-10T12:00:00Z"
        }"#;
        let info: UpdateInfo = serde_json::from_str(fixture).unwrap();
        assert_eq!(info.version, "26.7.10");
        assert_eq!(info.tag, "v26.7.10");
        assert!(info.url.ends_with("Kessel-26.7.10.dmg"));
        assert_eq!(info.sha256, "abc123");
        assert_eq!(info.pub_date_display(), "2026-07-10");
        // pub_date is optional.
        let minimal = r#"{"version":"26.7.10","tag":"v26.7.10","url":"u","sha256":"h"}"#;
        let info: UpdateInfo = serde_json::from_str(minimal).unwrap();
        assert_eq!(info.pub_date_display(), "");
    }
}
