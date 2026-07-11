//! SFTP operations OpenDAL doesn't expose: unix permission bits (list/stat)
//! and chmod. Runs its own short-lived session over the same site options
//! OpenDAL's sftp service uses (system ssh; key-file or ssh-agent auth), so
//! nothing here needs extra configuration.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use openssh_sftp_client::openssh::{KnownHosts, SessionBuilder};
use openssh_sftp_client::metadata::{Permissions, RawPermissions};
use openssh_sftp_client::{Sftp, SftpOptions};
use smol::stream::StreamExt as _;

/// Open one sftp session from a site's resolved operator options.
async fn connect(options: &HashMap<String, String>) -> Result<Sftp> {
    let endpoint = options
        .get("endpoint")
        .context("sftp site has no endpoint")?;
    let mut builder = SessionBuilder::default();
    if let Some(user) = options.get("user") {
        builder.user(user.clone());
    }
    if let Some(key) = options.get("key").filter(|key| !key.is_empty()) {
        builder.keyfile(key);
    }
    let known_hosts = match options.get("known_hosts_strategy").map(String::as_str) {
        Some("accept") => KnownHosts::Accept,
        Some("add") => KnownHosts::Add,
        _ => KnownHosts::Strict,
    };
    builder.known_hosts_check(known_hosts);

    let session = builder
        .connect(endpoint)
        .await
        .with_context(|| format!("ssh connect to `{endpoint}`"))?;
    Sftp::from_session(session, SftpOptions::default())
        .await
        .context("starting sftp subsystem")
}

/// Site path for an operator-relative path: rooted under the site's `root`
/// option; an unset/empty root leaves it home-relative, like OpenDAL does.
fn site_path(options: &HashMap<String, String>, rel: &str) -> PathBuf {
    let rel = rel.trim_end_matches('/');
    match options.get("root").map(String::as_str) {
        Some(root) if !root.is_empty() => Path::new(root).join(rel),
        _ => PathBuf::from(rel),
    }
}

/// Permission bits (mode & 0o7777) for every entry of one directory,
/// keyed by file name.
pub async fn list_permissions(
    options: &HashMap<String, String>,
    dir: &str,
) -> Result<HashMap<String, u32>> {
    let sftp = connect(options).await?;
    let mut fs = sftp.fs();
    let mut entries = Box::pin(
        fs.open_dir(site_path(options, dir))
            .await
            .with_context(|| format!("opening `{dir}`"))?
            .read_dir(),
    );

    let mut modes = HashMap::new();
    while let Some(entry) = entries.next().await {
        let entry = entry.context("reading sftp directory")?;
        if let Some(permissions) = entry.metadata().permissions() {
            let name = entry.filename().to_string_lossy().to_string();
            modes.insert(name, permissions.as_raw().bits());
        }
    }
    Ok(modes)
}

/// One entry's permission bits, for pre-filling the chmod dialog.
pub async fn stat_mode(options: &HashMap<String, String>, path: &str) -> Result<Option<u32>> {
    let sftp = connect(options).await?;
    let meta = sftp
        .fs()
        .metadata(site_path(options, path))
        .await
        .with_context(|| format!("stat `{path}`"))?;
    Ok(meta.permissions().map(|permissions| permissions.as_raw().bits()))
}

/// Set an entry's access + modified time (transfer timestamp preservation).
pub async fn set_mtime(
    options: &HashMap<String, String>,
    path: &str,
    mtime: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    use openssh_sftp_client::metadata::MetaDataBuilder;
    use openssh_sftp_client::UnixTimeStamp;

    let system_time = std::time::SystemTime::from(mtime);
    let stamp = UnixTimeStamp::new(system_time)
        .map_err(|error| anyhow::anyhow!("mtime out of sftp range: {error}"))?;
    let sftp = connect(options).await?;
    let metadata = MetaDataBuilder::new().time(stamp, stamp).create();
    sftp.fs()
        .set_metadata(site_path(options, path), metadata)
        .await
        .with_context(|| format!("setting mtime on `{path}`"))?;
    Ok(())
}

/// chmod one entry.
pub async fn chmod(options: &HashMap<String, String>, path: &str, mode: u32) -> Result<()> {
    let sftp = connect(options).await?;
    let permissions = Permissions::from_raw(RawPermissions::from_bits_truncate(mode));
    sftp.fs()
        .set_permissions(site_path(options, path), permissions)
        .await
        .with_context(|| format!("chmod `{path}`"))?;
    Ok(())
}
