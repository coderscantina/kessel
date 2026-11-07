//! SFTP operations OpenDAL doesn't expose: unix permission bits (list/stat)
//! and chmod. Runs its own short-lived session over the same site options
//! OpenDAL's sftp service uses (system ssh; key-file or ssh-agent auth), so
//! nothing here needs extra configuration.
//!
//! Also home to host key trust: scanning a server's keys, fingerprinting
//! them and appending trusted ones to `~/.ssh/known_hosts`, which is what the
//! system ssh (and so OpenDAL) checks against.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context as _, Result};
use base64::Engine as _;
use sha2::{Digest as _, Sha256};
use sqlx::SqlitePool;
use openssh_sftp_client::openssh::{KnownHosts, SessionBuilder};
use openssh_sftp_client::metadata::{Permissions, RawPermissions};
use openssh_sftp_client::{Sftp, SftpOptions};
use smol::stream::StreamExt as _;

/// Whether this build carries the sftp backend (see `sftp_unsupported.rs`).
pub const SUPPORTED: bool = true;

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
    // Strict unless a site predates the trust dialog and asked for
    // trust-on-first-use; the retired "accept" (ignore changed keys) is
    // treated the same way, never as blind acceptance.
    let known_hosts = match options.get("known_hosts_strategy").map(String::as_str) {
        Some("add" | "accept") => KnownHosts::Add,
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

/// One public host key a server offers, for the trust dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostKey {
    /// `ssh-ed25519`, `ecdsa-sha2-nistp256`, `ssh-rsa`, …
    pub key_type: String,
    /// Base64 key blob, as written in known_hosts.
    pub public_key: String,
    /// `SHA256:…`, the form `ssh` and `ssh-keygen -l` print.
    pub fingerprint: String,
}

/// Host and port an sftp endpoint points at (`ssh://user@host:2222`,
/// `host`, `host:2222`, `[::1]:22`). IPv6 hosts come back without brackets.
pub fn host_port(endpoint: &str) -> Option<(String, u16)> {
    let endpoint = endpoint.trim();
    let with_scheme = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("ssh://{endpoint}")
    };
    let url = url::Url::parse(&with_scheme).ok()?;
    let host = url
        .host_str()?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    (!host.is_empty()).then(|| (host, url.port().unwrap_or(22)))
}

/// The host column of a known_hosts line: `host` on port 22, `[host]:port`
/// otherwise, matching what ssh itself looks up.
fn known_hosts_host(host: &str, port: u16) -> String {
    if port == 22 {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    }
}

/// `SHA256:<base64, no padding>` of a base64 key blob.
fn fingerprint(public_key: &str) -> Option<String> {
    let blob = base64::engine::general_purpose::STANDARD
        .decode(public_key)
        .ok()?;
    let digest = Sha256::digest(&blob);
    Some(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest)
    ))
}

/// Keys from `ssh-keyscan` output (`host type key` lines; `#` lines are
/// banners). Malformed lines are skipped.
fn parse_keyscan(output: &str) -> Vec<HostKey> {
    output
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let (_host, key_type, public_key) = (parts.next()?, parts.next()?, parts.next()?);
            Some(HostKey {
                key_type: key_type.to_string(),
                public_key: public_key.to_string(),
                fingerprint: fingerprint(public_key)?,
            })
        })
        .collect()
}

/// Ask a server for its host keys with `ssh-keyscan`. Nothing is trusted
/// here; the caller shows the fingerprints first.
pub async fn scan_host_keys(host: &str, port: u16) -> Result<Vec<HostKey>> {
    // A leading dash would read as an ssh-keyscan option.
    if host.is_empty() || host.starts_with('-') {
        bail!("`{host}` is not a valid host name");
    }
    let output = smol::process::Command::new("ssh-keyscan")
        .args(["-T", "10", "-p", &port.to_string(), host])
        .output()
        .await
        .context("running ssh-keyscan")?;
    let keys = parse_keyscan(&String::from_utf8_lossy(&output.stdout));
    if keys.is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{host}:{port} didn't offer an ssh host key{}",
            stderr
                .lines()
                .find(|line| !line.starts_with('#') && !line.trim().is_empty())
                .map(|line| format!(": {line}"))
                .unwrap_or_default()
        );
    }
    Ok(keys)
}

/// The text to append to known_hosts so `keys` are trusted for host:port:
/// lines that aren't there yet, after a newline if the file lacks one.
fn known_hosts_append(existing: &str, host: &str, port: u16, keys: &[HostKey]) -> String {
    let host = known_hosts_host(host, port);
    let mut out = String::new();
    for key in keys {
        let line = format!("{host} {} {}", key.key_type, key.public_key);
        if !existing.lines().any(|known| known.trim() == line) && !out.contains(&line) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    if !out.is_empty() && !existing.is_empty() && !existing.ends_with('\n') {
        out.insert(0, '\n');
    }
    out
}

/// Key types in `keys` that known_hosts already holds with a different key.
/// `found` is `ssh-keygen -F` output: `#` comment lines, then known_hosts
/// lines (`host type key`, with the host possibly hashed).
fn changed_key_types(found: &str, keys: &[HostKey]) -> Vec<String> {
    let mut changed = Vec::new();
    for line in found.lines().filter(|line| !line.trim_start().starts_with('#')) {
        let mut parts = line.split_whitespace();
        let (Some(_host), Some(key_type), Some(public_key)) =
            (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let differs = keys
            .iter()
            .any(|key| key.key_type == key_type && key.public_key != public_key);
        if differs && !changed.iter().any(|known| known == key_type) {
            changed.push(key_type.to_string());
        }
    }
    changed
}

/// Refuse to pin keys that contradict ones already on file for host:port,
/// hashed entries included (`ssh-keygen -F` matches those).
async fn ensure_keys_not_changed(
    known_hosts: &Path,
    host: &str,
    port: u16,
    keys: &[HostKey],
) -> Result<()> {
    let output = smol::process::Command::new("ssh-keygen")
        .arg("-F")
        .arg(known_hosts_host(host, port))
        .arg("-f")
        .arg(known_hosts)
        .output()
        .await
        .context("running ssh-keygen")?;
    let changed = changed_key_types(&String::from_utf8_lossy(&output.stdout), keys);
    if !changed.is_empty() {
        bail!(
            "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! {host}:{port} already has a different {} host key in {}",
            changed.join(", "),
            known_hosts.display()
        );
    }
    Ok(())
}

/// Trust `keys` for host:port: append them to `~/.ssh/known_hosts` (created
/// 0600 in a 0700 `~/.ssh` when missing) and record the decision in SQLite.
pub async fn trust_host_keys(
    pool: &SqlitePool,
    host: &str,
    port: u16,
    keys: &[HostKey],
) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

    let ssh_dir = dirs::home_dir()
        .context("no home directory for ~/.ssh")?
        .join(".ssh");
    if !ssh_dir.exists() {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&ssh_dir)
            .with_context(|| format!("creating `{}`", ssh_dir.display()))?;
    }
    let path = ssh_dir.join("known_hosts");
    let existing = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            return Err(error).with_context(|| format!("reading `{}`", path.display()));
        }
    };
    if !existing.is_empty() {
        ensure_keys_not_changed(&path, host, port, keys).await?;
    }
    let append = known_hosts_append(&existing, host, port, keys);
    if !append.is_empty() {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)
            .and_then(|mut file| file.write_all(append.as_bytes()))
            .with_context(|| format!("writing `{}`", path.display()))?;
    }

    for key in keys {
        sqlx::query(
            "INSERT INTO known_hosts (host, port, key_type, public_key, trusted, added_at) \
             VALUES (?, ?, ?, ?, 1, datetime('now')) \
             ON CONFLICT (host, port, key_type) DO UPDATE SET \
               public_key = excluded.public_key, trusted = 1, added_at = excluded.added_at",
        )
        .bind(host)
        .bind(i64::from(port))
        .bind(&key.key_type)
        .bind(&key.public_key)
        .execute(pool)
        .await
        .context("recording the host key decision")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ED25519: &str = "AAAAC3NzaC1lZDI1NTE5AAAAIPn4eEcl0baoZfNK8KKZz9a7I0sh4nUjlTejOWxJXi6T";

    #[test]
    fn endpoint_host_and_port() {
        assert_eq!(host_port("ssh://me@example.com:2222"), Some(("example.com".into(), 2222)));
        assert_eq!(host_port("example.com"), Some(("example.com".into(), 22)));
        assert_eq!(host_port("10.0.0.5:2200"), Some(("10.0.0.5".into(), 2200)));
        assert_eq!(host_port("[::1]:2222"), Some(("::1".into(), 2222)));
        assert_eq!(host_port(""), None);
    }

    #[test]
    fn keyscan_output_matches_ssh_keygen_fingerprint() {
        let output = format!(
            "# 127.0.0.1:22999 SSH-2.0-OpenSSH_10.3\n[127.0.0.1]:22999 ssh-ed25519 {ED25519}\nbroken\n"
        );
        let keys = parse_keyscan(&output);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key_type, "ssh-ed25519");
        // Same value `ssh-keygen -lf` printed for this key.
        assert_eq!(keys[0].fingerprint, "SHA256:7tsIRTofeNHISUdIW2Ow9dKuz1G4NPp1M12VLvR7dF4");
    }

    #[test]
    fn changed_key_of_same_type_is_detected() {
        let other = "AAAAC3NzaC1lZDI1NTE5AAAAIOtherKeyOtherKeyOtherKeyOtherKeyOtherKey0000";
        let new = |key_type: &str, public_key: &str| HostKey {
            key_type: key_type.into(),
            public_key: public_key.into(),
            fingerprint: String::new(),
        };
        let found = format!(
            "# Host [h.example]:2222 found: line 3\n|1|c2FsdA==|aGFzaA== ssh-ed25519 {other}\n\
             # Host [h.example]:2222 found: line 4\n[h.example]:2222 ssh-rsa AAAAB3Rsa\n"
        );
        // Same type, different blob: changed (hashed entry included).
        assert_eq!(
            changed_key_types(&found, &[new("ssh-ed25519", ED25519)]),
            vec!["ssh-ed25519"]
        );
        // Identical key, or a type that isn't on file: fine.
        assert!(changed_key_types(&found, &[new("ssh-ed25519", other)]).is_empty());
        assert!(changed_key_types(&found, &[new("ecdsa-sha2-nistp256", ED25519)]).is_empty());
        assert!(changed_key_types("", &[new("ssh-ed25519", ED25519)]).is_empty());
    }

    #[test]
    fn known_hosts_lines_skip_duplicates_and_fix_missing_newline() {
        let key = HostKey {
            key_type: "ssh-ed25519".into(),
            public_key: ED25519.into(),
            fingerprint: String::new(),
        };
        let line = format!("[h.example]:2222 ssh-ed25519 {ED25519}\n");
        assert_eq!(known_hosts_append("", "h.example", 2222, &[key.clone()]), line);
        assert_eq!(
            known_hosts_append("other ssh-rsa AAAA", "h.example", 2222, &[key.clone()]),
            format!("\n{line}")
        );
        assert_eq!(known_hosts_append(&line, "h.example", 2222, &[key.clone()]), "");
        assert_eq!(
            known_hosts_append("", "h.example", 22, &[key]),
            format!("h.example ssh-ed25519 {ED25519}\n")
        );
    }
}
