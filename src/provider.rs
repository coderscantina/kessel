//! The provider layer: a thin capability-aware wrapper over OpenDAL.
//!
//! Every backend is an [`opendal::Operator`]; kessel's job here is only to
//! (a) build the Operator from a site profile plus keychain secrets, and
//! (b) compose the per-site middleware stack (retry / timeout / throttle /
//! concurrency / logging) that makes transfers resilient.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use opendal::layers::{
    ConcurrentLimitLayer, LoggingLayer, RetryLayer, ThrottleLayer, TimeoutLayer, TracingLayer,
};
use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::secrets::SecretStore;

/// opendal 0.58 exposes object timestamps as a jiff-backed `Timestamp`; kessel
/// works in chrono throughout, so convert via the RFC3339 string both agree on.
pub fn to_datetime(ts: opendal::raw::Timestamp) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&ts.to_string())
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Per-site middleware configuration, stored as JSON in `sites.layers`.
/// All fields optional so a site row only overrides what it changes;
/// merged over the app-level default stack from config.json5.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LayerConfig {
    pub retry: Option<RetryConfig>,
    pub timeout_secs: Option<u64>,
    pub throttle: Option<ThrottleConfig>,
    pub concurrent_limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_times: usize,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThrottleConfig {
    /// bytes per second
    pub bandwidth: u32,
    /// burst size in bytes
    pub burst: u32,
}

impl LayerConfig {
    /// Site-specific values win; anything unset falls back to app defaults.
    pub fn merged_over(&self, defaults: &LayerConfig) -> LayerConfig {
        LayerConfig {
            retry: self.retry.clone().or_else(|| defaults.retry.clone()),
            timeout_secs: self.timeout_secs.or(defaults.timeout_secs),
            throttle: self.throttle.clone().or_else(|| defaults.throttle.clone()),
            concurrent_limit: self.concurrent_limit.or(defaults.concurrent_limit),
        }
    }
}

/// A connection profile as loaded from the `sites` table.
#[derive(Debug, Clone, Default)]
pub struct SiteProfile {
    pub id: i64,
    pub name: String,
    /// OpenDAL service scheme: "s3", "webdav", "gcs", "azblob", "fs", ...
    pub scheme: String,
    /// Non-secret Operator options (endpoint, region, bucket, root, ...).
    pub options: HashMap<String, String>,
    pub layers: LayerConfig,
    pub secret_handle: Option<String>,
    /// Sidebar presentation: named icon and hex color from the app palette.
    pub icon: Option<String>,
    pub color: Option<String>,
    pub tags: Vec<String>,
    /// Bookmark folder this site lives in (`bookmark_folders.id`).
    pub folder_id: Option<i64>,
    /// Space this bookmark belongs to (`spaces.id`).
    pub space_id: i64,
}

/// What a connected backend can actually do, derived from OpenDAL's own
/// capability report. The UI uses this to enable/disable actions instead of
/// guessing per protocol.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub rename: bool,
    pub copy: bool,
    pub presign: bool,
    pub write_can_append: bool,
    /// S3-style backends have no real directories; paths are prefixes.
    pub virtual_dirs: bool,
    /// A fresh write goes to a temporary name that closing the writer renames
    /// onto the target, whatever was written. Only appends write in place.
    pub write_renames_on_close: bool,
}

/// One entry as seen by the browser UI: a name plus its full browse path.
/// In bucket-root mode the browse path is `bucket/` + the operator path, so
/// the UI never has to know which operator served it.
#[derive(Debug, Clone)]
pub struct RemoteEntry {
    pub name: String,
    /// Full browse path; directories end with `/`.
    pub path: String,
    pub is_dir: bool,
    /// Entry is a symlink (fs-style backends only; resolved via stat, so
    /// `is_dir` reflects the target).
    pub is_symlink: bool,
    /// Entry is a bucket (the root listing of an account-wide S3 site).
    pub is_bucket: bool,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
    /// Unix permission bits (`mode & 0o7777`): filled inline for local fs,
    /// via [`Provider::sftp_permissions`] for sftp.
    pub permissions: Option<u32>,
    /// S3 storage class, filled via [`Provider::storage_classes`].
    pub storage_class: Option<String>,
}

/// A live connection to one site: the layered Operator plus its capabilities.
#[derive(Clone)]
pub struct Provider {
    pub site_id: i64,
    /// The site's display name, for messages about it.
    pub site_name: String,
    pub caps: Capabilities,
    /// The site's backend scheme, for provider-specific UI (columns, menus).
    /// opendal 0.58 dropped the `Scheme` enum in favour of string schemes.
    pub scheme: String,
    /// Resolved operator options (secrets and AWS profile merged in), kept
    /// for the side channels OpenDAL can't express (ListBuckets, storage
    /// class, ACL, sftp chmod).
    options: Arc<HashMap<String, String>>,
    /// Bounds the side channels OpenDAL's timeout layer doesn't cover.
    side_channel_timeout: Duration,
    backend: Backend,
}

#[derive(Clone)]
enum Backend {
    Single(Operator),
    /// S3 site saved without a bucket: the root lists every bucket in the
    /// account (ListBuckets, see `crate::s3`), and the first path segment
    /// routes to a lazily-built per-bucket Operator.
    S3Root {
        /// Resolved operator options (secrets and AWS profile merged, no bucket).
        options: HashMap<String, String>,
        layers: LayerConfig,
        bucket_ops: Arc<Mutex<HashMap<String, Operator>>>,
    },
}

impl Provider {
    /// Build a layered Operator for a site, pulling secrets from the keychain
    /// and merging its layer config over the app defaults.
    pub fn connect(profile: &SiteProfile, default_layers: &LayerConfig) -> Result<Self> {
        let scheme = profile.scheme.as_str();

        let mut options = profile.options.clone();
        if let Some(handle) = &profile.secret_handle {
            options.extend(SecretStore::load(handle).context("loading site secrets")?);
        }

        // S3 profile mode: resolve ~/.aws at connect time so rotated keys are
        // picked up; explicitly set options (e.g. region) win over the profile.
        if let Some(aws_profile) = options.remove(crate::bookmarks::AWS_PROFILE_OPT) {
            for (key, value) in crate::aws::profile_options(&aws_profile)
                .with_context(|| format!("resolving AWS profile `{aws_profile}`"))?
            {
                options.entry(key).or_insert(value);
            }
        }

        let layers = profile.layers.merged_over(default_layers);
        let side_channel_timeout = Duration::from_secs(layers.timeout_secs.unwrap_or(DEFAULT_SIDE_CHANNEL_TIMEOUT_SECS));

        // No bucket on an S3 site means "browse the whole account": defer
        // per-bucket operators until navigation. Capabilities are per-scheme,
        // so a throwaway operator with a placeholder bucket reports them.
        if scheme == "s3" && options.get("bucket").is_none_or(String::is_empty) {
            let mut probe = options.clone();
            probe.insert("bucket".to_string(), "kessel-probe".to_string());
            let probe_op = Operator::via_iter(scheme, probe)
                .with_context(|| format!("building operator for site `{}`", profile.name))?;
            return Ok(Self {
                site_id: profile.id,
                site_name: profile.name.clone(),
                caps: Self::capabilities(&probe_op),
                scheme: scheme.to_string(),
                options: Arc::new(options.clone()),
                side_channel_timeout,
                backend: Backend::S3Root {
                    options,
                    layers,
                    bucket_ops: Arc::default(),
                },
            });
        }

        let resolved = Arc::new(options.clone());
        let op = Self::apply_layers(
            Operator::via_iter(scheme, options)
                .with_context(|| format!("building operator for site `{}`", profile.name))?,
            &layers,
        );

        let caps = Self::capabilities(&op);
        Ok(Self {
            site_id: profile.id,
            site_name: profile.name.clone(),
            caps,
            scheme: scheme.to_string(),
            options: resolved,
            side_channel_timeout,
            backend: Backend::Single(op),
        })
    }

    // The layer stack is the core value proposition: retry, timeout,
    // throttle and observability are OpenDAL's, not ours.
    fn apply_layers(mut op: Operator, layers: &LayerConfig) -> Operator {
        // Timeout goes inside retry, so a stalled attempt is retried instead
        // of ending the whole operation. The one knob also bounds each IO
        // step (a folder listing, a read chunk): OpenDAL's 10s IO default is
        // too tight for large FTP listings.
        if let Some(secs) = layers.timeout_secs {
            let timeout = Duration::from_secs(secs);
            op = op.layer(
                TimeoutLayer::new()
                    .with_timeout(timeout)
                    .with_io_timeout(timeout),
            );
        }
        if let Some(retry) = &layers.retry {
            op = op.layer(
                RetryLayer::new()
                    .with_max_times(retry.max_times)
                    .with_min_delay(Duration::from_millis(retry.min_delay_ms))
                    .with_max_delay(Duration::from_millis(retry.max_delay_ms))
                    .with_jitter(),
            );
        }
        if let Some(throttle) = &layers.throttle {
            op = op.layer(ThrottleLayer::new(throttle.bandwidth, throttle.burst));
        }
        if let Some(limit) = layers.concurrent_limit {
            op = op.layer(ConcurrentLimitLayer::new(limit));
        }
        op.layer(LoggingLayer::new(QuietNotFound)).layer(TracingLayer::new())
    }

    /// Resolve a browse path to the Operator serving it plus the
    /// operator-relative path. For single-operator sites that's the identity;
    /// in bucket-root mode the first segment picks the bucket.
    pub fn op_for(&self, path: &str) -> Result<(Operator, String)> {
        match &self.backend {
            Backend::Single(op) => Ok((op.clone(), path.to_string())),
            Backend::S3Root { .. } => {
                anyhow::ensure!(
                    !path.is_empty(),
                    "the bucket list itself cannot be read or written — pick a bucket"
                );
                let (bucket, rest) = path.split_once('/').unwrap_or((path, ""));
                Ok((self.bucket_op(bucket)?, rest.to_string()))
            }
        }
    }

    /// Delete one entry: recursive delete for directories (works on
    /// virtual-dir backends too), plain delete for files.
    pub async fn delete(&self, path: &str, is_dir: bool) -> Result<()> {
        let (op, rel) = self.op_for(path)?;
        if is_dir {
            op.delete_with(&rel).recursive(true).await?;
        } else {
            op.delete(&rel).await?;
        }
        Ok(())
    }

    /// Move one local entry to the OS trash (fs sites only). Remote deletes
    /// have no trash and go through [`Provider::delete`].
    pub async fn trash(&self, path: &str) -> Result<()> {
        anyhow::ensure!(self.scheme == "fs", "only local files can go to the trash");
        let full = self.fs_path(path.trim_end_matches('/'));
        tokio::task::spawn_blocking(move || {
            trash::delete(&full).with_context(|| format!("moving `{}` to the trash", full.display()))
        })
        .await
        .context("trash task panicked")?
    }

    /// Rename within one operator: server-side where the backend supports
    /// it, otherwise a copy + delete fallback (files only). Refuses to
    /// replace an existing destination.
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let (op, from_rel) = self.op_for(from)?;
        let (_, to_rel) = self.op_for(to)?;
        match op.stat(&to_rel).await {
            Ok(_) => {
                // On a case-insensitive filesystem a case-only rename stats
                // the source itself. Only then may it go ahead: the parent
                // listing holds the source's spelling but not the new one.
                let case_only = from_rel.to_lowercase() == to_rel.to_lowercase();
                if !(case_only && !self.has_exact_sibling(&op, &to_rel).await?) {
                    let name = to_rel.trim_end_matches('/').rsplit('/').next().unwrap_or(&to_rel);
                    return Err(opendal::Error::new(
                        opendal::ErrorKind::AlreadyExists,
                        format!("`{name}` already exists"),
                    )
                    .into());
                }
            }
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if self.caps.rename {
            self.rename_on_server(&op, &from_rel, &to_rel).await?;
        } else if self.caps.copy {
            anyhow::ensure!(
                !from_rel.ends_with('/'),
                "this backend cannot rename directories"
            );
            op.copy(&from_rel, &to_rel).await?;
            op.delete(&from_rel).await?;
        } else {
            anyhow::bail!("this backend supports neither rename nor copy");
        }
        Ok(())
    }

    /// One server-side rename of operator paths (guarded by `caps.rename`).
    /// OpenDAL has no FTP rename, so that goes through [`crate::ftp`].
    pub(crate) async fn rename_on_server(&self, op: &Operator, from: &str, to: &str) -> opendal::Result<()> {
        if self.scheme == "ftp" {
            crate::ftp::rename(&self.options, from, to, self.side_channel_timeout).await
        } else {
            op.rename(from, to).await
        }
    }

    /// Create an empty FTP file, which OpenDAL's writer can't do: it only
    /// reaches the server once it has bytes to send.
    pub(crate) async fn create_empty_ftp_file(&self, path: &str) -> Result<()> {
        let (op, rel) = self.op_for(path)?;
        if let Some((parent, _)) = rel.rsplit_once('/') {
            op.create_dir(&format!("{parent}/")).await?;
        }
        crate::ftp::create_empty(&self.options, &rel, self.side_channel_timeout).await?;
        Ok(())
    }

    /// Whether the parent listing of `rel` has an entry spelled exactly like
    /// it, telling a real second file from a case-insensitive alias.
    async fn has_exact_sibling(&self, op: &Operator, rel: &str) -> Result<bool> {
        let trimmed = rel.trim_end_matches('/');
        let (parent, name) = match trimmed.rsplit_once('/') {
            Some((parent, name)) => (format!("{parent}/"), name),
            None => ("/".to_string(), trimmed),
        };
        let entries = op.list(&parent).await?;
        Ok(contains_exact_name(
            entries.iter().map(|entry| entry.name()),
            name,
        ))
    }

    /// Download one remote file into a local path, streamed in chunks. The
    /// bytes land in a sibling `.part` file that replaces `dest` only once
    /// complete, so a failed download never leaves a truncated `dest`.
    /// Meant for previews and external edits; bulk transfers use the queue.
    pub async fn read_to_file(&self, path: &str, dest: &std::path::Path) -> Result<()> {
        use tokio::io::AsyncWriteExt as _;

        let (op, rel) = self.op_for(path)?;
        let mut part = dest.as_os_str().to_owned();
        part.push(".part");
        let part = std::path::PathBuf::from(part);

        let result: Result<()> = async {
            let mut stream = op.reader(&rel).await?.into_bytes_stream(..).await?;
            let mut file = tokio::fs::File::create(&part)
                .await
                .with_context(|| format!("creating `{}`", part.display()))?;
            while let Some(chunk) = stream.next().await {
                file.write_all(&chunk?)
                    .await
                    .with_context(|| format!("writing `{}`", part.display()))?;
            }
            file.flush().await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(error);
        }
        tokio::fs::rename(&part, dest)
            .await
            .with_context(|| format!("writing `{}`", dest.display()))?;
        Ok(())
    }

    /// Recursive (file count, total bytes) under a directory. Backends whose
    /// listings lack sizes (e.g. local fs) get a stat per file.
    pub async fn dir_size(&self, path: &str) -> Result<(u64, u64)> {
        let (op, rel) = self.op_for(path)?;
        let entries = op.list_with(&rel).recursive(true).await?;
        let mut files = 0u64;
        let mut bytes = 0u64;
        let mut stats = tokio::task::JoinSet::new();
        for entry in entries {
            let meta = entry.metadata();
            if meta.is_dir() {
                continue;
            }
            files += 1;
            // opendal 0.58 dropped the list-metadata capability flags; a
            // listed entry that carries no mtime came from a backend whose
            // listings lack sizes too (local fs), so stat it for the size.
            if meta.last_modified().is_none() {
                let op = op.clone();
                let rel = entry.path().to_string();
                stats.spawn(async move { op.stat(&rel).await.ok() });
            } else {
                bytes += meta.content_length();
            }
        }
        while let Some(joined) = stats.join_next().await {
            if let Ok(Some(meta)) = joined {
                bytes += meta.content_length();
            }
        }
        Ok((files, bytes))
    }

    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let (op, rel) = self.op_for(path)?;
        op.create_dir(&rel).await?;
        Ok(())
    }

    /// Server-side copy within one operator (guarded by `caps.copy`).
    pub async fn copy(&self, from: &str, to: &str) -> Result<()> {
        // Bucket-root sites route each bucket to its own operator; a copy
        // only stays server-side within one bucket.
        if let Backend::S3Root { .. } = &self.backend {
            let bucket = |path: &str| path.split('/').next().unwrap_or("").to_string();
            anyhow::ensure!(
                bucket(from) == bucket(to),
                "server-side copy cannot cross buckets"
            );
        }
        let (op, from_rel) = self.op_for(from)?;
        let (_, to_rel) = self.op_for(to)?;
        op.copy(&from_rel, &to_rel).await?;
        Ok(())
    }

    /// Copy a modified time onto an entry — OpenDAL's write path cannot set
    /// mtime, so this is a per-backend escape hatch (local fs and sftp; other
    /// backends report an error the caller may treat as best-effort).
    pub async fn set_mtime(&self, path: &str, mtime: DateTime<Utc>) -> Result<()> {
        match self.scheme.as_str() {
            "fs" => {
                let (op, rel) = self.op_for(path)?;
                let root = op.info().root().to_string();
                let full = crate::localfs::full_path(&root, rel.trim_start_matches('/'));
                let time = filetime::FileTime::from_unix_time(
                    mtime.timestamp(),
                    mtime.timestamp_subsec_nanos(),
                );
                tokio::task::spawn_blocking(move || filetime::set_file_mtime(&full, time))
                    .await
                    .context("set_mtime task panicked")??;
                Ok(())
            }
            "sftp" => crate::sftp::set_mtime(&self.options, path, mtime).await,
            _ => anyhow::bail!("this backend cannot set modified times"),
        }
    }

    /// Presigned GET URL (guarded by `caps.presign`).
    pub async fn presign_read(&self, path: &str, expire: Duration) -> Result<String> {
        let (op, rel) = self.op_for(path)?;
        let presigned = op.presign_read(&rel, expire).await?;
        Ok(presigned.uri().to_string())
    }

    // --- provider-specific side channels ---

    /// An s3 browse path as (bucket, object key), accounting for the site's
    /// root prefix and bucket-root mode.
    fn s3_location(&self, path: &str) -> Result<(String, String)> {
        anyhow::ensure!(self.scheme == "s3", "not an S3 site");
        match &self.backend {
            Backend::Single(_) => {
                let bucket = self
                    .options
                    .get("bucket")
                    .cloned()
                    .context("site has no bucket")?;
                let root = self
                    .options
                    .get("root")
                    .map(|root| root.trim_matches('/'))
                    .unwrap_or("");
                let key = if root.is_empty() {
                    path.to_string()
                } else {
                    format!("{root}/{path}")
                };
                Ok((bucket, key))
            }
            Backend::S3Root { .. } => {
                let (bucket, rest) = path
                    .split_once('/')
                    .context("pick a bucket first")?;
                Ok((bucket.to_string(), rest.to_string()))
            }
        }
    }

    /// Storage class per entry name for one S3 directory level.
    pub async fn storage_classes(&self, dir: &str) -> Result<HashMap<String, String>> {
        let (bucket, prefix) = self.s3_location(dir)?;
        let classes = crate::s3::storage_classes(&self.options, &bucket, &prefix).await?;
        Ok(classes
            .into_iter()
            .filter_map(|(key, class)| {
                let name = key.rsplit('/').next().unwrap_or(&key);
                (!name.is_empty()).then(|| (name.to_string(), class))
            })
            .collect())
    }

    /// Change one object's S3 storage class (copy-in-place).
    pub async fn set_storage_class(&self, path: &str, class: &str) -> Result<()> {
        let (bucket, key) = self.s3_location(path)?;
        crate::s3::set_storage_class(&self.options, &bucket, &key, class).await
    }

    /// Apply a canned ACL to one S3 object.
    pub async fn set_acl(&self, path: &str, acl: &str) -> Result<()> {
        let (bucket, key) = self.s3_location(path)?;
        crate::s3::set_acl(&self.options, &bucket, &key, acl).await
    }

    /// Regions for the given buckets (bucket-root S3 sites), fetched
    /// concurrently; buckets that fail are simply absent.
    pub async fn bucket_regions(&self, buckets: Vec<String>) -> HashMap<String, String> {
        let mut lookups = tokio::task::JoinSet::new();
        for bucket in buckets {
            let options = self.options.clone();
            lookups.spawn(async move {
                let region = crate::s3::bucket_region(&options, &bucket).await;
                (bucket, region)
            });
        }
        let mut regions = HashMap::new();
        while let Some(joined) = lookups.join_next().await {
            if let Ok((bucket, Ok(Some(region)))) = joined {
                regions.insert(bucket, region);
            }
        }
        regions
    }

    /// Permission bits per entry name for one sftp directory.
    pub async fn sftp_permissions(&self, dir: &str) -> Result<HashMap<String, u32>> {
        anyhow::ensure!(self.scheme == "sftp", "not an sftp site");
        crate::sftp::list_permissions(&self.options, dir).await
    }

    /// One entry's permission bits (chmod dialog pre-fill); fs and sftp.
    pub async fn stat_mode(&self, path: &str) -> Result<Option<u32>> {
        match self.scheme.as_str() {
            "sftp" => crate::sftp::stat_mode(&self.options, path).await,
            "fs" => {
                let full = self.fs_path(path);
                let meta = std::fs::symlink_metadata(&full)
                    .with_context(|| format!("stat `{}`", full.display()))?;
                Ok(unix_mode(&meta))
            }
            _ => anyhow::bail!("permissions are not supported on this backend"),
        }
    }

    /// chmod one entry; fs and sftp.
    pub async fn chmod(&self, path: &str, mode: u32) -> Result<()> {
        match self.scheme.as_str() {
            "sftp" => crate::sftp::chmod(&self.options, path, mode).await,
            #[cfg(unix)]
            "fs" => {
                let full = self.fs_path(path);
                std::fs::set_permissions(
                    &full,
                    std::os::unix::fs::PermissionsExt::from_mode(mode),
                )
                .with_context(|| format!("chmod `{}`", full.display()))
            }
            _ => anyhow::bail!("permissions are not supported on this backend"),
        }
    }

    /// Absolute local path for an fs-site browse path.
    fn fs_path(&self, path: &str) -> std::path::PathBuf {
        let root = self
            .options
            .get("root")
            .cloned()
            .unwrap_or_else(crate::localfs::root);
        crate::localfs::full_path(&root, path)
    }

    /// Connectivity probe for the bookmark form's "Test" button.
    pub async fn check(&self) -> Result<()> {
        match &self.backend {
            Backend::Single(op) => op.check().await.map_err(Into::into),
            Backend::S3Root { options, .. } => {
                crate::s3::list_buckets(options).await.map(|_| ())
            }
        }
    }

    /// List a browse path (OpenDAL-style: `""` is the root, dirs end `/`).
    /// In bucket-root mode the root is the account's buckets and the first
    /// path segment picks the bucket.
    pub async fn list(&self, path: &str) -> Result<Vec<RemoteEntry>> {
        match &self.backend {
            Backend::Single(op) => list_operator(op, path, "").await,
            Backend::S3Root { options, .. } => {
                if path.is_empty() {
                    let buckets = crate::s3::list_buckets(options).await?;
                    Ok(buckets
                        .into_iter()
                        .map(|bucket| RemoteEntry {
                            path: format!("{}/", bucket.name),
                            name: bucket.name,
                            is_dir: true,
                            is_symlink: false,
                            is_bucket: true,
                            size: 0,
                            modified: bucket.created,
                            permissions: None,
                            storage_class: None,
                        })
                        .collect())
                } else {
                    let (bucket, rest) = path.split_once('/').unwrap_or((path, ""));
                    let op = self.bucket_op(bucket)?;
                    list_operator(&op, rest, &format!("{bucket}/")).await
                }
            }
        }
    }

    fn bucket_op(&self, bucket: &str) -> Result<Operator> {
        let Backend::S3Root {
            options,
            layers,
            bucket_ops,
        } = &self.backend
        else {
            unreachable!("bucket_op is only called in bucket-root mode");
        };
        let mut cache = bucket_ops.lock().unwrap();
        if let Some(op) = cache.get(bucket) {
            return Ok(op.clone());
        }
        let mut options = options.clone();
        options.insert("bucket".to_string(), bucket.to_string());
        let op = Self::apply_layers(
            Operator::via_iter("s3", options)
                .with_context(|| format!("building operator for bucket `{bucket}`"))?,
            layers,
        );
        cache.insert(bucket.to_string(), op.clone());
        Ok(op)
    }

    fn capabilities(op: &Operator) -> Capabilities {
        let info = op.info();
        let cap = info.capability();
        let ftp = info.scheme() == "ftp";
        Capabilities {
            // OpenDAL can't rename on FTP; `rename_on_server` can.
            rename: cap.rename || ftp,
            copy: cap.copy,
            presign: cap.presign,
            write_can_append: cap.write_can_append,
            // Object stores report create_dir but directories are prefixes.
            virtual_dirs: matches!(
                info.scheme(),
                "s3" | "gcs" | "azblob"
            ),
            write_renames_on_close: ftp,
        }
    }
}

/// Side-channel limit for sites without a configured timeout.
const DEFAULT_SIDE_CHANNEL_TIMEOUT_SECS: u64 = 60;

/// Stats in flight at once while filling metadata for a listing.
const STAT_CONCURRENCY: usize = 16;

/// List one operator path, mapping entries to browse paths under `prefix`
/// (`""` for single-operator sites, `"bucket/"` in bucket-root mode).
///
/// Backends like `fs` return bare entry modes from list — no size, no mtime,
/// and symlinks surface as `EntryMode::Unknown`. opendal 0.58 dropped the
/// list-metadata capability flags, so completeness is judged per entry: one
/// that carries no mtime came from a listing without sizes/mtimes and is
/// stat'ed to fill them (stat also follows symlinks to their target).
async fn list_operator(op: &Operator, path: &str, prefix: &str) -> Result<Vec<RemoteEntry>> {
    let raw = op.list(path).await?;
    let info = op.info();
    // Local fs: OpenDAL has no notion of unix modes, so permissions come
    // from a plain stat beside the listing.
    let fs_root = (info.scheme() == "fs").then(|| info.root().to_string());

    let mut entries = Vec::new();
    let mut needs_stat = Vec::new();
    for entry in raw {
        if entry.path() == path {
            continue;
        }
        let meta = entry.metadata();
        let is_symlink = meta.mode() == opendal::EntryMode::Unknown;
        if meta.last_modified().is_none() || is_symlink {
            needs_stat.push((entries.len(), entry.path().to_string()));
        }
        let permissions = fs_root.as_deref().and_then(|root| {
            let full = crate::localfs::full_path(root, entry.path());
            std::fs::symlink_metadata(full).ok().and_then(|meta| unix_mode(&meta))
        });
        entries.push(RemoteEntry {
            name: entry.name().trim_end_matches('/').to_string(),
            path: format!("{prefix}{}", entry.path()),
            is_dir: meta.is_dir(),
            is_symlink,
            is_bucket: false,
            size: meta.content_length(),
            modified: meta.last_modified().and_then(to_datetime),
            permissions,
            storage_class: None,
        });
    }
    // Bounded so a folder with thousands of entries doesn't fire thousands
    // of stats at once.
    let mut stats = futures::stream::iter(needs_stat)
        .map(|(ix, rel)| {
            let op = op.clone();
            async move { (ix, op.stat(&rel).await.ok()) }
        })
        .buffer_unordered(STAT_CONCURRENCY);
    while let Some((ix, meta)) = stats.next().await {
        // A failed stat (e.g. dangling symlink) keeps the bare list entry.
        let Some(meta) = meta else { continue };
        let entry = &mut entries[ix];
        entry.size = meta.content_length();
        entry.modified = meta.last_modified().and_then(to_datetime);
        if entry.is_symlink && meta.is_dir() {
            entry.is_dir = true;
            if !entry.path.ends_with('/') {
                entry.path.push('/');
            }
        }
    }
    Ok(entries)
}

/// `mode & 0o7777` from local fs metadata; `None` where the local filesystem
/// has no unix mode to report (Windows), which blanks the permissions column.
#[cfg(unix)]
fn unix_mode(meta: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt as _;
    Some(meta.mode() & 0o7777)
}

#[cfg(not(unix))]
fn unix_mode(_meta: &std::fs::Metadata) -> Option<u32> {
    None
}

/// Whether `names` (directory entries may end with `/`) has exactly `name`.
fn contains_exact_name<'a>(names: impl IntoIterator<Item = &'a str>, name: &str) -> bool {
    names
        .into_iter()
        .any(|candidate| candidate.trim_end_matches('/') == name)
}

#[cfg(test)]
mod rename_tests {
    use super::contains_exact_name;

    #[test]
    fn exact_name_match_tells_a_second_file_from_a_case_alias() {
        // Case-sensitive backend with both files: the listing has the new spelling.
        assert!(contains_exact_name(["Report.txt", "report.txt"], "report.txt"));
        // Case-insensitive fs: the listing only holds the source's spelling.
        assert!(!contains_exact_name(["Report.txt"], "report.txt"));
        // Directory entries carry a trailing slash.
        assert!(contains_exact_name(["Photos/"], "Photos"));
    }

    #[tokio::test]
    async fn an_ftp_site_renames_through_its_capability_flag() {
        let server = crate::ftp::test_server::FakeFtp::start();
        server.put("report.txt", b"content");
        let profile = super::SiteProfile {
            scheme: "ftp".into(),
            options: [("endpoint".to_string(), format!("ftp://127.0.0.1:{}", server.port))].into(),
            ..Default::default()
        };
        let provider = super::Provider::connect(&profile, &Default::default()).unwrap();

        assert!(provider.caps.rename);
        provider.rename("report.txt", "final.txt").await.unwrap();
        assert_eq!(server.names(), ["final.txt"]);
    }
}

/// OpenDAL's default logging, except a `stat` that finds nothing: kessel
/// stats destinations before every write, so "not found" is the normal
/// answer there and must not show up as a warning in the logs panel.
#[derive(Clone, Copy, Debug)]
struct QuietNotFound;

impl opendal::layers::LoggingInterceptor for QuietNotFound {
    fn log(
        &self,
        info: &opendal::raw::ServiceInfo,
        operation: opendal::raw::Operation,
        context: &[(&str, &str)],
        message: &str,
        err: Option<&opendal::Error>,
    ) {
        let expected = operation == opendal::raw::Operation::Stat
            && err.is_some_and(|err| err.kind() == opendal::ErrorKind::NotFound);
        let err = if expected { None } else { err };
        opendal::layers::DefaultLoggingInterceptor.log(info, operation, context, message, err);
    }
}
