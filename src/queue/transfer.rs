//! One task = one file, streamed source → destination in ranged chunks.
//!
//! Safe overwrite: on backends that can rename (local fs, sftp, ftp, …) a
//! task writes into its own partial file next to the destination
//! (`.<name>.<task id>.kessel-part`) and renames it onto the real name only
//! after the copy finished and verified. The old file stays intact until
//! then. The task records the partial it owns, so cancel and removal can
//! delete it later. Object stores write the destination directly: their
//! uploads only become visible on a successful close.
//!
//! Resume model: the byte offset is checkpointed to SQLite every 500ms.
//! When the destination can append, a partial resumes where it stopped: the
//! partial's own length is the truth, provided the source provably didn't
//! change (same size, and matching ETag and/or mtime). Without that proof,
//! or without append support, the file restarts from zero; partial
//! multipart uploads are aborted rather than committed.
//!
//! Transient failures (dropped connections, 5xx, timeouts) are retried
//! inside each read/write by the site's OpenDAL RetryLayer; an error that
//! surfaces here means the layer stack gave up, and the engine decides by
//! error class whether to re-queue the task with backoff.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result};
use opendal::layers::ThrottleLayer;
use opendal::{Operator, Writer};
use sqlx::SqlitePool;
use tokio::time::Instant;

use crate::provider::{Capabilities, Provider};

use super::store::{self, ClaimedTask};
use super::ConflictPolicy;

/// Ranged-read / write unit. Also the multipart chunk size on upload.
pub(crate) const CHUNK: u64 = 8 * 1024 * 1024;
/// How often the resume offset is checkpointed to SQLite.
const PERSIST_EVERY: Duration = Duration::from_millis(500);
/// Suffix of task-owned partial files; expansion never picks them up.
pub(crate) const PARTIAL_SUFFIX: &str = ".kessel-part";

/// Shared per-job state between the engine and its workers: pause/cancel
/// flags (polled between chunks, so a pause lands within one chunk of I/O)
/// and live byte counters for progress that is finer than the checkpoints.
#[derive(Debug, Default)]
pub(crate) struct JobControl {
    pub paused: AtomicBool,
    pub cancelled: AtomicBool,
    /// Bytes moved by this job's workers this session; only grows. The
    /// engine samples it for throughput.
    pub moved: AtomicU64,
    /// Bytes running workers wrote beyond their last persisted checkpoint.
    /// Added to the persisted counters for live progress.
    pub unpersisted: AtomicU64,
}

#[derive(Clone, Copy, PartialEq)]
enum Signal {
    Run,
    Pause,
    Cancel,
}

impl JobControl {
    /// A pause or cancel was requested.
    pub fn halted(&self) -> bool {
        self.signal() != Signal::Run
    }

    fn signal(&self) -> Signal {
        if self.cancelled.load(Ordering::Relaxed) {
            Signal::Cancel
        } else if self.paused.load(Ordering::Relaxed) {
            Signal::Pause
        } else {
            Signal::Run
        }
    }
}

/// One worker's share of [`JobControl::unpersisted`]: bytes it wrote minus
/// what its checkpoints already persisted.
struct Live<'a> {
    control: &'a JobControl,
    offset: u64,
    persisted: u64,
}

impl Live<'_> {
    fn advance(&mut self, bytes: u64) {
        self.offset += bytes;
        self.control.moved.fetch_add(bytes, Ordering::Relaxed);
        self.control.unpersisted.fetch_add(bytes, Ordering::Relaxed);
    }

    fn checkpointed(&mut self, at: u64) {
        let delta = at.saturating_sub(self.persisted);
        self.persisted += delta;
        self.control.unpersisted.fetch_sub(delta, Ordering::Relaxed);
    }

    /// What the engine still has to take off `unpersisted` once it has
    /// written the task's final state.
    fn remainder(&self) -> u64 {
        self.offset.saturating_sub(self.persisted)
    }
}

/// Per-job behavior flags, derived from the job row and the app config.
#[derive(Clone)]
pub(crate) struct TransferOpts {
    /// Source and destination are the same site — server-side copy applies.
    pub same_site: bool,
    /// Re-stat the destination after the transfer and compare sizes (and
    /// checksums where both sides use the same algorithm).
    pub verify: bool,
    /// Copy the source mtime onto the destination where the backend can.
    pub preserve_timestamps: bool,
    /// The global bandwidth cap. Clones share one rate limiter, so every
    /// running transfer draws from the same budget.
    pub throttle: Option<ThrottleLayer>,
}

#[derive(Debug)]
pub(crate) enum Outcome {
    Done { bytes: u64 },
    /// Destination already existed and the policy said to leave it alone.
    Skipped,
    /// Destination already existed under policy `Ask`: parked for the user,
    /// with what the destination held at detection time.
    Conflict {
        policy: ConflictPolicy,
        dst_size: u64,
        dst_modified: Option<String>,
    },
    Paused,
    Cancelled,
    Failed(anyhow::Error),
}

/// Whether a destination gets a task-owned partial file instead of being
/// written in place.
pub(crate) fn stages(caps: &Capabilities) -> bool {
    caps.rename && !caps.virtual_dirs
}

/// The partial file next to `path` (an operator or browse path) that task
/// `task_id` writes before committing.
pub(crate) fn partial_path(path: &str, task_id: i64) -> String {
    let name = file_name(path);
    let parent = &path[..path.len() - name.len()];
    format!("{parent}.{name}.{task_id}{PARTIAL_SUFFIX}")
}

/// Run one claimed task. Also returns the bytes the worker still counts in
/// [`JobControl::unpersisted`]; the engine subtracts them after persisting
/// the outcome, so live progress never dips.
pub(crate) async fn run_task(
    pool: &SqlitePool,
    src: &Provider,
    dst: &Provider,
    task: &ClaimedTask,
    control: &JobControl,
    opts: TransferOpts,
) -> (Outcome, u64) {
    let mut live = Live {
        control,
        offset: 0,
        persisted: 0,
    };
    let outcome = match run_inner(pool, src, dst, task, control, &opts, &mut live).await {
        Ok(outcome) => outcome,
        Err(error) => Outcome::Failed(error),
    };
    (outcome, live.remainder())
}

/// Where a task's bytes go: the final destination, or a partial file that is
/// renamed onto it after a successful copy.
struct Dest {
    op: Operator,
    path: String,
    browse: String,
    /// `(operator path, browse path)` of the partial, when staging.
    partial: Option<(String, String)>,
}

impl Dest {
    fn write_path(&self) -> &str {
        self.partial.as_ref().map_or(&self.path, |(path, _)| path)
    }

    fn write_browse(&self) -> &str {
        self.partial.as_ref().map_or(&self.browse, |(_, browse)| browse)
    }
}

async fn run_inner(
    pool: &SqlitePool,
    src: &Provider,
    dst: &Provider,
    task: &ClaimedTask,
    control: &JobControl,
    opts: &TransferOpts,
    live: &mut Live<'_>,
) -> Result<Outcome> {
    let (src_op, src_path) = src.op_for(&task.src)?;
    let (dst_op, mut dst_path) = dst.op_for(&task.dst)?;
    // Browse-level destination path for Provider side channels (server-side
    // copy, mtime); kept in sync with `dst_path` on rename.
    let mut dst_browse = task.dst.clone();

    let meta = src_op
        .stat(&src_path)
        .await
        .with_context(|| format!("stat source `{}`", task.src))?;
    let size = meta.content_length();
    let etag = meta.etag().map(str::to_string);
    let modified = meta
        .last_modified()
        .and_then(crate::provider::to_datetime)
        .map(|t| t.to_rfc3339());
    let can_append = dst.caps.write_can_append;

    // Conflict check: only for a fresh transfer (byte_offset > 0 means the
    // partial destination is our own earlier work, handled by resume below).
    let mut resume_from_dst = None;
    if task.byte_offset == 0 {
        store::update_task_meta(pool, task.id, size, etag.as_deref(), 0, modified.as_deref())
            .await?;
        let destination = match dst_op.stat(&dst_path).await {
            Ok(meta) => Some(meta),
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("checking destination before transfer"),
        };
        if let Some(dst_meta) = destination {
            anyhow::ensure!(!dst_meta.is_dir(), "destination `{}` is a folder", task.dst);
            let dst_len = dst_meta.content_length();
            match task.conflict_policy {
                ConflictPolicy::Overwrite => {}
                ConflictPolicy::OverwriteNewer => {
                    match (meta.last_modified(), dst_meta.last_modified()) {
                        (Some(source), Some(destination)) if source > destination => {}
                        (Some(_), Some(_)) => return Ok(Outcome::Skipped),
                        _ => {
                            return Ok(Outcome::Conflict {
                                policy: task.conflict_policy,
                                dst_size: dst_len,
                                dst_modified: dst_meta
                                    .last_modified()
                                    .and_then(crate::provider::to_datetime)
                                    .map(|t| t.to_rfc3339()),
                            })
                        }
                    }
                }
                ConflictPolicy::Ask => {
                    return Ok(Outcome::Conflict {
                        policy: task.conflict_policy,
                        dst_size: dst_len,
                        dst_modified: dst_meta
                            .last_modified()
                            .and_then(crate::provider::to_datetime)
                            .map(|t| t.to_rfc3339()),
                    });
                }
                ConflictPolicy::Skip => return Ok(Outcome::Skipped),
                ConflictPolicy::Resume => {
                    if dst_len >= size {
                        return Ok(Outcome::Skipped);
                    }
                    if can_append {
                        resume_from_dst = Some(dst_len);
                    }
                    // No append support: fall through and overwrite.
                }
                ConflictPolicy::Rename => {
                    let new_name = unique_name(pool, task.id, &dst_op, &dst_path, &dst_browse).await?;
                    let old_name_len = file_name(&dst_path).len();
                    dst_path.truncate(dst_path.len() - old_name_len);
                    dst_path.push_str(&new_name);
                    dst_browse.truncate(dst_browse.len() - old_name_len);
                    dst_browse.push_str(&new_name);
                }
            }
        }
        if resume_from_dst.is_some() != task.in_place {
            store::set_task_in_place(pool, task.id, resume_from_dst.is_some()).await?;
        }
    }

    // Appending onto an existing destination (policy `Resume`) is the one
    // case that deliberately writes the real file in place, also when a
    // later run of the task picks up its resume offset.
    let in_place = resume_from_dst.is_some() || (task.byte_offset > 0 && task.in_place);
    let partial = (stages(&dst.caps) && !in_place).then(|| {
        (
            partial_path(&dst_path, task.id),
            partial_path(&dst_browse, task.id),
        )
    });
    let dest = Dest {
        op: dst_op,
        path: dst_path,
        browse: dst_browse,
        partial,
    };
    if let Some((_, browse)) = &dest.partial {
        if task.partial.as_deref() != Some(browse.as_str()) {
            if let Some(stale) = &task.partial {
                discard_stale_partial(dst, stale).await;
            }
            store::set_task_partial(pool, task.id, Some(browse)).await?;
        }
    }

    let offset = match resume_from_dst {
        Some(offset) => offset,
        None => resume_offset(&dest, task, size, etag.as_deref(), modified.as_deref(), can_append).await,
    };
    store::update_task_meta(
        pool,
        task.id,
        size,
        etag.as_deref(),
        offset,
        modified.as_deref(),
    )
    .await?;
    live.offset = offset;
    live.persisted = offset;

    if offset == size && size > 0 {
        finish_transfer(pool, src, &src_op, &src_path, dst, &dest, &meta, task, opts).await?;
        return Ok(Outcome::Done { bytes: size });
    }

    // Same-site fast path: let the backend copy server-side (S3 CopyObject
    // and friends) instead of streaming every byte through the client.
    if opts.same_site && dst.caps.copy && offset == 0 {
        match dst.copy(&task.src, dest.write_browse()).await {
            Ok(()) => {
                finish_transfer(pool, src, &src_op, &src_path, dst, &dest, &meta, task, opts).await?;
                return Ok(Outcome::Done { bytes: size });
            }
            Err(error) => {
                tracing::warn!(
                    src = %task.src, dst = %dest.browse, %error,
                    "server-side copy failed; falling back to streaming"
                );
            }
        }
    }

    // Only the streaming writer draws from the global bandwidth budget.
    let write_op = match &opts.throttle {
        Some(throttle) => dest.op.clone().layer(throttle.clone()),
        None => dest.op.clone(),
    };
    let mut writer = if offset > 0 {
        write_op
            .writer_with(dest.write_path())
            .append(true)
            .await
            .with_context(|| format!("open destination `{}` for append", task.dst))?
    } else {
        write_op
            .writer_with(dest.write_path())
            .chunk(CHUNK as usize)
            .await
            .with_context(|| format!("open destination `{}`", task.dst))?
    };

    let reader = match src_op.reader_with(&src_path).await {
        Ok(reader) => reader,
        Err(error) => {
            let _ = writer.abort().await;
            return Err(error).with_context(|| format!("open source `{}`", task.src));
        }
    };

    let mut offset = offset;
    let mut last_persist = Instant::now();
    let copy_result: Result<Option<Outcome>> = async {
        while offset < size {
            match control.signal() {
                Signal::Run => {}
                Signal::Pause => return Ok(Some(Outcome::Paused)),
                Signal::Cancel => return Ok(Some(Outcome::Cancelled)),
            }
            let end = (offset + CHUNK).min(size);
            let buf = reader
                .read(offset..end)
                .await
                .with_context(|| format!("read `{}` at {offset}", task.src))?;
            let read = buf.len() as u64;
            anyhow::ensure!(read > 0, "source `{}` truncated mid-transfer", task.src);
            writer
                .write(buf)
                .await
                .with_context(|| format!("write `{}` at {offset}", task.dst))?;
            offset += read;
            live.advance(read);
            if last_persist.elapsed() >= PERSIST_EVERY {
                let at = checkpoint(offset, can_append);
                store::update_task_offset(pool, task.id, at).await?;
                live.checkpointed(at);
                last_persist = Instant::now();
            }
        }
        Ok(None)
    }
    .await;

    match copy_result {
        Ok(None) => {
            writer
                .close()
                .await
                .with_context(|| format!("finalize `{}`", task.dst))?;
            finish_transfer(pool, src, &src_op, &src_path, dst, &dest, &meta, task, opts).await?;
            Ok(Outcome::Done { bytes: size })
        }
        Ok(Some(Outcome::Cancelled)) => {
            // Discard: no half-written object or partial file left behind.
            let _ = writer.abort().await;
            if let Some((partial, _)) = &dest.partial {
                // A partial that can't be deleted now stays recorded, so
                // the job's cleanup tries again.
                match dest.op.delete(partial).await {
                    Ok(()) => store::set_task_partial(pool, task.id, None).await?,
                    Err(error) => tracing::warn!(%partial, %error, "removing cancelled partial failed"),
                }
                store::update_task_offset(pool, task.id, 0).await?;
            }
            Ok(Outcome::Cancelled)
        }
        Ok(Some(outcome)) => {
            // Pause: keep the partial file where the backend can resume it,
            // discard it where it can't (the offset checkpoint resets too).
            settle_partial(writer, can_append).await;
            store::update_task_offset(pool, task.id, checkpoint(offset, can_append)).await?;
            Ok(outcome)
        }
        Err(error) => {
            settle_partial(writer, can_append).await;
            let _ = store::update_task_offset(pool, task.id, checkpoint(offset, can_append)).await;
            Err(error)
        }
    }
}

/// Post-transfer duties on a fully written destination: verification,
/// committing the partial onto the real name, timestamp preservation, and
/// the source deletion move semantics require.
#[allow(clippy::too_many_arguments)]
async fn finish_transfer(
    pool: &SqlitePool,
    src: &Provider,
    src_op: &Operator,
    src_path: &str,
    dst: &Provider,
    dest: &Dest,
    src_meta: &opendal::Metadata,
    task: &ClaimedTask,
    opts: &TransferOpts,
) -> Result<()> {
    let size = src_meta.content_length();

    if opts.verify {
        let dst_meta = dest
            .op
            .stat(dest.write_path())
            .await
            .with_context(|| format!("verifying `{}`", dest.browse))?;
        anyhow::ensure!(
            dst_meta.content_length() == size,
            "verification failed: destination holds {} bytes, source {size}",
            dst_meta.content_length(),
        );
        if let (Some(src_etag), Some(dst_etag)) = (src_meta.etag(), dst_meta.etag()) {
            if checksums_comparable(&src.scheme, &dst.scheme, src_etag, dst_etag) {
                anyhow::ensure!(
                    normalize_etag(src_etag) == normalize_etag(dst_etag),
                    "verification failed: checksum mismatch ({src_etag} → {dst_etag})",
                );
            }
        }
    }

    if let Some((partial, _)) = &dest.partial {
        commit_partial(&dest.op, partial, &dest.path).await?;
        store::set_task_partial(pool, task.id, None).await?;
    }

    if opts.preserve_timestamps {
        if let Some(mtime) = src_meta.last_modified().and_then(crate::provider::to_datetime) {
            if let Err(error) = dst.set_mtime(&dest.browse, mtime).await {
                // Best effort: object stores can't set mtime at all.
                tracing::debug!(dst = %dest.browse, %error, "preserving mtime not possible");
            }
        }
    }

    if task.move_source {
        // Only delete what was copied: a source that changed meanwhile stays.
        let now = src_op
            .stat(src_path)
            .await
            .with_context(|| format!("checking moved source `{}`", task.src))?;
        anyhow::ensure!(
            same_version(src_meta, &now),
            "`{}` changed while it was being moved, so it was kept at the source",
            task.src,
        );
        src_op
            .delete(src_path)
            .await
            .with_context(|| format!("deleting moved source `{}`", task.src))?;
    }

    Ok(())
}

/// Move a finished partial onto the real name. Some servers (SFTP v3) refuse
/// to rename over an existing file. Only for that case, with both files
/// present and no permission or not-found error, the old file is moved
/// aside to a backup name first, the partial renamed into place, and the
/// backup deleted. A failure in between moves the old file back, so it is
/// never lost.
async fn commit_partial(op: &Operator, partial: &str, path: &str) -> Result<()> {
    let Err(error) = op.rename(partial, path).await else {
        return Ok(());
    };
    let context = || format!("moving the finished file to `{path}`");
    let refused_over_existing = !matches!(
        error.kind(),
        opendal::ErrorKind::NotFound | opendal::ErrorKind::PermissionDenied
    ) && op.exists(partial).await.unwrap_or(false)
        && op.exists(path).await.unwrap_or(false);
    if !refused_over_existing {
        return Err(error).with_context(context);
    }

    let backup = format!(
        "{}.old{PARTIAL_SUFFIX}",
        partial.strip_suffix(PARTIAL_SUFFIX).unwrap_or(partial)
    );
    if op.rename(path, &backup).await.is_err() {
        return Err(error).with_context(context);
    }
    match op.rename(partial, path).await {
        Ok(()) => {
            if let Err(error) = op.delete(&backup).await {
                tracing::warn!(%backup, %error, "removing the replaced file's backup failed");
            }
            Ok(())
        }
        Err(error) => {
            if let Err(restore) = op.rename(&backup, path).await {
                tracing::error!(%backup, %path, error = %restore, "restoring the replaced file failed; it is kept under the backup name");
            }
            Err(error).with_context(context)
        }
    }
}

/// Delete a partial an earlier run of the task left under another name
/// (its destination changed since). Best effort: it is only clutter.
async fn discard_stale_partial(dst: &Provider, partial: &str) {
    let result = match dst.op_for(partial) {
        Ok((op, rel)) => op.delete(&rel).await.map_err(anyhow::Error::from),
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        tracing::warn!(%partial, error = %format!("{error:#}"), "removing an outdated partial failed");
    }
}

/// Backends whose single-part ETag is the content MD5 (S3 and its
/// compatible relatives). Checksums are only compared within one of these.
const MD5_ETAG_SCHEMES: &[&str] = &["s3", "oss", "cos", "obs"];

/// Two ETags mean the same thing only when both sides are the same kind of
/// MD5-ETag backend and both are plain (not multipart) MD5 hex digests.
fn checksums_comparable(src_scheme: &str, dst_scheme: &str, src: &str, dst: &str) -> bool {
    let is_md5 = |etag: &str| {
        let hex = normalize_etag(etag);
        hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit())
    };
    src_scheme == dst_scheme && MD5_ETAG_SCHEMES.contains(&src_scheme) && is_md5(src) && is_md5(dst)
}

fn normalize_etag(etag: &str) -> String {
    etag.trim_start_matches("W/").trim_matches('"').to_ascii_lowercase()
}

/// The source still is the version that was copied.
fn same_version(before: &opendal::Metadata, now: &opendal::Metadata) -> bool {
    before.content_length() == now.content_length()
        && before.etag() == now.etag()
        && before.last_modified() == now.last_modified()
}

/// First "name (n).ext" the destination doesn't already have and no other
/// task that may still write it targets (policy `rename`). Each candidate is
/// reserved on the task row first and probed while held: a task that still
/// writes the name blocks the reservation, one that finished it left the
/// file behind for the probe. A candidate that exists is released again.
async fn unique_name(
    pool: &SqlitePool,
    task_id: i64,
    dst_op: &Operator,
    dst_path: &str,
    dst_browse: &str,
) -> Result<String> {
    let name = file_name(dst_path);
    let parent = &dst_path[..dst_path.len() - name.len()];
    let browse_parent = &dst_browse[..dst_browse.len() - name.len()];
    // "archive.tar.gz" → ("archive", ".tar.gz") is nice-to-have; splitting at
    // the first dot after the first char covers it and keeps dotfiles whole.
    let (stem, ext) = match name.char_indices().skip(1).find(|(_, c)| *c == '.') {
        Some((ix, _)) => name.split_at(ix),
        None => (name, ""),
    };
    for n in 1..1000u32 {
        let candidate = format!("{stem} ({n}){ext}");
        if !store::reserve_task_dst(pool, task_id, &format!("{browse_parent}{candidate}")).await? {
            continue;
        }
        let probe = dst_op.stat(&format!("{parent}{candidate}")).await;
        if matches!(&probe, Err(error) if error.kind() == opendal::ErrorKind::NotFound) {
            return Ok(candidate);
        }
        store::set_task_dst(pool, task_id, dst_browse).await?;
        if let Err(error) = probe {
            return Err(error).context("probing rename target");
        }
    }
    anyhow::bail!("no free rename target for `{name}` after 999 tries")
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// A resume offset only counts if the source provably is the one the
/// partial came from and the destination actually holds that many bytes.
async fn resume_offset(
    dest: &Dest,
    task: &ClaimedTask,
    size: u64,
    etag: Option<&str>,
    modified: Option<&str>,
    can_append: bool,
) -> u64 {
    if task.byte_offset == 0 || !can_append || !source_unchanged(task, size, etag, modified) {
        return 0;
    }
    match dest.op.stat(dest.write_path()).await {
        Ok(meta) if meta.content_length() <= size => meta.content_length(),
        _ => 0,
    }
}

/// Same size, and every identity field (ETag, mtime) known on both sides
/// matches, with at least one of them known. Without that, a same-size
/// edit can't be told apart, so the caller restarts from zero.
fn source_unchanged(
    task: &ClaimedTask,
    size: u64,
    etag: Option<&str>,
    modified: Option<&str>,
) -> bool {
    if task.total_bytes != Some(size) {
        return false;
    }
    let mut proven = false;
    for (recorded, current) in [
        (task.src_etag.as_deref(), etag),
        (task.src_modified.as_deref(), modified),
    ] {
        match (recorded, current) {
            (Some(recorded), Some(current)) if recorded == current => proven = true,
            (None, None) => {}
            _ => return false,
        }
    }
    proven
}

/// Bytes recoverable after an interruption: everything written so far when
/// the destination can append, nothing otherwise.
fn checkpoint(offset: u64, can_append: bool) -> u64 {
    if can_append {
        offset
    } else {
        0
    }
}

async fn settle_partial(mut writer: Writer, can_append: bool) {
    let result = if can_append {
        writer.close().await.map(|_| ())
    } else {
        writer.abort().await
    };
    if let Err(error) = result {
        tracing::warn!(%error, "settling partial destination failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(etag: Option<&str>, modified: Option<&str>) -> ClaimedTask {
        ClaimedTask {
            id: 1,
            job_id: 1,
            src: "a".into(),
            dst: "b".into(),
            byte_offset: 4,
            total_bytes: Some(10),
            src_etag: etag.map(str::to_string),
            src_modified: modified.map(str::to_string),
            conflict_policy: ConflictPolicy::Overwrite,
            move_source: false,
            partial: None,
            in_place: false,
        }
    }

    #[tokio::test]
    async fn a_failed_commit_keeps_the_old_file() {
        let dir = std::env::temp_dir().join(format!("kessel-commit-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("file.txt"), b"OLD").unwrap();
        let op = Operator::via_iter("fs", [("root".to_string(), dir.to_string_lossy().to_string())]).unwrap();
        // The partial is missing: the rename fails for a reason other than
        // the destination existing, so the destination must stay.
        assert!(commit_partial(&op, ".file.txt.1.kessel-part", "file.txt").await.is_err());
        assert_eq!(std::fs::read(dir.join("file.txt")).unwrap(), b"OLD");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keep_both_never_hands_out_a_name_another_task_may_write() {
        let dir = std::env::temp_dir().join(format!("kessel-rename-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("file.txt"), b"OLD").unwrap();
        let op = Operator::via_iter("fs", [("root".to_string(), dir.to_string_lossy().to_string())]).unwrap();
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let job: i64 = sqlx::query_scalar(
            "INSERT INTO transfer_jobs (src_site_id, dst_site_id, src, dst, state, expanded) \
             VALUES (0, 0, 'src', '', 'active', 1) RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO transfer_tasks (job_id, src, dst, state) \
                 VALUES (?, 'src', 'file.txt', 'active') RETURNING id",
            )
            .bind(job)
            .fetch_one(&pool)
            .await
            .unwrap();
            ids.push(id);
        }
        let (a, b, c) = (ids[0], ids[1], ids[2]);
        let name = |id| unique_name(&pool, id, &op, "file.txt", "file.txt");

        // A holds "file (1).txt" before writing it: B's reservation fails
        // although the name is still free on disk.
        assert_eq!(name(a).await.unwrap(), "file (1).txt");
        assert_eq!(name(b).await.unwrap(), "file (2).txt");
        // A commits and completes, so its row stops holding the name; the
        // file now does. B fails and keeps "(2)" for a retry that resumes
        // its partial without looking at the destination again.
        std::fs::write(dir.join("file (1).txt"), b"A").unwrap();
        store::complete_task(&pool, a, 1).await.unwrap();
        assert!(store::task_failed(&pool, b, "denied", false, 0).await.unwrap());
        assert_eq!(name(c).await.unwrap(), "file (3).txt");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resume_needs_a_provably_unchanged_source() {
        let t1 = Some("2026-01-01T00:00:00+00:00");
        let t2 = Some("2026-01-02T00:00:00+00:00");
        assert!(source_unchanged(&task(None, t1), 10, None, t1));
        assert!(source_unchanged(&task(Some("e"), None), 10, Some("e"), None));
        // Same size, but nothing proves it is the same content.
        assert!(!source_unchanged(&task(None, None), 10, None, None));
        // Same-size edit: the mtime moved.
        assert!(!source_unchanged(&task(None, t1), 10, None, t2));
        assert!(!source_unchanged(&task(Some("e"), t1), 10, None, t1));
        assert!(!source_unchanged(&task(None, t1), 11, None, t1));
    }
}
