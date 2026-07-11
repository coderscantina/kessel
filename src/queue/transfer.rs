//! One task = one file, streamed source → destination in ranged chunks.
//!
//! Resume model: the byte offset is checkpointed to SQLite every 500ms.
//! When the destination backend can append (local fs, and any OpenDAL
//! service reporting `write_can_append`), a partial file resumes exactly
//! where it stopped — the destination's own length is the truth, clamped by
//! a source that must be unchanged (size + etag). Backends without append
//! (e.g. plain S3 multipart) restart the file; partial multipart uploads are
//! aborted rather than committed so no garbage objects appear.
//!
//! Everything transient (dropped connections, 5xx, timeouts) is already
//! retried *inside* each read/write by the site's OpenDAL RetryLayer; an
//! error surfacing here means the layer stack gave up.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result};
use opendal::{Operator, Writer};
use sqlx::SqlitePool;
use tokio::time::Instant;

use crate::provider::Provider;

use super::store::{self, ClaimedTask};
use super::ConflictPolicy;

/// Ranged-read / write unit. Also the multipart chunk size on upload.
const CHUNK: u64 = 8 * 1024 * 1024;
/// How often the resume offset is checkpointed to SQLite.
const PERSIST_EVERY: Duration = Duration::from_millis(500);

/// Shared pause/cancel flags, one per job; workers poll between chunks so a
/// pause takes effect within one chunk's worth of I/O.
#[derive(Debug, Default)]
pub(crate) struct JobControl {
    pub paused: AtomicBool,
    pub cancelled: AtomicBool,
}

#[derive(Clone, Copy, PartialEq)]
enum Signal {
    Run,
    Pause,
    Cancel,
}

impl JobControl {
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

/// Per-job behavior flags, derived from the job row and the app config.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransferOpts {
    /// Source and destination are the same site — server-side copy applies.
    pub same_site: bool,
    /// Re-stat the destination after the transfer and compare sizes/etags.
    pub verify: bool,
    /// Copy the source mtime onto the destination where the backend can.
    pub preserve_timestamps: bool,
}

#[derive(Debug)]
pub(crate) enum Outcome {
    Done { bytes: u64 },
    /// Destination already existed and the policy said to leave it alone.
    Skipped,
    /// Destination already existed under policy `Ask`: parked for the user,
    /// with what the destination held at detection time.
    Conflict {
        dst_size: u64,
        dst_modified: Option<String>,
    },
    Paused,
    Cancelled,
    Failed(anyhow::Error),
}

pub(crate) async fn run_task(
    pool: &SqlitePool,
    src: &Provider,
    dst: &Provider,
    task: &ClaimedTask,
    control: &JobControl,
    opts: TransferOpts,
) -> Outcome {
    match run_inner(pool, src, dst, task, control, opts).await {
        Ok(outcome) => outcome,
        Err(error) => Outcome::Failed(error),
    }
}

async fn run_inner(
    pool: &SqlitePool,
    src: &Provider,
    dst: &Provider,
    task: &ClaimedTask,
    control: &JobControl,
    opts: TransferOpts,
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
    let can_append = dst.caps.write_can_append;

    // Conflict check: only for a fresh transfer (byte_offset > 0 means the
    // partial destination is our own earlier work, handled by resume below).
    let mut resume_from_dst = None;
    if task.byte_offset == 0 {
        if let Ok(dst_meta) = dst_op.stat(&dst_path).await {
            if !dst_meta.is_dir() {
                let dst_len = dst_meta.content_length();
                match task.conflict_policy {
                    ConflictPolicy::Overwrite => {}
                    ConflictPolicy::Ask => {
                        return Ok(Outcome::Conflict {
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
                        let new_name = unique_name(&dst_op, &dst_path).await?;
                        let old_name_len = file_name(&dst_path).len();
                        dst_path.truncate(dst_path.len() - old_name_len);
                        dst_path.push_str(&new_name);
                        dst_browse.truncate(dst_browse.len() - old_name_len);
                        dst_browse.push_str(&new_name);
                        store::update_task_dst(pool, task.id, &dst_browse).await?;
                    }
                }
            }
        }
    }

    let offset = match resume_from_dst {
        Some(offset) => offset,
        None => resume_offset(&dst_op, &dst_path, task, size, etag.as_deref(), can_append).await,
    };
    store::update_task_meta(pool, task.id, size, etag.as_deref(), offset).await?;

    if offset == size && size > 0 {
        finish_transfer(
            &src_op, dst, &src_path, &dst_op, &dst_path, &dst_browse, &meta, task, opts,
        )
        .await?;
        return Ok(Outcome::Done { bytes: size });
    }

    // Same-site fast path: let the backend copy server-side (S3 CopyObject
    // and friends) instead of streaming every byte through the client.
    if opts.same_site && dst.caps.copy && offset == 0 {
        match dst.copy(&task.src, &dst_browse).await {
            Ok(()) => {
                finish_transfer(
                    &src_op, dst, &src_path, &dst_op, &dst_path, &dst_browse, &meta, task, opts,
                )
                .await?;
                return Ok(Outcome::Done { bytes: size });
            }
            Err(error) => {
                tracing::warn!(
                    src = %task.src, dst = %dst_browse, %error,
                    "server-side copy failed; falling back to streaming"
                );
            }
        }
    }

    let mut writer = if offset > 0 {
        dst_op
            .writer_with(&dst_path)
            .append(true)
            .await
            .with_context(|| format!("open destination `{}` for append", task.dst))?
    } else {
        dst_op
            .writer_with(&dst_path)
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
            if last_persist.elapsed() >= PERSIST_EVERY {
                store::update_task_offset(pool, task.id, checkpoint(offset, can_append)).await?;
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
            finish_transfer(
                &src_op, dst, &src_path, &dst_op, &dst_path, &dst_browse, &meta, task, opts,
            )
            .await?;
            Ok(Outcome::Done { bytes: size })
        }
        Ok(Some(Outcome::Cancelled)) => {
            // Discard: no half-written object left behind.
            let _ = writer.abort().await;
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

/// Post-transfer duties on a successfully written destination:
/// verification (size, comparable etags), timestamp preservation, and the
/// source deletion move semantics require.
#[allow(clippy::too_many_arguments)]
async fn finish_transfer(
    src_op: &Operator,
    dst: &Provider,
    src_path: &str,
    dst_op: &Operator,
    dst_path: &str,
    dst_browse: &str,
    src_meta: &opendal::Metadata,
    task: &ClaimedTask,
    opts: TransferOpts,
) -> Result<()> {
    let size = src_meta.content_length();

    if opts.verify {
        let dst_meta = dst_op
            .stat(dst_path)
            .await
            .with_context(|| format!("verifying `{dst_browse}`"))?;
        anyhow::ensure!(
            dst_meta.content_length() == size,
            "verification failed: destination holds {} bytes, source {size}",
            dst_meta.content_length(),
        );
        // Etags are only comparable when both sides expose a plain (single
        // part) checksum; multipart etags ('-') and missing etags are fine.
        if let (Some(src_etag), Some(dst_etag)) = (src_meta.etag(), dst_meta.etag()) {
            let comparable = !src_etag.contains('-') && !dst_etag.contains('-');
            anyhow::ensure!(
                !comparable || src_etag == dst_etag,
                "verification failed: checksum mismatch ({src_etag} → {dst_etag})",
            );
        }
    }

    if opts.preserve_timestamps {
        if let Some(mtime) = src_meta.last_modified().and_then(crate::provider::to_datetime) {
            if let Err(error) = dst.set_mtime(dst_browse, mtime).await {
                // Best effort: object stores can't set mtime at all.
                tracing::debug!(dst = %dst_browse, %error, "preserving mtime not possible");
            }
        }
    }

    if task.move_source {
        src_op
            .delete(src_path)
            .await
            .with_context(|| format!("deleting moved source `{}`", task.src))?;
    }

    Ok(())
}

/// First "name (n).ext" the destination doesn't already have (policy
/// `rename`).
async fn unique_name(dst_op: &Operator, dst_path: &str) -> Result<String> {
    let name = file_name(dst_path);
    let parent = &dst_path[..dst_path.len() - name.len()];
    // "archive.tar.gz" → ("archive", ".tar.gz") is nice-to-have; splitting at
    // the first dot after the first char covers it and keeps dotfiles whole.
    let (stem, ext) = match name[1..].find('.') {
        Some(ix) => name.split_at(ix + 1),
        None => (name, ""),
    };
    for n in 1..1000u32 {
        let candidate = format!("{stem} ({n}){ext}");
        match dst_op.stat(&format!("{parent}{candidate}")).await {
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => return Ok(candidate),
            Err(error) => return Err(error).context("probing rename target"),
            Ok(_) => {}
        }
    }
    anyhow::bail!("no free rename target for `{name}` after 999 tries")
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// A resume offset only counts if the source is unchanged since it was
/// recorded and the destination actually holds that many bytes.
async fn resume_offset(
    dst_op: &Operator,
    dst_path: &str,
    task: &ClaimedTask,
    size: u64,
    etag: Option<&str>,
    can_append: bool,
) -> u64 {
    if task.byte_offset == 0 || !can_append {
        return 0;
    }
    if task.total_bytes != Some(size) || task.src_etag.as_deref() != etag {
        return 0;
    }
    match dst_op.stat(dst_path).await {
        Ok(meta) if meta.content_length() <= size => meta.content_length(),
        _ => 0,
    }
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
