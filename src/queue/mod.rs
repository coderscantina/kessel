//! The persistent transfer queue: SQLite is the source of truth for every
//! job and per-file task (state + byte offset), so the queue survives a
//! crash or quit and resumes mid-transfer on relaunch.
//!
//! Both ends of a transfer are OpenDAL sites — the local filesystem is the
//! reserved site 0 — so upload, download and site-to-site copy are the same
//! code path: a layered source Operator streamed into a layered destination
//! Operator. Retry, timeout, throttling and tracing come from the per-site
//! OpenDAL layer stack built in [`crate::provider`]; this module owns only
//! scheduling, persistence and resume.

mod engine;
pub(crate) mod store;
mod transfer;

pub use engine::start;
pub(crate) use transfer::PARTIAL_SUFFIX;

use tokio::sync::{mpsc, oneshot, watch};

/// The reserved `sites` row backing the local-filesystem side of transfers.
pub const LOCAL_SITE_ID: i64 = 0;

/// A user-level transfer request. `src` is a file or directory browse path
/// (directories end with `/`); `dst_dir` is the destination directory prefix
/// (`""` = site root). Directory sources fan out into one task per file.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub src_site_id: i64,
    pub dst_site_id: i64,
    pub src: String,
    pub dst_dir: String,
    /// Name to write under at the destination; `None` keeps the source name.
    pub dst_name: Option<String>,
    pub conflict_policy: ConflictPolicy,
    /// Move semantics: delete each source file after its successful
    /// transfer (and clean up emptied source directories afterwards).
    pub move_source: bool,
}

/// An explicit list of files copied as one job with one task per file, for
/// callers that already know every pair (sync). Directories in between are
/// created implicitly by the writes. `src_label` / `dst_label` are what the
/// transfers panel shows as the job's source and destination.
#[derive(Debug, Clone)]
pub struct NewBatch {
    pub src_site_id: i64,
    pub dst_site_id: i64,
    pub src_label: String,
    pub dst_label: String,
    pub files: Vec<BatchFile>,
    pub conflict_policy: ConflictPolicy,
    /// Delete each source file once it arrived intact.
    pub move_source: bool,
}

/// One file of a [`NewBatch`], as full browse paths on each site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchFile {
    pub src: String,
    pub dst: String,
    /// Expected size, so the job's total is right before files start.
    pub size: Option<u64>,
}

/// What to do when a destination file already exists. Applied per file at
/// transfer time; `Ask` parks the file until the user decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictPolicy {
    #[default]
    Ask,
    Overwrite,
    /// Replace only a newer source; missing timestamps leave a conflict.
    OverwriteNewer,
    Skip,
    /// Append the missing tail when the destination provably is the start of
    /// the source and the backend can append; skip files the destination
    /// already holds completely; otherwise ask, and never overwrite.
    Resume,
    /// Write under a unique name ("file (1).txt").
    Rename,
}

impl ConflictPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ask => "ask",
            Self::Overwrite => "overwrite",
            Self::OverwriteNewer => "overwrite_newer",
            Self::Skip => "skip",
            Self::Resume => "resume",
            Self::Rename => "rename",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "overwrite" => Self::Overwrite,
            "overwrite_newer" => Self::OverwriteNewer,
            "skip" => Self::Skip,
            "resume" => Self::Resume,
            "rename" => Self::Rename,
            _ => Self::Ask,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Active,
    Paused,
    Failed,
    Done,
    /// Cancel requested; workers and partial cleanup are still winding
    /// down. Never stored: a `cancelled` row that hasn't settled yet.
    Cancelling,
    Cancelled,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Failed => "failed",
            Self::Done => "done",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "paused" => Self::Paused,
            "failed" => Self::Failed,
            "done" => Self::Done,
            "cancelling" => Self::Cancelling,
            "cancelled" => Self::Cancelled,
            _ => Self::Queued,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Done | Self::Cancelled)
    }
}

/// One job as shown to the UI; aggregates come straight from SQLite,
/// throughput from the engine's own sampling.
#[derive(Debug, Clone)]
pub struct JobSnapshot {
    pub id: i64,
    pub src_site_id: i64,
    pub dst_site_id: i64,
    pub src_site: String,
    pub dst_site: String,
    pub src: String,
    pub dst: String,
    /// Where a single-file job writes, after any "Keep both" rename; `None`
    /// for folder and batch jobs.
    pub dst_file: Option<String>,
    pub state: JobState,
    pub error: Option<String>,
    pub files_total: u64,
    pub files_done: u64,
    pub files_skipped: u64,
    /// Files parked awaiting a conflict decision.
    pub files_conflict: u64,
    /// Files that failed for good. A `Done` job with failed files finished
    /// with errors; `error` then holds the count and a representative reason.
    pub files_failed: u64,
    pub bytes_total: u64,
    /// Bytes transferred, including in-flight bytes not yet checkpointed.
    pub bytes_done: u64,
    /// Smoothed bytes/sec while the job is active.
    pub bps: u64,
    /// Scheduling priority (higher runs first); 0 is the default.
    pub priority: i64,
    /// Sources are deleted after successful transfer (move semantics).
    pub move_source: bool,
    /// Files found so far while the source tree is being listed.
    pub discovering: Option<u64>,
}

/// One per-file task, for the transfers panel's drill-down view.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub src: String,
    pub state: String,
    pub bytes_done: u64,
    pub total_bytes: Option<u64>,
    pub error: Option<String>,
}

/// One parked conflict, for the resolve dialog's size/date comparison.
#[derive(Debug, Clone)]
pub struct ConflictInfo {
    pub id: i64,
    pub dst: String,
    pub src_size: Option<u64>,
    pub src_modified: Option<String>,
    pub dst_size: Option<u64>,
    pub dst_modified: Option<String>,
}

/// One finished job from the audit trail (`transfer_history`).
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub src_site: String,
    pub dst_site: String,
    pub src: String,
    pub dst: String,
    pub outcome: String,
    pub files: i64,
    pub files_failed: i64,
    pub bytes: u64,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub finished_at: String,
}

/// How a job settled, as reported by [`QueueHandle::settled`].
#[derive(Debug, Clone)]
pub struct JobOutcome {
    /// `Done` (possibly with failed files), `Failed` or `Cancelled`.
    pub state: JobState,
    /// What went wrong, in plain words: the reason for a failed job, or the
    /// failure count plus a representative reason for a done one.
    pub error: Option<String>,
    pub files_done: u64,
    pub files_failed: u64,
}

impl JobSnapshot {
    /// "↑ upload" / "↓ download" / "⇄ copy", derived from which side is local.
    pub fn direction(&self) -> &'static str {
        match (
            self.src_site_id == LOCAL_SITE_ID,
            self.dst_site_id == LOCAL_SITE_ID,
        ) {
            (true, false) => "upload",
            (false, true) => "download",
            _ => "copy",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueueSnapshot {
    /// Newest first.
    pub jobs: Vec<JobSnapshot>,
    pub total_bps: u64,
    /// Current global concurrency cap (runtime-adjustable).
    pub max_parallel: usize,
    /// Current global bandwidth cap in bytes/sec, 0 = unlimited, shared by
    /// all transfers (runtime-adjustable; applies to files started afterwards).
    pub bandwidth_limit: u64,
}

#[derive(Debug)]
pub(crate) enum Command {
    Enqueue(NewJob),
    /// Enqueue and report the created job id back — used by drag-out file
    /// promises, which must know when *their* job settles.
    EnqueueTracked(NewJob, oneshot::Sender<i64>),
    /// Enqueue and report the persisted id or the rejection reason.
    Submit(NewJob, oneshot::Sender<anyhow::Result<i64>>),
    /// Persist a multi-file job and report its id or the rejection reason.
    SubmitBatch(NewBatch, oneshot::Sender<anyhow::Result<i64>>),
    /// Report the job's outcome once it settles.
    Settled(i64, oneshot::Sender<anyhow::Result<JobOutcome>>),
    Pause(i64),
    Resume(i64),
    Cancel(i64),
    Retry(i64),
    /// Re-run only the failed files of a settled job.
    RetryFailed(i64),
    /// Apply a decision to a job's parked conflicts (and future ones).
    Resolve(i64, ConflictPolicy),
    ResolveTask(i64, ConflictPolicy),
    ClearFinished,
    /// Remove one terminal job from the queue view (history keeps the row).
    Remove(i64),
    /// Set a job's scheduling priority (higher runs first).
    SetPriority(i64, i64),
    /// Pause / resume every non-terminal job at once.
    PauseAll,
    ResumeAll,
    /// Adjust the global concurrency cap at runtime.
    SetMaxParallel(usize),
    /// Adjust the global bandwidth cap (bytes/sec, 0 = unlimited) at
    /// runtime; takes effect for files started afterwards.
    SetBandwidth(u64),
    /// Apply reloaded `transfers.*` config values (retries, verify,
    /// preserve_timestamps, skip_hidden) at runtime.
    SetTransferSettings {
        retries: u32,
        verify: bool,
        preserve_timestamps: bool,
        skip_hidden: bool,
    },
    /// Apply a reloaded default OpenDAL layer stack; used for jobs prepared
    /// from now on (running jobs keep their connected stack).
    SetDefaultLayers(crate::provider::LayerConfig),
    /// Graceful quit: stop claiming, pause running workers so they flush
    /// their offsets and settle partial writes, then ack.
    Shutdown(oneshot::Sender<()>),
}

/// Cheap clonable handle to the engine: fire-and-forget commands plus a
/// watch channel the UI subscribes to for live snapshots.
#[derive(Clone)]
pub struct QueueHandle {
    cmds: mpsc::UnboundedSender<Command>,
    snapshot: watch::Receiver<QueueSnapshot>,
}

impl QueueHandle {
    /// Fire-and-forget enqueue; a rejected job only reaches the log.
    /// Prefer [`Self::submit`].
    pub fn enqueue(&self, job: NewJob) {
        let _ = self.cmds.send(Command::Enqueue(job));
    }
    /// Enqueue and get the job id back once the engine has persisted it.
    /// The receiver errors if the job was rejected (e.g. src == dst).
    pub fn enqueue_tracked(&self, job: NewJob) -> oneshot::Receiver<i64> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmds.send(Command::EnqueueTracked(job, tx));
        rx
    }
    /// Enqueue and wait until the job is persisted. Returns its id, or the
    /// reason it was rejected (e.g. source and destination are the same).
    pub async fn submit(&self, job: NewJob) -> anyhow::Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Command::Submit(job, tx))
            .map_err(|_| anyhow::anyhow!("the transfer queue has stopped"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("the transfer queue has stopped"))?
    }
    /// Persist a multi-file job (one task per listed file) and return its id.
    pub async fn submit_batch(&self, batch: NewBatch) -> anyhow::Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Command::SubmitBatch(batch, tx))
            .map_err(|_| anyhow::anyhow!("the transfer queue has stopped"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("the transfer queue has stopped"))?
    }
    /// Wait until the job settles (done, failed or cancelled) and return its
    /// outcome. Resolves at once for a job that already settled. Errors if
    /// the job doesn't exist or the engine stops first.
    pub async fn settled(&self, job_id: i64) -> anyhow::Result<JobOutcome> {
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Command::Settled(job_id, tx))
            .map_err(|_| anyhow::anyhow!("the transfer queue has stopped"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("the transfer queue has stopped"))?
    }
    /// Wait until the job reaches a terminal state. Prefer [`Self::settled`],
    /// which also carries the error; this reports an unknown job or a
    /// stopped engine as `Cancelled`.
    pub async fn wait_terminal(&self, job_id: i64) -> JobState {
        self.settled(job_id)
            .await
            .map_or(JobState::Cancelled, |outcome| outcome.state)
    }
    pub fn pause(&self, job_id: i64) {
        let _ = self.cmds.send(Command::Pause(job_id));
    }
    pub fn resume(&self, job_id: i64) {
        let _ = self.cmds.send(Command::Resume(job_id));
    }
    pub fn cancel(&self, job_id: i64) {
        let _ = self.cmds.send(Command::Cancel(job_id));
    }
    pub fn retry(&self, job_id: i64) {
        let _ = self.cmds.send(Command::Retry(job_id));
    }
    /// Re-run only the failed files of a settled job; done files stay done.
    pub fn retry_failed(&self, job_id: i64) {
        let _ = self.cmds.send(Command::RetryFailed(job_id));
    }
    /// Resolve a job's parked conflicts; the policy also applies to any
    /// conflicts the job hits later.
    pub fn resolve(&self, job_id: i64, policy: ConflictPolicy) {
        let _ = self.cmds.send(Command::Resolve(job_id, policy));
    }
    pub fn resolve_task(&self, task_id: i64, policy: ConflictPolicy) {
        let _ = self.cmds.send(Command::ResolveTask(task_id, policy));
    }
    pub fn clear_finished(&self) {
        let _ = self.cmds.send(Command::ClearFinished);
    }
    /// Remove one terminal job from the queue view.
    pub fn remove(&self, job_id: i64) {
        let _ = self.cmds.send(Command::Remove(job_id));
    }
    /// Set a job's scheduling priority (higher runs first).
    pub fn set_priority(&self, job_id: i64, priority: i64) {
        let _ = self.cmds.send(Command::SetPriority(job_id, priority));
    }
    pub fn pause_all(&self) {
        let _ = self.cmds.send(Command::PauseAll);
    }
    pub fn resume_all(&self) {
        let _ = self.cmds.send(Command::ResumeAll);
    }
    pub fn set_max_parallel(&self, limit: usize) {
        let _ = self.cmds.send(Command::SetMaxParallel(limit));
    }
    /// Bytes/sec, 0 = unlimited, shared by all transfers. Applies to files
    /// started afterwards.
    pub fn set_bandwidth(&self, bps: u64) {
        let _ = self.cmds.send(Command::SetBandwidth(bps));
    }
    /// Apply reloaded `transfers.*` behavior knobs at runtime.
    pub fn set_transfer_settings(&self, cfg: &crate::config::TransferConfig) {
        let _ = self.cmds.send(Command::SetTransferSettings {
            retries: cfg.retries,
            verify: cfg.verify,
            preserve_timestamps: cfg.preserve_timestamps,
            skip_hidden: cfg.skip_hidden,
        });
    }
    /// Apply a reloaded default layer stack for jobs prepared from now on.
    pub fn set_default_layers(&self, layers: crate::provider::LayerConfig) {
        let _ = self.cmds.send(Command::SetDefaultLayers(layers));
    }

    /// Ask the engine to wind down; the receiver resolves once running
    /// transfers have checkpointed and settled. Interrupted work resumes on
    /// next launch. Callers should race this against a timeout.
    pub fn shutdown(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmds.send(Command::Shutdown(tx));
        rx
    }

    pub fn subscribe(&self) -> watch::Receiver<QueueSnapshot> {
        self.snapshot.clone()
    }

    pub fn latest(&self) -> QueueSnapshot {
        self.snapshot.borrow().clone()
    }
}
