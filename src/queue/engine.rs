//! The queue engine: a single scheduler task on the app tokio runtime.
//!
//! Lifecycle of a job: `queued` → prepare (connect layered providers via
//! [`Provider::connect`], fan a directory out into per-file tasks) →
//! `active` → workers stream files under a global concurrency cap →
//! `done` / `failed` / `cancelled` (+ history row). Every state change goes
//! through SQLite first, so a crash at any point resumes cleanly on
//! relaunch (`db::requeue_interrupted` flips orphaned `active` rows back).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::config::TransferConfig;
use crate::provider::{LayerConfig, Provider, ThrottleConfig};

use super::store::{self, JobRow, NewTask};
use super::transfer::{self, JobControl, Outcome, TransferOpts};
use super::{Command, JobState, QueueHandle, QueueSnapshot};

/// Tasks are inserted in batches of this many rows while the source tree is
/// still being listed, so a million-file bucket never sits in memory.
const EXPANSION_BATCH: usize = 200;

/// Spawn the engine on the app runtime and hand back its control handle.
pub fn start(
    runtime: &tokio::runtime::Runtime,
    pool: SqlitePool,
    cfg: TransferConfig,
    mut default_layers: LayerConfig,
) -> QueueHandle {
    // The global bandwidth cap rides on OpenDAL's ThrottleLayer: it becomes
    // the default per-site throttle wherever a site doesn't set its own.
    if cfg.bandwidth_limit > 0 && default_layers.throttle.is_none() {
        default_layers.throttle = throttle_for(cfg.bandwidth_limit);
    }
    let max_parallel = cfg.max_parallel.max(1);

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (snap_tx, snap_rx) = watch::channel(QueueSnapshot {
        max_parallel,
        bandwidth_limit: cfg.bandwidth_limit,
        ..QueueSnapshot::default()
    });
    let (internal_tx, internal_rx) = mpsc::unbounded_channel();

    let engine = Engine {
        pool,
        default_layers,
        max_retries: cfg.retries,
        max_parallel,
        bandwidth_limit: cfg.bandwidth_limit,
        verify: cfg.verify,
        preserve_timestamps: cfg.preserve_timestamps,
        skip_hidden: cfg.skip_hidden,
        cmds: cmd_rx,
        internal_tx,
        internal_rx,
        snap_tx,
        jobs: HashMap::new(),
        speeds: HashMap::new(),
        pending_retries: HashSet::new(),
        shutdown_ack: None,
    };
    runtime.spawn(engine.run());

    QueueHandle {
        cmds: cmd_tx,
        snapshot: snap_rx,
    }
}

/// Providers are connected once per job and shared by its workers; cloning
/// an Operator is cheap (it's an Arc around the layered accessor).
#[derive(Clone)]
struct JobProviders {
    src: Provider,
    dst: Provider,
    /// Both ends are the same site — server-side copy applies.
    same_site: bool,
    /// The job transfers with move semantics (sources deleted on success).
    move_source: bool,
}

#[derive(Default)]
struct JobRuntime {
    control: Arc<JobControl>,
    providers: Option<JobProviders>,
    preparing: bool,
    active_tasks: usize,
}

enum Internal {
    Prepared(i64, Result<JobProviders>),
    TaskFinished {
        job_id: i64,
        task_id: i64,
        outcome: Outcome,
    },
}

/// Bytes/sec sampling per job, smoothed with an EMA so the UI number
/// doesn't flicker with checkpoint granularity.
struct Speed {
    last_bytes: u64,
    last_at: Instant,
    ema_bps: f64,
}

/// bytes/sec → OpenDAL throttle config (None = unlimited).
fn throttle_for(bps: u64) -> Option<ThrottleConfig> {
    (bps > 0).then(|| {
        let bandwidth = bps.min(u32::MAX as u64) as u32;
        ThrottleConfig {
            bandwidth,
            burst: bandwidth.saturating_mul(2).max(bandwidth),
        }
    })
}

struct Engine {
    pool: SqlitePool,
    default_layers: LayerConfig,
    max_retries: u32,
    /// Global concurrency cap, enforced against the engine's own in-flight
    /// worker count so it can be adjusted at runtime.
    max_parallel: usize,
    /// Mirror of the default throttle for the UI (bytes/sec, 0 = unlimited).
    bandwidth_limit: u64,
    /// Post-transfer verification / timestamp preservation / expansion
    /// filtering, from `transfers.*` in the config.
    verify: bool,
    preserve_timestamps: bool,
    skip_hidden: bool,
    cmds: mpsc::UnboundedReceiver<Command>,
    internal_tx: mpsc::UnboundedSender<Internal>,
    internal_rx: mpsc::UnboundedReceiver<Internal>,
    snap_tx: watch::Sender<QueueSnapshot>,
    jobs: HashMap<i64, JobRuntime>,
    speeds: HashMap<i64, Speed>,
    /// Retries deferred until the job's in-flight workers drain — resetting
    /// tasks under a live worker would let its final state-write race the
    /// re-queued run of the same task.
    pending_retries: HashSet<i64>,
    /// Set while a graceful shutdown drains workers; acked when idle.
    shutdown_ack: Option<oneshot::Sender<()>>,
}

impl Engine {
    async fn run(mut self) {
        self.publish().await;
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if self.shutdown_ack.is_some() {
                if !self.busy() {
                    if let Err(error) = store::requeue_shutdown_paused(&self.pool).await {
                        tracing::error!(%error, "shutdown flush failed");
                    }
                    if let Some(ack) = self.shutdown_ack.take() {
                        let _ = ack.send(());
                    }
                    break;
                }
            } else if let Err(error) = self.schedule().await {
                tracing::error!(%error, "queue scheduling failed");
            }
            tokio::select! {
                cmd = self.cmds.recv() => match cmd {
                    Some(cmd) => {
                        if let Err(error) = self.handle_command(cmd).await {
                            tracing::error!(%error, "queue command failed");
                        }
                    }
                    // Every handle dropped: the app is shutting down.
                    None => break,
                },
                Some(msg) = self.internal_rx.recv() => {
                    if let Err(error) = self.handle_internal(msg).await {
                        tracing::error!(%error, "queue bookkeeping failed");
                    }
                },
                _ = tick.tick(), if self.busy() => {},
            }
            self.publish().await;
        }
    }

    fn busy(&self) -> bool {
        self.jobs
            .values()
            .any(|rt| rt.preparing || rt.active_tasks > 0)
    }

    /// Kick off preparation for runnable jobs and claim tasks while
    /// concurrency permits are free.
    async fn schedule(&mut self) -> Result<()> {
        for job in store::runnable_jobs(&self.pool).await? {
            let rt = self.jobs.entry(job.id).or_default();
            if rt.providers.is_none() && !rt.preparing {
                rt.preparing = true;
                let pool = self.pool.clone();
                let defaults = self.default_layers.clone();
                let skip_hidden = self.skip_hidden;
                let tx = self.internal_tx.clone();
                tokio::spawn(async move {
                    let result = prepare_job(&pool, &job, &defaults, skip_hidden).await;
                    let _ = tx.send(Internal::Prepared(job.id, result));
                });
            }
        }

        let ready: Vec<i64> = self
            .jobs
            .iter()
            .filter(|(_, rt)| {
                rt.providers.is_some() && !rt.control.paused.load(Ordering::Relaxed)
            })
            .map(|(id, _)| *id)
            .collect();

        let mut in_flight: usize = self.jobs.values().map(|rt| rt.active_tasks).sum();
        while in_flight < self.max_parallel {
            let Some(task) = store::claim_task(&self.pool, &ready).await? else {
                break;
            };
            let rt = self
                .jobs
                .get_mut(&task.job_id)
                .expect("claimed tasks belong to ready jobs");
            rt.active_tasks += 1;
            in_flight += 1;
            let providers = rt.providers.clone().expect("ready jobs have providers");
            let control = rt.control.clone();
            let opts = TransferOpts {
                same_site: providers.same_site,
                verify: self.verify,
                preserve_timestamps: self.preserve_timestamps,
            };
            let pool = self.pool.clone();
            let tx = self.internal_tx.clone();
            tokio::spawn(run_worker(pool, task, providers, control, opts, tx));
        }
        Ok(())
    }

    async fn handle_command(&mut self, cmd: Command) -> Result<()> {
        match cmd {
            Command::Enqueue(job) => {
                let dst_name = job.dst_name.as_deref().unwrap_or(file_name(&job.src));
                anyhow::ensure!(
                    job.src_site_id != job.dst_site_id
                        || !format!("{}{dst_name}", job.dst_dir).eq(&job.src),
                    "source and destination are the same"
                );
                store::enqueue_job(&self.pool, &job).await?;
            }
            Command::EnqueueTracked(job, reply) => {
                let dst_name = job.dst_name.as_deref().unwrap_or(file_name(&job.src));
                anyhow::ensure!(
                    job.src_site_id != job.dst_site_id
                        || !format!("{}{dst_name}", job.dst_dir).eq(&job.src),
                    "source and destination are the same"
                );
                let id = store::enqueue_job(&self.pool, &job).await?;
                let _ = reply.send(id);
            }
            Command::Pause(id) => {
                if let Some(rt) = self.jobs.get(&id) {
                    rt.control.paused.store(true, Ordering::Relaxed);
                }
                store::pause_job(&self.pool, id).await?;
            }
            Command::Resume(id) => {
                store::resume_job(&self.pool, id).await?;
                if let Some(rt) = self.jobs.get(&id) {
                    rt.control.paused.store(false, Ordering::Relaxed);
                }
            }
            Command::Cancel(id) => {
                if let Some(rt) = self.jobs.get(&id) {
                    rt.control.cancelled.store(true, Ordering::Relaxed);
                }
                store::cancel_job(&self.pool, id).await?;
                self.maybe_finalize(id).await?;
            }
            Command::Retry(id) => {
                if self.jobs.get(&id).is_some_and(|rt| rt.active_tasks > 0) {
                    // Old workers still winding down (e.g. right after a
                    // cancel): hurry them and retry once the last reports in.
                    if let Some(rt) = self.jobs.get(&id) {
                        rt.control.cancelled.store(true, Ordering::Relaxed);
                    }
                    self.pending_retries.insert(id);
                } else {
                    self.retry_now(id).await?;
                }
            }
            Command::Resolve(id, policy) => {
                store::resolve_job_conflicts(&self.pool, id, policy).await?;
            }
            Command::ResolveTask(id, policy) => {
                store::resolve_task_conflict(&self.pool, id, policy).await?;
            }
            Command::ClearFinished => {
                for id in store::clear_finished(&self.pool).await? {
                    self.jobs.remove(&id);
                    self.speeds.remove(&id);
                }
            }
            Command::Remove(id) => {
                if store::remove_job(&self.pool, id).await? {
                    self.jobs.remove(&id);
                    self.speeds.remove(&id);
                }
            }
            Command::SetPriority(id, priority) => {
                store::set_job_priority(&self.pool, id, priority).await?;
            }
            Command::PauseAll => {
                for id in store::pause_all_jobs(&self.pool).await? {
                    if let Some(rt) = self.jobs.get(&id) {
                        rt.control.paused.store(true, Ordering::Relaxed);
                    }
                }
            }
            Command::ResumeAll => {
                for id in store::resume_all_jobs(&self.pool).await? {
                    if let Some(rt) = self.jobs.get(&id) {
                        rt.control.paused.store(false, Ordering::Relaxed);
                    }
                }
            }
            Command::SetMaxParallel(limit) => {
                // In-flight workers finish naturally; the claim loop simply
                // stops (or starts) claiming against the new cap.
                self.max_parallel = limit.max(1);
            }
            Command::SetBandwidth(bps) => {
                // Becomes the default per-site throttle for jobs prepared
                // from now on; running jobs keep their connected stack.
                self.bandwidth_limit = bps;
                self.default_layers.throttle = throttle_for(bps);
            }
            Command::SetTransferSettings {
                retries,
                verify,
                preserve_timestamps,
                skip_hidden,
            } => {
                self.max_retries = retries;
                self.verify = verify;
                self.preserve_timestamps = preserve_timestamps;
                self.skip_hidden = skip_hidden;
            }
            Command::SetDefaultLayers(mut layers) => {
                // The runtime bandwidth cap stays the default throttle
                // wherever the reloaded config doesn't set its own.
                if self.bandwidth_limit > 0 && layers.throttle.is_none() {
                    layers.throttle = throttle_for(self.bandwidth_limit);
                }
                self.default_layers = layers;
            }
            Command::Shutdown(ack) => {
                // Pause every worker (they checkpoint and settle partial
                // writes) without touching persisted job states — the run
                // loop acks and exits once the last worker reports in.
                for rt in self.jobs.values() {
                    rt.control.paused.store(true, Ordering::Relaxed);
                }
                self.shutdown_ack = Some(ack);
            }
        }
        Ok(())
    }

    async fn handle_internal(&mut self, msg: Internal) -> Result<()> {
        match msg {
            Internal::Prepared(job_id, result) => {
                let Some(rt) = self.jobs.get_mut(&job_id) else {
                    return Ok(());
                };
                rt.preparing = false;
                match result {
                    Ok(providers) => {
                        rt.providers = Some(providers);
                        // A retried job whose tasks all settled earlier
                        // (e.g. done before a cancel landed) has nothing to
                        // claim — settle it now instead of idling forever.
                        self.maybe_finalize(job_id).await?;
                    }
                    Err(error) => {
                        tracing::error!(job_id, error = %format!("{error:#}"), "job preparation failed");
                        store::fail_job(&self.pool, job_id, &format!("{error:#}")).await?;
                        self.jobs.remove(&job_id);
                    }
                }
            }
            Internal::TaskFinished {
                job_id,
                task_id,
                outcome,
            } => {
                if let Some(rt) = self.jobs.get_mut(&job_id) {
                    rt.active_tasks = rt.active_tasks.saturating_sub(1);
                }
                match outcome {
                    Outcome::Done { bytes } => {
                        store::complete_task(&self.pool, task_id, bytes).await?;
                    }
                    Outcome::Skipped => {
                        store::set_task_state(&self.pool, task_id, "skipped").await?;
                    }
                    Outcome::Conflict {
                        policy,
                        dst_size,
                        dst_modified,
                    } => {
                        store::park_task_conflict(
                            &self.pool,
                            task_id,
                            policy,
                            dst_size,
                            dst_modified.as_deref(),
                        )
                        .await?;
                    }
                    Outcome::Paused => {
                        store::set_task_state(&self.pool, task_id, "paused").await?;
                    }
                    Outcome::Cancelled => {
                        store::set_task_state(&self.pool, task_id, "cancelled").await?;
                    }
                    Outcome::Failed(error) => {
                        let msg = format!("{error:#}");
                        tracing::warn!(job_id, task_id, error = %msg, "transfer task failed");
                        store::task_failed(&self.pool, task_id, &msg, self.max_retries).await?;
                    }
                }
                let workers_idle = self
                    .jobs
                    .get(&job_id)
                    .is_none_or(|rt| rt.active_tasks == 0);
                if workers_idle && self.pending_retries.remove(&job_id) {
                    self.retry_now(job_id).await?;
                } else {
                    self.maybe_finalize(job_id).await?;
                }
            }
        }
        Ok(())
    }

    /// Fresh control flags and providers for the re-run. Only safe once the
    /// job has no in-flight workers.
    async fn retry_now(&mut self, job_id: i64) -> Result<()> {
        self.jobs.remove(&job_id);
        self.speeds.remove(&job_id);
        store::retry_job(&self.pool, job_id).await
    }

    async fn maybe_finalize(&mut self, job_id: i64) -> Result<()> {
        let workers_active = self
            .jobs
            .get(&job_id)
            .is_some_and(|rt| rt.active_tasks > 0 || rt.preparing);
        if workers_active {
            return Ok(());
        }
        let providers = self.jobs.get(&job_id).and_then(|rt| rt.providers.clone());
        if store::try_finalize_job(&self.pool, job_id).await? {
            self.jobs.remove(&job_id);
            self.speeds.remove(&job_id);
            // Move semantics: transferred files were deleted per task; once
            // the whole job is done, sweep the emptied source directories.
            if let Some(providers) = providers.filter(|p| p.move_source) {
                if let Some((JobState::Done, src)) =
                    store::job_final(&self.pool, job_id).await?
                {
                    if src.ends_with('/') {
                        tokio::spawn(cleanup_moved_source(providers.src, src));
                    }
                }
            }
        }
        Ok(())
    }

    async fn publish(&mut self) {
        let mut jobs = match store::snapshot_jobs(&self.pool).await {
            Ok(jobs) => jobs,
            Err(error) => {
                tracing::error!(%error, "queue snapshot failed");
                return;
            }
        };

        let now = Instant::now();
        let mut total_bps = 0u64;
        for job in &mut jobs {
            if job.state != JobState::Active {
                self.speeds.remove(&job.id);
                continue;
            }
            let speed = self.speeds.entry(job.id).or_insert(Speed {
                last_bytes: job.bytes_done,
                last_at: now,
                ema_bps: 0.0,
            });
            let dt = now.duration_since(speed.last_at).as_secs_f64();
            if dt >= 0.4 {
                let delta = job.bytes_done.saturating_sub(speed.last_bytes) as f64;
                speed.ema_bps = 0.6 * speed.ema_bps + 0.4 * (delta / dt);
                speed.last_bytes = job.bytes_done;
                speed.last_at = now;
            }
            job.bps = speed.ema_bps as u64;
            total_bps += job.bps;
        }

        let _ = self.snap_tx.send(QueueSnapshot {
            jobs,
            total_bps,
            max_parallel: self.max_parallel,
            bandwidth_limit: self.bandwidth_limit,
        });
    }
}

/// Connect both providers (keychain access is blocking → spawn_blocking) and,
/// first time through, fan a directory source out into per-file tasks.
async fn prepare_job(
    pool: &SqlitePool,
    job: &JobRow,
    defaults: &LayerConfig,
    skip_hidden: bool,
) -> Result<JobProviders> {
    let src_profile = store::get_site(pool, job.src_site_id).await?;
    let dst_profile = store::get_site(pool, job.dst_site_id).await?;
    let defaults = defaults.clone();
    let same_site = job.src_site_id == job.dst_site_id;
    let move_source = job.move_source;
    let providers = tokio::task::spawn_blocking(move || -> Result<JobProviders> {
        Ok(JobProviders {
            src: Provider::connect(&src_profile, &defaults)
                .with_context(|| format!("connecting `{}`", src_profile.name))?,
            dst: Provider::connect(&dst_profile, &defaults)
                .with_context(|| format!("connecting `{}`", dst_profile.name))?,
            same_site,
            move_source,
        })
    })
    .await
    .context("provider connect task panicked")??;

    if !job.expanded {
        // A previously interrupted fan-out may have left partial tasks —
        // start clean, then stream batches in and seal the expansion.
        store::begin_expansion(pool, job.id).await?;
        let count = expand_job(pool, &providers, job, skip_hidden).await?;
        anyhow::ensure!(count > 0, "nothing to transfer under `{}`", job.src);
        store::finish_expansion(pool, job.id).await?;
    } else {
        store::activate_job(pool, job.id).await?;
    }
    Ok(providers)
}

/// Turn the job's source into per-file tasks. A file maps 1:1; a directory
/// is listed recursively *as a stream* — tasks are inserted in batches while
/// the listing runs, so arbitrarily large trees never sit in memory.
/// Subdirectories are pre-created at the destination (so empty dirs
/// survive); symlinks and other non-regular entries are left behind.
/// Returns the number of file tasks created.
async fn expand_job(
    pool: &SqlitePool,
    providers: &JobProviders,
    job: &JobRow,
    skip_hidden: bool,
) -> Result<u64> {
    use futures::TryStreamExt as _;

    let (src_op, src_rel) = providers.src.op_for(&job.src)?;
    let name = job
        .dst_name
        .clone()
        .unwrap_or_else(|| file_name(&job.src).to_string());

    if !job.src.ends_with('/') {
        let meta = src_op
            .stat(&src_rel)
            .await
            .with_context(|| format!("stat `{}`", job.src))?;
        if !meta.is_dir() {
            let task = NewTask {
                src: job.src.clone(),
                dst: format!("{}{name}", job.dst),
                total_bytes: Some(meta.content_length()),
                src_etag: meta.etag().map(str::to_string),
            };
            store::insert_tasks(pool, job.id, &[task]).await?;
            return Ok(1);
        }
    }

    let src_dir = if job.src.ends_with('/') {
        job.src.clone()
    } else {
        format!("{}/", job.src)
    };
    let (src_op, src_rel) = providers.src.op_for(&src_dir)?;
    let base = format!("{}{name}/", job.dst);

    let mut lister = src_op
        .lister_with(&src_rel)
        .recursive(true)
        .await
        .with_context(|| format!("listing `{src_dir}`"))?;

    let mut batch: Vec<NewTask> = Vec::with_capacity(EXPANSION_BATCH);
    let mut count = 0u64;
    while let Some(entry) = lister
        .try_next()
        .await
        .with_context(|| format!("listing `{src_dir}`"))?
    {
        let rel = entry.path().strip_prefix(&src_rel).unwrap_or(entry.path());
        if rel.is_empty() {
            continue;
        }
        if skip_hidden && rel.split('/').any(|segment| segment.starts_with('.')) {
            continue;
        }
        let meta = entry.metadata();
        if meta.is_dir() {
            let (dst_op, dst_rel) = providers.dst.op_for(&format!("{base}{rel}"))?;
            if let Err(error) = dst_op.create_dir(&dst_rel).await {
                tracing::debug!(%error, path = %rel, "create_dir skipped");
            }
        } else if meta.is_file() {
            batch.push(NewTask {
                src: format!("{src_dir}{rel}"),
                dst: format!("{base}{rel}"),
                total_bytes: Some(meta.content_length()),
                src_etag: meta.etag().map(str::to_string),
            });
            count += 1;
            if batch.len() >= EXPANSION_BATCH {
                store::insert_tasks(pool, job.id, &batch).await?;
                batch.clear();
            }
        } else {
            // Symlink / special file: transferring it is undefined across
            // backends — leave it behind rather than guessing.
            tracing::debug!(path = %rel, "expansion skipped non-regular entry");
        }
    }
    store::insert_tasks(pool, job.id, &batch).await?;
    Ok(count)
}

/// After a successful move job: skipped or failed files stay put, so only
/// sweep the source tree when nothing but directories remains under it.
async fn cleanup_moved_source(provider: Provider, src: String) {
    use futures::TryStreamExt as _;

    let result: Result<()> = async {
        let (op, rel) = provider.op_for(&src)?;
        let mut lister = op.lister_with(&rel).recursive(true).await?;
        while let Some(entry) = lister.try_next().await? {
            if !entry.metadata().is_dir() {
                tracing::debug!(%src, "move: files remain at the source; leaving it in place");
                return Ok(());
            }
        }
        op.delete_with(&rel).recursive(true).await?;
        Ok(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%src, %error, "cleaning up moved source failed");
    }
}

async fn run_worker(
    pool: SqlitePool,
    task: store::ClaimedTask,
    providers: JobProviders,
    control: Arc<JobControl>,
    opts: TransferOpts,
    tx: mpsc::UnboundedSender<Internal>,
) {
    let outcome =
        transfer::run_task(&pool, &providers.src, &providers.dst, &task, &control, opts).await;
    let _ = tx.send(Internal::TaskFinished {
        job_id: task.job_id,
        task_id: task.id,
        outcome,
    });
}

/// Last path segment of a file or directory browse path.
fn file_name(path: &str) -> &str {
    path.trim_end_matches('/').rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use sqlx::Row as _;

    use super::*;
    use crate::queue::{ConflictPolicy, JobState, NewJob, QueueHandle, QueueSnapshot};

    fn job(src: String, dst_dir: String, policy: ConflictPolicy) -> NewJob {
        NewJob {
            src_site_id: 0,
            dst_site_id: 0,
            src,
            dst_dir,
            dst_name: None,
            conflict_policy: policy,
            move_source: false,
        }
    }

    struct TestEnv {
        rt: tokio::runtime::Runtime,
        handle: QueueHandle,
        pool: SqlitePool,
        dir: PathBuf,
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Real engine over a temp-dir SQLite DB; both transfer ends are the
    /// migration-seeded local site 0.
    fn test_env(name: &str) -> TestEnv {
        test_env_with(name, |_| {})
    }

    fn test_env_with(name: &str, tweak: impl FnOnce(&mut TransferConfig)) -> TestEnv {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!(
            "kessel-engine-test-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let pool = rt.block_on(crate::db::connect(&dir)).unwrap();
        let mut cfg = TransferConfig {
            max_parallel: 2,
            bandwidth_limit: 0,
            retries: 0,
            verify: true,
            preserve_timestamps: true,
            skip_hidden: false,
        };
        tweak(&mut cfg);
        let handle = start(&rt, pool.clone(), cfg, LayerConfig::default());
        TestEnv {
            rt,
            handle,
            pool,
            dir,
        }
    }

    /// Absolute path → browse path under the local site (root "/").
    fn browse(path: &Path) -> String {
        path.to_str().unwrap().trim_start_matches('/').to_string()
    }

    fn wait_until(
        handle: &QueueHandle,
        pred: impl Fn(&QueueSnapshot) -> bool,
    ) -> QueueSnapshot {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let snap = handle.latest();
            if pred(&snap) {
                return snap;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "queue did not reach expected state; last snapshot: {snap:?}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn history_outcomes(env: &TestEnv) -> Vec<String> {
        env.rt
            .block_on(
                sqlx::query("SELECT outcome FROM transfer_history ORDER BY id")
                    .fetch_all(&env.pool),
            )
            .unwrap()
            .into_iter()
            .map(|row| row.get("outcome"))
            .collect()
    }

    #[test]
    fn copies_a_single_file_and_records_history() {
        let env = test_env("single-file");
        let src_dir = env.dir.join("src");
        let dst_dir = env.dir.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("hello.txt"), b"kessel run").unwrap();

        env.handle.enqueue(job(
            browse(&src_dir.join("hello.txt")),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Ask,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        let job = &snap.jobs[0];
        assert_eq!(job.state, JobState::Done, "error: {:?}", job.error);
        assert_eq!(job.files_done, 1);
        assert_eq!(
            std::fs::read(dst_dir.join("hello.txt")).unwrap(),
            b"kessel run"
        );
        assert_eq!(history_outcomes(&env), vec!["done"]);
    }

    #[test]
    fn fans_out_a_directory_recursively() {
        let env = test_env("dir-fanout");
        let src_dir = env.dir.join("tree");
        std::fs::create_dir_all(src_dir.join("nested/deep")).unwrap();
        std::fs::write(src_dir.join("a.txt"), b"a").unwrap();
        std::fs::write(src_dir.join("nested/b.txt"), b"bb").unwrap();
        std::fs::write(src_dir.join("nested/deep/c.txt"), b"ccc").unwrap();
        let dst_dir = env.dir.join("out");

        env.handle.enqueue(job(
            format!("{}/", browse(&src_dir)),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Ask,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        let job = &snap.jobs[0];
        assert_eq!(job.state, JobState::Done, "error: {:?}", job.error);
        assert_eq!(job.files_total, 3);
        assert_eq!(job.files_done, 3);
        assert_eq!(job.bytes_done, 6);
        let copied = dst_dir.join("tree");
        assert_eq!(std::fs::read(copied.join("a.txt")).unwrap(), b"a");
        assert_eq!(std::fs::read(copied.join("nested/b.txt")).unwrap(), b"bb");
        assert_eq!(
            std::fs::read(copied.join("nested/deep/c.txt")).unwrap(),
            b"ccc"
        );
    }

    #[test]
    fn missing_source_fails_the_job_with_an_error() {
        let env = test_env("missing-src");

        env.handle.enqueue(job(
            browse(&env.dir.join("does-not-exist.txt")),
            format!("{}/", browse(&env.dir.join("dst"))),
            ConflictPolicy::Ask,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        let job = &snap.jobs[0];
        assert_eq!(job.state, JobState::Failed);
        assert!(job.error.is_some());
        assert_eq!(history_outcomes(&env), vec!["failed"]);
    }

    #[test]
    fn cancel_reaches_a_terminal_state_and_retry_reruns() {
        let env = test_env("cancel-retry");
        let src_dir = env.dir.join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("f.bin"), vec![7u8; 4096]).unwrap();
        let dst_dir = env.dir.join("dst");

        // Overwrite policy: a cancelled attempt may leave a partial file at
        // the destination, and the retry must plow through it, not park.
        env.handle.enqueue(job(
            browse(&src_dir.join("f.bin")),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Overwrite,
        ));
        let snap = wait_until(&env.handle, |snap| !snap.jobs.is_empty());
        let job_id = snap.jobs[0].id;

        // A cancel is legal in any pre-terminal state; whether it lands
        // before or after the tiny transfer finishes, the job must settle.
        env.handle.cancel(job_id);
        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert!(matches!(
            snap.jobs[0].state,
            JobState::Cancelled | JobState::Done
        ));

        if snap.jobs[0].state == JobState::Cancelled {
            env.handle.retry(job_id);
            // Wait for Done specifically — the terminal `cancelled` state
            // stays visible in snapshots until the retry is processed.
            wait_until(&env.handle, |snap| {
                snap.jobs.first().is_some_and(|job| job.state == JobState::Done)
            });
        }
        assert_eq!(
            std::fs::read(dst_dir.join("f.bin")).unwrap(),
            vec![7u8; 4096]
        );
    }

    /// Common setup for conflict tests: `src/name` holds `new_content`,
    /// `dst/name` already holds "OLD".
    fn conflict_env(name: &str, new_content: &[u8]) -> (TestEnv, PathBuf, PathBuf) {
        let env = test_env(name);
        let src_dir = env.dir.join("src");
        let dst_dir = env.dir.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::create_dir_all(&dst_dir).unwrap();
        std::fs::write(src_dir.join("file.txt"), new_content).unwrap();
        std::fs::write(dst_dir.join("file.txt"), b"OLD").unwrap();
        (env, src_dir, dst_dir)
    }

    #[test]
    fn skip_policy_leaves_existing_destination_untouched() {
        let (env, src_dir, dst_dir) = conflict_env("skip", b"NEW");

        env.handle.enqueue(job(
            browse(&src_dir.join("file.txt")),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Skip,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done);
        assert_eq!(snap.jobs[0].files_skipped, 1);
        assert_eq!(snap.jobs[0].files_done, 0);
        assert_eq!(std::fs::read(dst_dir.join("file.txt")).unwrap(), b"OLD");
    }

    #[test]
    fn ask_policy_parks_until_resolved_then_applies_the_decision() {
        let (env, src_dir, dst_dir) = conflict_env("ask", b"NEW");

        env.handle.enqueue(job(
            browse(&src_dir.join("file.txt")),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Ask,
        ));

        // Parks instead of transferring; the job stays active.
        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.files_conflict == 1)
        });
        assert_eq!(snap.jobs[0].state, JobState::Active);
        assert_eq!(std::fs::read(dst_dir.join("file.txt")).unwrap(), b"OLD");

        env.handle.resolve(snap.jobs[0].id, ConflictPolicy::Overwrite);
        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done);
        assert_eq!(std::fs::read(dst_dir.join("file.txt")).unwrap(), b"NEW");
    }

    #[test]
    fn resolving_one_file_leaves_other_conflicts_parked() {
        let env = test_env("individual-conflict");
        let src = env.dir.join("tree");
        let dst = env.dir.join("out");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(dst.join("tree")).unwrap();
        for name in ["a.txt", "b.txt"] {
            std::fs::write(src.join(name), b"NEW").unwrap();
            std::fs::write(dst.join("tree").join(name), b"OLD").unwrap();
        }
        env.handle.enqueue(job(
            format!("{}/", browse(&src)),
            format!("{}/", browse(&dst)),
            ConflictPolicy::Ask,
        ));
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.files_conflict == 2)
        });
        let id = snap.jobs[0].id;
        let conflicts = env
            .rt
            .block_on(store::job_conflicts(&env.pool, id, 2))
            .unwrap();
        env.handle
            .resolve_task(conflicts[0].id, ConflictPolicy::Overwrite);
        let snap = wait_until(&env.handle, |s| {
            s.jobs
                .first()
                .is_some_and(|j| j.files_done == 1 && j.files_conflict == 1)
        });
        assert_eq!(snap.jobs[0].state, JobState::Active);
        let remaining = env
            .rt
            .block_on(store::job_conflicts(&env.pool, id, 2))
            .unwrap();
        assert_eq!(remaining[0].id, conflicts[1].id);
        env.handle.resolve(id, ConflictPolicy::Skip);
        wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state == JobState::Done)
        });
        assert_eq!(
            std::fs::read(dst.join("tree").join(file_name(&conflicts[0].dst))).unwrap(),
            b"NEW"
        );
        assert_eq!(
            std::fs::read(dst.join("tree").join(file_name(&conflicts[1].dst))).unwrap(),
            b"OLD"
        );
    }

    #[test]
    fn overwrite_newer_compares_modification_times() {
        for (name, source_time, expected) in [
            ("newer", 200, b"NEW"),
            ("older", 50, b"OLD"),
            ("equal", 100, b"OLD"),
        ] {
            let (env, src, dst) = conflict_env(name, b"NEW");
            filetime::set_file_mtime(
                src.join("file.txt"),
                filetime::FileTime::from_unix_time(source_time, 0),
            )
            .unwrap();
            filetime::set_file_mtime(
                dst.join("file.txt"),
                filetime::FileTime::from_unix_time(100, 0),
            )
            .unwrap();
            env.handle.enqueue(job(
                browse(&src.join("file.txt")),
                format!("{}/", browse(&dst)),
                ConflictPolicy::OverwriteNewer,
            ));
            let snap = wait_until(&env.handle, |s| {
                s.jobs.first().is_some_and(|j| j.state.is_terminal())
            });
            assert_eq!(snap.jobs[0].state, JobState::Done);
            assert_eq!(std::fs::read(dst.join("file.txt")).unwrap(), expected);
        }
    }

    #[test]
    fn keeps_both_unicode_filenames() {
        let env = test_env("unicode-rename");
        let src = env.dir.join("src");
        let dst = env.dir.join("dst");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        std::fs::write(src.join("日本.tar.gz"), b"NEW").unwrap();
        std::fs::write(dst.join("日本.tar.gz"), b"OLD").unwrap();
        env.handle.enqueue(job(
            browse(&src.join("日本.tar.gz")),
            format!("{}/", browse(&dst)),
            ConflictPolicy::Rename,
        ));
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done);
        assert_eq!(std::fs::read(dst.join("日本 (1).tar.gz")).unwrap(), b"NEW");
        assert_eq!(std::fs::read(dst.join("日本.tar.gz")).unwrap(), b"OLD");
    }

    #[test]
    fn rename_policy_keeps_both_files() {
        let (env, src_dir, dst_dir) = conflict_env("rename", b"NEW");

        env.handle.enqueue(job(
            browse(&src_dir.join("file.txt")),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Rename,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done, "error: {:?}", snap.jobs[0].error);
        assert_eq!(std::fs::read(dst_dir.join("file.txt")).unwrap(), b"OLD");
        assert_eq!(
            std::fs::read(dst_dir.join("file (1).txt")).unwrap(),
            b"NEW"
        );
    }

    #[test]
    fn move_job_deletes_the_emptied_source_tree() {
        let env = test_env("move");
        let src_dir = env.dir.join("movetree");
        std::fs::create_dir_all(src_dir.join("nested")).unwrap();
        std::fs::write(src_dir.join("a.txt"), b"a").unwrap();
        std::fs::write(src_dir.join("nested/b.txt"), b"bb").unwrap();
        let dst_dir = env.dir.join("moved");

        env.handle.enqueue(NewJob {
            src_site_id: 0,
            dst_site_id: 0,
            src: format!("{}/", browse(&src_dir)),
            dst_dir: format!("{}/", browse(&dst_dir)),
            dst_name: None,
            conflict_policy: ConflictPolicy::Overwrite,
            move_source: true,
        });

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done, "error: {:?}", snap.jobs[0].error);
        let copied = dst_dir.join("movetree");
        assert_eq!(std::fs::read(copied.join("a.txt")).unwrap(), b"a");
        assert_eq!(std::fs::read(copied.join("nested/b.txt")).unwrap(), b"bb");
        // Source files go immediately; the emptied tree is swept afterwards.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while src_dir.exists() {
            assert!(std::time::Instant::now() < deadline, "source tree was not cleaned up");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn skip_hidden_leaves_dotfiles_behind() {
        let env = test_env_with("skip-hidden", |cfg| cfg.skip_hidden = true);
        let src_dir = env.dir.join("tree");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("visible.txt"), b"v").unwrap();
        std::fs::write(src_dir.join(".hidden"), b"h").unwrap();
        let dst_dir = env.dir.join("out");

        env.handle.enqueue(job(
            format!("{}/", browse(&src_dir)),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Overwrite,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done, "error: {:?}", snap.jobs[0].error);
        assert_eq!(snap.jobs[0].files_total, 1);
        let copied = dst_dir.join("tree");
        assert!(copied.join("visible.txt").exists());
        assert!(!copied.join(".hidden").exists());
    }

    #[test]
    fn preserves_the_source_modified_time() {
        let env = test_env("mtime");
        let src_dir = env.dir.join("src");
        let dst_dir = env.dir.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src_file = src_dir.join("old.txt");
        std::fs::write(&src_file, b"aged like fine wine").unwrap();
        let past = filetime::FileTime::from_unix_time(1_600_000_000, 0);
        filetime::set_file_mtime(&src_file, past).unwrap();

        env.handle.enqueue(job(
            browse(&src_file),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Overwrite,
        ));

        let snap = wait_until(&env.handle, |snap| {
            snap.jobs.first().is_some_and(|job| job.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done, "error: {:?}", snap.jobs[0].error);
        let dst_meta = std::fs::metadata(dst_dir.join("old.txt")).unwrap();
        let dst_mtime = filetime::FileTime::from_last_modification_time(&dst_meta);
        assert_eq!(dst_mtime.unix_seconds(), 1_600_000_000);
    }

    #[test]
    fn graceful_shutdown_acks_and_requeues_for_next_launch() {
        let env = test_env("shutdown");
        let ack = env.handle.shutdown();
        env.rt
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), ack).await })
            .expect("engine should ack shutdown promptly")
            .expect("ack channel should not drop");
    }
}
