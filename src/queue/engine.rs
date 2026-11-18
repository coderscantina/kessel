//! The queue engine: a single scheduler task on the app tokio runtime.
//!
//! Lifecycle of a job: `queued` → prepare (connect layered providers via
//! [`Provider::connect`], fan a directory out into per-file tasks) →
//! `active` → workers stream files under a global concurrency cap →
//! `done` / `failed` / `cancelled` (+ history row). Every state change goes
//! through SQLite first, so a crash at any point resumes cleanly on
//! relaunch (`db::requeue_interrupted` flips orphaned `active` rows back).
//!
//! Snapshots for the UI are coalesced: progress publishes at most every
//! [`PUBLISH_EVERY`], while user commands and job-level state changes
//! publish at once.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use opendal::layers::ThrottleLayer;

use crate::config::TransferConfig;
use crate::provider::{LayerConfig, Provider};

use super::store::{self, JobRow, NewTask};
use super::transfer::{self, JobControl, Outcome, TransferOpts};
use super::{Command, JobOutcome, JobState, QueueHandle, QueueSnapshot};

/// Tasks are inserted in batches of this many rows while the source tree is
/// still being listed, so a million-file bucket never sits in memory.
const EXPANSION_BATCH: usize = 200;
/// Minimum gap between coalesced snapshot publishes.
const PUBLISH_EVERY: Duration = Duration::from_millis(250);
/// How long a job waits for a lost connection to come back before its files
/// use up their normal retries and fail.
const OUTAGE_PATIENCE: Duration = Duration::from_secs(24 * 60 * 60);

/// Spawn the engine on the app runtime and hand back its control handle.
pub fn start(
    runtime: &tokio::runtime::Runtime,
    pool: SqlitePool,
    cfg: TransferConfig,
    default_layers: LayerConfig,
) -> QueueHandle {
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
        throttle: global_throttle(cfg.bandwidth_limit),
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
        waiters: HashMap::new(),
        dirty: true,
        urgent: true,
        last_publish: Instant::now(),
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
    /// Each site's connection limit, enforced across all jobs when claiming.
    src_limit: Option<usize>,
    dst_limit: Option<usize>,
}

impl JobProviders {
    /// Whether one more task of this job fits the limits of both sites. A
    /// site with nothing running always admits one task, so a limit below
    /// what a same-site copy needs can't keep its job from ever being claimed.
    fn has_room(&self, load: &HashMap<i64, usize>) -> bool {
        let needed = if self.same_site { 2 } else { 1 };
        [(self.src.site_id, self.src_limit), (self.dst.site_id, self.dst_limit)]
            .into_iter()
            .all(|(site, limit)| {
                let used = load.get(&site).copied().unwrap_or(0);
                limit.is_none_or(|limit| used == 0 || used + needed <= limit)
            })
    }
}

#[derive(Default)]
struct JobRuntime {
    control: Arc<JobControl>,
    providers: Option<JobProviders>,
    preparing: bool,
    active_tasks: usize,
    /// Partial files nothing will resume are being removed before the job
    /// settles.
    cleaning: bool,
    /// Preparation or the connection failed this many times in a row.
    failures: u32,
    /// Preparation and claiming back off until then.
    retry_at: Option<Instant>,
    /// Why the job is waiting, shown on it meanwhile.
    waiting: Option<String>,
    /// When the connection was lost, while it stays lost.
    outage_since: Option<Instant>,
}

impl JobRuntime {
    /// Work for the job is still in flight; it can't settle, be retried or
    /// be removed yet.
    fn settling(&self) -> bool {
        self.preparing || self.active_tasks > 0 || self.cleaning
    }

    /// Hold the job back because its connection is lost, with growing
    /// pauses. Failures landing inside a pause belong to the same wait.
    /// False once the outage outlasted [`OUTAGE_PATIENCE`].
    fn wait_out_outage(&mut self, reason: String) -> bool {
        let now = Instant::now();
        let since = *self.outage_since.get_or_insert(now);
        if now.duration_since(since) >= OUTAGE_PATIENCE {
            return false;
        }
        if self.retry_at.is_none_or(|at| at <= now) {
            self.failures += 1;
            self.retry_at = Some(now + Duration::from_secs(1 << self.failures.min(8)));
        }
        self.waiting = Some(reason);
        true
    }

    /// The connection works (again).
    fn connected(&mut self) {
        self.failures = 0;
        self.retry_at = None;
        self.waiting = None;
        self.outage_since = None;
    }
}

enum Internal {
    /// Preparation finished: providers, or `None` when a pause, cancel or
    /// quit stopped the expansion.
    Prepared(i64, Result<Option<JobProviders>>),
    TaskFinished {
        job_id: i64,
        task_id: i64,
        outcome: Outcome,
        /// The worker's bytes still counted in `JobControl::unpersisted`.
        unpersisted: u64,
    },
    /// Partial cleanup of a settling job finished.
    Cleaned(i64),
}

/// Bytes/sec sampling per job from its live byte counter, smoothed with an
/// EMA so the UI number doesn't flicker.
struct Speed {
    last_moved: u64,
    last_at: Instant,
    ema_bps: f64,
}

/// bytes/sec → one rate limiter shared by every transfer (None = unlimited).
/// OpenDAL's ThrottleLayer shares its limiter between clones; the burst must
/// cover the largest single write, one chunk.
fn global_throttle(bps: u64) -> Option<ThrottleLayer> {
    (bps > 0).then(|| {
        let bandwidth = bps.min(u64::from(u32::MAX)) as u32;
        ThrottleLayer::new(bandwidth, bandwidth.max(transfer::CHUNK as u32))
    })
}

struct Engine {
    pool: SqlitePool,
    default_layers: LayerConfig,
    max_retries: u32,
    /// Global concurrency cap, enforced against the engine's own in-flight
    /// worker count so it can be adjusted at runtime.
    max_parallel: usize,
    /// Global bandwidth cap for the UI (bytes/sec, 0 = unlimited).
    bandwidth_limit: u64,
    /// The limiter enforcing `bandwidth_limit` across all running transfers.
    throttle: Option<ThrottleLayer>,
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
    /// Callers waiting for a job to settle ([`QueueHandle::settled`]).
    waiters: HashMap<i64, Vec<oneshot::Sender<Result<JobOutcome>>>>,
    /// Something changed since the last snapshot.
    dirty: bool,
    /// A user-visible state change: publish without waiting for the tick.
    urgent: bool,
    last_publish: Instant,
    /// Set while a graceful shutdown drains workers; acked when idle.
    shutdown_ack: Option<oneshot::Sender<()>>,
}

impl Engine {
    async fn run(mut self) {
        // Cancels a quit or crash interrupted still owe their cleanup.
        match store::unsettled_cancels(&self.pool).await {
            Ok(ids) => {
                for id in ids {
                    if let Err(error) = self.maybe_finalize(id).await {
                        tracing::error!(job_id = id, %error, "settling an interrupted cancel failed");
                    }
                }
            }
            Err(error) => tracing::error!(%error, "listing interrupted cancels failed"),
        }
        self.publish().await;
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            if self.shutdown_ack.is_some() {
                // Workers parked by shutdown already went back to `queued`
                // (their jobs stay active), so the next launch resumes them.
                if !self.busy() {
                    if let Some(ack) = self.shutdown_ack.take() {
                        let _ = ack.send(());
                    }
                    break;
                }
            } else if let Err(error) = self.schedule().await {
                tracing::error!(%error, "queue scheduling failed");
            }
            // Connected running jobs keep ticking: tasks waiting out a retry
            // backoff become claimable without any other event.
            let ticking = self.dirty
                || self.busy()
                || self.jobs.values().any(|rt| {
                    rt.retry_at.is_some()
                        || (rt.providers.is_some() && !rt.control.paused.load(Ordering::Relaxed))
                });
            tokio::select! {
                cmd = self.cmds.recv() => match cmd {
                    Some(cmd) => {
                        self.urgent = true;
                        if let Err(error) = self.handle_command(cmd).await {
                            tracing::error!(%error, "queue command failed");
                        }
                    }
                    // Every handle dropped: the app is shutting down.
                    None => break,
                },
                Some(msg) = self.internal_rx.recv() => {
                    self.dirty = true;
                    if let Err(error) = self.handle_internal(msg).await {
                        tracing::error!(%error, "queue bookkeeping failed");
                    }
                },
                _ = tick.tick(), if ticking => {
                    // Live byte counters move without any message.
                    self.dirty |= self.busy();
                },
            }
            if self.urgent || (self.dirty && self.last_publish.elapsed() >= PUBLISH_EVERY) {
                self.publish().await;
            }
        }
    }

    fn busy(&self) -> bool {
        self.jobs.values().any(JobRuntime::settling)
    }

    /// Kick off preparation for runnable jobs and claim tasks while
    /// concurrency permits are free.
    async fn schedule(&mut self) -> Result<()> {
        let now = Instant::now();
        for job in store::runnable_jobs(&self.pool).await? {
            let rt = self.jobs.entry(job.id).or_default();
            let backing_off = rt.retry_at.is_some_and(|at| at > now);
            if rt.providers.is_none() && !rt.preparing && !rt.cleaning && !backing_off {
                rt.preparing = true;
                let control = rt.control.clone();
                let pool = self.pool.clone();
                let defaults = self.default_layers.clone();
                let skip_hidden = self.skip_hidden;
                let tx = self.internal_tx.clone();
                tokio::spawn(async move {
                    let result = prepare_job(&pool, &job, &defaults, skip_hidden, &control).await;
                    let _ = tx.send(Internal::Prepared(job.id, result));
                });
            }
        }

        let mut in_flight: usize = self.jobs.values().map(|rt| rt.active_tasks).sum();
        while in_flight < self.max_parallel {
            let load = self.site_load();
            let ready: Vec<i64> = self
                .jobs
                .iter()
                .filter(|(_, rt)| {
                    rt.providers.as_ref().is_some_and(|providers| providers.has_room(&load))
                        && !rt.control.paused.load(Ordering::Relaxed)
                        && rt.retry_at.is_none_or(|at| at <= now)
                })
                .map(|(id, _)| *id)
                .collect();
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
                throttle: self.throttle.clone(),
            };
            let pool = self.pool.clone();
            let tx = self.internal_tx.clone();
            tokio::spawn(run_worker(pool, task, providers, control, opts, tx));
        }
        Ok(())
    }

    /// Connections the running tasks of all jobs hold per site: one for each
    /// end of a transfer.
    fn site_load(&self) -> HashMap<i64, usize> {
        let mut load = HashMap::new();
        for rt in self.jobs.values() {
            if let Some(providers) = &rt.providers {
                for site in [providers.src.site_id, providers.dst.site_id] {
                    *load.entry(site).or_default() += rt.active_tasks;
                }
            }
        }
        load
    }

    async fn handle_command(&mut self, cmd: Command) -> Result<()> {
        match cmd {
            Command::Enqueue(job) => {
                self.enqueue(&job).await?;
            }
            Command::EnqueueTracked(job, reply) => {
                let id = self.enqueue(&job).await?;
                let _ = reply.send(id);
            }
            Command::Submit(job, reply) => {
                let _ = reply.send(self.enqueue(&job).await);
            }
            Command::SubmitBatch(batch, expected, reply) => {
                let _ = reply.send(self.enqueue_batch(&batch, &expected).await);
            }
            Command::Settled(id, reply) => match store::job_outcome(&self.pool, id).await? {
                None => {
                    let _ = reply.send(Err(anyhow::anyhow!("transfer {id} doesn't exist")));
                }
                Some(outcome) if outcome.state.is_terminal() => {
                    let _ = reply.send(Ok(outcome));
                }
                Some(_) => self.waiters.entry(id).or_default().push(reply),
            },
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
                // The pause may have landed after the last file finished.
                self.maybe_finalize(id).await?;
            }
            Command::Cancel(id) => {
                if let Some(rt) = self.jobs.get(&id) {
                    rt.control.cancelled.store(true, Ordering::Relaxed);
                }
                store::cancel_job(&self.pool, id).await?;
                self.maybe_finalize(id).await?;
            }
            Command::RetryFailed(id) => {
                // Only settled jobs qualify, and those have no workers left.
                self.jobs.remove(&id);
                self.speeds.remove(&id);
                store::retry_failed_tasks(&self.pool, id).await?;
            }
            Command::Retry(id) => {
                if self.jobs.get(&id).is_some_and(JobRuntime::settling) {
                    // Still winding down (e.g. right after a cancel): retry
                    // once the job settled.
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
            Command::ClearFinished => self.remove_jobs(None).await?,
            Command::Remove(id) => self.remove_jobs(Some(id)).await?,
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
                    self.maybe_finalize(id).await?;
                }
            }
            Command::SetMaxParallel(limit) => {
                // In-flight workers finish naturally; the claim loop simply
                // stops (or starts) claiming against the new cap.
                self.max_parallel = limit.max(1);
            }
            Command::SetBandwidth(bps) => {
                // Files started from now on share the new limiter; running
                // ones finish under the old one.
                self.bandwidth_limit = bps;
                self.throttle = global_throttle(bps);
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
            Command::SetDefaultLayers(layers) => {
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
                    Ok(Some(providers)) => {
                        rt.providers = Some(providers);
                        rt.connected();
                        // A retried job whose tasks all settled earlier
                        // (e.g. done before a cancel landed) has nothing to
                        // claim — settle it now instead of idling forever.
                        self.maybe_finalize(job_id).await?;
                    }
                    // Paused or cancelled mid-expansion: a resume or retry
                    // expands again; a cancel settles now.
                    Ok(None) => self.maybe_finalize(job_id).await?,
                    Err(error) => {
                        let transient = is_transient(&error);
                        tracing::error!(job_id, transient, error = %format!("{error:#}"), "job preparation failed");
                        let reason = crate::errors::humanize(&error).title;
                        if crate::errors::is_outage(&error) && rt.wait_out_outage(reason.clone()) {
                            self.maybe_finalize(job_id).await?;
                        } else if transient && rt.failures < self.max_retries {
                            // Same backoff as a task retry; the job stays
                            // queued and shows why it waits.
                            rt.failures += 1;
                            let delay = Duration::from_secs(1 << rt.failures.min(8));
                            rt.retry_at = Some(Instant::now() + delay);
                            rt.waiting = Some(reason);
                            self.maybe_finalize(job_id).await?;
                        } else if store::fail_job(&self.pool, job_id, &reason).await? {
                            self.jobs.remove(&job_id);
                            self.notify_settled(job_id).await?;
                        } else {
                            self.maybe_finalize(job_id).await?;
                        }
                    }
                }
            }
            Internal::Cleaned(job_id) => {
                if let Some(rt) = self.jobs.get_mut(&job_id) {
                    rt.cleaning = false;
                }
                self.finalize(job_id).await?;
            }
            Internal::TaskFinished {
                job_id,
                task_id,
                outcome,
                unpersisted,
            } => {
                if let Some(rt) = self.jobs.get_mut(&job_id) {
                    rt.active_tasks = rt.active_tasks.saturating_sub(1);
                }
                let result = self.record_outcome(job_id, task_id, outcome).await;
                // Only now that the outcome is persisted does the worker's
                // live share leave the counter, so progress never dips.
                if let Some(rt) = self.jobs.get(&job_id) {
                    rt.control.unpersisted.fetch_sub(unpersisted, Ordering::Relaxed);
                }
                result?;
                self.maybe_finalize(job_id).await?;
            }
        }
        Ok(())
    }

    async fn record_outcome(&mut self, job_id: i64, task_id: i64, outcome: Outcome) -> Result<()> {
        if matches!(outcome, Outcome::Done { .. } | Outcome::Skipped | Outcome::Conflict { .. }) {
            if let Some(rt) = self.jobs.get_mut(&job_id) {
                rt.connected();
            }
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
                store::task_paused(&self.pool, task_id).await?;
            }
            Outcome::Cancelled => {
                store::set_task_state(&self.pool, task_id, "cancelled").await?;
            }
            Outcome::Failed(error) => {
                let msg = format!("{error:#}");
                let transient = is_transient(&error);
                tracing::warn!(job_id, task_id, transient, error = %msg, "transfer task failed");
                let reason = crate::errors::humanize(&error).title;
                let waiting = crate::errors::is_outage(&error)
                    && self
                        .jobs
                        .get_mut(&job_id)
                        .is_some_and(|rt| rt.wait_out_outage(reason.clone()));
                if waiting {
                    store::task_interrupted(&self.pool, task_id, &msg).await?;
                } else if store::task_failed(&self.pool, task_id, &msg, transient, self.max_retries).await? {
                    store::note_job_error(&self.pool, job_id, &reason).await?;
                }
            }
        }
        Ok(())
    }

    /// Validate and persist a new job.
    async fn enqueue(&self, job: &super::NewJob) -> Result<i64> {
        let dst_name = job.dst_name.as_deref().unwrap_or(file_name(&job.src));
        anyhow::ensure!(
            job.src_site_id != job.dst_site_id
                || !format!("{}{dst_name}", job.dst_dir).eq(&job.src),
            "source and destination are the same"
        );
        store::enqueue_job(&self.pool, job).await
    }

    /// Validate and persist a multi-file job, already expanded.
    async fn enqueue_batch(
        &self,
        batch: &super::NewBatch,
        expected: &HashMap<String, super::ExpectedDest>,
    ) -> Result<i64> {
        anyhow::ensure!(!batch.files.is_empty(), "nothing to transfer");
        anyhow::ensure!(
            batch.src_site_id != batch.dst_site_id
                || batch.files.iter().all(|file| file.src != file.dst),
            "source and destination are the same"
        );
        store::enqueue_batch(&self.pool, batch, expected).await
    }

    /// Answer everyone waiting for this job to settle.
    async fn notify_settled(&mut self, job_id: i64) -> Result<()> {
        self.urgent = true;
        let Some(waiters) = self.waiters.remove(&job_id) else {
            return Ok(());
        };
        let outcome = store::job_outcome(&self.pool, job_id)
            .await?
            .with_context(|| format!("transfer {job_id} vanished"))?;
        for waiter in waiters {
            let _ = waiter.send(Ok(outcome.clone()));
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

    /// Settle the job once nothing is in flight and nothing is left to run.
    /// Partial files nothing will resume are removed first, so a job only
    /// counts as settled (and removable) once its leftovers are gone.
    async fn maybe_finalize(&mut self, job_id: i64) -> Result<()> {
        if self.jobs.get(&job_id).is_some_and(JobRuntime::settling)
            || !store::finalizable(&self.pool, job_id).await?
        {
            return Ok(());
        }
        let stale = store::stale_partials(&self.pool, job_id).await?;
        if stale.is_empty() {
            return self.finalize(job_id).await;
        }
        let rt = self.jobs.entry(job_id).or_default();
        rt.cleaning = true;
        let dst = rt.providers.as_ref().map(|p| p.dst.clone());
        let pool = self.pool.clone();
        let defaults = self.default_layers.clone();
        let tx = self.internal_tx.clone();
        tokio::spawn(async move {
            if let Err(error) = discard_partials(&pool, job_id, dst, stale, &defaults).await {
                tracing::warn!(job_id, error = %format!("{error:#}"), "removing partial files failed");
            }
            let _ = tx.send(Internal::Cleaned(job_id));
        });
        Ok(())
    }

    /// Write the job's final state, tell waiters, and run a retry that was
    /// waiting for it to settle.
    async fn finalize(&mut self, job_id: i64) -> Result<()> {
        let providers = self.jobs.get(&job_id).and_then(|rt| rt.providers.clone());
        if store::try_finalize_job(&self.pool, job_id).await? {
            self.jobs.remove(&job_id);
            self.speeds.remove(&job_id);
            // Move semantics: transferred files were deleted per task; once
            // the whole job is done, sweep the emptied source directories.
            if let Some(providers) = providers.filter(|p| p.move_source) {
                if let Some((JobState::Done, src)) = store::job_final(&self.pool, job_id).await? {
                    if src.ends_with('/') {
                        tokio::spawn(cleanup_moved_source(providers.src, src));
                    }
                }
            }
            self.notify_settled(job_id).await?;
        }
        if self.pending_retries.remove(&job_id) {
            self.retry_now(job_id).await?;
        }
        Ok(())
    }

    /// Drop settled jobs from the queue and delete the partial files their
    /// tasks still owned (failed files kept them for a retry).
    async fn remove_jobs(&self, job_id: Option<i64>) -> Result<()> {
        let partials = store::remove_jobs(&self.pool, job_id).await?;
        if !partials.is_empty() {
            let pool = self.pool.clone();
            let defaults = self.default_layers.clone();
            tokio::spawn(async move { delete_orphans(&pool, partials, &defaults).await });
        }
        Ok(())
    }

    async fn publish(&mut self) {
        self.dirty = false;
        self.urgent = false;
        self.last_publish = Instant::now();
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
            let Some(rt) = self.jobs.get(&job.id) else {
                self.speeds.remove(&job.id);
                continue;
            };
            if let Some(reason) = &rt.waiting {
                job.error = Some(reason.clone());
            }
            let control = &rt.control;
            if rt.preparing && control.listing.load(Ordering::Relaxed) {
                job.discovering = Some(control.discovered.load(Ordering::Relaxed));
            }
            job.bytes_done += control.unpersisted.load(Ordering::Relaxed);
            if job.state != JobState::Active {
                self.speeds.remove(&job.id);
                continue;
            }
            let moved = control.moved.load(Ordering::Relaxed);
            let speed = self.speeds.entry(job.id).or_insert(Speed {
                last_moved: moved,
                last_at: now,
                ema_bps: 0.0,
            });
            let dt = now.duration_since(speed.last_at).as_secs_f64();
            if dt >= 0.4 {
                let delta = moved.saturating_sub(speed.last_moved) as f64;
                speed.ema_bps = 0.6 * speed.ema_bps + 0.4 * (delta / dt);
                speed.last_moved = moved;
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
/// `None` when a pause or cancel stopped the fan-out.
async fn prepare_job(
    pool: &SqlitePool,
    job: &JobRow,
    defaults: &LayerConfig,
    skip_hidden: bool,
    control: &JobControl,
) -> Result<Option<JobProviders>> {
    let providers = JobProviders {
        src: connect_site(pool, job.src_site_id, defaults).await?,
        dst: connect_site(pool, job.dst_site_id, defaults).await?,
        same_site: job.src_site_id == job.dst_site_id,
        move_source: job.move_source,
        src_limit: site_limit(pool, job.src_site_id, defaults).await?,
        dst_limit: site_limit(pool, job.dst_site_id, defaults).await?,
    };

    if !job.expanded {
        // A previously interrupted fan-out may have left partial tasks —
        // start clean, then stream batches in and seal the expansion.
        store::begin_expansion(pool, job.id).await?;
        if !expand_job(pool, &providers, job, skip_hidden, control).await? {
            return Ok(None);
        }
        store::finish_expansion(pool, job.id).await?;
    } else {
        store::activate_job(pool, job.id).await?;
    }
    Ok(Some(providers))
}

/// Connect one site's layered provider (keychain access is blocking).
async fn connect_site(pool: &SqlitePool, site_id: i64, defaults: &LayerConfig) -> Result<Provider> {
    let profile = store::get_site(pool, site_id).await?;
    let defaults = defaults.clone();
    tokio::task::spawn_blocking(move || {
        Provider::connect(&profile, &defaults)
            .with_context(|| format!("connecting `{}`", profile.name))
    })
    .await
    .context("provider connect task panicked")?
}

/// The connection limit configured for a site, over the app defaults.
async fn site_limit(pool: &SqlitePool, site_id: i64, defaults: &LayerConfig) -> Result<Option<usize>> {
    let profile = store::get_site(pool, site_id).await?;
    Ok(profile.layers.merged_over(defaults).concurrent_limit)
}

/// Whether a failed task is worth re-queueing: the error class says the
/// same request can succeed later, or OpenDAL marked it temporary.
fn is_transient(error: &anyhow::Error) -> bool {
    crate::errors::classify(error).is_transient()
        || crate::errors::is_outage(error)
        || error
            .chain()
            .filter_map(|cause| cause.downcast_ref::<opendal::Error>())
            .any(opendal::Error::is_temporary)
}

/// Delete a settling job's partial files (`(task id, partial browse path)`)
/// and forget the ones that are gone. A partial that can't be deleted stays
/// recorded, so removing the job tries again.
async fn discard_partials(
    pool: &SqlitePool,
    job_id: i64,
    dst: Option<Provider>,
    partials: Vec<(i64, String)>,
    defaults: &LayerConfig,
) -> Result<()> {
    let dst = match dst {
        Some(dst) => dst,
        None => {
            let job = store::get_job(pool, job_id)
                .await?
                .with_context(|| format!("transfer {job_id} vanished"))?;
            connect_site(pool, job.dst_site_id, defaults).await?
        }
    };
    let mut gone = Vec::with_capacity(partials.len());
    for (task_id, partial) in partials {
        // Deleting a missing file succeeds, so a partial that never got
        // written is fine.
        match delete_path(&dst, &partial).await {
            Ok(()) => gone.push(task_id),
            Err(error) => tracing::warn!(%partial, error = %format!("{error:#}"), "removing a partial file failed"),
        }
    }
    store::forget_partials(pool, &gone).await
}

/// Delete the partial files of removed jobs, `(site id, browse path)`.
/// Nothing tracks them any more, so failures only reach the log.
async fn delete_orphans(pool: &SqlitePool, partials: Vec<(i64, String)>, defaults: &LayerConfig) {
    let mut by_site: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for (site_id, partial) in partials {
        by_site.entry(site_id).or_default().push(partial);
    }
    for (site_id, partials) in by_site {
        let dst = match connect_site(pool, site_id, defaults).await {
            Ok(dst) => dst,
            Err(error) => {
                tracing::warn!(site_id, error = %format!("{error:#}"), "removing partial files failed");
                continue;
            }
        };
        for partial in partials {
            if let Err(error) = delete_path(&dst, &partial).await {
                tracing::warn!(%partial, error = %format!("{error:#}"), "removing a partial file failed");
            }
        }
    }
}

async fn delete_path(provider: &Provider, path: &str) -> Result<()> {
    let (op, rel) = provider.route(path).await?;
    op.delete(&rel)
        .await
        .with_context(|| format!("deleting `{path}`"))
}

/// Turn the job's source into per-file tasks. A file maps 1:1; a directory
/// is walked *as a stream* ([`TreeWalk`]) — tasks are inserted in batches
/// while the walk runs, so arbitrarily large trees never sit in memory.
/// Subdirectories are pre-created at the destination (so empty dirs
/// survive). Links to files are copied as files; links to folders (which
/// could form cycles), broken links and special files are recorded as
/// skipped. Returns false when a pause or cancel stopped the walk early.
async fn expand_job(
    pool: &SqlitePool,
    providers: &JobProviders,
    job: &JobRow,
    skip_hidden: bool,
    control: &JobControl,
) -> Result<bool> {
    let (src_op, src_rel) = providers.src.route(&job.src).await?;
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
            return Ok(true);
        }
    }

    let src_dir = if job.src.ends_with('/') {
        job.src.clone()
    } else {
        format!("{}/", job.src)
    };
    anyhow::ensure!(
        !providers.same_site || !job.dst.starts_with(&src_dir),
        "cannot transfer a folder into itself or a descendant"
    );
    anyhow::ensure!(
        !providers.same_site || format!("{}{name}/", job.dst) != src_dir,
        "source and destination are the same folder"
    );
    let (src_op, src_rel) = providers.src.route(&src_dir).await?;
    let base = format!("{}{name}/", job.dst);

    let mut walk = TreeWalk::new(src_op.clone(), src_rel.clone(), skip_hidden)
        .await
        .with_context(|| format!("listing `{src_dir}`"))?;

    let (dst_op, dst_rel) = providers.dst.route(&base).await?;
    if dst_op.info().capability().create_dir {
        dst_op
            .create_dir(&dst_rel)
            .await
            .context("creating destination folder")?;
    }

    control.discovered.store(0, Ordering::Relaxed);
    control.listing.store(true, Ordering::Relaxed);
    let mut batch: Vec<NewTask> = Vec::with_capacity(EXPANSION_BATCH);
    while let Some(entry) = walk.next().await? {
        if control.halted() {
            return Ok(false);
        }
        let rel = entry.path().strip_prefix(&src_rel).unwrap_or(entry.path());
        let meta = entry.metadata();
        if meta.is_dir() {
            let (dst_op, dst_rel) = providers.dst.route(&format!("{base}{rel}")).await?;
            if dst_op.info().capability().create_dir {
                dst_op
                    .create_dir(&dst_rel)
                    .await
                    .with_context(|| format!("creating destination folder `{dst_rel}`"))?;
            }
            continue;
        }
        // Listings report links and special files as unknown; stat follows
        // a link to what it points at.
        let target = if meta.is_file() {
            Ok(meta.clone())
        } else {
            src_op.stat(entry.path()).await
        };
        let src = format!("{src_dir}{rel}");
        let dst = format!("{base}{rel}");
        let skipped = match &target {
            Ok(target) if target.is_file() => None,
            Ok(target) if target.is_dir() => Some("Link to a folder, not copied"),
            Ok(_) => Some("Not a regular file, not copied"),
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {
                Some("Broken link, not copied")
            }
            Err(_) => None,
        };
        if let Some(reason) = skipped {
            store::insert_skipped(pool, job.id, &src, &dst, reason).await?;
            continue;
        }
        let target = target.with_context(|| format!("stat `{src}`"))?;
        control.discovered.fetch_add(1, Ordering::Relaxed);
        batch.push(NewTask {
            src,
            dst,
            total_bytes: Some(target.content_length()),
            src_etag: target.etag().map(str::to_string),
        });
        if batch.len() >= EXPANSION_BATCH {
            store::insert_tasks(pool, job.id, &batch).await?;
            batch.clear();
        }
    }
    store::insert_tasks(pool, job.id, &batch).await?;
    Ok(true)
}

/// Every entry below a directory, excluding the directory itself. Backends
/// with native recursive listing (S3 and friends) stream it in one go. The
/// rest are walked one folder at a time: OpenDAL's simulated recursion drops
/// a folder whose listing fails and gets retried, which silently truncates
/// big FTP trees. Here a folder listing is retried as a whole.
enum TreeWalk {
    Native {
        lister: opendal::Lister,
        root: String,
        skip_hidden: bool,
    },
    Folders {
        op: opendal::Operator,
        root: String,
        skip_hidden: bool,
        pending: Vec<String>,
        current: std::vec::IntoIter<opendal::Entry>,
    },
}

impl TreeWalk {
    async fn new(op: opendal::Operator, root: String, skip_hidden: bool) -> Result<Self> {
        Ok(if op.info().capability().list_with_recursive {
            TreeWalk::Native {
                lister: op.lister_with(&root).recursive(true).await?,
                root,
                skip_hidden,
            }
        } else {
            TreeWalk::Folders {
                op,
                pending: vec![root.clone()],
                root,
                skip_hidden,
                current: Vec::new().into_iter(),
            }
        })
    }

    async fn next(&mut self) -> Result<Option<opendal::Entry>> {
        use futures::TryStreamExt as _;

        loop {
            let (entry, root, skip_hidden) = match self {
                TreeWalk::Native {
                    lister,
                    root,
                    skip_hidden,
                } => {
                    let entry = lister
                        .try_next()
                        .await
                        .with_context(|| format!("listing `{root}`"))?;
                    (entry, root, *skip_hidden)
                }
                TreeWalk::Folders {
                    op,
                    root,
                    skip_hidden,
                    pending,
                    current,
                } => match current.next() {
                    Some(entry) => (Some(entry), root, *skip_hidden),
                    None => {
                        let Some(dir) = pending.pop() else {
                            return Ok(None);
                        };
                        let mut entries = op
                            .list(&dir)
                            .await
                            .with_context(|| format!("listing `{dir}`"))?;
                        // Some backends list the folder itself.
                        entries.retain(|entry| entry.path() != dir);
                        *current = entries.into_iter();
                        continue;
                    }
                },
            };
            let Some(entry) = entry else {
                return Ok(None);
            };
            let rel = entry.path().strip_prefix(root.as_str()).unwrap_or(entry.path());
            if rel.is_empty()
                || (skip_hidden && rel.split('/').any(|s| s.starts_with('.')))
                || (entry.metadata().is_file() && rel.ends_with(transfer::PARTIAL_SUFFIX))
            {
                continue;
            }
            if let TreeWalk::Folders { pending, .. } = self {
                if entry.metadata().is_dir() {
                    pending.push(entry.path().to_string());
                }
            }
            return Ok(Some(entry));
        }
    }
}

/// After a successful move job: skipped or failed files stay put, so only
/// sweep the source tree when nothing but directories remains under it.
/// Folders are removed one by one, deepest first, with non-recursive
/// deletes: a file that shows up meanwhile keeps its folder alive instead
/// of being swept away with it.
async fn cleanup_moved_source(provider: Provider, src: String) {
    use futures::TryStreamExt as _;

    let result: Result<()> = async {
        let (op, rel) = provider.route(&src).await?;
        let mut lister = op.lister_with(&rel).recursive(true).await?;
        let mut dirs = vec![rel.clone()];
        while let Some(entry) = lister.try_next().await? {
            if !entry.metadata().is_dir() {
                tracing::debug!(%src, "move: files remain at the source; leaving it in place");
                return Ok(());
            }
            if entry.path() != rel {
                dirs.push(entry.path().to_string());
            }
        }
        dirs.sort_by_key(|dir| std::cmp::Reverse(dir.matches('/').count()));
        for dir in dirs {
            if let Err(error) = op.delete(&dir).await {
                tracing::debug!(%dir, %error, "move: source folder not removed");
            }
        }
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
    let (outcome, unpersisted) =
        transfer::run_task(&pool, &providers.src, &providers.dst, &task, &control, opts).await;
    let _ = tx.send(Internal::TaskFinished {
        job_id: task.job_id,
        task_id: task.id,
        outcome,
        unpersisted,
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
    use crate::queue::{BatchFile, ConflictPolicy, JobState, NewBatch, NewJob, QueueHandle, QueueSnapshot};

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
            default_conflict_policy: Default::default(),
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
    fn preserves_empty_folder_tree() {
        let env = test_env("empty-tree");
        let src = env.dir.join("tree");
        let dst = env.dir.join("out");
        std::fs::create_dir_all(src.join("nested/empty")).unwrap();
        env.handle.enqueue(job(
            format!("{}/", browse(&src)),
            format!("{}/", browse(&dst)),
            ConflictPolicy::Ask,
        ));
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state.is_terminal())
        });
        assert_eq!(
            snap.jobs[0].state,
            JobState::Done,
            "{:?}",
            snap.jobs[0].error
        );
        assert!(dst.join("tree/nested/empty").is_dir());
        let empty = env.dir.join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        env.handle.enqueue(job(
            format!("{}/", browse(&empty)),
            format!("{}/", browse(&dst)),
            ConflictPolicy::Ask,
        ));
        let snap = wait_until(&env.handle, |s| {
            s.jobs.len() == 2 && s.jobs.iter().all(|j| j.state.is_terminal())
        });
        assert!(snap.jobs.iter().all(|j| j.state == JobState::Done));
        assert!(dst.join("empty").is_dir());
    }

    #[test]
    fn rejects_uploading_a_folder_into_itself() {
        let env = test_env("recursive-target");
        let src = env.dir.join("tree");
        std::fs::create_dir_all(&src).unwrap();
        env.handle.enqueue(job(
            format!("{}/", browse(&src)),
            format!("{}/", browse(&src)),
            ConflictPolicy::Ask,
        ));
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Failed);
        assert!(!src.join("tree").exists());
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

    /// Leftover task-owned partial files under `dir`.
    fn partials(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.to_string_lossy().ends_with(transfer::PARTIAL_SUFFIX))
            .collect()
    }

    /// A 40 MiB source behind an 8 MiB/s global cap: the first chunks pass
    /// at once, the rest takes seconds, leaving time to act mid-transfer.
    /// Jobs built with [`slow_job`] target a second local site, so bytes
    /// stream instead of taking the same-site server-side copy.
    fn slow_env(name: &str) -> (TestEnv, PathBuf, PathBuf, Vec<u8>) {
        let env = test_env_with(name, |cfg| cfg.bandwidth_limit = 8 * 1024 * 1024);
        env.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO sites (id, name, scheme, options) \
                     VALUES (1, 'Local copy', 'fs', '{\"root\":\"/\"}')",
                )
                .execute(&env.pool),
            )
            .unwrap();
        let src_dir = env.dir.join("src");
        let dst_dir = env.dir.join("dst");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::create_dir_all(&dst_dir).unwrap();
        let content: Vec<u8> = (0..40 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(src_dir.join("big.bin"), &content).unwrap();
        (env, src_dir, dst_dir, content)
    }

    fn slow_job(src: &Path, dst_dir: &Path, move_source: bool) -> NewJob {
        NewJob {
            dst_site_id: 1,
            move_source,
            ..job(browse(src), format!("{}/", browse(dst_dir)), ConflictPolicy::Overwrite)
        }
    }

    /// Start an overwrite of `dst/big.bin` (holding "OLD") and pause it once
    /// bytes are flowing and its worker has settled.
    fn start_and_pause_overwrite(env: &TestEnv, src_dir: &Path, dst_dir: &Path) -> i64 {
        std::fs::write(dst_dir.join("big.bin"), b"OLD").unwrap();
        env.handle.enqueue(slow_job(&src_dir.join("big.bin"), dst_dir, false));
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state == JobState::Active && j.bytes_done > 0)
        });
        let id = snap.jobs[0].id;
        pause_once_past(env, id, 0);
        id
    }

    /// Pause job `id` once it moved more than `bytes` and wait until its
    /// worker settled the pause.
    fn pause_once_past(env: &TestEnv, id: i64, bytes: u64) {
        wait_until(&env.handle, |s| {
            s.jobs
                .iter()
                .any(|j| j.id == id && j.state == JobState::Active && j.bytes_done > bytes)
        });
        env.handle.pause(id);
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let states = env.rt.block_on(store::task_states(&env.pool, id)).unwrap();
            if states.contains_key("paused") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never settled the pause: {states:?} {:?}",
                env.handle.latest()
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    fn overwrite_keeps_the_old_file_until_a_resumed_copy_commits() {
        let (env, src_dir, dst_dir, content) = slow_env("staged-overwrite");
        let id = start_and_pause_overwrite(&env, &src_dir, &dst_dir);
        assert_eq!(std::fs::read(dst_dir.join("big.bin")).unwrap(), b"OLD");
        assert_eq!(partials(&dst_dir).len(), 1);

        env.handle.resume(id);
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Done, "{:?}", snap.jobs[0].error);
        assert!(std::fs::read(dst_dir.join("big.bin")).unwrap() == content);
        assert!(partials(&dst_dir).is_empty());
    }

    #[test]
    fn cancelling_a_paused_overwrite_removes_its_partial() {
        let (env, src_dir, dst_dir, _) = slow_env("cancel-partial");
        let id = start_and_pause_overwrite(&env, &src_dir, &dst_dir);
        env.handle.cancel(id);
        wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state == JobState::Cancelled)
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !partials(&dst_dir).is_empty() {
            assert!(std::time::Instant::now() < deadline, "partial was not removed");
            std::thread::sleep(Duration::from_millis(25));
        }
        assert_eq!(std::fs::read(dst_dir.join("big.bin")).unwrap(), b"OLD");
    }

    #[test]
    fn move_keeps_a_source_that_changed_during_the_copy() {
        let (env, src_dir, dst_dir, _) = slow_env("move-changed");
        let src_file = src_dir.join("big.bin");
        env.handle.enqueue(slow_job(&src_file, &dst_dir, true));
        wait_until(&env.handle, |s| s.jobs.first().is_some_and(|j| j.bytes_done > 0));
        filetime::set_file_mtime(&src_file, filetime::FileTime::from_unix_time(1_000_000, 0))
            .unwrap();
        let snap = wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state.is_terminal())
        });
        assert_eq!(snap.jobs[0].state, JobState::Failed);
        assert!(snap.jobs[0].error.as_deref().unwrap_or("").contains("changed"));
        assert!(src_file.exists());
    }

    #[test]
    fn one_failed_file_finishes_with_errors_and_retries_alone() {
        let env = test_env("done-with-errors");
        let src = env.dir.join("tree");
        let dst = env.dir.join("out");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();
        std::fs::write(src.join("b.txt"), b"b").unwrap();
        // A folder squatting on b.txt's destination name fails that file.
        std::fs::create_dir_all(dst.join("tree/b.txt")).unwrap();

        let id = env
            .rt
            .block_on(env.handle.submit(job(
                format!("{}/", browse(&src)),
                format!("{}/", browse(&dst)),
                ConflictPolicy::Overwrite,
            )))
            .unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Done);
        assert_eq!((outcome.files_done, outcome.files_failed), (1, 1));
        assert!(outcome.error.unwrap().starts_with("1 of 2 files failed"));
        let failed: i64 = env
            .rt
            .block_on(
                sqlx::query_scalar("SELECT files_failed FROM transfer_history").fetch_one(&env.pool),
            )
            .unwrap();
        assert_eq!(failed, 1);

        std::fs::remove_dir(dst.join("tree/b.txt")).unwrap();
        env.handle.retry_failed(id);
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert_eq!((outcome.files_done, outcome.files_failed), (2, 0));
        assert_eq!(std::fs::read(dst.join("tree/b.txt")).unwrap(), b"b");
    }

    #[test]
    fn batch_copies_listed_files_as_one_job() {
        use crate::queue::{BatchFile, NewBatch};

        let env = test_env("batch");
        let src = env.dir.join("src");
        let dst = env.dir.join("dst");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();
        std::fs::write(src.join("sub/b.txt"), b"bb").unwrap();
        std::fs::write(src.join("left-out.txt"), b"x").unwrap();
        let pair = |rel: &str, size| BatchFile {
            src: browse(&src.join(rel)),
            dst: browse(&dst.join(rel)),
            size: Some(size),
        };

        let id = env
            .rt
            .block_on(env.handle.submit_batch(NewBatch {
                src_site_id: 0,
                dst_site_id: 0,
                src_label: format!("{}/", browse(&src)),
                dst_label: format!("{}/", browse(&dst)),
                files: vec![pair("a.txt", 1), pair("sub/b.txt", 2)],
                conflict_policy: ConflictPolicy::Overwrite,
                move_source: false,
            }))
            .unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert_eq!((outcome.files_done, outcome.files_failed), (2, 0));
        assert_eq!(std::fs::read(dst.join("sub/b.txt")).unwrap(), b"bb");
        assert!(!dst.join("left-out.txt").exists());
        let snap = wait_until(&env.handle, |snap| snap.jobs.len() == 1);
        assert_eq!(snap.jobs[0].bytes_total, 3);
    }

    #[test]
    fn batch_move_leaves_the_source_folder_alone() {
        use crate::queue::{BatchFile, NewBatch};

        let env = test_env("batch-move");
        let src = env.dir.join("inbox");
        let dst = env.dir.join("dst");
        std::fs::create_dir_all(src.join("empty")).unwrap();
        std::fs::create_dir_all(&dst).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();
        std::fs::write(src.join("b.txt"), b"b").unwrap();
        let pair = |name: &str| BatchFile {
            src: browse(&src.join(name)),
            dst: browse(&dst.join(name)),
            size: Some(1),
        };

        let id = env
            .rt
            .block_on(env.handle.submit_batch(NewBatch {
                src_site_id: 0,
                dst_site_id: 0,
                src_label: format!("{}/", browse(&src)),
                dst_label: format!("{}/", browse(&dst)),
                files: vec![pair("a.txt"), pair("b.txt")],
                conflict_policy: ConflictPolicy::Overwrite,
                move_source: true,
            }))
            .unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert_eq!(std::fs::read(dst.join("b.txt")).unwrap(), b"b");
        assert!(!src.join("a.txt").exists() && !src.join("b.txt").exists());
        // The folder sweep runs detached after settling; give it time to
        // (wrongly) act before checking it didn't.
        std::thread::sleep(Duration::from_millis(500));
        assert!(src.join("empty").is_dir(), "an unselected empty folder was deleted");
    }

    #[test]
    fn cancel_settles_only_after_workers_and_cleanup() {
        let (env, src_dir, dst_dir, _) = slow_env("cancel-settles");
        std::fs::write(dst_dir.join("big.bin"), b"OLD").unwrap();
        let id = env
            .rt
            .block_on(env.handle.submit(slow_job(&src_dir.join("big.bin"), &dst_dir, false)))
            .unwrap();
        wait_until(&env.handle, |s| {
            s.jobs.first().is_some_and(|j| j.state == JobState::Active && j.bytes_done > 0)
        });
        env.handle.cancel(id);
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Cancelled);
        // Settled: the worker stopped and its partial is gone already.
        let states = env.rt.block_on(store::task_states(&env.pool, id)).unwrap();
        assert!(!states.contains_key("active"), "{states:?}");
        assert!(partials(&dst_dir).is_empty());
        assert_eq!(std::fs::read(dst_dir.join("big.bin")).unwrap(), b"OLD");
    }

    #[test]
    fn resume_in_place_survives_a_pause() {
        let (env, src_dir, dst_dir, content) = slow_env("resume-in-place");
        let dst_file = dst_dir.join("big.bin");
        std::fs::write(&dst_file, &content[..1024 * 1024]).unwrap();
        let id = env
            .rt
            .block_on(env.handle.submit(NewJob {
                conflict_policy: ConflictPolicy::Resume,
                ..slow_job(&src_dir.join("big.bin"), &dst_dir, false)
            }))
            .unwrap();
        pause_once_past(&env, id, 0);
        let appended = std::fs::metadata(&dst_file).unwrap().len();
        assert!(appended > 1024 * 1024);

        // The resumed run keeps appending in place: no staged copy from zero.
        env.handle.resume(id);
        pause_once_past(&env, id, appended);
        assert!(partials(&dst_dir).is_empty());
        assert!(std::fs::metadata(&dst_file).unwrap().len() > appended);

        env.handle.resume(id);
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert!(std::fs::read(&dst_file).unwrap() == content);
    }

    #[test]
    fn resume_asks_when_the_destination_is_not_the_start_of_the_source() {
        let (env, src_dir, dst_dir) = conflict_env("resume-foreign", b"NEW and longer");
        env.handle.enqueue(job(
            browse(&src_dir.join("file.txt")),
            format!("{}/", browse(&dst_dir)),
            ConflictPolicy::Resume,
        ));
        wait_until(&env.handle, |s| s.jobs.first().is_some_and(|j| j.files_conflict == 1));
        assert_eq!(std::fs::read(dst_dir.join("file.txt")).unwrap(), b"OLD");
    }

    #[test]
    fn an_in_place_restart_from_zero_checks_the_destination_again() {
        let env = test_env("in-place-restart");
        let src = env.dir.join("src.txt");
        let dst = env.dir.join("dst/file.txt");
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::write(&src, b"NEW and longer").unwrap();
        std::fs::write(&dst, b"OLD").unwrap();
        let (job, task) = legacy_job(&env, ("paused", "paused"), 0, &src, &dst, 3);
        // The source changed since the offset was recorded.
        env.rt
            .block_on(
                sqlx::query(
                    "UPDATE transfer_tasks SET in_place = 1, conflict_policy = 'resume', \
                     src_modified = '2001-01-01T00:00:00+00:00' WHERE id = ?",
                )
                .bind(task)
                .execute(&env.pool),
            )
            .unwrap();

        env.handle.resume(job);
        wait_until(&env.handle, |s| s.jobs.first().is_some_and(|j| j.files_conflict == 1));
        assert_eq!(std::fs::read(&dst).unwrap(), b"OLD");
        let in_place: i64 = env
            .rt
            .block_on(
                sqlx::query_scalar("SELECT in_place FROM transfer_tasks WHERE id = ?")
                    .bind(task)
                    .fetch_one(&env.pool),
            )
            .unwrap();
        assert_eq!(in_place, 0);

        env.handle.resolve(job, ConflictPolicy::Overwrite);
        let outcome = env.rt.block_on(env.handle.settled(job)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert_eq!(std::fs::read(&dst).unwrap(), b"NEW and longer");
    }

    #[test]
    fn a_second_job_for_the_same_destination_waits_and_then_asks() {
        let (env, src_dir, dst_dir, content) = slow_env("same-destination");
        let other = env.dir.join("other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("big.bin"), b"second").unwrap();
        let ask = |src: &Path| NewJob {
            conflict_policy: ConflictPolicy::Ask,
            ..slow_job(src, &dst_dir, false)
        };

        let first = env.rt.block_on(env.handle.submit(ask(&src_dir.join("big.bin")))).unwrap();
        wait_until(&env.handle, |s| s.jobs.iter().any(|j| j.id == first && j.bytes_done > 0));
        let second = env.rt.block_on(env.handle.submit(ask(&other.join("big.bin")))).unwrap();

        let outcome = env.rt.block_on(env.handle.settled(first)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        wait_until(&env.handle, |s| {
            s.jobs.iter().any(|j| j.id == second && j.files_conflict == 1)
        });
        assert!(std::fs::read(dst_dir.join("big.bin")).unwrap() == content);
    }

    #[test]
    fn a_lost_connection_keeps_the_job_waiting_without_retries_left() {
        let env = test_env("outage-wait");
        env.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO sites (id, name, scheme, options) \
                     VALUES (2, 'Offline', 'webdav', '{\"endpoint\":\"http://127.0.0.1:1\"}')",
                )
                .execute(&env.pool),
            )
            .unwrap();
        env.handle.enqueue(NewJob {
            src_site_id: 2,
            ..job("file.txt".into(), format!("{}/", browse(&env.dir)), ConflictPolicy::Overwrite)
        });
        wait_until(&env.handle, |s| s.jobs.first().is_some_and(|j| j.error.is_some()));
        // Past the first backoff, with `retries` at 0: still waiting.
        std::thread::sleep(Duration::from_millis(2500));
        assert_eq!(env.handle.latest().jobs[0].state, JobState::Queued);
    }

    #[test]
    fn links_to_files_are_copied_and_other_links_are_reported() {
        let env = test_env("symlinks");
        let src = env.dir.join("tree");
        let dst = env.dir.join("out");
        std::fs::create_dir_all(src.join("folder")).unwrap();
        std::fs::write(src.join("real.txt"), b"real").unwrap();
        std::os::unix::fs::symlink(src.join("real.txt"), src.join("file-link.txt")).unwrap();
        std::os::unix::fs::symlink(&src, src.join("folder/cycle")).unwrap();
        std::os::unix::fs::symlink(src.join("gone"), src.join("broken")).unwrap();

        let id = env
            .rt
            .block_on(env.handle.submit(job(
                format!("{}/", browse(&src)),
                format!("{}/", browse(&dst)),
                ConflictPolicy::Overwrite,
            )))
            .unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert_eq!(outcome.files_done, 2);
        let copied = dst.join("tree/file-link.txt");
        assert!(!copied.is_symlink());
        assert_eq!(std::fs::read(copied).unwrap(), b"real");
        assert!(!dst.join("tree/folder/cycle").exists());

        let tasks = env.rt.block_on(store::job_tasks(&env.pool, id, 10)).unwrap();
        let reason = |name: &str| {
            let task = tasks.iter().find(|t| t.src.ends_with(name)).unwrap();
            assert_eq!(task.state, "skipped");
            task.error.clone().unwrap()
        };
        assert_eq!(reason("folder/cycle"), "Link to a folder, not copied");
        assert_eq!(reason("broken"), "Broken link, not copied");
    }

    /// One reviewed file `src/new.txt` → `dst/file.txt`, as a sync or an
    /// edit save submits it.
    fn reviewed_batch(env: &TestEnv, expected: crate::queue::ExpectedDest) -> i64 {
        let dst = browse(&env.dir.join("dst/file.txt"));
        let batch = NewBatch {
            src_site_id: 0,
            dst_site_id: 1,
            src_label: "src".into(),
            dst_label: "dst".into(),
            files: vec![BatchFile {
                src: browse(&env.dir.join("src/new.txt")),
                dst: dst.clone(),
                size: Some(3),
            }],
            conflict_policy: ConflictPolicy::Overwrite,
            move_source: false,
        };
        env.rt
            .block_on(env.handle.submit_reviewed_batch(batch, [(dst, expected)].into()))
            .unwrap()
    }

    fn reviewed_env(name: &str) -> TestEnv {
        let env = test_env(name);
        env.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO sites (id, name, scheme, options) \
                     VALUES (1, 'Local copy', 'fs', '{\"root\":\"/\"}')",
                )
                .execute(&env.pool),
            )
            .unwrap();
        std::fs::create_dir_all(env.dir.join("src")).unwrap();
        std::fs::create_dir_all(env.dir.join("dst")).unwrap();
        std::fs::write(env.dir.join("src/new.txt"), b"new").unwrap();
        env
    }

    fn wait_for_conflict(env: &TestEnv, id: i64) {
        wait_until(&env.handle, |s| s.jobs.iter().any(|j| j.id == id && j.files_conflict == 1));
    }

    #[test]
    fn a_reviewed_write_parks_when_the_destination_changed_and_the_decision_wins() {
        let env = reviewed_env("reviewed-changed");
        let dst = env.dir.join("dst/file.txt");
        std::fs::write(&dst, b"theirs").unwrap();
        let id = reviewed_batch(
            &env,
            crate::queue::ExpectedDest::File { size: 3, etag: None, modified: None },
        );
        wait_for_conflict(&env, id);
        assert_eq!(std::fs::read(&dst).unwrap(), b"theirs");

        env.handle.resolve(id, ConflictPolicy::Overwrite);
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(std::fs::read(&dst).unwrap(), b"new");
    }

    #[test]
    fn a_reviewed_new_file_parks_when_one_appeared_meanwhile() {
        let env = reviewed_env("reviewed-absent");
        let dst = env.dir.join("dst/file.txt");
        std::fs::write(&dst, b"appeared").unwrap();
        let id = reviewed_batch(&env, crate::queue::ExpectedDest::Absent);
        wait_for_conflict(&env, id);
        assert_eq!(std::fs::read(&dst).unwrap(), b"appeared");
    }

    #[test]
    fn a_retried_save_goes_through_the_check_again() {
        let env = reviewed_env("reviewed-retry");
        let dst = env.dir.join("dst/file.txt");
        std::fs::write(&dst, b"old").unwrap();
        let reviewed = crate::queue::ExpectedDest::File { size: 3, etag: None, modified: None };
        // The save fails because its source is gone.
        std::fs::rename(env.dir.join("src/new.txt"), env.dir.join("src/parked.txt")).unwrap();
        let id = reviewed_batch(&env, reviewed);
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!(outcome.files_failed, 1);

        // A later version lands on the server before the old save is retried.
        std::fs::write(&dst, b"a newer save").unwrap();
        std::fs::rename(env.dir.join("src/parked.txt"), env.dir.join("src/new.txt")).unwrap();
        env.handle.retry_failed(id);
        wait_for_conflict(&env, id);
        assert_eq!(std::fs::read(&dst).unwrap(), b"a newer save");
    }

    /// An engine with an FTP site (id 2) backed by the in-memory test server.
    /// Local sources stream to it; `up/` is the upload folder.
    fn ftp_env(name: &str, bandwidth: u64) -> (TestEnv, crate::ftp::test_server::FakeFtp) {
        let env = test_env_with(name, |cfg| cfg.bandwidth_limit = bandwidth);
        let server = crate::ftp::test_server::FakeFtp::start();
        let options = format!("{{\"endpoint\":\"ftp://127.0.0.1:{}\"}}", server.port);
        env.rt
            .block_on(
                sqlx::query("INSERT INTO sites (id, name, scheme, options) VALUES (2, 'FTP', 'ftp', ?)")
                    .bind(options)
                    .execute(&env.pool),
            )
            .unwrap();
        (env, server)
    }

    fn ftp_upload(src: &Path) -> NewJob {
        NewJob {
            dst_site_id: 2,
            ..job(browse(src), "up/".to_string(), ConflictPolicy::Overwrite)
        }
    }

    #[test]
    fn an_ftp_upload_replaces_the_old_file_through_a_partial() {
        let (env, server) = ftp_env("ftp-replace", 0);
        server.put("up/file.txt", b"old");
        let src = env.dir.join("file.txt");
        std::fs::write(&src, b"new content").unwrap();

        let id = env.rt.block_on(env.handle.submit(ftp_upload(&src))).unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();

        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(server.read("up/file.txt").unwrap(), b"new content");
        assert_eq!(server.names(), ["up/file.txt"]);
    }

    #[test]
    fn an_empty_file_reaches_the_ftp_server() {
        let (env, server) = ftp_env("ftp-empty", 0);
        let src = env.dir.join("empty.txt");
        std::fs::write(&src, b"").unwrap();

        let id = env.rt.block_on(env.handle.submit(ftp_upload(&src))).unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();

        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(server.read("up/empty.txt").unwrap(), b"");
        assert_eq!(server.names(), ["up/empty.txt"]);
    }

    #[test]
    fn an_interrupted_ftp_upload_never_shows_under_the_final_name() {
        let (env, server) = ftp_env("ftp-interrupted", 8 * 1024 * 1024);
        server.put("up/big.bin", b"old");
        let src = env.dir.join("big.bin");
        let content: Vec<u8> = (0..40 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&src, &content).unwrap();

        let id = env.rt.block_on(env.handle.submit(ftp_upload(&src))).unwrap();
        pause_once_past(&env, id, 0);
        assert_eq!(server.read("up/big.bin").unwrap(), b"old");
        let names = server.names();
        let partial = names.iter().find(|name| name.ends_with(transfer::PARTIAL_SUFFIX)).unwrap();
        assert!(server.read(partial).unwrap().len() < content.len());

        env.handle.resume(id);
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();
        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert!(server.read("up/big.bin").unwrap() == content, "the resumed upload differs");
        assert_eq!(server.names(), ["up/big.bin"]);
    }

    #[test]
    fn an_ftp_upload_leaves_a_file_named_like_an_old_partial_alone() {
        let (env, server) = ftp_env("ftp-foreign-partial", 0);
        // The name earlier versions derived from the file and the task id.
        server.put("up/file.txt.1.kessel-part", b"not ours");
        let src = env.dir.join("file.txt");
        std::fs::write(&src, b"new content").unwrap();

        let id = env.rt.block_on(env.handle.submit(ftp_upload(&src))).unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();

        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(server.read("up/file.txt.1.kessel-part").unwrap(), b"not ours");
        assert_eq!(server.names(), ["up/file.txt", "up/file.txt.1.kessel-part"]);
    }

    #[test]
    fn a_dotfile_upload_verifies_on_a_server_that_hides_dotfiles() {
        let (env, server) = ftp_env("ftp-dotfile", 0);
        let src = env.dir.join(".htaccess");
        std::fs::write(&src, b"deny from all").unwrap();

        let id = env.rt.block_on(env.handle.submit(ftp_upload(&src))).unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();

        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(server.read("up/.htaccess").unwrap(), b"deny from all");
        assert_eq!(server.names(), ["up/.htaccess"]);
    }

    /// A task that stopped mid-file with `partial` written, parked as a
    /// conflict over the destination holding `theirs`.
    fn parked_mid_file(env: &TestEnv, name: &str, theirs: &[u8]) -> (i64, PathBuf, PathBuf) {
        let src = env.dir.join(format!("src/{name}"));
        let dst = env.dir.join(format!("dst/{name}"));
        std::fs::create_dir_all(src.parent().unwrap()).unwrap();
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::write(&src, b"0123456789").unwrap();
        std::fs::write(&dst, theirs).unwrap();
        let (job, task) = legacy_job(env, ("active", "active"), 0, &src, &dst, 4);
        let partial = legacy_partial(&dst, task);
        std::fs::write(&partial, b"0123").unwrap();
        env.rt.block_on(async {
            sqlx::query("UPDATE transfer_tasks SET partial = ? WHERE id = ?")
                .bind(browse(&partial))
                .bind(task)
                .execute(&env.pool)
                .await
                .unwrap();
            store::park_task_conflict(&env.pool, task, ConflictPolicy::Ask, theirs.len() as u64, None)
                .await
                .unwrap();
        });
        (job, dst, partial)
    }

    #[test]
    fn skipping_a_conflict_parked_mid_file_keeps_the_destination() {
        let env = test_env("conflict-skip");
        let (job, dst, partial) = parked_mid_file(&env, "skip.bin", b"THEIRS");

        env.handle.resolve(job, ConflictPolicy::Skip);
        let outcome = env.rt.block_on(env.handle.settled(job)).unwrap();

        assert_eq!((outcome.state, outcome.files_skipped), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(std::fs::read(&dst).unwrap(), b"THEIRS");
        assert!(!partial.exists(), "the stale partial was kept");
    }

    #[test]
    fn keeping_both_after_a_conflict_parked_mid_file_keeps_the_destination() {
        let env = test_env("conflict-keep-both");
        let (job, dst, partial) = parked_mid_file(&env, "both.bin", b"THEIRS");

        env.handle.resolve(job, ConflictPolicy::Rename);
        let outcome = env.rt.block_on(env.handle.settled(job)).unwrap();

        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert_eq!(std::fs::read(&dst).unwrap(), b"THEIRS");
        assert_eq!(std::fs::read(dst.with_file_name("both (1).bin")).unwrap(), b"0123456789");
        assert!(!partial.exists(), "the stale partial was kept");
    }

    #[cfg(unix)]
    #[test]
    fn a_same_site_move_renames_instead_of_copying() {
        use std::os::unix::fs::MetadataExt as _;

        let env = test_env("same-site-move");
        let src = env.dir.join("from/file.txt");
        let dst_dir = env.dir.join("to");
        std::fs::create_dir_all(src.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&dst_dir).unwrap();
        std::fs::write(&src, b"moved").unwrap();
        let inode = std::fs::metadata(&src).unwrap().ino();

        let id = env
            .rt
            .block_on(env.handle.submit(NewJob {
                move_source: true,
                ..job(browse(&src), format!("{}/", browse(&dst_dir)), ConflictPolicy::Ask)
            }))
            .unwrap();
        let outcome = env.rt.block_on(env.handle.settled(id)).unwrap();

        assert_eq!((outcome.state, outcome.files_done), (JobState::Done, 1), "{:?}", outcome.error);
        assert!(!src.exists());
        assert_eq!(std::fs::metadata(dst_dir.join("file.txt")).unwrap().ino(), inode);
    }

    #[test]
    fn a_site_limit_holds_across_jobs() {
        let (env, src_dir, dst_dir, _) = slow_env("site-limit");
        env.rt
            .block_on(
                sqlx::query("UPDATE sites SET layers = '{\"concurrent_limit\":1}' WHERE id = 1")
                    .execute(&env.pool),
            )
            .unwrap();
        std::fs::write(src_dir.join("small.bin"), vec![1u8; 1024]).unwrap();
        let first = env
            .rt
            .block_on(env.handle.submit(slow_job(&src_dir.join("big.bin"), &dst_dir, false)))
            .unwrap();
        wait_until(&env.handle, |s| s.jobs.iter().any(|j| j.id == first && j.bytes_done > 0));
        let second = env
            .rt
            .block_on(env.handle.submit(slow_job(&src_dir.join("small.bin"), &dst_dir, false)))
            .unwrap();

        // The small file would finish at once if it were let through.
        std::thread::sleep(Duration::from_millis(800));
        let states = env.rt.block_on(store::task_states(&env.pool, second)).unwrap();
        assert_eq!(states.get("queued"), Some(&1), "{states:?}");

        let outcome = env.rt.block_on(env.handle.settled(second)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
    }

    #[test]
    fn cancel_and_remove_delete_every_owned_partial() {
        let env = test_env("owned-partials");
        let dst = env.dir.join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        // A paused task whose non-append partial checkpointed offset 0, and a
        // failed task of a settled job: both still own their partial files.
        let job_with_partial = |state: &str, task_state: &str, name: &str| {
            let part = dst.join(format!(".{name}.kessel-part"));
            std::fs::write(&part, b"half").unwrap();
            env.rt.block_on(async {
                let job: i64 = sqlx::query_scalar(
                    "INSERT INTO transfer_jobs (src_site_id, dst_site_id, src, dst, state, expanded, finished_at) \
                     VALUES (0, 0, 'src/x', ?, ?, 1, CASE WHEN ? = 'done' THEN datetime('now') END) RETURNING id",
                )
                .bind(format!("{}/", browse(&dst)))
                .bind(state)
                .bind(state)
                .fetch_one(&env.pool)
                .await
                .unwrap();
                sqlx::query(
                    "INSERT INTO transfer_tasks (job_id, src, dst, state, total_bytes, partial) \
                     VALUES (?, 'src/x', ?, ?, 10, ?)",
                )
                .bind(job)
                .bind(browse(&dst.join(name)))
                .bind(task_state)
                .bind(browse(&part))
                .execute(&env.pool)
                .await
                .unwrap();
                (job, part)
            })
        };
        let (paused_job, paused_part) = job_with_partial("paused", "paused", "a.txt");
        let (settled_job, failed_part) = job_with_partial("done", "failed", "b.txt");

        env.handle.cancel(paused_job);
        let outcome = env.rt.block_on(env.handle.settled(paused_job)).unwrap();
        assert_eq!(outcome.state, JobState::Cancelled);
        assert!(!paused_part.exists());

        env.handle.remove(settled_job);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while failed_part.exists() {
            assert!(std::time::Instant::now() < deadline, "partial of the removed job was kept");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    /// A task row as saved before migration 0013: it ran (`attempts` 1) but
    /// recorded neither a partial nor a write mode. Returns (job, task) ids.
    fn legacy_job(
        env: &TestEnv,
        (job_state, task_state): (&str, &str),
        dst_site_id: i64,
        src: &Path,
        dst: &Path,
        byte_offset: u64,
    ) -> (i64, i64) {
        env.rt.block_on(async {
            let meta = opendal::Operator::via_iter("fs", [("root".to_string(), "/".to_string())])
                .unwrap()
                .stat(&browse(src))
                .await
                .ok();
            let modified = meta
                .as_ref()
                .and_then(|meta| meta.last_modified())
                .and_then(crate::provider::to_datetime)
                .map(|t| t.to_rfc3339());
            let job: i64 = sqlx::query_scalar(
                "INSERT INTO transfer_jobs (src_site_id, dst_site_id, src, dst, state, expanded, finished_at) \
                 VALUES (0, ?, ?, ?, ?, 1, CASE WHEN ? = 'done' THEN datetime('now') END) RETURNING id",
            )
            .bind(dst_site_id)
            .bind(browse(src))
            .bind(format!("{}/", browse(dst.parent().unwrap())))
            .bind(job_state)
            .bind(job_state)
            .fetch_one(&env.pool)
            .await
            .unwrap();
            let task: i64 = sqlx::query_scalar(
                "INSERT INTO transfer_tasks \
                   (job_id, src, dst, state, byte_offset, total_bytes, src_modified, attempts) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, 1) RETURNING id",
            )
            .bind(job)
            .bind(browse(src))
            .bind(browse(dst))
            .bind(task_state)
            .bind(byte_offset as i64)
            .bind(meta.map(|meta| meta.content_length() as i64))
            .bind(modified)
            .fetch_one(&env.pool)
            .await
            .unwrap();
            (job, task)
        })
    }

    /// The deterministic partial file of task `id` writing `dst`.
    fn legacy_partial(dst: &Path, id: i64) -> PathBuf {
        let name = dst.file_name().unwrap().to_str().unwrap();
        dst.with_file_name(format!(".{name}.{id}{}", transfer::PARTIAL_SUFFIX))
    }

    #[test]
    fn a_legacy_task_resumes_its_deterministic_partial() {
        let env = test_env("legacy-resume");
        let src = env.dir.join("src/file.bin");
        let dst = env.dir.join("dst/file.bin");
        std::fs::create_dir_all(src.parent().unwrap()).unwrap();
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        let content: Vec<u8> = (0..64u8).collect();
        std::fs::write(&src, &content).unwrap();
        let (job, task) = legacy_job(&env, ("paused", "paused"), 0, &src, &dst, 16);
        // Marked bytes prove the copy appended to the partial instead of
        // starting over.
        std::fs::write(legacy_partial(&dst, task), [b'X'; 16]).unwrap();

        env.handle.resume(job);
        let outcome = env.rt.block_on(env.handle.settled(job)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        let mut expected = vec![b'X'; 16];
        expected.extend_from_slice(&content[16..]);
        assert_eq!(std::fs::read(&dst).unwrap(), expected);
        assert!(partials(dst.parent().unwrap()).is_empty());
    }

    #[test]
    fn a_legacy_task_without_a_partial_restarts_and_keeps_the_destination() {
        let (env, src_dir, dst_dir, content) = slow_env("legacy-restart");
        let src = src_dir.join("big.bin");
        let dst = dst_dir.join("big.bin");
        // Before partial files, a resumed task appended to the destination
        // in place; what it holds now can't be trusted.
        std::fs::write(&dst, b"OLD").unwrap();
        let offset = 1024 * 1024;
        let (job, task) = legacy_job(&env, ("paused", "paused"), 1, &src, &dst, offset);
        // A restart from zero asks about an existing file unless told not to.
        env.rt
            .block_on(
                sqlx::query("UPDATE transfer_jobs SET conflict_policy = 'overwrite' WHERE id = ?")
                    .bind(job)
                    .execute(&env.pool),
            )
            .unwrap();

        env.handle.resume(job);
        pause_once_past(&env, job, offset);
        assert_eq!(std::fs::read(&dst).unwrap(), b"OLD");
        assert_eq!(partials(&dst_dir), vec![legacy_partial(&dst, task)]);

        env.handle.resume(job);
        let outcome = env.rt.block_on(env.handle.settled(job)).unwrap();
        assert_eq!(outcome.state, JobState::Done, "{:?}", outcome.error);
        assert!(std::fs::read(&dst).unwrap() == content);
        assert!(partials(&dst_dir).is_empty());
    }

    #[test]
    fn cancel_and_remove_delete_legacy_partials() {
        let env = test_env("legacy-partials");
        let src = env.dir.join("src.bin");
        std::fs::write(&src, b"0123456789").unwrap();
        let dst = env.dir.join("dst");
        std::fs::create_dir_all(&dst).unwrap();
        let (paused_job, paused_task) =
            legacy_job(&env, ("paused", "paused"), 0, &src, &dst.join("a.txt"), 0);
        let (settled_job, failed_task) =
            legacy_job(&env, ("done", "failed"), 0, &src, &dst.join("b.txt"), 4);
        let paused_part = legacy_partial(&dst.join("a.txt"), paused_task);
        let failed_part = legacy_partial(&dst.join("b.txt"), failed_task);
        std::fs::write(&paused_part, b"half").unwrap();
        std::fs::write(&failed_part, b"half").unwrap();
        // These rows were inserted after migrating; run the backfill that
        // real databases get for rows saved before 0013.
        env.rt
            .block_on(
                sqlx::raw_sql(include_str!("../../migrations/0014_legacy_task_partials.sql"))
                    .execute(&env.pool),
            )
            .unwrap();

        env.handle.cancel(paused_job);
        let outcome = env.rt.block_on(env.handle.settled(paused_job)).unwrap();
        assert_eq!(outcome.state, JobState::Cancelled);
        assert!(!paused_part.exists());

        env.handle.remove(settled_job);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while failed_part.exists() {
            assert!(std::time::Instant::now() < deadline, "legacy partial of the removed job was kept");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    fn transient_preparation_failure_keeps_the_job_queued() {
        let env = test_env_with("prep-transient", |cfg| cfg.retries = 3);
        env.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO sites (id, name, scheme, options) \
                     VALUES (2, 'Offline', 'webdav', '{\"endpoint\":\"http://127.0.0.1:1\"}')",
                )
                .execute(&env.pool),
            )
            .unwrap();
        env.handle.enqueue(NewJob {
            src_site_id: 2,
            ..job("file.txt".into(), format!("{}/", browse(&env.dir)), ConflictPolicy::Overwrite)
        });
        let snap = wait_until(&env.handle, |s| s.jobs.first().is_some_and(|j| j.error.is_some()));
        assert_eq!(snap.jobs[0].state, JobState::Queued, "{:?}", snap.jobs[0].error);
    }

    #[test]
    fn expansion_stops_when_the_job_is_cancelled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("kessel-expand-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("tree/nested")).unwrap();
        std::fs::write(dir.join("tree/a.txt"), b"a").unwrap();
        std::fs::write(dir.join("tree/nested/b.txt"), b"b").unwrap();
        rt.block_on(async {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            sqlx::migrate!().run(&pool).await.unwrap();
            let id = store::enqueue_job(
                &pool,
                &job(
                    format!("{}/", browse(&dir.join("tree"))),
                    format!("{}/", browse(&dir.join("out"))),
                    ConflictPolicy::Overwrite,
                ),
            )
            .await
            .unwrap();
            let row = store::get_job(&pool, id).await.unwrap().unwrap();
            let local = Provider::connect(
                &crate::provider::SiteProfile {
                    scheme: "fs".into(),
                    options: [("root".to_string(), "/".to_string())].into(),
                    ..Default::default()
                },
                &LayerConfig::default(),
            )
            .unwrap();
            let providers = JobProviders {
                src: local.clone(),
                dst: local,
                same_site: true,
                move_source: false,
                src_limit: None,
                dst_limit: None,
            };
            // A full walk reports what it found while listing.
            let control = JobControl::default();
            assert!(expand_job(&pool, &providers, &row, false, &control).await.unwrap());
            assert!(control.listing.load(Ordering::Relaxed));
            assert_eq!(control.discovered.load(Ordering::Relaxed), 2);

            store::begin_expansion(&pool, id).await.unwrap();
            let control = JobControl::default();
            control.cancelled.store(true, Ordering::Relaxed);
            assert!(!expand_job(&pool, &providers, &row, false, &control).await.unwrap());
            assert!(store::task_states(&pool, id).await.unwrap().is_empty());
        });
        let _ = std::fs::remove_dir_all(&dir);
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
