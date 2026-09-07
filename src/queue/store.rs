//! All queue SQL lives here. The engine never touches sqlx directly, so the
//! persistence contract (what survives a crash) is auditable in one file.

use std::collections::HashMap;

use anyhow::{Context as _, Result};
use sqlx::{Row as _, SqlitePool};

use crate::provider::{LayerConfig, SiteProfile};

use super::{
    ConflictInfo, ConflictPolicy, HistoryEntry, JobSnapshot, JobState, NewJob, TaskSnapshot,
};

/// A job row as the scheduler sees it.
#[derive(Debug, Clone)]
pub(crate) struct JobRow {
    pub id: i64,
    pub src_site_id: i64,
    pub dst_site_id: i64,
    pub src: String,
    pub dst: String,
    /// Destination name override ("Download As…"); `None` keeps the source name.
    pub dst_name: Option<String>,
    /// Directory fan-out finished (tasks are inserted in batches, so the
    /// mere presence of tasks doesn't prove the listing completed).
    pub expanded: bool,
    pub move_source: bool,
}

/// A task claimed for execution (state already flipped to `active`,
/// attempts already incremented).
#[derive(Debug, Clone)]
pub(crate) struct ClaimedTask {
    pub id: i64,
    pub job_id: i64,
    pub src: String,
    pub dst: String,
    pub byte_offset: u64,
    pub total_bytes: Option<u64>,
    pub src_etag: Option<String>,
    /// This file's decision, falling back to the owning job's policy.
    pub conflict_policy: ConflictPolicy,
    /// The owning job transfers with move semantics.
    pub move_source: bool,
}

/// A file discovered during directory expansion.
#[derive(Debug)]
pub(crate) struct NewTask {
    pub src: String,
    pub dst: String,
    pub total_bytes: Option<u64>,
    pub src_etag: Option<String>,
}

pub(crate) async fn get_site(pool: &SqlitePool, site_id: i64) -> Result<SiteProfile> {
    let row = sqlx::query(
        "SELECT id, name, scheme, options, layers, secret_handle FROM sites WHERE id = ?",
    )
    .bind(site_id)
    .fetch_optional(pool)
    .await?
    .with_context(|| format!("site {site_id} no longer exists"))?;

    Ok(SiteProfile {
        id: row.get("id"),
        name: row.get("name"),
        scheme: row.get("scheme"),
        options: serde_json::from_str(row.get::<String, _>("options").as_str())?,
        layers: serde_json::from_str::<LayerConfig>(row.get::<String, _>("layers").as_str())?,
        secret_handle: row.get("secret_handle"),
        // Transfers only need connection data, not sidebar presentation.
        ..Default::default()
    })
}

pub(crate) async fn enqueue_job(pool: &SqlitePool, job: &NewJob) -> Result<i64> {
    let id = sqlx::query(
        "INSERT INTO transfer_jobs \
           (src_site_id, dst_site_id, src, dst, dst_name, conflict_policy, move_source) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(job.src_site_id)
    .bind(job.dst_site_id)
    .bind(&job.src)
    .bind(&job.dst_dir)
    .bind(&job.dst_name)
    .bind(job.conflict_policy.as_str())
    .bind(job.move_source)
    .execute(pool)
    .await?
    .last_insert_rowid();
    Ok(id)
}

/// Jobs the scheduler may need to act on: queued (expand + run) or active
/// (running, or orphaned by a restart and needing fresh providers).
pub(crate) async fn runnable_jobs(pool: &SqlitePool) -> Result<Vec<JobRow>> {
    let rows = sqlx::query(
        "SELECT id, src_site_id, dst_site_id, src, dst, dst_name, expanded, move_source \
         FROM transfer_jobs \
         WHERE state IN ('queued', 'active') ORDER BY priority DESC, id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| JobRow {
            id: row.get("id"),
            src_site_id: row.get("src_site_id"),
            dst_site_id: row.get("dst_site_id"),
            src: row.get("src"),
            dst: row.get("dst"),
            dst_name: row.get("dst_name"),
            expanded: row.get::<i64, _>("expanded") != 0,
            move_source: row.get::<i64, _>("move_source") != 0,
        })
        .collect())
}

/// Start (or restart) a job's fan-out: drop any tasks a previously
/// interrupted expansion left behind. Tasks stream in via [`insert_tasks`]
/// and [`finish_expansion`] seals the fan-out.
pub(crate) async fn begin_expansion(pool: &SqlitePool, job_id: i64) -> Result<()> {
    sqlx::query("DELETE FROM transfer_tasks WHERE job_id = ?")
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Insert one batch of discovered tasks (multi-row insert, one transaction).
pub(crate) async fn insert_tasks(pool: &SqlitePool, job_id: i64, tasks: &[NewTask]) -> Result<()> {
    if tasks.is_empty() {
        return Ok(());
    }
    let mut builder = sqlx::QueryBuilder::new(
        "INSERT INTO transfer_tasks (job_id, src, dst, total_bytes, src_etag) ",
    );
    builder.push_values(tasks, |mut row, task| {
        row.push_bind(job_id)
            .push_bind(&task.src)
            .push_bind(&task.dst)
            .push_bind(task.total_bytes.map(|b| b as i64))
            .push_bind(&task.src_etag);
    });
    builder.build().execute(pool).await?;
    Ok(())
}

/// Seal a completed fan-out and flip the job to `active` in one transaction.
/// A crash before this point leaves `expanded = 0`, so expansion reruns from
/// scratch on the next launch.
pub(crate) async fn finish_expansion(pool: &SqlitePool, job_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE transfer_jobs SET expanded = 1 WHERE id = ?")
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    activate(&mut tx, job_id).await?;
    tx.commit().await?;
    Ok(())
}

/// `queued` → `active` (a user pause during preparation wins and is kept).
pub(crate) async fn activate_job(pool: &SqlitePool, job_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    activate(&mut tx, job_id).await?;
    tx.commit().await?;
    Ok(())
}

async fn activate(tx: &mut sqlx::SqliteConnection, job_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_jobs SET state = 'active', \
         started_at = COALESCE(started_at, datetime('now')) \
         WHERE id = ? AND state = 'queued'",
    )
    .bind(job_id)
    .execute(tx)
    .await?;
    Ok(())
}

/// Atomically claim the next queued task of any prepared job: flip it to
/// `active` and bump its attempt counter. `ready_job_ids` are jobs whose
/// providers are connected — only their tasks may run.
pub(crate) async fn claim_task(
    pool: &SqlitePool,
    ready_job_ids: &[i64],
) -> Result<Option<ClaimedTask>> {
    if ready_job_ids.is_empty() {
        return Ok(None);
    }
    let ids = ready_job_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "UPDATE transfer_tasks SET state = 'active', attempts = attempts + 1, \
         updated_at = datetime('now') \
         WHERE id = (SELECT t.id FROM transfer_tasks t \
             JOIN transfer_jobs j ON j.id = t.job_id \
             WHERE t.state = 'queued' AND j.state = 'active' AND j.id IN ({ids}) \
             ORDER BY j.priority DESC, t.id LIMIT 1) \
         RETURNING id, job_id, src, dst, byte_offset, total_bytes, src_etag, \
             COALESCE(conflict_policy, (SELECT conflict_policy FROM transfer_jobs WHERE id = job_id)) AS conflict_policy, \
             (SELECT move_source FROM transfer_jobs WHERE id = job_id) AS move_source"
    );
    // `ids` is a comma-joined list of i64 job ids, not user input.
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| ClaimedTask {
        id: row.get("id"),
        job_id: row.get("job_id"),
        src: row.get("src"),
        dst: row.get("dst"),
        byte_offset: row.get::<i64, _>("byte_offset") as u64,
        total_bytes: row.get::<Option<i64>, _>("total_bytes").map(|b| b as u64),
        src_etag: row.get("src_etag"),
        conflict_policy: ConflictPolicy::from_str(&row.get::<String, _>("conflict_policy")),
        move_source: row.get::<i64, _>("move_source") != 0,
    }))
}

/// Refresh source metadata and the (possibly reset) resume offset before the
/// byte loop starts.
pub(crate) async fn update_task_meta(
    pool: &SqlitePool,
    task_id: i64,
    total_bytes: u64,
    src_etag: Option<&str>,
    byte_offset: u64,
    src_modified: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET total_bytes = ?, src_etag = ?, byte_offset = ?, src_modified = ?, \
         updated_at = datetime('now') WHERE id = ?",
    )
    .bind(total_bytes as i64)
    .bind(src_etag)
    .bind(byte_offset as i64)
    .bind(src_modified)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// The periodic resume checkpoint — the most-written row in the schema.
pub(crate) async fn update_task_offset(
    pool: &SqlitePool,
    task_id: i64,
    byte_offset: u64,
) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET byte_offset = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(byte_offset as i64)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn complete_task(pool: &SqlitePool, task_id: i64, bytes: u64) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET state = 'done', byte_offset = ?, error = NULL, \
         updated_at = datetime('now') WHERE id = ?",
    )
    .bind(bytes as i64)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Park a task awaiting a user decision, recording what the destination
/// held so the prompt can show it.
pub(crate) async fn park_task_conflict(
    pool: &SqlitePool,
    task_id: i64,
    policy: ConflictPolicy,
    dst_size: u64,
    dst_modified: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET state = CASE WHEN COALESCE(conflict_policy, \
         (SELECT conflict_policy FROM transfer_jobs WHERE id = job_id)) = ? \
         THEN 'conflict' ELSE 'queued' END, dst_size = ?, dst_modified = ?, \
         updated_at = datetime('now') WHERE id = ?",
    )
    .bind(policy.as_str())
    .bind(dst_size as i64)
    .bind(dst_modified)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Persist a renamed destination (conflict policy `rename`) so retries and
/// resumes keep targeting the same unique name.
pub(crate) async fn update_task_dst(pool: &SqlitePool, task_id: i64, dst: &str) -> Result<()> {
    sqlx::query("UPDATE transfer_tasks SET dst = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(dst)
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Apply a conflict decision: the job's policy changes (covering future
/// conflicts too) and parked tasks go back in the queue.
pub(crate) async fn resolve_job_conflicts(
    pool: &SqlitePool,
    job_id: i64,
    policy: ConflictPolicy,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE transfer_jobs SET conflict_policy = ? WHERE id = ?")
        .bind(policy.as_str())
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE transfer_tasks SET conflict_policy = NULL, state = 'queued', updated_at = datetime('now') \
         WHERE job_id = ? AND state = 'conflict'",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn resolve_task_conflict(
    pool: &SqlitePool,
    task_id: i64,
    policy: ConflictPolicy,
) -> Result<()> {
    sqlx::query("UPDATE transfer_tasks SET conflict_policy = ?, state = 'queued', updated_at = datetime('now') WHERE id = ? AND state = 'conflict'")
        .bind(policy.as_str())
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn set_task_state(pool: &SqlitePool, task_id: i64, state: &str) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET state = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(state)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Task-level retry: re-queue (keeping the resume offset) while attempts
/// remain, otherwise fail the task for good.
pub(crate) async fn task_failed(
    pool: &SqlitePool,
    task_id: i64,
    error: &str,
    max_retries: u32,
) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET \
         state = CASE WHEN attempts <= ? THEN 'queued' ELSE 'failed' END, \
         error = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(max_retries as i64)
    .bind(error)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn pause_job(pool: &SqlitePool, job_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_jobs SET state = 'paused' \
         WHERE id = ? AND state IN ('queued', 'active')",
    )
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn resume_job(pool: &SqlitePool, job_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Back to `queued`: preparation (providers, and expansion if the pause
    // hit before fan-out) reruns and re-activates.
    sqlx::query("UPDATE transfer_jobs SET state = 'queued' WHERE id = ? AND state = 'paused'")
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE transfer_tasks SET state = 'queued', updated_at = datetime('now') \
         WHERE job_id = ? AND state = 'paused'",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn cancel_job(pool: &SqlitePool, job_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE transfer_jobs SET state = 'cancelled' \
         WHERE id = ? AND state IN ('queued', 'active', 'paused')",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE transfer_tasks SET state = 'cancelled', updated_at = datetime('now') \
         WHERE job_id = ? AND state IN ('queued', 'paused', 'conflict')",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Re-run a failed/cancelled job. Done tasks stay done; failed/cancelled
/// tasks re-queue with their resume offsets and a fresh attempt budget.
pub(crate) async fn retry_job(pool: &SqlitePool, job_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    let changed = sqlx::query(
        "UPDATE transfer_jobs SET state = 'queued', error = NULL, finished_at = NULL \
         WHERE id = ? AND state IN ('failed', 'cancelled')",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if changed > 0 {
        sqlx::query(
            "UPDATE transfer_tasks SET state = 'queued', attempts = 0, error = NULL, \
             updated_at = datetime('now') \
             WHERE job_id = ? AND state IN ('failed', 'cancelled', 'paused')",
        )
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// After a graceful shutdown, tasks that workers parked as `paused` belong
/// to still-`active` jobs (a user pause flips the job too). Re-queue them so
/// the next launch resumes automatically.
pub(crate) async fn requeue_shutdown_paused(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_tasks SET state = 'queued', updated_at = datetime('now') \
         WHERE state = 'paused' AND job_id IN \
           (SELECT id FROM transfer_jobs WHERE state = 'active')",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Preparation (connect / expand) failed: the whole job fails.
pub(crate) async fn fail_job(pool: &SqlitePool, job_id: i64, error: &str) -> Result<()> {
    sqlx::query(
        "UPDATE transfer_jobs SET state = 'failed', error = ? \
         WHERE id = ? AND state NOT IN ('done', 'cancelled')",
    )
    .bind(error)
    .bind(job_id)
    .execute(pool)
    .await?;
    finalize_history(pool, job_id).await
}

/// If nothing is left to run, settle the job's final state and write the
/// audit row. Idempotent: `finished_at` guards double finalization.
/// Returns true when the job just reached a terminal state.
pub(crate) async fn try_finalize_job(pool: &SqlitePool, job_id: i64) -> Result<bool> {
    let row = sqlx::query(
        "SELECT j.state, \
           SUM(CASE WHEN t.state IN ('queued', 'active', 'paused', 'conflict') THEN 1 ELSE 0 END) AS pending, \
           SUM(CASE WHEN t.state = 'failed' THEN 1 ELSE 0 END) AS failed \
         FROM transfer_jobs j LEFT JOIN transfer_tasks t ON t.job_id = j.id \
         WHERE j.id = ? GROUP BY j.id",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else { return Ok(false) };

    let state = JobState::from_str(&row.get::<String, _>("state"));
    let pending: i64 = row.get::<Option<i64>, _>("pending").unwrap_or(0);
    let failed: i64 = row.get::<Option<i64>, _>("failed").unwrap_or(0);

    let outcome = match state {
        JobState::Active if pending == 0 && failed > 0 => "failed",
        JobState::Active if pending == 0 => "done",
        JobState::Cancelled => "cancelled",
        _ => return Ok(false),
    };
    sqlx::query("UPDATE transfer_jobs SET state = ? WHERE id = ?")
        .bind(outcome)
        .bind(job_id)
        .execute(pool)
        .await?;
    finalize_history(pool, job_id).await?;
    Ok(true)
}

/// A job's settled state and source path (move-semantics cleanup).
pub(crate) async fn job_final(
    pool: &SqlitePool,
    job_id: i64,
) -> Result<Option<(JobState, String)>> {
    let row = sqlx::query("SELECT state, src FROM transfer_jobs WHERE id = ?")
        .bind(job_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| {
        (
            JobState::from_str(&row.get::<String, _>("state")),
            row.get("src"),
        )
    }))
}

/// Stamp `finished_at` and write the history row exactly once per outcome.
async fn finalize_history(pool: &SqlitePool, job_id: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    let stamped = sqlx::query(
        "UPDATE transfer_jobs SET finished_at = datetime('now') \
         WHERE id = ? AND finished_at IS NULL",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if stamped > 0 {
        sqlx::query(
            "INSERT INTO transfer_history \
               (job_id, src_site, dst_site, src, dst, outcome, files, bytes, duration_ms, error) \
             SELECT j.id, COALESCE(ss.name, '?'), COALESCE(ds.name, '?'), j.src, j.dst, j.state, \
               (SELECT COUNT(*) FROM transfer_tasks WHERE job_id = j.id AND state = 'done'), \
               COALESCE((SELECT SUM(byte_offset) FROM transfer_tasks WHERE job_id = j.id), 0), \
               CAST((julianday(j.finished_at) - julianday(COALESCE(j.started_at, j.created_at))) \
                    * 86400000 AS INTEGER), \
               j.error \
             FROM transfer_jobs j \
             LEFT JOIN sites ss ON ss.id = j.src_site_id \
             LEFT JOIN sites ds ON ds.id = j.dst_site_id \
             WHERE j.id = ?",
        )
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Drop terminal jobs from the queue view (history keeps the audit trail).
/// Returns the removed job ids so the engine can drop their runtime state.
pub(crate) async fn clear_finished(pool: &SqlitePool) -> Result<Vec<i64>> {
    let rows = sqlx::query(
        "DELETE FROM transfer_jobs WHERE state IN ('done', 'failed', 'cancelled') RETURNING id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}

/// Drop one terminal job from the queue view (history keeps the audit trail).
pub(crate) async fn remove_job(pool: &SqlitePool, job_id: i64) -> Result<bool> {
    let removed = sqlx::query(
        "DELETE FROM transfer_jobs \
         WHERE id = ? AND state IN ('done', 'failed', 'cancelled')",
    )
    .bind(job_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(removed > 0)
}

/// Set a job's scheduling priority (claim order is `priority DESC, id`).
pub(crate) async fn set_job_priority(pool: &SqlitePool, job_id: i64, priority: i64) -> Result<()> {
    sqlx::query("UPDATE transfer_jobs SET priority = ? WHERE id = ?")
        .bind(priority)
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Pause every job that can be paused; returns the affected ids so the
/// engine can flip their runtime flags too.
pub(crate) async fn pause_all_jobs(pool: &SqlitePool) -> Result<Vec<i64>> {
    let rows = sqlx::query(
        "UPDATE transfer_jobs SET state = 'paused' \
         WHERE state IN ('queued', 'active') RETURNING id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}

/// Resume every paused job; returns the affected ids.
pub(crate) async fn resume_all_jobs(pool: &SqlitePool) -> Result<Vec<i64>> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE transfer_jobs SET state = 'queued' WHERE state = 'paused' RETURNING id",
    )
    .fetch_all(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE transfer_tasks SET state = 'queued', updated_at = datetime('now') \
         WHERE state = 'paused' AND job_id IN \
           (SELECT id FROM transfer_jobs WHERE state = 'queued')",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}

/// Per-file tasks of one job for the drill-down view: problems first, then
/// running, queued, and finally settled files.
pub(crate) async fn job_tasks(
    pool: &SqlitePool,
    job_id: i64,
    limit: i64,
) -> Result<Vec<TaskSnapshot>> {
    let rows = sqlx::query(
        "SELECT src, state, byte_offset, total_bytes, error FROM transfer_tasks \
         WHERE job_id = ? \
         ORDER BY CASE state \
             WHEN 'failed' THEN 0 WHEN 'conflict' THEN 1 WHEN 'active' THEN 2 \
             WHEN 'paused' THEN 3 WHEN 'queued' THEN 4 ELSE 5 END, id \
         LIMIT ?",
    )
    .bind(job_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| TaskSnapshot {
            src: row.get("src"),
            state: row.get("state"),
            bytes_done: row.get::<i64, _>("byte_offset") as u64,
            total_bytes: row.get::<Option<i64>, _>("total_bytes").map(|b| b as u64),
            error: row.get("error"),
        })
        .collect())
}

/// Parked conflicts of one job, for the resolve dialog's comparison table.
pub(crate) async fn job_conflicts(
    pool: &SqlitePool,
    job_id: i64,
    limit: i64,
) -> Result<Vec<ConflictInfo>> {
    let rows = sqlx::query(
        "SELECT id, dst, total_bytes, src_modified, dst_size, dst_modified FROM transfer_tasks \
         WHERE job_id = ? AND state = 'conflict' ORDER BY id LIMIT ?",
    )
    .bind(job_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| ConflictInfo {
            id: row.get("id"),
            dst: row.get("dst"),
            src_size: row.get::<Option<i64>, _>("total_bytes").map(|b| b as u64),
            src_modified: row.get("src_modified"),
            dst_size: row.get::<Option<i64>, _>("dst_size").map(|b| b as u64),
            dst_modified: row.get("dst_modified"),
        })
        .collect())
}

/// The audit trail, newest first.
pub(crate) async fn history(pool: &SqlitePool, limit: i64) -> Result<Vec<HistoryEntry>> {
    let rows = sqlx::query(
        "SELECT src_site, dst_site, src, dst, outcome, files, bytes, duration_ms, error, \
           finished_at \
         FROM transfer_history ORDER BY id DESC LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| HistoryEntry {
            src_site: row.get("src_site"),
            dst_site: row.get("dst_site"),
            src: row.get("src"),
            dst: row.get("dst"),
            outcome: row.get("outcome"),
            files: row.get("files"),
            bytes: row.get::<i64, _>("bytes") as u64,
            duration_ms: row.get("duration_ms"),
            error: row.get("error"),
            finished_at: row.get("finished_at"),
        })
        .collect())
}

/// Everything the UI shows, in one aggregate query (newest job first).
/// `bps` is filled in by the engine.
pub(crate) async fn snapshot_jobs(pool: &SqlitePool) -> Result<Vec<JobSnapshot>> {
    let rows = sqlx::query(
        "SELECT j.id, j.src_site_id, j.dst_site_id, j.src, j.dst, j.state, j.error, \
           j.priority, j.move_source, \
           COALESCE(ss.name, 'Local') AS src_site, COALESCE(ds.name, 'Local') AS dst_site, \
           COUNT(t.id) AS files_total, \
           COALESCE(SUM(CASE WHEN t.state = 'done' THEN 1 ELSE 0 END), 0) AS files_done, \
           COALESCE(SUM(CASE WHEN t.state = 'skipped' THEN 1 ELSE 0 END), 0) AS files_skipped, \
           COALESCE(SUM(CASE WHEN t.state = 'conflict' THEN 1 ELSE 0 END), 0) AS files_conflict, \
           COALESCE(SUM(CASE WHEN t.state != 'skipped' THEN COALESCE(t.total_bytes, 0) ELSE 0 END), 0) AS bytes_total, \
           COALESCE(SUM(t.byte_offset), 0) AS bytes_done \
         FROM transfer_jobs j \
         LEFT JOIN transfer_tasks t ON t.job_id = j.id \
         LEFT JOIN sites ss ON ss.id = j.src_site_id \
         LEFT JOIN sites ds ON ds.id = j.dst_site_id \
         GROUP BY j.id ORDER BY j.id DESC LIMIT 500",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| JobSnapshot {
            id: row.get("id"),
            src_site_id: row.get("src_site_id"),
            dst_site_id: row.get("dst_site_id"),
            src_site: row.get("src_site"),
            dst_site: row.get("dst_site"),
            src: row.get("src"),
            dst: row.get("dst"),
            state: JobState::from_str(&row.get::<String, _>("state")),
            error: row.get("error"),
            files_total: row.get::<i64, _>("files_total") as u64,
            files_done: row.get::<i64, _>("files_done") as u64,
            files_skipped: row.get::<i64, _>("files_skipped") as u64,
            files_conflict: row.get::<i64, _>("files_conflict") as u64,
            bytes_total: row.get::<i64, _>("bytes_total") as u64,
            bytes_done: row.get::<i64, _>("bytes_done") as u64,
            bps: 0,
            priority: row.get("priority"),
            move_source: row.get::<i64, _>("move_source") != 0,
        })
        .collect())
}

/// Used by tests and diagnostics: per-state task counts for one job.
#[allow(dead_code)]
pub(crate) async fn task_states(pool: &SqlitePool, job_id: i64) -> Result<HashMap<String, i64>> {
    let rows = sqlx::query(
        "SELECT state, COUNT(*) AS n FROM transfer_tasks WHERE job_id = ? GROUP BY state",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("state"), row.get("n")))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn late_conflicts_follow_the_latest_rule() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let job = super::super::NewJob {
            src_site_id: 0,
            dst_site_id: 0,
            src: "source/file".into(),
            dst_dir: "target/".into(),
            dst_name: None,
            conflict_policy: ConflictPolicy::Ask,
            move_source: false,
        };
        let id = enqueue_job(&pool, &job).await.unwrap();
        begin_expansion(&pool, id).await.unwrap();
        insert_tasks(
            &pool,
            id,
            &[NewTask {
                src: job.src,
                dst: "target/file".into(),
                total_bytes: Some(3),
                src_etag: None,
            }],
        )
        .await
        .unwrap();
        finish_expansion(&pool, id).await.unwrap();
        let task = claim_task(&pool, &[id]).await.unwrap().unwrap();
        resolve_job_conflicts(&pool, id, ConflictPolicy::Overwrite)
            .await
            .unwrap();
        park_task_conflict(&pool, task.id, task.conflict_policy, 3, None)
            .await
            .unwrap();
        let task = claim_task(&pool, &[id]).await.unwrap().unwrap();
        assert_eq!(task.conflict_policy, ConflictPolicy::Overwrite);

        park_task_conflict(&pool, task.id, task.conflict_policy, 3, None)
            .await
            .unwrap();
        resolve_task_conflict(&pool, task.id, ConflictPolicy::OverwriteNewer)
            .await
            .unwrap();
        let task = claim_task(&pool, &[id]).await.unwrap().unwrap();
        park_task_conflict(&pool, task.id, task.conflict_policy, 3, None)
            .await
            .unwrap();
        resolve_job_conflicts(&pool, id, ConflictPolicy::Skip)
            .await
            .unwrap();
        let task = claim_task(&pool, &[id]).await.unwrap().unwrap();
        assert_eq!(task.conflict_policy, ConflictPolicy::Skip);
    }
}
