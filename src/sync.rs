//! Directory synchronization: a diff pass between two directories on any two
//! sites, feeding the existing transfer queue. The planner walks both trees,
//! compares files by relative path (modified time with slack for backends
//! with coarse clocks, then size), and produces a reviewable plan. Applying
//! the reviewed selection copies every file as one queue job and, in mirror
//! mode, deletes extraneous destination entries once that job succeeded.

use std::collections::BTreeMap;

use anyhow::Result;

use crate::queue::{BatchFile, ConflictPolicy, JobOutcome, JobState, NewBatch, QueueHandle};
use crate::provider::Provider;

/// Clock slack when comparing modified times across backends (FAT, S3 and
/// friends round or re-stamp timestamps).
const MTIME_SLACK_SECS: i64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Copy new and changed files; never delete.
    Update,
    /// Update, then delete destination entries the source doesn't have.
    Mirror,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanKind {
    /// New at the destination (`is_dir` = empty directory to create).
    Copy,
    /// The source is newer, or differs in size without the destination
    /// being newer; overwrite.
    Update,
    /// The destination changed after the source; overwriting would lose it.
    DestNewer,
    /// Same size but a side has no modified time, so equality is unproven.
    Unsure,
    /// Extraneous at the destination (mirror only); topmost entries only.
    Delete,
}

impl PlanKind {
    /// Review order.
    pub const ALL: [PlanKind; 5] = [
        PlanKind::Copy,
        PlanKind::Update,
        PlanKind::DestNewer,
        PlanKind::Unsure,
        PlanKind::Delete,
    ];

    /// Whether the review ticks this kind until the user says otherwise.
    /// Anything that could lose destination data must be opted into.
    pub fn selected_by_default(self) -> bool {
        matches!(self, PlanKind::Copy | PlanKind::Update | PlanKind::Delete)
    }
}

#[derive(Debug, Clone)]
pub struct PlanItem {
    /// Path relative to the compared roots (directories end with `/`).
    pub rel: String,
    pub is_dir: bool,
    pub kind: PlanKind,
    pub size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SyncPlan {
    pub items: Vec<PlanItem>,
}

impl SyncPlan {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Clone, Copy)]
struct FileMeta {
    is_dir: bool,
    size: u64,
    modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// Recursively list `root`, keyed by path relative to it (dirs end `/`).
/// Uses the provider's own per-directory listing so bucket-root S3 sites and
/// stat-completed fs listings behave exactly like the browser pane. The
/// queue's own partial files are not part of either tree.
async fn walk(provider: &Provider, root: &str) -> Result<BTreeMap<String, FileMeta>> {
    let mut out = BTreeMap::new();
    let mut pending = vec![root.to_string()];
    while let Some(dir) = pending.pop() {
        for entry in provider.list(&dir).await? {
            let Some(rel) = entry.path.strip_prefix(root) else {
                continue;
            };
            if rel.is_empty() || (!entry.is_dir && rel.ends_with(crate::queue::PARTIAL_SUFFIX)) {
                continue;
            }
            out.insert(
                rel.to_string(),
                FileMeta {
                    is_dir: entry.is_dir,
                    size: entry.size,
                    modified: entry.modified,
                },
            );
            if entry.is_dir {
                pending.push(entry.path.clone());
            }
        }
    }
    Ok(out)
}

/// What a file present on both sides needs; `None` when it is provably the
/// same. Times decide direction; size only matters when they don't.
fn compare(src: &FileMeta, dst: &FileMeta) -> Option<PlanKind> {
    match (src.modified, dst.modified) {
        (Some(src_time), Some(dst_time)) => {
            let ahead = (src_time - dst_time).num_seconds();
            if ahead > MTIME_SLACK_SECS {
                Some(PlanKind::Update)
            } else if ahead < -MTIME_SLACK_SECS {
                Some(PlanKind::DestNewer)
            } else {
                (src.size != dst.size).then_some(PlanKind::Update)
            }
        }
        _ if src.size != dst.size => Some(PlanKind::Update),
        _ => Some(PlanKind::Unsure),
    }
}

/// Compare `src_dir` on `src` against `dst_dir` on `dst`. Both dirs are
/// browse paths (`""` = site root, otherwise ending with `/`).
pub async fn plan(
    src: &Provider,
    src_dir: &str,
    dst: &Provider,
    dst_dir: &str,
    mode: SyncMode,
) -> Result<SyncPlan> {
    let (src_tree, dst_tree) = tokio::try_join!(walk(src, src_dir), walk(dst, dst_dir))?;

    let mut items = Vec::new();
    for (rel, src_meta) in &src_tree {
        match dst_tree.get(rel) {
            None => {
                // New directories only matter when empty — files below them
                // create the hierarchy implicitly on every backend.
                let is_empty_dir = src_meta.is_dir
                    && !src_tree
                        .keys()
                        .any(|other| other != rel && other.starts_with(rel.as_str()));
                if !src_meta.is_dir || is_empty_dir {
                    items.push(PlanItem {
                        rel: rel.clone(),
                        is_dir: src_meta.is_dir,
                        kind: PlanKind::Copy,
                        size: src_meta.size,
                    });
                }
            }
            Some(dst_meta) => {
                if !src_meta.is_dir && !dst_meta.is_dir {
                    if let Some(kind) = compare(src_meta, dst_meta) {
                        items.push(PlanItem {
                            rel: rel.clone(),
                            is_dir: false,
                            kind,
                            size: src_meta.size,
                        });
                    }
                }
                // dir vs file type clashes fall through: mirror deletes the
                // destination side below, update mode leaves it alone.
            }
        }
    }

    if mode == SyncMode::Mirror {
        // Topmost extraneous entries only — deleting a directory removes its
        // children, so anything under an already-deleted prefix is skipped
        // (BTreeMap iterates in path order, parents before children).
        let mut deleted_prefixes: Vec<String> = Vec::new();
        for (rel, dst_meta) in &dst_tree {
            if deleted_prefixes
                .iter()
                .any(|prefix| rel.starts_with(prefix.as_str()))
            {
                continue;
            }
            let extraneous = match src_tree.get(rel) {
                None => true,
                // Type clash (file here, dir there): remove and let the copy
                // pass rebuild it on the next run.
                Some(src_meta) => src_meta.is_dir != dst_meta.is_dir,
            };
            if extraneous {
                if dst_meta.is_dir {
                    deleted_prefixes.push(rel.clone());
                }
                items.push(PlanItem {
                    rel: rel.clone(),
                    is_dir: dst_meta.is_dir,
                    kind: PlanKind::Delete,
                    size: dst_meta.size,
                });
            }
        }
    }

    Ok(SyncPlan { items })
}

/// The destination-side work of a reviewed plan, as full browse paths.
#[derive(Debug, Clone, Default)]
pub struct SyncWork {
    pub copies: Vec<BatchFile>,
    /// Empty source directories to create at the destination.
    pub new_dirs: Vec<String>,
    /// `(path, is_dir)` extraneous destination entries.
    pub deletes: Vec<(String, bool)>,
}

impl SyncWork {
    /// The items whose `selected` flag is set, rooted at both directories.
    pub fn from_plan(plan: &SyncPlan, selected: &[bool], src_dir: &str, dst_dir: &str) -> Self {
        let mut work = Self::default();
        for (item, _) in plan.items.iter().zip(selected).filter(|(_, on)| **on) {
            let dst = format!("{dst_dir}{}", item.rel);
            match item.kind {
                PlanKind::Delete => work.deletes.push((dst, item.is_dir)),
                _ if item.is_dir => work.new_dirs.push(dst),
                _ => work.copies.push(BatchFile {
                    src: format!("{src_dir}{}", item.rel),
                    dst,
                    size: Some(item.size),
                }),
            }
        }
        work
    }
}

/// How a sync run ended, for the closing notification.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub copied: u64,
    pub deleted: u64,
    pub failed: u64,
    /// Deletes were held back because the copy phase didn't fully succeed.
    pub deletes_skipped: bool,
    /// The first failure, in plain words.
    pub reason: Option<String>,
}

impl SyncReport {
    fn fail(&mut self, count: u64, reason: String) {
        self.failed += count;
        self.reason.get_or_insert(reason);
    }
}

/// Run a reviewed sync: create empty directories and copy every file as one
/// queue job (labelled `src_label` → `dst_label`), wait for it to settle,
/// and only then delete. Any copy failure, cancellation or queue error
/// holds back every delete, so a mirror never removes files while the
/// destination is missing some of the source.
pub async fn run(
    queue: &QueueHandle,
    src_site_id: i64,
    dst: &Provider,
    src_label: String,
    dst_label: String,
    work: SyncWork,
) -> SyncReport {
    let mut report = SyncReport::default();
    let mut copies_ok = true;

    for dir in &work.new_dirs {
        if let Err(error) = dst.create_dir(dir).await {
            report.fail(1, crate::errors::humanize(&error).title);
        }
    }

    if !work.copies.is_empty() {
        let total = work.copies.len() as u64;
        let batch = NewBatch {
            src_site_id,
            dst_site_id: dst.site_id,
            src_label,
            dst_label,
            files: work.copies,
            conflict_policy: ConflictPolicy::Overwrite,
        };
        let settled = match queue.submit_batch(batch).await {
            Ok(job_id) => queue.settled(job_id).await,
            Err(error) => Err(error),
        };
        copies_ok = record_copies(&mut report, total, settled);
    }

    if !copies_ok || report.failed > 0 {
        report.deletes_skipped = !work.deletes.is_empty();
        return report;
    }
    for (path, is_dir) in &work.deletes {
        match dst.delete(path, *is_dir).await {
            Ok(()) => report.deleted += 1,
            Err(error) => report.fail(1, crate::errors::humanize(&error).title),
        }
    }
    report
}

/// Fold the copy job's outcome into the report. True only when the job
/// finished `Done` with every file copied; a cancelled job is never a
/// success, even with nothing missing.
fn record_copies(report: &mut SyncReport, total: u64, settled: Result<JobOutcome>) -> bool {
    match settled {
        Ok(outcome) => {
            report.copied = outcome.files_done;
            let missing = total.saturating_sub(outcome.files_done);
            let ok = outcome.state == JobState::Done && missing == 0;
            if !ok {
                let reason = outcome
                    .error
                    .unwrap_or_else(|| "The transfer was cancelled.".to_string());
                report.fail(missing, reason);
            }
            ok
        }
        Err(error) => {
            report.fail(total, crate::errors::humanize(&error).title);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_needs_proof_before_calling_files_equal() {
        use chrono::{TimeZone as _, Utc};
        let at = |secs| Utc.timestamp_opt(secs, 0).unwrap();
        let meta = |size, secs: Option<i64>| FileMeta {
            is_dir: false,
            size,
            modified: secs.map(at),
        };
        // Same size and time (within slack): nothing to do.
        assert_eq!(compare(&meta(1, Some(100)), &meta(1, Some(99))), None);
        // Newer source updates, whatever the sizes.
        assert_eq!(compare(&meta(1, Some(100)), &meta(1, Some(50))), Some(PlanKind::Update));
        // A newer destination is never silently overwritten.
        assert_eq!(compare(&meta(1, Some(50)), &meta(1, Some(100))), Some(PlanKind::DestNewer));
        assert_eq!(compare(&meta(1, Some(50)), &meta(2, Some(100))), Some(PlanKind::DestNewer));
        // Without times: a size difference is proof, equal size is not.
        assert_eq!(compare(&meta(1, None), &meta(2, Some(1))), Some(PlanKind::Update));
        assert_eq!(compare(&meta(1, None), &meta(1, None)), Some(PlanKind::Unsure));
        assert_eq!(compare(&meta(1, Some(1)), &meta(1, None)), Some(PlanKind::Unsure));
    }

    #[test]
    fn work_takes_only_selected_items() {
        let item = |rel: &str, is_dir, kind| PlanItem {
            rel: rel.to_string(),
            is_dir,
            kind,
            size: 3,
        };
        let plan = SyncPlan {
            items: vec![
                item("a/new.txt", false, PlanKind::Copy),
                item("theirs.txt", false, PlanKind::DestNewer),
                item("empty/", true, PlanKind::Copy),
                item("old/", true, PlanKind::Delete),
            ],
        };
        let work = SyncWork::from_plan(&plan, &[true, false, true, true], "s/", "d/");
        assert_eq!(
            work.copies,
            vec![BatchFile {
                src: "s/a/new.txt".into(),
                dst: "d/a/new.txt".into(),
                size: Some(3),
            }]
        );
        assert_eq!(work.new_dirs, vec!["d/empty/".to_string()]);
        assert_eq!(work.deletes, vec![("d/old/".to_string(), true)]);
    }

    #[test]
    fn a_cancelled_copy_holds_back_deletes_even_with_nothing_missing() {
        let mut report = SyncReport::default();
        let cancelled = JobOutcome {
            state: JobState::Cancelled,
            error: None,
            files_done: 2,
            files_failed: 0,
        };
        assert!(!record_copies(&mut report, 2, Ok(cancelled)));
        assert_eq!(report.copied, 2);
        assert!(report.reason.is_some());
    }

    #[test]
    fn walk_leaves_out_partial_files() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("kessel-sync-walk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("a.txt"), b"a").unwrap();
        std::fs::write(dir.join(".a.txt.3.kessel-part"), b"half").unwrap();
        std::fs::write(dir.join("sub/.b.txt.4.kessel-part"), b"half").unwrap();
        let local = crate::provider::SiteProfile {
            scheme: "fs".into(),
            options: [("root".to_string(), "/".to_string())].into(),
            ..Default::default()
        };
        let provider = Provider::connect(&local, &Default::default()).unwrap();
        let root = format!("{}/", dir.to_str().unwrap().trim_start_matches('/'));
        let tree = rt.block_on(walk(&provider, &root)).unwrap();
        assert_eq!(tree.keys().map(String::as_str).collect::<Vec<_>>(), ["a.txt", "sub/"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_failed_copy_holds_back_every_delete() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("kessel-sync-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::create_dir_all(dir.join("dst")).unwrap();
        std::fs::write(dir.join("dst/extra.txt"), b"keep me").unwrap();
        let browse = |rel: &str| format!("{}/{rel}", dir.to_str().unwrap().trim_start_matches('/'));

        let pool = rt.block_on(crate::db::connect(&dir)).unwrap();
        let cfg = crate::config::TransferConfig {
            max_parallel: 2,
            bandwidth_limit: 0,
            retries: 0,
            verify: true,
            preserve_timestamps: true,
            skip_hidden: false,
            default_conflict_policy: Default::default(),
        };
        let queue = crate::queue::start(&rt, pool, cfg, Default::default());
        let local = crate::provider::SiteProfile {
            scheme: "fs".into(),
            options: [("root".to_string(), "/".to_string())].into(),
            ..Default::default()
        };
        let dst = Provider::connect(&local, &Default::default()).unwrap();

        let work = SyncWork {
            // The source is missing, so the copy fails.
            copies: vec![BatchFile {
                src: browse("src/gone.txt"),
                dst: browse("dst/gone.txt"),
                size: Some(1),
            }],
            new_dirs: Vec::new(),
            deletes: vec![(browse("dst/extra.txt"), false)],
        };
        let report = rt.block_on(run(&queue, 0, &dst, "s".into(), "d".into(), work));
        assert_eq!((report.copied, report.deleted, report.failed), (0, 0, 1));
        assert!(report.deletes_skipped);
        assert!(report.reason.is_some());
        assert!(dir.join("dst/extra.txt").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
