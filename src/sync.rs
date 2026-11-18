//! Directory synchronization: a diff pass between two directories on any two
//! sites, feeding the existing transfer queue. The planner walks both trees,
//! compares files by relative path (modified time with slack for backends
//! with coarse clocks, then size), and produces a reviewable plan. Applying
//! the reviewed selection copies every file as one queue job and, in mirror
//! mode, deletes extraneous destination entries once that job succeeded.
//! Destination paths are re-checked right before they are overwritten or
//! deleted, so nothing that changed after the review is touched.

use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;

use crate::queue::{BatchFile, ConflictPolicy, JobOutcome, JobState, NewBatch, QueueHandle};
use crate::provider::Provider;

/// Clock slack when comparing modified times across backends (FAT, S3 and
/// friends round or re-stamp timestamps).
const MTIME_SLACK_SECS: i64 = 2;

/// Destination stats in flight at once while re-checking reviewed copies.
const RECHECK_CONCURRENCY: usize = 16;

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
    /// Extraneous at the destination (mirror only). Folders and their
    /// contents are separate rows, so a delete only covers what was listed.
    Delete,
    /// The two sides hold different kinds of entry at this path (file,
    /// folder or folder link). Shown for the user to resolve, never applied.
    Conflict,
}

impl PlanKind {
    /// Review order.
    pub const ALL: [PlanKind; 6] = [
        PlanKind::Copy,
        PlanKind::Update,
        PlanKind::DestNewer,
        PlanKind::Unsure,
        PlanKind::Delete,
        PlanKind::Conflict,
    ];

    /// Whether the review ticks this kind until the user says otherwise.
    /// Anything that could lose destination data must be opted into.
    pub fn selected_by_default(self) -> bool {
        matches!(self, PlanKind::Copy | PlanKind::Update)
    }
}

/// A destination file as the review saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewedFile {
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

impl ReviewedFile {
    /// Listings and stats can report a time at different precision, so
    /// times get the same slack as the comparison itself.
    fn matches(&self, size: u64, modified: Option<DateTime<Utc>>) -> bool {
        let same_time = match (self.modified, modified) {
            (Some(reviewed), Some(now)) => (reviewed - now).num_seconds().abs() <= MTIME_SLACK_SECS,
            _ => true,
        };
        self.size == size && same_time
    }
}

#[derive(Debug, Clone)]
pub struct PlanItem {
    /// Path relative to the compared roots (directories end with `/`).
    pub rel: String,
    pub is_dir: bool,
    pub kind: PlanKind,
    pub size: u64,
    /// The destination file this item acts on; `None` when the review saw
    /// no file there (new paths and folders).
    pub reviewed: Option<ReviewedFile>,
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
    modified: Option<DateTime<Utc>>,
    /// A link to a folder: recorded, but never entered, copied or deleted.
    dir_link: bool,
}

impl FileMeta {
    fn reviewed(&self) -> ReviewedFile {
        ReviewedFile {
            size: self.size,
            modified: self.modified,
        }
    }
}

/// Recursively list `root`, keyed by path relative to it (dirs end `/`).
/// Uses the provider's own per-directory listing so bucket-root S3 sites and
/// stat-completed fs listings behave exactly like the browser pane. The
/// queue's own partial files are not part of either tree. Folder links are
/// not followed, so a scan stays inside `root` and always ends.
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
            let dir_link = entry.is_dir && entry.is_symlink;
            out.insert(
                rel.to_string(),
                FileMeta {
                    is_dir: entry.is_dir,
                    size: entry.size,
                    modified: entry.modified,
                    dir_link,
                },
            );
            if entry.is_dir && !dir_link {
                pending.push(entry.path.clone());
            }
        }
    }
    Ok(out)
}

/// Whether copies to `provider` can carry the source's modified time (see
/// [`Provider::set_mtime`]). Elsewhere the upload time replaces it.
fn keeps_source_mtime(provider: &Provider) -> bool {
    matches!(provider.scheme.as_str(), "fs" | "sftp")
}

/// What a file present on both sides needs; `None` when it is provably the
/// same. Times decide direction; size only matters when they don't. A
/// destination that stamps uploads with their own time is always newer after
/// a copy, so there a newer file of the same size counts as equal.
fn compare(src: &FileMeta, dst: &FileMeta, dst_keeps_mtime: bool) -> Option<PlanKind> {
    match (src.modified, dst.modified) {
        (Some(src_time), Some(dst_time)) => {
            let ahead = (src_time - dst_time).num_seconds();
            if ahead > MTIME_SLACK_SECS {
                Some(PlanKind::Update)
            } else if ahead < -MTIME_SLACK_SECS {
                (dst_keeps_mtime || src.size != dst.size).then_some(PlanKind::DestNewer)
            } else {
                (src.size != dst.size).then_some(PlanKind::Update)
            }
        }
        _ if src.size != dst.size => Some(PlanKind::Update),
        _ => Some(PlanKind::Unsure),
    }
}

/// Where a browse directory really lives, comparable across bookmarks of
/// one store: a location inside another starts with that location's text.
async fn location(provider: &Provider, dir: &str) -> String {
    let scheme = &provider.scheme;
    // Only the account root of a bucket-root S3 site has no operator.
    let Ok((op, rel)) = provider.op_for(dir) else {
        return format!("{scheme}://");
    };
    let info = op.info();
    if scheme == "fs" {
        let full = crate::localfs::full_path(&info.root(), &rel);
        let real = tokio::fs::canonicalize(&full).await.unwrap_or(full);
        let path = real.to_string_lossy().replace('\\', "/");
        return format!("fs://{}/", path.trim_end_matches('/'));
    }
    let parts = [info.name(), info.root(), rel];
    let path: Vec<&str> = parts.iter().map(|part| part.trim_matches('/')).filter(|part| !part.is_empty()).collect();
    format!("{scheme}://{}/", path.join("/"))
}

/// Whether one location is the other or lies inside it.
fn overlapping(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// Whether `rel`, or a folder above it, sits on a conflicting path.
fn is_blocked(rel: &str, blocked: &HashSet<String>) -> bool {
    let key = format!("{}/", rel.trim_end_matches('/'));
    key.match_indices('/').any(|(ix, _)| blocked.contains(&key[..=ix]))
}

/// One row per path the two sides hold as different kinds of entry, plus
/// those paths (as `name/`) so nothing at or below them is planned.
fn conflicts(
    src_tree: &BTreeMap<String, FileMeta>,
    dst_tree: &BTreeMap<String, FileMeta>,
) -> (Vec<PlanItem>, HashSet<String>) {
    let mut items = Vec::new();
    let mut blocked = HashSet::new();
    for (rel, src_meta) in src_tree {
        // The same name as the other kind of entry is keyed differently.
        let twin = match rel.strip_suffix('/') {
            Some(file) => file.to_string(),
            None => format!("{rel}/"),
        };
        let clash = dst_tree.contains_key(&twin)
            || dst_tree.get(rel).is_some_and(|dst| dst.dir_link != src_meta.dir_link);
        if clash && !is_blocked(rel, &blocked) {
            let name = rel.trim_end_matches('/');
            blocked.insert(format!("{name}/"));
            items.push(PlanItem {
                rel: name.to_string(),
                is_dir: false,
                kind: PlanKind::Conflict,
                size: 0,
                reviewed: None,
            });
        }
    }
    (items, blocked)
}

/// Compare `src_dir` on `src` against `dst_dir` on `dst`. Both dirs are
/// browse paths (`""` = site root, otherwise ending with `/`). Roots that
/// overlap are refused: the destination would contain the source, and a
/// mirror could delete it.
pub async fn plan(
    src: &Provider,
    src_dir: &str,
    dst: &Provider,
    dst_dir: &str,
    mode: SyncMode,
) -> Result<SyncPlan> {
    let (src_root, dst_root) = tokio::join!(location(src, src_dir), location(dst, dst_dir));
    anyhow::ensure!(!overlapping(&src_root, &dst_root), "{}", crate::i18n::t("sync.overlap"));

    let (src_tree, dst_tree) = tokio::try_join!(walk(src, src_dir), walk(dst, dst_dir))?;
    let dst_keeps_mtime = keeps_source_mtime(dst);

    let (mut items, blocked) = conflicts(&src_tree, &dst_tree);
    let mut entries = src_tree.iter().peekable();
    while let Some((rel, src_meta)) = entries.next() {
        // Path order puts a folder's contents right after the folder.
        let has_children = entries.peek().is_some_and(|(next, _)| next.starts_with(rel.as_str()));
        if src_meta.dir_link || is_blocked(rel, &blocked) {
            continue;
        }
        match dst_tree.get(rel) {
            None => {
                // New directories only matter when empty: files below them
                // create the hierarchy implicitly on every backend.
                if !src_meta.is_dir || !has_children {
                    items.push(PlanItem {
                        rel: rel.clone(),
                        is_dir: src_meta.is_dir,
                        kind: PlanKind::Copy,
                        size: src_meta.size,
                        reviewed: None,
                    });
                }
            }
            Some(dst_meta) => {
                if !src_meta.is_dir && !dst_meta.is_dir {
                    if let Some(kind) = compare(src_meta, dst_meta, dst_keeps_mtime) {
                        items.push(PlanItem {
                            rel: rel.clone(),
                            is_dir: false,
                            kind,
                            size: src_meta.size,
                            reviewed: Some(dst_meta.reviewed()),
                        });
                    }
                }
            }
        }
    }

    if mode == SyncMode::Mirror {
        for (rel, dst_meta) in &dst_tree {
            if dst_meta.dir_link || src_tree.contains_key(rel) || is_blocked(rel, &blocked) {
                continue;
            }
            items.push(PlanItem {
                rel: rel.clone(),
                is_dir: dst_meta.is_dir,
                kind: PlanKind::Delete,
                size: dst_meta.size,
                reviewed: (!dst_meta.is_dir).then(|| dst_meta.reviewed()),
            });
        }
    }

    Ok(SyncPlan { items })
}

/// A reviewed copy and what its destination held at review time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewedCopy {
    pub file: BatchFile,
    pub reviewed: Option<ReviewedFile>,
}

/// A reviewed extraneous destination entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewedDelete {
    pub path: String,
    pub is_dir: bool,
    pub reviewed: Option<ReviewedFile>,
}

/// The destination-side work of a reviewed plan, as full browse paths.
#[derive(Debug, Clone, Default)]
pub struct SyncWork {
    pub copies: Vec<ReviewedCopy>,
    /// Empty source directories to create at the destination.
    pub new_dirs: Vec<String>,
    /// Extraneous destination entries, parents before their contents.
    pub deletes: Vec<ReviewedDelete>,
}

impl SyncWork {
    /// The items whose `selected` flag is set, rooted at both directories.
    /// Conflicts are never work, whatever their flag says.
    pub fn from_plan(plan: &SyncPlan, selected: &[bool], src_dir: &str, dst_dir: &str) -> Self {
        let mut work = Self::default();
        for (item, _) in plan.items.iter().zip(selected).filter(|(_, on)| **on) {
            let dst = format!("{dst_dir}{}", item.rel);
            match item.kind {
                PlanKind::Conflict => {}
                PlanKind::Delete => work.deletes.push(ReviewedDelete {
                    path: dst,
                    is_dir: item.is_dir,
                    reviewed: item.reviewed,
                }),
                _ if item.is_dir => work.new_dirs.push(dst),
                _ => work.copies.push(ReviewedCopy {
                    file: BatchFile {
                        src: format!("{src_dir}{}", item.rel),
                        dst,
                        size: Some(item.size),
                    },
                    reviewed: item.reviewed,
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
    /// Paths left alone because they changed after the review.
    pub skipped: u64,
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

/// Whether the destination file at `path` is still what the review saw
/// (`None` = nothing there).
async fn still_as_reviewed(dst: &Provider, path: &str, reviewed: Option<ReviewedFile>) -> Result<bool> {
    let (op, rel) = dst.op_for(path)?;
    match op.stat(&rel).await {
        Ok(meta) => {
            let modified = meta.last_modified().and_then(crate::provider::to_datetime);
            Ok(!meta.is_dir() && reviewed.is_some_and(|file| file.matches(meta.content_length(), modified)))
        }
        Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(reviewed.is_none()),
        Err(error) => Err(error.into()),
    }
}

/// The copies whose destination still matches the review. The rest are
/// counted as skipped, or as failed when the check itself failed.
async fn recheck_copies(dst: &Provider, copies: Vec<ReviewedCopy>, report: &mut SyncReport) -> Vec<BatchFile> {
    let checked: Vec<_> = futures::stream::iter(copies)
        .map(|copy| async move {
            let unchanged = still_as_reviewed(dst, &copy.file.dst, copy.reviewed).await;
            (copy.file, unchanged)
        })
        .buffered(RECHECK_CONCURRENCY)
        .collect()
        .await;
    let mut files = Vec::with_capacity(checked.len());
    for (file, unchanged) in checked {
        match unchanged {
            Ok(true) => files.push(file),
            Ok(false) => report.skipped += 1,
            Err(error) => report.fail(1, crate::errors::humanize(&error).title),
        }
    }
    files
}

/// Delete one reviewed entry if it is still as reviewed; false when it was
/// left alone. Folders are only removed once empty, so anything that
/// appeared in them after the review survives.
async fn delete_reviewed(dst: &Provider, entry: &ReviewedDelete) -> Result<bool> {
    if !entry.is_dir {
        if !still_as_reviewed(dst, &entry.path, entry.reviewed).await? {
            return Ok(false);
        }
    } else {
        let (op, rel) = dst.op_for(&entry.path)?;
        match op.stat(&rel).await {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return Ok(false),
            // A virtual folder disappears with its last object.
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {
                return Ok(dst.caps.virtual_dirs);
            }
            Err(error) => return Err(error.into()),
        }
        if !dst.list(&entry.path).await?.is_empty() {
            return Ok(false);
        }
    }
    dst.delete(&entry.path, false).await?;
    Ok(true)
}

/// Run a reviewed sync: create empty directories and copy every file as one
/// queue job (labelled `src_label` → `dst_label`), wait for it to settle,
/// and only then delete. Any copy failure, cancellation or queue error
/// holds back every delete, so a mirror never removes files while the
/// destination is missing some of the source. Paths that changed after the
/// review are skipped and counted in the report.
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

    let copies = recheck_copies(dst, work.copies, &mut report).await;
    if !copies.is_empty() {
        let total = copies.len() as u64;
        let batch = NewBatch {
            src_site_id,
            dst_site_id: dst.site_id,
            src_label,
            dst_label,
            files: copies,
            conflict_policy: ConflictPolicy::Overwrite,
            move_source: false,
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
    // Contents first, so reviewed folders are empty when their turn comes.
    for entry in work.deletes.iter().rev() {
        match delete_reviewed(dst, entry).await {
            Ok(true) => report.deleted += 1,
            Ok(false) => report.skipped += 1,
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
            dir_link: false,
        };
        let compare = |src: &FileMeta, dst: &FileMeta| compare(src, dst, true);
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
            reviewed: None,
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
            vec![ReviewedCopy {
                file: BatchFile {
                    src: "s/a/new.txt".into(),
                    dst: "d/a/new.txt".into(),
                    size: Some(3),
                },
                reviewed: None,
            }]
        );
        assert_eq!(work.new_dirs, vec!["d/empty/".to_string()]);
        assert_eq!(
            work.deletes,
            vec![ReviewedDelete {
                path: "d/old/".to_string(),
                is_dir: true,
                reviewed: None,
            }]
        );
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

    /// A provider over the whole local filesystem and a fresh temp dir with
    /// `src/` and `dst/` inside.
    fn local_fixture(tag: &str) -> (tokio::runtime::Runtime, std::path::PathBuf, Provider) {
        let dir = std::env::temp_dir().join(format!("kessel-sync-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::create_dir_all(dir.join("dst")).unwrap();
        let dir = dir.canonicalize().unwrap();
        let local = crate::provider::SiteProfile {
            scheme: "fs".into(),
            options: [("root".to_string(), "/".to_string())].into(),
            ..Default::default()
        };
        let provider = Provider::connect(&local, &Default::default()).unwrap();
        (tokio::runtime::Runtime::new().unwrap(), dir, provider)
    }

    fn browse_dir(dir: &std::path::Path, rel: &str) -> String {
        format!("{}/{rel}", dir.to_str().unwrap().trim_start_matches('/'))
    }

    #[test]
    fn overlapping_roots_are_refused_in_either_direction() {
        let (rt, dir, provider) = local_fixture("overlap");
        std::fs::create_dir_all(dir.join("dst/inner")).unwrap();
        std::fs::create_dir_all(dir.join("dst-sibling")).unwrap();
        let outer = browse_dir(&dir, "dst/");
        let inner = browse_dir(&dir, "dst/inner/");
        let sibling = browse_dir(&dir, "dst-sibling/");
        let run = |src: &str, dst: &str| rt.block_on(plan(&provider, src, &provider, dst, SyncMode::Mirror));

        assert!(run(&inner, &outer).is_err());
        assert!(run(&outer, &inner).is_err());
        assert!(run(&outer, &outer).is_err());
        // A shared name prefix is not nesting.
        assert!(run(&outer, &sibling).is_ok());

        // A second bookmark rooted elsewhere reaches the same folder.
        let rooted = crate::provider::SiteProfile {
            scheme: "fs".into(),
            options: [("root".to_string(), dir.join("dst").to_str().unwrap().to_string())].into(),
            ..Default::default()
        };
        let rooted = Provider::connect(&rooted, &Default::default()).unwrap();
        assert!(rt.block_on(plan(&provider, &inner, &rooted, "", SyncMode::Mirror)).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn walk_does_not_enter_folder_links() {
        let (rt, dir, provider) = local_fixture("links");
        std::fs::create_dir_all(dir.join("outside")).unwrap();
        std::fs::write(dir.join("outside/secret.txt"), b"x").unwrap();
        std::fs::write(dir.join("src/a.txt"), b"a").unwrap();
        std::os::unix::fs::symlink(dir.join("outside"), dir.join("src/away")).unwrap();
        std::os::unix::fs::symlink(dir.join("src"), dir.join("src/loop")).unwrap();

        let tree = rt.block_on(walk(&provider, &browse_dir(&dir, "src/"))).unwrap();
        assert_eq!(tree.keys().map(String::as_str).collect::<Vec<_>>(), ["a.txt", "away/", "loop/"]);
        assert!(tree["away/"].dir_link && tree["loop/"].dir_link);

        // Linked folders are neither copied nor mirrored away.
        let plan = rt
            .block_on(plan(&provider, &browse_dir(&dir, "src/"), &provider, &browse_dir(&dir, "dst/"), SyncMode::Mirror))
            .unwrap();
        assert_eq!(plan.items.iter().map(|item| item.rel.as_str()).collect::<Vec<_>>(), ["a.txt"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn deletions_start_unticked() {
        assert!(!PlanKind::Delete.selected_by_default());
        assert!(!PlanKind::Conflict.selected_by_default());
        assert!(PlanKind::Copy.selected_by_default());
    }

    #[test]
    fn apply_leaves_alone_what_changed_after_the_review() {
        let (rt, dir, provider) = local_fixture("recheck");
        std::fs::write(dir.join("src/edited.txt"), b"source").unwrap();
        std::fs::write(dir.join("dst/edited.txt"), b"old").unwrap();
        std::fs::write(dir.join("dst/stale.txt"), b"old").unwrap();
        std::fs::write(dir.join("dst/gone.txt"), b"old").unwrap();
        std::fs::create_dir_all(dir.join("dst/old")).unwrap();
        std::fs::write(dir.join("dst/old/listed.txt"), b"old").unwrap();
        std::fs::create_dir_all(dir.join("dst/clean")).unwrap();
        std::fs::write(dir.join("dst/clean/listed.txt"), b"old").unwrap();
        let (src_dir, dst_dir) = (browse_dir(&dir, "src/"), browse_dir(&dir, "dst/"));

        let reviewed = rt
            .block_on(plan(&provider, &src_dir, &provider, &dst_dir, SyncMode::Mirror))
            .unwrap();
        let selected = vec![true; reviewed.items.len()];
        let work = SyncWork::from_plan(&reviewed, &selected, &src_dir, &dst_dir);

        // After the review: two files change and a folder gains a child.
        std::fs::write(dir.join("dst/edited.txt"), b"changed there").unwrap();
        std::fs::write(dir.join("dst/stale.txt"), b"changed there").unwrap();
        std::fs::remove_file(dir.join("dst/gone.txt")).unwrap();
        std::fs::write(dir.join("dst/old/new.txt"), b"new").unwrap();

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
        let report = rt.block_on(run(&queue, 0, &provider, "s".into(), "d".into(), work));

        assert_eq!(std::fs::read(dir.join("dst/edited.txt")).unwrap(), b"changed there");
        assert_eq!(std::fs::read(dir.join("dst/stale.txt")).unwrap(), b"changed there");
        assert!(dir.join("dst/old/new.txt").exists());
        assert!(!dir.join("dst/old/listed.txt").exists());
        assert!(!dir.join("dst/clean").exists());
        // Deleted: both listed files and the folder left empty. Skipped:
        // edited.txt, stale.txt, gone.txt and the folder that gained a child.
        assert_eq!((report.copied, report.deleted, report.failed, report.skipped), (0, 3, 0, 4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_file_and_folder_at_one_path_is_a_conflict_that_is_never_applied() {
        let (rt, dir, provider) = local_fixture("clash");
        std::fs::write(dir.join("src/both"), b"file").unwrap();
        std::fs::create_dir_all(dir.join("dst/both")).unwrap();
        std::fs::write(dir.join("dst/both/child.txt"), b"keep").unwrap();
        let (src_dir, dst_dir) = (browse_dir(&dir, "src/"), browse_dir(&dir, "dst/"));

        let plan = rt
            .block_on(plan(&provider, &src_dir, &provider, &dst_dir, SyncMode::Mirror))
            .unwrap();
        assert_eq!(plan.items.len(), 1);
        assert_eq!((plan.items[0].rel.as_str(), plan.items[0].kind), ("both", PlanKind::Conflict));

        let work = SyncWork::from_plan(&plan, &[true], &src_dir, &dst_dir);
        assert!(work.copies.is_empty() && work.new_dirs.is_empty() && work.deletes.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_newer_destination_that_cannot_keep_times_counts_as_equal_at_the_same_size() {
        use chrono::{TimeZone as _, Utc};
        let meta = |size, secs| FileMeta {
            is_dir: false,
            size,
            modified: Some(Utc.timestamp_opt(secs, 0).unwrap()),
            dir_link: false,
        };
        assert_eq!(compare(&meta(5, 100), &meta(5, 900), false), None);
        assert_eq!(compare(&meta(5, 100), &meta(6, 900), false), Some(PlanKind::DestNewer));
        assert_eq!(compare(&meta(5, 900), &meta(5, 100), false), Some(PlanKind::Update));
        assert_eq!(compare(&meta(5, 100), &meta(5, 900), true), Some(PlanKind::DestNewer));
    }

    #[test]
    fn only_new_folders_without_contents_become_rows() {
        let (rt, dir, provider) = local_fixture("empty");
        std::fs::create_dir_all(dir.join("src/empty")).unwrap();
        std::fs::create_dir_all(dir.join("src/full/deep")).unwrap();
        std::fs::write(dir.join("src/full/deep/a.txt"), b"a").unwrap();
        let plan = rt
            .block_on(plan(&provider, &browse_dir(&dir, "src/"), &provider, &browse_dir(&dir, "dst/"), SyncMode::Update))
            .unwrap();
        assert_eq!(
            plan.items.iter().map(|item| item.rel.as_str()).collect::<Vec<_>>(),
            ["empty/", "full/deep/a.txt"]
        );
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
            copies: vec![ReviewedCopy {
                file: BatchFile {
                    src: browse("src/gone.txt"),
                    dst: browse("dst/gone.txt"),
                    size: Some(1),
                },
                reviewed: None,
            }],
            new_dirs: Vec::new(),
            deletes: vec![ReviewedDelete {
                path: browse("dst/extra.txt"),
                is_dir: false,
                reviewed: Some(ReviewedFile { size: 7, modified: None }),
            }],
        };
        let report = rt.block_on(run(&queue, 0, &dst, "s".into(), "d".into(), work));
        assert_eq!((report.copied, report.deleted, report.failed), (0, 0, 1));
        assert!(report.deletes_skipped);
        assert!(report.reason.is_some());
        assert!(dir.join("dst/extra.txt").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
