//! Directory synchronization: a diff pass between two directories on any two
//! sites, feeding the existing transfer queue. The planner walks both trees,
//! compares files by relative path (size, then modified time with slack for
//! backends with coarse clocks), and produces a reviewable plan; applying it
//! enqueues per-file copy jobs and — in mirror mode — deletes extraneous
//! destination entries.

use std::collections::BTreeMap;

use anyhow::Result;

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
    /// Exists on both sides but differs; overwrite.
    Update,
    /// Extraneous at the destination (mirror only); topmost entries only.
    Delete,
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
    pub fn counts(&self) -> (usize, usize, usize) {
        let mut counts = (0, 0, 0);
        for item in &self.items {
            match item.kind {
                PlanKind::Copy => counts.0 += 1,
                PlanKind::Update => counts.1 += 1,
                PlanKind::Delete => counts.2 += 1,
            }
        }
        counts
    }

    /// Bytes that will be transferred (copies + updates).
    pub fn transfer_bytes(&self) -> u64 {
        self.items
            .iter()
            .filter(|item| item.kind != PlanKind::Delete && !item.is_dir)
            .map(|item| item.size)
            .sum()
    }

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
/// stat-completed fs listings behave exactly like the browser pane.
async fn walk(provider: &Provider, root: &str) -> Result<BTreeMap<String, FileMeta>> {
    let mut out = BTreeMap::new();
    let mut pending = vec![root.to_string()];
    while let Some(dir) = pending.pop() {
        for entry in provider.list(&dir).await? {
            let Some(rel) = entry.path.strip_prefix(root) else {
                continue;
            };
            if rel.is_empty() {
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

fn differs(src: &FileMeta, dst: &FileMeta) -> bool {
    if src.size != dst.size {
        return true;
    }
    match (src.modified, dst.modified) {
        (Some(src_time), Some(dst_time)) => (src_time - dst_time).num_seconds() > MTIME_SLACK_SECS,
        // No comparable timestamps and equal sizes: treat as in sync.
        _ => false,
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
                if !src_meta.is_dir && !dst_meta.is_dir && differs(src_meta, dst_meta) {
                    items.push(PlanItem {
                        rel: rel.clone(),
                        is_dir: false,
                        kind: PlanKind::Update,
                        size: src_meta.size,
                    });
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

/// The destination directory prefix a plan item's transfer writes into:
/// `dst_dir` + the item's parent path.
pub fn dst_dir_for(dst_root: &str, rel: &str) -> String {
    match rel.trim_end_matches('/').rsplit_once('/') {
        Some((parent, _)) => format!("{dst_root}{parent}/"),
        None => dst_root.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dst_dir_for_nests_parents() {
        assert_eq!(dst_dir_for("", "file.txt"), "");
        assert_eq!(dst_dir_for("base/", "a/b/file.txt"), "base/a/b/");
        assert_eq!(dst_dir_for("base/", "dir/"), "base/");
        assert_eq!(dst_dir_for("base/", "a/dir/"), "base/a/");
    }

    #[test]
    fn differs_compares_size_then_mtime() {
        use chrono::{TimeZone as _, Utc};
        let at = |secs| Utc.timestamp_opt(secs, 0).unwrap();
        let meta = |size, secs: Option<i64>| FileMeta {
            is_dir: false,
            size,
            modified: secs.map(at),
        };
        assert!(differs(&meta(1, None), &meta(2, None)));
        assert!(!differs(&meta(1, None), &meta(1, None)));
        // Newer source wins; slack absorbs coarse clocks.
        assert!(differs(&meta(1, Some(100)), &meta(1, Some(50))));
        assert!(!differs(&meta(1, Some(100)), &meta(1, Some(99))));
        // Older source is not "changed" (no back-propagation).
        assert!(!differs(&meta(1, Some(50)), &meta(1, Some(100))));
    }
}
