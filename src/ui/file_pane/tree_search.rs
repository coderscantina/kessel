//! Recursive search by name under one folder.
//!
//! Results stream to the pane in small batches over a bounded channel, so a
//! slow UI slows the walk down instead of piling up memory. Hits are capped at
//! [`MAX_HITS`] and the folders waiting to be walked at [`MAX_PENDING_DIRS`],
//! which keeps a huge tree from exhausting memory. Dropping the receiver or
//! aborting the task cancels the walk.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use opendal::{Entry, Operator};
use tokio::sync::mpsc;

use crate::provider::{to_datetime, Provider};
use crate::queue::PARTIAL_SUFFIX;

/// Most hits one search keeps.
pub const MAX_HITS: usize = 5_000;
/// Most folders queued for walking; deeper ones are left out and reported.
const MAX_PENDING_DIRS: usize = 100_000;
const BATCH_SIZE: usize = 100;
const FLUSH_EVERY: Duration = Duration::from_millis(150);

#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub name: String,
    /// Full browse path (folders end with `/`).
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

pub enum SearchEvent {
    Batch { hits: Vec<Hit>, scanned: u64 },
    Finished(SearchEnd),
}

#[derive(Default)]
pub struct SearchEnd {
    pub scanned: u64,
    /// A limit stopped the walk early, so more matches may exist.
    pub capped: bool,
    /// Folders that could not be listed and were skipped.
    pub unreadable: u64,
    pub error: Option<anyhow::Error>,
}

/// What to look for.
#[derive(Clone)]
pub struct Query {
    /// Lowercased name fragment.
    pub needle: String,
    pub show_hidden: bool,
}

impl Query {
    pub fn new(text: &str, show_hidden: bool) -> Self {
        Self { needle: text.trim().to_lowercase(), show_hidden }
    }

    fn matches(&self, name: &str) -> bool {
        name.to_lowercase().contains(&self.needle)
    }
}

/// Walk the tree under `root` and report through `events` until done.
pub async fn run(provider: Provider, root: String, query: Query, events: mpsc::Sender<SearchEvent>) {
    let walked = match provider.op_for(&root) {
        Ok((op, rel)) => {
            let prefix = root.strip_suffix(rel.as_str()).unwrap_or_default().to_string();
            let walker = Walker::new(op, prefix, query, events.clone());
            walker.walk(rel, provider.caps.virtual_dirs).await
        }
        Err(error) => Some(SearchEnd { error: Some(error), ..SearchEnd::default() }),
    };
    if let Some(end) = walked {
        events.send(SearchEvent::Finished(end)).await.ok();
    }
}

struct Walker {
    op: Operator,
    /// Browse-path prefix of operator paths (`bucket/` in bucket-root mode).
    prefix: String,
    /// Operator path of the folder being searched.
    root: String,
    query: Query,
    events: mpsc::Sender<SearchEvent>,
    batch: Vec<Hit>,
    found: usize,
    scanned: u64,
    last_flush: Instant,
    /// Folders already reported, for object stores where a folder only shows
    /// up as a prefix of its keys. Holds matches only, so `MAX_HITS` bounds it.
    seen_dirs: HashSet<String>,
}

/// Whether the walk goes on.
#[derive(PartialEq)]
enum Flow {
    Continue,
    /// The hit limit was reached.
    Capped,
    /// The pane stopped listening.
    Cancelled,
}

impl Walker {
    fn new(op: Operator, prefix: String, query: Query, events: mpsc::Sender<SearchEvent>) -> Self {
        Self {
            op,
            prefix,
            root: String::new(),
            query,
            events,
            batch: Vec::new(),
            found: 0,
            scanned: 0,
            last_flush: Instant::now(),
            seen_dirs: HashSet::new(),
        }
    }

    /// `None` when the search was cancelled.
    async fn walk(mut self, rel: String, virtual_dirs: bool) -> Option<SearchEnd> {
        self.root = rel.clone();
        let mut end = if virtual_dirs {
            self.walk_flat(&rel).await?
        } else {
            self.walk_folders(rel).await?
        };
        if !self.flush().await {
            return None;
        }
        end.scanned = self.scanned;
        Some(end)
    }

    /// Object stores list a whole subtree in one paged request.
    async fn walk_flat(&mut self, rel: &str) -> Option<SearchEnd> {
        let mut end = SearchEnd::default();
        let mut lister = match self.op.lister_with(rel).recursive(true).await {
            Ok(lister) => lister,
            Err(error) => return Some(SearchEnd { error: Some(error.into()), ..end }),
        };
        while let Some(entry) = lister.next().await {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    end.error = Some(error.into());
                    break;
                }
            };
            if is_listed_folder(&entry, rel) {
                continue;
            }
            let mut flow = self.visit_parents(entry.path()).await;
            if flow == Flow::Continue {
                flow = self.visit(&entry).await;
            }
            match flow {
                Flow::Continue => {}
                Flow::Capped => {
                    end.capped = true;
                    break;
                }
                Flow::Cancelled => return None,
            }
        }
        Some(end)
    }

    /// Everything else is walked folder by folder, so one unreadable subfolder
    /// costs only its own subtree, while failing on the search root is an error.
    /// Symlinks are not followed, which rules out cycles.
    async fn walk_folders(&mut self, rel: String) -> Option<SearchEnd> {
        let mut end = SearchEnd::default();
        let mut pending = VecDeque::from([rel]);
        'walk: while let Some(dir) = pending.pop_front() {
            let mut lister = match self.op.lister(&dir).await {
                Ok(lister) => lister,
                Err(error) if dir == self.root => {
                    end.error = Some(error.into());
                    break;
                }
                Err(error) => {
                    tracing::debug!(%error, %dir, "search skipped an unreadable folder");
                    end.unreadable += 1;
                    continue;
                }
            };
            while let Some(entry) = lister.next().await {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) if dir == self.root => {
                        end.error = Some(error.into());
                        break 'walk;
                    }
                    Err(_) => {
                        end.unreadable += 1;
                        break;
                    }
                };
                if is_listed_folder(&entry, &dir) {
                    continue;
                }
                match self.visit(&entry).await {
                    Flow::Continue => {}
                    Flow::Capped => {
                        end.capped = true;
                        break 'walk;
                    }
                    Flow::Cancelled => return None,
                }
                if entry.metadata().is_dir() && self.included(entry.name()) {
                    if pending.len() < MAX_PENDING_DIRS {
                        pending.push_back(entry.path().to_string());
                    } else {
                        end.capped = true;
                    }
                }
            }
        }
        Some(end)
    }

    fn included(&self, name: &str) -> bool {
        self.query.show_hidden || !name.starts_with('.')
    }

    /// Report the folders between the search root and a listed key that no
    /// marker object stands for. Hidden folders hide everything below them.
    async fn visit_parents(&mut self, path: &str) -> Flow {
        let inner = path.strip_prefix(self.root.as_str()).unwrap_or(path);
        let Some((folders, _)) = inner.trim_end_matches('/').rsplit_once('/') else {
            return Flow::Continue;
        };
        let mut dir = self.root.clone();
        for name in folders.split('/') {
            if !self.included(name) {
                break;
            }
            dir.push_str(name);
            dir.push('/');
            if self.query.matches(name) && self.seen_dirs.insert(dir.clone()) {
                let hit = Hit {
                    name: name.to_string(),
                    path: format!("{}{dir}", self.prefix),
                    is_dir: true,
                    size: 0,
                    modified: None,
                };
                let flow = self.push(hit).await;
                if flow != Flow::Continue {
                    return flow;
                }
            }
        }
        Flow::Continue
    }

    async fn visit(&mut self, entry: &Entry) -> Flow {
        self.scanned += 1;
        let name = entry.name().trim_end_matches('/');
        // Only below the search root: searching inside `.config` still works.
        let inner = entry.path().strip_prefix(self.root.as_str()).unwrap_or(entry.path());
        let hidden_path = !self.query.show_hidden
            && inner.trim_end_matches('/').split('/').any(|part| part.starts_with('.'));
        if name.is_empty() || hidden_path || name.ends_with(PARTIAL_SUFFIX) || !self.query.matches(name) {
            return self.tick().await;
        }
        let is_dir = entry.metadata().is_dir();
        if is_dir && !self.seen_dirs.insert(entry.path().to_string()) {
            return self.tick().await;
        }
        let mut size = entry.metadata().content_length();
        let mut modified = entry.metadata().last_modified().and_then(to_datetime);
        // Local fs listings carry neither; a hit is worth the extra stat.
        if !is_dir && modified.is_none() {
            if let Ok(meta) = self.op.stat(entry.path()).await {
                size = meta.content_length();
                modified = meta.last_modified().and_then(to_datetime);
            }
        }
        let hit = Hit {
            name: name.to_string(),
            path: format!("{}{}", self.prefix, entry.path()),
            is_dir,
            size,
            modified,
        };
        self.push(hit).await
    }

    async fn push(&mut self, hit: Hit) -> Flow {
        self.batch.push(hit);
        self.found += 1;
        if self.found >= MAX_HITS {
            return Flow::Capped;
        }
        self.tick().await
    }

    /// Send what has gathered once enough hits or time have passed, so the
    /// counter keeps moving through long stretches without a match.
    async fn tick(&mut self) -> Flow {
        if self.batch.len() < BATCH_SIZE && self.last_flush.elapsed() < FLUSH_EVERY {
            return Flow::Continue;
        }
        if self.flush().await { Flow::Continue } else { Flow::Cancelled }
    }

    /// `false` when the pane stopped listening.
    async fn flush(&mut self) -> bool {
        self.last_flush = Instant::now();
        let hits = std::mem::take(&mut self.batch);
        let batch = SearchEvent::Batch { hits, scanned: self.scanned };
        self.events.send(batch).await.is_ok()
    }
}

/// A listing includes the folder itself, as `/` for the operator root.
fn is_listed_folder(entry: &Entry, dir: &str) -> bool {
    entry.path().trim_matches('/') == dir.trim_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A throwaway folder served through the local filesystem backend.
    struct Tree(std::path::PathBuf);

    impl Tree {
        fn new(label: &str, files: &[&str]) -> Self {
            let root = std::env::temp_dir().join(format!("kessel-search-{label}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            for file in files {
                let path = root.join(file);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, b"x").unwrap();
            }
            Self(root)
        }

        fn op(&self) -> Operator {
            let root = self.0.to_string_lossy().to_string();
            Operator::via_iter("fs", [("root".to_string(), root)]).unwrap()
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_unreadable_root_is_an_error_and_an_unreadable_subfolder_is_skipped() {
        use std::os::unix::fs::PermissionsExt as _;

        let tree = Tree::new("unreadable", &["top/locked/file.txt", "top/open.txt"]);
        let locked = tree.0.join("top/locked");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let (events, _rx) = mpsc::channel(8);
        let walker = |events| Walker::new(tree.op(), String::new(), Query::new("nothing", false), events);

        let root = walker(events.clone()).walk("top/locked/".into(), false).await.unwrap();
        let below = walker(events).walk("top/".into(), false).await.unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(root.error.is_some());
        assert_eq!(root.unreadable, 0);
        assert!(below.error.is_none());
        assert_eq!(below.unreadable, 1);
    }

    async fn search(op: Operator, rel: &str, text: &str, show_hidden: bool) -> (Vec<Hit>, SearchEnd) {
        let (tx, mut rx) = mpsc::channel(4);
        let query = Query::new(text, show_hidden);
        let walk = Walker::new(op, String::new(), query, tx).walk(rel.to_string(), false);
        let collect = async {
            let mut hits = Vec::new();
            while let Some(event) = rx.recv().await {
                match event {
                    SearchEvent::Batch { hits: batch, .. } => hits.extend(batch),
                    SearchEvent::Finished(_) => {}
                }
            }
            hits
        };
        let (end, mut hits) = tokio::join!(walk, collect);
        hits.sort_by(|a, b| a.path.cmp(&b.path));
        (hits, end.expect("not cancelled"))
    }

    #[tokio::test]
    async fn finds_names_in_subfolders_and_skips_hidden_ones() {
        let tree = Tree::new("names", &[
            "Report.txt",
            "deep/er/monthly-report.pdf",
            "deep/other.txt",
            "reports/keep.md",
            ".cache/report.tmp",
            "deep/report.bin.kessel-part",
        ]);
        let (hits, end) = search(tree.op(), "", "REPORT", false).await;
        let paths: Vec<&str> = hits.iter().map(|hit| hit.path.as_str()).collect();
        assert_eq!(paths, ["Report.txt", "deep/er/monthly-report.pdf", "reports/"]);
        assert!(hits[2].is_dir);
        assert!(hits[0].modified.is_some(), "file hits carry their metadata");
        assert!(!end.capped);

        let (hits, _) = search(tree.op(), "", "report.tmp", true).await;
        assert_eq!(hits[0].path, ".cache/report.tmp");
    }

    #[tokio::test]
    async fn stops_at_the_hit_limit() {
        let files: Vec<String> = (0..MAX_HITS + 20).map(|ix| format!("f/{ix}.log")).collect();
        let names: Vec<&str> = files.iter().map(String::as_str).collect();
        let tree = Tree::new("cap", &names);
        let (hits, end) = search(tree.op(), "", ".log", false).await;
        assert_eq!(hits.len(), MAX_HITS);
        assert!(end.capped);
    }
}
