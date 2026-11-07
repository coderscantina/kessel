//! Edit-in-external-editor sessions: download the remote file to a temp
//! path, open it with the OS default app, watch it for saves, and upload
//! each save back through the transfer queue (Overwrite policy), so retries,
//! throttling and progress all come for free.
//!
//! There is one session per remote file. Opening a file that already has a
//! session just reopens its local copy, so unsaved or not-yet-uploaded edits
//! are never overwritten by a fresh download. A second open while the first
//! is still downloading waits for it and reuses its session.
//!
//! Each session remembers what the *server* copy looked like when it was
//! last in sync (etag / mtime / size). Before uploading a save, the remote
//! is stat'ed again. If someone else changed it, or the check itself fails,
//! nothing is uploaded: a change raises a conflict the user decides on (see
//! [`open_conflict_dialog`]), a failed check leaves the local copy as it is.
//!
//! Sessions live for the rest of the process. An editor may keep the file
//! open indefinitely, and a watcher per edited file is cheap.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context as _, Result};
use gpui::prelude::*;
use gpui::{px, App, Window};
use gpui_component::button::Button;
use gpui_component::{v_flex, ActiveTheme as _, Sizable as _, WindowExt as _};
use notify::Watcher as _;
use tokio::sync::mpsc;

use crate::i18n::tf;
use crate::provider::Provider;
use crate::queue::{ConflictPolicy, JobState, NewJob, QueueHandle, LOCAL_SITE_ID};

/// Quiet period after a burst of editor writes before uploading.
const DEBOUNCE: Duration = Duration::from_millis(750);

/// One slot per (site id, remote path). Its async lock is held while the
/// session opens, so concurrent opens of the same file share one session.
type SessionKey = (i64, String);
type SessionSlot = Arc<tokio::sync::Mutex<Option<OpenSession>>>;
static SESSIONS: LazyLock<Mutex<HashMap<SessionKey, SessionSlot>>> = LazyLock::new(Mutex::default);

/// A running session: the file the editor works on, and the watcher that
/// keeps the session alive (dropping a `RecommendedWatcher` stops it).
struct OpenSession {
    temp: PathBuf,
    _watcher: notify::RecommendedWatcher,
}

/// What the remote held when we last were in sync with it.
#[derive(Debug, Clone, PartialEq)]
struct RemoteState {
    etag: Option<String>,
    modified: Option<String>,
    size: u64,
}

/// Identifies one version of the local file: a save changes the length or
/// the modified time. Comparing it against the last uploaded version is the
/// session's dirty check, so saves that land during an upload get their own
/// upload afterwards, and watcher noise that changes nothing is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalVersion {
    len: u64,
    modified: Option<SystemTime>,
}

impl LocalVersion {
    async fn of(path: &Path) -> Result<Self> {
        let meta = tokio::fs::metadata(path)
            .await
            .with_context(|| format!("reading `{}`", path.display()))?;
        Ok(Self {
            len: meta.len(),
            modified: meta.modified().ok(),
        })
    }
}

/// A save collided with a server-side change; the session waits on
/// `decision` until the user picks a side.
pub struct EditConflict {
    pub name: String,
    pub remote_modified: Option<String>,
    pub decision: mpsc::UnboundedSender<EditDecision>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditDecision {
    /// Upload the local save anyway.
    Overwrite,
    /// Throw the local changes away and re-download the server copy.
    DiscardLocal,
    /// Do nothing: the local file stays, nothing is uploaded.
    Cancel,
}

/// The outcome of one save, for a notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditNotice {
    Saved(String),
    Failed(String),
}

/// Show an edit session's upload result as a notification.
pub fn show_notice(notice: EditNotice, window: &mut Window, cx: &mut App) {
    let notification = match notice {
        EditNotice::Saved(message) => gpui_component::notification::Notification::success(message),
        EditNotice::Failed(message) => gpui_component::notification::Notification::error(message),
    };
    window.push_notification(notification, cx);
}

type Channel<T> = (smol::channel::Sender<T>, smol::channel::Receiver<T>);

fn conflicts() -> &'static Channel<EditConflict> {
    static CHANNEL: OnceLock<Channel<EditConflict>> = OnceLock::new();
    CHANNEL.get_or_init(smol::channel::unbounded)
}

fn notices() -> &'static Channel<EditNotice> {
    static CHANNEL: OnceLock<Channel<EditNotice>> = OnceLock::new();
    CHANNEL.get_or_init(smol::channel::unbounded)
}

/// Conflicts raised by edit sessions; the app listens and opens the dialog.
pub fn subscribe() -> smol::channel::Receiver<EditConflict> {
    conflicts().1.clone()
}

/// Upload results of edit sessions; the app shows each as a notification.
pub fn subscribe_notices() -> smol::channel::Receiver<EditNotice> {
    notices().1.clone()
}

/// Runs on the app tokio runtime: open `path` from `provider` in the default
/// app, starting a session (download, watch, upload saves) unless one is
/// already running for it.
pub async fn start(
    provider: Provider,
    path: String,
    name: String,
    queue: QueueHandle,
) -> Result<PathBuf> {
    let temp = session_file(provider, path, name, queue).await?;
    crate::ui::open_path_or_url(temp.to_string_lossy().as_ref());
    Ok(temp)
}

/// The local file of the session for `path`, opening the session (download
/// and watch) unless one is running or already being opened.
async fn session_file(provider: Provider, path: String, name: String, queue: QueueHandle) -> Result<PathBuf> {
    let slot = SESSIONS
        .lock()
        .expect("edit session registry poisoned")
        .entry((provider.site_id, path.clone()))
        .or_default()
        .clone();
    let mut open = slot.lock().await;
    if let Some(session) = open.as_ref() {
        return Ok(session.temp.clone());
    }
    let session = Arc::new(Session::open(provider, path, name, queue).await?);
    let temp = session.temp.clone();
    let watcher = watch(session)?;
    *open = Some(OpenSession {
        temp: temp.clone(),
        _watcher: watcher,
    });
    Ok(temp)
}

struct Session {
    provider: Provider,
    remote_path: String,
    /// The file the editor works on; its folder is watched.
    temp: PathBuf,
    /// Where each save is copied before uploading, so the upload reads one
    /// consistent version even if the editor saves again meanwhile.
    snapshot: PathBuf,
    name: String,
    queue: QueueHandle,
    sync: tokio::sync::Mutex<SyncPoint>,
}

/// The last state both sides agreed on.
struct SyncPoint {
    /// `None` when the server copy couldn't be read after an upload; the
    /// next save then asks before overwriting.
    remote: Option<RemoteState>,
    local: LocalVersion,
}

async fn stat_remote(provider: &Provider, path: &str) -> Result<RemoteState> {
    let (op, rel) = provider.op_for(path)?;
    let meta = op.stat(&rel).await?;
    Ok(RemoteState {
        etag: meta.etag().map(str::to_string),
        modified: meta
            .last_modified()
            .and_then(crate::provider::to_datetime)
            .map(|t| t.to_rfc3339()),
        size: meta.content_length(),
    })
}

/// Watch the session file's folder (editors that save by writing a new file
/// and renaming it over the old one replace the watched inode) and upload
/// after each quiet period following a save.
fn watch(session: Arc<Session>) -> Result<notify::RecommendedWatcher> {
    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    let target = session.temp.clone();
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        let Ok(event) = result else { return };
        let relevant = matches!(
            event.kind,
            notify::EventKind::Modify(_) | notify::EventKind::Create(_)
        ) && event.paths.iter().any(|path| path.file_name() == target.file_name());
        if relevant {
            tx.send(()).ok();
        }
    })?;
    let folder = session
        .temp
        .parent()
        .context("edit file has no parent folder")?;
    watcher.watch(folder, notify::RecursiveMode::NonRecursive)?;

    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            while let Ok(Some(())) = tokio::time::timeout(DEBOUNCE, rx.recv()).await {}
            session.flush().await;
        }
    });
    Ok(watcher)
}

impl Session {
    /// Download the remote file into a fresh per-process folder and record
    /// the sync point.
    async fn open(provider: Provider, remote_path: String, name: String, queue: QueueHandle) -> Result<Self> {
        let dir = std::env::temp_dir()
            .join("kessel-edit")
            .join(std::process::id().to_string())
            .join(format!("{}-{:x}", provider.site_id, fxhash(&remote_path)));
        for sub in ["edit", "upload"] {
            tokio::fs::create_dir_all(dir.join(sub))
                .await
                .with_context(|| format!("creating `{}`", dir.display()))?;
        }
        let temp = dir.join("edit").join(&name);
        let remote = stat_remote(&provider, &remote_path).await?;
        provider.read_to_file(&remote_path, &temp).await?;
        let local = LocalVersion::of(&temp).await?;
        Ok(Self {
            provider,
            remote_path,
            snapshot: dir.join("upload").join(&name),
            temp,
            name,
            queue,
            sync: tokio::sync::Mutex::new(SyncPoint {
                remote: Some(remote),
                local,
            }),
        })
    }

    /// Upload the local file until the server holds its latest version, or
    /// stop at the first failure, conflict left undecided, or discard. The
    /// local copy is never touched except by an explicit discard.
    async fn flush(&self) {
        let mut sync = self.sync.lock().await;
        loop {
            let local = match LocalVersion::of(&self.temp).await {
                Ok(local) => local,
                Err(error) => return self.failed(&error),
            };
            if local == sync.local {
                return;
            }
            let current = match stat_remote(&self.provider, &self.remote_path).await {
                Ok(current) => current,
                Err(error) => return self.failed(&error),
            };
            if sync.remote.as_ref() != Some(&current) {
                match self.decide(&current).await {
                    EditDecision::Overwrite => {}
                    EditDecision::DiscardLocal => return self.discard(&mut sync, current).await,
                    EditDecision::Cancel => return,
                }
            }
            match self.upload(local).await {
                Ok((remote, local)) => {
                    *sync = SyncPoint { remote, local };
                    self.notify(EditNotice::Saved(
                        tf(
                            "edit.saved",
                            &[("name", &self.name), ("site", &self.provider.site_name)],
                        )
                        .to_string(),
                    ));
                }
                Err(error) => return self.failed(&error),
            }
        }
    }

    /// Snapshot the local file, upload the snapshot and wait for the queue
    /// job to settle. Returns the new sync point. `local` was read before
    /// the snapshot, so a save racing the copy only makes the snapshot newer
    /// than recorded, and the next pass uploads once more.
    async fn upload(&self, local: LocalVersion) -> Result<(Option<RemoteState>, LocalVersion)> {
        tokio::fs::copy(&self.temp, &self.snapshot)
            .await
            .with_context(|| format!("copying `{}`", self.temp.display()))?;
        tracing::info!(file = %self.name, "external edit saved; uploading");
        let job_id = self
            .queue
            .submit(NewJob {
                src_site_id: LOCAL_SITE_ID,
                dst_site_id: self.provider.site_id,
                src: self.snapshot.to_string_lossy().trim_start_matches('/').to_string(),
                dst_dir: parent_of(&self.remote_path),
                dst_name: Some(self.name.clone()),
                conflict_policy: ConflictPolicy::Overwrite,
                move_source: false,
            })
            .await?;
        let outcome = self.queue.settled(job_id).await?;
        if outcome.state != JobState::Done || outcome.files_failed > 0 {
            anyhow::bail!(outcome
                .error
                .unwrap_or_else(|| "the upload was cancelled".to_string()));
        }
        // Unknown on a failed stat: the next save asks instead of guessing.
        let remote = stat_remote(&self.provider, &self.remote_path).await.ok();
        Ok((remote, local))
    }

    /// Raise the conflict to the UI and wait for the decision.
    async fn decide(&self, remote: &RemoteState) -> EditDecision {
        tracing::warn!(
            file = %self.name, remote = %self.remote_path,
            "external edit conflicts with a server-side change"
        );
        let (tx, mut rx) = mpsc::unbounded_channel();
        let conflict = EditConflict {
            name: self.name.clone(),
            remote_modified: remote.modified.clone(),
            decision: tx,
        };
        if conflicts().0.try_send(conflict).is_err() {
            return EditDecision::Cancel;
        }
        // A closed dialog (dropped sender) counts as Cancel.
        rx.recv().await.unwrap_or(EditDecision::Cancel)
    }

    /// Replace the local copy with the server's and make that the sync point.
    async fn discard(&self, sync: &mut SyncPoint, remote: RemoteState) {
        if let Err(error) = self
            .provider
            .read_to_file(&self.remote_path, &self.temp)
            .await
        {
            return self.failed(&error);
        }
        match LocalVersion::of(&self.temp).await {
            Ok(local) => {
                *sync = SyncPoint {
                    remote: Some(remote),
                    local,
                }
            }
            Err(error) => self.failed(&error),
        }
    }

    fn failed(&self, error: &anyhow::Error) {
        tracing::error!(file = %self.name, error = %format!("{error:#}"), "external edit upload failed");
        self.notify(EditNotice::Failed(
            tf(
                "edit.failed",
                &[
                    ("name", &self.name),
                    ("site", &self.provider.site_name),
                    ("reason", &crate::errors::humanize(error).title),
                ],
            )
            .to_string(),
        ));
    }

    fn notify(&self, notice: EditNotice) {
        notices().0.try_send(notice).ok();
    }
}

/// "The server copy changed" prompt for an external-edit session.
pub fn open_conflict_dialog(conflict: EditConflict, window: &mut Window, cx: &mut App) {
    let EditConflict {
        name,
        remote_modified,
        decision,
    } = conflict;
    let when = remote_modified
        .as_deref()
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|t| {
            t.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        });
    let summary = match when {
        Some(when) => format!(
            "The server copy of “{name}” changed on {when}, after your edit session started. \
             Uploading your save would overwrite that change."
        ),
        None => format!(
            "The server copy of “{name}” changed after your edit session started. \
             Uploading your save would overwrite that change."
        ),
    };
    const CHOICES: &[(&str, &str, EditDecision)] = &[
        (
            "Upload Anyway",
            "Replace the server copy with your local save.",
            EditDecision::Overwrite,
        ),
        (
            "Discard My Changes",
            "Throw your local edits away and reload the server copy.",
            EditDecision::DiscardLocal,
        ),
        (
            "Decide Later",
            "Keep editing locally; nothing is uploaded for this save.",
            EditDecision::Cancel,
        ),
    ];
    window.open_dialog(cx, move |dialog, _, cx| {
        let decision = decision.clone();
        dialog
            .title("Server file changed")
            .w(px(440.))
            .child(
                v_flex()
                    .gap_2()
                    .child(summary.clone())
                    .children(CHOICES.iter().map(|(label, explain, choice)| {
                        let decision = decision.clone();
                        let choice = *choice;
                        v_flex()
                            .gap_0p5()
                            .child(
                                Button::new(*label)
                                    .label(*label)
                                    .outline()
                                    .small()
                                    .on_click(move |_, window, cx| {
                                        decision.send(choice).ok();
                                        window.close_dialog(cx);
                                    }),
                            )
                            .child(
                                gpui::div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child(*explain),
                            )
                    })),
            )
    });
}

/// `"a/b/c.txt"` → `"a/b/"`.
fn parent_of(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(ix) => trimmed[..=ix].to_string(),
        None => String::new(),
    }
}

/// Tiny stable hash for temp dir names (no extra dependency).
fn fxhash(s: &str) -> u64 {
    s.bytes()
        .fold(0xcbf29ce484222325u64, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A queue and a local-filesystem provider standing in for a remote,
    /// over a fresh temp dir holding `notes.txt` = "v1".
    fn fixture(tag: &str) -> (tokio::runtime::Runtime, PathBuf, Provider, QueueHandle) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = std::env::temp_dir().join(format!("kessel-edit-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("notes.txt"), b"v1").unwrap();

        let pool = rt.block_on(crate::db::connect(&dir)).unwrap();
        let cfg = crate::config::TransferConfig {
            max_parallel: 2,
            bandwidth_limit: 0,
            retries: 0,
            verify: true,
            preserve_timestamps: false,
            skip_hidden: false,
            default_conflict_policy: Default::default(),
        };
        let queue = crate::queue::start(&rt, pool, cfg, Default::default());
        let local = crate::provider::SiteProfile {
            scheme: "fs".into(),
            options: [("root".to_string(), "/".to_string())].into(),
            ..Default::default()
        };
        let provider = Provider::connect(&local, &Default::default()).unwrap();
        (rt, dir, provider, queue)
    }

    #[test]
    fn uploads_each_new_save_once_and_never_after_a_failed_check() {
        let (rt, dir, provider, queue) = fixture("uploads");
        let remote_file = dir.join("notes.txt");
        let remote_path = remote_file.to_str().unwrap().trim_start_matches('/').to_string();
        let session = rt
            .block_on(Session::open(provider, remote_path, "notes.txt".into(), queue.clone()))
            .unwrap();
        let notices = subscribe_notices();

        std::fs::write(&session.temp, b"v2 local").unwrap();
        rt.block_on(session.flush());
        assert_eq!(std::fs::read(&remote_file).unwrap(), b"v2 local");
        assert!(matches!(notices.try_recv(), Ok(EditNotice::Saved(_))));

        // Nothing new locally: no upload, so no notice either.
        rt.block_on(session.flush());
        assert!(notices.try_recv().is_err());

        // The server copy can't be checked: keep the local save, upload nothing.
        std::fs::remove_file(&remote_file).unwrap();
        std::fs::write(&session.temp, b"v3 local, longer").unwrap();
        rt.block_on(session.flush());
        assert!(!remote_file.exists());
        assert_eq!(std::fs::read(&session.temp).unwrap(), b"v3 local, longer");
        assert!(matches!(notices.try_recv(), Ok(EditNotice::Failed(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_open_waits_for_and_reuses_the_first() {
        let (rt, dir, provider, queue) = fixture("reuse");
        let remote_path = dir.join("notes.txt").to_str().unwrap().trim_start_matches('/').to_string();
        let reused = dir.join("reused.txt");
        let opened = rt.block_on(async {
            let slot = SESSIONS
                .lock()
                .unwrap()
                .entry((provider.site_id, remote_path.clone()))
                .or_default()
                .clone();
            // A first open still downloading holds the slot.
            let mut first = slot.lock().await;
            let second = tokio::spawn(session_file(provider, remote_path, "notes.txt".into(), queue));
            tokio::time::sleep(Duration::from_millis(50)).await;
            *first = Some(OpenSession {
                temp: reused.clone(),
                _watcher: notify::recommended_watcher(|_: notify::Result<notify::Event>| {}).unwrap(),
            });
            drop(first);
            second.await.unwrap().unwrap()
        });
        assert_eq!(opened, reused);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
