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
//! last in sync (etag / mtime / size). Right before a save goes to the queue,
//! the remote is stat'ed again. If someone else changed it, or the check
//! itself fails, nothing is uploaded: a change raises a conflict the user
//! decides on (see [`open_conflict_dialog`]), a failed check keeps the local
//! copy and offers a retry. The queue job carries the accepted state too, so
//! its worker refuses to overwrite anything else, also when an old save is
//! retried from the transfers panel much later.
//!
//! Sessions live until the app quits, because an editor may keep the file
//! open indefinitely and a watcher per edited file is cheap. Quitting removes
//! the working copies that hold nothing unsaved (see [`end_sessions`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context as _, Result};
use gpui::prelude::*;
use gpui::{px, App, Window};
use gpui_component::button::Button;
use gpui_component::{v_flex, ActiveTheme as _, Sizable as _, WindowExt as _};
use notify::Watcher as _;
use tokio::sync::{mpsc, Notify};

use crate::i18n::{t, tf};
use crate::provider::Provider;
use crate::queue::{
    BatchFile, ConflictPolicy, ExpectedDest, JobOutcome, JobState, NewBatch, QueueHandle, LOCAL_SITE_ID,
};

/// Quiet period after a burst of editor writes before uploading.
const DEBOUNCE: Duration = Duration::from_millis(750);

/// One slot per (site id, remote path). Its async lock is held while the
/// session opens, so concurrent opens of the same file share one session.
type SessionKey = (i64, String);
type SessionSlot = Arc<tokio::sync::Mutex<Option<OpenSession>>>;
static SESSIONS: LazyLock<Mutex<HashMap<SessionKey, SessionSlot>>> = LazyLock::new(Mutex::default);

/// A running session and the watcher that keeps it alive (dropping a
/// `RecommendedWatcher` stops it).
struct OpenSession {
    session: Arc<Session>,
    _watcher: notify::RecommendedWatcher,
}

/// What the remote held when we last were in sync with it.
#[derive(Debug, Clone, PartialEq)]
struct RemoteState {
    etag: Option<String>,
    modified: Option<String>,
    size: u64,
}

impl RemoteState {
    /// What the queue must still find on the server when it writes this save.
    fn expected(&self) -> ExpectedDest {
        ExpectedDest::File {
            size: self.size,
            etag: self.etag.clone(),
            modified: self
                .modified
                .as_deref()
                .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
                .map(|t| t.with_timezone(&chrono::Utc)),
        }
    }
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
        Ok(Self::from_meta(&meta))
    }

    fn from_meta(meta: &std::fs::Metadata) -> Self {
        Self {
            len: meta.len(),
            modified: meta.modified().ok(),
        }
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
#[derive(Debug, Clone)]
pub enum EditNotice {
    Saved(String),
    Failed(EditFailure),
}

/// A save that didn't reach the server. The local copy is kept and `retry`
/// makes the session try again.
#[derive(Debug, Clone)]
pub struct EditFailure {
    pub message: String,
    retry: Arc<Notify>,
}

/// Show an edit session's upload result as a notification. A failure stays
/// on screen with a retry button until the user acts on it.
pub fn show_notice(notice: EditNotice, window: &mut Window, cx: &mut App) {
    use gpui_component::notification::Notification;
    let notification = match notice {
        EditNotice::Saved(message) => Notification::success(message),
        EditNotice::Failed(EditFailure { message, retry }) => Notification::error(message.clone())
            .id1::<EditFailure>(gpui::SharedString::from(message))
            .action(move |_, _, cx| {
                let retry = retry.clone();
                Button::new("edit-retry")
                    .label(t("edit.retry"))
                    .small()
                    .on_click(cx.listener(move |notification, _, window, cx| {
                        notification.dismiss(window, cx);
                        retry.notify_one();
                    }))
            }),
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
    if let Some(open) = open.as_ref() {
        return Ok(open.session.temp.clone());
    }
    let session = Arc::new(Session::open(provider, path, name, queue).await?);
    let temp = session.temp.clone();
    let watcher = watch(session.clone())?;
    *open = Some(OpenSession {
        session,
        _watcher: watcher,
    });
    Ok(temp)
}

/// End every session when the app quits: stop watching, remove working
/// copies that match what the server got, and keep the rest on disk.
pub fn end_sessions() {
    let slots: Vec<SessionSlot> = SESSIONS
        .lock()
        .expect("edit session registry poisoned")
        .drain()
        .map(|(_, slot)| slot)
        .collect();
    for slot in slots {
        // A slot still locked is a session that is opening: nothing to end.
        if let Some(open) = slot.try_lock().ok().and_then(|mut open| open.take()) {
            open.session.close();
        }
    }
}

struct Session {
    provider: Provider,
    remote_path: String,
    /// The session's own folder, holding the working copy and snapshots.
    dir: PathBuf,
    /// The file the editor works on; its folder is watched.
    temp: PathBuf,
    /// Numbers the per-save snapshot folders.
    saves: AtomicU64,
    /// Wakes the watcher task to try a failed save again.
    retry: Arc<Notify>,
    name: String,
    queue: QueueHandle,
    sync: tokio::sync::Mutex<SyncPoint>,
}

/// What to do with a save after checking the server copy.
enum Preflight {
    /// Go ahead, replacing exactly this server state.
    Upload(RemoteState),
    Discard(RemoteState),
    Stop,
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
/// after each quiet period following a save, or when a retry is requested.
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
        loop {
            tokio::select! {
                event = rx.recv() => {
                    if event.is_none() {
                        break;
                    }
                    while let Ok(Some(())) = tokio::time::timeout(DEBOUNCE, rx.recv()).await {}
                }
                () = session.retry.notified() => {}
            }
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
        tokio::fs::create_dir_all(dir.join("edit"))
            .await
            .with_context(|| format!("creating `{}`", dir.display()))?;
        let temp = dir.join("edit").join(&name);
        let remote = stat_remote(&provider, &remote_path).await?;
        provider.read_to_file(&remote_path, &temp).await?;
        let local = LocalVersion::of(&temp).await?;
        Ok(Self {
            provider,
            remote_path,
            dir,
            temp,
            saves: AtomicU64::new(0),
            retry: Arc::default(),
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
            let snapshot = match self.snapshot().await {
                Ok(snapshot) => snapshot,
                Err(error) => return self.failed(&error),
            };
            // The last step before the queue takes over, so the server has
            // as little time as possible to change unnoticed.
            let preflight = self.preflight(sync.remote.as_ref()).await;
            if !matches!(preflight, Ok(Preflight::Upload(_))) {
                remove_snapshot(&snapshot).await;
            }
            let accepted = match preflight {
                Ok(Preflight::Upload(accepted)) => accepted,
                Ok(Preflight::Discard(current)) => return self.discard(&mut sync, current).await,
                Ok(Preflight::Stop) => return,
                Err(error) => return self.failed(&error),
            };
            match self.upload(&snapshot, local, &accepted).await {
                // The server changed after the preflight: the next pass asks.
                Ok(None) => remove_snapshot(&snapshot).await,
                Ok(Some((remote, local))) => {
                    remove_snapshot(&snapshot).await;
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

    /// Copy the local file to a snapshot of its own, so the upload reads one
    /// consistent version even if the editor saves again, and a retried job
    /// sends the bytes of its own save.
    async fn snapshot(&self) -> Result<PathBuf> {
        let save = self.saves.fetch_add(1, Ordering::Relaxed);
        let dir = self.dir.join("upload").join(save.to_string());
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating `{}`", dir.display()))?;
        let snapshot = dir.join(&self.name);
        tokio::fs::copy(&self.temp, &snapshot)
            .await
            .with_context(|| format!("copying `{}`", self.temp.display()))?;
        Ok(snapshot)
    }

    /// Compare the server copy with the last agreed state and ask the user
    /// on a mismatch. An accepted overwrite covers only the version the user
    /// was shown, so the server is checked again after every answer.
    async fn preflight(&self, agreed: Option<&RemoteState>) -> Result<Preflight> {
        let mut accepted = agreed.cloned();
        loop {
            let current = stat_remote(&self.provider, &self.remote_path).await?;
            if accepted.as_ref() == Some(&current) {
                return Ok(Preflight::Upload(current));
            }
            match self.decide(&current).await {
                EditDecision::Overwrite => accepted = Some(current),
                EditDecision::DiscardLocal => return Ok(Preflight::Discard(current)),
                EditDecision::Cancel => return Ok(Preflight::Stop),
            }
        }
    }

    /// Upload a snapshot and wait for the queue job to settle. Returns the
    /// new sync point. `local` was read before the snapshot, so a save racing
    /// the copy only makes the snapshot newer than recorded, and the next
    /// pass uploads once more. A failed job keeps its snapshot for a retry
    /// from the transfers panel. `None` when the queue found the server no
    /// longer at `accepted`: the job is cancelled and nothing was written.
    async fn upload(
        &self,
        snapshot: &Path,
        local: LocalVersion,
        accepted: &RemoteState,
    ) -> Result<Option<(Option<RemoteState>, LocalVersion)>> {
        tracing::info!(file = %self.name, "external edit saved; uploading");
        let src = crate::localfs::browse_path(snapshot, false);
        let size = tokio::fs::metadata(snapshot).await.ok().map(|meta| meta.len());
        let job_id = self
            .queue
            .submit_reviewed_batch(
                NewBatch {
                    src_site_id: LOCAL_SITE_ID,
                    dst_site_id: self.provider.site_id,
                    src_label: src.clone(),
                    dst_label: parent_of(&self.remote_path),
                    files: vec![BatchFile {
                        src,
                        dst: self.remote_path.clone(),
                        size,
                    }],
                    conflict_policy: ConflictPolicy::Overwrite,
                    move_source: false,
                },
                HashMap::from([(self.remote_path.clone(), accepted.expected())]),
            )
            .await?;
        let Some(outcome) = self.settled_unless_parked(job_id).await? else {
            return Ok(None);
        };
        if outcome.state != JobState::Done || outcome.files_failed > 0 || outcome.files_done == 0 {
            anyhow::bail!(outcome
                .error
                .unwrap_or_else(|| "the upload was cancelled".to_string()));
        }
        // Unknown on a failed stat: the next save asks instead of guessing.
        let remote = stat_remote(&self.provider, &self.remote_path).await.ok();
        Ok(Some((remote, local)))
    }

    /// Wait for the upload job to settle. A job parked on a conflict is
    /// cancelled and reported as `None`, because this session asks the user
    /// itself instead of leaving the decision to the transfers panel.
    async fn settled_unless_parked(&self, job_id: i64) -> Result<Option<JobOutcome>> {
        let mut snapshots = self.queue.subscribe();
        let parked = async {
            loop {
                let conflict = snapshots
                    .borrow_and_update()
                    .jobs
                    .iter()
                    .any(|job| job.id == job_id && job.files_conflict > 0);
                if conflict || snapshots.changed().await.is_err() {
                    return conflict;
                }
            }
        };
        tokio::select! {
            outcome = self.queue.settled(job_id) => outcome.map(Some),
            true = parked => {
                self.queue.cancel(job_id);
                self.queue.settled(job_id).await?;
                Ok(None)
            }
        }
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
        self.notify(EditNotice::Failed(EditFailure {
            message: tf(
                "edit.failed",
                &[
                    ("name", &self.name),
                    ("site", &self.provider.site_name),
                    ("reason", &crate::errors::humanize(error).title),
                ],
            )
            .to_string(),
            retry: self.retry.clone(),
        }));
    }

    fn notify(&self, notice: EditNotice) {
        notices().0.try_send(notice).ok();
    }

    /// Remove the session's folder unless the working copy holds edits the
    /// server doesn't have. A save still uploading counts as unsaved.
    fn close(&self) {
        let clean = self.sync.try_lock().is_ok_and(|sync| {
            std::fs::metadata(&self.temp).is_ok_and(|meta| LocalVersion::from_meta(&meta) == sync.local)
        });
        if !clean {
            tracing::warn!(file = %self.temp.display(), "keeping edited copy with unsaved changes");
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(&self.dir) {
            tracing::warn!(dir = %self.dir.display(), %error, "removing edit session folder failed");
        }
        // Succeeds only for the last session of this process.
        if let Some(process_dir) = self.dir.parent() {
            let _ = std::fs::remove_dir(process_dir);
        }
    }
}

/// Remove a snapshot that is no longer needed, along with its save folder.
async fn remove_snapshot(snapshot: &Path) {
    if let Some(dir) = snapshot.parent() {
        if let Err(error) = tokio::fs::remove_dir_all(dir).await {
            tracing::debug!(dir = %dir.display(), %error, "removing edit snapshot failed");
        }
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
        Some(when) => tf("edit.conflict.summary_at", &[("name", &name), ("when", &when)]),
        None => tf("edit.conflict.summary", &[("name", &name)]),
    };
    const CHOICES: &[(&str, &str, EditDecision)] = &[
        ("edit.conflict.upload", "edit.conflict.upload_hint", EditDecision::Overwrite),
        ("edit.conflict.discard", "edit.conflict.discard_hint", EditDecision::DiscardLocal),
        ("edit.conflict.later", "edit.conflict.later_hint", EditDecision::Cancel),
    ];
    window.open_dialog(cx, move |dialog, _, cx| {
        let decision = decision.clone();
        dialog
            .title(t("edit.conflict.title"))
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
                                    .label(t(label))
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
                                    .child(t(explain)),
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

    /// The notice and conflict channels are process-wide, so tests that
    /// read them take turns.
    static CHANNELS: Mutex<()> = Mutex::new(());

    fn exclusive_channels() -> std::sync::MutexGuard<'static, ()> {
        let guard = CHANNELS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while subscribe_notices().try_recv().is_ok() {}
        guard
    }

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
        let _channels = exclusive_channels();
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
    fn an_upload_against_an_outdated_server_state_writes_nothing() {
        let (rt, dir, provider, queue) = fixture("stale");
        let remote_file = dir.join("notes.txt");
        let remote_path = remote_file.to_str().unwrap().trim_start_matches('/').to_string();
        let session = rt
            .block_on(Session::open(provider, remote_path, "notes.txt".into(), queue))
            .unwrap();
        std::fs::write(&session.temp, b"v2 local").unwrap();
        let local = rt.block_on(LocalVersion::of(&session.temp)).unwrap();
        let snapshot = rt.block_on(session.snapshot()).unwrap();
        let accepted = rt.block_on(session.sync.lock()).remote.clone().unwrap();

        // The server moves on after the preflight accepted its old state.
        std::fs::write(&remote_file, b"v2 from someone else").unwrap();
        let uploaded = rt.block_on(session.upload(&snapshot, local, &accepted)).unwrap();

        assert!(uploaded.is_none());
        assert_eq!(std::fs::read(&remote_file).unwrap(), b"v2 from someone else");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_open_waits_for_and_reuses_the_first() {
        let (rt, dir, provider, queue) = fixture("reuse");
        let remote_path = dir.join("notes.txt").to_str().unwrap().trim_start_matches('/').to_string();
        let reused_session = Arc::new(
            rt.block_on(Session::open(provider.clone(), remote_path.clone(), "notes.txt".into(), queue.clone()))
                .unwrap(),
        );
        let reused = reused_session.temp.clone();
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
                session: reused_session,
                _watcher: notify::recommended_watcher(|_: notify::Result<notify::Event>| {}).unwrap(),
            });
            drop(first);
            second.await.unwrap().unwrap()
        });
        assert_eq!(opened, reused);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_server_change_during_the_conflict_dialog_asks_again() {
        let _channels = exclusive_channels();
        let (rt, dir, provider, queue) = fixture("recheck");
        let remote_file = dir.join("notes.txt");
        let remote_path = remote_file.to_str().unwrap().trim_start_matches('/').to_string();
        let session = rt
            .block_on(Session::open(provider, remote_path, "notes.txt".into(), queue))
            .unwrap();
        std::fs::write(&remote_file, b"v2 server").unwrap();
        std::fs::write(&session.temp, b"v2 local").unwrap();

        let server = remote_file.clone();
        let user = std::thread::spawn(move || {
            let conflicts = subscribe();
            let first = conflicts.recv_blocking().unwrap();
            std::fs::write(&server, b"v3 server, changed meanwhile").unwrap();
            first.decision.send(EditDecision::Overwrite).unwrap();
            let second = conflicts.recv_blocking().unwrap();
            second.decision.send(EditDecision::Cancel).unwrap();
        });
        rt.block_on(session.flush());
        user.join().unwrap();

        assert_eq!(std::fs::read(&remote_file).unwrap(), b"v3 server, changed meanwhile");
        assert_eq!(std::fs::read(&session.temp).unwrap(), b"v2 local");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_save_gets_its_own_snapshot() {
        let (rt, dir, provider, queue) = fixture("snapshots");
        let remote_path = dir.join("notes.txt").to_str().unwrap().trim_start_matches('/').to_string();
        let session = rt
            .block_on(Session::open(provider, remote_path, "notes.txt".into(), queue))
            .unwrap();
        std::fs::write(&session.temp, b"first save").unwrap();
        let first = rt.block_on(session.snapshot()).unwrap();
        std::fs::write(&session.temp, b"second save").unwrap();
        let second = rt.block_on(session.snapshot()).unwrap();

        assert_ne!(first, second);
        assert_eq!(first.file_name(), second.file_name());
        assert_eq!(std::fs::read(&first).unwrap(), b"first save");
        assert_eq!(std::fs::read(&second).unwrap(), b"second save");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_retry_of_a_failed_save_uploads_it() {
        let _channels = exclusive_channels();
        let (rt, dir, provider, queue) = fixture("retry");
        let remote_file = dir.join("notes.txt");
        let parked = dir.join("parked.txt");
        let remote_path = remote_file.to_str().unwrap().trim_start_matches('/').to_string();
        let session = Arc::new(
            rt.block_on(Session::open(provider, remote_path, "notes.txt".into(), queue))
                .unwrap(),
        );
        let _watcher = rt.block_on(async { watch(session.clone()) }).unwrap();

        // The server copy can't be checked, so the save fails.
        std::fs::rename(&remote_file, &parked).unwrap();
        std::fs::write(&session.temp, b"v2 local").unwrap();
        rt.block_on(session.flush());
        let notices = subscribe_notices();
        let failure = loop {
            if let EditNotice::Failed(failure) = notices.recv_blocking().unwrap() {
                break failure;
            }
        };
        assert_eq!(std::fs::read(&session.temp).unwrap(), b"v2 local");

        std::fs::rename(&parked, &remote_file).unwrap();
        failure.retry.notify_one();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::fs::read(&remote_file).unwrap() != b"v2 local" {
            assert!(std::time::Instant::now() < deadline, "the retry never uploaded");
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn closing_removes_clean_copies_and_keeps_unsaved_ones() {
        let (rt, dir, provider, queue) = fixture("close");
        let open = |name: &str| {
            let file = dir.join(name);
            std::fs::write(&file, b"v1").unwrap();
            let remote_path = file.to_str().unwrap().trim_start_matches('/').to_string();
            rt.block_on(Session::open(provider.clone(), remote_path, name.into(), queue.clone()))
                .unwrap()
        };
        let clean = open("clean.txt");
        let unsaved = open("unsaved.txt");
        std::fs::write(&unsaved.temp, b"edits the server never got").unwrap();

        clean.close();
        unsaved.close();

        assert!(!clean.dir.exists());
        assert_eq!(std::fs::read(&unsaved.temp).unwrap(), b"edits the server never got");
        let _ = std::fs::remove_dir_all(&unsaved.dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
