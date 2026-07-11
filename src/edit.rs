//! Edit-in-external-editor sessions: download the remote file to a temp
//! path, open it with the OS default app, watch it for saves, and re-upload
//! each save through the transfer queue (Overwrite policy), so retries,
//! throttling and progress all come for free.
//!
//! Each session remembers what the *server* copy looked like when it was
//! last in sync (etag / mtime / size). Before re-uploading a save, the
//! remote is stat'ed again — if someone else changed it in the meantime the
//! session raises a conflict instead of silently clobbering their work; the
//! app shows a dialog (see [`open_conflict_dialog`]) and the session waits
//! for the decision.
//!
//! Sessions live for the rest of the process — an editor may keep the file
//! open indefinitely, and a leaked watcher per edited file is cheap.

use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context as _, Result};
use gpui::prelude::*;
use gpui::{px, App, Window};
use gpui_component::button::Button;
use gpui_component::{v_flex, ActiveTheme as _, Sizable as _, WindowExt as _};
use notify::Watcher as _;

use crate::provider::Provider;
use crate::queue::{ConflictPolicy, NewJob, QueueHandle, LOCAL_SITE_ID};

/// Keeps watchers alive; dropping a `RecommendedWatcher` stops it.
static SESSIONS: Mutex<Vec<notify::RecommendedWatcher>> = Mutex::new(Vec::new());

/// What the remote held when we last were in sync with it.
#[derive(Debug, Clone, PartialEq)]
struct RemoteState {
    etag: Option<String>,
    modified: Option<String>,
    size: u64,
}

/// A save collided with a server-side change; the session blocks on
/// `decision` until the user picks a side.
pub struct EditConflict {
    pub name: String,
    pub remote_modified: Option<String>,
    pub decision: mpsc::Sender<EditDecision>,
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

type ConflictChannel = (
    smol::channel::Sender<EditConflict>,
    smol::channel::Receiver<EditConflict>,
);

fn channel() -> &'static ConflictChannel {
    static CHANNEL: OnceLock<ConflictChannel> = OnceLock::new();
    CHANNEL.get_or_init(|| smol::channel::unbounded())
}

/// Conflicts raised by edit sessions; the app listens and opens the dialog.
pub fn subscribe() -> smol::channel::Receiver<EditConflict> {
    channel().1.clone()
}

/// Runs on the app tokio runtime: download `path` from `provider` into a
/// temp file, open it, and keep re-uploading saves into `dst_dir`/`name`.
pub async fn start(
    provider: Provider,
    path: String,
    name: String,
    queue: QueueHandle,
) -> Result<PathBuf> {
    let dir = std::env::temp_dir()
        .join("kessel-edit")
        .join(format!("{}-{:x}", provider.site_id, fxhash(&path)));
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating `{}`", dir.display()))?;
    let temp = dir.join(&name);
    provider.read_to_file(&path, &temp).await?;
    let baseline = stat_remote(&provider, &path).await.ok();

    let dst_dir = parent_of(&path);
    let session = Session {
        provider,
        remote_path: path,
        temp: temp.clone(),
        dst_dir,
        name,
        queue,
        runtime: tokio::runtime::Handle::current(),
        baseline: Mutex::new(baseline),
    };
    watch_and_upload(session)?;

    crate::ui::open_path_or_url(temp.to_string_lossy().as_ref());
    Ok(temp)
}

struct Session {
    provider: Provider,
    remote_path: String,
    temp: PathBuf,
    dst_dir: String,
    name: String,
    queue: QueueHandle,
    runtime: tokio::runtime::Handle,
    /// Remote state at the last known sync point; `None` means unknown
    /// (conflict detection sits out one round rather than block editing).
    baseline: Mutex<Option<RemoteState>>,
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

/// Watch one temp file; each quiet period after a burst of writes enqueues
/// an upload back to its origin — unless the server copy changed since the
/// last sync, in which case the user decides.
fn watch_and_upload(session: Session) -> Result<()> {
    let (tx, rx) = mpsc::channel::<()>();
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        if let Ok(event) = result {
            if matches!(
                event.kind,
                notify::EventKind::Modify(_) | notify::EventKind::Create(_)
            ) {
                tx.send(()).ok();
            }
        }
    })?;
    watcher.watch(&session.temp, notify::RecursiveMode::NonRecursive)?;
    SESSIONS.lock().unwrap().push(watcher);

    // Editors save in bursts (write + rename, backup files, …): wait for
    // 750ms of quiet before uploading, and coalesce everything in between.
    std::thread::spawn(move || {
        while rx.recv().is_ok() {
            while rx.recv_timeout(Duration::from_millis(750)).is_ok() {}
            match session.check_conflict() {
                Some(conflict_state) => {
                    if !session.resolve_conflict(conflict_state) {
                        continue;
                    }
                }
                None => {}
            }
            session.upload();
            // Swallow watcher noise our own re-download may have caused.
            while rx.try_recv().is_ok() {}
        }
    });
    Ok(())
}

impl Session {
    fn src(&self) -> String {
        self.temp.to_string_lossy().trim_start_matches('/').to_string()
    }

    /// The remote's current state when it differs from the sync baseline.
    fn check_conflict(&self) -> Option<RemoteState> {
        let baseline = self.baseline.lock().unwrap().clone()?;
        let current = self
            .runtime
            .block_on(stat_remote(&self.provider, &self.remote_path))
            .ok()?;
        (current != baseline).then_some(current)
    }

    /// Raise the conflict to the UI and wait. Returns true when the local
    /// save should still be uploaded.
    fn resolve_conflict(&self, remote: RemoteState) -> bool {
        tracing::warn!(
            file = %self.name, remote = %self.remote_path,
            "external edit conflicts with a server-side change"
        );
        let (tx, rx) = mpsc::channel();
        let conflict = EditConflict {
            name: self.name.clone(),
            remote_modified: remote.modified.clone(),
            decision: tx,
        };
        if channel().0.try_send(conflict).is_err() {
            // No UI listening (shutdown?): err on the safe side, skip upload.
            return false;
        }
        // A closed dialog (or dropped sender) counts as Cancel.
        match rx.recv().unwrap_or(EditDecision::Cancel) {
            EditDecision::Overwrite => true,
            EditDecision::DiscardLocal => {
                *self.baseline.lock().unwrap() = Some(remote);
                if let Err(error) = self
                    .runtime
                    .block_on(self.provider.read_to_file(&self.remote_path, &self.temp))
                {
                    tracing::error!(%error, "re-downloading the server copy failed");
                }
                false
            }
            EditDecision::Cancel => false,
        }
    }

    fn upload(&self) {
        tracing::info!(file = %self.name, "external edit saved; re-uploading");
        let before = self.baseline.lock().unwrap().clone();
        self.queue.enqueue(NewJob {
            src_site_id: LOCAL_SITE_ID,
            dst_site_id: self.provider.site_id,
            src: self.src(),
            dst_dir: self.dst_dir.clone(),
            dst_name: Some(self.name.clone()),
            conflict_policy: ConflictPolicy::Overwrite,
            move_source: false,
        });
        // Re-baseline once the upload lands: poll until the remote state
        // moves off the pre-upload snapshot. Times out into `None`, which
        // skips conflict detection for one save instead of false-alarming.
        let new_baseline = self.runtime.block_on(async {
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Ok(state) = stat_remote(&self.provider, &self.remote_path).await {
                    if before.as_ref() != Some(&state) {
                        return Some(state);
                    }
                }
            }
            None
        });
        if new_baseline.is_none() {
            tracing::debug!(file = %self.name, "post-upload re-baseline timed out");
        }
        *self.baseline.lock().unwrap() = new_baseline;
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
