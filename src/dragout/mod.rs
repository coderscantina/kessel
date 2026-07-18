//! Native outbound drag and drop — dragging entries out of kessel into
//! Finder or any other app. gpui only supports *incoming* external drags
//! (`ExternalPaths`), so the outbound side is patched on top of it: when an
//! internal row drag crosses the window edge, the workspace stops gpui's
//! drag and hands the gesture to an OS dragging session started on the raw
//! window handle.
//!
//! Local entries ride the pasteboard as real `file://` URLs (the OS copies
//! or moves them itself); remote entries become file *promises*, fulfilled
//! through the transfer queue only when a destination actually accepts the
//! drop — nothing is downloaded for a drag that ends in a cancel.

use std::path::{Path, PathBuf};

use gpui::Window;

/// One dragged entry, described for the OS.
pub struct DragItem {
    /// File name promised to the destination (also picks the drag icon).
    pub name: String,
    pub is_dir: bool,
    /// Already on this machine: dragged as a real URL, no promise involved.
    pub local_path: Option<PathBuf>,
}

/// Fulfill one promised item: produce the file/directory at `dest` (the
/// exact path the destination chose, e.g. `~/Desktop/report.pdf`) and return
/// once it exists. Called on a background thread; blocking is expected.
pub type FulfillFn = Box<dyn Fn(usize, &Path) -> anyhow::Result<()> + Send + Sync>;

#[cfg(target_os = "macos")]
mod macos;

/// Start an OS drag session for `items` out of `window`. Returns `false`
/// when no session could be started (unsupported platform, no suitable
/// mouse event) — the caller keeps its internal drag in that case.
pub fn start_drag(window: &Window, items: Vec<DragItem>, fulfill: FulfillFn) -> bool {
    #[cfg(target_os = "macos")]
    {
        macos::start_drag(window, items, fulfill)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (window, items, fulfill);
        false
    }
}
