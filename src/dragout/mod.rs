//! Native outbound drag and drop — dragging entries out of kessel into Finder,
//! Explorer, or any other app. gpui only supports *incoming* external drags
//! (`ExternalPaths`), so the outbound side is patched on top of it: when an
//! internal row drag crosses the window edge, the workspace stops gpui's drag
//! and hands the gesture to an OS dragging session started on the raw window
//! handle.
//!
//! Local entries are handed over as real paths, which the OS copies or moves
//! itself. Remote entries become file *promises*, fulfilled through the
//! transfer queue only when a destination actually accepts the drop — nothing
//! is downloaded for a drag that ends in a cancel.
//!
//! Each platform expresses that in its own idiom, behind the same two calls
//! below: `NSFilePromiseProvider` on a pasteboard (see `macos`), the shell's
//! `FILEGROUPDESCRIPTOR`/`FILECONTENTS` data object (see `windows`).

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
#[cfg(target_os = "windows")]
mod windows;

/// Whether the OS drag session has to be started from inside the mouse-event
/// dispatch that triggered it.
///
/// macOS does: `beginDraggingSessionWithItems:event:source:` wants the very
/// event being dispatched. Windows must not: `SHDoDragDrop` runs a modal
/// message loop of its own, and pumping messages from inside gpui's event
/// dispatch would re-enter it — so the caller defers the start instead.
pub const STARTS_IN_EVENT_DISPATCH: bool = cfg!(target_os = "macos");

/// Whether this platform can express `items` as an OS drag at all. Asked
/// before the internal drag is given up, so a selection the platform can't
/// carry keeps the in-app drag rather than losing the gesture entirely.
pub fn can_drag(items: &[DragItem]) -> bool {
    #[cfg(target_os = "macos")]
    {
        !items.is_empty()
    }
    #[cfg(target_os = "windows")]
    {
        windows::can_drag(items)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = items;
        false
    }
}

/// Start an OS drag session for `items` out of `window`. Returns `false`
/// when no session could be started (unsupported platform, no suitable
/// mouse event, or a selection this platform can't express) — the caller
/// keeps its internal drag in that case.
pub fn start_drag(window: &Window, items: Vec<DragItem>, fulfill: FulfillFn) -> bool {
    #[cfg(target_os = "macos")]
    {
        macos::start_drag(window, items, fulfill)
    }
    #[cfg(target_os = "windows")]
    {
        windows::start_drag(window, items, fulfill)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = (window, items, fulfill);
        false
    }
}
