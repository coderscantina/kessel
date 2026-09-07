//! Windows drag-out: an OLE drag session started with `SHDoDragDrop` on
//! gpui's `HWND` (reached through the raw window handle). `SHDoDragDrop` runs
//! its own modal loop and returns when the drop lands or the drag is
//! cancelled, exactly like every other Win32 drag source.
//!
//! Two shapes of data object, picked by what is being dragged:
//!
//! - **Every item already local** — `CF_HDROP`, a plain list of paths. The
//!   shell copies them itself; kessel isn't involved past the drop.
//! - **Anything remote** — the shell's file-promise protocol:
//!   `CFSTR_FILEDESCRIPTORW` names the files up front, and the target pulls
//!   each one's bytes with `CFSTR_FILECONTENTS`. Nothing is transferred for a
//!   drag that ends in a cancel, which is the whole point of the promise.
//!
//! The two are deliberately never offered together: a target that understands
//! both prefers `CF_HDROP` and would silently drop the promised half of a
//! mixed selection, so a drag with anything remote in it promises everything —
//! local items included, which just makes their fulfilment a local copy.
//!
//! Promised bytes are produced by the fulfill callback into a per-drag staging
//! directory and handed to the shell as a read-only `IStream` over that file.
//! The object declares `IDataObjectAsyncCapability`, so the shell pulls them on
//! its own thread, with its own progress dialog, instead of blocking kessel for
//! the length of a transfer. That means calls arrive on a thread that didn't
//! create the object — legal because `#[implement]` makes it agile (an
//! `IAgileObject` backed by the free-threaded marshaler) and everything it
//! holds is `Send + Sync`.
//!
//! **Remote directories are not promised.** A descriptor set has to name every
//! file in the drag up front, so promising a folder would mean walking the
//! remote tree inside the mouse handler. A drag carrying one is declined here
//! and stays an internal gpui drag — the same as before this module existed.

use std::ffi::c_void;
use std::os::windows::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use gpui::Window;
use raw_window_handle::{HasWindowHandle, RawWindowHandle};
use windows::core::{implement, Error, Ref, Result, BOOL, HRESULT, PCWSTR};
use windows::Win32::Foundation::{
    GlobalFree, DV_E_FORMATETC, DV_E_LINDEX, DV_E_TYMED, E_NOTIMPL, E_UNEXPECTED, HGLOBAL, HWND,
    OLE_E_ADVISENOTSUPPORTED, POINT, S_OK,
};
use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_NORMAL;
use windows::Win32::System::Com::{
    IAdviseSink, IBindCtx, IDataObject, IDataObject_Impl, IEnumFORMATETC, IEnumSTATDATA,
    DVASPECT_CONTENT, FORMATETC, STGMEDIUM, STGMEDIUM_0, TYMED_HGLOBAL, TYMED_ISTREAM,
};
use windows::Win32::System::DataExchange::RegisterClipboardFormatW;
use windows::Win32::System::Memory::{GlobalAlloc, GlobalLock, GlobalUnlock, GMEM_MOVEABLE};
use windows::Win32::System::Ole::{CF_HDROP, DROPEFFECT_COPY};
use windows::Win32::UI::Shell::{
    IDataObjectAsyncCapability, IDataObjectAsyncCapability_Impl, SHCreateStdEnumFmtEtc,
    SHCreateStreamOnFileEx, SHDoDragDrop, DROPFILES, FD_ATTRIBUTES, FD_PROGRESSUI, FILEDESCRIPTORW,
    FILEGROUPDESCRIPTORW,
};

use super::{DragItem, FulfillFn};

/// `STGM_READ`: the stream handed to the shell is read-only.
const STGM_READ: u32 = 0;
/// `DATADIR_GET` — the only direction this object enumerates.
const DATADIR_GET: u32 = 1;
/// `cFileName` is a fixed-size buffer that the shell reads as a C string.
const MAX_FILE_NAME: usize = 259;

/// One promised entry: the name the target will create it under, and the index
/// the fulfill callback knows it by.
struct Promise {
    name: String,
    index: usize,
}

/// The data object backing one drag. The shell holds it (and, for an async
/// extraction, hands it to its worker thread) until the drop is done with it.
#[implement(IDataObject, IDataObjectAsyncCapability)]
struct DragData {
    /// Paths for the `CF_HDROP` payload; empty on a promise drag.
    local: Vec<PathBuf>,
    /// Promised files in descriptor order; empty on a `CF_HDROP` drag.
    promises: Vec<Promise>,
    fulfill: FulfillFn,
    /// Where promised files are materialized; removed with the object.
    staging: PathBuf,
    /// The shell's named clipboard formats, registered once at construction.
    descriptor_format: u16,
    contents_format: u16,
    async_mode: AtomicBool,
    in_operation: AtomicBool,
}

impl Drop for DragData {
    fn drop(&mut self) {
        // Best-effort: whatever the drop target wanted has been copied by now.
        let _ = std::fs::remove_dir_all(&self.staging);
    }
}

impl DragData {
    /// The formats this object offers, in preference order — the target takes
    /// the first one it understands.
    fn formats(&self) -> Vec<FORMATETC> {
        let mut formats = Vec::new();
        if !self.local.is_empty() {
            formats.push(format_etc(CF_HDROP.0, TYMED_HGLOBAL.0 as u32, -1));
        }
        if !self.promises.is_empty() {
            formats.push(format_etc(self.descriptor_format, TYMED_HGLOBAL.0 as u32, -1));
            // One entry per promised file: `lindex` is which file is meant.
            for index in 0..self.promises.len() {
                formats.push(format_etc(
                    self.contents_format,
                    TYMED_ISTREAM.0 as u32,
                    index as i32,
                ));
            }
        }
        formats
    }

    /// Whether `request` names one of the formats above. `lindex` is left to
    /// `contents`, which range-checks it.
    fn supports(&self, request: &FORMATETC) -> bool {
        self.formats().iter().any(|offered| {
            offered.cfFormat == request.cfFormat
                && offered.dwAspect == request.dwAspect
                && request.tymed & offered.tymed != 0
        })
    }

    /// `CF_HDROP`: a `DROPFILES` header followed by the paths, NUL-separated
    /// and NUL-terminated.
    fn hdrop(&self) -> Result<STGMEDIUM> {
        let mut list: Vec<u16> = Vec::new();
        for path in &self.local {
            list.extend(path.as_os_str().encode_wide());
            list.push(0);
        }
        list.push(0);

        let header = size_of::<DROPFILES>();
        let global = GlobalMemory::new(header + list.len() * size_of::<u16>())?;
        unsafe {
            let base = global.lock()?;
            base.cast::<DROPFILES>().write(DROPFILES {
                pFiles: header as u32,
                pt: POINT::default(),
                fNC: false.into(),
                fWide: true.into(),
            });
            std::ptr::copy_nonoverlapping(
                list.as_ptr(),
                base.byte_add(header).cast::<u16>(),
                list.len(),
            );
            global.unlock();
        }
        Ok(global.into_medium())
    }

    /// `CFSTR_FILEDESCRIPTORW`: a count followed by one `FILEDESCRIPTORW` per
    /// promised file.
    fn descriptors(&self) -> Result<STGMEDIUM> {
        // FILEGROUPDESCRIPTORW carries the first descriptor inline.
        let bytes = size_of::<FILEGROUPDESCRIPTORW>()
            + self.promises.len().saturating_sub(1) * size_of::<FILEDESCRIPTORW>();
        let global = GlobalMemory::new(bytes)?;
        unsafe {
            let base = global.lock()?;
            let group = base.cast::<FILEGROUPDESCRIPTORW>();
            (&raw mut (*group).cItems).write(self.promises.len() as u32);
            let first = (&raw mut (*group).fgd).cast::<FILEDESCRIPTORW>();
            for (slot, promise) in self.promises.iter().enumerate() {
                // Filled in a local and moved in whole: FILEDESCRIPTORW is a
                // packed struct, so its fields can't be borrowed in place.
                // Over-long names are truncated rather than rejected — the
                // buffer is fixed-size and must stay NUL-terminated.
                let mut name = [0u16; 260];
                for (cell, unit) in name
                    .iter_mut()
                    .zip(promise.name.encode_utf16().take(MAX_FILE_NAME))
                {
                    *cell = unit;
                }
                first.add(slot).write(FILEDESCRIPTORW {
                    // FD_ATTRIBUTES marks dwFileAttributes as meaningful;
                    // FD_PROGRESSUI asks the shell for its own progress UI,
                    // since a promise takes as long as the transfer does.
                    dwFlags: (FD_ATTRIBUTES.0 | FD_PROGRESSUI.0) as u32,
                    dwFileAttributes: FILE_ATTRIBUTE_NORMAL.0,
                    cFileName: name,
                    ..Default::default()
                });
            }
            global.unlock();
        }
        Ok(global.into_medium())
    }

    /// `CFSTR_FILECONTENTS`: run the promise for `index` (once) and hand back
    /// a read-only stream over the file it produced.
    fn contents(&self, index: usize) -> Result<STGMEDIUM> {
        let promise = self.promises.get(index).ok_or_else(|| Error::from(DV_E_LINDEX))?;
        // A directory per item, so two promises sharing a file name can't
        // collide, and a retried request reuses what was already fetched.
        let dir = self.staging.join(promise.index.to_string());
        let path = dir.join(&promise.name);
        if !path.exists() {
            std::fs::create_dir_all(&dir).map_err(|error| {
                tracing::error!(%error, dir = %dir.display(), "staging a dragged file failed");
                Error::from(E_UNEXPECTED)
            })?;
            (self.fulfill)(promise.index, &path).map_err(|error| {
                tracing::error!(error = %format!("{error:#}"), "drag-out promise failed");
                Error::from(E_UNEXPECTED)
            })?;
        }

        let wide = wide_path(&path);
        let stream =
            unsafe { SHCreateStreamOnFileEx(PCWSTR(wide.as_ptr()), STGM_READ, 0, false, None)? };
        Ok(STGMEDIUM {
            tymed: TYMED_ISTREAM.0 as u32,
            u: STGMEDIUM_0 {
                pstm: std::mem::ManuallyDrop::new(Some(stream)),
            },
            pUnkForRelease: std::mem::ManuallyDrop::new(None),
        })
    }
}

impl IDataObject_Impl for DragData_Impl {
    fn GetData(&self, request: *const FORMATETC) -> Result<STGMEDIUM> {
        let request = unsafe { request.as_ref() }.ok_or_else(|| Error::from(E_UNEXPECTED))?;
        if !self.supports(request) {
            return Err(DV_E_FORMATETC.into());
        }
        if request.cfFormat == CF_HDROP.0 {
            self.hdrop()
        } else if request.cfFormat == self.descriptor_format {
            self.descriptors()
        } else if request.cfFormat == self.contents_format {
            self.contents(request.lindex.max(0) as usize)
        } else {
            Err(DV_E_FORMATETC.into())
        }
    }

    /// Only for caller-allocated media; everything here is allocated by
    /// `GetData` itself.
    fn GetDataHere(&self, _request: *const FORMATETC, _medium: *mut STGMEDIUM) -> Result<()> {
        Err(DV_E_TYMED.into())
    }

    fn QueryGetData(&self, request: *const FORMATETC) -> HRESULT {
        match unsafe { request.as_ref() } {
            Some(request) if self.supports(request) => S_OK,
            Some(_) => DV_E_FORMATETC,
            None => E_UNEXPECTED,
        }
    }

    /// No device-specific renderings to collapse.
    fn GetCanonicalFormatEtc(&self, _request: *const FORMATETC, _out: *mut FORMATETC) -> HRESULT {
        E_NOTIMPL
    }

    /// Read-only: the object renders what the drag was started with.
    fn SetData(
        &self,
        _request: *const FORMATETC,
        _medium: *const STGMEDIUM,
        _release: BOOL,
    ) -> Result<()> {
        Err(E_NOTIMPL.into())
    }

    fn EnumFormatEtc(&self, direction: u32) -> Result<IEnumFORMATETC> {
        if direction != DATADIR_GET {
            return Err(E_NOTIMPL.into());
        }
        unsafe { SHCreateStdEnumFmtEtc(&self.formats()) }
    }

    fn DAdvise(
        &self,
        _request: *const FORMATETC,
        _flags: u32,
        _sink: Ref<'_, IAdviseSink>,
    ) -> Result<u32> {
        Err(OLE_E_ADVISENOTSUPPORTED.into())
    }

    fn DUnadvise(&self, _connection: u32) -> Result<()> {
        Err(OLE_E_ADVISENOTSUPPORTED.into())
    }

    fn EnumDAdvise(&self) -> Result<IEnumSTATDATA> {
        Err(OLE_E_ADVISENOTSUPPORTED.into())
    }
}

/// Lets the shell pull promised files on its own thread, with its own progress
/// dialog, instead of blocking kessel's UI for the length of a transfer.
impl IDataObjectAsyncCapability_Impl for DragData_Impl {
    fn SetAsyncMode(&self, enabled: BOOL) -> Result<()> {
        self.async_mode.store(enabled.as_bool(), Ordering::Relaxed);
        Ok(())
    }

    fn GetAsyncMode(&self) -> Result<BOOL> {
        Ok(self.async_mode.load(Ordering::Relaxed).into())
    }

    fn StartOperation(&self, _reserved: Ref<'_, IBindCtx>) -> Result<()> {
        self.in_operation.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn InOperation(&self) -> Result<BOOL> {
        Ok(self.in_operation.load(Ordering::Relaxed).into())
    }

    fn EndOperation(
        &self,
        result: HRESULT,
        _reserved: Ref<'_, IBindCtx>,
        _effects: u32,
    ) -> Result<()> {
        self.in_operation.store(false, Ordering::Relaxed);
        if result.is_err() {
            tracing::warn!(?result, "the drop target reported a failed extraction");
        }
        Ok(())
    }
}

/// Everything but a drag carrying a remote folder, which can't be promised —
/// see the module docs.
pub(super) fn can_drag(items: &[DragItem]) -> bool {
    if items.is_empty() {
        return false;
    }
    let all_local = items.iter().all(|item| item.local_path.is_some());
    if !all_local && items.iter().any(|item| item.local_path.is_none() && item.is_dir) {
        tracing::info!("dragging remote folders out is not supported on Windows");
        return false;
    }
    true
}

pub(super) fn start_drag(window: &Window, items: Vec<DragItem>, fulfill: FulfillFn) -> bool {
    if !can_drag(&items) {
        return false;
    }
    // gpui's own `Window::window_handle` (an `AnyWindowHandle`) shadows the
    // raw-window-handle trait method, so call the trait explicitly.
    let Ok(handle) = HasWindowHandle::window_handle(window) else {
        return false;
    };
    let RawWindowHandle::Win32(win32) = handle.as_raw() else {
        return false;
    };
    let hwnd = HWND(win32.hwnd.get() as *mut c_void);

    let all_local = items.iter().all(|item| item.local_path.is_some());
    let (local, promises) = if all_local {
        let paths = items.iter().filter_map(|item| item.local_path.clone()).collect();
        (paths, Vec::new())
    } else {
        let promises = items
            .iter()
            .enumerate()
            .map(|(index, item)| Promise {
                name: item.name.clone(),
                index,
            })
            .collect();
        (Vec::new(), promises)
    };

    let descriptor_format = clipboard_format("FileGroupDescriptorW");
    let contents_format = clipboard_format("FileContents");
    if !all_local && (descriptor_format == 0 || contents_format == 0) {
        tracing::error!("registering the shell's file-promise clipboard formats failed");
        return false;
    }

    let data: IDataObject = DragData {
        local,
        promises,
        fulfill,
        staging: staging_dir(),
        descriptor_format,
        contents_format,
        async_mode: AtomicBool::new(false),
        in_operation: AtomicBool::new(false),
    }
    .into();

    // Blocks in its own modal loop until the drop lands or is cancelled, and
    // draws the drag image from the data object — the same thing Explorer does
    // for its own drags.
    match unsafe { SHDoDragDrop(Some(hwnd), &data, None, DROPEFFECT_COPY) } {
        Ok(effect) => tracing::debug!(effect = effect.0, "drag-out finished"),
        Err(error) => tracing::error!(%error, "starting the drag session failed"),
    }
    true
}

/// A staging directory unique to this drag: the object may outlive the drag
/// (async extraction), and several can be alive at once.
fn staging_dir() -> PathBuf {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let serial = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("kessel-drag-{}-{serial}", std::process::id()))
}

/// `RegisterClipboardFormatW` for one of the shell's named formats; `0` on
/// failure, which the caller treats as "no promise drag".
fn clipboard_format(name: &str) -> u16 {
    let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
    unsafe { RegisterClipboardFormatW(PCWSTR(wide.as_ptr())) as u16 }
}

fn format_etc(format: u16, tymed: u32, lindex: i32) -> FORMATETC {
    FORMATETC {
        cfFormat: format,
        ptd: std::ptr::null_mut(),
        dwAspect: DVASPECT_CONTENT.0,
        lindex,
        tymed,
    }
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(std::iter::once(0)).collect()
}

/// An `HGLOBAL` that frees itself unless it is handed to the shell.
struct GlobalMemory(HGLOBAL);

impl GlobalMemory {
    fn new(bytes: usize) -> Result<Self> {
        Ok(Self(unsafe { GlobalAlloc(GMEM_MOVEABLE, bytes) }?))
    }

    fn lock(&self) -> Result<*mut u8> {
        let base = unsafe { GlobalLock(self.0) };
        if base.is_null() {
            return Err(E_UNEXPECTED.into());
        }
        Ok(base.cast())
    }

    /// # Safety
    /// Only valid after a matching [`lock`](Self::lock); the pointer it
    /// returned must not be used again.
    unsafe fn unlock(&self) {
        // Fails once the lock count reaches zero, which is the normal case.
        let _ = unsafe { GlobalUnlock(self.0) };
    }

    /// Hand ownership to the shell, which frees the medium when it is done.
    fn into_medium(self) -> STGMEDIUM {
        let handle = self.0;
        std::mem::forget(self);
        STGMEDIUM {
            tymed: TYMED_HGLOBAL.0 as u32,
            u: STGMEDIUM_0 { hGlobal: handle },
            pUnkForRelease: std::mem::ManuallyDrop::new(None),
        }
    }
}

impl Drop for GlobalMemory {
    fn drop(&mut self) {
        let _ = unsafe { GlobalFree(Some(self.0)) };
    }
}
