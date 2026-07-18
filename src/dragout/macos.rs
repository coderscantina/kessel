//! macOS drag-out: an `NSDraggingSession` begun on gpui's `NSView` (reached
//! through the raw window handle) from inside the `mouseDragged` dispatch —
//! `NSApp.currentEvent` is that very event, which is exactly what
//! `beginDraggingSessionWithItems:event:source:` wants.
//!
//! Remote entries are `NSFilePromiseProvider`s; Finder resolves the final
//! destination path (unique-naming included) and calls the delegate on a
//! background operation queue, where the fulfill callback blocks until the
//! transfer queue has written the file. Local entries are plain `NSURL`s.

use std::cell::RefCell;
use std::path::PathBuf;

use gpui::Window;
use objc2::rc::Retained;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{define_class, msg_send, AllocAnyThread, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSApplication, NSDragOperation, NSDraggingContext, NSDraggingItem, NSDraggingSession,
    NSDraggingSource, NSEventType, NSFilePromiseProvider, NSFilePromiseProviderDelegate, NSImage,
    NSPasteboardWriting, NSView, NSWorkspace,
};
use objc2_foundation::{
    NSArray, NSDictionary, NSError, NSLocalizedDescriptionKey, NSNumber, NSObject,
    NSObjectProtocol, NSOperationQueue, NSPoint, NSRect, NSSize, NSString, NSURL,
};
use raw_window_handle::{HasWindowHandle, RawWindowHandle};

use super::{DragItem, FulfillFn};

/// State shared by every promise provider of one drag session.
struct PromiseIvars {
    /// Promised file names, indexed by the provider's `userInfo`.
    names: Vec<String>,
    fulfill: FulfillFn,
    /// Background queue promises are written on, so a long download never
    /// blocks the main thread.
    queue: Retained<NSOperationQueue>,
}

define_class!(
    #[unsafe(super(NSObject))]
    #[name = "KesselFilePromiseDelegate"]
    #[ivars = PromiseIvars]
    struct PromiseDelegate;

    unsafe impl NSObjectProtocol for PromiseDelegate {}

    unsafe impl NSFilePromiseProviderDelegate for PromiseDelegate {
        #[unsafe(method_id(filePromiseProvider:fileNameForType:))]
        fn file_name_for_type(
            &self,
            provider: &NSFilePromiseProvider,
            _file_type: &NSString,
        ) -> Retained<NSString> {
            let name = provider_index(provider)
                .and_then(|ix| self.ivars().names.get(ix))
                .map(String::as_str)
                .unwrap_or("file");
            NSString::from_str(name)
        }

        #[unsafe(method(filePromiseProvider:writePromiseToURL:completionHandler:))]
        fn write_promise_to_url(
            &self,
            provider: &NSFilePromiseProvider,
            url: &NSURL,
            completion: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            let dest = url.path().map(|path| PathBuf::from(path.to_string()));
            let result = match (provider_index(provider), dest) {
                (Some(ix), Some(dest)) => (self.ivars().fulfill)(ix, &dest),
                _ => Err(anyhow::anyhow!("unusable file promise destination")),
            };
            match result {
                Ok(()) => completion.call((std::ptr::null_mut(),)),
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), "drag-out promise failed");
                    let description = NSString::from_str(&format!("{error:#}"));
                    let user_info = NSDictionary::from_slices(
                        &[unsafe { NSLocalizedDescriptionKey }],
                        &[description.as_ref() as &AnyObject],
                    );
                    let error = unsafe {
                        NSError::errorWithDomain_code_userInfo(
                            &NSString::from_str("net.badsoft.kessel"),
                            1,
                            Some(&user_info),
                        )
                    };
                    completion.call((Retained::as_ptr(&error) as *mut NSError,));
                }
            }
        }

        #[unsafe(method_id(operationQueueForFilePromiseProvider:))]
        fn operation_queue_for(&self, _provider: &NSFilePromiseProvider) -> Retained<NSOperationQueue> {
            self.ivars().queue.clone()
        }
    }
);

impl PromiseDelegate {
    fn new(names: Vec<String>, fulfill: FulfillFn) -> Retained<Self> {
        let queue = NSOperationQueue::new();
        queue.setMaxConcurrentOperationCount(4);
        let this = Self::alloc().set_ivars(PromiseIvars { names, fulfill, queue });
        unsafe { msg_send![super(this), init] }
    }
}

/// The provider's `userInfo` carries the item index as an `NSNumber`.
fn provider_index(provider: &NSFilePromiseProvider) -> Option<usize> {
    let info = provider.userInfo()?;
    let number = info.downcast::<NSNumber>().ok()?;
    Some(number.unsignedIntegerValue())
}

define_class!(
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "KesselDragSource"]
    #[ivars = ()]
    struct DragSource;

    unsafe impl NSObjectProtocol for DragSource {}

    unsafe impl NSDraggingSource for DragSource {
        #[unsafe(method(draggingSession:sourceOperationMaskForDraggingContext:))]
        fn source_operation_mask(
            &self,
            _session: &NSDraggingSession,
            context: NSDraggingContext,
        ) -> NSDragOperation {
            // Within our own windows the internal gpui drag (which was
            // stopped on promotion) is the intended path; refuse the native
            // one so a drop back inside just cancels.
            match context {
                NSDraggingContext::OutsideApplication => NSDragOperation::Copy,
                _ => NSDragOperation::None,
            }
        }
    }
);

/// The pasteboard only holds *weak* delegate references, and promises are
/// redeemed after the session (sometimes long after — a paste of a promised
/// file). Everything a session needs therefore stays retained here until the
/// next drag replaces it.
struct SessionKeep {
    _source: Retained<DragSource>,
    _delegate: Retained<PromiseDelegate>,
}

thread_local! {
    static ACTIVE_SESSION: RefCell<Option<SessionKeep>> = const { RefCell::new(None) };
}

pub(super) fn start_drag(window: &Window, items: Vec<DragItem>, fulfill: FulfillFn) -> bool {
    let Some(mtm) = MainThreadMarker::new() else {
        return false;
    };
    // gpui's own `Window::window_handle` (an `AnyWindowHandle`) shadows the
    // raw-window-handle trait method, so call the trait explicitly.
    let Ok(handle) = HasWindowHandle::window_handle(window) else {
        return false;
    };
    let RawWindowHandle::AppKit(appkit) = handle.as_raw() else {
        return false;
    };
    if items.is_empty() {
        return false;
    }

    // Only a genuine in-flight left-button drag can seed a session.
    let app = NSApplication::sharedApplication(mtm);
    let Some(event) = app.currentEvent() else {
        return false;
    };
    if !matches!(
        event.r#type(),
        NSEventType::LeftMouseDragged | NSEventType::LeftMouseDown
    ) {
        return false;
    }

    let view = unsafe { appkit.ns_view.cast::<NSView>().as_ref() };
    let origin = view.convertPoint_fromView(event.locationInWindow(), None);

    let names: Vec<String> = items.iter().map(|item| item.name.clone()).collect();
    let delegate = PromiseDelegate::new(names, fulfill);

    let workspace = NSWorkspace::sharedWorkspace();
    let mut drag_items: Vec<Retained<NSDraggingItem>> = Vec::with_capacity(items.len());
    for (ix, item) in items.iter().enumerate() {
        let writer: Retained<ProtocolObject<dyn NSPasteboardWriting>> = match &item.local_path {
            Some(path) => {
                let url = NSURL::fileURLWithPath(&NSString::from_str(&path.to_string_lossy()));
                ProtocolObject::from_retained(url)
            }
            None => {
                let file_type = if item.is_dir { "public.folder" } else { "public.data" };
                let provider = NSFilePromiseProvider::initWithFileType_delegate(
                    NSFilePromiseProvider::alloc(),
                    &NSString::from_str(file_type),
                    ProtocolObject::from_ref(&*delegate),
                );
                unsafe { provider.setUserInfo(Some(&NSNumber::numberWithUnsignedInteger(ix))) };
                ProtocolObject::from_retained(provider)
            }
        };

        let icon = drag_icon(&workspace, item);
        let drag_item =
            NSDraggingItem::initWithPasteboardWriter(NSDraggingItem::alloc(), &writer);
        // Fan the stack out a touch so multi-item drags read as a pile.
        let offset = (ix.min(3)) as f64 * 3.0;
        let frame = NSRect {
            origin: NSPoint { x: origin.x - 16.0 + offset, y: origin.y - 16.0 - offset },
            size: NSSize { width: 32.0, height: 32.0 },
        };
        unsafe { drag_item.setDraggingFrame_contents(frame, Some(&icon)) };
        drag_items.push(drag_item);
    }

    let source = {
        let this = DragSource::alloc(mtm).set_ivars(());
        let this: Retained<DragSource> = unsafe { msg_send![super(this), init] };
        this
    };
    view.beginDraggingSessionWithItems_event_source(
        &NSArray::from_retained_slice(&drag_items),
        &event,
        ProtocolObject::from_ref(&*source),
    );
    ACTIVE_SESSION.with(|keep| {
        *keep.borrow_mut() = Some(SessionKeep { _source: source, _delegate: delegate });
    });
    true
}

/// Finder-style icon for the dragged item — the OS file icon by extension
/// (or the real file's icon for local items), so the drag reads native.
#[allow(deprecated)]
fn drag_icon(workspace: &NSWorkspace, item: &DragItem) -> Retained<NSImage> {
    if let Some(path) = &item.local_path {
        return workspace.iconForFile(&NSString::from_str(&path.to_string_lossy()));
    }
    let file_type = if item.is_dir {
        "public.folder".to_string()
    } else {
        std::path::Path::new(&item.name)
            .extension()
            .map(|ext| ext.to_string_lossy().to_string())
            .unwrap_or_else(|| "public.data".to_string())
    };
    workspace.iconForFileType(&NSString::from_str(&file_type))
}
