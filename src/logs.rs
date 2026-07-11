//! In-app log capture: a bounded ring buffer fed by a `tracing` layer, so
//! the bottom log panel shows exactly what the subscriber stack sees —
//! including OpenDAL's per-operation LoggingLayer/TracingLayer output. The
//! reloadable `EnvFilter` in `main` gates this layer too, so the panel
//! follows `logging.filter` from config.json5.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use tracing::field::{Field, Visit};

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub time: chrono::DateTime<chrono::Local>,
    pub level: tracing::Level,
    pub target: String,
    pub message: String,
}

const CAPACITY: usize = 2000;

static BUFFER: Mutex<VecDeque<LogEntry>> = Mutex::new(VecDeque::new());
static VERSION: AtomicU64 = AtomicU64::new(0);

/// Monotonic change counter — the panel polls this instead of cloning the
/// buffer every frame.
pub fn version() -> u64 {
    VERSION.load(Ordering::Relaxed)
}

pub fn entries() -> Vec<LogEntry> {
    BUFFER.lock().unwrap().iter().cloned().collect()
}

pub fn clear() {
    BUFFER.lock().unwrap().clear();
    VERSION.fetch_add(1, Ordering::Relaxed);
}

/// The capture layer for the subscriber registry in `main`.
pub fn layer() -> BufferLayer {
    BufferLayer
}

pub struct BufferLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for BufferLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        #[derive(Default)]
        struct Collector {
            message: String,
            fields: String,
        }
        impl Visit for Collector {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.message = format!("{value:?}");
                } else {
                    write!(self.fields, " {}={:?}", field.name(), value).ok();
                }
            }
        }

        let mut collector = Collector::default();
        event.record(&mut collector);
        let mut message = collector.message;
        message.push_str(&collector.fields);

        let metadata = event.metadata();
        let entry = LogEntry {
            time: chrono::Local::now(),
            level: *metadata.level(),
            target: metadata.target().to_string(),
            message,
        };
        let mut buffer = BUFFER.lock().unwrap();
        if buffer.len() >= CAPACITY {
            buffer.pop_front();
        }
        buffer.push_back(entry);
        VERSION.fetch_add(1, Ordering::Relaxed);
    }
}
