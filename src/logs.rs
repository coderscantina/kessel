//! In-app log capture: a bounded ring buffer fed by a `tracing` layer, so
//! the bottom log panel shows exactly what the subscriber stack sees —
//! including OpenDAL's per-operation LoggingLayer/TracingLayer output. The
//! reloadable `EnvFilter` in `main` gates this layer too, so the panel
//! follows `logging.filter` from config.json5.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
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

/// Size at which `kessel.log` rotates to `kessel.log.1`; one generation is
/// kept, so the log directory stays under twice this.
const LOG_FILE_LIMIT: u64 = 5 * 1024 * 1024;

/// A size-capped log file for the fmt layer, e.g.
/// `fmt::layer().with_ansi(false).with_writer(Mutex::new(file))`. Needed
/// because Windows release builds have no console to read.
pub struct RotatingFile {
    path: PathBuf,
    file: File,
    size: u64,
    limit: u64,
}

impl RotatingFile {
    pub fn open(dir: &Path) -> io::Result<Self> {
        Self::open_with_limit(dir, LOG_FILE_LIMIT)
    }

    fn open_with_limit(dir: &Path, limit: u64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("kessel.log");
        let file = File::options().create(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        Ok(Self { path, file, size, limit })
    }

    fn rotate(&mut self) -> io::Result<()> {
        let mut old = self.path.as_os_str().to_owned();
        old.push(".1");
        std::fs::rename(&self.path, old)?;
        self.file = File::options().create(true).append(true).open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

impl io::Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.size >= self.limit {
            self.rotate()?;
        }
        let written = self.file.write(buf)?;
        self.size += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Record panics in the tracing log (and so the log file) before the default
/// hook runs; without a console a release build would otherwise vanish silently.
pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(
            "panic: {info}\n{}",
            std::backtrace::Backtrace::force_capture()
        );
        previous(info);
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn rotating_file_keeps_one_previous_generation() {
        let dir = std::env::temp_dir().join(format!("kessel-log-test-{}", std::process::id()));
        let mut log = RotatingFile::open_with_limit(&dir, 10).unwrap();
        log.write_all(b"0123456789").unwrap();
        log.write_all(b"second").unwrap();
        assert_eq!(std::fs::read(dir.join("kessel.log.1")).unwrap(), b"0123456789");
        assert_eq!(std::fs::read(dir.join("kessel.log")).unwrap(), b"second");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
