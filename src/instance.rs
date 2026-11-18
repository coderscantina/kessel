//! One running kessel per data directory. Two instances would share one
//! transfer queue: the second launch would re-queue the first one's active
//! tasks and both would append to the same partial files.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

/// How long a launch waits for the lock: an in-app update starts the new
/// version before the old one has finished quitting.
pub const UPDATE_PATIENCE: Duration = Duration::from_secs(8);
const POLL_EVERY: Duration = Duration::from_millis(100);

/// Take the exclusive lock on the data directory, waiting up to `patience`.
/// The OS releases it when the returned file closes, also after a crash.
/// `None` when another instance still holds it.
pub fn acquire(data_dir: &Path, patience: Duration) -> Result<Option<File>> {
    let path = data_dir.join("kessel.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening `{}`", path.display()))?;
    let deadline = Instant::now() + patience;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(Some(file)),
            Err(TryLockError::WouldBlock) if Instant::now() >= deadline => return Ok(None),
            Err(TryLockError::WouldBlock) => std::thread::sleep(POLL_EVERY),
            Err(TryLockError::Error(error)) => {
                return Err(error).with_context(|| format!("locking `{}`", path.display()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_instance_waits_for_the_first_and_gives_up_in_time() {
        let dir = std::env::temp_dir().join(format!("kessel-instance-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let first = acquire(&dir, Duration::ZERO).unwrap().expect("the lock is free");
        assert!(acquire(&dir, Duration::from_millis(150)).unwrap().is_none());

        // An exiting instance hands over to the waiting one.
        let exiting = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(first);
        });
        assert!(acquire(&dir, Duration::from_secs(5)).unwrap().is_some());
        exiting.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
