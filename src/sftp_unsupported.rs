//! Stand-in for `sftp.rs` on platforms without an sftp backend.
//!
//! The sftp transport (`openssh`) drives the system `ssh` binary through a
//! unix ControlMaster socket and does not build off unix, so neither
//! OpenDAL's sftp service nor kessel's own permissions session exist in the
//! Windows build. `main.rs` swaps this module in for `sftp.rs` there; the
//! entry points keep their signatures so no call site needs a `cfg`.
//!
//! Reaching any of these means a bookmark asked for a scheme the build does
//! not carry — the provider rejects `sftp` sites before this at connect time.

use std::collections::HashMap;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};

/// Whether this build carries the sftp backend. Drives the bookmark editor's
/// driver list, so unsupported sites can't be created in the first place.
pub const SUPPORTED: bool = false;

pub async fn list_permissions(
    _options: &HashMap<String, String>,
    _dir: &str,
) -> Result<HashMap<String, u32>> {
    bail!("sftp is not supported on this platform")
}

pub async fn stat_mode(_options: &HashMap<String, String>, _path: &str) -> Result<Option<u32>> {
    bail!("sftp is not supported on this platform")
}

pub async fn set_mtime(
    _options: &HashMap<String, String>,
    _path: &str,
    _mtime: DateTime<Utc>,
) -> Result<()> {
    bail!("sftp is not supported on this platform")
}

pub async fn chmod(_options: &HashMap<String, String>, _path: &str, _mode: u32) -> Result<()> {
    bail!("sftp is not supported on this platform")
}
