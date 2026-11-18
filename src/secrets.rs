//! Secrets live in the OS keychain via `keyring`, never in SQLite.
//! The database stores only opaque handles (`sites.secret_handle`).

use anyhow::Result;
use keyring::Entry;

const SERVICE: &str = "com.coderscantina.kessel";

/// A secret is a JSON map of sensitive Operator options for one site,
/// e.g. `{"secret_access_key": "...", "access_key_id": "..."}`.
pub struct SecretStore;

impl SecretStore {
    pub fn store(handle: &str, secrets: &std::collections::HashMap<String, String>) -> Result<()> {
        let entry = Entry::new(SERVICE, handle)?;
        entry.set_password(&serde_json::to_string(secrets)?)?;
        Ok(())
    }

    pub fn load(handle: &str) -> Result<std::collections::HashMap<String, String>> {
        let entry = Entry::new(SERVICE, handle)?;
        Ok(serde_json::from_str(&entry.get_password()?)?)
    }

    /// Like [`load`](Self::load), but a missing entry is `None` instead of
    /// an error. Any other keychain failure (locked, denied, corrupt) stays
    /// an error so callers never mistake it for "no secrets".
    pub fn load_optional(
        handle: &str,
    ) -> Result<Option<std::collections::HashMap<String, String>>> {
        let entry = Entry::new(SERVICE, handle)?;
        match entry.get_password() {
            Ok(json) => Ok(Some(serde_json::from_str(&json)?)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn delete(handle: &str) -> Result<()> {
        let entry = Entry::new(SERVICE, handle)?;
        entry.delete_credential()?;
        Ok(())
    }

    /// Deterministic handle for a site row.
    pub fn handle_for_site(site_id: i64) -> String {
        format!("site-{site_id}")
    }
}
