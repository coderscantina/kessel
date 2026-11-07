//! Plain-language wording for errors shown in the UI.
//!
//! Every user-facing error goes through [`humanize`]: it gives a short
//! sentence saying what went wrong, an optional next step, and keeps the raw
//! chain for a "Details" disclosure. Classification looks at the OpenDAL
//! error kind first, then at well-known ssh/io messages in the chain.

use opendal::ErrorKind;

/// A user-facing error: `title` is one sentence, `hint` the next step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Humanized {
    pub title: String,
    pub hint: Option<String>,
    /// The full `{error:#}` chain, for a details disclosure or the log.
    pub details: String,
}

/// What kind of failure an error is, independent of backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    NotFound,
    PermissionDenied,
    AuthFailed,
    HostKeyUnknown,
    HostKeyChanged,
    HostUnreachable,
    Timeout,
    RateLimited,
    AlreadyExists,
    DiskFull,
    Unsupported,
    ConfigInvalid,
    Other,
}

impl Class {
    /// Whether retrying the same operation later can succeed.
    pub fn is_transient(self) -> bool {
        matches!(self, Self::HostUnreachable | Self::Timeout | Self::RateLimited)
    }
}

/// Classify an error by its OpenDAL kind or its message chain.
pub fn classify(error: &anyhow::Error) -> Class {
    let text = format!("{error:#}").to_lowercase();
    // ssh failures surface as opaque "unexpected" errors, so the message
    // wins over the OpenDAL kind for them.
    if text.contains("remote host identification has changed") {
        return Class::HostKeyChanged;
    }
    if text.contains("host key verification failed") || text.contains("no matching host key") {
        return Class::HostKeyUnknown;
    }
    if text.contains("permission denied (publickey")
        || text.contains("authentication failed")
        || text.contains("invalidaccesskeyid")
        || text.contains("signaturedoesnotmatch")
        || text.contains("530 login")
        || text.contains("login incorrect")
    {
        return Class::AuthFailed;
    }
    if let Some(kind) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<opendal::Error>())
        .map(opendal::Error::kind)
    {
        match kind {
            ErrorKind::NotFound => return Class::NotFound,
            ErrorKind::PermissionDenied => return Class::PermissionDenied,
            ErrorKind::AlreadyExists => return Class::AlreadyExists,
            ErrorKind::RateLimited => return Class::RateLimited,
            ErrorKind::Unsupported => return Class::Unsupported,
            ErrorKind::ConfigInvalid => return Class::ConfigInvalid,
            _ => {}
        }
    }
    if let Some(io) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
    {
        use std::io::ErrorKind as Io;
        match io.kind() {
            Io::NotFound => return Class::NotFound,
            Io::PermissionDenied => return Class::PermissionDenied,
            Io::AlreadyExists => return Class::AlreadyExists,
            Io::TimedOut => return Class::Timeout,
            Io::StorageFull | Io::QuotaExceeded => return Class::DiskFull,
            Io::ConnectionRefused | Io::ConnectionReset | Io::HostUnreachable => {
                return Class::HostUnreachable;
            }
            _ => {}
        }
    }
    if text.contains("timed out") || text.contains("timeout") {
        Class::Timeout
    } else if text.contains("could not resolve hostname")
        || text.contains("failed to lookup address")
        || text.contains("dns error")
        || text.contains("connection refused")
        || text.contains("no route to host")
        || text.contains("network is unreachable")
    {
        Class::HostUnreachable
    } else if text.contains("no space left") {
        Class::DiskFull
    } else {
        Class::Other
    }
}

/// Turn an error into wording for the UI.
pub fn humanize(error: &anyhow::Error) -> Humanized {
    let details = format!("{error:#}");
    let (title, hint) = match classify(error) {
        Class::NotFound => ("The file or folder no longer exists.", Some("Refresh to see the current contents.")),
        Class::PermissionDenied => ("You don't have permission to do that here.", Some("Check the account's access rights on the server.")),
        Class::AuthFailed => ("The server rejected the login.", Some("Check the user name, password or key in the site settings.")),
        Class::HostKeyUnknown => ("The server's identity isn't trusted yet.", Some("Compare the fingerprint with your server admin, then trust it.")),
        Class::HostKeyChanged => ("The server's identity changed since the last visit.", Some("This can mean an attack. Ask your server admin before connecting.")),
        Class::HostUnreachable => ("Couldn't reach the server.", Some("Check the address, port and your network connection.")),
        Class::Timeout => ("The server took too long to answer.", Some("Try again, or raise the timeout in the site settings.")),
        Class::RateLimited => ("The server is limiting requests right now.", Some("kessel will retry. Fewer parallel transfers can help.")),
        Class::AlreadyExists => ("Something with that name already exists.", None),
        Class::DiskFull => ("There isn't enough space on the destination.", Some("Free up space and retry.")),
        Class::Unsupported => ("This server doesn't support that operation.", None),
        Class::ConfigInvalid => ("The site settings are incomplete or invalid.", Some("Open the site settings and check the highlighted fields.")),
        Class::Other => {
            // Unknown errors keep their own top-level message: it beats a
            // generic sentence.
            let top = error.to_string();
            return Humanized { title: top, hint: None, details };
        }
    };
    Humanized {
        title: title.to_string(),
        hint: hint.map(str::to_string),
        details,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(message: &str) -> anyhow::Error {
        anyhow::anyhow!(message.to_string()).context("listing `/srv`")
    }

    #[test]
    fn classifies_ssh_messages() {
        assert_eq!(classify(&err("Host key verification failed.")), Class::HostKeyUnknown);
        assert_eq!(classify(&err("user@h: Permission denied (publickey).")), Class::AuthFailed);
        assert_eq!(classify(&err("ssh: Could not resolve hostname nope")), Class::HostUnreachable);
    }

    #[test]
    fn classifies_opendal_kind() {
        let e = anyhow::Error::new(opendal::Error::new(ErrorKind::NotFound, "gone")).context("stat");
        assert_eq!(classify(&e), Class::NotFound);
        assert!(!Class::NotFound.is_transient());
        assert!(Class::Timeout.is_transient());
    }

    #[test]
    fn unknown_errors_keep_their_message() {
        let h = humanize(&anyhow::anyhow!("weird thing"));
        assert_eq!(h.title, "weird thing");
        assert_eq!(h.hint, None);
    }
}
