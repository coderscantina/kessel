//! Driver registry: what each OpenDAL backend needs from the bookmark form.
//! Adding a backend = adding a `DriverSpec` here (plus its Cargo feature).
//! Fields map 1:1 to OpenDAL operator option keys; `secret` fields are stored
//! in the OS keychain, everything else in the `sites.options` JSON.

use std::collections::HashMap;

/// Kessel-internal option key for S3 "use an ~/.aws profile" mode. Stripped
/// and resolved to real credentials in `Provider::connect`, never passed to
/// the Operator.
pub const AWS_PROFILE_OPT: &str = "aws_profile";

pub struct FieldSpec {
    /// OpenDAL operator option key.
    pub key: &'static str,
    pub label: &'static str,
    pub placeholder: &'static str,
    /// Keychain-backed and rendered masked. Secret fields are never
    /// `required` so that "leave blank to keep the stored value" works when
    /// editing.
    pub secret: bool,
    pub required: bool,
}

pub struct DriverSpec {
    /// OpenDAL service scheme, as stored in `sites.scheme`.
    pub scheme: &'static str,
    pub label: &'static str,
    pub fields: &'static [FieldSpec],
}

const fn field(
    key: &'static str,
    label: &'static str,
    placeholder: &'static str,
    secret: bool,
    required: bool,
) -> FieldSpec {
    FieldSpec {
        key,
        label,
        placeholder,
        secret,
        required,
    }
}

pub const DRIVERS: &[DriverSpec] = &[
    DriverSpec {
        scheme: "s3",
        label: "Amazon S3 (and compatible)",
        fields: &[
            field(
                "bucket",
                "Bucket",
                "my-bucket (empty to browse all buckets)",
                false,
                false,
            ),
            field("region", "Region", "eu-central-1", false, false),
            field(
                "endpoint",
                "Endpoint",
                "https://… (MinIO, R2, …; empty for AWS)",
                false,
                false,
            ),
            field("root", "Root path", "/", false, false),
            field("access_key_id", "Access key ID", "AKIA…", true, false),
            field("secret_access_key", "Secret access key", "", true, false),
            field(
                "session_token",
                "Session token",
                "STS session token (optional, short-lived creds)",
                true,
                false,
            ),
        ],
    },
    DriverSpec {
        scheme: "gcs",
        label: "Google Cloud Storage",
        fields: &[
            field("bucket", "Bucket", "my-bucket", false, true),
            field("root", "Root path", "/", false, false),
            field(
                "credential_path",
                "Credential file",
                "/path/to/service-account.json",
                false,
                false,
            ),
        ],
    },
    DriverSpec {
        scheme: "azblob",
        label: "Azure Blob Storage",
        fields: &[
            field("container", "Container", "my-container", false, true),
            field(
                "endpoint",
                "Endpoint",
                "https://account.blob.core.windows.net",
                false,
                false,
            ),
            field("root", "Root path", "/", false, false),
            field("account_name", "Account name", "", false, false),
            field("account_key", "Account key", "", true, false),
        ],
    },
    // OpenDAL's SFTP service rides the system's ssh in batch mode: auth is
    // key-file or ssh-agent (ssh never asks for a password), and host keys
    // are checked strictly against ~/.ssh/known_hosts. Unknown hosts go
    // through the trust dialog (`crate::ui::connect`).
    DriverSpec {
        scheme: "sftp",
        label: "SFTP",
        fields: &[
            field("endpoint", "Host", "ssh://host[:22]", false, true),
            field("user", "User", "", false, true),
            field(
                "key",
                "Private key",
                "~/.ssh/id_ed25519 (empty for ssh-agent)",
                false,
                false,
            ),
            field("root", "Root path", "/", false, false),
        ],
    },
    DriverSpec {
        scheme: "ftp",
        label: "FTP",
        fields: &[
            field("endpoint", "Host", "ftp://host[:21]", false, true),
            field("user", "User", "anonymous", false, false),
            field("password", "Password", "", true, false),
            field("root", "Root path", "/", false, false),
        ],
    },
    DriverSpec {
        scheme: "webdav",
        label: "WebDAV",
        fields: &[
            field(
                "endpoint",
                "Endpoint",
                "https://dav.example.com",
                false,
                true,
            ),
            field("root", "Root path", "/", false, false),
            field("username", "Username", "", false, false),
            field("password", "Password", "", true, false),
        ],
    },
    // Cloud drives authenticate with OAuth: the form's "Sign in with
    // browser" flow (see `crate::oauth`) obtains a refresh token that lands
    // in the keychain next to the client secret; OpenDAL's signer exchanges
    // and renews access tokens from there.
    DriverSpec {
        scheme: "gdrive",
        label: "Google Drive",
        fields: &[
            field("root", "Root path", "/", false, false),
            field(
                "client_id",
                "OAuth client ID",
                "….apps.googleusercontent.com (Desktop app)",
                false,
                true,
            ),
            field("client_secret", "OAuth client secret", "", true, false),
        ],
    },
    DriverSpec {
        scheme: "dropbox",
        label: "Dropbox",
        fields: &[
            field("root", "Root path", "/", false, false),
            field("client_id", "App key", "from dropbox.com/developers", false, true),
            field("client_secret", "App secret", "", true, false),
        ],
    },
    DriverSpec {
        scheme: "onedrive",
        label: "OneDrive",
        fields: &[
            field("root", "Root path", "/", false, false),
            field(
                "client_id",
                "Application (client) ID",
                "Azure app registration (public client)",
                false,
                true,
            ),
            field("client_secret", "Client secret (optional)", "", true, false),
        ],
    },
    DriverSpec {
        scheme: "fs",
        label: "Local filesystem",
        fields: &[field("root", "Root path", "/ — or C:\\, D:\\… on Windows", false, true)],
    },
];

/// The drivers this build can actually connect with, in menu order. The sftp
/// transport only exists on unix (see `sftp_unsupported.rs`), so it isn't
/// offered on Windows — a bookmark for it could never connect.
pub fn available_drivers() -> Vec<&'static DriverSpec> {
    DRIVERS
        .iter()
        .filter(|driver| driver.scheme != "sftp" || crate::sftp::SUPPORTED)
        .collect()
}

/// Schemes whose credentials come from the browser OAuth flow.
pub fn is_oauth_scheme(scheme: &str) -> bool {
    matches!(scheme, "gdrive" | "dropbox" | "onedrive")
}

pub fn driver_by_scheme(scheme: &str) -> Option<&'static DriverSpec> {
    DRIVERS.iter().find(|driver| driver.scheme == scheme)
}

/// The secret option keys a scheme may store in the keychain. Used to drop
/// stale entries (e.g. leftover S3 keys after a bookmark became WebDAV).
pub fn secret_keys(scheme: &str) -> Vec<&'static str> {
    let mut keys: Vec<&'static str> = driver_by_scheme(scheme)
        .map(|spec| {
            spec.fields
                .iter()
                .filter(|field| field.secret)
                .map(|field| field.key)
                .collect()
        })
        .unwrap_or_default();
    // OAuth refresh tokens are keychain-only and have no form field.
    if is_oauth_scheme(scheme) {
        keys.push("refresh_token");
    }
    keys
}

/// `sites.space_id` of quick-connect sites: a real row (so the transfer
/// queue and tab restore can resolve it) in a space that never exists, so it
/// stays out of the sidebar until "Save as site" moves it into one.
pub const QUICK_CONNECT_SPACE: i64 = 0;

/// A connection parsed from a quick-connect URL, ready to become a site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuickTarget {
    /// OpenDAL scheme (`sftp`, `ftp`, `webdav`, `s3`).
    pub scheme: &'static str,
    /// Non-secret operator options, keyed like the driver's form fields.
    pub options: HashMap<String, String>,
    /// Secrets that came with the URL (`ftp://user:pass@host`).
    pub secrets: HashMap<String, String>,
    /// Display name: the URL without its password.
    pub name: String,
}

/// Why a quick-connect URL can't be used. `message` words it for the UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuickUrlError {
    Empty,
    Invalid,
    UnknownScheme(String),
    MissingHost,
    UnknownOption(String),
    SftpPassword,
    SftpUnavailable,
}

impl QuickUrlError {
    pub fn message(&self) -> String {
        use crate::i18n::{t, tf};
        match self {
            Self::Empty => t("connect.url.empty").to_string(),
            Self::Invalid => t("connect.url.invalid").to_string(),
            Self::UnknownScheme(scheme) => {
                tf("connect.url.unknown_scheme", &[("scheme", scheme)]).to_string()
            }
            Self::MissingHost => t("connect.url.missing_host").to_string(),
            Self::UnknownOption(key) => {
                tf("connect.url.unknown_option", &[("option", key)]).to_string()
            }
            Self::SftpPassword => t("connect.url.sftp_password").to_string(),
            Self::SftpUnavailable => t("connect.url.sftp_unavailable").to_string(),
        }
    }
}

/// Parse `sftp://user@host:port/path`, `ftp(s)://…`, `webdav(s)://…` or
/// `s3://bucket/prefix?region=…&endpoint=…&profile=…` into site options.
/// The path becomes the site's root, like the form's "Root path" field.
pub fn parse_quick_url(input: &str) -> Result<QuickTarget, QuickUrlError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(QuickUrlError::Empty);
    }
    let url = url::Url::parse(input).map_err(|_| QuickUrlError::Invalid)?;
    let user = percent_decode(url.username());
    let password = url.password().map(percent_decode);
    let host = url.host_str().map(str::to_string).unwrap_or_default();
    let port = url.port().map(|port| format!(":{port}")).unwrap_or_default();
    let path = percent_decode(url.path());
    let root = path.trim_end_matches('/');

    let mut options = HashMap::new();
    let mut secrets = HashMap::new();
    let mut set = |key: &str, value: &str| {
        if !value.is_empty() {
            options.insert(key.to_string(), value.to_string());
        }
    };

    let scheme = match url.scheme() {
        "sftp" | "ssh" => {
            if !crate::sftp::SUPPORTED {
                return Err(QuickUrlError::SftpUnavailable);
            }
            if password.is_some() {
                return Err(QuickUrlError::SftpPassword);
            }
            set("endpoint", &format!("ssh://{host}{port}"));
            set("user", &user);
            "sftp"
        }
        "ftp" | "ftps" => {
            set("endpoint", &format!("{}://{host}{port}", url.scheme()));
            set("user", &user);
            "ftp"
        }
        "webdav" | "dav" | "webdavs" | "davs" => {
            let http = if url.scheme().ends_with('s') { "https" } else { "http" };
            set("endpoint", &format!("{http}://{host}{port}"));
            set("username", &user);
            "webdav"
        }
        "s3" => {
            set("bucket", &host);
            "s3"
        }
        other => return Err(QuickUrlError::UnknownScheme(other.to_string())),
    };
    if host.is_empty() && scheme != "s3" {
        return Err(QuickUrlError::MissingHost);
    }
    set("root", root);

    for (key, value) in url.query_pairs() {
        let option = match (scheme, key.as_ref()) {
            ("s3", "region") => "region",
            ("s3", "endpoint") => "endpoint",
            ("s3", "profile") => AWS_PROFILE_OPT,
            _ => return Err(QuickUrlError::UnknownOption(key.into_owned())),
        };
        set(option, &value);
    }

    if let Some(password) = password.filter(|password| !password.is_empty()) {
        secrets.insert("password".to_string(), password);
    }

    let mut display = url.clone();
    let _ = display.set_password(None);
    let name = display
        .as_str()
        .trim_end_matches('/')
        .split_once("://")
        .map_or(input, |(_, rest)| rest)
        .to_string();

    Ok(QuickTarget {
        scheme,
        options,
        secrets,
        name,
    })
}

/// Decode `%XX` escapes (URL user, password and path); invalid escapes stay
/// as typed.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut ix = 0;
    while ix < bytes.len() {
        let hex = bytes
            .get(ix + 1..ix + 3)
            .and_then(|pair| std::str::from_utf8(pair).ok())
            .and_then(|pair| u8::from_str_radix(pair, 16).ok());
        match (bytes[ix], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                ix += 3;
            }
            (byte, _) => {
                out.push(byte);
                ix += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The secret fields a quick-connect target should ask for before
/// connecting: a password once a user is given (FTP, WebDAV), or optional
/// access keys for S3 without a profile. SFTP never asks: ssh signs in with
/// keys or the agent.
pub fn quick_prompt_fields(target: &QuickTarget) -> Vec<&'static FieldSpec> {
    let Some(spec) = driver_by_scheme(target.scheme) else {
        return Vec::new();
    };
    let wanted: &[&str] = match target.scheme {
        "ftp" if target.options.contains_key("user") => &["password"],
        "webdav" if target.options.contains_key("username") => &["password"],
        "s3" if !target.options.contains_key(AWS_PROFILE_OPT) => {
            &["access_key_id", "secret_access_key"]
        }
        _ => &[],
    };
    spec.fields
        .iter()
        .filter(|field| wanted.contains(&field.key) && !target.secrets.contains_key(field.key))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(target: &QuickTarget) -> Vec<(&str, &str)> {
        let mut pairs: Vec<(&str, &str)> = target
            .options
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        pairs.sort();
        pairs
    }

    #[test]
    fn parses_sftp_with_user_port_and_path() {
        let target = parse_quick_url(" sftp://deploy@example.com:2222/var/www/ ").unwrap();
        assert_eq!(target.scheme, "sftp");
        assert_eq!(
            opts(&target),
            [
                ("endpoint", "ssh://example.com:2222"),
                ("root", "/var/www"),
                ("user", "deploy"),
            ]
        );
        assert_eq!(target.name, "deploy@example.com:2222/var/www");
        assert!(quick_prompt_fields(&target).is_empty());
    }

    #[test]
    fn rejects_sftp_passwords() {
        assert_eq!(
            parse_quick_url("sftp://me:secret@host"),
            Err(QuickUrlError::SftpPassword)
        );
    }

    #[test]
    fn ftp_password_is_a_secret_and_hidden_from_the_name() {
        let target = parse_quick_url("ftps://anna:p%40ss@files.example.org").unwrap();
        assert_eq!(target.scheme, "ftp");
        assert_eq!(
            opts(&target),
            [("endpoint", "ftps://files.example.org"), ("user", "anna")]
        );
        assert_eq!(target.secrets.get("password").map(String::as_str), Some("p@ss"));
        assert_eq!(target.name, "anna@files.example.org");
        assert!(quick_prompt_fields(&target).is_empty());
    }

    #[test]
    fn ftp_user_without_password_asks_for_it() {
        let target = parse_quick_url("ftp://anna@files.example.org:2121").unwrap();
        assert_eq!(target.options["endpoint"], "ftp://files.example.org:2121");
        let fields: Vec<&str> = quick_prompt_fields(&target).iter().map(|f| f.key).collect();
        assert_eq!(fields, ["password"]);
        let anonymous = parse_quick_url("ftp://files.example.org").unwrap();
        assert!(quick_prompt_fields(&anonymous).is_empty());
    }

    #[test]
    fn webdavs_maps_to_https_with_root() {
        let target = parse_quick_url("webdavs://me@cloud.example.com/remote.php/dav").unwrap();
        assert_eq!(target.scheme, "webdav");
        assert_eq!(
            opts(&target),
            [
                ("endpoint", "https://cloud.example.com"),
                ("root", "/remote.php/dav"),
                ("username", "me"),
            ]
        );
    }

    #[test]
    fn s3_bucket_prefix_and_options() {
        let target =
            parse_quick_url("s3://media/photos/2026?region=eu-central-1&profile=work").unwrap();
        assert_eq!(
            opts(&target),
            [
                ("aws_profile", "work"),
                ("bucket", "media"),
                ("region", "eu-central-1"),
                ("root", "/photos/2026"),
            ]
        );
        assert!(quick_prompt_fields(&target).is_empty());
        let keys = parse_quick_url("s3://media").unwrap();
        assert_eq!(quick_prompt_fields(&keys).len(), 2);
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(parse_quick_url("  "), Err(QuickUrlError::Empty));
        assert_eq!(parse_quick_url("example.com"), Err(QuickUrlError::Invalid));
        assert_eq!(
            parse_quick_url("gopher://example.com"),
            Err(QuickUrlError::UnknownScheme("gopher".into()))
        );
        assert_eq!(parse_quick_url("sftp:///srv"), Err(QuickUrlError::MissingHost));
        assert_eq!(
            parse_quick_url("ftp://host/?passive=1"),
            Err(QuickUrlError::UnknownOption("passive".into()))
        );
    }
}
