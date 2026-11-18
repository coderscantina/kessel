//! Driver registry: what each OpenDAL backend needs from the bookmark form.
//! Adding a backend = adding a `DriverSpec` here (plus its Cargo feature).
//! Fields map 1:1 to OpenDAL operator option keys; `secret` fields are stored
//! in the OS keychain, everything else in the `sites.options` JSON.

use std::collections::HashMap;

use crate::provider::{LayerConfig, ThrottleConfig};

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
            field("endpoint", "Host", "host[:21]", false, true),
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

/// Where to register an OAuth client for a cloud-drive scheme.
pub fn oauth_setup_url(scheme: &str) -> Option<&'static str> {
    match scheme {
        "gdrive" => Some("https://developers.google.com/workspace/guides/create-credentials#oauth-client-id"),
        "dropbox" => Some("https://www.dropbox.com/developers/apps"),
        "onedrive" => Some(
            "https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app",
        ),
        _ => None,
    }
}

/// Split a stored FTP endpoint into host and whether it uses explicit TLS.
/// OpenDAL reads a bare host as FTPS and only `ftp://` as plain FTP, so the
/// form shows exactly that.
pub fn split_ftp_endpoint(endpoint: &str) -> (&str, bool) {
    match endpoint.split_once("://") {
        Some(("ftp", host)) => (host, false),
        Some((_, host)) => (host, true),
        None => (endpoint, true),
    }
}

/// The endpoint option for an FTP site: always carries its scheme, so the
/// TLS choice never depends on OpenDAL's handling of a bare host.
pub fn ftp_endpoint(host: &str, tls: bool) -> String {
    format!("{}://{host}", if tls { "ftps" } else { "ftp" })
}

/// Highest per-site connection count the form accepts.
pub const MAX_CONNECTIONS: usize = 64;
/// Highest per-site speed limit the form accepts, in KiB/s. Keeps the byte
/// rate inside `u32`.
pub const MAX_SPEED_KIB: u32 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitError {
    Connections,
    Speed,
}

impl LimitError {
    pub fn message(self) -> String {
        use crate::i18n::tf;
        match self {
            Self::Connections => tf(
                "site.limits.connections_invalid",
                &[("max", &MAX_CONNECTIONS.to_string())],
            ),
            Self::Speed => tf("site.limits.speed_invalid", &[("max", &MAX_SPEED_KIB.to_string())]),
        }
        .to_string()
    }
}

/// `base` with the form's connection count and speed limit applied. An empty
/// field clears the limit, so the app-level default applies again.
pub fn apply_limits(
    base: &LayerConfig,
    connections: &str,
    speed_kib: &str,
) -> Result<LayerConfig, LimitError> {
    let (connections, speed_kib) = (connections.trim(), speed_kib.trim());
    let concurrent_limit = match connections {
        "" => None,
        text => match text.parse::<usize>() {
            Ok(count) if (1..=MAX_CONNECTIONS).contains(&count) => Some(count),
            _ => return Err(LimitError::Connections),
        },
    };
    let throttle = match speed_kib {
        "" => None,
        text => match text.parse::<u32>() {
            Ok(kib) if (1..=MAX_SPEED_KIB).contains(&kib) => {
                Some(ThrottleConfig { bandwidth: kib * 1024 })
            }
            _ => return Err(LimitError::Speed),
        },
    };
    Ok(LayerConfig {
        concurrent_limit,
        throttle,
        ..base.clone()
    })
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
    fn ftp_endpoint_keeps_the_tls_choice_explicit() {
        assert_eq!(split_ftp_endpoint("files.example.org"), ("files.example.org", true));
        assert_eq!(split_ftp_endpoint("ftp://files.example.org"), ("files.example.org", false));
        assert_eq!(split_ftp_endpoint("ftps://files.example.org:21"), ("files.example.org:21", true));
        assert_eq!(ftp_endpoint("files.example.org", true), "ftps://files.example.org");
        assert_eq!(ftp_endpoint("files.example.org", false), "ftp://files.example.org");
    }

    #[test]
    fn limits_validate_and_clear() {
        let base = LayerConfig {
            timeout_secs: Some(9),
            ..Default::default()
        };
        let layers = apply_limits(&base, " 4 ", "512").unwrap();
        assert_eq!(layers.concurrent_limit, Some(4));
        assert_eq!(layers.throttle.as_ref().map(|t| t.bandwidth), Some(512 * 1024));
        assert_eq!(layers.timeout_secs, Some(9));
        assert_eq!(apply_limits(&layers, "", "").unwrap().concurrent_limit, None);
        assert_eq!(apply_limits(&base, "0", ""), Err(LimitError::Connections));
        assert_eq!(apply_limits(&base, "65", ""), Err(LimitError::Connections));
        assert_eq!(apply_limits(&base, "", "fast"), Err(LimitError::Speed));
        assert_eq!(apply_limits(&base, "", "1000001"), Err(LimitError::Speed));
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
