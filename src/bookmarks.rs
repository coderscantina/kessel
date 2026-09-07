//! Driver registry: what each OpenDAL backend needs from the bookmark form.
//! Adding a backend = adding a `DriverSpec` here (plus its Cargo feature).
//! Fields map 1:1 to OpenDAL operator option keys; `secret` fields are stored
//! in the OS keychain, everything else in the `sites.options` JSON.

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
    // OpenDAL's SFTP service rides the system's ssh: auth is key-file or
    // ssh-agent (no password auth), and host keys are checked against
    // ~/.ssh/known_hosts per the chosen strategy.
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
            field(
                "known_hosts_strategy",
                "Host key check",
                "strict | accept | add (default strict)",
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
