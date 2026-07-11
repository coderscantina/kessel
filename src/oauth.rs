//! OAuth 2.0 for cloud-drive backends (Google Drive, Dropbox, OneDrive):
//! authorization-code flow with PKCE and a loopback redirect.
//!
//! kessel ships no client credentials — users register their own app with
//! the provider (redirect URI `http://localhost:53682/`) and paste the
//! client id (and secret, where the provider requires one) into the
//! bookmark form. The flow's result is a long-lived **refresh token**,
//! stored only in the OS keychain; OpenDAL's service signers exchange it
//! for access tokens and keep them fresh from then on.

use std::time::Duration;

use anyhow::{bail, Context as _, Result};
use base64::Engine as _;
use rand::Rng as _;
use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

/// Fixed loopback port so the redirect URI is stable enough to register with
/// providers that require an exact match (Dropbox, Azure). rclone popularized
/// this exact port for the same reason.
pub const REDIRECT_PORT: u16 = 53682;

/// How long we wait for the user to finish the browser round-trip.
const FLOW_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudProvider {
    Gdrive,
    Dropbox,
    Onedrive,
}

impl CloudProvider {
    pub fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "gdrive" => Some(Self::Gdrive),
            "dropbox" => Some(Self::Dropbox),
            "onedrive" => Some(Self::Onedrive),
            _ => None,
        }
    }

    fn auth_url(self) -> &'static str {
        match self {
            Self::Gdrive => "https://accounts.google.com/o/oauth2/v2/auth",
            Self::Dropbox => "https://www.dropbox.com/oauth2/authorize",
            Self::Onedrive => "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
        }
    }

    fn token_url(self) -> &'static str {
        match self {
            Self::Gdrive => "https://oauth2.googleapis.com/token",
            Self::Dropbox => "https://api.dropboxapi.com/oauth2/token",
            Self::Onedrive => "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        }
    }

    fn scope(self) -> &'static str {
        match self {
            Self::Gdrive => "https://www.googleapis.com/auth/drive",
            Self::Dropbox => {
                "files.metadata.read files.content.read files.content.write account_info.read"
            }
            Self::Onedrive => "Files.ReadWrite.All offline_access",
        }
    }

    /// Provider-specific query params that make the grant return a refresh
    /// token.
    fn extra_auth_params(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Gdrive => &[("access_type", "offline"), ("prompt", "consent")],
            Self::Dropbox => &[("token_access_type", "offline")],
            Self::Onedrive => &[],
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct TokenResponse {
    refresh_token: Option<String>,
    #[allow(dead_code)]
    access_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    b64url(&bytes)
}

/// Run the full authorization flow: bind the loopback listener, open the
/// system browser, wait for the redirect, exchange the code. Returns the
/// refresh token to store in the keychain.
pub async fn authorize(
    provider: CloudProvider,
    client_id: &str,
    client_secret: Option<&str>,
) -> Result<String> {
    let redirect_uri = format!("http://localhost:{REDIRECT_PORT}/");
    let listener = TcpListener::bind(("127.0.0.1", REDIRECT_PORT))
        .await
        .with_context(|| {
            format!("port {REDIRECT_PORT} is in use — close the other sign-in and retry")
        })?;

    let verifier = random_token();
    let challenge = b64url(&sha2::Sha256::digest(verifier.as_bytes()));
    let state = random_token();

    let mut url = url::Url::parse(provider.auth_url()).expect("static auth url");
    url.query_pairs_mut()
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", provider.scope())
        .append_pair("state", &state)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256");
    for (key, value) in provider.extra_auth_params() {
        url.query_pairs_mut().append_pair(key, value);
    }
    crate::ui::open_path_or_url(url.as_str());

    let code = tokio::time::timeout(FLOW_TIMEOUT, wait_for_code(&listener, &state))
        .await
        .context("timed out waiting for the browser sign-in")??;

    // Exchange the code for tokens.
    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", &redirect_uri),
        ("client_id", client_id),
        ("code_verifier", &verifier),
    ];
    if let Some(secret) = client_secret.filter(|secret| !secret.is_empty()) {
        form.push(("client_secret", secret));
    }
    let response = reqwest::Client::new()
        .post(provider.token_url())
        .form(&form)
        .send()
        .await
        .context("token exchange request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let tokens: TokenResponse = serde_json::from_str(&body)
        .with_context(|| format!("token response ({status}): {body}"))?;
    if let Some(error) = tokens.error {
        bail!(
            "token exchange failed: {error} {}",
            tokens.error_description.unwrap_or_default()
        );
    }
    tokens.refresh_token.context(
        "the provider returned no refresh token — remove the app's prior consent and retry",
    )
}

/// Accept loopback connections until the OAuth redirect arrives; verify the
/// `state` parameter and answer the browser with a small confirmation page.
async fn wait_for_code(listener: &TcpListener, expected_state: &str) -> Result<String> {
    loop {
        let (mut stream, _) = listener.accept().await.context("accepting redirect")?;
        // The request line is all we need; read a chunk and parse it.
        let mut buffer = vec![0u8; 8192];
        let read = stream.read(&mut buffer).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buffer[..read]);
        let Some(path) = request.split_whitespace().nth(1) else {
            continue;
        };
        // Stray requests (favicon etc.) get a 404 and the loop continues.
        let Ok(url) = url::Url::parse(&format!("http://localhost{path}")) else {
            continue;
        };
        let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
        let (code, state) = (query.get("code"), query.get("state"));
        let (Some(code), Some(state)) = (code, state) else {
            let _ = stream
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await;
            continue;
        };
        let ok = state.as_ref() == expected_state;
        let page = if ok {
            "<html><body style=\"font-family:system-ui;padding:2rem\">\
             <h2>kessel — signed in</h2><p>You can close this window and return to the app.</p>\
             </body></html>"
        } else {
            "<html><body style=\"font-family:system-ui;padding:2rem\">\
             <h2>kessel — sign-in rejected</h2><p>State mismatch; please retry from the app.</p>\
             </body></html>"
        };
        let _ = stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
                    page.len()
                )
                .as_bytes(),
            )
            .await;
        let _ = stream.shutdown().await;
        if !ok {
            bail!("OAuth state mismatch — possible interception; sign-in aborted");
        }
        return Ok(code.to_string());
    }
}
