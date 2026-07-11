//! Interactive AWS IAM Identity Center (SSO) sign-in via the OIDC **device
//! authorization** grant — the same flow as `aws sso login`, run from inside
//! the app so users never have to drop to a terminal.
//!
//! Unlike the cloud-drive OAuth flow (`crate::oauth`), the device grant needs
//! no loopback listener: the browser talks to AWS directly and kessel polls
//! for the result. The steps are:
//!
//!   1. `RegisterClient` — obtain an ephemeral OIDC client id/secret.
//!   2. `StartDeviceAuthorization` — get a pre-filled verification URL.
//!   3. Open that URL in the system browser; the user logs in and approves.
//!   4. Poll `CreateToken` until approval lands (or the flow times out).
//!
//! The resulting access token is written to `~/.aws/sso/cache/<sha1(start_url)>.json`
//! in the aws-cli schema. From there kessel's existing SSO resolution in
//! `crate::aws` (reqsign's `SSOCredentialProvider`) picks it up and exchanges
//! it for temporary S3 credentials — and the AWS CLI can refresh it too.

use std::time::Duration;

use anyhow::{bail, Context as _, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};

/// Overall cap on the round-trip (browser login + polling). Matches the OAuth
/// flow's budget for consistency.
const FLOW_TIMEOUT: Duration = Duration::from_secs(300);

/// A name reported to Identity Center for the ephemeral OIDC client.
const CLIENT_NAME: &str = "kessel";

/// The OAuth 2.0 device authorization grant type.
const GRANT_DEVICE_CODE: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// Regional SSO-OIDC endpoint. The path segments are the service's HTTP
/// bindings (`/client/register`, `/device_authorization`, `/token`).
fn oidc_endpoint(region: &str, path: &str) -> String {
    format!("https://oidc.{region}.amazonaws.com{path}")
}

/// Run the full device-authorization flow for one SSO start URL, writing the
/// resulting token to the shared cache. `Ok(())` means the cache now holds a
/// valid token that `crate::aws` can resolve into S3 credentials.
pub async fn sso_login(start_url: &str, region: &str) -> Result<()> {
    let client = reqwest::Client::new();

    // 1. Register an ephemeral public OIDC client.
    let registration: RegisterClientResponse = post_json(
        &client,
        &oidc_endpoint(region, "/client/register"),
        &RegisterClientRequest {
            client_name: CLIENT_NAME,
            client_type: "public",
        },
    )
    .await
    .context("registering the SSO OIDC client")?;

    // 2. Start the device authorization and get the verification URL.
    let authorization: StartDeviceAuthResponse = post_json(
        &client,
        &oidc_endpoint(region, "/device_authorization"),
        &StartDeviceAuthRequest {
            client_id: &registration.client_id,
            client_secret: &registration.client_secret,
            start_url,
        },
    )
    .await
    .context("starting the SSO device authorization")?;

    // 3. Open the browser at the pre-filled URL (falls back to the bare one).
    let verification_url = authorization
        .verification_uri_complete
        .clone()
        .unwrap_or_else(|| authorization.verification_uri.clone());
    crate::ui::open_path_or_url(&verification_url);

    // 4. Poll for the token until the user approves (server dictates the pace).
    let interval = Duration::from_secs(authorization.interval.unwrap_or(5).max(1));
    let token = tokio::time::timeout(
        FLOW_TIMEOUT,
        poll_token(&client, region, &registration, &authorization.device_code, interval),
    )
    .await
    .context("timed out waiting for the SSO browser approval")??;

    // 5. Persist to the shared cache so reqsign (and the AWS CLI) find it.
    write_cache(start_url, region, &registration, &token).await
}

/// Poll `CreateToken` on the given interval, treating `authorization_pending`
/// as "keep waiting" and `slow_down` as "back off", per the device-grant spec.
async fn poll_token(
    client: &reqwest::Client,
    region: &str,
    registration: &RegisterClientResponse,
    device_code: &str,
    mut interval: Duration,
) -> Result<TokenResult> {
    let token_url = oidc_endpoint(region, "/token");
    loop {
        tokio::time::sleep(interval).await;

        let body = serde_json::to_string(&CreateTokenRequest {
            client_id: &registration.client_id,
            client_secret: &registration.client_secret,
            grant_type: GRANT_DEVICE_CODE,
            device_code,
        })?;
        let (status, text) = post(client, &token_url, body).await?;
        let response: CreateTokenResponse = serde_json::from_str(&text)
            .with_context(|| format!("parsing SSO token response ({status}): {text}"))?;

        if let Some(access_token) = response.access_token {
            return Ok(TokenResult {
                access_token,
                expires_in: response.expires_in.unwrap_or(0),
                refresh_token: response.refresh_token,
            });
        }
        match response.error.as_deref() {
            Some("authorization_pending") => continue,
            Some("slow_down") => {
                interval += Duration::from_secs(5);
                continue;
            }
            Some(other) => bail!(
                "SSO sign-in failed: {other} {}",
                response.error_description.unwrap_or_default()
            ),
            None => bail!("SSO token endpoint returned neither a token nor an error: {text}"),
        }
    }
}

/// Write the token to `~/.aws/sso/cache/<sha1(start_url)>.json` in the aws-cli
/// schema. reqsign only reads `accessToken` / `expiresAt`; the extra fields let
/// the AWS CLI refresh the session from the same file.
async fn write_cache(
    start_url: &str,
    region: &str,
    registration: &RegisterClientResponse,
    token: &TokenResult,
) -> Result<()> {
    let home = dirs::home_dir().context("cannot locate the home directory")?;
    let cache_dir = home.join(".aws").join("sso").join("cache");
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .with_context(|| format!("creating {}", cache_dir.display()))?;

    let now = reqsign::time::Timestamp::now().as_second();
    let expires_at = reqsign::time::Timestamp::from_second(now + token.expires_in)
        .map_err(|error| anyhow::anyhow!("computing token expiry: {error}"))?
        .format_rfc3339_zulu();
    let registration_expires_at =
        reqsign::time::Timestamp::from_second(registration.client_secret_expires_at)
            .map(reqsign::time::Timestamp::format_rfc3339_zulu)
            .ok();

    let cached = CachedToken {
        start_url,
        region,
        access_token: &token.access_token,
        expires_at: &expires_at,
        client_id: &registration.client_id,
        client_secret: &registration.client_secret,
        refresh_token: token.refresh_token.as_deref(),
        registration_expires_at: registration_expires_at.as_deref(),
    };
    let json = serde_json::to_vec_pretty(&cached).context("serializing the SSO token cache")?;

    let path = cache_dir.join(format!("{}.json", hex_sha1(start_url.as_bytes())));
    tokio::fs::write(&path, json)
        .await
        .with_context(|| format!("writing the SSO token cache to {}", path.display()))?;
    Ok(())
}

/// POST a JSON body and deserialize a JSON response, failing on any non-2xx.
/// Used for `RegisterClient` / `StartDeviceAuthorization`, whose errors are
/// terminal (the token poll handles its own expected 4xx states instead).
async fn post_json<Req: Serialize, Res: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    request: &Req,
) -> Result<Res> {
    let (status, text) = post(client, url, serde_json::to_string(request)?).await?;
    if !status.is_success() {
        bail!("SSO OIDC request to {url} failed ({status}): {text}");
    }
    serde_json::from_str(&text).with_context(|| format!("parsing SSO OIDC response: {text}"))
}

/// Raw JSON POST returning the status and body without failing on non-2xx, so
/// callers can classify the response themselves.
async fn post(
    client: &reqwest::Client,
    url: &str,
    body: String,
) -> Result<(reqwest::StatusCode, String)> {
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .context("SSO OIDC request failed")?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    Ok((status, text))
}

fn hex_sha1(data: &[u8]) -> String {
    let digest = Sha1::digest(data);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

#[derive(Serialize)]
struct RegisterClientRequest<'a> {
    #[serde(rename = "clientName")]
    client_name: &'a str,
    #[serde(rename = "clientType")]
    client_type: &'a str,
}

#[derive(Deserialize)]
struct RegisterClientResponse {
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "clientSecret")]
    client_secret: String,
    /// Unix seconds; carried through to the cache for aws-cli refresh.
    #[serde(rename = "clientSecretExpiresAt", default)]
    client_secret_expires_at: i64,
}

#[derive(Serialize)]
struct StartDeviceAuthRequest<'a> {
    #[serde(rename = "clientId")]
    client_id: &'a str,
    #[serde(rename = "clientSecret")]
    client_secret: &'a str,
    #[serde(rename = "startUrl")]
    start_url: &'a str,
}

#[derive(Deserialize)]
struct StartDeviceAuthResponse {
    #[serde(rename = "deviceCode")]
    device_code: String,
    #[serde(rename = "verificationUri")]
    verification_uri: String,
    #[serde(rename = "verificationUriComplete")]
    verification_uri_complete: Option<String>,
    /// Minimum seconds between token polls, if the server specifies one.
    interval: Option<u64>,
}

#[derive(Serialize)]
struct CreateTokenRequest<'a> {
    #[serde(rename = "clientId")]
    client_id: &'a str,
    #[serde(rename = "clientSecret")]
    client_secret: &'a str,
    #[serde(rename = "grantType")]
    grant_type: &'a str,
    #[serde(rename = "deviceCode")]
    device_code: &'a str,
}

#[derive(Deserialize)]
struct CreateTokenResponse {
    #[serde(rename = "accessToken")]
    access_token: Option<String>,
    #[serde(rename = "expiresIn")]
    expires_in: Option<i64>,
    #[serde(rename = "refreshToken")]
    refresh_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

struct TokenResult {
    access_token: String,
    expires_in: i64,
    refresh_token: Option<String>,
}

/// The `~/.aws/sso/cache/*.json` shape written by the AWS CLI. reqsign reads
/// only `accessToken` / `expiresAt`; the rest enable CLI-side refresh.
#[derive(Serialize)]
struct CachedToken<'a> {
    #[serde(rename = "startUrl")]
    start_url: &'a str,
    region: &'a str,
    #[serde(rename = "accessToken")]
    access_token: &'a str,
    #[serde(rename = "expiresAt")]
    expires_at: &'a str,
    #[serde(rename = "clientId")]
    client_id: &'a str,
    #[serde(rename = "clientSecret")]
    client_secret: &'a str,
    #[serde(rename = "refreshToken", skip_serializing_if = "Option::is_none")]
    refresh_token: Option<&'a str>,
    #[serde(
        rename = "registrationExpiresAt",
        skip_serializing_if = "Option::is_none"
    )]
    registration_expires_at: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_matches_sha1_convention() {
        // Known SHA-1 vector: this is the file name reqsign / the AWS CLI look
        // for, so it must be lowercase hex of exactly the start URL's bytes.
        assert_eq!(
            hex_sha1(b"abc"),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        let key = hex_sha1(b"https://example.awsapps.com/start");
        assert_eq!(key.len(), 40);
        assert!(key.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }
}
