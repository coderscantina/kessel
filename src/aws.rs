//! Reader for the AWS shared config files (`~/.aws/credentials`,
//! `~/.aws/config`), used when an S3 bookmark authenticates via a named
//! profile instead of directly stored keys. Profiles are resolved at connect
//! time so rotated keys are picked up and nothing is persisted by kessel.
//!
//! Three profile shapes are supported:
//!   * **static** — `aws_access_key_id` / `aws_secret_access_key` (+ optional
//!     `aws_session_token`); returned as-is, no network.
//!   * **assume-role** — `role_arn` + `source_profile`; the source profile's
//!     static keys sign an STS `AssumeRole` call for temporary credentials.
//!   * **SSO** — `sso_*` inline config or an `sso_session` reference; the
//!     cached SSO token (`~/.aws/sso/cache`) is exchanged for temporary
//!     credentials via `GetRoleCredentials`.
//!
//! Assume-role/SSO resolution is handled by reqsign's credential providers
//! (`AssumeRoleCredentialProvider`, `SSOCredentialProvider`) — the same crate
//! kessel already signs S3 requests with. Because those calls hit the network,
//! resolved temporary credentials are cached in-process until shortly before
//! they expire, so provider reconnects stay fast.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

use anyhow::{bail, Context as _, Result};
use reqsign::aws::{
    AssumeRoleCredentialProvider, RequestSigner, SSOCredentialProvider,
    StaticCredentialProvider,
};
use reqsign::{default_context, ProvideCredential, Signer};

type Ini = HashMap<String, HashMap<String, String>>;

/// Re-resolve temporary credentials once they come within this many seconds
/// of expiring, so a connect never signs with an about-to-die token.
const EXPIRY_SKEW_SECS: i64 = 300;

/// In-process cache of resolved temporary credentials, keyed by profile name.
/// Assume-role / SSO resolution is a network round-trip; caching keeps
/// provider reconnects (which all go through `profile_options`) fast.
static CREDENTIAL_CACHE: LazyLock<Mutex<HashMap<String, ResolvedCreds>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ResolvedCreds {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    /// Unix seconds; `None` means non-expiring (never re-resolved on that basis).
    expires_at: Option<i64>,
}

impl ResolvedCreds {
    /// Still safely usable (not within the expiry skew window)?
    fn fresh(&self) -> bool {
        match self.expires_at {
            Some(exp) => exp - now_secs() > EXPIRY_SKEW_SECS,
            None => true,
        }
    }
}

fn now_secs() -> i64 {
    reqsign::time::Timestamp::now().as_second()
}

fn credentials_path() -> Option<PathBuf> {
    std::env::var_os("AWS_SHARED_CREDENTIALS_FILE")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".aws/credentials")))
}

fn config_path() -> Option<PathBuf> {
    std::env::var_os("AWS_CONFIG_FILE")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".aws/config")))
}

/// `[section]` / `key = value` / `#`-or-`;` comments. Nested `s3 =` blocks
/// and other exotica are ignored, which is fine for credential lookup.
fn parse_ini(text: &str) -> Ini {
    let mut sections = Ini::new();
    let mut current = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            current = name.trim().to_string();
        } else if let Some((key, value)) = line.split_once('=') {
            sections
                .entry(current.clone())
                .or_default()
                .insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    sections
}

fn load(path: Option<PathBuf>) -> Ini {
    path.and_then(|p| fs::read_to_string(p).ok())
        .map(|text| parse_ini(&text))
        .unwrap_or_default()
}

/// Profile names for the bookmark form's picker: union of both files, sorted.
/// The config file names sections `[profile foo]` (except `[default]`).
pub fn list_profiles() -> Vec<String> {
    let mut names: Vec<String> = load(credentials_path())
        .into_keys()
        .chain(load(config_path()).into_keys().filter_map(|name| {
            // Config file: keep `[profile foo]` and `[default]`; drop
            // `[sso-session …]` and other non-profile sections.
            if name == "default" {
                Some(name)
            } else {
                name.strip_prefix("profile ").map(str::to_string)
            }
        }))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The `[section]` that holds a profile's config-file settings: `[default]`
/// for the default profile, `[profile name]` otherwise (with a bare `[name]`
/// fallback some tools still write).
fn config_section<'a>(config: &'a Ini, profile: &str) -> Option<&'a HashMap<String, String>> {
    if profile == "default" {
        config.get("default")
    } else {
        config
            .get(&format!("profile {profile}"))
            .or_else(|| config.get(profile))
    }
}

/// Static keys for a profile, from either file (credentials file wins).
fn static_keys(
    profile: &str,
    credentials: &Ini,
    config: &Ini,
) -> Option<(String, String, Option<String>)> {
    let cred = credentials.get(profile);
    let conf = config_section(config, profile);
    let get = |key: &str| {
        cred.and_then(|s| s.get(key))
            .or_else(|| conf.and_then(|s| s.get(key)))
            .cloned()
    };
    match (get("aws_access_key_id"), get("aws_secret_access_key")) {
        (Some(access_key), Some(secret_key)) => {
            Some((access_key, secret_key, get("aws_session_token")))
        }
        _ => None,
    }
}

/// How a profile authenticates, decided from its parsed config. Kept separate
/// from the (networked) resolution so the classification is unit-testable.
#[derive(Debug, PartialEq)]
enum ProfileKind {
    Static,
    AssumeRole {
        role_arn: String,
        source_profile: String,
        external_id: Option<String>,
        role_session_name: Option<String>,
        mfa_serial: Option<String>,
    },
    /// SSO with all four settings inline on the profile.
    SsoInline,
    /// Modern `sso_session = name` form: start URL / region live in a separate
    /// `[sso-session name]` section; account id / role name stay on the profile.
    SsoSession {
        account_id: String,
        role_name: String,
        start_url: String,
        region: String,
    },
    /// No usable credential configuration.
    Unsupported,
}

fn classify(profile: &str, credentials: &Ini, config: &Ini) -> ProfileKind {
    if static_keys(profile, credentials, config).is_some() {
        return ProfileKind::Static;
    }
    let Some(section) = config_section(config, profile) else {
        return ProfileKind::Unsupported;
    };
    let get = |key: &str| section.get(key).cloned();

    if let Some(role_arn) = get("role_arn") {
        // `credential_source` (EC2/ECS/env) is a valid alternative to
        // `source_profile` but kessel only resolves the source-profile form.
        let Some(source_profile) = get("source_profile") else {
            return ProfileKind::Unsupported;
        };
        return ProfileKind::AssumeRole {
            role_arn,
            source_profile,
            external_id: get("external_id"),
            role_session_name: get("role_session_name"),
            mfa_serial: get("mfa_serial"),
        };
    }

    if let Some(session) = get("sso_session") {
        // Pull start URL / region from the shared `[sso-session name]` block.
        let sess = config.get(&format!("sso-session {session}"));
        match (
            get("sso_account_id"),
            get("sso_role_name"),
            sess.and_then(|s| s.get("sso_start_url")).cloned(),
            sess.and_then(|s| s.get("sso_region")).cloned(),
        ) {
            (Some(account_id), Some(role_name), Some(start_url), Some(region)) => {
                return ProfileKind::SsoSession {
                    account_id,
                    role_name,
                    start_url,
                    region,
                };
            }
            _ => return ProfileKind::Unsupported,
        }
    }

    if get("sso_start_url").is_some() {
        return ProfileKind::SsoInline;
    }

    ProfileKind::Unsupported
}

/// SSO profiles from `~/.aws`, mapped to the `(start_url, region)` the
/// interactive device-authorization login (`crate::sso`) needs — to run the
/// flow and to key the token cache. Non-SSO profiles are omitted, so the
/// bookmark form shows the "Sign in with AWS SSO" button only where it applies.
pub fn sso_profiles() -> HashMap<String, (String, String)> {
    let credentials = load(credentials_path());
    let config = load(config_path());
    list_profiles()
        .into_iter()
        .filter_map(|name| {
            sso_params(&name, &credentials, &config).map(|params| (name, params))
        })
        .collect()
}

/// `(start_url, region)` for an SSO profile, from whichever config shape it
/// uses; `None` for static / assume-role / unsupported profiles.
fn sso_params(profile: &str, credentials: &Ini, config: &Ini) -> Option<(String, String)> {
    match classify(profile, credentials, config) {
        ProfileKind::SsoSession {
            start_url, region, ..
        } => Some((start_url, region)),
        // Inline SSO keeps the start URL / region directly on the profile.
        ProfileKind::SsoInline => {
            let section = config_section(config, profile)?;
            Some((
                section.get("sso_start_url")?.clone(),
                section.get("sso_region")?.clone(),
            ))
        }
        _ => None,
    }
}

/// Resolve a profile to OpenDAL S3 operator options: temporary or static
/// access keys (plus session token) and the region. Assume-role / SSO
/// profiles hit the network on a cache miss; static profiles never do.
pub fn profile_options(profile: &str) -> Result<HashMap<String, String>> {
    let credentials = load(credentials_path());
    let config = load(config_path());

    if credentials.get(profile).is_none() && config_section(&config, profile).is_none() {
        bail!("AWS profile `{profile}` not found in ~/.aws");
    }

    // Region comes from the credentials or config file regardless of how the
    // keys are sourced; it also seeds the STS/SSO endpoint region below.
    let region = credentials
        .get(profile)
        .and_then(|s| s.get("region"))
        .or_else(|| config_section(&config, profile).and_then(|s| s.get("region")))
        .cloned();

    let mut options = HashMap::new();
    if let Some(region) = &region {
        options.insert("region".to_string(), region.clone());
    }

    let creds = resolve_credentials(profile, &credentials, &config, region.as_deref())?;
    options.insert("access_key_id".to_string(), creds.access_key_id);
    options.insert("secret_access_key".to_string(), creds.secret_access_key);
    if let Some(token) = creds.session_token {
        options.insert("session_token".to_string(), token);
    }
    Ok(options)
}

fn resolve_credentials(
    profile: &str,
    credentials: &Ini,
    config: &Ini,
    region: Option<&str>,
) -> Result<ResolvedCreds> {
    let kind = classify(profile, credentials, config);

    // Static profiles are cheap and rotate freely; never cache them.
    if let ProfileKind::Static = kind {
        let (access_key_id, secret_access_key, session_token) =
            static_keys(profile, credentials, config).expect("classified Static");
        return Ok(ResolvedCreds {
            access_key_id,
            secret_access_key,
            session_token,
            expires_at: None,
        });
    }

    // Serve a still-fresh cached resolution without touching the network.
    if let Some(cached) = CREDENTIAL_CACHE.lock().unwrap().get(profile) {
        if cached.fresh() {
            return Ok(cached.clone());
        }
    }

    let region = region.unwrap_or("us-east-1").to_string();
    let credential = match kind {
        ProfileKind::Static => unreachable!("handled above"),
        ProfileKind::AssumeRole {
            role_arn,
            source_profile,
            external_id,
            role_session_name,
            mfa_serial,
        } => {
            if mfa_serial.is_some() {
                bail!(
                    "AWS profile `{profile}` requires MFA (mfa_serial), which kessel \
                     cannot prompt for — run `aws sts get-session-token` and paste the \
                     temporary access key, secret and session token instead"
                );
            }
            let (src_ak, src_sk, src_token) = static_keys(&source_profile, credentials, config)
                .with_context(|| {
                    format!(
                        "assume-role profile `{profile}` needs static keys in its \
                         source_profile `{source_profile}`, which were not found"
                    )
                })?;
            block_on(async move {
                let ctx = default_context();
                let mut source = StaticCredentialProvider::new(&src_ak, &src_sk);
                if let Some(token) = &src_token {
                    source = source.with_session_token(token);
                }
                let sts_signer =
                    Signer::new(ctx.clone(), source, RequestSigner::new("sts", &region));
                let mut provider =
                    AssumeRoleCredentialProvider::new(role_arn, sts_signer).with_region(region);
                if let Some(id) = external_id {
                    provider = provider.with_external_id(id);
                }
                provider = provider
                    .with_role_session_name(role_session_name.unwrap_or_else(|| "kessel".into()));
                provider
                    .provide_credential(&ctx)
                    .await
                    .map_err(|error| anyhow::anyhow!("STS AssumeRole failed: {error}"))
            })?
        }
        ProfileKind::SsoInline => block_on({
            let profile = profile.to_string();
            async move {
                let ctx = default_context();
                SSOCredentialProvider::new()
                    .with_profile(profile)
                    .provide_credential(&ctx)
                    .await
                    .map_err(|error| anyhow::anyhow!("AWS SSO resolution failed: {error}"))
            }
        })?,
        ProfileKind::SsoSession {
            account_id,
            role_name,
            start_url,
            region: sso_region,
        } => block_on(async move {
            let ctx = default_context();
            SSOCredentialProvider::new()
                .with_account_id(account_id)
                .with_role_name(role_name)
                .with_start_url(start_url)
                .with_region(sso_region)
                .provide_credential(&ctx)
                .await
                .map_err(|error| anyhow::anyhow!("AWS SSO resolution failed: {error}"))
        })?,
        ProfileKind::Unsupported => bail!(
            "AWS profile `{profile}` has no static keys and no supported SSO or \
             assume-role configuration"
        ),
    };

    // A missing SSO token surfaces as `None` from the provider — the common
    // "your session lapsed" case, so point the user at the fix directly.
    let credential = credential.with_context(|| {
        format!(
            "AWS profile `{profile}`: no valid SSO session found — run \
             `aws sso login --profile {profile}` and try again"
        )
    })?;

    let resolved = ResolvedCreds {
        access_key_id: credential.access_key_id,
        secret_access_key: credential.secret_access_key,
        session_token: credential.session_token,
        expires_at: credential.expires_in.map(|ts| ts.as_second()),
    };
    CREDENTIAL_CACHE
        .lock()
        .unwrap()
        .insert(profile.to_string(), resolved.clone());
    Ok(resolved)
}

/// Run a networked resolution future to completion. `profile_options` is sync
/// but is called from `Provider::connect` on the app's (multi-threaded) tokio
/// runtime, so we hand the future back to that runtime via `block_in_place`
/// rather than spinning up a nested runtime. Off-runtime callers (unit tests
/// that don't reach the network) get a throwaway current-thread runtime.
fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("building a temporary tokio runtime")
            .block_on(future),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_profiles_and_keys() {
        let ini = parse_ini(
            "# comment\n\
             [default]\n\
             aws_access_key_id = AKIADEFAULT\n\
             aws_secret_access_key= sekret \n\
             \n\
             [profile prod]\n\
             region = eu-central-1\n\
             ; another comment\n",
        );
        assert_eq!(ini["default"]["aws_access_key_id"], "AKIADEFAULT");
        assert_eq!(ini["default"]["aws_secret_access_key"], "sekret");
        assert_eq!(ini["profile prod"]["region"], "eu-central-1");
    }

    #[test]
    fn resolves_static_profile_via_env_paths() {
        let dir = std::env::temp_dir().join("kessel-aws-test-static");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("credentials"),
            "[work]\naws_access_key_id = AKIAWORK\naws_secret_access_key = shh\n\
             aws_session_token = tok\n",
        )
        .unwrap();
        std::fs::write(dir.join("config"), "[profile work]\nregion = us-west-2\n").unwrap();
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", dir.join("credentials"));
        std::env::set_var("AWS_CONFIG_FILE", dir.join("config"));

        assert_eq!(list_profiles(), vec!["work".to_string()]);
        let options = profile_options("work").unwrap();
        assert_eq!(options["access_key_id"], "AKIAWORK");
        assert_eq!(options["secret_access_key"], "shh");
        assert_eq!(options["session_token"], "tok");
        assert_eq!(options["region"], "us-west-2");
        assert!(profile_options("missing").is_err());

        std::env::remove_var("AWS_SHARED_CREDENTIALS_FILE");
        std::env::remove_var("AWS_CONFIG_FILE");
    }

    #[test]
    fn classifies_static_profile() {
        let credentials = parse_ini("[work]\naws_access_key_id = A\naws_secret_access_key = B\n");
        let config = Ini::new();
        assert_eq!(classify("work", &credentials, &config), ProfileKind::Static);
    }

    #[test]
    fn classifies_assume_role_profile() {
        let credentials = parse_ini("[base]\naws_access_key_id = A\naws_secret_access_key = B\n");
        let config = parse_ini(
            "[profile deploy]\n\
             role_arn = arn:aws:iam::123456789012:role/Deploy\n\
             source_profile = base\n\
             external_id = xyz\n\
             role_session_name = kessel-test\n",
        );
        assert_eq!(
            classify("deploy", &credentials, &config),
            ProfileKind::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/Deploy".into(),
                source_profile: "base".into(),
                external_id: Some("xyz".into()),
                role_session_name: Some("kessel-test".into()),
                mfa_serial: None,
            }
        );
    }

    #[test]
    fn assume_role_without_source_profile_is_unsupported() {
        let config = parse_ini(
            "[profile deploy]\n\
             role_arn = arn:aws:iam::123456789012:role/Deploy\n\
             credential_source = Ec2InstanceMetadata\n",
        );
        assert_eq!(
            classify("deploy", &Ini::new(), &config),
            ProfileKind::Unsupported
        );
    }

    #[test]
    fn classifies_inline_sso_profile() {
        let config = parse_ini(
            "[profile sso]\n\
             sso_start_url = https://example.awsapps.com/start\n\
             sso_region = us-east-1\n\
             sso_account_id = 123456789012\n\
             sso_role_name = ReadOnly\n",
        );
        assert_eq!(classify("sso", &Ini::new(), &config), ProfileKind::SsoInline);
    }

    #[test]
    fn classifies_sso_session_profile() {
        let config = parse_ini(
            "[profile sso]\n\
             sso_session = corp\n\
             sso_account_id = 123456789012\n\
             sso_role_name = ReadOnly\n\
             \n\
             [sso-session corp]\n\
             sso_start_url = https://corp.awsapps.com/start\n\
             sso_region = eu-central-1\n",
        );
        assert_eq!(
            classify("sso", &Ini::new(), &config),
            ProfileKind::SsoSession {
                account_id: "123456789012".into(),
                role_name: "ReadOnly".into(),
                start_url: "https://corp.awsapps.com/start".into(),
                region: "eu-central-1".into(),
            }
        );
    }

    #[test]
    fn sso_params_reads_both_config_shapes() {
        let inline = parse_ini(
            "[profile sso]\n\
             sso_start_url = https://example.awsapps.com/start\n\
             sso_region = us-east-1\n\
             sso_account_id = 123456789012\n\
             sso_role_name = ReadOnly\n",
        );
        assert_eq!(
            sso_params("sso", &Ini::new(), &inline),
            Some((
                "https://example.awsapps.com/start".into(),
                "us-east-1".into()
            ))
        );

        let session = parse_ini(
            "[profile sso]\n\
             sso_session = corp\n\
             sso_account_id = 123456789012\n\
             sso_role_name = ReadOnly\n\
             \n\
             [sso-session corp]\n\
             sso_start_url = https://corp.awsapps.com/start\n\
             sso_region = eu-central-1\n",
        );
        assert_eq!(
            sso_params("sso", &Ini::new(), &session),
            Some((
                "https://corp.awsapps.com/start".into(),
                "eu-central-1".into()
            ))
        );

        // Static profiles have no SSO params.
        let static_profile =
            parse_ini("[work]\naws_access_key_id = A\naws_secret_access_key = B\n");
        assert_eq!(sso_params("work", &static_profile, &Ini::new()), None);
    }

    #[test]
    fn profile_without_credentials_is_unsupported() {
        let config = parse_ini("[profile empty]\nregion = us-east-1\n");
        assert_eq!(
            classify("empty", &Ini::new(), &config),
            ProfileKind::Unsupported
        );
    }
}
