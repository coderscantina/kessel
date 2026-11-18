//! S3 operations that OpenDAL's bucket-scoped `Operator` can't express,
//! hand-signed with reqsign: ListBuckets (the "no bucket configured — browse
//! all buckets" root view), per-object storage classes (ListObjectsV2 keeps
//! them, OpenDAL's lister drops them), storage-class changes (copy-in-place),
//! canned ACLs and bucket regions.

use std::collections::HashMap;

use anyhow::{bail, Context as _, Result};
use chrono::{DateTime, Utc};
use reqsign::aws::{default_signer, StaticCredentialProvider};

pub struct Bucket {
    pub name: String,
    pub created: Option<DateTime<Utc>>,
}

/// Signing context pulled out of a site's resolved operator options
/// (keychain secrets and AWS profile already merged in).
struct Signing {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
    endpoint: String,
}

fn signing(options: &HashMap<String, String>) -> Result<Signing> {
    let get = |key: &str| options.get(key).map(String::as_str);
    let (Some(access_key), Some(secret_key)) = (get("access_key_id"), get("secret_access_key"))
    else {
        bail!("this operation needs an access key and secret (or an AWS profile)");
    };
    let region = get("region").unwrap_or("us-east-1").to_string();
    let endpoint = match get("endpoint") {
        Some(endpoint) => endpoint.trim_end_matches('/').to_string(),
        None => format!("https://s3.{region}.amazonaws.com"),
    };
    Ok(Signing {
        access_key_id: access_key.to_string(),
        secret_access_key: secret_key.to_string(),
        session_token: options.get("session_token").cloned(),
        region,
        endpoint,
    })
}

/// Sign and execute one request; non-2xx becomes an error carrying the S3
/// `<Message>` when present. Returns the response body (empty for HEAD) and
/// the response headers.
async fn send(
    signing: &Signing,
    method: reqwest::Method,
    url: reqwest::Url,
    headers: &[(&'static str, String)],
    what: &str,
) -> Result<(String, reqwest::header::HeaderMap)> {
    let (status, body, headers) = execute(signing, &reqwest::Client::new(), method, url, headers, what).await?;
    if !status.is_success() {
        let message = tag_text(&body, "Message").unwrap_or(&body);
        bail!("{what} failed ({status}): {}", message.trim());
    }
    Ok((body, headers))
}

/// Sign and execute one request, whatever status it answers with.
async fn execute(
    signing: &Signing,
    client: &reqwest::Client,
    method: reqwest::Method,
    url: reqwest::Url,
    headers: &[(&'static str, String)],
    what: &str,
) -> Result<(reqwest::StatusCode, String, reqwest::header::HeaderMap)> {
    // reqsign 0.20 signs an `http::request::Parts` (SigV4, empty/unsigned
    // payload). Build the request parts, sign, then hand off to reqwest.
    let (mut parts, _) = http::Request::builder()
        .method(method)
        .uri(url.as_str())
        .body(())
        .with_context(|| format!("building {what} request"))?
        .into_parts();
    for (name, value) in headers {
        parts.headers.insert(
            *name,
            value
                .parse()
                .with_context(|| format!("invalid header value for {name}"))?,
        );
    }

    let mut credential =
        StaticCredentialProvider::new(&signing.access_key_id, &signing.secret_access_key);
    if let Some(token) = &signing.session_token {
        credential = credential.with_session_token(token);
    }
    default_signer("s3", &signing.region)
        .with_credential_provider(credential)
        .sign(&mut parts, None)
        .await
        .map_err(|error| anyhow::anyhow!("signing {what} request: {error}"))?;

    let request = reqwest::Request::try_from(http::Request::from_parts(parts, Vec::<u8>::new()))
        .with_context(|| format!("building {what} request"))?;
    let response = client
        .execute(request)
        .await
        .with_context(|| format!("{what} on {}", signing.endpoint))?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .text()
        .await
        .with_context(|| format!("reading {what} response"))?;
    Ok((status, body, headers))
}

/// `{endpoint}/{bucket}/{key}` with each path segment percent-encoded.
fn object_url(signing: &Signing, bucket: &str, key: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&signing.endpoint)
        .with_context(|| format!("invalid S3 endpoint `{}`", signing.endpoint))?;
    url.path_segments_mut()
        .ok()
        .context("S3 endpoint cannot carry a path")?
        .push(bucket)
        .extend(key.split('/'));
    Ok(url)
}

/// List the account's buckets. `options` are the site's resolved operator
/// options (keychain secrets and AWS profile already merged in), the same map
/// a bucket-scoped Operator would be built from.
pub async fn list_buckets(options: &HashMap<String, String>) -> Result<Vec<Bucket>> {
    let signing = signing(options)?;
    let url = reqwest::Url::parse(&signing.endpoint)
        .with_context(|| format!("invalid S3 endpoint `{}`", signing.endpoint))?;
    let (body, _) = send(&signing, reqwest::Method::GET, url, &[], "ListBuckets").await?;
    Ok(parse_list_buckets(&body))
}

/// Per-object storage classes for one directory level: full object key →
/// storage class ("STANDARD", "GLACIER", …). One ListObjectsV2 page per call
/// (fine for a browse view; deeper pages are simply left unknown).
pub async fn storage_classes(
    options: &HashMap<String, String>,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, String>> {
    let signing = signing(options)?;
    let mut url = reqwest::Url::parse(&signing.endpoint)?;
    url.path_segments_mut().ok().context("bad endpoint")?.push(bucket);
    url.query_pairs_mut()
        .append_pair("list-type", "2")
        .append_pair("delimiter", "/")
        .append_pair("prefix", prefix)
        .append_pair("max-keys", "1000");
    let (body, _) = send(&signing, reqwest::Method::GET, url, &[], "ListObjectsV2").await?;
    Ok(parse_storage_classes(&body))
}

/// Key → storage class from a `ListObjectsV2` document.
fn parse_storage_classes(xml: &str) -> HashMap<String, String> {
    xml.split("<Contents>")
        .skip(1)
        .filter_map(|block| {
            Some((
                tag_text(block, "Key")?.to_string(),
                tag_text(block, "StorageClass")?.to_string(),
            ))
        })
        .collect()
}

/// Change an object's storage class: CopyObject onto itself with the new
/// class. Metadata is carried over; objects above the 5 GB copy limit fail
/// with S3's own error.
pub async fn set_storage_class(
    options: &HashMap<String, String>,
    bucket: &str,
    key: &str,
    class: &str,
) -> Result<()> {
    let signing = signing(options)?;
    let url = object_url(&signing, bucket, key)?;
    // The copy-source header wants the percent-encoded /bucket/key path.
    let copy_source = url.path().to_string();
    send(
        &signing,
        reqwest::Method::PUT,
        url,
        &[
            ("x-amz-copy-source", copy_source),
            ("x-amz-storage-class", class.to_string()),
            ("x-amz-metadata-directive", "COPY".to_string()),
        ],
        "CopyObject (storage class)",
    )
    .await?;
    Ok(())
}

/// Apply a canned ACL ("private", "public-read", …) to one object.
pub async fn set_acl(
    options: &HashMap<String, String>,
    bucket: &str,
    key: &str,
    acl: &str,
) -> Result<()> {
    let signing = signing(options)?;
    let mut url = object_url(&signing, bucket, key)?;
    url.set_query(Some("acl"));
    send(
        &signing,
        reqwest::Method::PUT,
        url,
        &[("x-amz-acl", acl.to_string())],
        "PutObjectAcl",
    )
    .await?;
    Ok(())
}

/// A bucket's region, from HeadBucket's `x-amz-bucket-region` header. S3 sends
/// it with the 301 or 403 a request for another region gets too, so the status
/// doesn't matter and redirects aren't followed.
pub async fn bucket_region(
    options: &HashMap<String, String>,
    bucket: &str,
) -> Result<Option<String>> {
    let signing = signing(options)?;
    let mut url = reqwest::Url::parse(&signing.endpoint)?;
    url.path_segments_mut().ok().context("bad endpoint")?.push(bucket);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("building HeadBucket client")?;
    let (_, _, headers) = execute(&signing, &client, reqwest::Method::HEAD, url, &[], "HeadBucket").await?;
    Ok(headers
        .get("x-amz-bucket-region")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string))
}

/// Pull the buckets out of a `ListAllMyBucketsResult` document. Bucket names
/// are restricted to `[a-z0-9.-]`, so plain tag scanning is safe and spares
/// us an XML dependency.
fn parse_list_buckets(xml: &str) -> Vec<Bucket> {
    let section = xml
        .split("<Buckets>")
        .nth(1)
        .and_then(|rest| rest.split("</Buckets>").next())
        .unwrap_or("");
    section
        .split("<Bucket>")
        .skip(1)
        .filter_map(|block| {
            let name = tag_text(block, "Name")?;
            let created = tag_text(block, "CreationDate")
                .and_then(|date| DateTime::parse_from_rfc3339(date).ok())
                .map(|date| date.with_timezone(&Utc));
            Some(Bucket {
                name: name.to_string(),
                created,
            })
        })
        .collect()
}

fn tag_text<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let start = block.find(&format!("<{tag}>"))? + tag.len() + 2;
    let end = block[start..].find(&format!("</{tag}>"))? + start;
    Some(&block[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_bucket_in_another_region_is_found_through_the_redirect() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 301 Moved Permanently\r\nx-amz-bucket-region: eu-west-1\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        let options = HashMap::from([
            ("access_key_id".to_string(), "key".to_string()),
            ("secret_access_key".to_string(), "secret".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
            ("endpoint".to_string(), endpoint),
        ]);

        let region = bucket_region(&options, "elsewhere").await.unwrap();
        assert_eq!(region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn parses_list_buckets_response() {
        let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
            <ListAllMyBucketsResult>\
              <Owner><ID>abc</ID><DisplayName>me</DisplayName></Owner>\
              <Buckets>\
                <Bucket><Name>alpha</Name><CreationDate>2024-01-02T03:04:05.000Z</CreationDate></Bucket>\
                <Bucket><Name>beta</Name></Bucket>\
              </Buckets>\
            </ListAllMyBucketsResult>";
        let buckets = parse_list_buckets(xml);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].name, "alpha");
        assert!(buckets[0].created.is_some());
        assert_eq!(buckets[1].name, "beta");
        assert!(buckets[1].created.is_none());
    }

    #[test]
    fn parses_storage_classes_from_list_objects() {
        let xml = "<ListBucketResult>\
            <Contents><Key>docs/a.txt</Key><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>\
            <Contents><Key>docs/b.txt</Key><StorageClass>GLACIER</StorageClass></Contents>\
            <Contents><Key>docs/c.txt</Key></Contents>\
            </ListBucketResult>";
        let classes = parse_storage_classes(xml);
        assert_eq!(classes.len(), 2);
        assert_eq!(classes["docs/a.txt"], "STANDARD");
        assert_eq!(classes["docs/b.txt"], "GLACIER");
    }

    #[test]
    fn empty_or_error_bodies_parse_to_nothing() {
        assert!(parse_list_buckets("<Error><Message>denied</Message></Error>").is_empty());
        assert_eq!(tag_text("<Error><Message>denied</Message></Error>", "Message"), Some("denied"));
    }
}
