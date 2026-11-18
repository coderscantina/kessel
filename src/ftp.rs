//! FTP commands OpenDAL's service doesn't offer: rename, and creating an empty
//! file (its writer does nothing without bytes). Each call opens one short
//! control connection with the settings the site's Operator uses.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_rustls::rustls::{self, ClientConfig, RootCertStore};
use futures_rustls::TlsConnector;
use opendal::{Error, ErrorKind, Result};
use suppaftp::async_std::{AsyncRustlsConnector, AsyncRustlsFtpStream, ImplAsyncFtpStream};
use suppaftp::types::FileType;
use suppaftp::{FtpError, Status};

/// Rename `from` to `to`, operator-relative paths below the site root. Most
/// servers replace an existing `to`; the rest answer with an error.
pub async fn rename(
    options: &HashMap<String, String>,
    from: &str,
    to: &str,
    timeout: Duration,
) -> Result<()> {
    let (from, to) = (from.trim_end_matches('/'), to.trim_end_matches('/'));
    bounded(timeout, async {
        let mut ftp = connect(options).await?;
        ftp.rename(from, to).await.map_err(into_opendal)?;
        let _ = ftp.quit().await;
        Ok(())
    })
    .await
}

/// Create (or truncate) `path` as an empty file. Its folder must exist.
pub async fn create_empty(options: &HashMap<String, String>, path: &str, timeout: Duration) -> Result<()> {
    bounded(timeout, async {
        let mut ftp = connect(options).await?;
        ftp.put_file(path, &mut futures::io::empty())
            .await
            .map_err(into_opendal)?;
        let _ = ftp.quit().await;
        Ok(())
    })
    .await
}

async fn bounded(timeout: Duration, work: impl Future<Output = Result<()>>) -> Result<()> {
    tokio::time::timeout(timeout, work).await.unwrap_or_else(|_| {
        Err(Error::new(ErrorKind::Unexpected, "ftp command timed out").set_temporary())
    })
}

/// Where and how to reach the server, read the way OpenDAL's FTP builder
/// reads its `endpoint`: no scheme means explicit FTPS, port 21 by default.
#[derive(Debug, PartialEq, Eq)]
struct Endpoint {
    host: String,
    port: u16,
    secure: bool,
}

fn parse_endpoint(endpoint: &str) -> Result<Endpoint> {
    let invalid = || Error::new(ErrorKind::ConfigInvalid, "ftp endpoint is invalid");
    let uri: http::Uri = endpoint.parse().map_err(|_| invalid())?;
    let secure = match uri.scheme_str() {
        Some("ftp") => false,
        Some("ftps") | None => true,
        Some(_) => return Err(invalid()),
    };
    Ok(Endpoint {
        host: uri.host().unwrap_or("127.0.0.1").to_string(),
        port: uri.port_u16().unwrap_or(21),
        secure,
    })
}

async fn connect(options: &HashMap<String, String>) -> Result<AsyncRustlsFtpStream> {
    let option = |key: &str| options.get(key).map_or("", String::as_str);
    let endpoint = parse_endpoint(option("endpoint"))?;
    let mut ftp = ImplAsyncFtpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(into_opendal)?;
    if endpoint.secure {
        ftp = ftp
            .into_secure(tls_connector()?, &endpoint.host)
            .await
            .map_err(into_opendal)?;
    }
    if !option("user").is_empty() {
        ftp.login(option("user"), option("password"))
            .await
            .map_err(into_opendal)?;
    }
    ftp.cwd(opendal::raw::normalize_root(option("root")))
        .await
        .map_err(into_opendal)?;
    ftp.transfer_type(FileType::Binary).await.map_err(into_opendal)?;
    Ok(ftp)
}

/// Verifies the server against the platform's trusted certificates.
fn tls_connector() -> Result<AsyncRustlsConnector> {
    let tls_error = |error: rustls::Error| Error::new(ErrorKind::Unexpected, "ftp tls setup failed").set_source(error);
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
    let config = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(AsyncRustlsConnector::from(TlsConnector::from(Arc::new(config))))
}

/// Dropped connections and "too many connections" can pass; the queue retries
/// those. Anything else the server refused stays an error.
fn into_opendal(error: FtpError) -> Error {
    let temporary = match &error {
        FtpError::ConnectionError(_) | FtpError::BadResponse => true,
        FtpError::UnexpectedResponse(response) => response.status == Status::NotAvailable,
        _ => false,
    };
    let kind = match &error {
        FtpError::UnexpectedResponse(response) if response.status == Status::NotLoggedIn => {
            ErrorKind::PermissionDenied
        }
        _ => ErrorKind::Unexpected,
    };
    let converted = Error::new(kind, "ftp command failed").set_source(error);
    if temporary {
        converted.set_temporary()
    } else {
        converted
    }
}

#[cfg(test)]
pub(crate) mod test_server {
    //! A small in-memory FTP server speaking just what OpenDAL and this
    //! module send. Like vsftpd it hides dotfiles from `LIST`.

    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{BufRead as _, BufReader, Read as _, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Tree {
        files: BTreeMap<String, Vec<u8>>,
        dirs: BTreeSet<String>,
    }

    #[derive(Clone)]
    pub(crate) struct FakeFtp {
        pub port: u16,
        tree: Arc<Mutex<Tree>>,
    }

    impl FakeFtp {
        pub fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = Self { port, tree: Arc::default() };
            let shared = server.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming().flatten() {
                    let session = shared.clone();
                    std::thread::spawn(move || session.serve(stream));
                }
            });
            server
        }

        pub fn put(&self, path: &str, content: &[u8]) {
            self.tree.lock().unwrap().files.insert(path.to_string(), content.to_vec());
        }

        pub fn read(&self, path: &str) -> Option<Vec<u8>> {
            self.tree.lock().unwrap().files.get(path).cloned()
        }

        pub fn names(&self) -> Vec<String> {
            self.tree.lock().unwrap().files.keys().cloned().collect()
        }

        fn serve(&self, stream: TcpStream) -> Option<()> {
            let mut out = stream.try_clone().ok()?;
            let mut lines = BufReader::new(stream);
            let mut passive: Option<TcpListener> = None;
            let mut rename_from = String::new();
            write!(out, "220 ready\r\n").ok()?;
            loop {
                let mut line = String::new();
                if lines.read_line(&mut line).ok()? == 0 {
                    return None;
                }
                let line = line.trim_end();
                let (command, arg) = line.split_once(' ').unwrap_or((line, ""));
                let path = arg.trim_start_matches('/').to_string();
                let reply = match command {
                    "USER" | "PASS" => "230 ok".to_string(),
                    "QUIT" => {
                        write!(out, "221 bye\r\n").ok()?;
                        return Some(());
                    }
                    "PASV" => {
                        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
                        let port = listener.local_addr().ok()?.port();
                        passive = Some(listener);
                        format!("227 Entering Passive Mode (127,0,0,1,{},{})", port >> 8, port & 255)
                    }
                    "MKD" => {
                        self.tree.lock().unwrap().dirs.insert(path.trim_end_matches('/').to_string());
                        "257 created".to_string()
                    }
                    "DELE" => match self.tree.lock().unwrap().files.remove(&path) {
                        Some(_) => "250 deleted".to_string(),
                        None => "550 not found".to_string(),
                    },
                    "RNFR" if self.tree.lock().unwrap().files.contains_key(&path) => {
                        rename_from = path;
                        "350 go on".to_string()
                    }
                    "RNFR" => "550 not found".to_string(),
                    "RNTO" => {
                        let mut tree = self.tree.lock().unwrap();
                        let content = tree.files.remove(&rename_from)?;
                        tree.files.insert(path, content);
                        "250 renamed".to_string()
                    }
                    "LIST" | "STOR" | "APPE" | "RETR" => {
                        write!(out, "150 data\r\n").ok()?;
                        let (mut data, _) = passive.take()?.accept().ok()?;
                        match command {
                            "LIST" => data.write_all(self.listing(&path).as_bytes()).ok()?,
                            "RETR" => data.write_all(&self.read(&path)?).ok()?,
                            _ => self.receive(command == "APPE", path, &mut data),
                        }
                        drop(data);
                        "226 done".to_string()
                    }
                    _ => "200 ok".to_string(),
                };
                write!(out, "{reply}\r\n").ok()?;
            }
        }

        /// Stores bytes as they arrive, so an interrupted upload leaves what
        /// reached the server.
        fn receive(&self, append: bool, path: String, data: &mut TcpStream) {
            if !append {
                self.tree.lock().unwrap().files.insert(path.clone(), Vec::new());
            }
            let mut buf = [0u8; 64 * 1024];
            while let Ok(read @ 1..) = data.read(&mut buf) {
                let mut tree = self.tree.lock().unwrap();
                tree.files.entry(path.clone()).or_default().extend_from_slice(&buf[..read]);
            }
        }

        fn listing(&self, dir: &str) -> String {
            let dir = dir.trim_end_matches('/');
            let prefix = if dir.is_empty() { String::new() } else { format!("{dir}/") };
            let tree = self.tree.lock().unwrap();
            let child = |path: &'_ str| {
                path.strip_prefix(&prefix)
                    .filter(|name| !name.contains('/') && !name.starts_with('.'))
                    .map(str::to_string)
            };
            let files = tree.files.iter().filter_map(|(path, content)| {
                child(path).map(|name| format!("-rw-r--r-- 1 o g {} Jan 01 12:00 {name}\r\n", content.len()))
            });
            let dirs = tree
                .dirs
                .iter()
                .filter_map(|path| child(path).map(|name| format!("drwxr-xr-x 1 o g 0 Jan 01 12:00 {name}\r\n")));
            files.chain(dirs).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_are_read_like_the_operator_reads_them() {
        let endpoint = |secure, port| Endpoint { host: "files.example.org".into(), port, secure };
        assert_eq!(parse_endpoint("ftp://files.example.org").unwrap(), endpoint(false, 21));
        assert_eq!(parse_endpoint("ftps://files.example.org:2121").unwrap(), endpoint(true, 2121));
        assert_eq!(parse_endpoint("files.example.org:990").unwrap(), endpoint(true, 990));
        assert!(parse_endpoint("sftp://files.example.org").is_err());
    }

    #[tokio::test]
    async fn renames_and_creates_empty_files_on_the_server() {
        let server = test_server::FakeFtp::start();
        server.put("old.txt", b"content");
        let options = HashMap::from([("endpoint".to_string(), format!("ftp://127.0.0.1:{}", server.port))]);

        rename(&options, "old.txt", "new.txt", Duration::from_secs(5)).await.unwrap();
        create_empty(&options, "empty.txt", Duration::from_secs(5)).await.unwrap();

        assert_eq!(server.names(), ["empty.txt", "new.txt"]);
        assert_eq!(server.read("new.txt").unwrap(), b"content");
        assert_eq!(server.read("empty.txt").unwrap(), b"");
    }
}
