//! Import sites from other clients: FileZilla's `sitemanager.xml` and
//! Cyberduck's `.duck` bookmark files (an XML property list). Parsing only;
//! the bookmark manager previews the result before anything is saved.
//!
//! Passwords are never read. Sites that sign in with one are flagged so the
//! user enters it on first connect.

use std::collections::HashMap;

use quick_xml::events::Event;
use quick_xml::Reader;

use crate::bookmarks::ftp_endpoint;

/// A site the import can create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportTarget {
    pub scheme: &'static str,
    pub options: HashMap<String, String>,
    /// Signs in with a password the file kept but we don't import.
    pub needs_credentials: bool,
}

/// One row of the import preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportEntry {
    pub name: String,
    /// Source folder; nested folders are joined with " / ".
    pub folder: Option<String>,
    /// `Err` names what the source site uses that kessel can't connect to.
    pub target: Result<ImportTarget, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("not a FileZilla or Cyberduck bookmark file")]
    Unrecognized,
    #[error("the file is not valid XML: {0}")]
    Xml(String),
}

/// Parse one file's text, detecting FileZilla or Cyberduck by content.
pub fn parse(content: &str) -> Result<Vec<ImportEntry>, ImportError> {
    if content.contains("<FileZilla3") {
        parse_filezilla(content)
    } else if content.contains("<plist") {
        parse_cyberduck(content)
    } else {
        Err(ImportError::Unrecognized)
    }
}

#[derive(Default)]
struct Node {
    name: String,
    text: String,
    children: Vec<Node>,
}

impl Node {
    fn child(&self, name: &str) -> Option<&Node> {
        self.children.iter().find(|child| child.name == name)
    }

    /// Trimmed text of the named child; `None` when missing or empty.
    fn value(&self, name: &str) -> Option<&str> {
        self.child(name)
            .map(|child| child.text.trim())
            .filter(|text| !text.is_empty())
    }
}

/// A minimal element tree: names and text only, which is all either format
/// needs. Returns the document element.
fn parse_tree(xml: &str) -> Result<Node, ImportError> {
    let mut reader = Reader::from_str(xml);
    let mut stack = vec![Node::default()];
    let xml_error = |error: &dyn std::fmt::Display| ImportError::Xml(error.to_string());
    let element = |name: &[u8]| Node {
        name: String::from_utf8_lossy(name).into_owned(),
        ..Default::default()
    };
    loop {
        match reader.read_event().map_err(|error| xml_error(&error))? {
            Event::Start(start) => stack.push(element(start.name().as_ref())),
            Event::Empty(start) => {
                let node = element(start.name().as_ref());
                stack.last_mut().expect("root").children.push(node);
            }
            Event::End(_) => {
                let node = stack.pop().expect("root");
                stack.last_mut().ok_or(ImportError::Unrecognized)?.children.push(node);
            }
            Event::Text(text) => {
                let text = text.decode().map_err(|error| xml_error(&error))?;
                stack.last_mut().expect("root").text.push_str(&text);
            }
            Event::CData(data) => {
                let text = data.decode().map_err(|error| xml_error(&error))?;
                stack.last_mut().expect("root").text.push_str(&text);
            }
            Event::GeneralRef(reference) => {
                let text = match reference.resolve_char_ref().map_err(|error| xml_error(&error))? {
                    Some(ch) => ch.to_string(),
                    None => match reference.decode().map_err(|error| xml_error(&error))?.as_ref() {
                        "amp" => "&".into(),
                        "lt" => "<".into(),
                        "gt" => ">".into(),
                        "quot" => "\"".into(),
                        "apos" => "'".into(),
                        other => format!("&{other};"),
                    },
                };
                stack.last_mut().expect("root").text.push_str(&text);
            }
            Event::Eof => break,
            _ => {}
        }
    }
    let mut root = stack.pop().ok_or(ImportError::Unrecognized)?;
    if !stack.is_empty() || root.children.is_empty() {
        return Err(ImportError::Unrecognized);
    }
    Ok(root.children.swap_remove(0))
}

/// What a site needs to become a kessel target, independent of the source.
/// Passwords are not part of it on purpose.
struct Source<'a> {
    name: Option<&'a str>,
    host: &'a str,
    port: Option<u16>,
    user: Option<&'a str>,
    key_file: Option<&'a str>,
    root: Option<&'a str>,
}

enum Kind {
    Ftp { tls: bool },
    Sftp,
    WebDav { tls: bool },
}

fn entry(source: Source, kind: Kind, folder: Option<String>) -> ImportEntry {
    let port = |default: u16| {
        source
            .port
            .filter(|port| *port != default)
            .map(|port| format!(":{port}"))
            .unwrap_or_default()
    };
    let host = source.host;
    let mut options = HashMap::new();
    let mut set = |key: &str, value: Option<&str>| {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            options.insert(key.to_string(), value.to_string());
        }
    };
    let user = source.user.filter(|user| !user.eq_ignore_ascii_case("anonymous"));
    let (scheme, needs_credentials) = match kind {
        Kind::Ftp { tls } => {
            set("endpoint", Some(&ftp_endpoint(&format!("{host}{}", port(21)), tls)));
            set("user", user);
            ("ftp", user.is_some())
        }
        Kind::Sftp => {
            set("endpoint", Some(&format!("ssh://{host}{}", port(22))));
            set("user", user);
            set("key", source.key_file);
            ("sftp", false)
        }
        Kind::WebDav { tls } => {
            let (scheme, default) = if tls { ("https", 443) } else { ("http", 80) };
            set("endpoint", Some(&format!("{scheme}://{host}{}", port(default))));
            set("username", user);
            ("webdav", user.is_some())
        }
    };
    set("root", source.root.filter(|root| root.starts_with('/')));
    let target = if scheme == "sftp" && !crate::sftp::SUPPORTED {
        Err("SFTP (not available on this platform)".to_string())
    } else {
        Ok(ImportTarget {
            scheme,
            options,
            needs_credentials,
        })
    };
    ImportEntry {
        name: source.name.unwrap_or(host).to_string(),
        folder,
        target,
    }
}

fn unsupported(name: Option<&str>, host: &str, protocol: String, folder: Option<String>) -> ImportEntry {
    ImportEntry {
        name: name.unwrap_or(host).to_string(),
        folder,
        target: Err(protocol),
    }
}

/// FileZilla's numeric `<Protocol>`: 0 FTP (TLS when the server offers it),
/// 1 SFTP, 4 explicit FTPS, 6 plain FTP. Implicit FTPS and everything else
/// has no OpenDAL service behind it.
fn filezilla_kind(protocol: i64) -> Result<Kind, String> {
    match protocol {
        0 | 4 => Ok(Kind::Ftp { tls: true }),
        6 => Ok(Kind::Ftp { tls: false }),
        1 => Ok(Kind::Sftp),
        3 => Err("FTPS with implicit TLS".into()),
        2 => Err("HTTP".into()),
        5 => Err("HTTPS".into()),
        7 => Err("Amazon S3".into()),
        8 => Err("Storj".into()),
        other => Err(format!("FileZilla protocol {other}")),
    }
}

fn parse_filezilla(xml: &str) -> Result<Vec<ImportEntry>, ImportError> {
    let root = parse_tree(xml)?;
    let mut entries = Vec::new();
    if let Some(servers) = root.child("Servers") {
        collect_filezilla(servers, &mut Vec::new(), &mut entries);
    }
    Ok(entries)
}

fn collect_filezilla(parent: &Node, folders: &mut Vec<String>, out: &mut Vec<ImportEntry>) {
    for node in &parent.children {
        match node.name.as_str() {
            "Folder" => {
                folders.push(node.text.trim().to_string());
                collect_filezilla(node, folders, out);
                folders.pop();
            }
            "Server" => {
                let folder = (!folders.is_empty()).then(|| folders.join(" / "));
                let Some(host) = node.value("Host") else { continue };
                let name = node.value("Name");
                let protocol = node
                    .value("Protocol")
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(0);
                out.push(match filezilla_kind(protocol) {
                    Ok(kind) => {
                        // Logontype 0 is anonymous: no user to sign in as.
                        let anonymous = node.value("Logontype") == Some("0");
                        entry(
                            Source {
                                name,
                                host,
                                port: node.value("Port").and_then(|text| text.parse().ok()),
                                user: node.value("User").filter(|_| !anonymous),
                                key_file: node.value("Keyfile"),
                                root: None,
                            },
                            kind,
                            folder,
                        )
                    }
                    Err(label) => unsupported(name, host, label, folder),
                });
            }
            _ => {}
        }
    }
}

/// A plist `<dict>` as key to string value; other value types are skipped.
fn plist_dict(dict: &Node) -> HashMap<&str, &str> {
    dict.children
        .chunks(2)
        .filter_map(|pair| match pair {
            [key, value] if key.name == "key" => Some((key.text.trim(), value.text.trim())),
            _ => None,
        })
        .collect()
}

fn parse_cyberduck(xml: &str) -> Result<Vec<ImportEntry>, ImportError> {
    let root = parse_tree(xml)?;
    let dict = root.child("dict").ok_or(ImportError::Unrecognized)?;
    let values = plist_dict(dict);
    let get = |key: &str| values.get(key).copied().filter(|value| !value.is_empty());
    let Some(host) = get("Hostname") else {
        return Err(ImportError::Unrecognized);
    };
    let name = get("Nickname");
    let port = get("Port").and_then(|text| text.parse::<u16>().ok());
    let kind = match get("Protocol").unwrap_or("") {
        "ftp" => Ok(Kind::Ftp { tls: false }),
        // Port 990 is implicit TLS; OpenDAL's FTP service only speaks AUTH TLS.
        "ftps" if port == Some(990) => Err("FTPS with implicit TLS".to_string()),
        "ftps" => Ok(Kind::Ftp { tls: true }),
        "sftp" => Ok(Kind::Sftp),
        "dav" => Ok(Kind::WebDav { tls: false }),
        "davs" => Ok(Kind::WebDav { tls: true }),
        "s3" => Err("Amazon S3".to_string()),
        "googlestorage" => Err("Google Cloud Storage".to_string()),
        "azure" => Err("Azure Blob Storage".to_string()),
        "dropbox" => Err("Dropbox".to_string()),
        "onedrive" => Err("OneDrive".to_string()),
        "googledrive" => Err("Google Drive".to_string()),
        "" => Err("unknown protocol".to_string()),
        other => Err(format!("{other} protocol")),
    };
    Ok(vec![match kind {
        Ok(kind) => entry(
            Source {
                name,
                host,
                port,
                user: get("Username"),
                key_file: get("Private Key File"),
                root: get("Path"),
            },
            kind,
            None,
        ),
        Err(label) => unsupported(name, host, label, None),
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILEZILLA: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<FileZilla3 version="3.66.0" platform="*nix">
  <Servers>
    <Server>
      <Host>ftp.example.com</Host><Port>21</Port><Protocol>0</Protocol>
      <Logontype>0</Logontype><User>anonymous</User><Name>Public &amp; open</Name>
    </Server>
    <Folder expanded="1">Work
      <Server>
        <Host>files.example.com</Host><Port>2121</Port><Protocol>6</Protocol>
        <Logontype>1</Logontype><User>anna</User><Pass encoding="base64">c2VjcmV0</Pass>
        <Name>Plain</Name>
      </Server>
      <Folder>Prod
        <Server>
          <Host>ssh.example.com</Host><Port>22</Port><Protocol>1</Protocol>
          <Logontype>5</Logontype><User>deploy</User><Keyfile>/home/a/.ssh/id_ed25519</Keyfile>
          <Name>Deploy</Name>
        </Server>
        <Server>
          <Host>secure.example.com</Host><Protocol>3</Protocol><Name>Implicit</Name>
        </Server>
      </Folder>
    </Folder>
  </Servers>
</FileZilla3>"#;

    #[test]
    fn filezilla_keeps_folders_and_never_reads_passwords() {
        let entries = parse(FILEZILLA).unwrap();
        assert_eq!(entries.len(), 4);

        let anon = &entries[0];
        assert_eq!(anon.name, "Public & open");
        assert_eq!(anon.folder, None);
        let target = anon.target.as_ref().unwrap();
        assert_eq!(target.options["endpoint"], "ftps://ftp.example.com");
        assert!(!target.options.contains_key("user"));
        assert!(!target.needs_credentials);

        let plain = entries[1].target.as_ref().unwrap();
        assert_eq!(entries[1].folder.as_deref(), Some("Work"));
        assert_eq!(plain.options["endpoint"], "ftp://files.example.com:2121");
        assert_eq!(plain.options["user"], "anna");
        assert!(plain.needs_credentials);
        assert!(!format!("{plain:?}").contains("c2VjcmV0"));

        let sftp = entries[2].target.as_ref().unwrap();
        assert_eq!(entries[2].folder.as_deref(), Some("Work / Prod"));
        assert_eq!(sftp.scheme, "sftp");
        assert_eq!(sftp.options["key"], "/home/a/.ssh/id_ed25519");

        assert_eq!(entries[3].target, Err("FTPS with implicit TLS".to_string()));
    }

    #[test]
    fn cyberduck_bookmark_maps_protocol_and_path() {
        let duck = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Protocol</key><string>davs</string>
  <key>Nickname</key><string>Cloud</string>
  <key>Hostname</key><string>cloud.example.com</string>
  <key>Port</key><string>443</string>
  <key>Username</key><string>me</string>
  <key>Path</key><string>/remote.php/dav</string>
  <key>Labels</key><array><string>x</string></array>
</dict></plist>"#;
        let entries = parse(duck).unwrap();
        assert_eq!(entries.len(), 1);
        let target = entries[0].target.as_ref().unwrap();
        assert_eq!(entries[0].name, "Cloud");
        assert_eq!(target.scheme, "webdav");
        assert_eq!(target.options["endpoint"], "https://cloud.example.com");
        assert_eq!(target.options["root"], "/remote.php/dav");
        assert!(target.needs_credentials);

        let s3 = duck.replace("davs", "s3");
        assert_eq!(parse(&s3).unwrap()[0].target, Err("Amazon S3".to_string()));
    }

    #[test]
    fn rejects_other_files() {
        assert!(matches!(parse("{\"a\": 1}"), Err(ImportError::Unrecognized)));
    }
}
