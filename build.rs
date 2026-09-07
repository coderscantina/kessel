//! Generates the embedded-icon table for `src/assets.rs` from everything
//! under `assets/icons/` (recursively, SVGs only). `assets/icons/app/` holds
//! the bundle artwork (.icns etc.) and is deliberately skipped.
//!
//! On Windows it also compiles the resource section — app icon and version
//! info — into the executable. macOS carries the equivalent in the `.app`
//! bundle's Info.plist instead (see scripts/bundle.sh).

use std::fmt::Write as _;
use std::path::Path;

fn collect(dir: &Path, root: &Path, out: &mut String) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .expect("read assets/icons")
        .map(|e| e.expect("read dir entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "app") {
                continue;
            }
            collect(&path, root, out);
        } else if path.extension().is_some_and(|ext| ext == "svg") {
            let rel = path.strip_prefix(root).unwrap().display();
            writeln!(
                out,
                r#"    ("icons/{rel}", include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/icons/{rel}")) as &[u8]),"#
            )
            .unwrap();
        }
    }
}

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let icons = Path::new(&manifest).join("assets/icons");
    let mut table = String::from("pub const EMBEDDED_ICONS: &[(&str, &[u8])] = &[\n");
    collect(&icons, &icons, &mut table);
    table.push_str("];\n");

    let out = Path::new(&std::env::var("OUT_DIR").unwrap()).join("embedded_icons.rs");
    std::fs::write(out, table).expect("write embedded_icons.rs");
    println!("cargo::rerun-if-changed=assets/icons");

    windows_resources();
}

/// Compile the Windows resource section. The dev profile gets the Coaxium
/// icon, so a debug build is distinguishable in the taskbar — the same split
/// scripts/bundle.sh makes for the ICNS.
#[cfg(windows)]
fn windows_resources() {
    let icon = if std::env::var("PROFILE").as_deref() == Ok("debug") {
        "assets/icons/app/kessel-dev.ico"
    } else {
        "assets/icons/app/kessel.ico"
    };
    println!("cargo::rerun-if-changed={icon}");

    let mut res = winresource::WindowsResource::new();
    res.set_icon(icon)
        .set("ProductName", "Kessel")
        .set("FileDescription", env!("CARGO_PKG_DESCRIPTION"))
        .set("CompanyName", "Coders Cantina")
        .set("LegalCopyright", "© Coders Cantina — coderscantina.com");
    if let Err(error) = res.compile() {
        // A missing resource compiler shouldn't stop a local build; the
        // executable just ends up without an icon.
        println!("cargo::warning=windows resources not compiled: {error}");
    }
}

#[cfg(not(windows))]
fn windows_resources() {}
