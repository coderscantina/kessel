#!/usr/bin/env bash
# Assemble Kessel.app. Usage: scripts/bundle.sh [dev|release]  (default: release)
#
# dev     -> debug build,   Coaxium icon (kessel-dev.icns), id com.coderscantina.kessel.dev
# release -> release build, Dark icon    (kessel.icns),     id com.coderscantina.kessel
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-release}"
VERSION="$(cargo pkgid | sed 's/.*#//')"

case "$MODE" in
  dev)
    cargo build
    BIN=target/debug/kessel
    ICNS=assets/icons/app/kessel-dev.icns
    BUNDLE_ID=com.coderscantina.kessel.dev
    APP_NAME="Kessel Dev"
    ;;
  release)
    # KESSEL_BIN overrides the binary (e.g. a lipo'd universal binary from CI)
    if [[ -z "${KESSEL_BIN:-}" ]]; then
      cargo build --release
    fi
    BIN="${KESSEL_BIN:-target/release/kessel}"
    ICNS=assets/icons/app/kessel.icns
    BUNDLE_ID=com.coderscantina.kessel
    APP_NAME="Kessel"
    ;;
  *)
    echo "usage: $0 [dev|release]" >&2
    exit 1
    ;;
esac

APP="target/bundle/$MODE/$APP_NAME.app"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"

cp "$BIN" "$APP/Contents/MacOS/kessel"
cp "$ICNS" "$APP/Contents/Resources/kessel.icns"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>            <string>$APP_NAME</string>
    <key>CFBundleDisplayName</key>     <string>$APP_NAME</string>
    <key>CFBundleIdentifier</key>      <string>$BUNDLE_ID</string>
    <key>CFBundleExecutable</key>      <string>kessel</string>
    <key>CFBundleIconFile</key>        <string>kessel</string>
    <key>CFBundleVersion</key>         <string>$VERSION</string>
    <key>CFBundleShortVersionString</key> <string>$VERSION</string>
    <key>CFBundlePackageType</key>     <string>APPL</string>
    <key>CFBundleInfoDictionaryVersion</key> <string>6.0</string>
    <key>NSHumanReadableCopyright</key> <string>© Coders Cantina — coderscantina.com</string>
    <key>LSMinimumSystemVersion</key>  <string>12.0</string>
    <key>NSHighResolutionCapable</key> <true/>
    <key>NSSupportsAutomaticGraphicsSwitching</key> <true/>
</dict>
</plist>
PLIST

# Ad-hoc sign so macOS (incl. Tahoe's stricter Gatekeeper) will launch it locally
codesign --force --deep --sign - "$APP"

echo "bundled: $APP"
