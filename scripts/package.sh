#!/usr/bin/env bash
# Sign, notarize and package Kessel.app into a distributable DMG.
#
# Usage: scripts/package.sh [--skip-notarize]
#
# Expects scripts/bundle.sh release to have produced target/bundle/release/Kessel.app
# (this script runs it if the bundle is missing).
#
# Environment:
#   KESSEL_SIGN_IDENTITY   "Developer ID Application: ..." — required for real signing;
#                          falls back to ad-hoc ("-") with a warning (not notarizable).
#   KESSEL_NOTARY_PROFILE  notarytool keychain profile name (from
#                          `xcrun notarytool store-credentials`), OR:
#   APPLE_ID / APPLE_TEAM_ID / APPLE_APP_PASSWORD   direct notarytool credentials.
#
# Outputs:
#   target/dist/Kessel-<version>.dmg          signed, notarized, stapled
#   target/dist/Kessel-<version>.dmg.sha256   checksum (used by the update manifest)
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_NOTARIZE=0
[[ "${1:-}" == "--skip-notarize" ]] && SKIP_NOTARIZE=1

VERSION="$(cargo pkgid | sed 's/.*#//')"
APP="target/bundle/release/Kessel.app"
DIST="target/dist"
DMG="$DIST/Kessel-${VERSION}.dmg"
IDENTITY="${KESSEL_SIGN_IDENTITY:--}"

[[ -d "$APP" ]] || scripts/bundle.sh release

if [[ "$IDENTITY" == "-" ]]; then
    if [[ -n "${KESSEL_REQUIRE_SIGNING:-}" ]]; then
        echo "ERROR: release builds must be signed, but KESSEL_SIGN_IDENTITY is not set." >&2
        exit 1
    fi
    echo "WARNING: KESSEL_SIGN_IDENTITY not set — ad-hoc signing (cannot be notarized)." >&2
    SKIP_NOTARIZE=1
fi

# ─── Sign (hardened runtime + entitlements) ───────────────────────────────────
echo "Signing with identity: $IDENTITY"
codesign --force --deep --options runtime \
    --entitlements scripts/entitlements.plist \
    --sign "$IDENTITY" --timestamp "$APP"
codesign --verify --strict "$APP"

# ─── DMG ──────────────────────────────────────────────────────────────────────
mkdir -p "$DIST"
rm -f "$DMG"

STAGING=$(mktemp -d)
trap 'rm -rf "$STAGING"' EXIT
cp -R "$APP" "$STAGING/"
ln -s /Applications "$STAGING/Applications"

hdiutil create -volname "Kessel" -srcfolder "$STAGING" -ov -format UDZO "$DMG"

if [[ "$IDENTITY" != "-" ]]; then
    codesign --force --sign "$IDENTITY" --timestamp "$DMG"
fi

# ─── Notarize + staple ────────────────────────────────────────────────────────
if [[ $SKIP_NOTARIZE -eq 0 ]]; then
    echo "Submitting to Apple notary service..."
    if [[ -n "${KESSEL_NOTARY_PROFILE:-}" ]]; then
        xcrun notarytool submit "$DMG" --keychain-profile "$KESSEL_NOTARY_PROFILE" --wait
    else
        : "${APPLE_ID:?APPLE_ID required when KESSEL_NOTARY_PROFILE is unset}"
        : "${APPLE_TEAM_ID:?APPLE_TEAM_ID required}"
        : "${APPLE_APP_PASSWORD:?APPLE_APP_PASSWORD required}"
        xcrun notarytool submit "$DMG" \
            --apple-id "$APPLE_ID" --team-id "$APPLE_TEAM_ID" \
            --password "$APPLE_APP_PASSWORD" --wait
    fi
    xcrun stapler staple "$DMG"
    xcrun stapler validate "$DMG"
else
    echo "Skipping notarization."
fi

shasum -a 256 "$DMG" | awk '{print $1}' > "${DMG}.sha256"
echo "packaged: $DMG ($(cat "${DMG}.sha256"))"
