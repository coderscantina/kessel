#!/usr/bin/env bash
# Regenerate the app ICNS files from the SVG sources in assets/icons/app/.
#
#   Kessel-Coaxium.svg -> kessel-dev.icns (dev builds)
#   Kessel-Dark.svg    -> kessel.icns     (release builds)
#
# The SVGs carry a Big Sur-style rounded-rect background; for the ICNS the
# corner radius is stripped so the artwork is full-bleed and macOS Tahoe can
# apply its own squircle mask (pre-rounded icons get the shrunk-on-a-plate
# fallback treatment).
#
# Requires: rsvg-convert (brew install librsvg), iconutil (macOS).
set -euo pipefail
cd "$(dirname "$0")/.."

APP=assets/icons/app
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

build() { # $1 = source svg, $2 = output icns basename
  local src="$APP/$1" fullbleed="$TMP/$2.svg" iconset="$TMP/$2.iconset"
  sed 's/rx="[^"]*" ry="[^"]*"//' "$src" > "$fullbleed"
  mkdir -p "$iconset"
  for s in 16 32 128 256 512; do
    rsvg-convert -w "$s" -h "$s" "$fullbleed" -o "$iconset/icon_${s}x${s}.png"
    rsvg-convert -w $((s * 2)) -h $((s * 2)) "$fullbleed" -o "$iconset/icon_${s}x${s}@2x.png"
  done
  iconutil -c icns "$iconset" -o "$APP/$2.icns"
  echo "generated: $APP/$2.icns"
}

build Kessel-Coaxium.svg kessel-dev
build Kessel-Dark.svg kessel
