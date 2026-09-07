#!/usr/bin/env bash
# Regenerate the app icons from the SVG sources in assets/icons/app/.
#
#   Kessel-Coaxium.svg -> kessel-dev.icns / kessel-dev.ico  (dev builds)
#   Kessel-Dark.svg    -> kessel.icns     / kessel.ico      (release builds)
#
# The SVGs carry a Big Sur-style rounded-rect background. For the ICNS the
# corner radius is stripped so the artwork is full-bleed and macOS Tahoe can
# apply its own squircle mask (pre-rounded icons get the shrunk-on-a-plate
# fallback treatment). Windows applies no mask, so the ICO keeps the rounded
# rect exactly as drawn.
#
# The ICO is assembled by hand rather than with ImageMagick: the modern format
# is just an index over PNG blobs, so rsvg-convert plus a few lines of struct
# packing is the whole job — and one fewer tool to install.
#
# Requires: rsvg-convert (brew install librsvg), python3; iconutil (macOS) for
# the ICNS half — pass --ico-only to skip it elsewhere.
set -euo pipefail
cd "$(dirname "$0")/.."

ICO_ONLY=0
[[ "${1:-}" == "--ico-only" ]] && ICO_ONLY=1

APP=assets/icons/app
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ICO sizes: 16/32/48 are the shell's list and taskbar sizes, 256 is what the
# large-icon views and the installer use; the rest fill in the scaling steps.
ICO_SIZES=(16 24 32 48 64 128 256)

build_icns() { # $1 = source svg, $2 = output basename
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

build_ico() { # $1 = source svg, $2 = output basename
  local src="$APP/$1" dir="$TMP/$2.ico.d"
  mkdir -p "$dir"
  local pngs=()
  for s in "${ICO_SIZES[@]}"; do
    rsvg-convert -w "$s" -h "$s" "$src" -o "$dir/$s.png"
    pngs+=("$dir/$s.png")
  done
  python3 scripts/make-ico.py "$APP/$2.ico" "${pngs[@]}"
  echo "generated: $APP/$2.ico"
}

for pair in "Kessel-Coaxium.svg kessel-dev" "Kessel-Dark.svg kessel"; do
  # shellcheck disable=SC2086
  set -- $pair
  [[ $ICO_ONLY -eq 1 ]] || build_icns "$1" "$2"
  build_ico "$1" "$2"
done
