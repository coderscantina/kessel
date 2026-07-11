#!/usr/bin/env bash
# Fetch every Lucide SVG that gpui-component's IconName can reference into
# assets/icons/. gpui-component ships no assets — the host app must provide
# all of them, or icons silently render blank.
#
# The name list mirrors gpui-component's src/icon.rs (icons/<name>.svg).
# Names that aren't real Lucide icons are mapped to the closest Lucide
# equivalent in MAP below.
#
# Usage: scripts/sync-icons.sh
set -euo pipefail

cd "$(dirname "$0")/.."
DEST=assets/icons
BASE=https://raw.githubusercontent.com/lucide-icons/lucide/main/icons

# gpui-component icon name → lucide icon name (only where they differ)
declare -A MAP=(
  [close]=x
  [code]=code-xml
  [dash]=minus
  [flask]=flask-conical
  [gamepad]=gamepad-2
  [home]=house
  [key]=key-round
  [inspector]=square-mouse-pointer
  [resize-corner]=move-diagonal-2
  [sort-ascending]=arrow-up-narrow-wide
  [sort-descending]=arrow-down-narrow-wide
  [window-close]=x
  [window-maximize]=square
  [window-minimize]=minus
  [window-restore]=copy
)

NAMES=(
  a-large-small arrow-down arrow-left arrow-right arrow-up asterisk bell
  book-open bot building-2 calendar case-sensitive chart-pie check
  chevron-down chevron-left chevron-right chevron-up chevrons-up-down
  circle-check circle-user circle-x close copy dash delete ellipsis
  ellipsis-vertical external-link eye eye-off file folder folder-closed
  folder-open frame gallery-vertical-end globe heart heart-off inbox
  info inspector layout-dashboard loader loader-circle map maximize menu
  minimize minus moon palette panel-bottom panel-bottom-open panel-left
  panel-left-close panel-left-open panel-right panel-right-close
  panel-right-open plus redo redo-2 replace resize-corner search settings
  settings-2 sort-ascending sort-descending square-terminal star star-off
  sun thumbs-down thumbs-up triangle-alert undo undo-2 user window-close
  window-maximize window-minimize window-restore
  # kessel's own toolbar/footer icons (not in IconName)
  folder-plus rotate-cw gauge list-checks
  # bookmark/space picker icons (see ui::BOOKMARK_ICONS)
  archive briefcase camera cloud code cpu database film flask gamepad
  hard-drive home image key lock music newspaper package rocket server
  shield wrench zap
)

# Brand icons were removed from lucide main; pin github to the last tag
# that shipped it.
curl -fsSL "https://raw.githubusercontent.com/lucide-icons/lucide/0.462.0/icons/github.svg" \
  -o "$DEST/github.svg" && echo "ok   github.svg  (lucide 0.462.0)"

failed=()
for name in "${NAMES[@]}"; do
  src=${MAP[$name]:-$name}
  if curl -fsSL "$BASE/$src.svg" -o "$DEST/$name.svg.tmp"; then
    mv "$DEST/$name.svg.tmp" "$DEST/$name.svg"
    echo "ok   $name.svg${MAP[$name]:+  (lucide: $src)}"
  else
    rm -f "$DEST/$name.svg.tmp"
    failed+=("$name (lucide: $src)")
  fi
done

if ((${#failed[@]})); then
  printf 'FAILED: %s\n' "${failed[@]}" >&2
  exit 1
fi
