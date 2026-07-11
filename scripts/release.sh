#!/usr/bin/env bash
# Cut a CalVer release: vYY.M.D (e.g. v26.7.10).
#
# Flow (deliberate ordering):
#   1. Bump Cargo.toml to the CalVer version and create the release commit FIRST,
#      so the commit set for the changelog is final.
#   2. Generate the CHANGELOG.md entry from <prev tag>..HEAD — every listed short
#      hash is a commit that already exists and will not change.
#   3. Amend the release commit to include CHANGELOG.md, then tag and push.
#      (The release commit itself is excluded from its own changelog entry.)
#
# Usage: scripts/release.sh [--dry-run] [--backfill]
set -euo pipefail
cd "$(dirname "$0")/.."

CHANGELOG_FILE="CHANGELOG.md"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

# ─── Helpers ──────────────────────────────────────────────────────────────────

# Build the changelog block for a given tag range.
# Usage: changelog_block <new_tag> <previous_tag|""> <upper_ref>
changelog_block() {
    local tag="$1"
    local prev_tag="$2"
    local upper="${3:-HEAD}"
    local date_str

    # Derive date from tag (vYY.M.D → 20YY-M-D) or fall back to today
    if [[ "$tag" =~ ^v([0-9]{2})\.([0-9]+)\.([0-9]+)$ ]]; then
        date_str="20${BASH_REMATCH[1]}-${BASH_REMATCH[2]}-${BASH_REMATCH[3]}"
    else
        date_str=$(date +"%Y-%m-%d")
    fi

    local commits
    if [[ -z "$prev_tag" ]]; then
        commits=$(git log "$upper" --pretty=format:"- %h %s" --no-merges 2>/dev/null || true)
    else
        commits=$(git log "${prev_tag}..${upper}" --pretty=format:"- %h %s" --no-merges 2>/dev/null || true)
    fi

    # Drop release commits themselves from the listing
    commits=$(echo "$commits" | grep -v "🔖 Release" || true)
    [[ -z "$commits" ]] && commits="- No changes recorded"

    echo "## [$tag] — $date_str"
    echo ""
    echo "$commits"
    echo ""
}

# Prepend a block of text to CHANGELOG.md, keeping the header intact.
prepend_to_changelog() {
    local block="$1"
    local tmp
    tmp=$(mktemp)

    if [[ ! -f "$CHANGELOG_FILE" ]]; then
        {
            echo "# Changelog"
            echo ""
            echo "All notable changes to kessel are documented here."
            echo "Versions follow [CalVer](https://calver.org/) (\`vYY.M.D\`); commits follow the [Gitmoji](https://gitmoji.dev/) convention."
            echo ""
            echo "$block"
        } > "$CHANGELOG_FILE"
    else
        local first_section
        first_section=$(grep -n "^## " "$CHANGELOG_FILE" | head -1 | cut -d: -f1)

        if [[ -n "$first_section" ]]; then
            head -n $(( first_section - 1 )) "$CHANGELOG_FILE" > "$tmp"
            echo "$block" >> "$tmp"
            tail -n "+${first_section}" "$CHANGELOG_FILE" >> "$tmp"
        else
            cat "$CHANGELOG_FILE" > "$tmp"
            echo "" >> "$tmp"
            echo "$block" >> "$tmp"
        fi

        mv "$tmp" "$CHANGELOG_FILE"
    fi
}

# Set version = X.Y.Z in Cargo.toml's [package] section (first `version =` line).
set_cargo_version() {
    local version="$1"
    sed -i '' "0,/^version = \".*\"/s//version = \"${version}\"/" Cargo.toml
    # Refresh Cargo.lock's own entry without touching dependency versions
    cargo update --package kessel --precise "$version" 2>/dev/null \
        || cargo metadata --format-version 1 --no-deps > /dev/null
}

# ─── Backfill mode ────────────────────────────────────────────────────────────

if [[ "${1:-}" == "--backfill" ]]; then
    echo "Backfilling $CHANGELOG_FILE from all existing tags..."

    ALL_TAGS=()
    while IFS= read -r tag; do
        ALL_TAGS+=("$tag")
    done < <(git tag --sort=creatordate)

    if [[ ${#ALL_TAGS[@]} -eq 0 ]]; then
        echo "No tags found."
        exit 0
    fi

    {
        echo "# Changelog"
        echo ""
        echo "All notable changes to kessel are documented here."
        echo "Versions follow [CalVer](https://calver.org/) (\`vYY.M.D\`); commits follow the [Gitmoji](https://gitmoji.dev/) convention."
        echo ""
    } > "$CHANGELOG_FILE"

    for (( i=${#ALL_TAGS[@]}-1; i>=0; i-- )); do
        tag="${ALL_TAGS[$i]}"
        prev_tag=""
        (( i > 0 )) && prev_tag="${ALL_TAGS[$i-1]}"
        echo "  → $tag"
        changelog_block "$tag" "$prev_tag" "$tag" >> "$CHANGELOG_FILE"
    done

    echo ""
    echo "Done. $CHANGELOG_FILE written with ${#ALL_TAGS[@]} releases."
    exit 0
fi

# ─── Normal release flow ──────────────────────────────────────────────────────

if [[ $(git rev-parse --abbrev-ref HEAD) != "main" ]]; then
    echo "Error: Not on main branch. Please checkout main before releasing." >&2
    exit 1
fi

if [[ $DRY_RUN -eq 0 && -n $(git status --porcelain) ]]; then
    echo "Error: Working tree is not clean. Commit or stash changes first." >&2
    exit 1
fi

# Sync with origin if a remote exists
if git remote get-url origin >/dev/null 2>&1; then
    git fetch origin main
    local_commit=$(git rev-parse HEAD)
    remote_commit=$(git rev-parse origin/main)
    base_commit=$(git merge-base HEAD origin/main)

    if [[ $local_commit != "$remote_commit" && $local_commit == "$base_commit" ]]; then
        echo "Error: Local main is behind origin/main. Please pull first." >&2
        exit 1
    fi
fi

new_version=$(date +"%y.%-m.%-d")
new_tag="v${new_version}"

if git rev-parse --verify "$new_tag" >/dev/null 2>&1; then
    echo "Error: Tag $new_tag already exists (one release per day with vYY.M.D)." >&2
    exit 1
fi

previous_tag=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

if [[ $DRY_RUN -eq 1 ]]; then
    echo "Would release $new_tag (previous: ${previous_tag:-none}):"
    echo
    changelog_block "$new_tag" "$previous_tag" "HEAD"
    exit 0
fi

# 1. Version bump + release commit first — fixes the commit set for the changelog
echo "Bumping Cargo.toml to ${new_version}..."
set_cargo_version "$new_version"
git add Cargo.toml Cargo.lock
git commit -m "🔖 Release ${new_tag}"

# 2. Generate the changelog entry — all listed hashes are now final
echo "Updating $CHANGELOG_FILE..."
block=$(changelog_block "$new_tag" "$previous_tag" "HEAD")
prepend_to_changelog "$block"

# 3. Fold the changelog into the release commit, then tag
git add "$CHANGELOG_FILE"
git commit --amend --no-edit
git tag "$new_tag"

if git remote get-url origin >/dev/null 2>&1; then
    git push origin main
    git push origin "$new_tag"
    echo "Created and pushed tag: $new_tag"
else
    echo "Created tag: $new_tag (no 'origin' remote — push manually when ready)"
fi
