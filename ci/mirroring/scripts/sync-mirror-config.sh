#!/usr/bin/env bash
# sync-mirror-config.sh — Regenerate Copybara mirror infrastructure from the
# canonical generator in universal-driver.
#
# Copy this file into your repo as ci/mirroring/sync-mirror-config.sh,
# customize the variables below, and run it from the repo root to (re)generate
# all mirroring files.
#
# Usage:
#   cd /path/to/your-repo
#   bash ci/mirroring/sync-mirror-config.sh
#
# The generated files should be committed to the repo. Re-run this script
# whenever the upstream generator is updated (it always fetches the latest
# version from universal-driver main).
#
# The script stores the SHA-256 of the generator it last ran. On subsequent
# runs, if the upstream generator hasn't changed, generation is skipped.
# Pass --force to regenerate regardless.

set -euo pipefail

##############################################################################
# CUSTOMIZE THESE FOR YOUR REPO
##############################################################################

# Internal repository name (snowflake-eng/<name>).
export REPO_NAME="snowflake-connector-net"

# Mirror repository name (snowflakedb/<name>). Leave empty if same as REPO_NAME.
export MIRROR_REPO_NAME=""

# Name of the primary branch ("main" or "master"). Defaults to "main".
export MAIN_BRANCH="master"

# Additional paths to exclude from the mirror (comma-separated).
# The default set (NOMIRROR/, _internal/, .ai/, .cursor/, .claude/, ci/mirroring/,
# mirror workflows, CODEOWNERS) is always included.
# Leave empty if no extra exclusions are needed.
export EXTRA_EXCLUDED_PATHS=""

# Set to "true" if this repo IS the source-of-truth for the generator
# (i.e., it owns ci/mirroring/scripts/generate_mirror_config.py). The staleness check
# in mirror.yml will be disabled since the repo can't be stale against itself.
export IS_SOURCE_OF_TRUTH_REPO="false"

# Name of the GitHub secret for snowflake-eng (internal) access.
# Defaults to "DRIVER_MIRROR_TOKEN". Change if your repo uses a different secret name.
export INTERNAL_TOKEN_NAME="SNOWFLAKE_EMU_TOKEN"

# Slack channel ID for mirror failure notifications.
# Leave empty to omit the notify-on-failure job from mirror.yml.
export SLACK_CHANNEL_ID="C092X1UAAMB"

# Slack subteam ID for on-call mention in failure alerts.
export SLACK_ONCALL_SUBTEAM_ID="S077RA1UXAS"

##############################################################################
# DO NOT EDIT BELOW THIS LINE
##############################################################################

GENERATOR_REPO="git@github.com:snowflake-eng/drivers.git"
GENERATOR_REF="main"
GENERATOR_DIR="ci/mirroring/scripts"
GENERATOR_NAME="generate_mirror_config.py"
HASH_FILE="ci/mirroring/.generator-hash"
FORCE=false

if [[ "${1:-}" == "--force" ]]; then
    FORCE=true
fi

echo "Fetching mirror config generator from snowflake-eng/drivers (${GENERATOR_REF})..."
TMPDIR="$(mktemp -d)"
trap 'rm -rf "${TMPDIR}"' EXIT
git clone --depth 1 --filter=blob:none --sparse --branch "${GENERATOR_REF}" \
    "${GENERATOR_REPO}" "${TMPDIR}/drivers" --quiet
git -C "${TMPDIR}/drivers" sparse-checkout set "${GENERATOR_DIR}"
if [[ ! -f "${TMPDIR}/drivers/${GENERATOR_DIR}/${GENERATOR_NAME}" ]]; then
    echo "ERROR: ${GENERATOR_DIR}/${GENERATOR_NAME} not found in ${GENERATOR_REPO} (branch: ${GENERATOR_REF})" >&2
    exit 1
fi

# Hash the entire ci/mirroring/scripts/ directory (generator + templates).
# Changes to any file in the directory trigger regeneration.
CURRENT_HASH="$(find "${TMPDIR}/drivers/${GENERATOR_DIR}" -type f | sort | xargs cat | shasum -a 256 | cut -d' ' -f1)"

if [[ "${FORCE}" == "false" ]] && [[ -f "${HASH_FILE}" ]]; then
    STORED_HASH="$(cat "${HASH_FILE}")"
    if [[ "${CURRENT_HASH}" == "${STORED_HASH}" ]]; then
        echo "Generator unchanged (SHA-256: ${CURRENT_HASH:0:12}...). Skipping generation."
        echo "Run with --force to regenerate anyway."
        exit 0
    fi
fi

echo "Running generator for ${REPO_NAME}..."
python3 "${TMPDIR}/drivers/${GENERATOR_DIR}/${GENERATOR_NAME}"

# Store the hash for next run.
mkdir -p "$(dirname "${HASH_FILE}")"
printf '%s\n' "${CURRENT_HASH}" > "${HASH_FILE}"
echo "Stored generator hash: ${CURRENT_HASH:0:12}..."
