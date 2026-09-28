#!/bin/bash -e
#
# Builds the WIF e2e test artifacts on the Jenkins node, inside the
# build image (which carries the .NET SDK). The resulting publish output
# is staged as ci/wif/artifacts/wif_tests.tar.gz so the outer
# ci/test_wif.sh can scp it to the bare WIF cloud VMs and run the tests
# there. This avoids the VMs needing to pull source from GitHub.
#
set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$THIS_DIR/.." && pwd )"
ARTIFACT_DIR="$THIS_DIR/wif/artifacts"
source "$THIS_DIR/_init.sh"

cd "$REPO_ROOT"
rm -rf "$ARTIFACT_DIR"
mkdir -p "$ARTIFACT_DIR"

SRC_COPY="$(mktemp -d /tmp/wif-src.XXXXXX)"
trap 'rm -rf "$SRC_COPY"' EXIT
git -C "$REPO_ROOT" archive --format=tar HEAD | tar -C "$SRC_COPY" -xf -

BUILD_IMAGE="${BUILD_IMAGE_NAMES[dotnet-ubuntu264-net10]}"
echo "Using build image: $BUILD_IMAGE"
docker pull "$BUILD_IMAGE"

echo "Publishing Snowflake.Data.Tests for net9.0 from isolated HEAD copy $SRC_COPY..."
docker run \
    --rm \
    --platform linux/amd64 \
    -v "$SRC_COPY":/mnt/host \
    -v "$ARTIFACT_DIR":/mnt/artifacts \
    -e LOCAL_USER_ID=$(id -u $USER) \
    "$BUILD_IMAGE" \
    bash -c "cd /mnt/host && dotnet publish Snowflake.Data.Tests -f net9.0 -c Debug --no-self-contained -o /mnt/artifacts/publish"

echo "Creating wif_tests.tar.gz..."
tar czf "$ARTIFACT_DIR/wif_tests.tar.gz" -C "$ARTIFACT_DIR/publish" .
rm -rf "$ARTIFACT_DIR/publish"

echo "WIF artifacts staged in $ARTIFACT_DIR:"
ls -la "$ARTIFACT_DIR"
