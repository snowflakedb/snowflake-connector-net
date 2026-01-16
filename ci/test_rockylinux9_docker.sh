#!/bin/bash -e
#
# Test Snowflake .NET Connector in Rocky Linux 9 Docker
#
# This script builds a Rocky Linux 9 Docker image with all required .NET SDKs
# and runs the test script inside the container. It is called by GitHub Actions.
#
# Required environment variables (set by GHA workflow):
#   - net_version: Target framework (e.g., "net8.0")
#   - snowflake_cloud_env: Cloud environment (e.g., "AWS", "AZURE", "GCP")
#   - PARAMETER_SECRET: GPG passphrase (passed to container for decryption)
#   - DOTNET_COVERAGE_VERSION: dotnet-coverage tool version (e.g., "17.8.4")
#

if [[ -z "$DOTNET_COVERAGE_VERSION" ]]; then
    echo "[ERROR] DOTNET_COVERAGE_VERSION environment variable is required"
    exit 1
fi

# Set constants
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${CONNECTOR_DIR}}
BASE_IMAGE="rockylinux:9"
CONTAINER_NAME="test_dotnetconnector_rockylinux9"

echo "[INFO] Building Rocky Linux 9 Docker image with .NET SDKs"

# Get current user/group IDs to match host permissions
USER_ID=$(id -u)
GROUP_ID=$(id -g)

pushd $CONNECTOR_DIR
docker build --pull -t ${CONTAINER_NAME}:1.0 \
    --build-arg USER_ID=$USER_ID \
    --build-arg GROUP_ID=$GROUP_ID \
    --build-arg DOTNET_COVERAGE_VERSION="${DOTNET_COVERAGE_VERSION}" \
    . -f ci/image/Dockerfile.dotnet-rhel9-build-test
popd

echo "[INFO] Starting Rocky Linux 9 Docker container"
docker run --network=host \
    -e CI \
    -e PARAMETER_SECRET \
    -e net_version \
    -e snowflake_cloud_env \
    --mount type=bind,source="${CONNECTOR_DIR}",target=/home/user/snowflake-connector-net \
    ${CONTAINER_NAME}:1.0 \
    /home/user/snowflake-connector-net/ci/test_rockylinux9.sh
