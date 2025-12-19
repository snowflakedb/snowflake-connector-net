#!/bin/bash -e

# Test Snowflake .NET Connector in Rocky Linux 9 Docker

# NOTES:
#   - By default this script runs .NET 8.0 tests, as these are commonly used
#   - To test specific .NET version(s) pass in versions like: `./test_rockylinux9_docker.sh "net8.0 net9.0"`

set -o pipefail

# In case this is ran from dev-vm
DOTNET_TARGET_FRAMEWORKS=${1:-"net8.0"}

# Set constants
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${CONNECTOR_DIR}}

# Set base image directly
BASE_IMAGE=${BASE_IMAGE_ROCKYLINUX9:-rockylinux:9}
echo "[Info] Using base image: $BASE_IMAGE"

CONTAINER_NAME=test_dotnetconnector_rockylinux9

echo "[Info] Building Rocky Linux 9 Docker image with .NET SDK"
GOSU_URL=https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64

# Build from repo root using the RHEL9 Dockerfile in ci/image/
cd $CONNECTOR_DIR
docker build --pull -t ${CONTAINER_NAME}:1.0 \
    --build-arg BASE_IMAGE=$BASE_IMAGE \
    --build-arg GOSU_URL="$GOSU_URL" \
    --build-arg DOTNET_TARGET_FRAMEWORKS="${DOTNET_TARGET_FRAMEWORKS}" \
    . -f ci/image/Dockerfile.dotnet-rhel9-build

user_id=$(id -u ${USER})
docker run --network=host \
    -e TERM=vt102 \
    -e LOCAL_USER_ID=${user_id} \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e SF_REGRESS_LOGS \
    -e SF_PROJECT_ROOT \
    -e cloud_provider \
    -e JENKINS_HOME \
    -e is_old_driver \
    -e GITHUB_ACTIONS \
    -e DOTNET_TARGET_FRAMEWORKS \
    --mount type=bind,source="${CONNECTOR_DIR}",target=/home/user/snowflake-connector-net \
    ${CONTAINER_NAME}:1.0 \
    /home/user/snowflake-connector-net/ci/test_rockylinux9.sh ${DOTNET_TARGET_FRAMEWORKS}
 
