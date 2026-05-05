#!/bin/bash -e

export PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
export INTERNAL_REPO=artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/internal-development-docker-drivers-local
export DOCKER_REGISTRY_NAME=snowflakedb

if [[ -z "$GITHUB_ACTIONS" ]]; then
    export WORKSPACE=${WORKSPACE:-/tmp}
else
    export WORKSPACE=$GITHUB_WORKSPACE
fi

export DRIVER_NAME=dotnet

# Build images
BUILD_IMAGE_VERSION=1

# Test Images
TEST_IMAGE_VERSION=1

declare -A BUILD_IMAGE_NAMES=(
    [$DRIVER_NAME-ubuntu264-net10]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-ubuntu264-net10-build:$BUILD_IMAGE_VERSION
)
export BUILD_IMAGE_NAMES

declare -A TEST_IMAGE_NAMES=(
    [$DRIVER_NAME-ubuntu264-net10]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-ubuntu264-net10-test:$TEST_IMAGE_VERSION
)
export TEST_IMAGE_NAMES
