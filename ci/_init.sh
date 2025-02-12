#!/bin/bash -e

export PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
export INTERNAL_REPO=nexus.int.snowflakecomputing.com:8086
if [[ -z "$GITHUB_ACTIONS" ]]; then
    # Use the internal Docker Registry
    export DOCKER_REGISTRY_NAME=$INTERNAL_REPO/docker
    export WORKSPACE=${WORKSPACE:-/tmp}
else
    # Use Docker Hub
    export DOCKER_REGISTRY_NAME=snowflakedb
    export WORKSPACE=$GITHUB_WORKSPACE
fi

export DRIVER_NAME=dotnet

# Build images
BUILD_IMAGE_VERSION=2

# Test Images
TEST_IMAGE_VERSION=2

declare -A BUILD_IMAGE_NAMES=(
    [$DRIVER_NAME-ubuntu204-net9]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-ubuntu204-net9-build:$BUILD_IMAGE_VERSION
)
export BUILD_IMAGE_NAMES

declare -A TEST_IMAGE_NAMES=(
    [$DRIVER_NAME-ubuntu204-net9]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-ubuntu204-net9-test:$TEST_IMAGE_VERSION
)
export TEST_IMAGE_NAMES
