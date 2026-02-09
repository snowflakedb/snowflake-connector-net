#!/bin/bash -e
#
# Build Dotnet driver
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/_init.sh

if [[ -z "$GITHUB_ACTIONS" ]]; then
    export GIT_URL=${GIT_URL:-https://github.com/snowflakedb/snowflake-connector-net.git}
    export GIT_BRANCH=${GIT_BRANCH:-origin/$(git rev-parse --abbrev-ref HEAD)}
    export GIT_COMMIT=${GIT_COMMIT:-$(git rev-parse HEAD)}
else
    export GIT_URL=https://github.com/${GITHUB_REPOSITORY}.git
    export GIT_BRANCH=origin/$(basename ${GITHUB_REF})
    export GIT_COMMIT=${GITHUB_SHA}
fi

echo "GIT_URL: $GIT_URL, GIT_BRANCH: $GIT_BRANCH, GIT_COMMIT: $GIT_COMMIT"

for name in "${!BUILD_IMAGE_NAMES[@]}"; do
    echo "[INFO] Building $DRIVER_NAME on $name"
    docker pull "${BUILD_IMAGE_NAMES[$name]}"
    docker run \
        -v $(cd $THIS_DIR/.. && pwd):/mnt/host \
        -v $WORKSPACE:/mnt/workspace \
        -e LOCAL_USER_ID=$(id -u $USER) \
        -e GIT_URL \
        -e GIT_BRANCH \
        -e GIT_COMMIT \
        -e AWS_ACCESS_KEY_ID \
        -e AWS_SECRET_ACCESS_KEY \
        -e GITHUB_ACTIONS \
        -e GITHUB_SHA \
        -e GITHUB_REF \
        -e GITHUB_EVENT_NAME \
        "${BUILD_IMAGE_NAMES[$name]}" \
        "/mnt/host/ci/container/build_component.sh"
done
