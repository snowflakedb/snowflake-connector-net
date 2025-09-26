#!/bin/bash -e

set -o pipefail
export THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "$THIS_DIR/scripts/setup_gpg.sh"
export WORKSPACE=${WORKSPACE:-/tmp}
export INTERNAL_REPO=nexus.int.snowflakecomputing.com:8086


CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ -n "$JENKINS_HOME" ]]; then
  ROOT_DIR="$(cd "${CI_DIR}/.." && pwd)"
  export WORKSPACE=${WORKSPACE:-/tmp}

  source $CI_DIR/_init.sh
  source $CI_DIR/scripts/login_internal_docker.sh

fi

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../.github/workflows/parameters/parameters_aws_auth_tests.json "$THIS_DIR/../.github/workflows/parameters/parameters_aws_auth_tests.json.gpg"
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../.github/workflows/parameters/rsa_keys/rsa_key.p8 "$THIS_DIR/../.github/workflows/parameters/rsa_keys/rsa_key.p8.gpg"
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../.github/workflows/parameters/rsa_keys/rsa_key_invalid.p8 "$THIS_DIR/../.github/workflows/parameters/rsa_keys/rsa_key_invalid.p8.gpg"

docker run \
  -v $(cd $THIS_DIR/.. && pwd):/mnt/host \
  -v $WORKSPACE:/mnt/workspace \
  --rm \
  nexus.int.snowflakecomputing.com:8086/docker/snowdrivers-test-external-browser-dotnet:4 \
  "/mnt/host/ci/container/test_authentication.sh"
