#!/bin/bash -e


set -o pipefail

export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}

AUTH_PARAMETER_FILE=./.github/workflows/parameters/parameters_aws_auth_tests.json
eval $(jq -r '.authtestparams | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $AUTH_PARAMETER_FILE)

export SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH=./.github/workflows/parameters/rsa_keys/rsa_key.p8
export SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH=./.github/workflows/parameters/rsa_keys/rsa_key_invalid.p8

dotnet test --framework net9.0 -l "console;verbosity=info" --filter FullyQualifiedName~AuthenticationTests
