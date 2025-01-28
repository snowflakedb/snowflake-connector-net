#!/bin/bash -e


set -o pipefail

export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}

AUTH_PARAMETER_FILE=./.github/workflows/parameters/parameters_aws_auth_tests.json
eval $(jq -r '.authtestparams | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $AUTH_PARAMETER_FILE)

export SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH=./.github/workflows/parameters/rsa_keys/rsa_key.p8
export SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH=./.github/workflows/parameters/rsa_keys/rsa_key_invalid.p8

PARAMETERS_JSON="./Snowflake.Data.Tests/parameters.json"
echo '{ "testconnection": {' > $PARAMETERS_JSON
env | grep '^SNOWFLAKE_TEST' | while IFS='=' read -r key value; do
  echo "  \"$key\": \"$value\"," >> $PARAMETERS_JSON
done
# Remove the last comma and close the JSON object
awk '{if (NR>1) print prev; prev=$0} END {sub(/,$/, "", prev); print prev}' $PARAMETERS_JSON > tmp && mv tmp $PARAMETERS_JSON
echo '}}' >> $PARAMETERS_JSON

dotnet test -l "console;verbosity=diagnostic" --filter FullyQualifiedName~AuthenticationTests
