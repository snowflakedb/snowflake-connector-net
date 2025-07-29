#!/bin/bash -e

set -o pipefail

export SF_ENABLE_EXPERIMENTAL_AUTHENTICATION=true

SNOWFLAKE_TEST_WIF_ACCOUNT="$SNOWFLAKE_TEST_WIF_ACCOUNT" \
SNOWFLAKE_TEST_WIF_HOST="$SNOWFLAKE_TEST_WIF_HOST" \
dotnet test --framework net9.0 -l "console;verbosity=info" --filter FullyQualifiedName~WIFTests
