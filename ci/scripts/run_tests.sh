#!/bin/bash
#
# Run .NET connector tests with automatic retry for flaky integration tests.
#
# If fewer than 3 integration tests fail (and no unit tests fail), retries
# only those tests once. Produces a warning annotation on success-after-retry.
#
# Usage:
#   ./run_tests.sh <platform>
#
# Arguments:
#   platform - Platform prefix for result files (windows, linux, macos, rockylinux9)
#
# Required environment variables:
#   use_dotnet_run      - "true" to use 'dotnet run', otherwise uses 'dotnet test'
#   net_version         - Target framework (e.g., "net8.0")
#   snowflake_cloud_env - Cloud environment (e.g., "AWS", "AZURE", "GCP")
#
# Dependencies:
#   xmllint (pre-installed on Ubuntu, macOS, Rocky Linux)
#

PLATFORM="$1"
if [[ "$PLATFORM" != "windows" && "$PLATFORM" != "linux" && "$PLATFORM" != "macos" && "$PLATFORM" != "rockylinux9" ]]; then
    echo "[ERROR] Invalid platform: '$PLATFORM'. Must be one of: windows, linux, macos, rockylinux9"
    exit 1
fi
cd Snowflake.Data.Tests

RESULTS_BASE="${PLATFORM}_${net_version}_${snowflake_cloud_env}_results"
COVERAGE_FILE="${PLATFORM}_${net_version}_${snowflake_cloud_env}_coverage.xml"
JUNIT_XML="${RESULTS_BASE}.junit.xml"

if [[ "$use_dotnet_run" == "true" ]]; then
    TEST_CMD="dotnet run"
    ARGS="--framework $net_version --no-build --verbosity detailed -junit ${RESULTS_BASE}.junit.xml"
else
    TEST_CMD="dotnet test"
    ARGS="--framework $net_version --no-build --verbosity detailed --logger junit;LogFilePath=${RESULTS_BASE}.junit.xml"
fi

echo "[INFO] Running tests: platform=$PLATFORM, framework=$net_version, cloud=$snowflake_cloud_env"

dotnet-coverage collect "$TEST_CMD ${ARGS}" --output "$COVERAGE_FILE" --output-format cobertura --settings coverage.config
