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

set +e
dotnet-coverage collect "$TEST_CMD ${ARGS}" --output "$COVERAGE_FILE" --output-format cobertura --settings coverage.config
TEST_EXIT=$?
set -e

if [ $TEST_EXIT -eq 0 ]; then
    exit 0
fi

# --- Tests failed — attempt retry if conditions are met ---

echo "[RETRY] Tests failed (exit code: $TEST_EXIT). Checking if retry is appropriate..."

# Parse failure counts
TOTAL_FAILURES=$(xmllint --xpath 'count(//testcase[failure or error])' "$JUNIT_XML" 2>/dev/null)
INTEGRATION_FAILURES=$(xmllint --xpath 'count(//testcase[failure or error][starts-with(@classname,"Snowflake.Data.Tests.IntegrationTests")])' "$JUNIT_XML" 2>/dev/null)

# xmllint may return floats like "3.0"
TOTAL_FAILURES=${TOTAL_FAILURES%%.*}
INTEGRATION_FAILURES=${INTEGRATION_FAILURES%%.*}
OTHER_FAILURES=$((TOTAL_FAILURES - INTEGRATION_FAILURES))

echo "[RETRY] Failure summary: total=$TOTAL_FAILURES, integration=$INTEGRATION_FAILURES, other=$OTHER_FAILURES"

if [[ "$OTHER_FAILURES" -gt 0 ]]; then
    echo "[RETRY] Other test failures detected ($OTHER_FAILURES). Not retrying."
    exit 1
fi

if [[ "$INTEGRATION_FAILURES" -eq 0 ]]; then
    echo "[RETRY] No integration test failures in JUnit XML. Cannot determine what to retry."
    exit 1
fi

if [[ "$INTEGRATION_FAILURES" -ge 3 ]]; then
    echo "[RETRY] Too many failures ($INTEGRATION_FAILURES >= 3). Likely a real issue."
    exit 1
fi

# Extract failed test FQNs
FAILED_TESTS=""
for i in $(seq 1 "$INTEGRATION_FAILURES"); do
    CLASSNAME=$(xmllint --xpath "string((//testcase[failure or error][starts-with(@classname,'Snowflake.Data.Tests.IntegrationTests')])[$i]/@classname)" "$JUNIT_XML" 2>/dev/null)
    NAME=$(xmllint --xpath "string((//testcase[failure or error][starts-with(@classname,'Snowflake.Data.Tests.IntegrationTests')])[$i]/@name)" "$JUNIT_XML" 2>/dev/null)
    FQN="${CLASSNAME}.${NAME}"
    if [[ -n "$FAILED_TESTS" ]]; then
        FAILED_TESTS="${FAILED_TESTS}"$'\n'"${FQN}"
    else
        FAILED_TESTS="${FQN}"
    fi
done

echo "[RETRY] Will retry $INTEGRATION_FAILURES integration test(s):"
echo "$FAILED_TESTS" | while read -r t; do echo "[RETRY]   - $t"; done

if [[ "$use_dotnet_run" == "true" ]]; then
    FILTER=""
        while IFS= read -r t; do
            [[ -n "$FILTER" ]] && FILTER="${FILTER}|"
            FILTER="${FILTER} -class ${t}"
        done <<< "$FAILED_TESTS"
        RETRY_CMD="dotnet run --framework "$net_version" --no-build $FILTER"
else
    FILTER=""
    while IFS= read -r t; do
        [[ -n "$FILTER" ]] && FILTER="${FILTER}|"
        FILTER="${FILTER}FullyQualifiedName=${t}"
    done <<< "$FAILED_TESTS"
    RETRY_CMD="dotnet test --framework "$net_version" --no-build --filter \"$FILTER\""
fi

echo "[RETRY] Command: ${RETRY_CMD}"
echo "[RETRY] ============================================"

set +e
"${RETRY_CMD}"
RETRY_EXIT=$?
set -e

echo "[RETRY] ============================================"

if [[ $RETRY_EXIT -ne 0 ]]; then
    echo "[RETRY] FAILED: Tests still failing after retry."
    exit 1
fi

echo "[RETRY] SUCCESS: All $INTEGRATION_FAILURES flaky test(s) passed on retry."

TESTS_LIST=$(echo "$FAILED_TESTS" | tr '\n' ',' | sed 's/,$//')

# Write to GITHUB_OUTPUT (native GHA steps)
if [[ -n "$GITHUB_OUTPUT" ]]; then
    echo "retried_tests=$TESTS_LIST" >> "$GITHUB_OUTPUT"
fi

# Write marker file (Docker-based jobs)
echo "$TESTS_LIST" > .retried_tests
