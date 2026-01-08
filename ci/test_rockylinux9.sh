#!/bin/bash -e
#
# Test Snowflake .NET Connector in Rocky Linux 9
#
# Required env var:
#   TARGET_FRAMEWORK - e.g., "net8.0"
#
# This script uses dotnet-coverage like the standard Linux tests in GHA.
# It runs inside the docker container started by test_rockylinux9_docker.sh.

TARGET_FRAMEWORK="${TARGET_FRAMEWORK:-net8.0}"
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"

# Get cloud environment from env var (matches GHA workflow)
CLOUD_ENV=${snowflake_cloud_env:-AWS}

echo "[Info] Testing target framework: ${TARGET_FRAMEWORK}"
echo "[Info] Cloud environment: ${CLOUD_ENV}"

# Verify dotnet is available
if ! command -v dotnet &> /dev/null; then
    echo "[ERROR] dotnet command not found. Please ensure .NET SDK is installed."
    exit 1
fi

# Display .NET version info
echo "[Info] .NET SDK version:"
dotnet --version
echo "[Info] Available .NET runtimes:"
dotnet --list-runtimes
echo "[Info] dotnet-coverage version:"
dotnet-coverage --version || echo "[Warning] dotnet-coverage not found, will skip coverage collection"

# Replace test parameters and setup authentication
# This is only needed for Jenkins, not GitHub Actions
if [[ "$GITHUB_ACTIONS" != "true" ]]; then
    echo "[Info] Running in Jenkins environment, setting up parameters"
    
    if [[ -f ${CONNECTOR_DIR}/Snowflake.Data.Tests/parameters_jenkins.json ]]; then
        cp ${CONNECTOR_DIR}/Snowflake.Data.Tests/parameters_jenkins.json ${CONNECTOR_DIR}/Snowflake.Data.Tests/parameters.json
        echo "[Info] Copied parameters_jenkins.json to parameters.json"
    else
        echo "[Warning] parameters_jenkins.json not found, using existing parameters.json"
    fi
else
    echo "[Info] Running in GitHub Actions, using existing configuration"
    echo "[Info] Checking if Snowflake.Data.Tests/parameters.json exists:"
    ls -la ${CONNECTOR_DIR}/Snowflake.Data.Tests/parameters.json || echo "[ERROR] parameters.json NOT FOUND!"
    echo "[Info] Checking if RSA key file exists:"
    ls -la ${CONNECTOR_DIR}/Snowflake.Data.Tests/rsa_key_dotnet_*.p8 || echo "[ERROR] RSA key NOT FOUND!"
fi

# Fetch wiremock (required for mock tests)
echo "[Info] Fetching wiremock"
mkdir -p ${CONNECTOR_DIR}/.wiremock
curl -f https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/3.11.0/wiremock-standalone-3.11.0.jar --output ${CONNECTOR_DIR}/.wiremock/wiremock-standalone.jar

cd $CONNECTOR_DIR

# Build the driver (matches test-linux pattern in GHA)
echo "[Info] Building driver with SF_PUBLIC_ENVIRONMENT"
dotnet restore
dotnet build '-p:DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT'

# Run tests (matches test-linux pattern in GHA)
cd Snowflake.Data.Tests
echo "[Info] Running tests for ${TARGET_FRAMEWORK}"

# Use dotnet-coverage collect like the standard Linux tests in GHA workflow
dotnet-coverage collect \
    "dotnet test --framework ${TARGET_FRAMEWORK} --no-build --logger \"junit;LogFilePath=rockylinux9_${TARGET_FRAMEWORK}_${CLOUD_ENV}_results.junit.xml\" --verbosity normal" \
    --output "rockylinux9_${TARGET_FRAMEWORK}_${CLOUD_ENV}_coverage.xml" \
    --output-format cobertura \
    --settings coverage.config

echo "[Info] Tests completed successfully"
