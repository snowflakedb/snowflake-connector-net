#!/bin/bash -e
#
# Test Snowflake .NET Connector in Rocky Linux 9
# NOTES:
#   - Target frameworks to be tested should be passed in as the first argument, e.g: "net8.0 net9.0". If omitted net8.0 will be assumed.
#   - This script assumes that the project has been built for all target frameworks to be tested
#   - This is the script that test_rockylinux9_docker.sh runs inside of the docker container

DOTNET_TARGET_FRAMEWORKS="${1:-net8.0}"
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"

# Get first available target framework for initial setup
SETUP_TARGET_FRAMEWORK=$(echo ${DOTNET_TARGET_FRAMEWORKS} | awk '{print $1}')
echo "[Info] Using .NET ${SETUP_TARGET_FRAMEWORK} for initial setup"

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

source ${THIS_DIR}/scripts/log_analyze_setup.sh

if [[ -d ${CLIENT_LOG_DIR_PATH_DOCKER} ]]; then
    rm -rf ${CLIENT_LOG_DIR_PATH_DOCKER}/*
else
    mkdir ${CLIENT_LOG_DIR_PATH_DOCKER}
fi

# Replace test parameters and setup authentication
# This is only needed for Jenkins, not GitHub Actions
if [[ "$GITHUB_ACTIONS" != "true" ]]; then
    echo "[Info] Running in Jenkins environment, setting up parameters"
    
    # For Jenkins, we might need to setup parameters differently
    # This would be equivalent to change_snowflake_test_pwd.py for .NET
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

# Fetch wiremock (same as Python version)
echo "[Info] Fetching wiremock"
mkdir -p ${CONNECTOR_DIR}/.wiremock
curl -f https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/3.11.0/wiremock-standalone-3.11.0.jar --output ${CONNECTOR_DIR}/.wiremock/wiremock-standalone.jar

cd $CONNECTOR_DIR

# Set environment flags like existing Docker tests (WIF pattern)
export SF_ENABLE_EXPERIMENTAL_AUTHENTICATION=true

echo "[Info] Building solution with SF_PUBLIC_ENVIRONMENT (container pattern)"
dotnet restore snowflake-connector-net.sln
cd Snowflake.Data.Tests
dotnet restore

# Single build with SF_PUBLIC_ENVIRONMENT for all frameworks (simplified approach)
echo "[Info] Building test project with SF_PUBLIC_ENVIRONMENT for all frameworks"
dotnet build --configuration Release '-p:DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT'

# Single test run per framework (following existing Docker patterns exactly)
for TARGET_FRAMEWORK in ${DOTNET_TARGET_FRAMEWORKS}; do
    echo "[Info] Testing with .NET ${TARGET_FRAMEWORK} (single run like existing Docker tests)"
    
    # Single dotnet test run like authentication/WIF/component Docker tests
    dotnet test \
        --framework ${TARGET_FRAMEWORK} \
        --configuration Release \
        --logger "console;verbosity=detailed" \
        --logger "trx;LogFileName=test_results_${TARGET_FRAMEWORK}.trx" \
        --collect:"XPlat Code Coverage" \
        --results-directory ${CONNECTOR_DIR}/TestResults \
        --no-build
done

echo "[Info] All tests completed successfully"
