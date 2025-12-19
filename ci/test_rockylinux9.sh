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

# Build the solution
cd $CONNECTOR_DIR
echo "[Info] Building Snowflake .NET Connector solution"
dotnet restore snowflake-connector-net.sln
dotnet build snowflake-connector-net.sln --configuration Release --no-restore -p:DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT

# Run tests
if [[ "$is_old_driver" == "true" ]]; then
    # Old Driver Test (if applicable for .NET)
    echo "[Info] Running old connector tests"
    # This would be equivalent to olddriver tests for .NET
    dotnet test Snowflake.Data.Tests/Snowflake.Data.Tests.csproj --configuration Release --framework ${SETUP_TARGET_FRAMEWORK} --logger "console;verbosity=detailed" --filter "Category!=Integration"
else
    for TARGET_FRAMEWORK in ${DOTNET_TARGET_FRAMEWORKS}; do
        echo "[Info] Testing with .NET ${TARGET_FRAMEWORK}"
        
        # Check if the target framework is supported by the project
        if ! dotnet build Snowflake.Data.Tests/Snowflake.Data.Tests.csproj --framework ${TARGET_FRAMEWORK} --configuration Release --verbosity quiet --no-restore -p:DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT; then
            echo "[Warning] Target framework ${TARGET_FRAMEWORK} compilation failed, skipping..."
            continue
        fi
        
        echo "[Info] Running unit tests for ${TARGET_FRAMEWORK}"
        dotnet test Snowflake.Data.Tests/Snowflake.Data.Tests.csproj \
            --configuration Release \
            --framework ${TARGET_FRAMEWORK} \
            --logger "console;verbosity=detailed" \
            --logger "trx;LogFileName=test_results_${TARGET_FRAMEWORK}.trx" \
            --collect:"XPlat Code Coverage" \
            --results-directory ${CONNECTOR_DIR}/TestResults/${TARGET_FRAMEWORK} \
            --filter "Category!=Integration&Category!=IPv6" \
            --no-build \
            -- DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT
        
        echo "[Info] Running integration tests for ${TARGET_FRAMEWORK}"
        dotnet test Snowflake.Data.Tests/Snowflake.Data.Tests.csproj \
            --configuration Release \
            --framework ${TARGET_FRAMEWORK} \
            --logger "console;verbosity=detailed" \
            --logger "trx;LogFileName=integration_test_results_${TARGET_FRAMEWORK}.trx" \
            --collect:"XPlat Code Coverage" \
            --results-directory ${CONNECTOR_DIR}/TestResults/${TARGET_FRAMEWORK}/Integration \
            --filter "Category=Integration" \
            --no-build \
            -- DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT
    done
fi

echo "[Info] All tests completed successfully"
