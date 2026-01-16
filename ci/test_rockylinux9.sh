#!/bin/bash -e
#
# Test Snowflake .NET Connector in Rocky Linux 9
#
# This script runs inside the Rocky Linux 9 Docker container.
# It builds the .NET connector and runs tests for a specific target framework.
#
# Required environment variables:
#   - net_version: Target framework (e.g., "net8.0")
#   - snowflake_cloud_env: Cloud environment (e.g., "AWS", "AZURE", "GCP")
#   - PARAMETER_SECRET: GPG passphrase for decrypting test parameters
#
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"

# Validate required environment variables
if [[ -z "$net_version" ]]; then
    echo "[ERROR] net_version environment variable not set."
    exit 1
fi

if [[ -z "$snowflake_cloud_env" ]]; then
    echo "[ERROR] snowflake_cloud_env environment variable not set."
    exit 1
fi
echo "[INFO] Using cloud environment: ${snowflake_cloud_env}"

if [[ -z "$PARAMETER_SECRET" ]]; then
    echo "[ERROR] PARAMETER_SECRET environment variable is required"
    exit 1
fi

# Decrypt parameters
echo "[INFO] Decrypting test parameters for ${snowflake_cloud_env}..."
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_SECRET" \
    "${CONNECTOR_DIR}/.github/workflows/parameters/parameters_${snowflake_cloud_env}.json.gpg" \
    > "${CONNECTOR_DIR}/Snowflake.Data.Tests/parameters.json"

# Decrypt RSA key
echo "[INFO] Decrypting RSA key for ${snowflake_cloud_env}..."
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_SECRET" \
    "${CONNECTOR_DIR}/.github/workflows/parameters/rsa_keys/rsa_key_dotnet_${snowflake_cloud_env}.p8.gpg" \
    > "${CONNECTOR_DIR}/Snowflake.Data.Tests/rsa_key_dotnet_${snowflake_cloud_env}.p8"

# Validate .NET SDK is installed
if ! command -v dotnet &> /dev/null; then
    echo "[ERROR] dotnet command not found. Please ensure .NET SDK is installed."
    exit 1
fi

# Display .NET version info
echo "[INFO] .NET SDK version:"
dotnet --version
echo "[INFO] Available .NET runtimes:"
dotnet --list-runtimes
echo "[INFO] dotnet-coverage version:"
dotnet-coverage --version

# Build from project root
echo "[INFO] Building driver with SF_PUBLIC_ENVIRONMENT"
pushd "${CONNECTOR_DIR}"
dotnet restore
dotnet build '-p:DefineAdditionalConstants=SF_PUBLIC_ENVIRONMENT'
popd

# Run tests from Snowflake.Data.Tests directory
echo "[INFO] Running tests for ${net_version}"
pushd "${CONNECTOR_DIR}/Snowflake.Data.Tests"
dotnet-coverage collect \
    "dotnet test --framework ${net_version} --no-build --logger \"junit;LogFilePath=rockylinux9_${net_version}_${snowflake_cloud_env}_results.junit.xml\" --verbosity normal" \
    --output "rockylinux9_${net_version}_${snowflake_cloud_env}_coverage.xml" \
    --output-format cobertura \
    --settings coverage.config
popd

echo "[INFO] Tests completed successfully"
