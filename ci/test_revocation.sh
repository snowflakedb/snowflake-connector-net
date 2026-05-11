#!/bin/bash
#
# Test certificate revocation validation using the revocation-validation framework.
#

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DRIVER_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${DRIVER_DIR}}

echo "[Info] Starting revocation validation tests"
echo "[Info] .NET driver path: $DRIVER_DIR"

# The framework's .NET client defaults to ~/repos/snowflake-connector-net for per-scenario clients
mkdir -p "$HOME/repos"
ln -sfn "$DRIVER_DIR" "$HOME/repos/snowflake-connector-net"

set -e

# Clone revocation-validation framework
REVOCATION_DIR="/tmp/revocation-validation"
DOTNET_VERSION="${DOTNET_VERSION:-10}"

rm -rf "$REVOCATION_DIR"
if [ "$DOTNET_VERSION" = "10" ]; then
    REVOCATION_BRANCH="SNOW-3470124/net10"
else
    REVOCATION_BRANCH="main"
fi

CLONE_ARGS="--branch $REVOCATION_BRANCH"

if [ -n "$GITHUB_USER" ] && [ -n "$GITHUB_TOKEN" ]; then
    git clone $CLONE_ARGS "https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/snowflake-eng/revocation-validation.git" "$REVOCATION_DIR"
else
    git clone $CLONE_ARGS "https://github.com/snowflake-eng/revocation-validation.git" "$REVOCATION_DIR"
fi

cd "$REVOCATION_DIR"

# Fix the hardcoded connector path in the test app's .csproj
CSPROJ="$REVOCATION_DIR/validation/clients/snowflake-dotnet/SnowflakeTest.csproj"
sed -i "s|/Users/snoonan/repos/snowflake-connector-net|${DRIVER_DIR}|g" "$CSPROJ"
echo "[Info] Updated .csproj to reference: $DRIVER_DIR"

# Build the .NET test app using Docker (dotnet SDK not on bare node)
echo "[Info] Building .NET test app..."
docker run --rm \
    -v "$REVOCATION_DIR/validation/clients/snowflake-dotnet:/src" \
    -v "$DRIVER_DIR:/connector" \
    -w /src \
    mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION}.0 \
    bash -c "sed -i 's|${DRIVER_DIR}|/connector|g' SnowflakeTest.csproj && dotnet publish -f net${DOTNET_VERSION}.0 -p:TargetFrameworks=net${DOTNET_VERSION}.0 -c Release -o /src/bin/Release/net${DOTNET_VERSION}.0"
echo "[Info] Build complete: $(ls $REVOCATION_DIR/validation/clients/snowflake-dotnet/bin/Release/net${DOTNET_VERSION}.0/SnowflakeTest.dll)"

echo "[Info] Running tests with Go $(go version | grep -oE 'go[0-9]+\.[0-9]+')..."

go run . \
    --client snowflake-dotnet \
    --dotnet-connector-path "${DRIVER_DIR}" \
    --output "${WORKSPACE}/revocation-results.json" \
    --output-html "${WORKSPACE}/revocation-report.html" \
    --log-level debug

EXIT_CODE=$?

if [ -f "${WORKSPACE}/revocation-results.json" ]; then
    echo "[Info] Results: ${WORKSPACE}/revocation-results.json"
fi
if [ -f "${WORKSPACE}/revocation-report.html" ]; then
    echo "[Info] Report: ${WORKSPACE}/revocation-report.html"
fi

exit $EXIT_CODE
