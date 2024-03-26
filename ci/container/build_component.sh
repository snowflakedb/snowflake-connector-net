#!/bin/bash -e
#
# Build Dotnet Driver
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}

cd $SOURCE_ROOT
cp Snowflake.Data.Tests/parameters-local.json Snowflake.Data.Tests/parameters.json
echo "[INFO] Running dotnet restore"
dotnet restore
echo "[INFO] Building"
dotnet build
