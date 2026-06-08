#!/bin/bash -e

set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(cd "${THIS_DIR}/.." && pwd)"

echo "[INFO] Running Unit Tests for Snowflake .NET Connector"

dotnet run ^
    --project ./Snowflake.Data.Tests/Snowflake.Data.Tests.csproj ^
    --framework net9.0 ^
    -p:TargetFrameworks=net9.0 ^
    -namespace "Snowflake.Data.Tests.UnitTests" ^
    -l "console;verbosity=normal" ^
    --logger:"junit;LogFilePath=%ROOT_DIR%\junit-dotnet-unit.xml"

echo "[INFO] Test Results: $ROOT_DIR/junit-dotnet-unit.xml"
