#!/bin/bash -e

dotnet run ^
    --project ./Snowflake.Data.Tests/Snowflake.Data.Tests.csproj ^
    --framework net9.0 ^
    -p:TargetFrameworks=net9.0 ^
    -namespace "Snowflake.Data.Tests.UnitTests" ^
    --verbosity detailed ^

echo "[INFO] Done testing"
