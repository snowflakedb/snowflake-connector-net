#!/bin/bash -e

set -o pipefail

cd Snowflake.Data.Tests
dotnet run --framework net9.0 -p:TargetFrameworks=net9.0 -namespace "Snowflake.Data.WIFTests"
