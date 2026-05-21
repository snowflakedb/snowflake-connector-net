#!/bin/bash -e

set -o pipefail

export SF_ENABLE_EXPERIMENTAL_AUTHENTICATION=true

cd Snowflake.Data.Tests
dotnet run --framework net9.0 -p:TargetFrameworks=net9.0 -namespace "Snowflake.Data.WIFTests"
