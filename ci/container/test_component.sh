#!/bin/bash -e

set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}
export WORKSPACE=${WORKSPACE:-/mnt/workspace}

cd $SOURCE_ROOT
cp Snowflake.Data.Tests/parameters-local.json Snowflake.Data.Tests/parameters.json
# todo unignore MaxLobSizeIT after SNOW-1058345 is fixed
dotnet test -f net6.0 -l "console;verbosity=normal" --logger:"junit;LogFilePath=$WORKSPACE/junit-dotnet.xml" --filter "FullyQualifiedName!=Snowflake.Data.Tests.IntegrationTests.MaxLobSizeIT" 
