#!/bin/bash -e

set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}
export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export JENKINS_HOME='/var/jenkins_home'

cd $SOURCE_ROOT
cp Snowflake.Data.Tests/parameters-local.json Snowflake.Data.Tests/parameters.json
dotnet run  --project ./Snowflake.Data.Tests/Snowflake.Data.Tests.csproj --framework net10.0 -jUnit "junit-dotnet.xml"
