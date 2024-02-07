#!/bin/bash -e

set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}

cd $SOURCE_ROOT
dotnet test -f net6.0 -l "console;verbosity=normal" --logger:"junit;LogFilePath=$SOURCE_ROOT/junit-dotnet.xml"
