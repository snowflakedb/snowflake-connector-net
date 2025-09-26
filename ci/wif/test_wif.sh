#!/bin/bash -e

set -o pipefail

export SF_ENABLE_EXPERIMENTAL_AUTHENTICATION=true

dotnet test --framework net9.0 -l "console;verbosity=info" --filter FullyQualifiedName~WIFTests
