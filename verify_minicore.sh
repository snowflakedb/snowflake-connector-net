#!/bin/bash
set -e

# 1. Build NuGet package
echo "📦 Building NuGet package..."
cd Snowflake.Data
dotnet pack -c Release -o ../artifacts

# 2. Create test project structure
echo "🔨 Setting up verification app..."
cd ..
mkdir -p MinicoreVerifyApp

if [ ! -f MinicoreVerifyApp/MinicoreVerifyApp.csproj ]; then
    cd MinicoreVerifyApp
    dotnet new console --force
    # Add logging abstraction required for the interface
    dotnet add package Microsoft.Extensions.Logging.Abstractions --version 9.0.5
    cd ..
fi

# 3. Update dependencies
cd MinicoreVerifyApp

# Find the latest version built
PACKAGE_VERSION=$(ls ../artifacts/Snowflake.Data.*.nupkg | grep -v symbols | head -n 1 | sed -E 's/.*Snowflake\.Data\.(.*)\.nupkg/\1/')
echo "📍 Testing package version: $PACKAGE_VERSION"

# Remove existing package reference to ensure we get the fresh one
dotnet remove package Snowflake.Data 2>/dev/null || true
dotnet add package Snowflake.Data --version "$PACKAGE_VERSION" --source ../artifacts

# 4. Run verification
echo "🚀 Running verification..."
dotnet run

# Cleanup (optional)
cd ..
