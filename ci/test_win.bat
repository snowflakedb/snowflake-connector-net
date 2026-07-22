set THIS_DIR=%~dp0
set ROOT_DIR=%THIS_DIR%..

pushd %ROOT_DIR%

echo [INFO] Building Unit Tests for Snowflake .NET Driver

dotnet build ^
    -p:nodeReuse=false ^
    -p:mt=false

echo [INFO] Running Unit Tests for Snowflake .NET Driver

dotnet run ^
    --no-build ^
    --framework net10.0 ^
    --project .\Snowflake.Data.Tests\Snowflake.Data.Tests.csproj ^
    -namespace "Snowflake.Data.Tests.UnitTests" ^
    --verbosity detailed ^
    -junit "%%ROOT_DIR%\junit-dotnet-unit.xml"

set EXIT_CODE=%ERRORLEVEL%

popd

echo [INFO] Test Results: %ROOT_DIR%\junit-dotnet-unit.xml
exit /b %EXIT_CODE%
