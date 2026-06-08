set THIS_DIR=%~dp0
set ROOT_DIR=%THIS_DIR%..

pushd %ROOT_DIR%

echo [INFO] Running Unit Tests for Snowflake .NET Connector

dotnet test Snowflake.Data.Tests/Snowflake.Data.Tests.csproj ^
    --filter "FullyQualifiedName~Snowflake.Data.Tests.UnitTests" ^
    -l "console;verbosity=normal" ^
    --logger:"junit;LogFilePath=%ROOT_DIR%\junit-dotnet-unit.xml"

set EXIT_CODE=%ERRORLEVEL%

popd

echo [INFO] Test Results: %ROOT_DIR%\junit-dotnet-unit.xml
exit /b %EXIT_CODE%
