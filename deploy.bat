REM Scripts to build .net driver and deploy
SET VERSION=%1
SET API_KEY=%2

SET ROOT_DIR=%~dp0
cd %ROOT_DIR%

dotnet build Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=snKey.snk

dotnet pack Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n --no-build  --output %ROOT_DIR%

dotnet nuget push Snowflake.Data.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
