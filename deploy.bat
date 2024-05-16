REM Scripts to build .net driver and deploy
SET VERSION=%1
SET API_KEY=%2

SET ROOT_DIR=%~dp0 
cd %ROOT_DIR%

dotnet pack Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.Core\Snowflake.Data.Core.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.AWS\Snowflake.Data.AWS.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.Azure\Snowflake.Data.Azure.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.GCP\Snowflake.Data.GCP.csproj -c Release --force -v n --output %ROOT_DIR%


dotnet nuget push Snowflake.Data.Core.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
dotnet nuget push Snowflake.Data.AWS.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
dotnet nuget push Snowflake.Data.Azure.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
dotnet nuget push Snowflake.Data.GCP.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
dotnet nuget push Snowflake.Data.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json