REM Scripts to build .net driver and deploy
SET VERSION=%1
SET API_KEY=%2
SET SNKEY=%3

SET ROOT_DIR=%~dp0 
cd %ROOT_DIR%

echo -----BEGIN CERTIFICATE----- > %WORKSPACE%\coded.txt
echo %SNKEY% >> %WORKSPACE%\coded.txt
echo -----END CERTIFICATE----- >> %WORKSPACE%\coded.txt

certutil -decode %WORKSPACE%\coded.txt %WORKSPACE%\key.snk

dotnet build Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=%WORKSPACE%\key.snk 
"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data\bin\Release\netstandard2.0\Snowflake.Data.dll"

dotnet pack Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n --no-build  --output %ROOT_DIR%

dotnet nuget push Snowflake.Data.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
