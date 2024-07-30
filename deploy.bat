REM Scripts to build .net driver and deploy
SET VERSION=%1
SET API_KEY=%2
SET SNKEY=%3

SET ROOT_DIR=%~dp0 
cd %ROOT_DIR%

aws s3 cp s3://sfc-eng-jenkins/repository/net/sign-artifact.exe .
sign-artifact.exe sign-artifact -o snowflakedb -r snowflake-connector-net -l 20 -v -u -d -f Snowflake.Data.%VERSION%.nupkg
dir
@REM echo -----BEGIN CERTIFICATE----- > %WORKSPACE%\coded.txt
@REM echo %SNKEY% >> %WORKSPACE%\coded.txt
@REM echo -----END CERTIFICATE----- >> %WORKSPACE%\coded.txt

@REM certutil -decode %WORKSPACE%\coded.txt %WORKSPACE%\key.snk

@REM dotnet build Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=%WORKSPACE%\key.snk 
@REM "C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data\bin\Release\netstandard2.0\Snowflake.Data.dll"

@REM dotnet pack Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n --no-build  --output %ROOT_DIR%

@REM dotnet nuget push Snowflake.Data.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
