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

REM TODO: SNOW-2027569 - Uncomment once issue with signature files are not valid after artifacts are published. Workaround is to sign after artifacts are published.
REM aws s3 cp s3://sfc-eng-jenkins/repository/net/sign-artifact.exe .
REM @For /F Delims^= %%G In ('""certutil.exe" -HashFile "sign-artifact.exe" SHA512|"find.exe" /V ":""')Do @Set "SHA=%%G"
REM if not %SHA%==94f0b4a78979ded42f7f8c8ce2534691f9f874888bcf7963876f5be881cf6d0ce00e6f8d3e656492249fcfcb890ad656745f2cf68f98e828eb02ded6189a87d4 (
REM   echo "Failed to verify the sha for the signing script"
REM   exit 1
REM )
REM sign-artifact.exe sign-artifact -o snowflakedb -r snowflake-connector-net -t v%VERSION%  -l 20 -v -u -f Snowflake.Data.%VERSION%.nupkg

dotnet nuget push Snowflake.Data.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
