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
dotnet build Snowflake.Data.Core\Snowflake.Data.Core.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=%WORKSPACE%\key.snk 
dotnet build Snowflake.Data.AWS\Snowflake.Data.AWS.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=%WORKSPACE%\key.snk 
dotnet build Snowflake.Data.Azure\Snowflake.Data.Azure.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=%WORKSPACE%\key.snk 
dotnet build Snowflake.Data.GCP\Snowflake.Data.GCP.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile=%WORKSPACE%\key.snk 

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data\bin\Release\netstandard2.0\Snowflake.Data.dll"
"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data.Core\bin\Release\netstandard2.0\Snowflake.Data.Core.dll"
"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data.AWS\bin\Release\netstandard2.0\Snowflake.Data.AWS.dll"
"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data.Azure\bin\Release\netstandard2.0\Snowflake.Data.Azure.dll"
"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v %WORKSPACE%"\Snowflake.Data.GCP\bin\Release\netstandard2.0\Snowflake.Data.GCP.dll"

dotnet pack Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.Core\Snowflake.Data.Core.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.AWS\Snowflake.Data.AWS.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.Azure\Snowflake.Data.Azure.csproj -c Release --force -v n --output %ROOT_DIR%
dotnet pack Snowflake.Data.GCP\Snowflake.Data.GCP.csproj -c Release --force -v n --output %ROOT_DIR%

@REM dotnet nuget push Snowflake.Data.Core.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
@REM dotnet nuget push Snowflake.Data.AWS.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
@REM dotnet nuget push Snowflake.Data.Azure.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
@REM dotnet nuget push Snowflake.Data.GCP.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
@REM dotnet nuget push Snowflake.Data.%VERSION%.nupkg -k %API_KEY% -s https://api.nuget.org/v3/index.json
