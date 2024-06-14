REM Scripts to build .net driver and deploy
SET VERSION=%1
SET API_KEY=%2
SET SN_KEY=%3

SET ROOT_DIR=%~dp0
cd %ROOT_DIR%

echo %SN_KEY% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"

REM command to sign with strong name Snowflake.Data.dll should be here
where sn.exe

dir "C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"
"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -R "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\Snowflake.Data\bin\Release\net8.0\Snowflake.Data.dll" "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\snkey.txt"

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\Snowflake.Data\bin\Release\net8.0\Snowflake.Data.dll"

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\Snowflake.Data\bin\Release\net8.0\Snowflake.Data.dll"
