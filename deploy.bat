REM Scripts to build .net driver and deploy
SET VERSION=%1
SET API_KEY=%2
SET LINE1=%3
SET LINE2=%4
SET LINE3=%5
SET LINE4=%6
SET LINE5=%7
SET LINE6=%8
SET LINE7=%9
SET LINE8="LCqulHPiEugaUP/qoW2glsK3sWyYz9nRzWS+Dyb4EbpzCEI+9QgBZMLIAdjlAzpt"
SET LINE9="6EEg3kCjtpB/MlMmWf7475TFEEt8jvN5tpYswUJ6eGb3Lzna7m1r6n1/VcSShvsZ"
SET LINE10="3UYdbKsAlocfNBYSen3oWyE7ZB8AU+Dz8d7VMJ1DsxaWRc6kmTdZ9Sx0Z9+WCwY1"
SET LINE11="MREs9191JAWlhaa0qqZAe3mHGG4bXexGRAcS0fkIiLASd7absXnQA2toL6FIqQOn"
SET LINE12="/GVuJ+PQS7RnD2043nRHsHbdX2xKaB0pnJx96Z049pIpVDAvYMYlkm4F855NGbXj"
SET LINE13="QHLjpqPo7XoSoVrHgIpkRBsMv2c="

SET ROOT_DIR=%~dp0
cd %ROOT_DIR%

echo "-----BEGIN CERTIFICATE-----" > "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE1% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE2% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE3% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE4% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE5% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE6% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE7% >>"C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE8% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE9% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE10% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE11% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE12% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo %LINE13% >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"
echo "-----END CERTIFICATE-----" >> "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"

type "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt"

certutil -decode "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\coded.txt" "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\key.snk"

type "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\key.snk"

:: "C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64\sn.exe" -k "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\newkey.snk"
:: certutil -f  -encode "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\newkey.snk" "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\newkeyencoded.txt"
:: certutil -f  -decode "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\newkeyencoded.txt" "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\newkeydecoded.snk"

dir "C:\jenkins\workspace\NugetPushDotNetDriverSignTest"

dotnet build Snowflake.Data\Snowflake.Data.csproj -c Release --force -v n /p:SignAssembly=true /p:AssemblyOriginatorKeyFile="C:\jenkins\workspace\NugetPushDotNetDriverSignTest\key.snk"

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64"\sn.exe -v "C:\jenkins\workspace\NugetPushDotNetDriverSignTest\Snowflake.Data\bin\Release\net8.0\Snowflake.Data.dll"
