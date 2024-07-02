cd "C:\jenkins\workspace\DotNetKeyTest"

SETLOCAL EnableDelayedExpansion
SET Footer="-----END CERTIFICATE-----"

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64\sn.exe" -k "C:\jenkins\workspace\DotNetKeyTest\newkey.snk"

certutil -f -encode "C:\jenkins\workspace\DotNetKeyTest\newkey.snk" "C:\jenkins\workspace\DotNetKeyTest\newkey.snk.asc"

more +1 "C:\jenkins\workspace\DotNetKeyTest\newkey.snk.asc" > "C:\jenkins\workspace\DotNetKeyTest\newkey1.snk.asc"

FINDSTR /R /I /V "^$ Footer" "C:\jenkins\workspace\DotNetKeyTest\newkey1.snk.asc">>"C:\jenkins\workspace\DotNetKeyTest\newkey2.snk.asc"

for /f "Tokens=* Delims=" %%x in (newkey2.snk.asc) do set Content=!Content!%%x

echo %Content% > "C:\jenkins\workspace\DotNetKeyTest\snk.txt"

aws s3 cp "C:\jenkins\workspace\DotNetKeyTest\snk.txt" "s3://sfc-eng-jenkins/repository/python_connector/ank2.txt"
