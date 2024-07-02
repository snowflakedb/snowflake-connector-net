cd "C:\jenkins\workspace\DotNetKeyTest"

SETLOCAL EnableDelayedExpansion

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64\sn.exe" -k "C:\jenkins\workspace\DotNetKeyTest\newkey.snk"

certutil -f -encode "C:\jenkins\workspace\DotNetKeyTest\newkey.snk" "C:\jenkins\workspace\DotNetKeyTest\newkey.snk.asc"

more +1 "C:\jenkins\workspace\DotNetKeyTest\newkey.snk.asc"|findstr /vX "-----END CERTIFICATE-----" > "C:\jenkins\workspace\DotNetKeyTest\newkey1.snk.asc"

for /f "Tokens=* Delims=" %%x in (newkey1.snk.asc) do set Content=!Content!%%x

echo %Content% > "C:\jenkins\workspace\DotNetKeyTest\snk.txt"

aws s3 cp "C:\jenkins\workspace\DotNetKeyTest\snk.txt" "s3://sfc-eng-jenkins/repository/python_connector/ank1.txt"
