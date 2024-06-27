SETLOCAL EnableDelayedExpansion

"C:\Program Files (x86)\Microsoft SDKs\Windows\v10.0A\bin\NETFX 4.8 Tools\x64\sn.exe" -k "C:\jenkins\workspace\DotNetKeyTest\newkey.snk"

certutil -f -encode "C:\jenkins\workspace\DotNetKeyTest\newkey.snk" "C:\jenkins\workspace\DotNetKeyTest\newkey.snk.asc"

for /f "Tokens=* Delims=" %%x in ("C:\jenkins\workspace\DotNetKeyTest\newkey.snk.asc") do set Content=!Content!%%x

echo %Content%
