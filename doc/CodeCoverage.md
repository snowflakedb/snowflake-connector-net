## Getting the code coverage

1. Go to .NET project directory

2. Clean the directory

```
dotnet clean snowflake-connector-net.sln && dotnet nuget locals all --clear
```

3. Create parameters.json containing connection info for AWS, AZURE, or GCP account and place inside the Snowflake.Data.Tests folder

4. Build the project for .NET6

```
dotnet build snowflake-connector-net.sln /p:DebugType=Full
```

5. Run dotnet-cover on the .NET6 build

```
dotnet-coverage collect "dotnet test --framework net6.0 --no-build -l console;verbosity=normal" --output net6.0_AWS_coverage.xml --output-format cobertura --settings coverage.config
```

6. Build the project for .NET Framework

```
msbuild snowflake-connector-net.sln -p:Configuration=Release
```

7. Run dotnet-cover on the .NET Framework build

```
dotnet-coverage collect "dotnet test --framework net472 --no-build -l console;verbosity=normal" --output net472_AWS_coverage.xml --output-format cobertura --settings coverage.config
```

<br />
Repeat steps 3, 5, and 7 for the other cloud providers. <br />
Note: no need to rebuild the connector again. <br /><br />

For Azure:<br />

3. Create parameters.json containing connection info for AZURE account and place inside the Snowflake.Data.Tests folder

4. Run dotnet-cover on the .NET6 build

```
dotnet-coverage collect "dotnet test --framework net6.0 --no-build -l console;verbosity=normal" --output net6.0_AZURE_coverage.xml --output-format cobertura --settings coverage.config
```

7. Run dotnet-cover on the .NET Framework build

```
dotnet-coverage collect "dotnet test --framework net472 --no-build -l console;verbosity=normal" --output net472_AZURE_coverage.xml --output-format cobertura --settings coverage.config
```

<br />
For GCP:<br />

3. Create parameters.json containing connection info for GCP account and place inside the Snowflake.Data.Tests folder

4. Run dotnet-cover on the .NET6 build

```
dotnet-coverage collect "dotnet test --framework net6.0 --no-build -l console;verbosity=normal" --output net6.0_GCP_coverage.xml --output-format cobertura --settings coverage.config
```

7. Run dotnet-cover on the .NET Framework build

```
dotnet-coverage collect "dotnet test --framework net472 --no-build -l console;verbosity=normal" --output net472_GCP_coverage.xml --output-format cobertura --settings coverage.config
```
