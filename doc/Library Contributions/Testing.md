# Testing the Connector

Before running tests, create a parameters.json file under Snowflake.Data.Tests\ directory. In this file, specify username, password and account info that tests will run against. Here is a sample parameters.json file

```
{
  "testconnection": {
    "SNOWFLAKE_TEST_USER": "snowman",
    "SNOWFLAKE_TEST_PASSWORD": "XXXXXXX",
    "SNOWFLAKE_TEST_ACCOUNT": "TESTACCOUNT",
    "SNOWFLAKE_TEST_WAREHOUSE": "TESTWH",
    "SNOWFLAKE_TEST_DATABASE": "TESTDB",
    "SNOWFLAKE_TEST_SCHEMA": "TESTSCHEMA",
    "SNOWFLAKE_TEST_ROLE": "TESTROLE",
    "SNOWFLAKE_TEST_HOST": "testaccount.snowflakecomputing.com"
  }
}
```

## Command Prompt

The build solution file builds the connector and tests binaries. Issue the following command from the command line to run the tests. The test binary is located in the Debug directory if you built the solution file in Debug mode.

```{r, engine='bash', code_block_name}
cd Snowflake.Data.Tests
dotnet test -f net6.0 -l "console;verbosity=normal"
```

Tests can also be run under code coverage:

```{r, engine='bash', code_block_name}
dotnet-coverage collect "dotnet test --framework net6.0 --no-build -l console;verbosity=normal" --output net6.0_coverage.xml --output-format cobertura --settings coverage.config
```

You can run only specific suite of tests (integration or unit).

Running unit tests:

```bash
cd Snowflake.Data.Tests
dotnet test -l "console;verbosity=normal" --filter FullyQualifiedName~UnitTests -l console;verbosity=normal
```

Running integration tests:

```bash
cd Snowflake.Data.Tests
dotnet test -l "console;verbosity=normal" --filter FullyQualifiedName~IntegrationTests
```

## Visual Studio 2017

Tests can also be run under Visual Studio 2017. Open the solution file in Visual Studio 2017 and run tests using Test Explorer.

