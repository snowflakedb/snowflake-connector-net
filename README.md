Snowflake Connector for .NET
============================

[![Build status](https://ci.appveyor.com/api/projects/status/2gx5agsb7i3m5ije/branch/master?svg=true)](https://ci.appveyor.com/project/howryu/snowflake-connector-net/branch/master)
[![codecov](https://codecov.io/gh/snowflakedb/snowflake-connector-net/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-connector-net)
[![NuGet](https://img.shields.io/nuget/v/Snowflake.Data.svg)](https://www.nuget.org/packages/Snowflake.Data/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The Snowflake .NET connector supports most core functionality. Currently, the PUT and GET commands are not supported. All other query types are supported. 

Library target is under .NET Framework 4.6 and .NET Standard 2.0.


Building the Package
====================

Prerequisites
-------------

This project is developed under Visual Studio 2017. All other versions of Visual Studio are not supported.

Steps
-----

1. Check out the source code from GitHub:
```{r, engine='bash', code_block_name}
git clone git@github.com:snowflakedb/snowflake-connector-net snowflake-connector-net
```

2. Pull down the dependency:
```{r, engine='bash', code_block_name}
cd snowflake-connector-net
nuget restore
```

3. Build the solution file 
```{r, engine='bash', code_block_name}
msbuild snowflake-connector-net.sln /p:Configuration=Release
```

Installing the Package
======================

Package ID for Snowflake Connector for .Net is Snowflake.Data. 

Packages can be directly downloaded from [nuget.org](https://www.nuget.org/). 

It can also be downloaded using Visual Studio UI (Tools > NuGet Package Manager > Manage NuGet Packages for Solution and search for "Snowflake.Data")

Alternatively, packages can also be downloaded using Package Manager Console:
```{r, engine='bash', code_block_name}
PM> Install-Package Snowflake.Data
```

Testing the Connector
=====================

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

Command Prompt
--------------
The build solution file builds the connector and tests binaries. Issue the following command from the command line to run the tests. The test binary is located in the Debug directory if you built the solution file in Debug mode. 

```{r, engine='bash', code_block_name}
cd Snowflake.Data.Tests
dotnet test -f netcoreapp2.0
```


Tests can also be run under code coverage:

```{r, engine='bash', code_block_name}
OpenCover.4.6.519\tools\OpenCover.Console.exe -target:"dotnet.exe" -returntargetcode -targetargs:"test -f netcoreapp2.0" -register:user -filter:"+[Snowflake.Data]*" -output:"netcoreapp2.0_coverage.xml" -oldStyle 
```

Visual Studio 2017
------------------
Tests can also be run under Visual Studio 2017. Open the solution file in Visual Studio 2017 and run tests using Test Explorer.


Usage
=====

Create a Connection
-------------------

To connect to Snowflake, specify a valid connection string composed of key-value pairs separated by semicolons, 
i.e "\<key1\>=\<value1\>;\<key2\>=\<value2\>...". The following table lists all valid connection properties:

<br />

| Connection Property | Required | Comment                                                                       |
|---------------------|----------|-------------------------------------------------------------------------------|
| ACCOUNT             | Yes      | Account should not include region or clound provider information. i.e. account should be XXX instead of XXX.us-east-1.|
| DB                  | No       |                                                                               |
| HOST                | No       | If no value specified, driver will use \<ACCOUNT\>.snowflakecomputing.com. However, if you are not in us-west deployment, or you want to use global url, HOST is required, i.e. XXX.us-east-1.snowflakecomputing.com, or XXX-jkabfvdjisoa778wqfgeruishafeuw89q.global.snowflakecomputing.com|
| PASSWORD            | Depends  | Ignored for external browser, required for other authentication methods.      |
| ROLE                | No       |                                                                               |
| SCHEMA              | No       |                                                                               |
| USER                | Yes      | For okta and externalbrowser, this should be the login name for your idp.     |
| WAREHOUSE           | No       |                                                                               |
| CONNECTION_TIMEOUT  | No       | Total timeout in seconds when connecting to Snowflake. Default to 120 seconds |
| AUTHENTICATOR       | No       | The method of authentication. Currently support snowflake(default), [native SSO okta](https://docs.snowflake.net/manuals/user-guide/admin-security-fed-auth-use.html#native-sso-okta-only) and [externalbrowser](https://docs.snowflake.net/manuals/user-guide/admin-security-fed-auth-use.html#browser-based-sso).  |
|VALIDATE_DEFAULT_PARAMETERS| No       | Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default to be true. |

<br />

Sample code to open a connection to Snowflake:

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema";

    conn.Open();
    
    conn.Close();
}
```

Run a Query and Read Data
-------------------------

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "select * from t";
    IDataReader reader = cmd.ExecuteReader();
                
    while(reader.Read())
    {
        Console.WriteLine(reader.GetString(0));
    }
    
    conn.Close();
}
```

Bind Parameter
--------------

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "insert into t values (?),(?),(?)";
                  
    var p1 = cmd.CreateParameter();
    p1.ParameterName = "1";
    p1.Value = 10;
    p1.DbType = DbType.Int32;
    cmd.Parameters.Add(p1);

    var p2 = cmd.CreateParameter();
    p2.ParameterName = "2";
    p2.Value = 10000L;
    p2.DbType = DbType.Int32;
    cmd.Parameters.Add(p2);

    var p3 = cmd.CreateParameter();
    p3.ParameterName = "3";
    p3.Value = (short)1;
    p3.DbType = DbType.Int16;
    cmd.Parameters.Add(p3);

    var count = cmd.ExecuteNonQuery();
    Assert.AreEqual(3, count);             
    
    conn.Close();
}
```

Logging
-------
The Snowflake Connector for .NET uses [log4net](http://logging.apache.org/log4net/) as the logging framework.

Here is a sample app.config file that uses [log4net](http://logging.apache.org/log4net/)

```xml
  <configSections>
    <section name="log4net" type="log4net.Config.Log4NetConfigurationSectionHandler, log4net"/>
  </configSections>
  
  <log4net>
    <appender name="MyRollingFileAppender" type="log4net.Appender.RollingFileAppender">
      <file value="snowflake_dotnet.log" />
      <appendToFile value="true"/>
      <rollingStyle value="Size" />
      <maximumFileSize value="10MB" />
      <staticLogFileName value="true" />
      <maxSizeRollBackups value="10" />
      <layout type="log4net.Layout.PatternLayout">
        <!-- <header value="[DateTime]  [Thread]  [Level]  [ClassName] Message&#13;&#10;" /> -->
        <conversionPattern value="[%date] [%t] [%-5level] [%logger] %message%newline" />
      </layout>
    </appender>

    <root>
      <level value="ALL" />
      <appender-ref ref="MyRollingFileAppender" />
    </root>
  </log4net>
```

Notice
----------------
Snowflake has identified an issue on Feb 20, 2020, with our logging code for the .NET drivers in which we write Master and Session tokens in the clear to the debug logs. The debug logs are collected locally on the drive where your programs are running. This issue impacts only those instances where the programs are run with debug flags enabled, i.e. setting the log level value= "Debug” or “All" in the log4Net config

Under normal conditions, the Master and Session tokens captured in the log files are short-lived for about 4 and 1 hours, respectively. They will expire after the 4-hour window unless explicitly refreshed, in which case they could be refreshed indefinitely.

If you are using the .NET driver please take the following action:
1. Upgrade to the latest version(v1.1.0) as soon as possible.
2. Remove all “Debugging” options for any existing .NET drivers in use.
3. Delete any logs collected thus far and make sure that all copies are deleted. 
4. If you cannot upgrade for any reason, please ensure all debugging is disabled
5. If you are concerned about a potential compromise, contact Snowflake Customer Support for assistance with invalidating all active sessions/tokens. 

