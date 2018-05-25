Snowflake Connector for .NET
============================

[![Build status](https://ci.appveyor.com/api/projects/status/2gx5agsb7i3m5ije/branch/master?svg=true)](https://ci.appveyor.com/project/howryu/snowflake-connector-net/branch/master)
[![codecov](https://codecov.io/gh/snowflakedb/snowflake-connector-net/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-connector-net)
[![NuGet](https://img.shields.io/nuget/v/Snowflake.Data.svg)](https://www.nuget.org/packages/Snowflake.Data/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This is package for Snowflake .NET connector. Currently PUT/GET commands are not supported. All other query types are supported. 

Library target is under .NET Framework 4.6 and .NET Standard 2.0.

Build
=====
Prerequisites
-------------
This project is developed under Visual Studio 2017. All other version of visual studio is not supported.

Steps
-----
1. Checkout source code from Github:
```{r, engine='bash', code_block_name}
git clone git@github.com:snowflakedb/snowflake-connector-net snowflake-connector-net
```

2. Pulldown dependency:
```{r, engine='bash', code_block_name}
cd snowflake-connector-net
nuget restore
```

3. Build the solution file 
```{r, engine='bash', code_block_name}
msbuild snowflake-connector-net.sln /p:Configuration=Release
```

Install
=======
Package ID for Snowflake Connector for .Net is Snowflake.Data. 

Packages can be directly downloaded from [nuget.org](https://www.nuget.org/). 

It can also be downloaded using Visual Studio UI (Tools > NuGet Package Manager > Manage NuGet Packages for Solution and search for "Snowflake.Data")

Alternatively, packages can also be downloaded using Package Manager Console:
```{r, engine='bash', code_block_name}
PM> Install-Package Snowflake.Data
```

Test
====
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
    "SNOWFLAKE_TEST_ROLE": "TESTROLE"
  }
}
```

Run Tests from Command Prompt
-----------------------------
Build Solution file will both build connector binary and tests binary. Issue the following command from command line will run the tests. The test binary will be under Debug directory if building the solution file in Debug mode. 

```{r, engine='bash', code_block_name}
cd Snowflake.Data.Tests
dotnet test -f netcoreapp2.0
```


Tests can also be run under code coverage:

```{r, engine='bash', code_block_name}
OpenCover.4.6.519\tools\OpenCover.Console.exe -target:"dotnet.exe" -returntargetcode -targetargs:"test -f netcoreapp2.0" -register:user -filter:"+[Snowflake.Data]*" -output:"netcoreapp2.0_coverage.xml" -oldStyle 
```

Run Tests from Visual Studio 2017
---------------------------------
Test can also be run under Visual Studio 2017. Open the solution file in VS2017 and run tests under Test Explorer.


Usage
=====

Create Connection
-----------------

To connect to Snowflake, specify a valid connection string, which is key value pairs seperated by semi colon, 
i.e in the format of "\<key1\>=\<value1\>;\<key2\>=\<value2\>...". Valid connection property can be found in 
the following table.

<br />

| Connection Property | Required | Comment                                                                       |
|---------------------|----------|-------------------------------------------------------------------------------|
| ACCOUNT             | Yes      |                                                                               |
| DB                  | No       |                                                                               |
| HOST                | No       | If no value specified, driver will use \<ACCOUNT\>.snowflakecomputing.com     |
| PASSWORD            | Yes      |                                                                               |
| ROLE                | No       |                                                                               |
| SCHEMA              | No       |                                                                               |
| USER                | Yes      |                                                                               |
| WAREHOUSE           | No       |                                                                               |
| CONNECTION_TIMEOUT  | No       | Total timeout in seconds when connecting to Snowflake. Default to 120 seconds |

<br />

Sample code to open a connection to Snowflake:
```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema"

    conn.Open();
    
    conn.Close();
}
```

Run a query and Read data
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

Bind paramter
-------------
```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "insert into t values (?),(?),(?)";
    IDataReader reader = cmd.ExecuteReader();
                  
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
Snowflake .Net Driver use [log4net](http://logging.apache.org/log4net/) as logging framework.


Here is a sample app.config which use [log4net](http://logging.apache.org/log4net/)
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

