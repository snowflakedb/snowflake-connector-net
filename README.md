Snowflake Connector for .NET
============================

[![Build status](https://ci.appveyor.com/api/projects/status/2gx5agsb7i3m5ije/branch/master?svg=true)](https://ci.appveyor.com/project/howryu/snowflake-connector-net/branch/master)
[![codecov](https://codecov.io/gh/snowflakedb/snowflake-connector-net/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-connector-net)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Build
=====
Prerequisites
-------------
This project is developed under Visual Studio 2015. All other version of visual studio is not supported.

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

Run Tests from Command Prompt
-----------------------------
Build Solution file will both build connector binary and tests binary. Issue the following command from command line will run the tests. The test binary will be under Debug directory if building the solution file in Debug mode. 

```{r, engine='bash', code_block_name}
.\packages\NUnit.ConsoleRunner.3.6.1\tools\nunit3-console.exe .\Snowflake.Data.Tests\bin\Release\Snowflake.Data.Tests.dll
```


Tests can also be run under code coverage:

```{r, engine='bash', code_block_name}
.\packages\OpenCover.4.6.519\tools\OpenCover.Console.exe -target:".\packages\NUnit.ConsoleRunner.3.6.1\tools\nunit3-console.exe" -returntargetcode -targetargs:".\Snowflake.Data.Tests\bin\Release\Snowflake.Data.Tests.dll" -register:user -filter:"+[Snowflake.Data]*" -output:"coverage.xml"  
```

Run Tests from Visual Studio 2015
---------------------------------
Test can also be run under Visual Studio 2015. Open the solution file in VS2015 and run tests under Test Explorer.


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
Snowflake .Net Driver use [Common.Logging](https://github.com/net-commons/common-logging) as logging framework. The driver package only includes facade, which means application should provide actaul logging implementation package. 


Here is a sample app.config which use [log4net](http://logging.apache.org/log4net/) as actual logging implementation.
```xml
  <configSections>
    <sectionGroup name="common">
      <section name="logging" type="Common.Logging.ConfigurationSectionHandler, Common.Logging" />
    </sectionGroup>
    <section name="log4net" type="log4net.Config.Log4NetConfigurationSectionHandler, log4net"/>
  </configSections>
  <common>
    <logging>
      <factoryAdapter type="Common.Logging.Log4Net.Log4NetLoggerFactoryAdapter, Common.Logging.Log4Net1215">
        <arg key="configType" value="INLINE" />
      </factoryAdapter>
    </logging>
  </common>
  
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

