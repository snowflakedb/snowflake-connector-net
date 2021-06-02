
Snowflake Connector for .NET
============================

[![NuGet](https://img.shields.io/nuget/v/Snowflake.Data.svg)](https://www.nuget.org/packages/Snowflake.Data/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The Snowflake .NET connector supports most core functionality. Currently, the PUT and GET commands are not supported. All other query types are supported. 

Library target is under .NET Framework 4.7.2 and .NET Standard 2.0.

Please refer to the Notice section below for information about safe usage of the .NET Driver

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
i.e "\<key1\>=\<value1\>;\<key2\>=\<value2\>...".

Note: If the keyword or value contains an equal sign (=), you must precede the equal sign with another equal sign. For example, if the keyword is "key" and the value is "value_part1=value_part2", use "key=value_part1==value_part2".

The following table lists all valid connection properties:
<br />

| Connection Property        | Required | Comment                                                                       |
|----------------------------|----------|-------------------------------------------------------------------------------|
| ACCOUNT                    | Yes      | Account should not include region or cloud provider information. e.g. account should be XXX instead of XXX.us-east-1.|
| DB                         | No       |                                                                               |
| HOST                       | No       | If no value is specified, the driver uses \<ACCOUNT\>.snowflakecomputing.com. However, if you are not in us-west deployment, or you want to use global url, HOST is required, e.g. XXX.us-east-1.snowflakecomputing.com, or XXX-jkabfvdjisoa778wqfgeruishafeuw89q.global.snowflakecomputing.com|
| PASSWORD                   | Depends  | Required if AUTHENTICATOR is set to `snowflake` (the default value) or the URL for native SSO through Okta. Ignored for all the other authentication types.|
| ROLE                       | No       |                                                                               |
| SCHEMA                     | No       |                                                                               |
| USER                       | Yes      | If AUTHENTICATOR is set to `externalbrowser` or the URL for native SSO through Okta, set this to the login name for your identity provider (IdP).     |
| WAREHOUSE                  | No       |                                                                               |
| CONNECTION_TIMEOUT         | No       | Total timeout in seconds when connecting to Snowflake. Default to 120 seconds |
| AUTHENTICATOR              | No       | The method of authentication. Currently supports the following values: <br /> - snowflake (default): You must also set USER and PASSWORD. <br /> - [the URL for native SSO through Okta](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#native-sso-okta-only): You must also set USER and PASSWORD. <br /> - [externalbrowser](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#browser-based-sso): You must also set USER. <br /> - [snowflake_jwt](https://docs.snowflake.com/en/user-guide/key-pair-auth.html): You must also set PRIVATE_KEY_FILE or PRIVATE_KEY. <br /> - [oauth](https://docs.snowflake.com/en/user-guide/oauth.html): You must also set TOKEN.
| VALIDATE_DEFAULT_PARAMETERS| No       | Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default to be true. |
| PRIVATE_KEY_FILE           |Depends   | The path to the private key file to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt|
| PRIVATE_KEY_PWD            |No        | The passphrase to use for decrypting the private key, if the key is encrypted.|
| PRIVATE_KEY                |Depends   | The private key to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt. <br /> If the private key value includes any equal signs (=), make sure to replace each equal sign with two signs (==) to ensure that the connection string is parsed correctly.|
| TOKEN                      |Depends   | The OAuth token to use for OAuth authentication. Must be used in combination with AUTHENTICATOR=oauth.|
| INSECUREMODE               |No   	    | Set to true to disable the certificate revocation list check. Default is false.|


<br />

The following example demonstrates how to open a connection to Snowflake. This example uses a password for authentication.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema";

    conn.Open();
    
    conn.Close();
}
```

If you are using a different method for authentication, see the examples below:

* **Key-pair authentication**

  After setting up [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html), you can specify the
  private key for authentication in one of the following ways:

  * Specify the file containing an unencrypted private key:

    ```cs
    using (IDbConnection conn = new SnowflakeDbConnection())
    {
        conn.ConnectionString = "account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key_file={pathToThePrivateKeyFile};db=testdb;schema=testschema";

        conn.Open();
    
        conn.Close();
    }
    ```

    where:

    * `{pathToThePrivateKeyFile}` is the path to the file containing the unencrypted private key.

  * Specify the file containing an encrypted private key:

    ```cs
    using (IDbConnection conn = new SnowflakeDbConnection())
    {
        conn.ConnectionString = "account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key_file={pathToThePrivateKeyFile};private_key_pwd={passwordForDecryptingThePrivateKey};db=testdb;schema=testschema";

        conn.Open();
    
        conn.Close();
    }
    ```

    where:

    * `{pathToThePrivateKeyFile}` is the path to the file containing the unencrypted private key.
    * `{passwordForDecryptingThePrivateKey}` is the password for decrypting the private key.

  * Specify an unencrypted private key (read from a file):

    ```cs
    using (IDbConnection conn = new SnowflakeDbConnection())
    {
        string privateKeyContent = File.ReadAllText({pathToThePrivateKeyFile}).Replace("=", "==");

        conn.ConnectionString = String.Format("account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key={0};db=testdb;schema=testschema", privateKeyContent);

        conn.Open();
    
        conn.Close();
    }
    ```

    where:

    * `{pathToThePrivateKeyFile}` is the path to the file containing the unencrypted private key.

* **OAuth**

  After setting up [OAuth](https://docs.snowflake.com/en/user-guide/oauth.html), set `AUTHENTICATOR=oauth` and `TOKEN` to the
  OAuth token in the connection string.

  ```cs
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;user=testuser;authenticator=oauth;token={oauthTokenValue};db=testdb;schema=testschema";

      conn.Open();
    
      conn.Close();
  }
  ```

  where:

  * `{oauthTokenValue}` is the oauth token to use for authentication.

* **Browser-based SSO**

  In the connection string, set `AUTHENTICATOR=externalbrowser`, and set `USER` to the login name for your IdP.

  ```cs
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;authenticator=externalbrowser;user={login_name_for_IdP};db=testdb;schema=testschema";

      conn.Open();

      conn.Close();
  }
  ```

  where:

  * `{login_name_for_IdP}` is your login name for your IdP.


* **Native SSO through Okta**

  In the connection string, set `AUTHENTICATOR` to the
  [URL of the endpoint for your Okta account](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#label-native-sso-okta),
  and set `USER` to the login name for your IdP.

  ```cs
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;authenticator={okta_url_endpoint};user={login_name_for_IdP};db=testdb;schema=testschema";

      conn.Open();

      conn.Close();
  }
  ```

  where:

  * `{okta_url_endpoint}` is the URL for the endpoint for your Okta account (e.g. `https://<okta_account_name>.okta.com`).
  * `{login_name_for_IdP}` is your login name for your IdP.


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

This example shows how bound parameters are converted from C# data types to
Snowflake data types. For example, if the data type of the Snowflake column
is INTEGER, then you can bind C# data types Int32 or Int16.

This example inserts 3 rows into a table with one column.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = connectionString;
    conn.Open();

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "create or replace table T(cola int)";
    int count = cmd.ExecuteNonQuery();
    Assert.AreEqual(0, count);

    IDbCommand cmd = conn.CreateCommand();
    cmd.CommandText = "insert into t values (?), (?), (?)";

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

    cmd.CommandText = "drop table if exists T";
    count = cmd.ExecuteNonQuery();
    Assert.AreEqual(0, count);

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
1. CVE-2019-0820 -  
This CVE has been reported in systems.text.regularexpressions.dll which is used by the regular expressions packages - systems.text.regularexpressions.4.3.1.nupkg. This vulnerability manifests itself ONLY when the following .NET runtime environments are being used: 

* v1.0 branch: 1.0 - 1.0.16 (exclusive)
* v1.1 branch: 1.1 - 1.1.13 (exclusive)
* v2.1 branch: 2.1 - 2.1.11 (exclusive)
* v2.2 branch: 2.2 - 2.2.5  (exclusive)

	In order to mitigate this vulnerability, we recommend to update to higher Runtime versions. If you're already running on a .NET Runtime version higher than the ones listed above, you're not going to be affected by this vulnerability. 

2. Logging -  
	Snowflake has identified an issue on Feb 20, 2020, with our logging code for the .NET drivers in which we write Master and Session tokens in the clear to the debug logs. The debug logs are collected locally on the drive where your programs are running. This issue impacts only those instances where the programs are run with debug flags enabled, i.e. setting the log level value= "Debug” or “All" in the log4Net config

	Under normal conditions, the Master and Session tokens captured in the log files are short-lived for about 4 and 1 hours, respectively. They will expire after the 4-hour window unless explicitly refreshed, in which case they could be refreshed indefinitely.

	If you are using the .NET driver please take the following action:
* Upgrade to the latest version(v1.1.0) as soon as possible.
* Remove all “Debugging” options for any existing .NET drivers in use.
* Delete any logs collected thus far and make sure that all copies are deleted. 
* If you cannot upgrade for any reason, please ensure all debugging is disabled
* If you are concerned about a potential compromise, contact Snowflake Customer Support for assistance with invalidating all active sessions/tokens. 

3. Global HTTP connection settings -  
	Snowflake has identified an issue where the driver is globally enforcing TLS 1.2 and certificate revocation checks with the .NET Driver v1.2.1 and earlier versions.  
	Starting with v2.0.0, the driver will set these locally.  
	
	Note that the driver is now targeting .Net framework 4.7.2.  
	When upgrading to v2.0.0, you might also need to run "Update-Package -reinstall" to update the dependencies.
