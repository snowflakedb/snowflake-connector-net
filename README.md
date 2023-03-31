
Snowflake Connector for .NET
============================

[![NuGet](https://img.shields.io/nuget/v/Snowflake.Data.svg)](https://www.nuget.org/packages/Snowflake.Data/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The Snowflake .NET connector supports the the following .NET framework and libraries versions:

- .NET Framework 4.7.1
- .NET Framework 4.7.2
- .NET Framework 4.7.3
- .NET 6.0

Please refer to the Notice section below for information about safe usage of the .NET Driver

Building the Package
====================

Prerequisites
-------------

The Snowflake .NET connector supports only Windows.

This project is developed under Visual Studio 2017. Earlier versions of Visual Studio are not supported.

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
dotnet test -f netcoreapp6.0
```


Tests can also be run under code coverage:

```{r, engine='bash', code_block_name}
OpenCover.4.6.519\tools\OpenCover.Console.exe -target:"dotnet.exe" -returntargetcode -targetargs:"test -f netcoreapp6.0" -register:user -filter:"+[Snowflake.Data]*" -output:"netcoreapp6.0_coverage.xml" -oldStyle 
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

**Note**: If the keyword or value contains an equal sign (=), you must precede the equal sign with another equal sign. For example, if the keyword is "key" and the value is "value_part1=value_part2", use "key=value_part1==value_part2".

The following table lists all valid connection properties:
<br />

| Connection Property        | Required | Comment                                                                       |
|----------------------------|----------|-------------------------------------------------------------------------------|
| ACCOUNT                    | Yes      | Your full account name might include additional segments that identify the region and cloud platform where your account is hosted	|
| APPLICATION                | No       | **_Snowflake partner use only_**: Specifies the name of a partner application to connect through .NET. The name must match the following pattern:  ^\[A-Za-z](\[A-Za-z0-9.-]){1,50}$ (one letter followed by 1 to 50 letter, digit, .,- or, \_ characters).   |
| DB                         | No       |                                                                               |
| HOST                       | No       | Specifies the hostname for your account in the following format: \<ACCOUNT\>.snowflakecomputing.com. <br /> If no value is specified, the driver uses \<ACCOUNT\>.snowflakecomputing.com. |
| PASSWORD                   | Depends  | Required if AUTHENTICATOR is set to `snowflake` (the default value) or the URL for native SSO through Okta. Ignored for all the other authentication types.|
| ROLE                       | No       |                                                                               |
| SCHEMA                     | No       |                                                                               |
| USER                       | Yes      | If AUTHENTICATOR is set to `externalbrowser` or the URL for native SSO through Okta, set this to the login name for your identity provider (IdP).     |
| WAREHOUSE                  | No       |                                                                               |
| CONNECTION_TIMEOUT         | No       | Total timeout in seconds when connecting to Snowflake. The default is 120 seconds |
| CLIENT_SESSION_KEEP_ALIVE  | No       | Whether to keep the current session active after a period of inactivity, or to force the user to login again. If the value is `true`, Snowflake keeps the session active indefinitely, even if there is no activity from the user. If the value is `false`, the user must log in again after four hours of inactivity. The default is `false`. Setting this value overrides the server session property for the current session.|
| DISABLERETRY               | No       | Set this property to `true` to prevent the driver from reconnecting automatically when the connection fails or drops. The default value is `false`. |
| AUTHENTICATOR              | No       | The method of authentication. Currently supports the following values: <br /> - snowflake (default): You must also set USER and PASSWORD. <br /> - [the URL for native SSO through Okta](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#native-sso-okta-only): You must also set USER and PASSWORD. <br /> - [externalbrowser](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#browser-based-sso): You must also set USER. <br /> - [snowflake_jwt](https://docs.snowflake.com/en/user-guide/key-pair-auth.html): You must also set PRIVATE_KEY_FILE or PRIVATE_KEY. <br /> - [oauth](https://docs.snowflake.com/en/user-guide/oauth.html): You must also set TOKEN.
| VALIDATE_DEFAULT_PARAMETERS| No       | Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default to be true. |
| PRIVATE_KEY_FILE           |Depends   | The path to the private key file to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt|
| PRIVATE_KEY_PWD            |No        | The passphrase to use for decrypting the private key, if the key is encrypted.|
| PRIVATE_KEY                |Depends   | The private key to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt. <br /> If the private key value includes any equal signs (=), make sure to replace each equal sign with two signs (==) to ensure that the connection string is parsed correctly.|
| TOKEN                      |Depends   | The OAuth token to use for OAuth authentication. Must be used in combination with AUTHENTICATOR=oauth.|
| INSECUREMODE               |No   	| Set to true to disable the certificate revocation list check. Default is false.|
| USEPROXY                   | No       | Set to true if you need to use a proxy server. The default value is false. <br/> <br/> This parameter was introduced in v2.0.4. |
| PROXYHOST                  | Depends  | The hostname of the proxy server. <br/> <br/> If USEPROXY is set to `true`, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4. |
| PROXYPORT                  | Depends  | The port number of the proxy server. <br/> <br/> If USEPROXY is set to `true`, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4. |
| PROXYUSER                  | No       | The username for authenticating to the proxy server. <br/> <br/> This parameter was introduced in v2.0.4. |
| PROXYPASSWORD              | Depends  | The password for authenticating to the proxy server. <br/> <br/> If USEPROXY is `true` and PROXYUSER is set, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4. |
| NONPROXYHOSTS              | No       | The list of hosts that the driver should connect to directly, bypassing the proxy server. Separate the hostnames with a pipe symbol (\|). You can also use an asterisk (`*`) as a wildcard. <br/> <br/> This parameter was introduced in v2.0.4. |

<br />

### Password-based Authentication

The following example demonstrates how to open a connection to Snowflake. This example uses a password for authentication.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema";

    conn.Open();
    
    conn.Close();
}
```

<a id="sample-connection-strings"></a>

Beginning with version 2.0.18, the .NET connector uses Microsoft [DbConnectionStringBuilder](https://learn.microsoft.com/en-us/dotnet/api/system.data.oledb.oledbconnection.connectionstring?view=dotnet-plat-ext-6.0#remarks) to follow the .NET specification for escaping characters in connection strings. 

The following examples show how you can include different types of special characters in a connection string:

- To include a single quote (') character:

  ``` cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=test'password;"
  );
  ```

- To include a double quote (") character:

  ``` cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=test\"password;"
  );
  ```

- To include a semicolon (;):

  ``` cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=\"test;password\";"
  );
  ```

- To include an equal sign (=):

  ``` cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=test=password;"
  );
  ```

  Note that previously you needed to use a double equal sign (==) to escape the character. However, beginning with version 2.0.18, you can use a single equal size.

### Other Authentication Methods 

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
        string privateKeyContent = File.ReadAllText({pathToThePrivateKeyFile});

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

In v2.0.4 and later releases, you can configure the driver to connect through a proxy server. The following example configures the
driver to connect through the proxy server `myproxyserver` on port `8888`. The driver authenticates to the proxy server as the
user `test` with the password `test`:

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema;useProxy=true;proxyHost=myproxyserver;proxyPort=8888;proxyUser=test;proxyPassword=test";

    conn.Open();
    
    conn.Close();
}
```

Using Connection Pools
----------------------

Instead of creating a connection each time your client application needs to access Snowflake, you can define a cache of Snowflake connections that can be reused as needed. Connection pooling usually reduces the lag time to make a connection. However, it can slow down client failover to an alternative DNS when a DNS problem occurs.

The Snowflake .NET driver provides the following functions for managing connection pools.

| Function | Description |
|----------|--------------|
| SnowflakeDbConnectionPool.ClearAllPools() | Removes all connections from the connection pool. |
| SnowflakeDbConnection.SetMaxPoolSize(n) | Sets the maximum number of connections for the connection pool, where _n_ is the number of connections. |
| SnowflakeDBConnection.SetTimeout(n) | Sets the number of seconds to keep an unresponsive connection in the connection pool.|
| SnowflakeDbConnectionPool.GetCurrentPoolSize() | Returns the number of connections currently in the connection pool. |
| SnowflakeDbConnectionPool.SetPooling() | Determines whether to enable (`true`) or disable (`false`) connecing pooling. Default: `true`.|

The following sample demonstrates how to monitor the size of a connection pool as connections are added and dropped from the pool.

```cs
public void TestConnectionPoolClean()
{
  SnowflakeDbConnectionPool.ClearAllPools();
  SnowflakeDbConnectionPool.SetMaxPoolSize(2);
  var conn1 = new SnowflakeDbConnection();
  conn1.ConnectionString = ConnectionString;
  conn1.Open();
  Assert.AreEqual(ConnectionState.Open, conn1.State);

  var conn2 = new SnowflakeDbConnection();
  conn2.ConnectionString = ConnectionString + " retryCount=1";
  conn2.Open();
  Assert.AreEqual(ConnectionState.Open, conn2.State);
  Assert.AreEqual(0, SnowflakeDbConnectionPool.GetCurrentPoolSize());
  conn1.Close();
  conn2.Close();
  Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
  var conn3 = new SnowflakeDbConnection();
  conn3.ConnectionString = ConnectionString + "  retryCount=2";
  conn3.Open();
  Assert.AreEqual(ConnectionState.Open, conn3.State);

  var conn4 = new SnowflakeDbConnection();
  conn4.ConnectionString = ConnectionString + "  retryCount=3";
  conn4.Open();
  Assert.AreEqual(ConnectionState.Open, conn4.State);

  conn3.Close();
  Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());
  conn4.Close();
  Assert.AreEqual(2, SnowflakeDbConnectionPool.GetCurrentPoolSize());

  Assert.AreEqual(ConnectionState.Closed, conn1.State);
  Assert.AreEqual(ConnectionState.Closed, conn2.State);
  Assert.AreEqual(ConnectionState.Closed, conn3.State);
  Assert.AreEqual(ConnectionState.Closed, conn4.State);
}
```

Mapping .NET and Snowflake Data Types
-------------------------------------

The .NET driver supports the following mappings from .NET to Snowflake data types.


| .NET Framekwork Data Type | Data Type in Snowflake |
| ------------------------------ | ---------------------- |
| `int`, `long`                 | `NUMBER(38, 0)`        |
| `decimal`                       | `NUMBER(38, <scale>)`  |
| `double` | `REAL` |
| `string` | `TEXT` |
| `bool` | `BOOLEAN` |
| `byte` | `BINARY` |
| `datetime` | `DATE` |


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

Note that for a `TIME` column, the reader returns a `System.DateTime` value. If you need a `System.TimeSpan` column, call the
`getTimeSpan` method in `SnowflakeDbDataReader`. This method was introduced in the v2.0.4 release.

Note that because this method is not available in the generic `IDataReader` interface, you must cast the object as
`SnowflakeDbDataReader` before calling the method. For example:

```cs
TimeSpan timeSpanTime = ((SnowflakeDbDataReader)reader).GetTimeSpan(13);
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

Bind Array Variables
--------------------

The sample code creates a table with a single integer column and then uses array binding to populate the table with values 0 to 70000.

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
	conn.ConnectionString = ConnectionString;
	conn.Open();

	using (IDbCommand cmd = conn.CreateCommand())
	{
		cmd.CommandText = "create or replace table putArrayBind(colA integer)";
		cmd.ExecuteNonQuery();

		string insertCommand = "insert into putArrayBind values (?)";
		cmd.CommandText = insertCommand;

		int total = 70000;

		List<int> arrint = new List<int>();
		for (int i = 0; i < total; i++)
		{
			arrint.Add(i);
		}
		var p1 = cmd.CreateParameter();
		p1.ParameterName = "1";
		p1.DbType = DbType.Int16;
		p1.Value = arrint.ToArray();
		cmd.Parameters.Add(p1);

		count = cmd.ExecuteNonQuery(); // count = 70000
	}

	conn.Close();
}
```

Close the Connection
--------------------

To close the connection, call the `Close` method of `SnowflakeDbConnection`.

If you want to avoid blocking threads while the connection is closing, call the `CloseAsync` method instead, passing in a
`CancellationToken`. This method was introduced in the v2.0.4 release.

Note that because this method is not available in the generic `IDbConnection` interface, you must cast the object as
`SnowflakeDbConnection` before calling the method. For example:

```cs
CancellationTokenSource cancellationTokenSource  = new CancellationTokenSource();
// Close the connection
((SnowflakeDbConnection)conn).CloseAsync(cancellationTokenSource.Token);
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

Getting the code coverage
----------------

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

5. Run OpenCover on the .NET6 build
```
OpenCover.Console.exe -target:"C:\Program Files\dotnet\dotnet.exe" -returntargetcode -targetargs:"test -f net6.0 -v n" -register:user -filter:"+[Snowflake.Data]*" -output:"net6.0_AWS_coverage.xml" -oldStyle
```

6. Build the project for .NET Framework
```
msbuild snowflake-connector-net.sln -p:Configuration=Release
```

7. Run OpenCover on the .NET Framework build
```
OpenCover.Console.exe -target:"C:\Program Files\dotnet\dotnet.exe" -returntargetcode -targetargs:"test -f net472 -v n" -register:user -filter:"+[Snowflake.Data]*" -output:"net472_AWS_coverage.xml" -oldStyle
```

<br />
Repeat steps 3, 5, and 7 for the other cloud providers. <br />
Note: no need to rebuild the connector again. <br /><br />

For Azure:<br />

3. Create parameters.json containing connection info for AZURE account and place inside the Snowflake.Data.Tests folder

5. Run OpenCover on the .NET6 build
```
OpenCover.Console.exe -target:"C:\Program Files\dotnet\dotnet.exe" -returntargetcode -targetargs:"test -f net6.0 -v n" -register:user -filter:"+[Snowflake.Data]*" -output:"net6.0_AZURE_coverage.xml" -oldStyle
```

7. Run OpenCover on the .NET Framework build
```
OpenCover.Console.exe -target:"C:\Program Files\dotnet\dotnet.exe" -returntargetcode -targetargs:"test -f net472 -v n" -register:user -filter:"+[Snowflake.Data]*" -output:"net472_AZURE_coverage.xml" -oldStyle
```

<br />
For GCP:<br />

3. Create parameters.json containing connection info for GCP account and place inside the Snowflake.Data.Tests folder

5. Run OpenCover on the .NET6 build
```
OpenCover.Console.exe -target:"C:\Program Files\dotnet\dotnet.exe" -returntargetcode -targetargs:"test -f net6.0 -v n" -register:user -filter:"+[Snowflake.Data]*" -output:"net6.0_GCP_coverage.xml" -oldStyle
```

7. Run OpenCover on the .NET Framework build
```
OpenCover.Console.exe -target:"C:\Program Files\dotnet\dotnet.exe" -returntargetcode -targetargs:"test -f net472 -v n" -register:user -filter:"+[Snowflake.Data]*" -output:"net472_GCP_coverage.xml" -oldStyle
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
	
  Note that the driver is now targeting .NET 6.0. When upgrading, you might also need to run “Update-Package -reinstall” to update the dependencies.
