## Connecting

To connect to Snowflake, specify a valid connection string composed of key-value pairs separated by semicolons,
i.e "\<key1\>=\<value1\>;\<key2\>=\<value2\>...".

**Note**: If the value specified in the connection string contains any signs like semicolon (`;`) or equal sign (`=`) or any phrases which can interfere with parsing the connection string,
please surround the value with double quotation marks (`""`). For example `password="=;;;=dummy==password;;"`.

The following table lists all valid connection properties:
<br />

| Connection Property            | Required | Comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|--------------------------------| -------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ACCOUNT                        | Yes      | Your full account name might include additional segments that identify the region and cloud platform where your account is hosted                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| APPLICATION                    | No       | **_Snowflake partner use only_**: Specifies the name of a partner application to connect through .NET. The name must match the following pattern: ^\[A-Za-z](\[A-Za-z0-9.-]){1,50}$ (one letter followed by 1 to 50 letter, digit, .,- or, \_ characters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| DB                             | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| HOST                           | No       | Specifies the hostname for your account in the following format: \<ACCOUNT\>.snowflakecomputing.com. <br /> If no value is specified, the driver uses \<ACCOUNT\>.snowflakecomputing.com.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| PASSWORD                       | Depends  | Required if AUTHENTICATOR is set to `snowflake` (the default value) or the URL for native SSO through Okta. Ignored for all the other authentication types.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ROLE                           | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| SCHEMA                         | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| USER                           | Depends  | If AUTHENTICATOR is set to `externalbrowser` this is optional. For native SSO through Okta, set this to the login name for your identity provider (IdP).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| WAREHOUSE                      | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| CONNECTION_TIMEOUT             | No       | Total timeout in seconds when connecting to Snowflake. The default is 300 seconds                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| RETRY_TIMEOUT                  | No       | Total timeout in seconds for supported endpoints of retry policy. The default is 300 seconds. The value can only be increased from the default value or set to 0 for infinite timeout                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| MAXHTTPRETRIES                 | No       | Maximum number of times to retry failed HTTP requests (default: 7). You can set `MAXHTTPRETRIES=0` to remove the retry limit, but doing so runs the risk of the .NET driver infinitely retrying failed HTTP calls.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| CLIENT_SESSION_KEEP_ALIVE      | No       | Whether to keep the current session active after a period of inactivity, or to force the user to login again. If the value is `true`, Snowflake keeps the session active indefinitely, even if there is no activity from the user. If the value is `false`, the user must log in again after four hours of inactivity. The default is `false`. Setting this value overrides the server session property for the current session.                                                                                                                                                                                                                                                                                          |
| BROWSER_RESPONSE_TIMEOUT       | No       | Number to seconds to wait for authentication in an external browser (default: 120).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| DISABLERETRY                   | No       | Set this property to `true` to prevent the driver from reconnecting automatically when the connection fails or drops. The default value is `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| AUTHENTICATOR                  | No       | The method of authentication. Currently supports the following values: <br /> - snowflake (default): You must also set USER and PASSWORD. <br /> - [the URL for native SSO through Okta](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#native-sso-okta-only): You must also set USER and PASSWORD. <br /> - [externalbrowser](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#browser-based-sso): You must also set USER. <br /> - [snowflake_jwt](https://docs.snowflake.com/en/user-guide/key-pair-auth.html): You must also set PRIVATE_KEY_FILE or PRIVATE_KEY. <br /> - [oauth](https://docs.snowflake.com/en/user-guide/oauth.html): You must also set TOKEN. |
| VALIDATE_DEFAULT_PARAMETERS    | No       | Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default to be true.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PRIVATE_KEY_FILE               | Depends  | The path to the private key file to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PRIVATE_KEY_PWD                | No       | The passphrase to use for decrypting the private key, if the key is encrypted.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| PRIVATE_KEY                    | Depends  | The private key to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt. <br /> If the private key value includes any equal signs (=), make sure to replace each equal sign with two signs (==) to ensure that the connection string is parsed correctly.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| TOKEN                          | Depends  | The OAuth token to use for OAuth authentication. Must be used in combination with AUTHENTICATOR=oauth.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| INSECUREMODE                   | No       | Set to true to disable the certificate revocation list check. Default is false.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| USEPROXY                       | No       | Set to true if you need to use a proxy server. The default value is false. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PROXYHOST                      | Depends  | The hostname of the proxy server. <br/> <br/> If USEPROXY is set to `true`, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PROXYPORT                      | Depends  | The port number of the proxy server. <br/> <br/> If USEPROXY is set to `true`, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| PROXYUSER                      | No       | The username for authenticating to the proxy server. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| PROXYPASSWORD                  | Depends  | The password for authenticating to the proxy server. <br/> <br/> If USEPROXY is `true` and PROXYUSER is set, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| NONPROXYHOSTS                  | No       | The list of hosts that the driver should connect to directly, bypassing the proxy server. Separate the hostnames with a pipe symbol (\|). You can also use an asterisk (`*`) as a wildcard. <br/> The host target value should fully match with any item from the proxy host list to bypass the proxy server. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                        |
| FILE_TRANSFER_MEMORY_THRESHOLD | No       | The maximum number of bytes to store in memory used in order to provide a file encryption. If encrypting/decrypting file size exceeds provided value a temporary file will be created and the work will be continued in the temporary file instead of memory. <br/> If no value provided 1MB will be used as a default value (that is 1048576 bytes). <br/> It is possible to configure any integer value bigger than zero representing maximal number of bytes to reside in memory.                                                                                                                                                                                                                                      |
| CLIENT_CONFIG_FILE             | No       | The location of the client configuration json file. In this file you can configure easy logging feature.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ALLOWUNDERSCORESINHOST         | No       | Specifies whether to allow underscores in account names. This impacts PrivateLink customers whose account names contain underscores. In this situation, you must override the default value by setting allowUnderscoresInHost to true.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| QUERY_TAG                      | No       | Optional string that can be used to tag queries and other SQL statements executed within a connection. The tags are displayed in the output of the QUERY_HISTORY , QUERY_HISTORY_BY_* functions.<br/> To set QUERY_TAG on the statement level you can use SnowflakeDbCommand.QueryTag.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| MAXPOOLSIZE                    | No       | Maximum number of connections in a pool. Default value is 10. `maxPoolSize` value cannot be lower than `minPoolSize` value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| MINPOOLSIZE                    | No       | Expected minimum number of connections in pool. When you get a connection from the pool, more connections might be initialised in background to increase the pool size to `minPoolSize`. If you specify 0 or 1 there will be no attempts to create extra initialisations in background. The default value is 2. `maxPoolSize` value cannot be lower than `minPoolSize` value. The parameter is used only in a new version of connection pool.                                                                                                                                                                                                                                                                             |
| CHANGEDSESSION                 | No       | Specifies what should happen with a closed connection when some of its session variables are altered (e. g. you used `ALTER SESSION SET SCHEMA` to change the databese schema). The default behaviour is `OriginalPool` which means the session stays in the original pool. Currently no other option is possible. Parameter used only in a new version of connection pool.                                                                                                                                                                                                                                                                                                                                               |
| WAITINGFORIDLESESSIONTIMEOUT   | No       | Timeout for waiting for an idle session when pool is full. It happens when there is no idle session and we cannot create a new one because of reaching `maxPoolSize`. The default value is 30 seconds. Usage of units possible and allowed are: e. g. `1000ms` (milliseconds), `15s` (seconds), `2m` (minutes) where seconds are default for a skipped postfix. Special values: `0` - immediate fail for new connection to open when session is full. You cannot specify infinite value.                                                                                                                                                                                                                                  |
| EXPIRATIONTIMEOUT              | No       | Timeout for using each connection. Connections which last more than specified timeout are considered to be expired and are being removed from the pool. The default is 1 hour. Usage of units possible and allowed are: e. g. `360000ms` (milliseconds), `3600s` (seconds), `60m` (minutes) where seconds are default for a skipped postfix. Special values: `0` - immediate expiration of the connection just after its creation. Expiration timeout cannot be set to infinity.                                                                                                                                                                                                                                          |
| POOLINGENABLED                 | No       | Boolean flag indicating if the connection should be a part of a pool. The default value is `true`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| DISABLE_SAML_URL_CHECK         | No       | Specifies whether to check if the saml postback url matches the host url from the connection string. The default value is `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

<br />

**Note**: Connections should not be shared across multiple threads.

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

  ```cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=test'password;"
  );
  ```

- To include a double quote (") character:

  ```cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=test\"password;"
  );
  ```

- To include a semicolon (;):

  ```cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=\"test;password\";"
  );
  ```

- To include an equal sign (=):

  ```cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "user=testuser; " +
    "password=test=password;"
  );
  ```

  Note that previously you needed to use a double equal sign (==) to escape the character. However, beginning with version 2.0.18, you can use a single equal size.


Snowflake supports using [double quote identifiers](https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers) for object property values (WAREHOUSE, DATABASE, SCHEMA AND ROLES). The value should be delimited with `\"` in the connection string. The value is case-sensitive and allow to use special characters as part of the value.

  ```cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "database=\"testDB\";"
  );
  ```
- To include a `"` character as part of the value should be escaped using `\"\"`.

  ```cs
  string connectionString = String.Format(
    "account=testaccount; " +
    "database=\"\"\"test\"\"user\"\"\";" // DATABASE => ""test"db""
  );
  ```

### Other Authentication Methods

If you are using a different method for authentication, see the examples below:

- **Key-pair authentication**

  After setting up [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html), you can specify the
  private key for authentication in one of the following ways:

    - Specify the file containing an unencrypted private key:

      ```cs
      using (IDbConnection conn = new SnowflakeDbConnection())
      {
          conn.ConnectionString = "account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key_file={pathToThePrivateKeyFile};db=testdb;schema=testschema";

          conn.Open();

          conn.Close();
      }
      ```

      where:

        - `{pathToThePrivateKeyFile}` is the path to the file containing the unencrypted private key.

    - Specify the file containing an encrypted private key:

      ```cs
      using (IDbConnection conn = new SnowflakeDbConnection())
      {
          conn.ConnectionString = "account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key_file={pathToThePrivateKeyFile};private_key_pwd={passwordForDecryptingThePrivateKey};db=testdb;schema=testschema";

          conn.Open();

          conn.Close();
      }
      ```

      where:

        - `{pathToThePrivateKeyFile}` is the path to the file containing the private key.
        - `{passwordForDecryptingThePrivateKey}` (optional) is the password for decrypting the private key if it is encrypted.

    - Specify an unencrypted private key (read from a file):

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

        - `{pathToThePrivateKeyFile}` is the path to the file containing the unencrypted private key.

- **OAuth**

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

    - `{oauthTokenValue}` is the oauth token to use for authentication.

- **Browser-based SSO**

  In the connection string, set `AUTHENTICATOR=externalbrowser`.
  Optionally, `USER` can be set. In that case only if user authenticated via external browser matches the one from configuration, authentication will complete.

  ```cs
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;authenticator=externalbrowser;user={login_name_for_IdP};db=testdb;schema=testschema";

      conn.Open();

      conn.Close();
  }
  ```

  where:

    - `{login_name_for_IdP}` is your login name for your IdP.

  You can override the default timeout after which external browser authentication is marked as failed.
  The timeout prevents the infinite hang when the user does not provide the login details, e.g. when closing the browser tab.
  To override, you can provide `BROWSER_RESPONSE_TIMEOUT` parameter (in seconds).

- **Native SSO through Okta**

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

    - `{okta_url_endpoint}` is the URL for the endpoint for your Okta account (e.g. `https://<okta_account_name>.okta.com`).
    - `{login_name_for_IdP}` is your login name for your IdP.

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

The NONPROXYHOSTS property could be set to specify if the server proxy should be bypassed by an specified host. This should be defined using the full host url or including the url + `*` wilcard symbol.

Examples:

- `*` (Bypassed all hosts from the proxy server)
- `*.snowflakecomputing.com` ('Bypass all host that ends with `snowflakecomputing.com`')
- `https:\\testaccount.snowflakecomputing.com` (Bypass proxy server using full host url).
- `*.myserver.com | *testaccount*` (You can specify multiple regex for the property divided by `|`)


> Note: The nonproxyhost value should match the full url including the http or https section. The '*' wilcard could be added to bypass the hostname successfully.

- `myaccount.snowflakecomputing.com` (Not bypassed).
- `*myaccount.snowflakecomputing.com` (Bypassed).

