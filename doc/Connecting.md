## Connecting

To connect to Snowflake, specify a valid connection string composed of key-value pairs separated by semicolons,
i.e "\<key1\>=\<value1\>;\<key2\>=\<value2\>...".

**Note**: If the value specified in the connection string contains any signs like semicolon (`;`) or equal sign (`=`) or any phrases which can interfere with parsing the connection string,
please surround the value with double quotation marks (`""`). For example `password="=;;;=dummy==password;;"`.

The following table lists all valid connection properties:
<br />

| Connection Property               | Required | Comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ACCOUNT                           | Yes      | Your full account name might include additional segments that identify the region and cloud platform where your account is hosted                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| APPLICATION                       | No       | **_Snowflake partner use only_**: Specifies the name of a partner application to connect through .NET. The name must match the following pattern: ^\[A-Za-z](\[A-Za-z0-9.-]){1,50}$ (one letter followed by 1 to 50 letter, digit, .,- or, \_ characters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| DB                                | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| HOST                              | No       | Specifies the hostname for your account in the following format: \<ACCOUNT\>.snowflakecomputing.com. <br /> If no value is specified, the driver uses \<ACCOUNT\>.snowflakecomputing.com.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| PASSWORD                          | Depends  | Required if AUTHENTICATOR is set to `snowflake` (the default value) or the URL for native SSO through Okta. Ignored for all the other authentication types.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ROLE                              | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| SCHEMA                            | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| USER                              | Depends  | If AUTHENTICATOR is set to `externalbrowser` this is optional. For native SSO through Okta, set this to the login name for your identity provider (IdP).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| WAREHOUSE                         | No       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| CONNECTION_TIMEOUT                | No       | Total timeout in seconds when connecting to Snowflake. The default is 300 seconds                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| RETRY_TIMEOUT                     | No       | Total timeout in seconds for supported endpoints of retry policy. The default is 300 seconds. The value can only be increased from the default value or set to 0 for infinite timeout                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| MAXHTTPRETRIES                    | No       | Maximum number of times to retry failed HTTP requests (default: 7). You can set `MAXHTTPRETRIES=0` to remove the retry limit, but doing so runs the risk of the .NET driver infinitely retrying failed HTTP calls.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| CLIENT_SESSION_KEEP_ALIVE         | No       | Whether to keep the current session active after a period of inactivity, or to force the user to login again. If the value is `true`, Snowflake keeps the session active indefinitely, even if there is no activity from the user. If the value is `false`, the user must log in again after four hours of inactivity. The default is `false`. Setting this value overrides the server session property for the current session.                                                                                                                                                                                                                                                                                          |
| BROWSER_RESPONSE_TIMEOUT          | No       | Number to seconds to wait for authentication in an external browser (default: 120).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| DISABLERETRY                      | No       | Set this property to `true` to prevent the driver from reconnecting automatically when the connection fails or drops. The default value is `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| AUTHENTICATOR                     | No       | The method of authentication. Currently supports the following values: <br /> - snowflake (default): You must also set USER and PASSWORD. <br /> - [the URL for native SSO through Okta](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#native-sso-okta-only): You must also set USER and PASSWORD. <br /> - [externalbrowser](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#browser-based-sso): You must also set USER. <br /> - [snowflake_jwt](https://docs.snowflake.com/en/user-guide/key-pair-auth.html): You must also set PRIVATE_KEY_FILE or PRIVATE_KEY. <br /> - [oauth](https://docs.snowflake.com/en/user-guide/oauth.html): You must also set TOKEN. |
| VALIDATE_DEFAULT_PARAMETERS       | No       | Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default to be true.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PRIVATE_KEY_FILE                  | Depends  | The path to the private key file to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PRIVATE_KEY_PWD                   | No       | The passphrase to use for decrypting the private key, if the key is encrypted.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| PRIVATE_KEY                       | Depends  | The private key to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt. <br /> If the private key value includes any equal signs (=), make sure to replace each equal sign with two signs (==) to ensure that the connection string is parsed correctly.                                                                                                                                                                                                                                                                                                                                                                                                                        |
| TOKEN                             | Depends  | The OAuth token to use for OAuth authentication or Programmatic Access Token authentication or Workload Identity Federation for `OIDC`. Must be used in combination with `AUTHENTICATOR=oauth` or `AUTHENTICATOR=programmatic_access_token` or `AUTHENTICATOR=workload_identity`.                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| INSECUREMODE                      | No       | Set to true to disable the certificate revocation list check. Default is false.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| USEPROXY                          | No       | Set to true if you need to use a proxy server. The default value is false. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PROXYHOST                         | Depends  | The hostname of the proxy server. <br/> <br/> If USEPROXY is set to `true`, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PROXYPORT                         | Depends  | The port number of the proxy server. <br/> <br/> If USEPROXY is set to `true`, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| PROXYUSER                         | No       | The username for authenticating to the proxy server. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| PROXYPASSWORD                     | Depends  | The password for authenticating to the proxy server. <br/> <br/> If USEPROXY is `true` and PROXYUSER is set, you must set this parameter. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| NONPROXYHOSTS                     | No       | The list of hosts that the driver should connect to directly, bypassing the proxy server. Separate the hostnames with a pipe symbol (\|). You can also use an asterisk (`*`) as a wildcard. <br/> The host target value should fully match with any item from the proxy host list to bypass the proxy server. <br/> <br/> This parameter was introduced in v2.0.4.                                                                                                                                                                                                                                                                                                                                                        |
| FILE_TRANSFER_MEMORY_THRESHOLD    | No       | The maximum number of bytes to store in memory used in order to provide a file encryption. If encrypting/decrypting file size exceeds provided value a temporary file will be created and the work will be continued in the temporary file instead of memory. <br/> If no value provided 1MB will be used as a default value (that is 1048576 bytes). <br/> It is possible to configure any integer value bigger than zero representing maximal number of bytes to reside in memory.                                                                                                                                                                                                                                      |
| CLIENT_CONFIG_FILE                | No       | The location of the client configuration json file. In this file you can configure easy logging feature.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ALLOWUNDERSCORESINHOST            | No       | Specifies whether to allow underscores in account names. This impacts PrivateLink customers whose account names contain underscores. In this situation, you must override the default value by setting allowUnderscoresInHost to true.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| QUERY_TAG                         | No       | Optional string that can be used to tag queries and other SQL statements executed within a connection. The tags are displayed in the output of the QUERY_HISTORY , QUERY_HISTORY_BY_* functions.<br/> To set QUERY_TAG on the statement level you can use SnowflakeDbCommand.QueryTag.                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| MAXPOOLSIZE                       | No       | Maximum number of connections in a pool. Default value is 10. `maxPoolSize` value cannot be lower than `minPoolSize` value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| MINPOOLSIZE                       | No       | Expected minimum number of connections in pool. When you get a connection from the pool, more connections might be initialised in background to increase the pool size to `minPoolSize`. If you specify 0 or 1 there will be no attempts to create extra initialisations in background. The default value is 2. `maxPoolSize` value cannot be lower than `minPoolSize` value. The parameter is used only in a new version of connection pool.                                                                                                                                                                                                                                                                             |
| CHANGEDSESSION                    | No       | Specifies what should happen with a closed connection when some of its session variables are altered (e. g. you used `ALTER SESSION SET SCHEMA` to change the databese schema). The default behaviour is `OriginalPool` which means the session stays in the original pool. Currently no other option is possible. Parameter used only in a new version of connection pool.                                                                                                                                                                                                                                                                                                                                               |
| WAITINGFORIDLESESSIONTIMEOUT      | No       | Timeout for waiting for an idle session when pool is full. It happens when there is no idle session and we cannot create a new one because of reaching `maxPoolSize`. The default value is 30 seconds. Usage of units possible and allowed are: e. g. `1000ms` (milliseconds), `15s` (seconds), `2m` (minutes) where seconds are default for a skipped postfix. Special values: `0` - immediate fail for new connection to open when session is full. You cannot specify infinite value.                                                                                                                                                                                                                                  |
| EXPIRATIONTIMEOUT                 | No       | Timeout for using each connection. Connections which last more than specified timeout are considered to be expired and are being removed from the pool. The default is 1 hour. Usage of units possible and allowed are: e. g. `360000ms` (milliseconds), `3600s` (seconds), `60m` (minutes) where seconds are default for a skipped postfix. Special values: `0` - immediate expiration of the connection just after its creation. Expiration timeout cannot be set to infinity.                                                                                                                                                                                                                                          |
| POOLINGENABLED                    | No       | Boolean flag indicating if the connection should be a part of a pool. The default value is `true`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| DISABLE_SAML_URL_CHECK            | No       | Specifies whether to check if the saml postback url matches the host url from the connection string. The default value is `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| CLIENT_STORE_TEMPORARY_CREDENTIAL | No       | Specifies whether to cache tokens and use them for external bowser or oauth autorization code flow. The default value is `true` for Windows and `false` on non-Windows platforms.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PASSCODE                          | No       | Passcode from your 2FA application to be used in Multi Factor Authentication.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PASSCODEINPASSWORD                | No       | Boolean flag indicating if MFA passcode is added to the password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| OAUTHCLIENTID                     | Depends  | ClientId used for OAuth flows. It is required for OAuth Authorization Code Flow and OAuth Client Credentials Flow. For OAuth Authorization Code Flow and Snowflake used as the Identity provider it will be automatically filled with a default `LOCAL_APPLICATION` value if neither of `OAUTHCLIENTID` and `OAUTHCLIENTSECRET` properties are provided.                                                                                                                                                                                                                                                                                                                                                                  |
| OAUTHCLIENTSECRET                 | Depends  | ClientSecret used for OAuth flows. It is required for OAuth Authorization Code Flow and OAuth Client Credentials Flow. For OAuth Authorization Code Flow and Snowflake used as the Identity provider it will be automatically filled with a default `LOCAL_APPLICATION` value if neither of `OAUTHCLIENTID` and `OAUTHCLIENTSECRET` properties are provided.                                                                                                                                                                                                                                                                                                                                                              |
| OAUTHSCOPE                        | Depends  | The requested scope in OAuth flows. If not provided the default value is built based on `role`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| OAUTHAUTHORIZATIONURL             | Depends  | The url of the authorization endpoint (the one to get authorization code) for OAuth Authorization Code Flow. Required for non-Snowflake Identity Providers. Optional for Snowflake-provided OAuth service. See more: [Snowflake OAuth](https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview)                                                                                                                                                                                                                                                                                                                                                                                                                 |
| OAUTHTOKENREQUESTURL              | Depends  | The url of the token endpoint (the one to get access token/refresh token) for OAuth Authorization Code Flow or OAuth Client Credential Flow. Required for OAuth Client Credentials Flow. For OAuth Authorization Code Flow, required in case of non-Snowflake Identity Providers, but optional for Snowflake-provided OAuth service. See more: [Snowflake OAuth](https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview)                                                                                                                                                                                                                                                                                       |
| OAUTHREDIRECTURI                  | Depends  | The url of the local endpoint the driver will listen to in OAuth Authorization Code Flow to get an authorization code from the Identity Provider. Required for non-Snowflake Identity providers. Optional for Snowflake-provided OAuth service. See more: [Snowflake OAuth](https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview)                                                                                                                                                                                                                                                                                                                                                                            |
| WIFPROVIDER                       | No       | The type of attestation provider for Workload Identity Federation authentication. You can specify one of following values: `OIDC`, `AZURE`, `AWS`, `GCP`. If you don't provide it the provider is going to be auto-detected. It is recommended to specify the value because auto-detection increases latency.                                                                                                                                                                                                                                                                                                                                                                                                             |
| WIFENTRARESOURCE                  | No       | The entra resource used for Azure provider in Workload Identity Federation authentication. The default value for it is `api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| OAUTHENABLESINGLEUSEREFRESHTOKENS | No       | Used in OAuth Authorization Code Flow authentication. The default value is `false`. When set to `true` the driver requests the Identity Provider for single use refresh tokens.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
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

        - `{pathToThePrivateKeyFile}` is the path to the file containing the unencrypted private key.
        - `{passwordForDecryptingThePrivateKey}` is the password for decrypting the private key.

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

- **OAuth Authorization Code Flow**

The difference from simple OAuth authentication is that the driver doesn't get the token from the connection parameters but instead communicates with
the Identity Provider to get the token by itself. The Identity Provider can be Snowflake or any other provider e.g. Okta, etc.
For this kind of authentication a browser is started for the Identity Provider authorization endpoint.
After a human submits a request in a browser the driver gets authorization code from the Identity Provider and then use it to get an access token
which then is used to authorize in Snowflake.


The access tokens and refresh tokens are cached if `CLIENT_STORE_TEMPORARY_CREDENTIAL` property is set to true (the default value for that is `true` on Windows and `false` on Mac/Linux).
Caching the tokens means that once the token is cached it can be reused for subsequent authentications for which the cache is also enabled.
Thanks to that you can reduce the number of interactions with the Identity Provider and also reduce the human effort in submitting authentication data in the browser.


Example:
```csharp
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;user=testuser;db=testdb;schema=testschema;authenticator=oauth_authorization_code;oauthScope=testScope;oauthClientId=testClientId;oauthClientSecret=testClientSecret;oauthAuthorizationUrl=https://testauthorize.okta.com;oauthTokenRequestUrl=https://testtoken.okta.com;oauthRedirectUri=http://localhost:8001/snowflake/oauth-redirect";

      conn.Open();

      conn.Close();
  }
```

Alternatively you can provide `oauthClientSecret` property as a secure string of the connection instead of specifying it in the connection string:

```csharp
  using (SnowflakeDbConnection conn = new SnowflakeDbConnection("connection-string-without-client-secret"))
  {
      conn.OAuthClientSecret = ...; // configure client secret here
      conn.Open();
  }
```

Note: On Mac/Linux OS the browser is started by open/xdg-open command. Make sure that the command is properly configured on your PATH environmental variable.

- **OAuth Client Credentials Flow**

It is a similar authentication to OAuth Authorization Code Flow because the driver gets an access token from the Identity Provider instead of getting a token in the connection parameters,
but does not require any human interaction and does not use a token cache.
The driver gets an access token from the Identity Provider and then use it to authenticate in Snowflake.


Example:
```csharp
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;user=testuser;db=testdb;schema=testschema;authenticator=oauth_client_credentials;oauthScope=testScope;oauthClientId=testClientId;oauthClientSecret=testClientSecret;oauthTokenRequestUrl=https://testtoken.okta.com;";

      conn.Open();

      conn.Close();
  }
```

Alternatively you can provide `oauthClientSecret` property as a secure string of the connection instead of specifying it in the connection string:

```csharp
  using (SnowflakeDbConnection conn = new SnowflakeDbConnection("connection-string-without-client-secret"))
  {
      conn.OAuthClientSecret = ...; // configure client secret here
      conn.Open();
  }
```

- **Programmatic Access Token**

In Snowflake, you can generate a programmatic access token for your user and role restrictions and use this token to authenticate.

```csharp
  using (IDbConnection conn = new SnowflakeDbConnection())
  {
      conn.ConnectionString = "account=testaccount;user=testuser;db=testdb;schema=testschema;authenticator=programmatic_access_token;token=testtoken";
      conn.Open();
  }
```

Alternatively you can provide `token` property as a secure string of the connection instead of specifying it in the connection string:

```csharp
  using (SnowflakeDbConnection conn = new SnowflakeDbConnection("connection-string-without-token"))
  {
      conn.Token = ...; // configure client secret here
      conn.Open();
  }
```

- **Workload Identity Federation**

In this type of authentication credentials can be retrieved from a cloud on which your application is running (AWS, Azure, GCP) and then a token generated based on that is used to authenticate in Snowflake.
OIDC provider allows you to provide your own token which will be used to authenticate in Snowflake.

If you don't provide `WIFPROVIDER` property it will be auto-detected. The order in which the driver tries to produce attestation is: OIDC, Azure, AWS, GCP.
If you know on which cloud your application is running it is recommended to provide `WIFPROVIDER` parameter because auto-detection increases latency.

**Note**: Workload Identity Federation authentication currently is an experimental feature.
You need to set environmental variable `SF_ENABLE_EXPERIMENTAL_AUTHENTICATION` to `true` if you want to use this authentication.

Using Workload Identity Federation for AWS cloud:
```csharp
    var conn = new SnowflakeDbConnection("authenticator=workload_identity;wifProvider=aws;account=test;");
```

Using Workload Identity Federation for Azure cloud:
```csharp
    var conn1 = new SnowflakeDbConnection("authenticator=workload_identity;wifProvider=azure;account=test;"); // with default entra resource
    var conn2 = new SnowflakeDbConnection("authenticator=workload_identity;wifProvider=azure;wifEntraResource=api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad;account=test;"); // with provided entra resource
```

Using Workload Identity Federation for GCP cloud:
```csharp
    var conn = new SnowflakeDbConnection("authenticator=workload_identity;wifProvider=gcp;account=test;");
```

Using your own token (OIDC) for Workload Identity Federation:
```csharp
    var conn1 = new SnowflakeDbConnection("authenticator=workload_identity;wifProvider=oidc;token=yourtoken;account=test;"); // provide token in connection string

    var conn2 = new SnowflakeDbConnection("authenticator=workload_identity;wifProvider=oidc;account=test;");
    var conn2.Token = ...; // provide token by connection property
```

Using auto-detection:
```csharp
    var conn = new SnowflakeDbConnection("authenticator=workload_identity;account=test;");
```

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

Note: On Mac/Linux OS the browser is started by `open`/`xdg-open` command. Make sure that the command is properly configured on your `PATH` environmental variable.

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

### Snowflake credentials using a configuration file

.NET Drivers allows to add connections definitions to a configuration file. For a connection defined in this way all supported parameters in .NET could be defined and will be used to generate our connection string.

.NET Driver looks for the `connections.toml` in the following locations, in order.

* `SNOWFLAKE_HOME` environment variable,  You can modify the environment variable to use a different location.
* Otherwise, it uses the `connections.toml` file in `.snowflake` subfolder of the home directory, that is, based on your operating system:
  * MacOS/Linux: `~/.snowflake/connections.toml`
  * Windows: `%USERPROFILE%\.snowflake\connections.toml`

For MacOS and Linux systems, .NET Driver demands the connections.toml file to  have limited file permissions to read and write for the file owner only. To set the file required file permissions execute the following commands:

``` BASH
chown $USER connections.toml
chmod 0600 connections.toml
```

In the C# code to use this mechanism you should not specify any connection and it will try to use the configuration file.

``` toml
[myconnection]
account = "myaccount"
user = "jdoe"
password = "xyz1234"
```

```cs
using (IDbConnection conn = new SnowflakeDbConnection())
{
    conn.Open(); // Reads connection definition from configuration file.

    conn.Close();
}
```

By default the name of the connection will be `default`. You can also change the default connection name by setting the SNOWFLAKE_DEFAULT_CONNECTION_NAME environment variable, as shown:

```bash
set SNOWFLAKE_DEFAULT_CONNECTION_NAME=my_prod_connection
```

The following examples show how you can include different types of special characters in a toml key value pair string:

- To include a single quote (') character:

  ```toml
  [default]
  host = "fakeaccount.snowflakecomputing.com"
  user = "fakeuser"
  password = "fake\'password"
  ```

- To include a double quote (") character:

  ```toml
  [default]
  host = "fakeaccount.snowflakecomputing.com"
  user = "fakeuser"
  password = "fake\"password"
  ```
  - In case that double quote is use with other character that requires be wrap with double quoted it shoud use \\"\\" for a ":

    ```toml
    [default]
    host = "fakeaccount.snowflakecomputing.com"
    user = "fakeuser"
    password = "\";fake\"\"password\""
    ```

- To include a semicolon (;):

  ```toml
  [default]
  host = "fakeaccount.snowflakecomputing.com"
  user = "fakeuser"
  password = "\";fakepassword\""
  ```

- To include an equal sign (=):

  ```toml
  [default]
  host = "fakeaccount.snowflakecomputing.com"
  user = "fakeuser"
  password = "fake=password"
  ```
