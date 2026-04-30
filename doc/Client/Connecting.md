## Connecting

To connect to Snowflake, specify a valid connection string composed of key-value pairs separated by semicolons,
i.e "\<key1\>=\<value1\>;\<key2\>=\<value2\>...".

**Note**: If the value contains semicolons (`;`), equal signs (`=`), or other characters that interfere with parsing,
surround it with double quotation marks (`""`). For example `password="=;;;=dummy==password;;"`.

**Note**: Connections should not be shared across multiple threads.

For the full list of supported connection string properties, see the [Connection Properties Reference](#connection-properties-reference) at the bottom of this page.

### Password-based Authentication

```csharp
using var conn = new SnowflakeDbConnection();
conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema";
await conn.OpenAsync(cancellationToken);
```

<a id="sample-connection-strings"></a>

Beginning with version 2.0.18, the connector uses Microsoft [DbConnectionStringBuilder](https://learn.microsoft.com/en-us/dotnet/api/system.data.oledb.oledbconnection.connectionstring?view=dotnet-plat-ext-6.0#remarks) to follow the .NET specification for escaping characters in connection strings.

Special characters in connection strings:

- Single quote (`'`):
  ```csharp
  var connectionString = "account=testaccount;user=testuser;password=test'password;";
  ```

- Double quote (`"`):
  ```csharp
  var connectionString = "account=testaccount;user=testuser;password=test\"password;";
  ```

- Semicolon (`;`):
  ```csharp
  var connectionString = "account=testaccount;user=testuser;password=\"test;password\";";
  ```

- Equal sign (`=`):
  ```csharp
  var connectionString = "account=testaccount;user=testuser;password=test=password;";
  ```

  Note that prior to version 2.0.18, a double equal sign (`==`) was required to escape `=`. This is no longer necessary.

Snowflake supports [double-quoted identifiers](https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers) for object property values (WAREHOUSE, DATABASE, SCHEMA, ROLE). The value is case-sensitive and allows special characters:

```csharp
var connectionString = "account=testaccount;db=\"testDB\";";
```

To include a `"` within the value, escape it with `\"\"`:

```csharp
var connectionString = "account=testaccount;db=\"\"\"test\"\"db\"\"\";";
// DATABASE => ""test"db""
```

### Other Authentication Methods

- **Key-pair authentication**

  After setting up [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html), specify the
  private key in one of the following ways:

    - Unencrypted private key file:

      ```csharp
      using var conn = new SnowflakeDbConnection();
      conn.ConnectionString = "account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key_file={pathToThePrivateKeyFile};db=testdb;schema=testschema";
      await conn.OpenAsync(cancellationToken);
      ```

    - Encrypted private key file:

      ```csharp
      using var conn = new SnowflakeDbConnection();
      conn.ConnectionString = "account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key_file={pathToThePrivateKeyFile};private_key_pwd={passwordForDecryptingThePrivateKey};db=testdb;schema=testschema";
      await conn.OpenAsync(cancellationToken);
      ```

    - Unencrypted private key read from a file:

      ```csharp
      var privateKeyContent = File.ReadAllText(pathToThePrivateKeyFile);

      using var conn = new SnowflakeDbConnection();
      conn.ConnectionString = $"account=testaccount;authenticator=snowflake_jwt;user=testuser;private_key={privateKeyContent};db=testdb;schema=testschema";
      await conn.OpenAsync(cancellationToken);
      ```

- **OAuth**

  After setting up [OAuth](https://docs.snowflake.com/en/user-guide/oauth.html), set `AUTHENTICATOR=oauth` and `TOKEN` to the OAuth token.

  ```csharp
  using var conn = new SnowflakeDbConnection();
  conn.ConnectionString = "account=testaccount;user=testuser;authenticator=oauth;token={oauthTokenValue};db=testdb;schema=testschema";
  conn.Open();
  ```

- **OAuth Authorization Code Flow**

  Unlike simple OAuth, the driver communicates directly with the Identity Provider (Snowflake, Okta, etc.) to obtain tokens.
  A browser is opened for the authorization endpoint; after the user authenticates, the driver exchanges the authorization code for an access token.

  Tokens are cached when `CLIENT_STORE_TEMPORARY_CREDENTIAL=true` (default on Windows, `false` on Mac/Linux), reducing repeated browser interactions.

  ```csharp
  using var conn = new SnowflakeDbConnection();
  conn.ConnectionString = "account=testaccount;user=testuser;db=testdb;schema=testschema;authenticator=oauth_authorization_code;oauthScope=testScope;oauthClientId=testClientId;oauthClientSecret=testClientSecret;oauthAuthorizationUrl=https://testauthorize.okta.com;oauthTokenRequestUrl=https://testtoken.okta.com;oauthRedirectUri=http://localhost:8001/snowflake/oauth-redirect";
  await conn.OpenAsync(cancellationToken);
  ```

  The client secret can also be provided as a secure property instead of in the connection string:

  ```csharp
  using var conn = new SnowflakeDbConnection("connection-string-without-client-secret");
  conn.OAuthClientSecret = ...; // configure client secret here
  await conn.OpenAsync(cancellationToken);
  ```

  Note: On Mac/Linux the browser is started via `open`/`xdg-open`. Ensure the command is on your `PATH`.

- **OAuth Client Credentials Flow**

  Similar to OAuth Authorization Code Flow, but does not require human interaction or a token cache.
  The driver obtains an access token directly from the Identity Provider.

  ```csharp
  using var conn = new SnowflakeDbConnection();
  conn.ConnectionString = "account=testaccount;user=testuser;db=testdb;schema=testschema;authenticator=oauth_client_credentials;oauthScope=testScope;oauthClientId=testClientId;oauthClientSecret=testClientSecret;oauthTokenRequestUrl=https://testtoken.okta.com;";
  await conn.OpenAsync(cancellationToken);
  ```

  The client secret can also be provided as a secure property:

  ```csharp
  using var conn = new SnowflakeDbConnection("connection-string-without-client-secret");
  conn.OAuthClientSecret = ...; // configure client secret here
  await conn.OpenAsync(cancellationToken);
  ```

- **Programmatic Access Token**

  Generate a [programmatic access token](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens) for your user and use it to authenticate.

  ```csharp
  using var conn = new SnowflakeDbConnection();
  conn.ConnectionString = "account=testaccount;user=testuser;db=testdb;schema=testschema;authenticator=programmatic_access_token;token=testtoken";
  await conn.OpenAsync(cancellationToken);
  ```

  The token can also be provided as a secure property:

  ```csharp
  using var conn = new SnowflakeDbConnection("connection-string-without-token");
  conn.Token = ...; // configure token here
  await conn.OpenAsync(cancellationToken);
  ```

- **Workload Identity Federation**

  Credentials are retrieved from the cloud platform where the application is running (AWS, Azure, GCP). The OIDC provider option allows supplying your own token.

  ```csharp
  // AWS
  using var connAws = new SnowflakeDbConnection("authenticator=workload_identity;workload_identity_provider=aws;account=test;");

  // Azure (default entra resource)
  using var connAzure = new SnowflakeDbConnection("authenticator=workload_identity;workload_identity_provider=azure;account=test;");

  // Azure (custom entra resource)
  using var connAzureCustom = new SnowflakeDbConnection("authenticator=workload_identity;workload_identity_provider=azure;workload_identity_entra_resource=api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad;account=test;");

  // GCP
  using var connGcp = new SnowflakeDbConnection("authenticator=workload_identity;workload_identity_provider=gcp;account=test;");

  // OIDC (your own token)
  using var connOidc = new SnowflakeDbConnection("authenticator=workload_identity;workload_identity_provider=oidc;token=yourtoken;account=test;");
  ```

  For Azure, the client ID can be set via the `MANAGED_IDENTITY_CLIENT_ID` environment variable.

- **Browser-based SSO**

  Set `AUTHENTICATOR=externalbrowser`. Optionally set `USER` to restrict authentication to a specific identity.

  ```csharp
  using var conn = new SnowflakeDbConnection();
  conn.ConnectionString = "account=testaccount;authenticator=externalbrowser;user={login_name_for_IdP};db=testdb;schema=testschema";
  await conn.OpenAsync(cancellationToken);
  ```

  Override the default browser timeout with `BROWSER_RESPONSE_TIMEOUT` (in seconds).

  Note: On Mac/Linux the browser is started via `open`/`xdg-open`. Ensure the command is on your `PATH`.

- **Native SSO through Okta**

  Set `AUTHENTICATOR` to the
  [URL of your Okta account endpoint](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#label-native-sso-okta),
  and `USER` to the login name for your IdP.

  ```csharp
  using var conn = new SnowflakeDbConnection();
  conn.ConnectionString = "account=testaccount;authenticator={okta_url_endpoint};user={login_name_for_IdP};db=testdb;schema=testschema";
  await conn.OpenAsync(cancellationToken);
  ```

### Proxy Configuration

In v2.0.4 and later, the driver can connect through a proxy server:

```csharp
using var conn = new SnowflakeDbConnection();
conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;db=testdb;schema=testschema;useProxy=true;proxyHost=myproxyserver;proxyPort=8888;proxyUser=test;proxyPassword=test";
await conn.OpenAsync(cancellationToken);
```

Use `NONPROXYHOSTS` to bypass the proxy for specific hosts. Values must match the full URL including scheme. Separate multiple patterns with `|`.

Examples:

- `*` — bypass all hosts
- `*.snowflakecomputing.com` — bypass all Snowflake hosts
- `https:\\testaccount.snowflakecomputing.com` — bypass a specific host
- `*.myserver.com | *testaccount*` — multiple patterns

> Note: Patterns without a scheme or wildcard will not match. `myaccount.snowflakecomputing.com` is **not** bypassed, but `*myaccount.snowflakecomputing.com` is.

### Snowflake credentials using a configuration file

Connection parameters can be defined in a `connections.toml` file. The driver looks for it in:

1. The path in `SNOWFLAKE_HOME` environment variable
2. `~/.snowflake/connections.toml` (Mac/Linux) or `%USERPROFILE%\.snowflake\connections.toml` (Windows)

On Mac/Linux, the file must have restricted permissions:

```bash
chown $USER connections.toml
chmod 0600 connections.toml
```

Example `connections.toml`:

```toml
[myconnection]
account = "myaccount"
user = "jdoe"
password = "xyz1234"
```

```csharp
using var conn = new SnowflakeDbConnection();
await conn.OpenAsync(cancellationToken); // reads connection definition from configuration file
```

The default connection name is `default`. Override it with the `SNOWFLAKE_DEFAULT_CONNECTION_NAME` environment variable:

```bash
set SNOWFLAKE_DEFAULT_CONNECTION_NAME=my_prod_connection
```

Special characters in TOML values:

- Single quote (`'`): `password = "fake\'password"`
- Double quote (`"`): `password = "fake\"password"`
  - When combined with other characters requiring double-quote wrapping: `password = "\";fake\"\"password\""`
- Semicolon (`;`): `password = "\";fakepassword\""`
- Equal sign (`=`): `password = "fake=password"`

---

## Connection Properties Reference

| Connection Property               | Required | Comment |
|-----------------------------------|----------|---------|
| ACCOUNT                           | Yes      | Your full account name might include additional segments that identify the region and cloud platform where your account is hosted |
| APPLICATION                       | No       | **_Snowflake partner use only_**: Specifies the name of a partner application to connect through .NET. The name must match the following pattern: ^\[A-Za-z](\[A-Za-z0-9.-]){1,50}$ (one letter followed by 1 to 50 letter, digit, .,- or, \_ characters). |
| DB                                | No       | |
| HOST                              | No       | Specifies the hostname for your account in the following format: \<ACCOUNT\>.snowflakecomputing.com. <br /> If no value is specified, the driver uses \<ACCOUNT\>.snowflakecomputing.com. |
| PASSWORD                          | Depends  | Required if AUTHENTICATOR is set to `snowflake` (the default value) or the URL for native SSO through Okta. Ignored for all the other authentication types. |
| ROLE                              | No       | |
| SCHEMA                            | No       | |
| USER                              | Depends  | If AUTHENTICATOR is set to `externalbrowser` this is optional. For native SSO through Okta, set this to the login name for your identity provider (IdP). |
| WAREHOUSE                         | No       | |
| CONNECTION_TIMEOUT                | No       | Total timeout in seconds when connecting to Snowflake. The default is 300 seconds |
| RETRY_TIMEOUT                     | No       | Total timeout in seconds for supported endpoints of retry policy. The default is 300 seconds. The value can only be increased from the default value or set to 0 for infinite timeout |
| MAXHTTPRETRIES                    | No       | Maximum number of times to retry failed HTTP requests (default: 7). Set to `0` to remove the retry limit (risks infinite retries). |
| CLIENT_SESSION_KEEP_ALIVE         | No       | Whether to keep the current session active after a period of inactivity, or to force the user to login again. If the value is `true`, Snowflake keeps the session active indefinitely, even if there is no activity from the user. If the value is `false`, the user must log in again after four hours of inactivity. The default is `false`. Setting this value overrides the server session property for the current session. |
| BROWSER_RESPONSE_TIMEOUT          | No       | Number to seconds to wait for authentication in an external browser (default: 120). |
| DISABLERETRY                      | No       | Set to `true` to prevent the driver from reconnecting automatically when the connection fails or drops. Default: `false`. |
| AUTHENTICATOR                     | No       | The method of authentication. Currently supports the following values: <br/> - snowflake (default): You must also set USER and PASSWORD. <br/> - [the URL for native SSO through Okta](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#native-sso-okta-only): You must also set USER and PASSWORD. <br/> - [externalbrowser](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html#browser-based-sso): You can also set USER. <br/> - [snowflake_jwt](https://docs.snowflake.com/en/user-guide/key-pair-auth.html): You must also set PRIVATE_KEY_FILE or PRIVATE_KEY. <br/> - [oauth](https://docs.snowflake.com/en/user-guide/oauth.html): You must also set TOKEN. <br/> - [oauth_authorization_code](https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview): when authorizing with Snowflake all the OAUTH prefixed parameters are optional, for external provider required are: OAUTHCLIENTID, OAUTHCLIENTSECRET, OAUTHSCOPE, OAUTHAUTHORIZATIONURL, OAUTHTOKENREQUESTURL, OAUTHREDIRECTURI <br/> - [oauth_client_credentials](https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview): You must provide OAUTHCLIENTID, OAUTHCLIENTSECRET, OAUTHSCOPE, OAUTHTOKENREQUESTURL, OAUTHREDIRECTURI <br/> - [programmatic_access_token](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens): You must provide TOKEN parameter<br/> - [workload_identity](https://docs.snowflake.com/en/user-guide/workload-identity-federation): you must provide WORKLOAD_IDENTITY_PROVIDER, mandatory is WORKLOAD_IDENTITY_ENTRA_RESOURCE for Azure and TOKEN for OIDC provider |
| VALIDATE_DEFAULT_PARAMETERS       | No       | Whether DB, SCHEMA and WAREHOUSE should be verified when making connection. Default: `true`. |
| PRIVATE_KEY_FILE                  | Depends  | The path to the private key file to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt |
| PRIVATE_KEY_PWD                   | No       | The passphrase to use for decrypting the private key, if the key is encrypted. |
| PRIVATE_KEY                       | Depends  | The private key to use for key-pair authentication. Must be used in combination with AUTHENTICATOR=snowflake_jwt. <br /> If the private key value includes any equal signs (=), make sure to replace each equal sign with two signs (==) to ensure that the connection string is parsed correctly. |
| TOKEN                             | Depends  | The OAuth token to use for OAuth authentication or Programmatic Access Token authentication or Workload Identity Federation for `OIDC`. Must be used in combination with `AUTHENTICATOR=oauth` or `AUTHENTICATOR=programmatic_access_token` or `AUTHENTICATOR=workload_identity`. |
| USEPROXY                          | No       | Set to `true` to use a proxy server. Default: `false`. |
| PROXYHOST                         | Depends  | The hostname of the proxy server. Required if USEPROXY is `true`. |
| PROXYPORT                         | Depends  | The port number of the proxy server. Required if USEPROXY is `true`. |
| PROXYUSER                         | No       | The username for authenticating to the proxy server. |
| PROXYPASSWORD                     | Depends  | The password for authenticating to the proxy server. Required if USEPROXY is `true` and PROXYUSER is set. |
| NONPROXYHOSTS                     | No       | Pipe-separated (`\|`) list of hosts to bypass the proxy. Supports asterisk (`*`) wildcards. The value must match the full URL including scheme. |
| FILE_TRANSFER_MEMORY_THRESHOLD    | No       | Maximum bytes to hold in memory for file encryption/decryption. If the file exceeds this size, a temporary file is used. Default: 1048576 (1 MB). |
| CLIENT_CONFIG_FILE                | No       | Path to the client configuration JSON file for easy logging. |
| ALLOWUNDERSCORESINHOST            | No       | Allow underscores in account names. Required for PrivateLink customers whose account names contain underscores. Default: `false`. |
| QUERY_TAG                         | No       | Tag applied to queries and SQL statements within the connection. Visible in QUERY_HISTORY output. Can also be set per-statement via `SnowflakeDbCommand.QueryTag`. |
| MAXPOOLSIZE                       | No       | Maximum number of connections in a pool. Default: 10. Cannot be lower than `MINPOOLSIZE`. |
| MINPOOLSIZE                       | No       | Minimum number of connections in a pool. Additional connections are created in the background. Default: 2. Cannot exceed `MAXPOOLSIZE`. |
| CHANGEDSESSION                    | No       | Behavior when a connection's session variables (schema, database, warehouse, role) are altered. Default: `OriginalPool`. |
| WAITINGFORIDLESESSIONTIMEOUT      | No       | Timeout when waiting for an idle session in a full pool. Supports units: `1000ms`, `15s`, `2m` (default unit: seconds). Default: 30s. Set to `0` for immediate failure. |
| EXPIRATIONTIMEOUT                 | No       | Maximum lifetime of a pooled connection. Supports units: `360000ms`, `3600s`, `60m` (default unit: seconds). Default: 1 hour. Set to `0` for immediate expiration. |
| POOLINGENABLED                    | No       | Enable or disable connection pooling. Default: `true`. |
| DISABLE_SAML_URL_CHECK            | No       | Skip SAML postback URL validation against the connection string host. Default: `false`. |
| CLIENT_STORE_TEMPORARY_CREDENTIAL | No       | Cache tokens for external browser or OAuth authorization code flow. Default: `true` on Windows, `false` elsewhere. |
| PASSCODE                          | No       | Passcode from a 2FA application for Multi-Factor Authentication. |
| PASSCODEINPASSWORD                | No       | Whether the MFA passcode is appended to the password. |
| OAUTHCLIENTID                     | Depends  | Client ID for OAuth flows. Required for OAuth Authorization Code Flow and OAuth Client Credentials Flow. Auto-filled with `LOCAL_APPLICATION` for Snowflake-provided OAuth when neither OAUTHCLIENTID nor OAUTHCLIENTSECRET are set. |
| OAUTHCLIENTSECRET                 | Depends  | Client secret for OAuth flows. Required for OAuth Authorization Code Flow and OAuth Client Credentials Flow. Auto-filled with `LOCAL_APPLICATION` for Snowflake-provided OAuth when neither OAUTHCLIENTID nor OAUTHCLIENTSECRET are set. |
| OAUTHSCOPE                        | Depends  | Requested scope for OAuth flows. If not provided, built from `ROLE`. |
| OAUTHAUTHORIZATIONURL             | Depends  | Authorization endpoint URL for OAuth Authorization Code Flow. Required for non-Snowflake Identity Providers. |
| OAUTHTOKENREQUESTURL              | Depends  | Token endpoint URL for OAuth flows. Required for OAuth Client Credentials Flow and non-Snowflake Identity Providers in Authorization Code Flow. |
| OAUTHREDIRECTURI                  | Depends  | Local endpoint the driver listens on for OAuth Authorization Code Flow. Required for non-Snowflake Identity Providers. |
| WORKLOAD_IDENTITY_PROVIDER        | Depends  | Attestation provider for Workload Identity Federation: `OIDC`, `AZURE`, `AWS`, or `GCP`. Required when AUTHENTICATOR is `workload_identity`. |
| WORKLOAD_IDENTITY_ENTRA_RESOURCE  | No       | Entra resource for Azure Workload Identity Federation. Default: `api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad`. |
| WORKLOAD_IMPERSONATION_PATH       | No       | Comma-separated identities for transitive service account impersonation. **AWS and GCP only.** For GCP: service account emails. For AWS: IAM role ARNs. Each identity needs permissions to impersonate the next. |
| OAUTHENABLESINGLEUSEREFRESHTOKENS | No       | Request single-use refresh tokens in OAuth Authorization Code Flow. Default: `false`. |
| CERTREVOCATIONCHECKMODE           | No       | Certificate revocation check mode. Values: `disabled` (default), `enabled`, `advisory`, `native`. `Advisory` allows connections when CRL check encounters errors but blocks revoked certificates. `Native` uses `System.Net.Http.HttpClientHandler`. |
| ENABLECRLDISKCACHING              | No       | Enable file-based CRL cache when driver CRL checks are active. Default: `true`. |
| ENABLECRLINMEMORYCACHING          | No       | Enable in-memory CRL cache when driver CRL checks are active. Default: `true`. |
| ALLOWCERTIFICATESWITHOUTCRLURL    | No       | Accept certificates without a CRL URL when driver CRL checks are active. Default: `false`. |
| CRLDOWNLOADTIMEOUT                | No       | Timeout in seconds for downloading CRL files. Default: `10`. |
| CRLDOWNLOADMAXSIZE                | No       | Maximum size in bytes for CRL file downloads. Default: `209715200` (200 MB). |
| MINTLS                            | No       | Minimum TLS version. Values: `TLS12` (default), `TLS13`. |
| MAXTLS                            | No       | Maximum TLS version. Values: `TLS12`, `TLS13` (default). |
| SERVICE_POINT_CONNECTION_LIMIT    | No       | Maximum connections for the ServicePoint object. Default: 20. Only the limit from the first connection string takes effect. |
| HONORSESSIONTIMEZONE              | No       | When `true`, TIMESTAMP_LTZ values honor the session TIMEZONE parameter instead of the local machine timezone. Default: `false`. |
