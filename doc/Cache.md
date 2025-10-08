# Cache

## Tokens Caching

The Snowflake .NET driver provides the ability to cache tokens using different implementations.

### Enabling the Cache

#### Client Side
The **SSO token cache** and the token cache for **OAuth** authorization code flow is enabled by setting the parameter `client_store_temporary_credential` to `true`. The default value is based on OS:
- **Windows**: `true`
- **Linux**: `false`
- **Mac**: `false`

In case of **MFA token caching** (`username_password_mfa` authenticator) the cache is always enabled (you don't need to set any additional parameters for caching) because this authenticator doesn't make much sense without caching.

#### Server Side
Enabling SSO or MFA token caching on the client driver side is not enough to make it work, you also need to allow them on the server side, by toggling the relevant Snowflake parameters.
Please see below documentation:
* Allow SSO token to be cached (`ALLOW_ID_TOKEN`) - https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use#using-connection-caching-to-minimize-the-number-of-prompts-for-authentication-optional
* Allow MFA token to be cached (`ALLOW_CLIENT_MFA_CACHING`) - https://docs.snowflake.com/en/user-guide/security-mfa#label-mfa-token-caching

### Types of Cache
- **In-memory implementation:** The most secure option. Stores credentials that persist within the application's runtime
- **Windows Credential Manager:** The second most secure option. Leverages built-in Windows capabilities to cache tokens in the Windows Credential Manager
- **File-based implementation:** The least secure option. Stores credentials in a JSON file on the system
- **Custom implementation:** Users can choose to implement their own version of the cache

### Available Cache Type by Operating System
**Windows**
- Windows Credential Manager (default)
- In-memory implementation

**Linux**
- File-based implementation (default)
- In-memory implementation

**Mac**
- File-based implementation (default)
- In-memory implementation

### Switching Cache Type

To switch the cache type, simply call the following method based on your preference:
- **In-memory implementation:**
```cs
SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();
```
- **Windows Credential Manager:**
```cs
SnowflakeCredentialManagerFactory.UseWindowsCredentialManager();
```
- **File-based implementation:**
```cs
SnowflakeCredentialManagerFactory.UseFileCredentialManager();
```
- **Custom implementation:**
```cs
SnowflakeCredentialManagerFactory.SetCredentialManager(CustomCredentialManagerImplementation);
```

## Certificate Revocation List (CRL) Caching

Starting with version 5.0.0, Snowflake .NET driver uses by default a new algorithm for Certificate Revocation List-driven revocation checks.
If you need to use CRL checks please make sure to enable that with connection parameter `CERTREVOCATIONCHECKMODE = enabled`.
To ensure optimal performance, the driver uses both in-memory and on-disk cache for CRL data.
Parameters `ENABLECRLCACHING` and `ENABLECRLDISKCACHING` allow you to control the caching behavior when CRL checks are turned on.

### Cache File Location

The location for CRL cache file depends on the operating system as in the table below:

| Operating System | CRL Cache File Location                              |
|------------------|------------------------------------------------------|
| Windows          | `%USERPROFILE%\AppData\Local\Snowflake\Caches\crls\` |
| Linux            | `$HOME/.cache/snowflake/crls/`                       |
| MacOS            | `$HOME/Library/Caches/Snowflake/crls/`               |

### Cache Behavior Configuration

Two environment variables control how long CRLs are cached:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SF_CRL_VALIDITY_TIME` | 1 day | Maximum age (in days) for a cached CRL to be considered fresh. After this time, the driver will attempt to download a newer version. |
| `SF_CRL_CACHE_REMOVAL_DELAY` | 7 days | Interval (in days) for periodic cleanup of expired/stale CRLs from cache. |
