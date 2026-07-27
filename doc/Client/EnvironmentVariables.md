# Environment Variables

The Snowflake .NET driver reads a number of environment variables to control its behavior. None of these are required — sensible defaults are used when they are not set.

## Quick Reference

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SNOWFLAKE_HOME` | string | _(empty)_ | Directory containing `connections.toml` |
| `SNOWFLAKE_DEFAULT_CONNECTION_NAME` | string | `default` | Connection name to use from `connections.toml` |
| `SF_CLIENT_CONFIG_FILE` | string | _(empty)_ | Path to the logging configuration file |
| `SF_CHUNK_DOWNLOAD_IDLE_TIMEOUT` | int (seconds) | `180` | Max idle time between chunk download reads |
| `SF_CHUNK_DOWNLOAD_READ_TIMEOUT` | int (seconds) | `0` (disabled) | Per-read timeout for chunk downloads |
| `SF_DISABLE_MINICORE` | bool | `false` | Disables loading of the native minicore library |
| `SNOWFLAKE_DISABLE_PLATFORM_DETECTION` | bool | `false` | Disables platform detection telemetry |
| `SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION` | bool | `false` | Skips Unix file permission checks on token/config files |
| `SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE` | bool | `false` | Suppresses warnings about overly permissive file permissions |
| `SNOWFLAKE_ENABLE_AWS_WIF_OUTBOUND_TOKEN` | bool | `false` | Use JWT-based AWS WIF attestation instead of signed request |
| `MANAGED_IDENTITY_CLIENT_ID` | string | _(empty)_ | Azure Managed Identity client ID for WIF authentication |
| `SF_CRL_VALIDITY_TIME` | int (days) | `1` | How long a cached CRL is considered fresh |
| `SF_CRL_CACHE_REMOVAL_DELAY` | int (days) | `7` | Interval for cleaning up expired CRL entries |
| `SF_TEMPORARY_CREDENTIAL_CACHE_DIR` | string | _(empty)_ | Custom directory for credential cache storage |
| `XDG_CACHE_HOME` | string | _(empty)_ | Base cache directory (XDG standard) |
| `HOME` | string | _(system)_ | User home directory (fallback for cache resolution) |

---

## Connection & Configuration

### SNOWFLAKE_HOME

Specifies the directory where the driver looks for `connections.toml`.

- **Type:** Directory path
- **Default:** `~/.snowflake` (Unix) or `%USERPROFILE%\.snowflake` (Windows)

When set, the driver reads connection definitions from `{SNOWFLAKE_HOME}/connections.toml`. When unset, it falls back to the platform-specific default.

### SNOWFLAKE_DEFAULT_CONNECTION_NAME

Selects which named connection section from `connections.toml` to use when no explicit connection name is provided.

- **Type:** String (connection section name)
- **Default:** `default`

If the specified connection name does not exist in the TOML file and is not the literal `default`, an exception is thrown.

### SF_CLIENT_CONFIG_FILE

Full path to the easy logging configuration file (`sf_client_config.json`).

- **Type:** File path
- **Default:** _(empty — triggers automatic discovery)_

When not set, the driver searches for the configuration file in the following order:
1. `CLIENT_CONFIG_FILE` connection string parameter
2. Application/driver directory
3. `~/.snowflake/sf_client_config.json`

---

## Performance & Timeouts

### SF_CHUNK_DOWNLOAD_IDLE_TIMEOUT

Maximum time (in seconds) the driver waits without receiving any data between consecutive reads during result set chunk downloads. If the connection goes idle for longer than this value, the download is aborted with a timeout error.

- **Type:** Integer (seconds)
- **Default:** `180`
- **Disabled:** Set to `0`

The idle timer resets after each successful read. This detects stalled connections where the server stops sending data mid-transfer.

### SF_CHUNK_DOWNLOAD_READ_TIMEOUT

Maximum time (in seconds) a single read operation may take during result set chunk downloads. If an individual read call does not complete within this deadline, the download is aborted.

- **Type:** Integer (seconds)
- **Default:** `0` (disabled)

Unlike the idle timeout which measures gaps between reads, this timeout applies to each individual read operation independently.

---

## Telemetry & Diagnostics

### SF_DISABLE_MINICORE

Disables loading of the native `sf_mini_core` library.

- **Type:** Boolean (`true`/`false`)
- **Default:** `false`

When set to `true`, the driver skips initialization of the native minicore component. This is read once at startup and cannot be changed at runtime.

### SNOWFLAKE_DISABLE_PLATFORM_DETECTION

Disables automatic cloud platform detection.

- **Type:** Boolean (`true`/`false`)
- **Default:** `false`

By default, the driver detects the runtime environment (AWS Lambda, Azure Functions, GCP Cloud Run, EC2, Azure VM, etc.) and includes this information in the login request. Detection runs once at startup in the background with a 200ms timeout per check.

When set to `true`, no platform detection is performed and no platform information is sent to Snowflake.

---

## Security & Authentication

### SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION

Bypasses Unix file permission validation when reading token files and TOML configuration files.

- **Type:** Boolean (`true`/`false`)
- **Default:** `false`

When `false` (default), the driver validates that sensitive files have restrictive permissions (e.g., `chmod 600`). Set to `true` in environments where standard file permissions cannot be applied.

> **Note:** On Windows, Unix file permission checks do not apply and this variable has no effect.

> **Warning:** Disabling permission checks on Unix/macOS reduces security. Use only when necessary (e.g., containerized environments with non-standard file systems).

### SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE

Suppresses warning log messages when configuration files are readable by group or other users.

- **Type:** Boolean (`true`/`false`)
- **Default:** `false`

This only suppresses warnings — it does not change any security behavior. File operations proceed regardless; the driver simply stops logging about overly permissive read permissions.

### SNOWFLAKE_ENABLE_AWS_WIF_OUTBOUND_TOKEN

Selects the attestation method for AWS Workload Identity Federation (WIF) authentication.

- **Type:** Boolean (`true`/`false`)
- **Default:** `false`

| Value | Method | Description |
|-------|--------|-------------|
| `false` | GetCallerIdentity | Signs a request and sends the base64-encoded signed payload (legacy) |
| `true` | GetWebIdentityToken | Calls AWS STS and returns a JWT token directly |

The JWT-based method (`true`) is required for transitive role assumption (chaining multiple role ARNs).

### MANAGED_IDENTITY_CLIENT_ID

Specifies an Azure Managed Identity client ID when using Azure WIF authentication.

- **Type:** String (Azure client ID)
- **Default:** _(empty — uses the default managed identity)_

When set, the `client_id` parameter is added to token requests sent to Azure's Managed Identity endpoint. This is necessary when using user-assigned managed identities or when multiple managed identities are configured on the host.

---

## Certificate Revocation (CRL)

### SF_CRL_VALIDITY_TIME

Maximum age (in days) for a cached CRL to be considered fresh. After this period, the driver attempts to download a newer version.

- **Type:** Integer (days)
- **Default:** `1`

### SF_CRL_CACHE_REMOVAL_DELAY

Interval (in days) for periodic cleanup of expired CRL entries from cache.

- **Type:** Integer (days)
- **Default:** `7`

This should generally be greater than or equal to `SF_CRL_VALIDITY_TIME`.

---

## Caching & Storage

### SF_TEMPORARY_CREDENTIAL_CACHE_DIR

Custom directory for storing cached credentials (SSO tokens, OAuth tokens, MFA tokens).

- **Type:** Directory path
- **Default:** _(empty — falls through to XDG/HOME resolution)_

When set, this takes priority over all other cache directory resolution methods.

### XDG_CACHE_HOME

Base directory for user-specific cache files (XDG Base Directory Specification). The driver stores credentials under `{XDG_CACHE_HOME}/snowflake/`.

- **Type:** Directory path
- **Default:** _(empty — falls back to `$HOME/.cache`)_

This is only used when `SF_TEMPORARY_CREDENTIAL_CACHE_DIR` is not set.

### HOME

User home directory. Used as the final fallback for resolving cache and configuration paths.

- **Type:** Directory path
- **Default:** _(system-provided)_
- **Windows equivalent:** `%USERPROFILE%`

The driver uses this to construct default paths:

| Purpose | Linux | macOS | Windows |
|---------|-------|-------|---------|
| Credential cache | `$HOME/.cache/snowflake/` | `$HOME/Library/Caches/Snowflake/` | `%LOCALAPPDATA%\Snowflake\Caches\` |
| CRL cache | `$HOME/.cache/snowflake/crls/` | `$HOME/Library/Caches/Snowflake/crls/` | `%LOCALAPPDATA%\Snowflake\Caches\crls\` |
| Configuration | `$HOME/.snowflake/` | `$HOME/.snowflake/` | `%USERPROFILE%\.snowflake\` |

**Cache directory resolution order:**
1. `SF_TEMPORARY_CREDENTIAL_CACHE_DIR` (highest priority)
2. `XDG_CACHE_HOME/snowflake`
3. Platform-specific default (see table above)

---

## Notes

- **Boolean parsing:** Only literal `true`/`True`/`TRUE` and `false`/`False`/`FALSE` are recognized. Values like `1`, `yes`, or `on` are not valid and will fall back to the default.
- **Integer parsing:** Values that overflow `int` or contain decimals are treated as invalid and fall back to the default.
- **Read timing:** Most variables are read once at startup or first use and cached. Changing them after the driver has initialized typically has no effect.
