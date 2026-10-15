## Logging

The Snowflake Connector for .NET can use its own logging implementation or a custom logger that implements the `ILogger` interface from `Microsoft.Extensions.Logging`.

Snowflake's built-in logger is **OFF** by default and can be enabled through a configuration file, as shown in the [Easy logging](https://github.com/snowflakedb/snowflake-connector-net/edit/SNOW-834781-Remove-log4net/doc/Logging.md#easy-logging) section

A custom logger can be enabled or disabled using the following methods:
```
SnowflakeDbLoggerConfig.SetCustomLogger(customILogger); // Enables the custom logger that implements the ILogger interface
SnowflakeDbLoggerConfig.ResetCustomLogger(); // Disables the custom logger
```

## Query Text and Parameter Logging

The driver can optionally log SQL query text and bind parameter values for debugging purposes. These features are **disabled by default** for security reasons.

**Warning**: Enabling query logging may expose sensitive data (PII, credentials, financial data) in your log output. Ensure logs are protected as confidential data.

### Configuration

Query logging can be configured via connection string, TOML configuration, or programmatically:

**Connection string:**
```csharp
conn.ConnectionString = "account=testaccount;user=testuser;password=XXXXX;" +
    "LOG_QUERY_TEXT=true;LOG_QUERY_PARAMETERS=true;LOG_MAX_QUERY_LENGTH=1000";
```

**TOML configuration (`connections.toml`):**
```toml
[default]
account = "testaccount"
user = "testuser"
log_query_text = true
log_query_parameters = true
log_max_query_length = 1000
```

**Programmatic (sets process-wide defaults):**
```csharp
SnowflakeDbLoggerConfig.SetLogQueryText(true);
SnowflakeDbLoggerConfig.SetLogQueryParameters(true);
SnowflakeDbLoggerConfig.SetLogMaxQueryLength(1000);
```

### Precedence

When the same parameter is set in multiple places, the highest-precedence value wins:

1. **Connection string** (highest)
2. **TOML configuration**
3. **Programmatic defaults** (lowest)

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `LOG_QUERY_TEXT` | bool | `false` | Log SQL query text at INFO level before each query execution. |
| `LOG_QUERY_PARAMETERS` | bool | `false` | Log bind parameter values at INFO level before each query execution. Requires `LOG_QUERY_TEXT=true`; ignored otherwise. |
| `LOG_MAX_QUERY_LENGTH` | int | `-1` | Maximum character length for logged query text and parameters. `-1` means no truncation. Must be `-1` (no limit) or at least `50`. Query text and parameters are truncated independently. |

### Log output

When enabled, query text and parameters are logged at **INFO** level right before each query is sent to the server:

```
[INFO] Query [<requestId>]: SELECT * FROM users WHERE id = ?
[INFO] Parameters [<requestId>]: [john, 42]
```

When truncation is active:

```
[INFO] Query [<requestId>]: SELECT * FROM u... [truncated from 5000 to 1000 chars]
```

The `requestId` can be used to correlate query text with parameters and with Snowflake's query history.

### Secret masking

All logged content passes through the driver's secret masking pipeline on a best-effort basis. The driver attempts to detect and redact patterns that look like passwords, tokens, keys, and other credentials before they are written to the log. However, this masking is **not guaranteed to catch every sensitive value** — it relies on pattern matching and may miss application-specific secrets, PII, or non-standard credential formats. You should treat query logs as confidential data regardless of masking, and ensure your logging infrastructure applies appropriate access controls and retention policies.

## Easy logging

The Easy Logging feature lets you change the log level for all driver classes and add an extra file appender for logs from the driver's classes at runtime. You can specify the log levels and the directory in which to save log files in a configuration file (default: `sf_client_config.json`).

You typically change log levels only when debugging your application.

**Note**
This logging configuration file features support only the following log levels:

- OFF
- ERROR
- WARNING
- INFO
- DEBUG
- TRACE

This configuration file uses JSON to define the `log_level`, `log_path` logging parameters, and `log_file_unix_permissions` as follows:

```json
{
  "common": {
    "log_level": "INFO",
    "log_path": "c:\\some-path\\some-directory"
  },
  "dotnet": {
	  "log_file_unix_permissions": 640
  }
}
```

where:

- `log_level` is the desired logging level.
- `log_path` is the location to store the log files. The driver automatically creates a `dotnet` subdirectory in the specified `log_path`. For example, if you set log_path to `c:\logs`, the drivers creates the `c:\logs\dotnet` directory and stores the logs there.
- `log_file_unix_permissions` is the desired log file permission level for Unix.

The driver looks for the location of the configuration file in the following order:

- `CLIENT_CONFIG_FILE` connection parameter, containing the full path to the configuration file (e.g. `"ACCOUNT=test;USER=test;PASSWORD=test;CLIENT_CONFIG_FILE=C:\\some-path\\client_config.json;"`)
- `SF_CLIENT_CONFIG_FILE` environment variable, containing the full path to the configuration file.
- .NET driver/application directory, where the file must be named `sf_client_config.json`.
- User’s home directory, where the file must be named `sf_client_config.json`.

**Note**
To enhance security, the driver no longer searches a temporary directory for easy logging configurations. Additionally, the driver now requires the logging configuration file on Unix-style systems to limit file permissions to allow only the file owner to modify the files (such as `chmod 0600` or `chmod 0644`).

To minimize the number of searches for a configuration file, the driver reads the file only for:

- The first connection.
- The first connection with `CLIENT_CONFIG_FILE` parameter.

The extra logs are stored in a `dotnet` subfolder of the specified directory, such as `C:\some-path\some-directory\dotnet`.

If a client uses the `log4net` library for application logging, enabling easy logging affects the log level in those logs as well.
