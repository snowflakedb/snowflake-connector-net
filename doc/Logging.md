## Logging

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

This configuration file uses JSON to define the `log_level` and `log_path` logging parameters, as follows:

```json
{
  "common": {
    "log_level": "INFO",
    "log_path": "c:\\some-path\\some-directory"
  }
}
```

where:

- `log_level` is the desired logging level.
- `log_path` is the location to store the log files. The driver automatically creates a `dotnet` subdirectory in the specified `log_path`. For example, if you set log_path to `c:\logs`, the drivers creates the `c:\logs\dotnet` directory and stores the logs there.

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
