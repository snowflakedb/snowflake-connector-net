using System;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client;

/// <summary>
/// Provides static methods to configure the Snowflake .NET driver logging behavior,
/// including custom logger registration and query text/parameter logging defaults.
/// </summary>
public class SnowflakeDbLoggerConfig
{
    /// <summary>
    /// Resets the custom logger to a no-op implementation, disabling any previously registered custom logger.
    /// </summary>
    public static void ResetCustomLogger() => SFLoggerFactory.CustomLogger = new LoggerEmptyImpl();

    /// <summary>
    /// Registers a custom <see cref="ILogger"/> implementation to receive driver log output.
    /// </summary>
    /// <param name="customLogger">The custom logger instance to use.</param>
    public static void SetCustomLogger(ILogger customLogger) => SFLoggerFactory.CustomLogger = customLogger;

    /// <summary>
    /// Sets the process-wide default for logging SQL query text before execution.
    /// Can be overridden per-connection via the <c>LOG_QUERY_TEXT</c> connection string property.
    /// </summary>
    /// <param name="enabled"><c>true</c> to enable query text logging; <c>false</c> to disable.</param>
    public static void SetLogQueryText(bool enabled) => SFLoggerFactory.SetDefaultLogQueryText(enabled);

    /// <summary>
    /// Sets the process-wide default for logging bind parameter values before execution.
    /// Requires <see cref="SetLogQueryText"/> to also be enabled; ignored otherwise.
    /// Can be overridden per-connection via the <c>LOG_QUERY_PARAMETERS</c> connection string property.
    /// </summary>
    /// <param name="enabled"><c>true</c> to enable parameter logging; <c>false</c> to disable.</param>
    public static void SetLogQueryParameters(bool enabled) => SFLoggerFactory.SetDefaultLogQueryParameters(enabled);

    /// <summary>
    /// Sets the process-wide default maximum character length for logged query text and parameters.
    /// Values exceeding this limit are truncated with an indicator.
    /// Can be overridden per-connection via the <c>LOG_MAX_QUERY_LENGTH</c> connection string property.
    /// </summary>
    /// <param name="maxLength">Maximum characters to log. Use <c>-1</c> for no truncation, or at least 50.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxLength"/> is not -1 and is less than 50.</exception>
    public static void SetLogMaxQueryLength(int maxLength)
    {
        if (maxLength != -1 && maxLength < 50)
            throw new ArgumentOutOfRangeException(nameof(maxLength), maxLength, "Must be -1 (no limit) or at least 50.");
        SFLoggerFactory.SetDefaultLogMaxQueryLength(maxLength);
    }
}
