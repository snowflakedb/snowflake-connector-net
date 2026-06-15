namespace Snowflake.Data.Telemetry;

/// <summary>
/// Provides public configuration methods for the session telemetry module.
/// Changes apply only to sessions created after the configuration is set.
/// </summary>
public static class SessionTelemetryModuleFacade
{
    /// <summary>
    /// Sets the number of buffered activities that triggers an automatic flush. Default is 100.
    /// Only affects sessions created after this call.
    /// </summary>
    /// <param name="flushSize">The buffer size threshold. Must be greater than 0.</param>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="flushSize"/> is less than or equal to 0.</exception>
    public static void SetFlushSize(int flushSize) => SessionTelemetryModule.SetFlushSize(flushSize);

    /// <summary>
    /// Sets the periodic flush interval. Default is 60000 ms (1 minute).
    /// Only affects sessions created after this call.
    /// </summary>
    /// <param name="flushIntervalInMilliseconds">The flush interval in milliseconds. Must be greater than 0.</param>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="flushIntervalInMilliseconds"/> is less than or equal to 0.</exception>
    public static void SetFlushInterval(int flushIntervalInMilliseconds) => SessionTelemetryModule.SetFlushInterval(flushIntervalInMilliseconds);
}
