namespace Snowflake.Data.Core.Telemetry
{
    /// <summary>
    /// Field keys used in telemetry message payloads.
    /// </summary>
    internal static class TelemetryField
    {
        internal const string Type = "type";
        internal const string Source = "source";
        internal const string DriverType = "DriverType";
        internal const string DriverVersion = "DriverVersion";
        internal const string QueryId = "QueryID";
        internal const string SqlState = "SQLState";
        internal const string ErrorNumber = "ErrorNumber";
        internal const string Stacktrace = "Stacktrace";
        internal const string Reason = "reason";
        internal const string Exception = "exception";
    }

    /// <summary>
    /// Event type values for the "type" field in telemetry messages.
    /// </summary>
    internal static class TelemetryEventType
    {
        internal const string SqlException = "client_sql_exception";
        internal const string HttpException = "client_http_exception";
    }
}
