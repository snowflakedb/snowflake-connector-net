namespace Snowflake.Data.Telemetry;

/// <summary>
/// Methods constructing an activity
/// </summary>
internal static class TelemetryActivities
{
    internal const string ExecuteNonQuery = "ExecuteNonQuery";
    internal const string ExecuteNonQueryAsync = "ExecuteNonQueryAsync";
    internal const string ExecuteScalar = "ExecuteScalar";
    internal const string ExecuteScalarAsync = "ExecuteScalarAsync";
    internal const string ExecuteDbDataReader = "ExecuteDbDataReader";
    internal const string ExecuteDbDataReaderAsync = "ExecuteDbDataReaderAsync";
    internal const string ExecuteInAsyncMode = "ExecuteInAsyncMode";
    internal const string ExecuteAsyncInAsyncMode = "ExecuteAsyncInAsyncMode";
    internal const string GetQueryStatus = "GetQueryStatus";
    internal const string GetQueryStatusAsync = "GetQueryStatusAsync";
    internal const string GetResultsFromQueryId = "GetResultsFromQueryId";
    internal const string GetResultsFromQueryIdAsync = "GetResultsFromQueryIdAsync";
}

/// <summary>
/// Field keys used in telemetry message payloads.
/// </summary>
internal static class TelemetryField
{
    internal const string Type = "type";
    internal const string DriverType = "DriverType";
    internal const string DriverVersion = "DriverVersion";
    internal const string Source = "Source";
    internal const string Duration = "DurationMs";

    // OT
    internal const string ScopeName = "otel.scope.name";
    internal const string ScopeVersion = "otel.scope.version";
    internal const string StatusCode = "otel.status_code";
    internal const string EventName = "otel.event.name";

    internal const string EventTime = "EventTime";
}

internal static class TelemetryTags
{
    internal const string DbSystem = "db.system";
    internal const string DbName = "db.namespace";
    internal const string DbWarehouse = "snowflake.warehouse";
    internal const string DbRole = "snowflake.role";
    internal const string SessionId = "snowflake.session.id";
    internal const string Snowflake = "snowflake";
    internal const string StatusCode = "status.code";
    internal const string StatusDescription = "status.description";
    internal const string Exception = "exception";
    internal const string ExceptionMessage = "exception.message";
    internal const string ExceptionErrorCode = "exception.errorcode";
}

internal static class TelemetryEvents
{
    internal const string RowCountOverflow = "RowCountOverflow";
    internal const string RowCountNegative = "RowCountNegative";
    internal const string DqlResultSetSkipped = "DqlResultSetSkipped";
}

internal static class TelemetryTransport
{
    internal const int TelemetryFlushSize = 10;
    internal const int TelemetryFlushIntervalInMs = 1000 * 60;
}
