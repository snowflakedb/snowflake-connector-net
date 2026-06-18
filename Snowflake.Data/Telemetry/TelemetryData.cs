using System.Collections.Generic;
using Newtonsoft.Json;

namespace Snowflake.Data.Telemetry;

/// <summary>
/// A single telemetry log entry consisting of a message and a timestamp.
/// Matches the wire format expected by the /telemetry/send endpoint.
/// </summary>
internal sealed class TelemetryData
{
    [JsonProperty(PropertyName = "message")]
    internal Dictionary<string, string> Message { get; }

    [JsonProperty(PropertyName = "timestamp")]
    internal long Timestamp { get; }

    internal TelemetryData(Dictionary<string, string> message, long timestampMs)
    {
        Message = message;
        Timestamp = timestampMs;
    }
}

/// <summary>
/// Wrapper for the telemetry send request body: {"logs": [...]}.
/// </summary>
internal sealed class TelemetryRequest
{
    [JsonProperty(PropertyName = "logs")]
    internal List<TelemetryData> Logs { get; }

    internal TelemetryRequest(List<TelemetryData> logs)
    {
        Logs = logs;
    }
}
