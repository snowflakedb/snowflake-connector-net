## Telemetry

The Snowflake Connector for .NET supports client-side telemetry based on `System.Diagnostics.Activity` (OpenTelemetry-compatible). When enabled, the driver automatically instruments command executions (queries, non-queries, async operations) and sends telemetry data to Snowflake.

You can also create your own custom activities to instrument application-level operations and have them sent to Snowflake alongside the driver's internal telemetry.

### Enabling telemetry

Client telemetry is controlled by the `CLIENT_TELEMETRY_ENABLED` parameter. It can be set in two ways:

1. **Connection string** (client-side default): `client_telemetry_enabled=true` in the connection string. Defaults to `true`.
2. **Server session parameter**: The server sends `CLIENT_TELEMETRY_ENABLED` in the login response, which overrides the connection string value. This parameter can be set at the account, database, or session level:

```sql
-- Disable at account level
ALTER ACCOUNT SET CLIENT_TELEMETRY_ENABLED = FALSE;

-- Disable at database level
ALTER DATABASE my_db SET CLIENT_TELEMETRY_ENABLED = FALSE;

-- Disable for current session
ALTER SESSION SET CLIENT_TELEMETRY_ENABLED = FALSE;
```

```csharp
// Explicitly enable (this is also the default)
var connectionString = "account=myaccount;user=myuser;password=mypass;client_telemetry_enabled=true;";

// Explicitly disable
var connectionString = "account=myaccount;user=myuser;password=mypass;client_telemetry_enabled=false;";
```

The driver automatically:
- Creates activities for every command execution (`ExecuteNonQuery`, `ExecuteScalar`, `ExecuteReader`, etc.)
- Enriches activities with session context (warehouse, role, database, session id)
- Buffers and sends telemetry data to Snowflake's `/telemetry/send` endpoint

### Sending custom telemetry events

You can create your own activities using the public `StartActivity` extension method on `SnowflakeDbCommand`. These activities are emitted on a separate activity source (`Client_custom_activity`) and are **sent to Snowflake** through the same telemetry pipeline as internal driver activities.

```csharp
using System.Diagnostics;
using Snowflake.Data.Client;
using Snowflake.Data.Telemetry;

using var connection = new SnowflakeDbConnection("account=myaccount;user=myuser;password=mypass;");
connection.Open();

using var command = (SnowflakeDbCommand)connection.CreateCommand();

// Start a custom activity — requires an open connection with telemetry enabled
using var activity = command.StartActivity("MyCustomOperation");

// Add your own tags
activity?.SetTag("app.module", "billing");
activity?.SetTag("app.batch_size", "500");

// Add named events within the activity
activity?.AddTelemetryEvent("ValidationComplete");

// Add events with tags
activity?.AddTelemetryEvent("BatchProcessed",
    new KeyValuePair<string, object>("batch.size", 500),
    new KeyValuePair<string, object>("batch.duration_ms", 1234));

// On success:
activity?.SetSuccess();

// Or on failure:
// activity?.SetException(exception);
```

### Server payload format

When the driver flushes telemetry to Snowflake's `/telemetry/send` endpoint, each activity event is converted to a JSON log entry. Below are examples showing how the C# API calls map to the server-side payload.

**Example 1: Activity with a simple event**

```csharp
using var activity = command.StartActivity("MyCustomOperation");
activity?.SetTag("app.module", "billing");
activity?.AddTelemetryEvent("ValidationComplete");
activity?.SetSuccess();
```

Produces two log entries — a synthetic activity event (carrying activity-level tags) and the explicit event:
```json
[
    { "message": { "otel.event.name": "MyCustomOperation", "otel.status_code": "OK", "tag.app.module": "billing", "tag.db.system": "snowflake", "tag.snowflake.session.id": <session_id>, ... } },
    { "message": { "otel.event.name": "ValidationComplete", "otel.status_code": "OK", ... } }
]
```

**Example 2: Activity with multiple events (produces multiple log entries)**

```csharp
using var activity = command.StartActivity("MultiStepImport");
activity?.AddTelemetryEvent("StepOneComplete");
activity?.AddTelemetryEvent("StepTwoComplete");
activity?.SetSuccess();
```

Produces **three** log entries — synthetic activity event + one per explicit event:
```json
[
    { "message": { "otel.event.name": "MultiStepImport", "otel.status_code": "OK", "tag.db.system": "snowflake", "tag.snowflake.session.id": <session_id>, ... } },
    { "message": { "otel.event.name": "StepOneComplete", "otel.status_code": "OK", ... } },
    { "message": { "otel.event.name": "StepTwoComplete", "otel.status_code": "OK", ... } }
]
```

**Example 3: Activity with no explicit events (synthetic event)**

If no events are added, only the synthetic activity event is produced:
```csharp
using var activity = command.StartActivity("SimpleQuery");
activity?.SetSuccess();
```

```json
{ "message": { "otel.event.name": "SimpleQuery", "otel.status_code": "OK", "tag.db.system": "snowflake", "tag.snowflake.session.id": <session_id>, ... } }
```

**Example 4: Failed activity**

```csharp
using var activity = command.StartActivity("FailedOp");
activity?.SetException(new InvalidOperationException("something broke"));
```

Produces two log entries — synthetic activity event + the exception event:
```json
[
    { "message": { "otel.event.name": "FailedOp", "otel.status_code": "ERROR", "tag.status.code": "ERROR", "tag.db.system": "snowflake", ... } },
    { "message": { "otel.event.name": "exception", "otel.status_code": "ERROR", "tag.exception": "System.InvalidOperationException", ... } }
]
```

**Example 5: Activity with detailed events**
```csharp
using var activity = command.StartActivity("MyCustomOperationWithEvents");
activity?.SetTag("app.module", "billing_extra");
activity?.AddTelemetryEvent("ValidationStarted", new KeyValuePair<string, object>("Mode", "Basic"));
activity?.AddTelemetryEvent("ValidationConfirmed", new Dictionary<string, object>
{
    ["Mode"] = "Advanced",
    ["ConfirmedBy"] = "John"
});
activity?.AddTelemetryEvent("ValidationComplete");
activity?.SetSuccess();
```

Produces four log entries — synthetic activity event (with activity tags) + one per explicit event (with event tags):
```json
{ "message": { "otel.event.name": "MyCustomOperationWithEvents", "otel.status_code": "OK", "tag.app.module": "billing_extra", "tag.db.system": "snowflake", ... } }
{ "message": { "otel.event.name": "ValidationStarted", "otel.status_code": "OK", "tag.Mode": "Basic", ... } }
{ "message": { "otel.event.name": "ValidationConfirmed", "otel.status_code": "OK", "tag.Mode": "Advanced", "tag.ConfirmedBy": "John", ... } }
{ "message": { "otel.event.name": "ValidationComplete", "otel.status_code": "OK", ... } }
```

### Listening to activities externally

In addition to the built-in Snowflake telemetry pipeline, you can subscribe to either activity source with your own `ActivityListener` or OpenTelemetry SDK — for example, to export spans to your own observability backend.

```csharp
using System.Diagnostics;
using Snowflake.Data.Telemetry;

var listener = new ActivityListener
{
    ShouldListenTo = source => source.Name == ActivityStarter.CustomSourceName,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
    ActivityStopped = activity =>
    {
        Console.WriteLine($"Activity: {activity.OperationName}");
        Console.WriteLine($"  Duration: {activity.Duration.TotalMilliseconds}ms");
        Console.WriteLine($"  Session: {activity.GetTagItem(TelemetryTags.SessionId)}");
        Console.WriteLine($"  Status: {activity.GetTagItem(TelemetryTags.StatusCode)}");
    }
};
ActivitySource.AddActivityListener(listener);
```

If you use the OpenTelemetry SDK, add the source name to your tracer provider configuration:

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;

var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource(ActivityStarter.CustomSourceName)  // "Client_custom_activity"
    .AddConsoleExporter()
    .Build();
```

### Activity sources

| Source name | Description |
|---|---|
| `Snowflake_dotnet_activity` | Internal driver telemetry (auto-instrumented commands). Sent to Snowflake. |
| `Client_custom_activity` | User-created activities via `command.StartActivity()`. Also sent to Snowflake. You can additionally subscribe with your own listener or OTel SDK. |

### Session tags

Every activity (internal and custom) is automatically enriched with:

| Tag | Description |
|---|---|
| `db.system` | Always `"snowflake"` |
| `db.namespace` | Database name |
| `snowflake.warehouse` | Warehouse name |
| `snowflake.role` | Role name |
| `snowflake.session.id` | Session identifier |
