## Telemetry

The Snowflake Connector for .NET supports client-side telemetry based on `System.Diagnostics.Activity` (OpenTelemetry-compatible). When enabled, the driver automatically instruments command executions (queries, non-queries, async operations) and sends telemetry data to Snowflake.

You can also create your own custom activities to instrument application-level operations and have them sent to Snowflake alongside the driver's internal telemetry.

### Enabling telemetry

Client telemetry is controlled by the `CLIENT_TELEMETRY_ENABLED` parameter. It can be set in two ways:

1. **Connection string** (client-side default): `client_telemetry_enabled=true` in the connection string. Defaults to `true`.
2. **Server session parameter**: The server sends `CLIENT_TELEMETRY_ENABLED` in the login response, which overrides the connection string value.

```
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

// On success:
activity?.SetSuccess();

// Or on failure:
// activity?.SetException(exception);
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
