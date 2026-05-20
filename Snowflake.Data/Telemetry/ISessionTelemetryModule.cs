using System;
using System.Diagnostics;

namespace Snowflake.Data.Telemetry;

internal interface ISessionTelemetryModule : IDisposable
{
    internal string SessionId { get; }
    internal string SessionToken { get; }
    internal bool IsDisposed { get; }

    internal int CurrentBufferSize { get; }

    internal bool IsServiceAvailable { get; }

    internal bool IsFlushInProgress { get; }

    internal void OnActivityStopped(Activity activity);
}
