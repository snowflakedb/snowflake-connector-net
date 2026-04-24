using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Log;
using LockObject =
#if NET10_0_OR_GREATER
    System.Threading.Lock;
#else
System.Object;
#endif

namespace Snowflake.Data.Telemetry;

/// <summary>
/// Static coordinator that owns the ActivityListener and routes activity events
/// to per-session <see cref="SessionTelemetryModule"/> instances.
/// </summary>
internal static class SnowflakeTelemetryModule
{
    private static readonly SFLogger s_logger = SFLoggerFactory.GetSFLogger<SessionTelemetryModule>();

    private static readonly LockObject s_lock = new();
    private static readonly Dictionary<string, SessionTelemetryModule> s_sessions = new();

    private static readonly string[] s_activitySources = [ActivityStarter.ActivitySourceName, ActivityStarter.ClientDefinedTelemetrySourceName];

    static SnowflakeTelemetryModule()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => s_activitySources.Contains(source?.Name),
            ActivityStarted = OnActivityStarted,
            ActivityStopped = OnActivityStopped,
            Sample = Sample,
        };
        ActivitySource.AddActivityListener(listener);
    }

    internal static void Register(SFSession session)
    {
        if (!session.IsClientTelemetryEnabled())
        {
            s_logger.Debug("Session tried to register to telemetry module, but it has client telemetry disabled!");
            return;
        }

        lock (s_lock)
        {
            if (s_sessions.TryGetValue(session.sessionId, out var existing))
            {
                s_logger.Debug("Session re-registered in client telemetry module.");
                existing.UpdateToken(session.sessionToken);
                return;
            }

            s_logger.Debug("Session registering in client telemetry module.");
            var module = new SessionTelemetryModule(session);
            s_sessions.Add(session.sessionId, module);
        }
    }

    internal static void Unregister(string sessionId)
    {
        s_logger.Debug($"Unregistering session with id: {sessionId} from client telemetry module.");
        SessionTelemetryModule module;
        lock (s_lock)
        {
            if (!s_sessions.TryGetValue(sessionId, out module))
                return;

            s_sessions.Remove(sessionId);
        }

        module.Dispose();
    }

    internal static async Task UnregisterAsync(string sessionId, CancellationToken cancellationToken)
    {
        s_logger.Debug($"Unregistering session with id: {sessionId} from client telemetry module.");
        SessionTelemetryModule module;
        lock (s_lock)
        {
            if (!s_sessions.TryGetValue(sessionId, out module))
                return;

            s_sessions.Remove(sessionId);
        }

        await module.FlushAsync(cancellationToken).ConfigureAwait(false);
        module.Dispose();
    }

    internal static bool TryGetSession(string sessionId, out ISessionTelemetryModule module)
    {
        module = null;
        SessionTelemetryModule concreteModule;
        lock (s_lock)
        {
            if (!s_sessions.TryGetValue(sessionId, out concreteModule))
                return false;
        }

        module = concreteModule;
        return true;
    }

    private static void OnActivityStopped(Activity activity)
    {
        if (!s_activitySources.Contains(activity.Source.Name))
        {
            s_logger.Debug($"Activity {activity.Source.Name} will not be handled by this component!");
            return;
        }

        var sessionId = activity.GetTagItem(TelemetryTags.SessionId) as string;
        if (string.IsNullOrEmpty(sessionId))
        {
            s_logger.Warn($"Activity with no {TelemetryTags.SessionId} recorded!");
            return;
        }

        SessionTelemetryModule module;
        lock (s_lock)
        {
            if (!s_sessions.TryGetValue(sessionId, out module))
            {
                s_logger.Warn($"Activity with no matching session telemetry module recorded! SessionId: {sessionId}");
                return;
            }
        }

        module.OnActivityStoppedImpl(activity);
    }

    private static void OnActivityStarted(Activity activity)
    {
        var sessionId = activity.GetTagItem(TelemetryTags.SessionId) as string;
        s_logger.Info($"Activity {activity.DisplayName} for session: {sessionId} started..");
    }

    private static ActivitySamplingResult Sample(ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData;
}
