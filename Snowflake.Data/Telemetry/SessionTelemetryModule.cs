using System;
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

internal sealed class SessionTelemetryModule : ISessionTelemetryModule
{
    private static readonly SFLogger s_logger = SFLoggerFactory.GetSFLogger<SessionTelemetryModule>();

    private static volatile int s_flushSize = 100;
    private static volatile int s_flushIntervalMs = 60 * 1_000;

    private readonly int _flushSize;
    private readonly TimeSpan _sendTimeout;
    private readonly LockObject _lock = new();
    private readonly string _sessionId;
    private readonly IRestRequester _restRequester;
    private volatile string _sessionToken;

    private readonly Uri _url;
    private readonly Timer _flushTimer;

    private List<TelemetryData> _buffer = new(s_flushSize);
    private volatile bool _isServiceAvailable = true;
    private volatile int _disposed;
    private volatile int _flushInProgress;
    private bool IsDisposed() => _disposed != 0;

    internal SessionTelemetryModule(SFSession session) : this(session, s_flushSize, TimeSpan.FromMilliseconds(s_flushIntervalMs))
    {
    }

    internal SessionTelemetryModule(SFSession session, int flushSize, TimeSpan flushInterval)
    {
        _flushSize = flushSize;
        _sessionId = session.sessionId;
        _sessionToken = session.sessionToken;
        _restRequester = session.restRequester;
        _url = session.BuildUri(RestPath.SF_TELEMETRY_PATH);
        _sendTimeout = session.connectionTimeout;
        _flushTimer = new Timer(_ => FlushTimerCallback(), null, flushInterval, flushInterval);
    }

    public static void SetFlushSize(int flushSize)
    {
        if (flushSize <= 0)
            throw new ArgumentException("Flush size must be greater than 0.");

        Interlocked.Exchange(ref s_flushSize, flushSize);
    }

    public static void SetFlushInterval(int flushIntervalInMilliseconds)
    {
        if (flushIntervalInMilliseconds <= 0)
            throw new ArgumentException("Flush interval must be greater than 0.");

        Interlocked.Exchange(ref s_flushIntervalMs, flushIntervalInMilliseconds);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            s_logger.Debug("Trying to dispose this component, but it is already disposed.");
            return;
        }

        _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
        _flushTimer.Dispose();
        Flush(true);
    }

    internal void UpdateToken(string sessionToken) => _sessionToken = sessionToken;

    internal void OnActivityStoppedImpl(Activity activity)
    {
        try
        {
            if (!IsTelemetryEnabled())
            {
                s_logger.Trace($"Recorded activity, but client telemetry is disabled. Activity will be ignored.");
                return;
            }

            var telemetryData = ConvertActivityToTelemetryData(activity);
            AddData(telemetryData);
        }
        catch (Exception ex)
        {
            s_logger.Debug($"Failed to process activity telemetry for session {_sessionId}: {ex.Message}.");
        }
    }

    internal async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (!IsTelemetryEnabled())
        {
            s_logger.Trace("Trying to flush telemetry, but client telemetry is disabled.");
            return;
        }

        List<TelemetryData> toSend;
        lock (_lock)
        {
            if (_buffer.Count == 0)
                return;

            toSend = _buffer;
            _buffer = new List<TelemetryData>(_flushSize);
        }

        try
        {
            var request = BuildSendRequest(toSend);
            var response = await _restRequester.PostAsync<NullDataResponse>(request, cancellationToken).ConfigureAwait(false);

            if (response is not { success: true })
            {
                s_logger.Warn($"Non-success response from telemetry server for session {_sessionId}. Disabling telemetry.");
                DisableTelemetry();
                return;
            }

            s_logger.Debug($"Successfully sent {toSend.Count} telemetry logs for session {_sessionId}.");
        }
        catch (Exception ex)
        {
            s_logger.Warn($"Failed to send telemetry for session {_sessionId}: {ex.Message}. Disabling telemetry.");
            DisableTelemetry();
        }
    }

    private void Flush(bool isCallingFromDispose = false)
    {
        if (!_isServiceAvailable || (!isCallingFromDispose && IsDisposed()))
        {
            s_logger.Trace("Trying to flush telemetry, but client telemetry is disabled.");
            return;
        }

        List<TelemetryData> toSend;
        lock (_lock)
        {
            if (_buffer.Count == 0)
                return;

            toSend = _buffer;
            _buffer = new List<TelemetryData>(_flushSize);
        }

        try
        {
            var request = BuildSendRequest(toSend);
            var response = _restRequester.Post<NullDataResponse>(request);

            if (response is not { success: true })
            {
                s_logger.Warn($"Non-success response from telemetry server for session {_sessionId}. Disabling telemetry.");
                DisableTelemetry();
                return;
            }

            s_logger.Debug($"Successfully sent {toSend.Count} telemetry logs for session {_sessionId}.");
        }
        catch (Exception ex)
        {
            s_logger.Warn($"Failed to send telemetry for session {_sessionId}: {ex.Message}. Disabling telemetry.");
            DisableTelemetry();
        }
    }


    private void AddData(IEnumerable<TelemetryData> data)
    {
        int count;
        lock (_lock)
        {
            _buffer.AddRange(data);
            count = _buffer.Count;
        }

        if (count < _flushSize)
            return;

        if (Interlocked.CompareExchange(ref _flushInProgress, 1, 0) != 0)
        {
            s_logger.Debug($"Collected enough data to send, but previous flush is still in progress. Buffer will grow.");
            return;
        }

        s_logger.Debug($"Auto-flushing telemetry batch of size {count} for session {_sessionId}.");
        _ = Task.Run(() =>
        {
            try { Flush(); }
            finally { Interlocked.Exchange(ref _flushInProgress, 0); }
        });
    }

    private bool IsTelemetryEnabled() => _isServiceAvailable && !IsDisposed();

    private void DisableTelemetry()
    {
        _isServiceAvailable = false;

        if (!IsDisposed())
            _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
    }

    private void FlushTimerCallback()
    {
        if (!IsTelemetryEnabled())
        {
            s_logger.Trace("Timer tries to flush client telemetry, but client telemetry is disabled.");
            return;
        }

        try
        {
            Flush();
        }
        catch (Exception ex)
        {
            s_logger.Warn($"Periodic flush failed for session {_sessionId}: {ex.Message}.");
        }
    }

    private static IEnumerable<TelemetryData> ConvertActivityToTelemetryData(Activity activity)
    {
        var timestamp = new DateTimeOffset(activity.StartTimeUtc);
        var activityEvent = new ActivityEvent(activity.DisplayName, timestamp, new ActivityTagsCollection(activity.TagObjects));
        var events = activity.Events.Prepend(activityEvent);
        foreach (var @event in events)
        {
            var message = new Dictionary<string, string>
            {
                { TelemetryField.Type, "client_activity" },
                { TelemetryField.Source, activity.Source.Name },
                { TelemetryField.ScopeName, activity.Source.Name },
                { TelemetryField.ScopeVersion, SFEnvironment.DriverVersion },
                { TelemetryField.DriverType, SFEnvironment.DriverName },
                { TelemetryField.DriverVersion, SFEnvironment.DriverVersion },
                { TelemetryField.Duration, activity.Duration.TotalMilliseconds.ToString("F0") },
                { TelemetryField.EventName, @event.Name},
                { TelemetryField.EventTime, @event.Timestamp.ToString()}
            };

            foreach (var tag in @event.Tags)
            {
                if (tag.Value is null)
                    continue;

                var maskedTagValue = SecretDetector.MaskSecrets(tag.Value.ToString());
                message[$"tag.{tag.Key}"] = maskedTagValue.maskedText;
            }

#if NET6_0_OR_GREATER
        message[TelemetryField.StatusCode] = activity.Status switch
        {
            ActivityStatusCode.Error => "ERROR",
            ActivityStatusCode.Ok => "OK",
            _ => "UNSET"
        };
#else
            var statusTag = activity.GetTagItem(TelemetryTags.StatusCode);
            message[TelemetryField.StatusCode] = statusTag?.ToString() ?? "UNSET";
#endif

            yield return new TelemetryData(message, @event.Timestamp.ToUnixTimeMilliseconds());
        }
    }

    private SFRestRequest BuildSendRequest(List<TelemetryData> data)
    {
        return new SFRestRequest
        {
            Url = _url,
            authorizationToken = $"Snowflake Token=\"{_sessionToken}\"",
            jsonBody = new TelemetryRequest(data),
            sid = _sessionId,
            RestTimeout = _sendTimeout,
        };
    }

    string ISessionTelemetryModule.SessionId => _sessionId;
    string ISessionTelemetryModule.SessionToken => _sessionToken;
    bool ISessionTelemetryModule.IsDisposed => IsDisposed();
    bool ISessionTelemetryModule.IsServiceAvailable => _isServiceAvailable;
    bool ISessionTelemetryModule.IsFlushInProgress => _flushInProgress != 0;

    int ISessionTelemetryModule.CurrentBufferSize
    {
        get
        {
            lock (_lock)
            {
                return _buffer.Count;
            }
        }
    }

    void ISessionTelemetryModule.OnActivityStopped(Activity activity) => OnActivityStoppedImpl(activity);
}
