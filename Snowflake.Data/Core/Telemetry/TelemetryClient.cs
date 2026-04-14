using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Telemetry
{
    /// <summary>
    /// In-band telemetry client that batches log entries and sends them
    /// to the Snowflake /telemetry/send endpoint using the session token.
    /// Thread-safe. Self-disables on send failure. Never throws to callers.
    /// </summary>
    internal class TelemetryClient
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<TelemetryClient>();

        internal const int DefaultFlushSize = 100;

        private readonly object _lock = new object();
        private readonly IRestRequester _restRequester;
        private readonly SFSession _session;
        private readonly int _flushSize;

        private List<TelemetryData> _logBatch = new List<TelemetryData>();
        private volatile bool _isClosed;

        /// <summary>
        /// When false, the telemetry service has been disabled (either by the server
        /// via CLIENT_TELEMETRY_ENABLED=false, or because a send failed).
        /// </summary>
        private volatile bool _isTelemetryServiceAvailable = true;

        internal TelemetryClient(SFSession session, IRestRequester restRequester, int flushSize = DefaultFlushSize)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _restRequester = restRequester ?? throw new ArgumentNullException(nameof(restRequester));
            _flushSize = flushSize;
        }

        /// <summary>
        /// Whether telemetry is enabled. Checks both the server parameter and the service availability flag.
        /// </summary>
        internal bool IsTelemetryEnabled()
        {
            if (!_isTelemetryServiceAvailable)
                return false;

            if (_session.ParameterMap.TryGetValue(SFSessionParameter.CLIENT_TELEMETRY_ENABLED, out var value))
            {
                if (bool.TryParse(value?.ToString(), out var enabled))
                    return enabled;
            }

            // Default: enabled (matches JDBC/Go behavior)
            return true;
        }

        /// <summary>
        /// Add a telemetry log entry to the batch.
        /// If the batch reaches the flush size, sends asynchronously.
        /// Silently no-ops if telemetry is disabled or closed.
        /// </summary>
        internal void AddLog(TelemetryData data)
        {
            try
            {
                if (_isClosed || !IsTelemetryEnabled())
                    return;

                int batchSize;
                lock (_lock)
                {
                    _logBatch.Add(data);
                    batchSize = _logBatch.Count;
                }

                if (batchSize == _flushSize)
                {
                    logger.Debug($"Force flushing telemetry batch of size: {batchSize}");
                    _ = Task.Run(async () => await SendBatchAsync().ConfigureAwait(false));
                }
            }
            catch (Exception ex)
            {
                logger.Debug($"Failed to add telemetry log: {ex.Message}");
            }
        }

        /// <summary>
        /// Send all cached logs to the server. Swaps the batch under the lock,
        /// then sends without holding the lock.
        /// </summary>
        internal bool SendBatch()
        {
            if (_isClosed || !IsTelemetryEnabled())
                return false;

            List<TelemetryData> toSend;
            lock (_lock)
            {
                toSend = _logBatch;
                _logBatch = new List<TelemetryData>();
            }

            if (toSend.Count == 0)
            {
                logger.Debug("Nothing to send to telemetry.");
                return true;
            }

            try
            {
                var request = BuildSendRequest(toSend);
                var response = _restRequester.Post<NullDataResponse>(request);

                if (response == null || !response.success)
                {
                    logger.Info("Non-success response from telemetry server. Disabling telemetry.");
                    _isTelemetryServiceAvailable = false;
                    return false;
                }

                logger.Debug($"Successfully sent {toSend.Count} telemetry logs.");
                return true;
            }
            catch (Exception ex)
            {
                logger.Debug($"Failed to send telemetry batch: {ex.Message}. Disabling telemetry.");
                _isTelemetryServiceAvailable = false;
                return false;
            }
        }

        /// <summary>
        /// Async variant of SendBatch.
        /// </summary>
        internal async Task<bool> SendBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_isClosed || !IsTelemetryEnabled())
                return false;

            List<TelemetryData> toSend;
            lock (_lock)
            {
                toSend = _logBatch;
                _logBatch = new List<TelemetryData>();
            }

            if (toSend.Count == 0)
            {
                logger.Debug("Nothing to send to telemetry.");
                return true;
            }

            try
            {
                var request = BuildSendRequest(toSend);
                var response = await _restRequester.PostAsync<NullDataResponse>(request, cancellationToken).ConfigureAwait(false);

                if (response == null || !response.success)
                {
                    logger.Info("Non-success response from telemetry server. Disabling telemetry.");
                    _isTelemetryServiceAvailable = false;
                    return false;
                }

                logger.Debug($"Successfully sent {toSend.Count} telemetry logs.");
                return true;
            }
            catch (Exception ex)
            {
                logger.Debug($"Failed to send telemetry batch: {ex.Message}. Disabling telemetry.");
                _isTelemetryServiceAvailable = false;
                return false;
            }
        }

        /// <summary>
        /// Flush remaining logs and mark the client as closed (sync).
        /// </summary>
        internal void Close()
        {
            if (_isClosed)
                return;

            try
            {
                SendBatch();
            }
            catch (Exception ex)
            {
                logger.Debug($"Error flushing telemetry on close: {ex.Message}");
            }
            finally
            {
                _isClosed = true;
            }
        }

        /// <summary>
        /// Flush remaining logs and mark the client as closed (async).
        /// </summary>
        internal async Task CloseAsync(CancellationToken cancellationToken = default)
        {
            if (_isClosed)
                return;

            try
            {
                await SendBatchAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.Debug($"Error flushing telemetry on close: {ex.Message}");
            }
            finally
            {
                _isClosed = true;
            }
        }

        /// <summary>
        /// Disable the telemetry service (e.g. when the server parameter changes).
        /// </summary>
        internal void Disable()
        {
            _isTelemetryServiceAvailable = false;
        }

        internal bool IsClosed => _isClosed;

        /// <summary>
        /// Number of logs currently in the buffer. Primarily for testing.
        /// </summary>
        internal int BufferSize
        {
            get
            {
                lock (_lock)
                {
                    return _logBatch.Count;
                }
            }
        }

        private SFRestRequest BuildSendRequest(List<TelemetryData> logs)
        {
            return new SFRestRequest
            {
                Url = _session.BuildUri(RestPath.SF_TELEMETRY_PATH),
                authorizationToken = string.Format("Snowflake Token=\"{0}\"", _session.sessionToken),
                jsonBody = new TelemetryRequest(logs),
                sid = _session.sessionId,
                RestTimeout = TimeSpan.FromSeconds(30),
                HttpTimeout = TimeSpan.FromSeconds(10)
            };
        }
    }
}
