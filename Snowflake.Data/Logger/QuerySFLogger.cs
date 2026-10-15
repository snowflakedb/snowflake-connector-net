using System;
using System.Linq;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Logger;

internal sealed class QuerySFLogger : IQuerySFLogger
{
    private readonly QueryLoggingConfig _config;
    private readonly SFLogger _decorated;
    private readonly ISecretDetector _secretDetector;
    private const string TruncateFormat = "... [truncated from {0} to {1} chars]";

    internal QuerySFLogger(QueryLoggingConfig config, SFLogger decorated) : this(config, decorated, SecretDetector.Instance)
    {
    }

    internal QuerySFLogger(QueryLoggingConfig config, SFLogger decorated, ISecretDetector secretDetector)
    {
        _config = config;
        _decorated = decorated;
        _secretDetector = secretDetector;
    }

    public bool IsTraceEnabled() => _decorated.IsTraceEnabled();

    public bool IsDebugEnabled() => _decorated.IsDebugEnabled();

    public bool IsInfoEnabled() => _decorated.IsInfoEnabled();

    public bool IsWarnEnabled() => _decorated.IsWarnEnabled();

    public bool IsErrorEnabled() => _decorated.IsErrorEnabled();

    public void Trace(string msg, Exception ex = null) => _decorated.Trace(msg, ex);

    public void Debug(string msg, Exception ex = null) => _decorated.Debug(msg, ex);

    public void Info(string msg, Exception ex = null) => _decorated.Info(msg, ex);

    public void Warn(string msg, Exception ex = null) => _decorated.Warn(msg, ex);

    public void Error(string msg, Exception ex = null) => _decorated.Error(msg, ex);

    public void LogQuery(SFRestRequest request, string requestId)
    {
        if (!_config.LogQueryText || request.jsonBody is not QueryRequest queryRequest || !_decorated.IsInfoEnabled())
            return;

        LogQueryText(queryRequest, requestId);
        LogQueryParameters(queryRequest, requestId);
    }

    private void LogQueryText(QueryRequest queryRequest, string requestId)
    {
        if (string.IsNullOrEmpty(queryRequest.sqlText))
            return;

        var maskedText = _secretDetector.MaskSecrets(queryRequest.sqlText);

        if (maskedText.ErrStr != null)
        {
            _decorated.Error($"Exception occurred when trying to mask secrets in query. Details: {maskedText.ErrStr}");
            return;
        }

        var sanitized = SanitizeNewlines(maskedText.MaskedText);
        var truncated = Truncate(sanitized);
        var queryLog = $"Query [{requestId}]: {truncated}";
        _decorated.Info(queryLog);
    }

    private void LogQueryParameters(QueryRequest queryRequest, string requestId)
    {
        if (!_config.LogQueryParameters || queryRequest.parameterBindings is not { Count: > 0 })
            return;

        var paramBindings = queryRequest.parameterBindings.Values
            .Select(b => b.value?.ToString() ?? "null");
        var values = string.Join(", ", paramBindings);

        var maskedText = _secretDetector.MaskSecrets(values);
        if (maskedText.ErrStr != null)
        {
            _decorated.Error($"Exception occurred when trying to mask secrets in query. Details: {maskedText.ErrStr}");
            return;
        }

        var sanitized = SanitizeNewlines(maskedText.MaskedText);
        var truncated = Truncate(sanitized);
        var log = $"Parameters [{requestId}]: [{truncated}]";
        _decorated.Info(log);
    }

    private static string SanitizeNewlines(string value) =>
        value?.Replace("\r\n", "\\n").Replace("\r", "\\n").Replace("\n", "\\n");

    /// <summary>
    ///  Truncates the value with appended suffix to <see cref="QueryLoggingConfig.MaxQueryLength"/> length.
    /// </summary>
    private string Truncate(string value)
    {
        if (_config.MaxQueryLength == -1 || value.Length <= _config.MaxQueryLength)
            return value;

        var valueLength = value.Length.ToString();
        var maxQueryLengthStr = _config.MaxQueryLength.ToString();
        var truncateSuffix = string.Format(TruncateFormat, valueLength, maxQueryLengthStr);

        var truncateIndex = _config.MaxQueryLength - truncateSuffix.Length;
        return value.Substring(0, truncateIndex) + truncateSuffix;
    }
}
