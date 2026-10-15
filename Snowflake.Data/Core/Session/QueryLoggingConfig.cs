using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session;

internal class QueryLoggingConfig
{
    public const int MaxQueryLengthPropertyMinValue = 50;
    private readonly bool? _logQueryText;
    private readonly int? _maxQueryLength;
    private readonly bool? _logQueryParameters;

    internal bool LogQueryText => _logQueryText ?? SFLoggerFactory.DefaultLogQueryText;

    internal bool LogQueryParameters => _logQueryParameters ?? SFLoggerFactory.DefaultLogQueryParameters;

    internal int MaxQueryLength => _maxQueryLength ?? SFLoggerFactory.DefaultLogMaxQueryLength;

    protected QueryLoggingConfig(bool logQueryText, bool logQueryParameters, int maxQueryLength)
    {
        _logQueryText = logQueryText;
        _logQueryParameters = logQueryParameters;
        _maxQueryLength = maxQueryLength;
    }

    private static QueryLoggingConfig Disabled => new(false, false, -1);

    internal static void ValidateAndExtract(SFSessionProperties properties, SFLogger logger, out QueryLoggingConfig config)
    {
        var logQueryText = ResolveBoolean(properties, SFSessionProperty.LOG_QUERY_TEXT) ?? SFLoggerFactory.DefaultLogQueryText;
        var logQueryParams = ResolveBoolean(properties, SFSessionProperty.LOG_QUERY_PARAMETERS) ?? SFLoggerFactory.DefaultLogQueryParameters;
        var maxQueryLength = ResolveMaxQueryLength(properties) ?? SFLoggerFactory.DefaultLogMaxQueryLength;

        if (!logQueryText)
        {
            if (logQueryParams)
                logger.Warn($"{SFSessionProperty.LOG_QUERY_PARAMETERS} is set but {nameof(SFSessionProperty.LOG_QUERY_TEXT)} is disabled - parameter logging requires query text logging, {SFSessionProperty.LOG_QUERY_PARAMETERS} will be ignored");

            if (maxQueryLength > 0)
                logger.Warn($"{nameof(SFSessionProperty.LOG_MAX_QUERY_LENGTH)} is set but {nameof(SFSessionProperty.LOG_QUERY_TEXT)} is disabled, {SFSessionProperty.LOG_MAX_QUERY_LENGTH} will be ignored");

            config = Disabled;
            return;
        }

        logger.Warn($"{nameof(SFSessionProperty.LOG_QUERY_TEXT)} is enabled: SQL query text will appear in logs - ensure logs are protected as confidential data");
        if (logQueryParams)
            logger.Warn($"{nameof(SFSessionProperty.LOG_QUERY_PARAMETERS)} is enabled: bind parameter values will appear in logs - ensure logs are protected as confidential data");

        config = new QueryLoggingConfig(true, logQueryParams, maxQueryLength);
    }

    private static bool? ResolveBoolean(SFSessionProperties properties, SFSessionProperty property)
    {
        var extractedValue = properties.ExtractPropertyOrDefault(property, null);
        if (extractedValue == null)
            return null;

        if (bool.TryParse(properties[property], out var parsed))
            return parsed;

        var exception = new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, $"Parameter {property} value should be parsable as boolean.");
        throw exception;
    }

    private static int? ResolveMaxQueryLength(SFSessionProperties properties)
    {
        var value = properties.ExtractPropertyOrEmptyString(SFSessionProperty.LOG_MAX_QUERY_LENGTH);
        if (string.IsNullOrEmpty(value))
            return null;

        if (!int.TryParse(value, out var parsed))
            throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING,
                $"Invalid value for {nameof(SFSessionProperty.LOG_MAX_QUERY_LENGTH)} must be an integer.");

        if (parsed != -1 && parsed < MaxQueryLengthPropertyMinValue)
            throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING,
                $"Invalid value for {nameof(SFSessionProperty.LOG_MAX_QUERY_LENGTH)}: must be -1 (no limit) or at least {MaxQueryLengthPropertyMinValue}.");

        return parsed;
    }
}
