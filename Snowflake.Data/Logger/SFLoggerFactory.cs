using Microsoft.Extensions.Logging;

namespace Snowflake.Data.Log;

internal class SFLoggerFactory
{
    internal static ILogger CustomLogger = new LoggerEmptyImpl();

    internal static bool DefaultLogQueryText;
    internal static bool DefaultLogQueryParameters;
    internal static int DefaultLogMaxQueryLength = -1;

    internal static void SetDefaultLogQueryText(bool enabled) => DefaultLogQueryText = enabled;
    internal static void SetDefaultLogQueryParameters(bool enabled) => DefaultLogQueryParameters = enabled;
    internal static void SetDefaultLogMaxQueryLength(int maxLength) => DefaultLogMaxQueryLength = maxLength;

    internal static SFLogger GetLogger<T>() => new SFLoggerPair(GetSFLogger<T>());

    internal static SFLogger GetSFLogger<T>()
    {
        var logger = new SFLoggerImpl(typeof(T));
        return logger;
    }
}
