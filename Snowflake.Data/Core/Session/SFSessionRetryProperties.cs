using System;

namespace Snowflake.Data.Core.Session;

internal readonly record struct SFSessionRetryProperties(
    TimeSpan ConnectionTimeout,
    bool DisableRetry,
    bool ForceRetryOn404,
    TimeSpan RetryTimeout,
    int MaxHttpRetries,
    bool IncludeRetryReason)
{

    internal interface IExtractor
    {
        SFSessionRetryProperties ExtractProperties(SFSessionProperties propertiesDictionary, SessionPropertiesWithDefaultValuesExtractor defaultValuesExtractor);
    }

    internal class Extractor : IExtractor
    {
        public SFSessionRetryProperties ExtractProperties(SFSessionProperties propertiesDictionary, SessionPropertiesWithDefaultValuesExtractor defaultValuesExtractor)
        {
            return new SFSessionRetryProperties(
                ConnectionTimeout: defaultValuesExtractor.ExtractTimeout(SFSessionProperty.CONNECTION_TIMEOUT),
                DisableRetry: bool.Parse(propertiesDictionary[SFSessionProperty.DISABLERETRY]),
                ForceRetryOn404: bool.Parse(propertiesDictionary[SFSessionProperty.FORCERETRYON404]),
                RetryTimeout: defaultValuesExtractor.ExtractTimeout(SFSessionProperty.RETRY_TIMEOUT),
                MaxHttpRetries: int.Parse(propertiesDictionary[SFSessionProperty.MAXHTTPRETRIES]),
                IncludeRetryReason: bool.Parse(propertiesDictionary[SFSessionProperty.INCLUDERETRYREASON]));
        }
    }
}
