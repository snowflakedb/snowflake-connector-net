using System;
using System.Collections.Generic;
using System.Threading;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{

    internal class SFSessionHttpClientProperties
    {
        internal static readonly int s_maxHttpRetriesDefault = 7;
        internal static readonly int s_retryTimeoutDefault = 300;
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSessionHttpClientProperties>();

        internal bool validateDefaultParameters;
        internal bool clientSessionKeepAlive;
        internal int timeoutInSec;
        internal bool insecureMode;
        internal bool disableRetry;
        internal bool forceRetryOn404;
        internal int retryTimeout;
        internal int maxHttpRetries;
        internal bool includeRetryReason;
        internal SFSessionHttpClientProxyProperties proxyProperties;

        internal void CheckPropertiesAreValid()
        {
            if (timeoutInSec < s_retryTimeoutDefault)
            {
                logger.Warn($"Connection timeout provided is less than recommended minimum value of" +
                            $" {s_retryTimeoutDefault}");
            }

            if (timeoutInSec < 0)
            {
                logger.Warn($"Connection timeout provided is negative. Timeout will be infinite.");
            }

            if (retryTimeout > 0 && retryTimeout < s_retryTimeoutDefault)
            {
                logger.Warn($"Max retry timeout provided is less than the allowed minimum value of" +
                            $" {s_retryTimeoutDefault}");

                retryTimeout = s_retryTimeoutDefault;
            }
            else if (retryTimeout == 0)
            {
                logger.Warn($"Max retry timeout provided is 0. Timeout will be infinite");
            }

            // Use the shorter timeout between CONNECTION_TIMEOUT and RETRY_TIMEOUT
            if (retryTimeout < timeoutInSec)
            {
                timeoutInSec = retryTimeout;
            }

            if (maxHttpRetries > 0 && maxHttpRetries < s_maxHttpRetriesDefault)
            {
                logger.Warn($"Max retry count provided is less than the allowed minimum value of" +
            $" {s_maxHttpRetriesDefault}");

                maxHttpRetries = s_maxHttpRetriesDefault;
            }
            else if (maxHttpRetries == 0)
            {
                logger.Warn($"Max retry count provided is 0. Retry count will be infinite");
            }
        }

        internal TimeSpan TimeoutDuration()
        {
            return timeoutInSec > 0 ? TimeSpan.FromSeconds(timeoutInSec) : Timeout.InfiniteTimeSpan;
        }

        internal HttpClientConfig BuildHttpClientConfig()
        {
            return new HttpClientConfig(
                !insecureMode,
                proxyProperties.proxyHost,
                proxyProperties.proxyPort,
                proxyProperties.proxyUser,
                proxyProperties.proxyPassword,
                proxyProperties.nonProxyHosts,
                disableRetry,
                forceRetryOn404,
                maxHttpRetries,
                includeRetryReason);
        }

        internal Dictionary<SFSessionParameter, object> ToParameterMap()
        {
            var parameterMap = new Dictionary<SFSessionParameter, object>();
            parameterMap[SFSessionParameter.CLIENT_VALIDATE_DEFAULT_PARAMETERS] = validateDefaultParameters;
            parameterMap[SFSessionParameter.CLIENT_SESSION_KEEP_ALIVE] = clientSessionKeepAlive;
            return parameterMap;
        }

        internal interface IExtractor
        {
            SFSessionHttpClientProperties ExtractProperties(SFSessionProperties propertiesDictionary);
        }

        internal class Extractor : IExtractor
        {
            private SFSessionHttpClientProxyProperties.IExtractor proxyPropertiesExtractor;

            public Extractor(SFSessionHttpClientProxyProperties.IExtractor proxyPropertiesExtractor)
            {
                this.proxyPropertiesExtractor = proxyPropertiesExtractor;
            }

            public SFSessionHttpClientProperties ExtractProperties(SFSessionProperties propertiesDictionary)
            {
                return new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = Boolean.Parse(propertiesDictionary[SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS]),
                    clientSessionKeepAlive = Boolean.Parse(propertiesDictionary[SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE]),
                    timeoutInSec = int.Parse(propertiesDictionary[SFSessionProperty.CONNECTION_TIMEOUT]),
                    insecureMode = Boolean.Parse(propertiesDictionary[SFSessionProperty.INSECUREMODE]),
                    disableRetry = Boolean.Parse(propertiesDictionary[SFSessionProperty.DISABLERETRY]),
                    forceRetryOn404 = Boolean.Parse(propertiesDictionary[SFSessionProperty.FORCERETRYON404]),
                    retryTimeout = int.Parse(propertiesDictionary[SFSessionProperty.RETRY_TIMEOUT]),
                    maxHttpRetries = int.Parse(propertiesDictionary[SFSessionProperty.MAXHTTPRETRIES]),
                    includeRetryReason = Boolean.Parse(propertiesDictionary[SFSessionProperty.INCLUDERETRYREASON]),
                    proxyProperties = proxyPropertiesExtractor.ExtractProperties(propertiesDictionary)
                };
            }
        }
    }
}