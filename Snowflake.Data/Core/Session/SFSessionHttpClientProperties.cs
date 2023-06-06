using System;
using System.Collections.Generic;
using System.Threading;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core;

internal class SFSessionHttpClientProperties
{
    private static int recommendedMinTimeoutSec = BaseRestRequest.DEFAULT_REST_RETRY_SECONDS_TIMEOUT;
    private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SFSessionHttpClientProperties>();
    
    internal bool validateDefaultParameters;
    internal bool clientSessionKeepAlive;
    internal int timeoutInSec;
    internal bool insecureMode;
    internal bool disableRetry;
    internal bool forceRetryOn404;
    internal SFSessionHttpClientProxyProperties proxyProperties;
    
    internal void WarnOnTimeout()
    {
        if (timeoutInSec < recommendedMinTimeoutSec)
        {
            logger.Warn($"Connection timeout provided is less than recommended minimum value of" +
                        $" {recommendedMinTimeoutSec}");
        }
        if (timeoutInSec < 0)
        {
            logger.Warn($"Connection timeout provided is negative. Timeout will be infinite.");
        }                
    }
    
    internal TimeSpan TimeoutDuration()
    {
        return timeoutInSec > 0 ? TimeSpan.FromSeconds(timeoutInSec) : Timeout.InfiniteTimeSpan;
    }

    internal HttpClientConfig BuildHttpClientConfig()
    {
        return new HttpClientConfig(
            insecureMode,
            proxyProperties.proxyHost,
            proxyProperties.proxyPort,
            proxyProperties.proxyUser,
            proxyProperties.proxyPassword,
            proxyProperties.nonProxyHosts,
            disableRetry,
            forceRetryOn404);
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
        public SFSessionHttpClientProperties ExtractProperties(SFSessionProperties propertiesDictionary);
    }
    
    internal class Extractor: IExtractor
    {
        private SFSessionHttpClientProxyProperties.Extractor proxyPropertiesExtractor;

        public Extractor(SFSessionHttpClientProxyProperties.Extractor proxyPropertiesExtractor)
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
                proxyProperties = proxyPropertiesExtractor.ExtractProperties(propertiesDictionary)
            };
        }        
    }
}