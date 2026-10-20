using System;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.Util;

internal sealed class SFSessionHttpClientPropertiesForTests : SFSessionHttpClientProperties
{
    public SFSessionHttpClientPropertiesForTests(
        bool clientSessionKeepAlive = false,
        bool validateDefaultParameters = true,
        SFSessionRetryProperties retryProperties = default,
        SFSessionHttpClientProxyProperties proxyProperties = null,
        int maxPoolSize = DefaultMaxPoolSize,
        int minPoolSize = DefaultMinPoolSize,
        ChangedSessionBehavior changedSession = DefaultChangedSession,
        TimeSpan waitingForSessionIdleTimeout = default,
        TimeSpan expirationTimeout = default,
        bool poolingEnabled = DefaultPoolingEnabled,
        bool disableSamlUrlCheck = false,
        bool clientStoreTemporaryCredential = false,
        int servicePointConnectionLimit = DefaultConnectionLimit,
        SFSessionCRLProperties crlProperties = default,
        SFSessionTLSProperties? tlsProperties = null,
        TimeSpan cleanupWait = default,
        TimeSpan abortRequestTimeout = default)
        : base(
            clientSessionKeepAlive,
            validateDefaultParameters,
            retryProperties,
            proxyProperties,
            maxPoolSize,
            minPoolSize,
            changedSession,
            waitingForSessionIdleTimeout,
            expirationTimeout,
            poolingEnabled,
            disableSamlUrlCheck,
            clientStoreTemporaryCredential,
            servicePointConnectionLimit,
            crlProperties,
            tlsProperties ?? new SFSessionTLSProperties("tls12", "tls13", false, null),
            cleanupWait,
            abortRequestTimeout)
    {
    }
}
