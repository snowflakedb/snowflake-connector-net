using System;
using System.Collections.Generic;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{

    internal class SFSessionHttpClientProperties
    {
        private static readonly Extractor s_propertiesExtractor = new Extractor(new SFSessionHttpClientProxyProperties.Extractor());
        public const int DefaultMaxPoolSize = 10;
        public const int DefaultMinPoolSize = 2;
        public const ChangedSessionBehavior DefaultChangedSession = ChangedSessionBehavior.Destroy;
        public static readonly TimeSpan DefaultWaitingForIdleSessionTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromMinutes(5);
        public static readonly TimeSpan DefaultExpirationTimeout = TimeSpan.FromHours(1);
        public const bool DefaultPoolingEnabled = true;
        public const int DefaultMaxHttpRetries = 7;
        public static readonly TimeSpan DefaultRetryTimeout = TimeSpan.FromSeconds(300);
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFSessionHttpClientProperties>();

        internal bool validateDefaultParameters;
        internal bool clientSessionKeepAlive;
        internal TimeSpan connectionTimeout;
        internal bool insecureMode;
        internal bool disableRetry;
        internal bool forceRetryOn404;
        internal TimeSpan retryTimeout;
        internal int maxHttpRetries;
        internal bool includeRetryReason;
        internal SFSessionHttpClientProxyProperties proxyProperties;
        internal bool _disableSamlUrlCheck;
        private int _maxPoolSize;
        private int _minPoolSize;
        private ChangedSessionBehavior _changedSession;
        private TimeSpan _waitingForSessionIdleTimeout;
        private TimeSpan _expirationTimeout;
        private bool _poolingEnabled;

        public static SFSessionHttpClientProperties ExtractAndValidate(SFSessionProperties properties)
        {
            var extractedProperties = s_propertiesExtractor.ExtractProperties(properties);
            extractedProperties.CheckPropertiesAreValid();
            return extractedProperties;
        }

        public void DisablePoolingDefaultIfSecretsProvidedExternally(SFSessionProperties properties)
        {
            var authenticator = properties[SFSessionProperty.AUTHENTICATOR].ToLower();
            if (ExternalBrowserAuthenticator.AUTH_NAME.Equals(authenticator))
            {
                DisablePoolingIfNotExplicitlyEnabled(properties, "external browser");

            } else if (KeyPairAuthenticator.AUTH_NAME.Equals(authenticator)
                       && properties.IsNonEmptyValueProvided(SFSessionProperty.PRIVATE_KEY_FILE)
                       && !properties.IsNonEmptyValueProvided(SFSessionProperty.PRIVATE_KEY_PWD))
            {
                DisablePoolingIfNotExplicitlyEnabled(properties, "key pair with private key in a file");
            }
        }

        private void DisablePoolingIfNotExplicitlyEnabled(SFSessionProperties properties, string authenticationDescription)
        {
            if (!properties.IsPoolingEnabledValueProvided && _poolingEnabled)
            {
                _poolingEnabled = false;
                s_logger.Info($"Disabling connection pooling for {authenticationDescription} authentication");
            }
            else if (properties.IsPoolingEnabledValueProvided && _poolingEnabled)
            {
                s_logger.Warn($"Connection pooling is enabled for {authenticationDescription} authentication which is not recommended");
            }
        }

        private void CheckPropertiesAreValid()
        {
            try
            {
                ValidateConnectionTimeout();
                ValidateRetryTimeout();
                ShortenConnectionTimeoutByRetryTimeout();
                ValidateHttpRetries();
                ValidateMinMaxPoolSize();
                ValidateWaitingForSessionIdleTimeout();
            }
            catch (SnowflakeDbException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new SnowflakeDbException(SFError.INVALID_CONNECTION_STRING, exception);
            }
        }

        private void ValidateConnectionTimeout()
        {
            if (TimeoutHelper.IsZeroLength(connectionTimeout))
            {
                s_logger.Warn("Connection timeout provided is 0. Timeout will be infinite");
                connectionTimeout = TimeoutHelper.Infinity();
            }
            else if (TimeoutHelper.IsInfinite(connectionTimeout))
            {
                s_logger.Warn("Connection timeout provided is negative. Timeout will be infinite.");
            }
            if (!TimeoutHelper.IsInfinite(connectionTimeout) && connectionTimeout < DefaultRetryTimeout)
            {
                s_logger.Warn($"Connection timeout provided is less than recommended minimum value of {DefaultRetryTimeout}");
            }
        }

        private void ValidateRetryTimeout()
        {
            if (retryTimeout.TotalMilliseconds > 0 && retryTimeout < DefaultRetryTimeout)
            {
                s_logger.Warn($"Max retry timeout provided is less than the allowed minimum value of {DefaultRetryTimeout}");
                retryTimeout = DefaultRetryTimeout;
            }
            else if (TimeoutHelper.IsZeroLength(retryTimeout))
            {
                s_logger.Warn($"Max retry timeout provided is 0. Timeout will be infinite");
                retryTimeout = TimeoutHelper.Infinity();
            }
            else if (TimeoutHelper.IsInfinite(retryTimeout))
            {
                s_logger.Warn($"Max retry timeout provided is negative. Timeout will be infinite");
            }
        }

        private void ShortenConnectionTimeoutByRetryTimeout()
        {
            if (!TimeoutHelper.IsInfinite(retryTimeout) && retryTimeout < connectionTimeout)
            {
                s_logger.Warn($"Connection timeout greater than retry timeout. Setting connection time same as retry timeout");
                connectionTimeout = retryTimeout;
            }
        }

        private void ValidateHttpRetries()
        {
            if (maxHttpRetries > 0 && maxHttpRetries < DefaultMaxHttpRetries)
            {
                    s_logger.Warn($"Max retry count provided is less than the allowed minimum value of {DefaultMaxHttpRetries}");

                maxHttpRetries = DefaultMaxHttpRetries;
            }
            else if (maxHttpRetries == 0)
            {
                s_logger.Warn($"Max retry count provided is 0. Retry count will be infinite");
            }
        }

        private void ValidateMinMaxPoolSize()
        {
            if (_minPoolSize > _maxPoolSize)
            {
                throw new Exception("MinPoolSize cannot be greater than MaxPoolSize");
            }
        }

        private void ValidateWaitingForSessionIdleTimeout()
        {
            if (TimeoutHelper.IsInfinite(_waitingForSessionIdleTimeout))
            {
                throw new Exception("Waiting for idle session timeout cannot be infinite");
            }
            if (TimeoutHelper.IsZeroLength(_waitingForSessionIdleTimeout))
            {
                s_logger.Warn("Waiting for idle session timeout is 0. There will be no waiting for idle session");
            }
        }

        public HttpClientConfig BuildHttpClientConfig()
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

        public ConnectionPoolConfig BuildConnectionPoolConfig() =>
            new ConnectionPoolConfig()
            {
                MaxPoolSize = _maxPoolSize,
                MinPoolSize = _minPoolSize,
                ChangedSession = _changedSession,
                WaitingForIdleSessionTimeout = _waitingForSessionIdleTimeout,
                ExpirationTimeout = _expirationTimeout,
                PoolingEnabled = _poolingEnabled,
                ConnectionTimeout = connectionTimeout
            };

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
                var extractor = new SessionPropertiesWithDefaultValuesExtractor(propertiesDictionary, true);
                return new SFSessionHttpClientProperties()
                {
                    validateDefaultParameters = Boolean.Parse(propertiesDictionary[SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS]),
                    clientSessionKeepAlive = Boolean.Parse(propertiesDictionary[SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE]),
                    connectionTimeout = extractor.ExtractTimeout(SFSessionProperty.CONNECTION_TIMEOUT),
                    insecureMode = Boolean.Parse(propertiesDictionary[SFSessionProperty.INSECUREMODE]),
                    disableRetry = Boolean.Parse(propertiesDictionary[SFSessionProperty.DISABLERETRY]),
                    forceRetryOn404 = Boolean.Parse(propertiesDictionary[SFSessionProperty.FORCERETRYON404]),
                    retryTimeout = extractor.ExtractTimeout(SFSessionProperty.RETRY_TIMEOUT),
                    maxHttpRetries = int.Parse(propertiesDictionary[SFSessionProperty.MAXHTTPRETRIES]),
                    includeRetryReason = Boolean.Parse(propertiesDictionary[SFSessionProperty.INCLUDERETRYREASON]),
                    proxyProperties = proxyPropertiesExtractor.ExtractProperties(propertiesDictionary),
                    _maxPoolSize = extractor.ExtractPositiveIntegerWithDefaultValue(SFSessionProperty.MAXPOOLSIZE),
                    _minPoolSize = extractor.ExtractNonNegativeIntegerWithDefaultValue(SFSessionProperty.MINPOOLSIZE),
                    _changedSession = ExtractChangedSession(extractor, SFSessionProperty.CHANGEDSESSION),
                    _waitingForSessionIdleTimeout = extractor.ExtractTimeout(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT),
                    _expirationTimeout = extractor.ExtractTimeout(SFSessionProperty.EXPIRATIONTIMEOUT),
                    _poolingEnabled = extractor.ExtractBooleanWithDefaultValue(SFSessionProperty.POOLINGENABLED),
                    _disableSamlUrlCheck = extractor.ExtractBooleanWithDefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK)
                };
            }

            private ChangedSessionBehavior ExtractChangedSession(
                SessionPropertiesWithDefaultValuesExtractor extractor,
                SFSessionProperty property) =>
                extractor.ExtractPropertyWithDefaultValue(
                    property,
                    i => (ChangedSessionBehavior)Enum.Parse(typeof(ChangedSessionBehavior), i, true),
                    s => !string.IsNullOrEmpty(s),
                    b => true
                );
        }
    }
}
