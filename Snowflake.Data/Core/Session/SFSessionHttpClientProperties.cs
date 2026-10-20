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
        private static readonly Extractor s_propertiesExtractor = GetDefaultPropertiesExtractor();

        public const int DefaultMaxPoolSize = 10;
        public const int DefaultMinPoolSize = 2;
        public const ChangedSessionBehavior DefaultChangedSession = ChangedSessionBehavior.Destroy;
        public static readonly TimeSpan DefaultWaitingForIdleSessionTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromMinutes(5);
        public static readonly TimeSpan DefaultExpirationTimeout = TimeSpan.FromHours(1);
        public const bool DefaultPoolingEnabled = true;
        public const int DefaultMaxHttpRetries = 7;
        public const int DefaultConnectionLimit = 20;
        public static TimeSpan DefaultRetryTimeout { get; internal set; } = TimeSpan.FromSeconds(300);
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFSessionHttpClientProperties>();
        internal const int MaxConnectionLimit = 1000;

        internal bool _validateDefaultParameters { get; }
        internal bool _clientSessionKeepAlive { get; }
        internal SFSessionRetryProperties _retryProperties { get; private set; }
        internal SFSessionHttpClientProxyProperties _proxyProperties { get; }
        internal SFSessionCRLProperties _crlProperties { get; }
        internal SFSessionTLSProperties _tlsProperties { get; }
        private int _maxPoolSize { get; }
        private int _minPoolSize { get; }
        private ChangedSessionBehavior _changedSession { get; }
        private TimeSpan _waitingForSessionIdleTimeout { get; }
        private TimeSpan _expirationTimeout { get; }
        internal bool _poolingEnabled { get; private set; }
        internal bool _clientStoreTemporaryCredential { get; }
        internal int _servicePointConnectionLimit { get; private set; }
        internal TimeSpan _cleanupWait { get; }
        internal TimeSpan _abortRequestTimeout { get; }
        public bool _disableSamlUrlCheck { get; }

        private protected SFSessionHttpClientProperties(
            bool clientSessionKeepAlive,
            bool validateDefaultParameters,
                SFSessionRetryProperties retryProperties,
                SFSessionHttpClientProxyProperties proxyProperties,
                int maxPoolSize,
                int minPoolSize,
                ChangedSessionBehavior changedSession,
                TimeSpan waitingForSessionIdleTimeout,
                TimeSpan expirationTimeout,
                bool poolingEnabled,
                bool disableSamlUrlCheck,
                bool clientStoreTemporaryCredential,
                int servicePointConnectionLimit,
                SFSessionCRLProperties crlProperties,
                SFSessionTLSProperties tlsProperties,
                TimeSpan cleanupWait,
                TimeSpan abortRequestTimeout)
        {
            _clientSessionKeepAlive = clientSessionKeepAlive;
            _validateDefaultParameters = validateDefaultParameters;
            _retryProperties = retryProperties;
            _proxyProperties = proxyProperties;
            _maxPoolSize = maxPoolSize;
            _minPoolSize = minPoolSize;
            _changedSession = changedSession;
            _waitingForSessionIdleTimeout = waitingForSessionIdleTimeout;
            _expirationTimeout = expirationTimeout;
            _poolingEnabled = poolingEnabled;
            _disableSamlUrlCheck = disableSamlUrlCheck;
            _clientStoreTemporaryCredential = clientStoreTemporaryCredential;
            _servicePointConnectionLimit = servicePointConnectionLimit;
            _crlProperties = crlProperties;
            _tlsProperties = tlsProperties;
            _cleanupWait = cleanupWait;
            _abortRequestTimeout = abortRequestTimeout;
        }

        public static SFSessionHttpClientProperties ExtractAndValidate(SFSessionProperties properties)
        {
            var extractedProperties = s_propertiesExtractor.ExtractProperties(properties);
            extractedProperties.CheckPropertiesAreValid();
            return extractedProperties;
        }

        public void DisablePoolingDefaultIfSecretsProvidedExternally(SFSessionProperties properties)
        {
            var authenticator = properties[SFSessionProperty.AUTHENTICATOR];
            if (ExternalBrowserAuthenticator.IsExternalBrowserAuthenticator(authenticator))
            {
                DisablePoolingIfNotExplicitlyEnabled(properties, "external browser");

            }
            else if (KeyPairAuthenticator.IsKeyPairAuthenticator(authenticator)
                       && properties.IsNonEmptyValueProvided(SFSessionProperty.PRIVATE_KEY_FILE)
                       && !properties.IsNonEmptyValueProvided(SFSessionProperty.PRIVATE_KEY_PWD))
            {
                DisablePoolingIfNotExplicitlyEnabled(properties, "key pair with private key in a file");
            }
            else if (OAuthAuthorizationCodeAuthenticator.IsOAuthAuthorizationCodeAuthenticator(authenticator))
            {
                DisablePoolingIfNotExplicitlyEnabled(properties, "oauth authorization code");
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
                ValidateRetryProperties();
                ValidateMinMaxPoolSize();
                ValidateWaitingForSessionIdleTimeout();
                ValidateConnectionLimit();
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

        private void ValidateRetryProperties()
        {
            var (connectionTimeout, retryTimeout, maxHttpRetries) = (_retryProperties.ConnectionTimeout, _retryProperties.RetryTimeout, _retryProperties.MaxHttpRetries);
            if (TimeoutHelper.IsZeroLength(connectionTimeout))
            {
                s_logger.Warn("Connection timeout provided is 0. Timeout will be infinite");
                connectionTimeout = TimeoutHelper.Infinity();
            }
            else if (TimeoutHelper.IsInfinite(connectionTimeout))
            {
                s_logger.Warn("Connection timeout provided is negative. Timeout will be infinite.");
            }

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

            if (!TimeoutHelper.IsInfinite(retryTimeout) && retryTimeout < connectionTimeout)
            {
                s_logger.Warn($"Connection timeout greater than retry timeout. Setting connection time same as retry timeout");
                connectionTimeout = retryTimeout;
            }

            if (maxHttpRetries is > 0 and < DefaultMaxHttpRetries)
            {
                s_logger.Warn($"Max retry count provided is less than the allowed minimum value of {DefaultMaxHttpRetries}");

                maxHttpRetries = DefaultMaxHttpRetries;
            }
            else if (maxHttpRetries == 0)
            {
                s_logger.Warn($"Max retry count provided is 0. Retry count will be infinite");
            }

            _retryProperties = _retryProperties with { ConnectionTimeout = connectionTimeout, RetryTimeout = retryTimeout, MaxHttpRetries = maxHttpRetries };
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

        private void ValidateConnectionLimit()
        {
            if (_servicePointConnectionLimit is < 1 or > MaxConnectionLimit)
            {
                s_logger.Warn($"Connection limit must be between 1 and {MaxConnectionLimit}. Using the default value of {DefaultConnectionLimit}");
                _servicePointConnectionLimit = DefaultConnectionLimit;
            }
        }

        public HttpClientConfig BuildHttpClientConfig(string clientType)
        {
            return new HttpClientConfig(
                _proxyProperties,
                _retryProperties,
                _crlProperties,
                _servicePointConnectionLimit,
                _tlsProperties,
                clientType
                );
        }

        public ConnectionPoolConfig BuildConnectionPoolConfig() =>
            new()
            {
                MaxPoolSize = _maxPoolSize,
                MinPoolSize = _minPoolSize,
                ChangedSession = _changedSession,
                WaitingForIdleSessionTimeout = _waitingForSessionIdleTimeout,
                ExpirationTimeout = _expirationTimeout,
                PoolingEnabled = _poolingEnabled,
                ConnectionTimeout = _retryProperties.ConnectionTimeout
            };

        internal interface IExtractor
        {
            SFSessionHttpClientProperties ExtractProperties(SFSessionProperties propertiesDictionary);
        }

        internal class Extractor : IExtractor
        {
            private readonly SFSessionHttpClientProxyProperties.IExtractor _proxyPropertiesExtractor;
            private readonly SFSessionRetryProperties.IExtractor _retryPropertiesExtractor;
            private readonly SFSessionCRLProperties.IExtractor _crlPropertiesExtractor;
            private readonly SFSessionTLSProperties.IExtractor _tlsPropertiesExtractor;

            public Extractor(
                SFSessionHttpClientProxyProperties.IExtractor proxyPropertiesExtractor,
                SFSessionRetryProperties.IExtractor retryPropertiesExtractor,
                SFSessionCRLProperties.IExtractor crlPropertiesExtractor,
                SFSessionTLSProperties.IExtractor tlsPropertiesExtractor)
            {
                _proxyPropertiesExtractor = proxyPropertiesExtractor;
                _retryPropertiesExtractor = retryPropertiesExtractor;
                _crlPropertiesExtractor = crlPropertiesExtractor;
                _tlsPropertiesExtractor = tlsPropertiesExtractor;
            }

            public SFSessionHttpClientProperties ExtractProperties(SFSessionProperties propertiesDictionary)
            {
                var extractor = new SessionPropertiesWithDefaultValuesExtractor(propertiesDictionary, true);

                return new SFSessionHttpClientProperties
                (
                    validateDefaultParameters: bool.Parse(propertiesDictionary[SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS]),
                    clientSessionKeepAlive: bool.Parse(propertiesDictionary[SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE]),
                    retryProperties: _retryPropertiesExtractor.ExtractProperties(propertiesDictionary, extractor),
                    proxyProperties: _proxyPropertiesExtractor.ExtractProperties(propertiesDictionary),
                    maxPoolSize: extractor.ExtractPositiveIntegerWithDefaultValue(SFSessionProperty.MAXPOOLSIZE),
                    minPoolSize: extractor.ExtractNonNegativeIntegerWithDefaultValue(SFSessionProperty.MINPOOLSIZE),
                    changedSession: ExtractChangedSession(extractor, SFSessionProperty.CHANGEDSESSION),
                    waitingForSessionIdleTimeout: extractor.ExtractTimeout(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT),
                    expirationTimeout: extractor.ExtractTimeout(SFSessionProperty.EXPIRATIONTIMEOUT),
                    poolingEnabled: extractor.ExtractBooleanWithDefaultValue(SFSessionProperty.POOLINGENABLED),
                    disableSamlUrlCheck: extractor.ExtractBooleanWithDefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK),
                    clientStoreTemporaryCredential: bool.Parse(propertiesDictionary[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL]),
                    servicePointConnectionLimit: int.Parse(propertiesDictionary[SFSessionProperty.SERVICE_POINT_CONNECTION_LIMIT]),
                    crlProperties: _crlPropertiesExtractor.ExtractProperties(propertiesDictionary),
                    tlsProperties: _tlsPropertiesExtractor.ExtractProperties(propertiesDictionary),
                    cleanupWait: extractor.ExtractTimeout(SFSessionProperty.CLEANUP_WAIT),
                    abortRequestTimeout: extractor.ExtractTimeout(SFSessionProperty.ABORT_REQUEST_TIMEOUT)
                );
            }

            private static ChangedSessionBehavior ExtractChangedSession(
                SessionPropertiesWithDefaultValuesExtractor extractor,
                SFSessionProperty property) =>
                extractor.ExtractPropertyWithDefaultValue(
                    property,
                    i => (ChangedSessionBehavior)Enum.Parse(typeof(ChangedSessionBehavior), i, true),
                    s => !string.IsNullOrEmpty(s),
                    _ => true
                );
        }

        private static Extractor GetDefaultPropertiesExtractor()
        {
            var proxyPropertiesExtractor = new SFSessionHttpClientProxyProperties.Extractor();
            var retryPropertiesExtractor = new SFSessionRetryProperties.Extractor();
            var crlPropertiesExtractor = new SFSessionCRLProperties.Extractor();
            var tlsPropertiesExtractor = new SFSessionTLSProperties.Extractor();

            return new(proxyPropertiesExtractor, retryPropertiesExtractor, crlPropertiesExtractor, tlsPropertiesExtractor);
        }

    }
}
