using System.Threading.Tasks;
using System.Net.Http;
using System.Net;
using System;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using Snowflake.Data.Core.FileTransfer;
using Snowflake.Data.Log;
using System.Collections.Specialized;
using System.Web;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.Linq;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Core.Revocation;
using Snowflake.Data.Core.Tools;
using TimeProvider = Snowflake.Data.Core.Tools.TimeProvider;

namespace Snowflake.Data.Core
{
    internal class HttpClientConfig
    {
        public HttpClientConfig(
            string proxyHost,
            string proxyPort,
            string proxyUser,
            string proxyPassword,
            string noProxyList,
            bool disableRetry,
            bool forceRetryOn404,
            int maxHttpRetries,
            int connectionLimit,
            bool includeRetryReason = true,
            string certRevocationCheckMode = "DISABLED",
            bool enableCRLDiskCaching = true,
            bool enableCRLInMemoryCaching = true,
            bool allowCertificatesWithoutCrlUrl = true,
            int crlDownloadTimeout = 10,
            long crlDownloadMaxSize = 209715200,
            string minTlsProtocol = null,
            string maxTlsProtocol = null,
            bool tlsProtocolsExplicitlyRequested = false,
            TlsCipherPolicy tlsCipherPolicy = null
        )
        {
            ProxyHost = proxyHost;
            ProxyPort = proxyPort;
            ProxyUser = proxyUser;
            ProxyPassword = proxyPassword;
            NoProxyList = noProxyList;
            DisableRetry = disableRetry;
            ForceRetryOn404 = forceRetryOn404;
            MaxHttpRetries = maxHttpRetries;
            IncludeRetryReason = includeRetryReason;
            ConnectionLimit = connectionLimit;
            CertRevocationCheckMode = (CertRevocationCheckMode)Enum.Parse(typeof(CertRevocationCheckMode), certRevocationCheckMode, true);
            EnableCRLDiskCaching = enableCRLDiskCaching;
            EnableCRLInMemoryCaching = enableCRLInMemoryCaching;
            AllowCertificatesWithoutCrlUrl = allowCertificatesWithoutCrlUrl;
            CrlDownloadTimeout = crlDownloadTimeout;
            CrlDownloadMaxSize = crlDownloadMaxSize;
            TlsProtocolsExplicitlyRequested = tlsProtocolsExplicitlyRequested;
            MinTlsProtocol = SslProtocolsExtensions.FromString(minTlsProtocol ?? SFSessionProperty.MINTLS.GetDefaultValue());
            MaxTlsProtocol = SslProtocolsExtensions.FromString(maxTlsProtocol ?? SFSessionProperty.MAXTLS.GetDefaultValue());
            CipherPolicy = tlsCipherPolicy ?? TlsCipherPolicy.FromEnvironment();
            CipherPolicy?.WarnIfInconsistentWith(MinTlsProtocol, MaxTlsProtocol);

            ConfKey = string.Join(";",
                new string[]
                {
                    proxyHost,
                    proxyPort,
                    proxyUser,
                    proxyPassword,
                    noProxyList,
                    disableRetry.ToString(),
                    forceRetryOn404.ToString(),
                    maxHttpRetries.ToString(),
                    includeRetryReason.ToString(),
                    connectionLimit.ToString(),
                    certRevocationCheckMode,
                    enableCRLDiskCaching.ToString(),
                    enableCRLInMemoryCaching.ToString(),
                    allowCertificatesWithoutCrlUrl.ToString(),
                    crlDownloadTimeout.ToString(),
                    crlDownloadMaxSize.ToString(),
                    minTlsProtocol,
                    maxTlsProtocol,
                    tlsProtocolsExplicitlyRequested.ToString(),
                    CipherPolicy?.CanonicalValue
                });
        }

        public readonly string ProxyHost;
        public readonly string ProxyPort;
        public readonly string ProxyUser;
        public readonly string ProxyPassword;
        public readonly string NoProxyList;
        public readonly bool DisableRetry;
        public readonly bool ForceRetryOn404;
        public readonly int MaxHttpRetries;
        public readonly bool IncludeRetryReason;
        public readonly int ConnectionLimit;
        internal readonly CertRevocationCheckMode CertRevocationCheckMode;
        internal readonly bool EnableCRLDiskCaching;
        internal readonly bool EnableCRLInMemoryCaching;
        internal readonly bool AllowCertificatesWithoutCrlUrl;
        internal readonly int CrlDownloadTimeout;
        internal readonly long CrlDownloadMaxSize;
        internal readonly SslProtocols MinTlsProtocol;
        internal readonly SslProtocols MaxTlsProtocol;
        internal readonly TlsCipherPolicy CipherPolicy;

        /// <summary>
        /// True when MINTLS or MAXTLS came from the connection string rather than from the defaults.
        /// Only an explicit request is worth failing over when it cannot be applied.
        /// </summary>
        internal readonly bool TlsProtocolsExplicitlyRequested;

        // Key used to identify the HttpClient with the configuration matching the settings
        public readonly string ConfKey;

        internal bool IsCustomCrlCheckConfigured() =>
            CertRevocationCheckMode == CertRevocationCheckMode.Enabled || CertRevocationCheckMode == CertRevocationCheckMode.Advisory;

        internal bool IsDotnetCrlCheckEnabled() => CertRevocationCheckMode == CertRevocationCheckMode.Native;

        public SslProtocols GetRequestedTlsProtocolsRange()
        {
            return MinTlsProtocol | MaxTlsProtocol;
        }
    }

    internal sealed class HttpUtil
    {
        static internal readonly int MAX_BACKOFF = 16;
        private static readonly int s_baseBackOffTime = 1;
        private static readonly int s_exponentialFactor = 2;
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<HttpUtil>();
        internal const int DefaultConnectionLimit = 50;

        private static readonly List<string> s_supportedEndpointsForRetryPolicy = new List<string>
        {
            RestPath.SF_LOGIN_PATH,
            RestPath.SF_AUTHENTICATOR_REQUEST_PATH,
            RestPath.SF_TOKEN_REQUEST_PATH
        };

        private HttpUtil()
        {
            IncreaseLowDefaultConnectionLimitOfServicePointManager();
        }

        internal void IncreaseLowDefaultConnectionLimitOfServicePointManager()
        {
#if !NET9_0_OR_GREATER // ServicePointManager has no effect on HttpClient from net9
            var currentLimit = ServicePointManager.DefaultConnectionLimit;

            // Only increase if below Snowflake's minimum requirement
            if (currentLimit < DefaultConnectionLimit)
            {
                // This value is used by AWS SDK and can cause deadlock,
                // so we need to increase the default value of 2
                // See: https://github.com/aws/aws-sdk-net/issues/152
                ServicePointManager.DefaultConnectionLimit = DefaultConnectionLimit;
                logger.Debug($"Increasing ServicePointManager.DefaultConnectionLimit from {currentLimit} to minimum default value of {DefaultConnectionLimit}");
            }
            else
            {
                logger.Debug($"Using the current ServicePointManager.DefaultConnectionLimit value of {currentLimit}");
            }
#endif
        }

        internal static HttpUtil Instance { get; } = new HttpUtil();

        private readonly object _httpClientProviderLock = new object();

        // Handlers for the cloud storage SDKs. They live here with the session registry so that one
        // type owns every HttpClient and handler lifetime in the driver, but they are built
        // differently on purpose: no RetryHandler, because the storage SDKs retry themselves and
        // wrapping them would retry a stage transfer twice, and no revocation settings, which have
        // never applied to stage transfers. Keyed on the storage identity - TLS protocols, proxy,
        // redirect and connection limit - rather than on a session ConfKey.
        private readonly ConcurrentDictionary<string, HttpMessageHandler> _storageHandlers =
            new ConcurrentDictionary<string, HttpMessageHandler>();

        private Dictionary<string, HttpClient> _HttpClients = new Dictionary<string, HttpClient>();

        private IRestRequester _restRequesterForCrlCheck;

        internal HttpClient GetHttpClient(HttpClientConfig config, DelegatingHandler customHandler = null)
        {
            lock (_httpClientProviderLock)
            {
                return RegisterNewHttpClientIfNecessary(config, customHandler);
            }
        }

        private HttpClient RegisterNewHttpClientIfNecessary(HttpClientConfig config, DelegatingHandler customHandler = null)
        {
            string name = config.ConfKey;
            if (!_HttpClients.ContainsKey(name))
            {
                logger.Debug("Http client not registered. Adding.");
                var httpClient = CreateNewHttpClient(config, customHandler);

                // Add the new client key to the list
                _HttpClients.Add(name, httpClient);
            }

            return _HttpClients[name];
        }

        internal HttpClient CreateNewHttpClient(HttpClientConfig config, DelegatingHandler customHandler = null) =>
            new HttpClient(
                new RetryHandler(SetupCustomHttpHandler(config, customHandler), config.DisableRetry, config.ForceRetryOn404, config.MaxHttpRetries,
                    config.IncludeRetryReason, config.ConnectionLimit))
            {
                Timeout = Timeout.InfiniteTimeSpan
            };

        /// <summary>
        /// Returns an HttpClient for a cloud storage SDK over a shared handler. The caller may dispose
        /// it - the handler, and with it the connection pool, survives, which matters because some SDK
        /// transports dispose the client they are given. The timeout is infinite: the SDKs apply their
        /// own per-request deadlines, and a client level timeout would cut large transfers short.
        /// </summary>
        internal HttpClient CreateStorageHttpClientShared(
            SslProtocols tlsProtocols,
            ProxyCredentials proxyCredentials = null,
            bool allowAutoRedirect = false,
            int? maxConnectionsPerServer = null)
        {
            var cipherPolicy = TlsCipherPolicy.FromEnvironment();
            return new HttpClient(
                _storageHandlers.GetOrAdd(
                    BuildStorageHandlerKey(tlsProtocols, proxyCredentials, allowAutoRedirect, maxConnectionsPerServer, cipherPolicy),
                    _ => CreateStorageHandler(tlsProtocols, proxyCredentials, allowAutoRedirect, maxConnectionsPerServer, cipherPolicy)),
                disposeHandler: false)
            {
                Timeout = Timeout.InfiniteTimeSpan
            };
        }

        /// <summary>
        /// Creates a storage handler which is not shared. Used where an SDK takes ownership of it.
        /// </summary>
        internal static HttpMessageHandler CreateStorageHandler(
            SslProtocols tlsProtocols,
            ProxyCredentials proxyCredentials = null,
            bool allowAutoRedirect = false,
            int? maxConnectionsPerServer = null,
            TlsCipherPolicy cipherPolicy = null)
        {
            cipherPolicy ??= TlsCipherPolicy.FromEnvironment();
            if (cipherPolicy != null)
            {
                return CreateSocketsHttpHandler(
                    tlsProtocols,
                    cipherPolicy,
                    proxyCredentials,
                    allowAutoRedirect,
                    maxConnectionsPerServer);
            }

            var handler = new HttpClientHandler { AllowAutoRedirect = allowAutoRedirect };
            if (maxConnectionsPerServer.HasValue)
            {
                handler.MaxConnectionsPerServer = maxConnectionsPerServer.Value;
            }

            ApplyTlsProtocols(handler, tlsProtocols);
            ApplyStorageProxy(handler, proxyCredentials);
            return handler;
        }

        internal static void ApplyTlsProtocols(HttpClientHandler handler, SslProtocols tlsProtocols)
        {
            if (tlsProtocols == SslProtocols.None) // no protocol restriction to put on the handler
            {
                return;
            }

            try
            {
                handler.SslProtocols = tlsProtocols;
            }
            catch (PlatformNotSupportedException cause)
            {
                // The transfer fails rather than silently running on the protocols chosen by the OS.
                throw TlsProtocolsNotSupported(tlsProtocols,
                    "this runtime does not support setting TLS protocols on HTTP connections.", cause);
            }
        }

        private static HttpMessageHandler CreateSocketsHttpHandler(
            SslProtocols tlsProtocols,
            TlsCipherPolicy cipherPolicy,
            ProxyCredentials proxyCredentials,
            bool allowAutoRedirect,
            int? maxConnectionsPerServer)
        {
#if NET8_0_OR_GREATER
            var handler = new SocketsHttpHandler
            {
                AllowAutoRedirect = allowAutoRedirect,
                SslOptions = new SslClientAuthenticationOptions
                {
                    EnabledSslProtocols = tlsProtocols,
                    CipherSuitesPolicy = cipherPolicy.CreatePlatformPolicy(),
                    CertificateRevocationCheckMode = X509RevocationMode.NoCheck
                }
            };
            if (maxConnectionsPerServer.HasValue)
            {
                handler.MaxConnectionsPerServer = maxConnectionsPerServer.Value;
            }
            ApplyProxy(handler, proxyCredentials);
            return handler;
#else
            throw cipherPolicy.Unsupported(
                "this .NET target does not expose per-connection TLS cipher configuration. Run the driver on .NET 8 or newer on Linux or macOS.");
#endif
        }

        /// <summary>
        /// Builds the error raised wherever requested TLS protocols cannot be applied, so that every
        /// storage path reports the same error code and shape.
        /// </summary>
        internal static SnowflakeDbException TlsProtocolsNotSupported(SslProtocols tlsProtocols, string reason, Exception cause = null)
        {
            var exception = new SnowflakeDbException(cause, SFError.TLS_CONFIGURATION_NOT_SUPPORTED,
                tlsProtocols.ToDisplayString(), reason);
            logger.Error(exception.Message, exception);
            return exception;
        }

        private static void ApplyStorageProxy(HttpClientHandler handler, ProxyCredentials proxyCredentials)
        {
            // The storage SDKs stop applying their own proxy configuration once a transport is
            // injected, so it has to be repeated here.
            if (proxyCredentials == null || string.IsNullOrEmpty(proxyCredentials.ProxyHost))
            {
                return;
            }

            var proxy = new WebProxy(proxyCredentials.ProxyHost, proxyCredentials.ProxyPort);
            if (!string.IsNullOrEmpty(proxyCredentials.ProxyUser))
            {
                proxy.Credentials = new NetworkCredential(proxyCredentials.ProxyUser, proxyCredentials.ProxyPassword);
            }

            handler.Proxy = proxy;
            handler.UseProxy = true;
        }

#if NET8_0_OR_GREATER
        private static void ApplyProxy(SocketsHttpHandler handler, ProxyCredentials proxyCredentials)
        {
            if (proxyCredentials == null || string.IsNullOrEmpty(proxyCredentials.ProxyHost))
            {
                return;
            }

            var proxy = new WebProxy(proxyCredentials.ProxyHost, proxyCredentials.ProxyPort);
            if (!string.IsNullOrEmpty(proxyCredentials.ProxyUser))
            {
                proxy.Credentials = new NetworkCredential(proxyCredentials.ProxyUser, proxyCredentials.ProxyPassword);
            }

            handler.Proxy = proxy;
            handler.UseProxy = true;
        }
#endif

        internal static string BuildStorageHandlerKey(
            SslProtocols tlsProtocols,
            ProxyCredentials proxyCredentials,
            bool allowAutoRedirect,
            int? maxConnectionsPerServer,
            TlsCipherPolicy cipherPolicy = null) =>
            string.Join(";",
                ((int)tlsProtocols).ToString(),
                (cipherPolicy ?? TlsCipherPolicy.FromEnvironment())?.CanonicalValue,
                allowAutoRedirect.ToString(),
                maxConnectionsPerServer?.ToString(),
                proxyCredentials?.ProxyHost,
                proxyCredentials?.ProxyPort.ToString(),
                // The credentials must not appear in a key kept for the lifetime of the process, but
                // handlers still have to be told apart when only the credentials differ.
                HashProxyCredentials(proxyCredentials));

        private static string HashProxyCredentials(ProxyCredentials proxyCredentials)
        {
            if (proxyCredentials == null || string.IsNullOrEmpty(proxyCredentials.ProxyUser))
            {
                return null;
            }

            using var sha256 = SHA256.Create();
            var material = Encoding.UTF8.GetBytes($"{proxyCredentials.ProxyUser}\0{proxyCredentials.ProxyPassword}");
            return BitConverter.ToString(sha256.ComputeHash(material)).Replace("-", string.Empty);
        }


        private IRestRequester GetHttpClientForCrlCheck()
        {
            if (_restRequesterForCrlCheck != null)
            {
                return _restRequesterForCrlCheck;
            }

            lock (_httpClientProviderLock)
            {
                if (_restRequesterForCrlCheck != null)
                {
                    return _restRequesterForCrlCheck;
                }

                var httpClient = new HttpClient();
                _restRequesterForCrlCheck = new RestRequester(httpClient);
                return _restRequesterForCrlCheck;
            }
        }

        internal HttpMessageHandler SetupCustomHttpHandler(HttpClientConfig config, DelegatingHandler customHandler = null)
        {
            if (customHandler != null)
            {
                if (config.CipherPolicy != null)
                {
                    throw config.CipherPolicy.Unsupported(
                        "a custom HTTP handler was supplied, so the driver cannot apply the requested cipher suites.");
                }
                RejectAutoRedirect(customHandler);
                return customHandler;
            }

            HttpMessageHandler httpHandler = CreateHttpClientHandler(config);

            // Add a proxy if necessary
            if (null != config.ProxyHost)
            {
                // Proxy needed
                WebProxy proxy = new WebProxy(config.ProxyHost, int.Parse(config.ProxyPort));

                // Add credential if provided
                if (!String.IsNullOrEmpty(config.ProxyUser))
                {
                    ICredentials credentials = new NetworkCredential(config.ProxyUser, config.ProxyPassword);
                    proxy.Credentials = credentials;
                }

                // Add bypasslist if provided
                if (!String.IsNullOrEmpty(config.NoProxyList))
                {
                    string[] bypassList = config.NoProxyList.Split(
                        new char[] { '|' },
                        StringSplitOptions.RemoveEmptyEntries);
                    // Convert simplified syntax to standard regular expression syntax
                    string entry = null;
                    for (int i = 0; i < bypassList.Length; i++)
                    {
                        // Get the original entry
                        entry = bypassList[i].Trim();
                        // . -> [.] because . means any char
                        entry = entry.Replace(".", "[.]");
                        // * -> .*  because * is a quantifier and need a char or group to apply to
                        entry = entry.Replace("*", ".*");

                        entry = entry.StartsWith("^") ? entry : $"^{entry}";

                        entry = entry.EndsWith("$") ? entry : $"{entry}$";

                        // Replace with the valid entry syntax
                        bypassList[i] = entry;
                    }

                    proxy.BypassList = bypassList;
                }

                ApplyProxy(httpHandler, proxy);
                return httpHandler;
            }

            return httpHandler;
        }

        private static void RejectAutoRedirect(HttpMessageHandler handler)
        {
            var current = handler;
            while (current is DelegatingHandler delegating)
                current = delegating.InnerHandler;

            var autoRedirect = current switch
            {
                HttpClientHandler h => h.AllowAutoRedirect,
#if NET8_0_OR_GREATER
                SocketsHttpHandler s => s.AllowAutoRedirect,
#endif
                _ => false
            };

            if (autoRedirect)
                throw new SecurityException(
                    "Custom HTTP handler has AllowAutoRedirect enabled. " +
                    "This bypasses the driver's redirect safety checks and must be disabled.");
        }

        private HttpMessageHandler CreateHttpClientHandler(HttpClientConfig config)
        {
            if (config.CipherPolicy != null)
            {
                return CreateSocketsHttpHandler(config);
            }

            bool customizedCrlCheck = false;
            try
            {
                if (config.IsCustomCrlCheckConfigured())
                {
                    customizedCrlCheck = true;
                    return CreateHttpClientHandlerWithCustomizedCrlCheck(config);
                }

                return CreateHttpClientHandlerWithDotnetCrlCheck(config);
            }
            // special logic for .NET framework 4.7.1 that
            // CheckCertificateRevocationList and SslProtocols are not supported
            catch (PlatformNotSupportedException exception)
            {
                if (customizedCrlCheck)
                {
                    logger.Error(
                        "Could not use customized Crl revocation check. Probably you are using old .net framework 4.6.2 or 4.7.1 where revocation check is done by Windows OS");
                }

                return CreateFallbackHttpClientHandler(config, exception, CanApplyTlsProtocols(config.GetRequestedTlsProtocolsRange()));
            }
        }

        private HttpMessageHandler CreateSocketsHttpHandler(HttpClientConfig config)
        {
#if NET8_0_OR_GREATER
            CertificateRevocationVerifier revocationVerifier = null;
            if (config.IsCustomCrlCheckConfigured())
            {
                revocationVerifier = CreateCertificateRevocationVerifier(config);
            }

            return new SocketsHttpHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false,
                UseProxy = false,
                AllowAutoRedirect = false,
                SslOptions = new SslClientAuthenticationOptions
                {
                    EnabledSslProtocols = config.GetRequestedTlsProtocolsRange(),
                    CipherSuitesPolicy = config.CipherPolicy.CreatePlatformPolicy(),
                    CertificateRevocationCheckMode = config.IsDotnetCrlCheckEnabled()
                        ? X509RevocationMode.Online
                        : X509RevocationMode.NoCheck,
                    RemoteCertificateValidationCallback = revocationVerifier == null
                        ? null
                        : revocationVerifier.SslStreamCertificateValidationCallback
                }
            };
#else
            throw config.CipherPolicy.Unsupported(
                "this .NET target does not expose per-connection TLS cipher configuration. Run the driver on .NET 8 or newer on Linux or macOS.");
#endif
        }

        private static void ApplyProxy(HttpMessageHandler handler, IWebProxy proxy)
        {
            switch (handler)
            {
                case HttpClientHandler httpClientHandler:
                    httpClientHandler.UseProxy = true;
                    httpClientHandler.Proxy = proxy;
                    break;
#if NET8_0_OR_GREATER
                case SocketsHttpHandler socketsHttpHandler:
                    socketsHttpHandler.UseProxy = true;
                    socketsHttpHandler.Proxy = proxy;
                    break;
#endif
                default:
                    throw new InvalidOperationException($"Cannot configure a proxy on HTTP handler type {handler.GetType().FullName}.");
            }
        }

        /// <summary>
        /// Checks whether the requested TLS protocols can be applied on the current runtime.
        /// The setting is probed on its own so that a runtime rejecting only CheckCertificateRevocationList
        /// does not cost us the requested TLS protocol restriction.
        /// </summary>
        private static bool CanApplyTlsProtocols(SslProtocols requestedTlsProtocols)
        {
            if (requestedTlsProtocols == SslProtocols.None) // nothing requested, protocol selection is left to the OS
            {
                return true;
            }

            try
            {
                using var probedHandler = new HttpClientHandler();
                probedHandler.SslProtocols = requestedTlsProtocols;
                return true;
            }
            catch (PlatformNotSupportedException)
            {
                return false;
            }
        }

        internal HttpClientHandler CreateFallbackHttpClientHandler(HttpClientConfig config, Exception cause, bool canApplyTlsProtocols)
        {
            if (!canApplyTlsProtocols)
            {
                var protocols = $"MINTLS={config.MinTlsProtocol.ToDisplayString()}, MAXTLS={config.MaxTlsProtocol.ToDisplayString()}";
                if (config.TlsProtocolsExplicitlyRequested)
                {
                    // Falling back to the protocols chosen by the OS would silently drop the
                    // requested TLS restriction, so the connection is failed instead.
                    var exception = new SnowflakeDbException(cause, SFError.TLS_CONFIGURATION_NOT_SUPPORTED,
                        protocols,
                        "this runtime does not support setting TLS protocols on HTTP connections (.NET Framework 4.6.2 and 4.7.1). Run the driver on .NET Framework 4.8 or newer, or on a modern .NET runtime.");
                    logger.Error(exception.Message, exception);
                    throw exception;
                }

                // Nothing was requested, only the defaults apply, so protocol selection is left to
                // the OS as it was before MINTLS/MAXTLS existed.
                logger.Warn($"This runtime does not support setting TLS protocols on HTTP connections. Default protocols ({protocols}) are not applied and the operating system chooses them instead");
            }

            logger.Warn("Revocation check settings are not supported on this runtime. Creating HttpClientHandler without them, the requested TLS protocols are preserved");
            var handler = new HttpClientHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false, // Disable cookies
                UseProxy = false,
                AllowAutoRedirect = false
            };

            var requestedTlsProtocols = config.GetRequestedTlsProtocolsRange();
            if (canApplyTlsProtocols && requestedTlsProtocols != SslProtocols.None)
            {
                handler.SslProtocols = requestedTlsProtocols;
            }

            return handler;
        }

        private HttpClientHandler CreateHttpClientHandlerWithDotnetCrlCheck(HttpClientConfig config)
        {
            logger.Debug("Creating HttpClientHandler without CRL check customizations");
            return new HttpClientHandler
            {
                CheckCertificateRevocationList = config.IsDotnetCrlCheckEnabled(),
                SslProtocols = config.GetRequestedTlsProtocolsRange(),
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false, // Disable cookies
                UseProxy = false,
                AllowAutoRedirect = false
            };
        }

        private HttpClientHandler CreateHttpClientHandlerWithCustomizedCrlCheck(HttpClientConfig config)
        {
            logger.Debug("Creating HttpClientHandler with customized CRL check");
            var revocationVerifier = CreateCertificateRevocationVerifier(config);
            return new HttpClientHandler
            {
                CheckCertificateRevocationList = false,
                ServerCertificateCustomValidationCallback = revocationVerifier.CertificateValidationCallback,
                SslProtocols = config.GetRequestedTlsProtocolsRange(),
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false, // Disable cookies
                UseProxy = false,
                AllowAutoRedirect = false
            };
        }

        private CertificateRevocationVerifier CreateCertificateRevocationVerifier(HttpClientConfig config) =>
            new CertificateRevocationVerifier(
                config,
                TimeProvider.Instance,
                GetHttpClientForCrlCheck(),
                CertificateCrlDistributionPointsExtractor.Instance,
                new CrlParser(EnvironmentFacade.Instance),
                new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching));

        /// <summary>
        /// UriUpdater would update the uri in each retry. During construction, it would take in an uri that would later
        /// be updated in each retry and figure out the rules to apply when updating.
        /// </summary>
        internal class UriUpdater
        {
            /// <summary>
            /// IRule defines how the queryParams of a uri should be updated in each retry
            /// </summary>
            interface IRule
            {
                void apply(NameValueCollection queryParams);
            }

            /// <summary>
            /// RetryCountRule would update the retryCount parameter
            /// </summary>
            class RetryCountRule : IRule
            {
                int retryCount;

                internal RetryCountRule()
                {
                    retryCount = 1;
                }

                void IRule.apply(NameValueCollection queryParams)
                {
                    if (retryCount == 1)
                    {
                        queryParams.Add(RestParams.SF_QUERY_RETRY_COUNT, retryCount.ToString());
                    }
                    else
                    {
                        queryParams.Set(RestParams.SF_QUERY_RETRY_COUNT, retryCount.ToString());
                    }

                    retryCount++;
                }
            }

            /// <summary>
            /// RequestUUIDRule would update the request_guid query with a new RequestGUID
            /// </summary>
            class RequestUUIDRule : IRule
            {
                void IRule.apply(NameValueCollection queryParams)
                {
                    queryParams.Set(RestParams.SF_QUERY_REQUEST_GUID, Guid.NewGuid().ToString());
                }
            }

            /// <summary>
            /// RetryReasonRule would update the retryReason parameter
            /// </summary>
            private class RetryReasonRule : IRule
            {
                private HttpStatusCode _retryReason;

                internal RetryReasonRule()
                {
                    _retryReason = 0;
                }

                public void SetRetryReason(HttpStatusCode reason)
                {
                    _retryReason = reason;
                }

                void IRule.apply(NameValueCollection queryParams)
                {
                    var retryCode = ((int)_retryReason).ToString();
                    queryParams.Set(RestParams.SF_QUERY_RETRY_REASON, retryCode);
                }
            }

            UriBuilder uriBuilder;
            List<IRule> rules;

            internal UriUpdater(Uri uri, bool includeRetryReason = true)
            {
                uriBuilder = new UriBuilder(uri);
                rules = new List<IRule>();

                if (uri.AbsolutePath.StartsWith(RestPath.SF_QUERY_PATH))
                {
                    rules.Add(new RetryCountRule());
                    if (includeRetryReason)
                    {
                        rules.Add(new RetryReasonRule());
                    }
                }

                if (uri.Query != null && uri.Query.Contains(RestParams.SF_QUERY_REQUEST_GUID))
                {
                    rules.Add(new RequestUUIDRule());
                }
            }

            internal void Update(ref HttpRequestMessage requestMessage, HttpStatusCode retryReason, Uri location)
            {
                if (IsRedirectHTTPCode(retryReason))
                {
                    var redirectUri = GetRedirectedUri(requestMessage.RequestUri, location);
                    if ((requestMessage.Method == HttpMethod.Post || retryReason == HttpStatusCode.SeeOther) && retryReason != HttpStatusCode.TemporaryRedirect && retryReason != (HttpStatusCode)308)
                    {
                        requestMessage.Method = HttpMethod.Get;
                        requestMessage.Content?.Dispose();
                        requestMessage.Content = null;
                    }

                    requestMessage.RequestUri = redirectUri;
                    return;
                }

                // Optimization to bypass parsing if there is no rules at all.
                if (rules.Count == 0)
                {
                    requestMessage.RequestUri = uriBuilder.Uri;
                    return;
                }

                var queryParams = HttpUtility.ParseQueryString(uriBuilder.Query);

                foreach (IRule rule in rules)
                {
                    if (rule is RetryReasonRule)
                    {
                        ((RetryReasonRule)rule).SetRetryReason(retryReason);
                    }

                    rule.apply(queryParams);
                }

                uriBuilder.Query = queryParams.ToString();

                requestMessage.RequestUri = uriBuilder.Uri;
            }

            private Uri GetRedirectedUri(Uri requestUri, Uri location)
            {
                if (requestUri == null || location == null)
                    return uriBuilder.Uri;

                Uri targetUrl;
                if (location.IsAbsoluteUri)
                {
                    targetUrl = location;
                }
                else if (!Uri.TryCreate(requestUri, location, out targetUrl))
                {
                    logger.Error($"Redirect URI resolution failed for location '{location.ToMaskedString()}' against request '{requestUri.ToMaskedString()}'. Falling back to {uriBuilder.Uri.ToMaskedString()}!");
                    return uriBuilder.Uri;
                }

                return targetUrl;
            }
        }

        private class RetryHandler : DelegatingHandler
        {
            private static SFLogger logger = SFLoggerFactory.GetLogger<RetryHandler>();

            private const int MaxRedirectsCount = 20;

            private bool disableRetry;
            private bool forceRetryOn404;
            private int maxRetryCount;
            private bool includeRetryReason;
            private int connectionLimit;

            internal RetryHandler(HttpMessageHandler innerHandler, bool disableRetry, bool forceRetryOn404, int maxRetryCount,
                bool includeRetryReason, int connectionLimit) : base(innerHandler)
            {
                this.disableRetry = disableRetry;
                this.forceRetryOn404 = forceRetryOn404;
                this.maxRetryCount = maxRetryCount;
                this.includeRetryReason = includeRetryReason;
                this.connectionLimit = connectionLimit;
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage requestMessage,
                CancellationToken cancellationToken)
            {
                HttpResponseMessage response = null;
                var absolutePath = requestMessage.RequestUri!.AbsolutePath;
                var isLoginRequest = IsLoginEndpoint(absolutePath);
                var isOktaSSORequest = IsOktaSSORequest(requestMessage.RequestUri.Host, absolutePath);
                var backOffInSec = s_baseBackOffTime;
                Exception lastException = null;


#pragma warning disable SYSLIB0014 // TODO SNOW-3662960
                var p = ServicePointManager.FindServicePoint(requestMessage.RequestUri);
#pragma warning restore SYSLIB0014
                p.Expect100Continue = false; // Saves about 100 ms per request
                p.UseNagleAlgorithm = false; // Saves about 200 ms per request
                p.ConnectionLimit = connectionLimit; // Default value is 2, we need more connections for performing multiple parallel queries

                var httpTimeout = (TimeSpan)requestMessage.GetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY);
                var restTimeout = (TimeSpan)requestMessage.GetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY);

                if (logger.IsDebugEnabled())
                {
                    logger.Debug("Http request timeout : " + httpTimeout);
                    logger.Debug("Rest request timeout : " + restTimeout);
                }

                CancellationTokenSource childCts = null;

                var updater = new UriUpdater(requestMessage.RequestUri, includeRetryReason);
                var (retryCount, redirectsCount) = (0, 0);

                var startTime = DateTimeOffset.UtcNow;
                while (true)
                {
                    try
                    {
                        childCts = null;
                        response = null;

                        if (!httpTimeout.Equals(Timeout.InfiniteTimeSpan))
                        {
                            childCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                            if (httpTimeout.Ticks == 0)
                                childCts.Cancel();
                            else
                                childCts.CancelAfter(httpTimeout);
                        }

                        response = await base.SendAsync(requestMessage, childCts?.Token ?? cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        lastException = e;
                        if (cancellationToken.IsCancellationRequested)
                        {
                            logger.Info("SF rest request timeout or explicit cancel called.");
                            cancellationToken.ThrowIfCancellationRequested();
                        }
                        else if (childCts is { Token.IsCancellationRequested: true })
                        {
                            logger.Warn($"Http request timeout. Retry the request after max {backOffInSec} sec.");
                        }
                        else
                        {
                            var innermostException = GetInnerMostException(e);

                            if (innermostException is AuthenticationException)
                            {
                                logger.Error("Non-retryable error encountered: ", e);
                                throw;
                            }
                            else
                            {
                                //TODO: Should probably check to see if the error is recoverable or transient.
                                logger.Warn("Error occurred during request, retrying...", e);
                            }
                        }
                    }

                    var isRedirecting = false;
                    var totalRetryTime = (int)(DateTimeOffset.UtcNow - startTime).TotalSeconds;

                    childCts?.Dispose();

                    HttpStatusCode errorReason = default;

                    if (response != null)
                    {
                        if (isOktaSSORequest)
                        {
                            response.Content.Headers.Add(OktaAuthenticator.RetryCountHeader, retryCount.ToString());
                            response.Content.Headers.Add(OktaAuthenticator.TimeoutElapsedHeader, totalRetryTime.ToString());
                        }

                        if (response.IsSuccessStatusCode)
                        {
                            logger.Debug($"Success Response: StatusCode: {(int)response.StatusCode}, ReasonPhrase: '{response.ReasonPhrase}'");
                            return response;
                        }
                        else
                        {
                            logger.Debug($"Failed Response: StatusCode: {(int)response.StatusCode}, ReasonPhrase: '{response.ReasonPhrase}'");
                            var isRetryable = IsRetryableHTTPCode(response.StatusCode, forceRetryOn404);

                            if (IsRedirectHTTPCode(response.StatusCode))
                            {
                                if (!IsSafeRedirect(requestMessage.RequestUri, response.Headers?.Location))
                                {
                                    logger.Warn($"Unsafe redirect location '{response.Headers?.Location.ToMaskedString()}' for request '{requestMessage.RequestUri.ToMaskedString()}'. Stopping retry.");
                                    throw new SecurityException($"Unsafe redirect rejected: '{response.Headers?.Location.ToMaskedString()}' is not a safe redirect target for '{requestMessage.RequestUri.ToMaskedString()}'");
                                }

                                if (++redirectsCount > MaxRedirectsCount)
                                {
                                    logger.Warn($"Maximum redirect hops ({MaxRedirectsCount}) exceeded for request '{requestMessage.RequestUri.ToMaskedString()}'. Stopping..");
                                    throw new SecurityException($"Redirect loop detected: more than {MaxRedirectsCount} consecutive redirects from '{requestMessage.RequestUri.ToMaskedString()}'");
                                }

                                isRedirecting = true;
                            }

                            if (!isRedirecting && (!isRetryable || disableRetry))
                            {
                                // No need to keep retrying, stop here
                                return response;
                            }
                        }

                        errorReason = response.StatusCode;
                    }
                    else
                    {
                        logger.Info("Response returned was null.");
                    }

                    if (restTimeout.TotalSeconds > 0 && totalRetryTime >= restTimeout.TotalSeconds)
                    {
                        logger.Debug($"stop retry as connection_timeout {restTimeout.TotalSeconds} sec. reached");
                        if (response != null)
                        {
                            return response;
                        }

                        var errorMessage = $"http request failed and connection_timeout {restTimeout.TotalSeconds} sec. reached.\n";
                        errorMessage += $"Last exception encountered: {lastException}";
                        logger.Error(errorMessage);
                        throw new OperationCanceledException(errorMessage);
                    }

                    if (restTimeout.TotalSeconds > 0 && totalRetryTime + backOffInSec > restTimeout.TotalSeconds)
                    {
                        // No need to wait more than necessary if it can be avoided.
                        backOffInSec = (int)restTimeout.TotalSeconds - totalRetryTime;
                    }

                    retryCount += isRedirecting ? 0 : 1;
                    var waitingTime = isRedirecting ? 0 : backOffInSec;
                    redirectsCount *= isRedirecting ? 1 : 0;
                    if (maxRetryCount > 0 && (retryCount > maxRetryCount))
                    {
                        logger.Debug($"stop retry as maxHttpRetries {maxRetryCount} reached");
                        if (response != null)
                        {
                            return response;
                        }

                        var errorMessage = $"http request failed and max retry {maxRetryCount} reached.\n";
                        errorMessage += $"Last exception encountered: {lastException}";
                        logger.Error(errorMessage);
                        throw new OperationCanceledException(errorMessage);
                    }

                    updater.Update(ref requestMessage, errorReason, response?.Headers?.Location);

                    // Disposing of the response if not null now that we don't need it anymore
                    response?.Dispose();

                    logger.Debug($"Sleep {waitingTime} seconds and then retry the request, retryCount: {retryCount}");

                    await Task.Delay(TimeSpan.FromSeconds(waitingTime), cancellationToken).ConfigureAwait(false);

                    var jitter = GetJitter(backOffInSec);

                    // Set backoff time
                    if (isLoginRequest)
                    {
                        // Choose between previous sleep time and new base sleep time for login requests
                        backOffInSec = (int)ChooseRandom(
                            backOffInSec + jitter,
                            Math.Pow(s_exponentialFactor, retryCount) + jitter);
                    }
                    else if (backOffInSec < MAX_BACKOFF)
                    {
                        // Multiply sleep by 2 for non-login requests
                        backOffInSec *= 2;
                    }
                }
            }
        }

        internal static Exception UnpackAggregateException(Exception exception) =>
            exception is AggregateException ? ((AggregateException)exception).InnerException : exception;

        static private Exception GetInnerMostException(Exception exception)
        {
            var innermostException = exception;
            while (innermostException.InnerException != null && innermostException != innermostException.InnerException)
                innermostException = innermostException.InnerException;
            return innermostException;
        }

        /// <summary>
        /// Check whether the error is retryable or not.
        /// </summary>
        /// <param name="statusCode">The http status code.</param>
        /// <param name="forceRetryOn404">Should 404 be retried.</param>
        /// <returns>True if the request should be retried, false otherwise.</returns>
        public static bool IsRetryableHTTPCode(HttpStatusCode statusCode, bool forceRetryOn404)
        {
            if (forceRetryOn404 && statusCode == HttpStatusCode.NotFound)
                return true;
            return statusCode is >= HttpStatusCode.InternalServerError and < (HttpStatusCode)600 ||
                   statusCode == HttpStatusCode.Forbidden ||
                   statusCode == HttpStatusCode.RequestTimeout ||
                   statusCode == (HttpStatusCode)429 || // Too many requests
                   IsRedirectHTTPCode(statusCode);
        }

        /// <summary>
        /// All 3xx codes that carry a Location header the driver should follow. Includes 300
        /// (Multiple Choices / Ambiguous): although RFC 9110 section 15.4.1 does not mandate a
        /// Location header for 300, in practice servers that return 300 with a Location expect
        /// the client to follow it. The IsSafeRedirect guard rejects the redirect when Location
        /// is null, so a 300 without one is harmlessly returned to the caller.
        /// </summary>
        public static bool IsRedirectHTTPCode(HttpStatusCode statusCode)
        {
            return
                statusCode is HttpStatusCode.TemporaryRedirect
                    or (HttpStatusCode)308 // Permanent redirect
                    or HttpStatusCode.SeeOther
                    or HttpStatusCode.Redirect
                    or HttpStatusCode.Moved
                    or HttpStatusCode.Ambiguous;
        }

        /// <summary>
        /// Validates that a redirect target URL is safe to follow:
        /// 1. Both request URI and redirect location are non-null and request URI is absolute.
        /// 2. Scheme is HTTP or HTTPS and matches the original request scheme (prevents HTTPS -> HTTP downgrade).
        /// 3. Host matches the original request host (prevents cross-origin redirect).
        /// 4. Port matches the original request port.
        /// </summary>
        /// <param name="requestUri">The original request URI.</param>
        /// <param name="location">The redirect location URI.</param>
        /// <returns>True if the redirect is safe to follow, false otherwise.</returns>
        internal static bool IsSafeRedirect(Uri requestUri, Uri location)
        {
            if (requestUri == null || location == null || !requestUri.IsAbsoluteUri)
                return false;

            try
            {
                var targetUri = location.IsAbsoluteUri ? location : new Uri(requestUri, location);

                if (!targetUri.IsAbsoluteUri)
                {
                    logger.Warn($"Redirect rejected: resolved location '{location.ToMaskedString()}' is not an absolute URI for request '{requestUri.ToMaskedString()}'");
                    return false;
                }

                if (!string.Equals(targetUri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(targetUri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
                {
                    logger.Warn($"Redirect rejected: non-HTTP scheme '{targetUri.Scheme}' in location '{targetUri.ToMaskedString()}' for request '{requestUri.ToMaskedString()}'");
                    return false;
                }

                if (!string.Equals(requestUri.Scheme, targetUri.Scheme, StringComparison.OrdinalIgnoreCase))
                {
                    logger.Warn($"Redirect rejected: scheme mismatch ('{requestUri.Scheme}' -> '{targetUri.Scheme}') in location '{targetUri.ToMaskedString()}' for request '{requestUri.ToMaskedString()}'");
                    return false;
                }

                if (!string.Equals(requestUri.Host, targetUri.Host, StringComparison.OrdinalIgnoreCase))
                {
                    logger.Warn($"Redirect rejected: different origin ('{requestUri.Host}' -> '{targetUri.Host}') in location '{targetUri.ToMaskedString()}' for request '{requestUri.ToMaskedString()}'");
                    return false;
                }

                if (requestUri.Port != targetUri.Port)
                {
                    logger.Warn($"Redirect rejected: port mismatch ('{requestUri.Port}' -> '{targetUri.Port}') in location '{targetUri.ToMaskedString()}' for request '{requestUri.ToMaskedString()}'");
                    return false;
                }

                return true;
            }
            catch (UriFormatException exception)
            {
                logger.Error($"Redirect rejected: failed to resolve location '{location.ToMaskedString()}' against request '{requestUri.ToMaskedString()}': {exception.Message}");
                return false;
            }
        }

        /// <summary>
        /// Get the jitter amount based on current wait time.
        /// </summary>
        /// <param name="curWaitTime">The current retry backoff time.</param>
        /// <returns>The new jitter amount.</returns>
        static internal double GetJitter(double curWaitTime)
        {
            double multiplicationFactor = ChooseRandom(-1, 1);
            double jitterAmount = 0.5 * curWaitTime * multiplicationFactor;
            return jitterAmount;
        }

        /// <summary>
        /// Randomly generates a number between a given range.
        /// </summary>
        /// <param name="min">The min range (inclusive).</param>
        /// <param name="max">The max range (inclusive).</param>
        /// <returns>The random number.</returns>
        static double ChooseRandom(double min, double max)
        {
            var next = new Random().NextDouble();

            return min + (next * (max - min));
        }

        /// <summary>
        /// Checks if the endpoint is a login request.
        /// </summary>
        /// <param name="endpoint">The endpoint to check.</param>
        /// <returns>True if the endpoint is a login request, false otherwise.</returns>
        static internal bool IsLoginEndpoint(string endpoint)
        {
            return null != s_supportedEndpointsForRetryPolicy.FirstOrDefault(ep => endpoint.Equals(ep));
        }

        /// <summary>
        /// Checks if request is for Okta and an SSO SAML endpoint.
        /// </summary>
        /// <param name="host">The host url to check.</param>
        /// <param name="endpoint">The endpoint to check.</param>
        /// <returns>True if the endpoint is an okta sso saml request, false otherwise.</returns>
        static internal bool IsOktaSSORequest(string host, string endpoint)
        {
            return host.Contains(OktaUrl.DOMAIN) && endpoint.Contains(OktaUrl.SSO_SAML_PATH);
        }
    }
}
