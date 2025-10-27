using System.Threading.Tasks;
using System.Net.Http;
using System.Net;
using System;
using System.Threading;
using System.Collections.Generic;
using Snowflake.Data.Log;
using System.Collections.Specialized;
using System.Web;
using System.Security.Authentication;
using System.Linq;
using Snowflake.Data.Core.Authenticator;
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
            string minTlsProtocol = "TLS12",
            string maxTlsProtocol = "TLS13"
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
            MinTlsProtocol = minTlsProtocol != null ? SslProtocolsExtensions.FromString(minTlsProtocol) : SslProtocols.None;
            MaxTlsProtocol = minTlsProtocol != null ? SslProtocolsExtensions.FromString(maxTlsProtocol) : SslProtocols.None;

            ConfKey = string.Join(";",
                new string[] {
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
                    minTlsProtocol,
                    maxTlsProtocol
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
        internal readonly SslProtocols MinTlsProtocol;
        internal readonly SslProtocols MaxTlsProtocol;

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
        }

        internal static HttpUtil Instance { get; } = new HttpUtil();

        private readonly object _httpClientProviderLock = new object();

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
                new RetryHandler(SetupCustomHttpHandler(config, customHandler), config.DisableRetry, config.ForceRetryOn404, config.MaxHttpRetries, config.IncludeRetryReason, config.ConnectionLimit))
            {
                Timeout = Timeout.InfiniteTimeSpan
            };


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

                HttpClientHandler httpHandlerWithProxy = (HttpClientHandler)httpHandler;
                httpHandlerWithProxy.UseProxy = true;
                httpHandlerWithProxy.Proxy = proxy;
                return httpHandlerWithProxy;
            }
            return httpHandler;
        }

        private HttpClientHandler CreateHttpClientHandler(HttpClientConfig config)
        {
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
            catch (PlatformNotSupportedException)
            {
                if (customizedCrlCheck)
                {
                    logger.Error("Could not use customized Crl revocation check. Probably you are using old .net framework 4.6.2 or 4.7.1 where revocation check is done by Windows OS");
                }
                return new HttpClientHandler
                {
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                    UseCookies = false, // Disable cookies
                    UseProxy = false
                };
            }
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
                UseProxy = false
            };
        }

        private HttpClientHandler CreateHttpClientHandlerWithCustomizedCrlCheck(HttpClientConfig config)
        {
            logger.Debug("Creating HttpClientHandler with customized CRL check");
            var revocationVerifier = new CertificateRevocationVerifier(
                config,
                TimeProvider.Instance,
                GetHttpClientForCrlCheck(),
                CertificateCrlDistributionPointsExtractor.Instance,
                new CrlParser(EnvironmentOperations.Instance),
                new CrlRepository(config.EnableCRLInMemoryCaching, config.EnableCRLDiskCaching));
            return new HttpClientHandler
            {
                CheckCertificateRevocationList = false,
                ServerCertificateCustomValidationCallback = revocationVerifier.CertificateValidationCallback,
                SslProtocols = config.GetRequestedTlsProtocolsRange(),
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                UseCookies = false, // Disable cookies
                UseProxy = false
            };
        }

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
            class RetryReasonRule : IRule
            {
                int retryReason;

                internal RetryReasonRule()
                {
                    retryReason = 0;
                }

                public void SetRetryReason(int reason)
                {
                    retryReason = reason;
                }

                void IRule.apply(NameValueCollection queryParams)
                {
                    queryParams.Set(RestParams.SF_QUERY_RETRY_REASON, retryReason.ToString());
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

            internal Uri Update(int retryReason = 0)
            {
                // Optimization to bypass parsing if there is no rules at all.
                if (rules.Count == 0)
                {
                    return uriBuilder.Uri;
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

                return uriBuilder.Uri;
            }
        }
        private class RetryHandler : DelegatingHandler
        {
            static private SFLogger logger = SFLoggerFactory.GetLogger<RetryHandler>();

            private bool disableRetry;
            private bool forceRetryOn404;
            private int maxRetryCount;
            private bool includeRetryReason;
            private int connectionLimit;

            internal RetryHandler(HttpMessageHandler innerHandler, bool disableRetry, bool forceRetryOn404, int maxRetryCount, bool includeRetryReason, int connectionLimit) : base(innerHandler)
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
                string absolutePath = requestMessage.RequestUri.AbsolutePath;
                bool isLoginRequest = IsLoginEndpoint(absolutePath);
                bool isOktaSSORequest = IsOktaSSORequest(requestMessage.RequestUri.Host, absolutePath);
                int backOffInSec = s_baseBackOffTime;
                int totalRetryTime = 0;
                Exception lastException = null;

                ServicePoint p = ServicePointManager.FindServicePoint(requestMessage.RequestUri);
                p.Expect100Continue = false; // Saves about 100 ms per request
                p.UseNagleAlgorithm = false; // Saves about 200 ms per request
                p.ConnectionLimit = connectionLimit;    // Default value is 2, we need more connections for performing multiple parallel queries

                TimeSpan httpTimeout = (TimeSpan)requestMessage.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY];
                TimeSpan restTimeout = (TimeSpan)requestMessage.Properties[BaseRestRequest.REST_REQUEST_TIMEOUT_KEY];

                if (logger.IsDebugEnabled())
                {
                    logger.Debug("Http request timeout : " + httpTimeout);
                    logger.Debug("Rest request timeout : " + restTimeout);
                }

                CancellationTokenSource childCts = null;

                UriUpdater updater = new UriUpdater(requestMessage.RequestUri, includeRetryReason);
                int retryCount = 0;

                long startTimeInMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                while (true)
                {
                    try
                    {
                        childCts = null;

                        if (!httpTimeout.Equals(Timeout.InfiniteTimeSpan))
                        {
                            childCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                            if (httpTimeout.Ticks == 0)
                                childCts.Cancel();
                            else
                                childCts.CancelAfter(httpTimeout);
                        }
                        response = await base.SendAsync(requestMessage, childCts == null ?
                            cancellationToken : childCts.Token).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        lastException = e;
                        if (cancellationToken.IsCancellationRequested)
                        {
                            logger.Info("SF rest request timeout or explicit cancel called.");
                            cancellationToken.ThrowIfCancellationRequested();
                        }
                        else if (childCts != null && childCts.Token.IsCancellationRequested)
                        {
                            logger.Warn($"Http request timeout. Retry the request after max {backOffInSec} sec.");
                        }
                        else
                        {
                            Exception innermostException = GetInnerMostException(e);

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

                    totalRetryTime = (int)((DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - startTimeInMilliseconds) / 1000);

                    if (childCts != null)
                    {
                        childCts.Dispose();
                    }

                    int errorReason = 0;

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
                            bool isRetryable = isRetryableHTTPCode((int)response.StatusCode, forceRetryOn404);

                            if (!isRetryable || disableRetry)
                            {
                                // No need to keep retrying, stop here
                                return response;
                            }
                        }
                        errorReason = (int)response.StatusCode;
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

                    retryCount++;
                    if ((maxRetryCount > 0) && (retryCount > maxRetryCount))
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

                    // Disposing of the response if not null now that we don't need it anymore
                    response?.Dispose();

                    requestMessage.RequestUri = updater.Update(errorReason);

                    logger.Debug($"Sleep {backOffInSec} seconds and then retry the request, retryCount: {retryCount}");

                    await Task.Delay(TimeSpan.FromSeconds(backOffInSec), cancellationToken).ConfigureAwait(false);

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
        /// Check whether or not the error is retryable or not.
        /// </summary>
        /// <param name="statusCode">The http status code.</param>
        /// <returns>True if the request should be retried, false otherwise.</returns>
        static public bool isRetryableHTTPCode(int statusCode, bool forceRetryOn404)
        {
            if (forceRetryOn404 && statusCode == 404)
                return true;
            return (500 <= statusCode && statusCode < 600) ||
            // Forbidden
            (statusCode == 403) ||
            // Request timeout
            (statusCode == 408) ||
            // Too many requests
            (statusCode == 429);
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


