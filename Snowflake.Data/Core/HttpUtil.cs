/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

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

namespace Snowflake.Data.Core
{
    class HttpUtil
    {

        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<HttpUtil>();

        static private HttpClient HttpClient = null;

        static private HttpClient HttpClientNoCrlCheck = null;

        static private CookieContainer cookieContainer = null;

        static private object httpClientInitLock = new object();

        /// <summary>
        /// Clear all the cookies for the given uri.
        /// </summary>
        /// <param name="uri">The URI to clear.</param>
        static public void ClearCookies(Uri uri)
        {
            if (cookieContainer == null)
            {
                return;
            }

            var cookies = cookieContainer.GetCookies(uri);
            foreach (Cookie cookie in cookies)
            {
                cookie.Expired = true;
            }
        }
        
        static public HttpClient getHttpClient(bool insecureMode)
        {
            lock (httpClientInitLock)
            {
                HttpClient httpClient = insecureMode ? HttpClientNoCrlCheck : HttpClient;
                if (httpClient == null)
                {
                    httpClient = initHttpClient(!insecureMode);

                    if (insecureMode)
                    {
                        HttpClientNoCrlCheck = httpClient;
                    }
                    else
                    {
                        HttpClient = httpClient;
                    }
                }
                return httpClient;
            }
        }

        static private HttpClient initHttpClient(bool crlCheckEnabled)
        {
            logger.Debug("Creating new http client handler with CheckCertificateRevocationList : " + crlCheckEnabled);
            HttpClientHandler httpHandler = new HttpClientHandler()
            {
                // Verify no certificates have been revoked
                CheckCertificateRevocationList = crlCheckEnabled,
                // Enforce tls v1.2
                SslProtocols = SslProtocols.Tls12,
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
               CookieContainer = cookieContainer = new CookieContainer()
           };

            var httpClient = new HttpClient(new RetryHandler(httpHandler));
            // HttpClient has a default timeout of 100 000 ms, we don't want to interfere with our
            // own connection and command timeout
            httpClient.Timeout = Timeout.InfiniteTimeSpan;
            return httpClient;
        }

        /// <summary>
        /// IRule defines how the queryParams of a uri should be updated in each retry
        /// </summary>
        interface IRule
        {
            void apply(NameValueCollection queryParams);
        }

        /// <summary>
        /// RetryCoundRule would update the retryCount parameter
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
        /// UriUpdater would update the uri in each retry. During construction, it would take in an uri that would later
        /// be updated in each retry and figure out the rules to apply when updating.
        /// </summary>
        internal class UriUpdater
        {
            UriBuilder uriBuilder;
            List<IRule> rules;
            internal UriUpdater(Uri uri)
            {
                uriBuilder = new UriBuilder(uri);
                rules = new List<IRule>();

                if (uri.AbsolutePath.StartsWith(RestPath.SF_QUERY_PATH))
                {
                    rules.Add(new RetryCountRule());
                }

                if (uri.Query != null && uri.Query.Contains(RestParams.SF_QUERY_REQUEST_GUID))
                {
                    rules.Add(new RequestUUIDRule());
                }
            }

            internal Uri Update()
            {
                // Optimization to bypass parsing if there is no rules at all.
                if (rules.Count == 0)
                {
                    return uriBuilder.Uri;
                }

                var queryParams = HttpUtility.ParseQueryString(uriBuilder.Query);

                foreach (IRule rule in rules)
                {
                    rule.apply(queryParams);
                }

                uriBuilder.Query = queryParams.ToString();
                
                return uriBuilder.Uri;
            }
        }

        class RetryHandler : DelegatingHandler
        {
            static private SFLogger logger = SFLoggerFactory.GetLogger<RetryHandler>();
            
            internal RetryHandler(HttpMessageHandler innerHandler) : base(innerHandler)
            {
            }
            
            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage requestMessage,
                CancellationToken cancellationToken)
            {
                HttpResponseMessage response = null;
                int backOffInSec = 1;
                int totalRetryTime = 0;
                int maxDefaultBackoff = 16;

                ServicePoint p = ServicePointManager.FindServicePoint(requestMessage.RequestUri);
                p.Expect100Continue = false; // Saves about 100 ms per request
                p.UseNagleAlgorithm = false; // Saves about 200 ms per request
                p.ConnectionLimit = 20;      // Default value is 2, we need more connections for performing multiple parallel queries

                TimeSpan httpTimeout = (TimeSpan)requestMessage.Properties[SFRestRequest.HTTP_REQUEST_TIMEOUT_KEY];
                TimeSpan restTimeout = (TimeSpan)requestMessage.Properties[SFRestRequest.REST_REQUEST_TIMEOUT_KEY];

                if (logger.IsDebugEnabled())
                {
                    logger.Debug("Http request timeout : " + httpTimeout);
                    logger.Debug("Rest request timeout : " + restTimeout);
                }

                CancellationTokenSource childCts = null;

                UriUpdater updater = new UriUpdater(requestMessage.RequestUri);

                while (true)
                {

                    try
                    {
                        childCts = null;

                        if (!httpTimeout.Equals(Timeout.InfiniteTimeSpan)) {
                            childCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                            childCts.CancelAfter(httpTimeout);
                        }
                        response = await base.SendAsync(requestMessage, childCts == null ? 
                            cancellationToken : childCts.Token).ConfigureAwait(false);
                    }
                    catch(Exception e)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            logger.Debug("SF rest request timeout or explicit cancel called.");
                            cancellationToken.ThrowIfCancellationRequested(); 
                        }
                        else if (childCts != null && childCts.Token.IsCancellationRequested)
                        {
                            logger.Warn("Http request timeout. Retry the request");
                            totalRetryTime += (int)httpTimeout.TotalSeconds;
                        }
                        else
                        {
                            //TODO: Should probably check to see if the error is recoverable or transient.
                            logger.Warn("Error occurred during request, retrying...", e);
                        }
                    }

                    if (childCts != null)
                    {
                        childCts.Dispose();
                    }

                    if (response != null)
                    {
                        if (response.IsSuccessStatusCode) {
                            return response;
                        }
                        else
                        {
                            logger.Debug($"Failed Response: {response.ToString()}");
                            bool isRetryable = isRetryableHTTPCode((int)response.StatusCode);
                            if (!isRetryable)
                            {
                                // No need to keep retrying, stop here
                                return response;
                            }
                        }
                    }
                    else
                    {
                        logger.Info("Response returned was null.");
                    }

                    // Disposing of the response if not null now that we don't need it anymore
                    response?.Dispose();

                    requestMessage.RequestUri = updater.Update();

                    logger.Debug($"Sleep {backOffInSec} seconds and then retry the request");
                    await Task.Delay(TimeSpan.FromSeconds(backOffInSec), cancellationToken);
                    totalRetryTime += backOffInSec;
                    // Set next backoff time
                    backOffInSec = backOffInSec >= maxDefaultBackoff ?
                            maxDefaultBackoff : backOffInSec * 2;

                    if ((restTimeout.TotalSeconds > 0) && (totalRetryTime + backOffInSec > restTimeout.TotalSeconds))
                    {
                        // No need to wait more than necessary if it can be avoided.
                        // If the rest timeout will be reached before the next back-off,
                        // use a smaller one to give the Rest request a chance to timeout early
                        backOffInSec = Math.Max(1, (int)restTimeout.TotalSeconds - totalRetryTime - 1);
                    }
                }
            }

            /// <summary>
            /// Check whether or not the error is retryable or not.
            /// </summary>
            /// <param name="statusCode">The http status code.</param>
            /// <returns>True if the request should be retried, false otherwise.</returns>
            private bool isRetryableHTTPCode(int statusCode)
            {
                return (500 <= statusCode) && (statusCode < 600) ||
                // Forbidden
                (statusCode == 403) ||
                // Request timeout
                (statusCode == 408);
            }
        }
    }
}
