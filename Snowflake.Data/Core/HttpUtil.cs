/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
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

namespace Snowflake.Data.Core
{
    class HttpUtil
    {
        static private HttpClient httpClient = null;

        static private object httpClientInitLock = new object();
        
        static public HttpClient getHttpClient()
        {
            lock (httpClientInitLock)
            {
                if (httpClient == null)
                {
                    initHttpClient();
                }
                return httpClient;
            }
        }        

        static private void initHttpClient()
        {
            // enforce tls v1.2
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.CheckCertificateRevocationList = true;

            // Control how many simultaneous connections to each host are allowed from this client
            ServicePointManager.DefaultConnectionLimit = 20;

            HttpUtil.httpClient = new HttpClient(new RetryHandler(new HttpClientHandler(){
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            }));
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

                TimeSpan httpTimeout = (TimeSpan)requestMessage.Properties["TIMEOUT_PER_HTTP_REQUEST"];

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
                            logger.Debug("SF rest request timeout.");
                            cancellationToken.ThrowIfCancellationRequested(); 
                        }
                        else if (childCts != null && childCts.Token.IsCancellationRequested)
                        {
                            logger.Warn("Http request timeout. Retry the request");
                        }
                        else
                        {
                            //TODO: Should probably check to see if the error is recoverable or transient.
                            logger.Warn("Error occurred during request, retrying...", e);
                        }
                    }

                    if (response != null)
                    {
                        if (response.IsSuccessStatusCode) {
                            return response;
                        }
                        logger.Debug($"Failed Response: {response.ToString()}");
                    }
                    else 
                    {
                        logger.Info("Response returned was null.");
                    }

                    requestMessage.RequestUri = updater.Update();

                    logger.Debug($"Sleep {backOffInSec} seconds and then retry the request");
                    Thread.Sleep(backOffInSec * 1000);
                    backOffInSec = backOffInSec >= 16 ? 16 : backOffInSec * 2;
                }
            }
        }
    }
}
