/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading.Tasks;
using System.Net.Http;
using System.Net;
using System;
using System.Threading;
using Snowflake.Data.Log;

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

            HttpUtil.httpClient = new HttpClient(new RetryHandler(new HttpClientHandler()));
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
                            cancellationToken : childCts.Token);
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
                            logger.Debug($"Success Response: {response.ToString()}");
                            return response;
                        }
                        logger.Debug($"Failed Response: {response.ToString()}");
                    }
                    else 
                    {
                        logger.Info("Response returned was null.");
                    }

                    logger.Debug($"Sleep {backOffInSec} seconds and then retry the request");
                    Thread.Sleep(backOffInSec * 1000);
                    backOffInSec = backOffInSec >= 16 ? 16 : backOffInSec * 2;
                }
            }
        }
    }
}
