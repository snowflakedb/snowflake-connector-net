/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Threading.Tasks;
using System.Net.Http;
using System.Net;
using System;
using System.Threading;
using Common.Logging;

namespace Snowflake.Data.Core
{
    class HttpUtil
    {
        static public HttpClient initHttpClient(TimeSpan timeout)
        {
            // enforce tls v1.2
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.CheckCertificateRevocationList = true;

            return new HttpClient(new RetryHandler(new HttpClientHandler())) { Timeout  = timeout };
        }

        class RetryHandler : DelegatingHandler
        {
            static private ILog logger = LogManager.GetLogger<RetryHandler>();
            
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
                            throw e;
                        }
                    }

                    if (response != null)
                    {
                        if (response.IsSuccessStatusCode) {
                            logger.TraceFormat("Success Response {0}", response.ToString());
                            return response;
                        }
                        logger.TraceFormat("Failed Response: {0}", response.ToString());
                    }
                    else 
                    {
                        logger.Info("Response returned was null.");
                    }

                    logger.DebugFormat("Sleep {0} seconds and then retry the request", backOffInSec);
                    Thread.Sleep(backOffInSec * 1000);
                    backOffInSec = backOffInSec >= 16 ? 16 : backOffInSec * 2;
                }
            }
        }
    }
}
