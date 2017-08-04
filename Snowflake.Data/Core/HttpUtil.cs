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
        static private HttpClient httpClient;

        static public HttpClient getHttpClient()
        {
            if (httpClient == null)
            {
                initHttpClient();
            }
            return httpClient;
        }

        static private void initHttpClient()
        {
            // enforce tls v1.2
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.CheckCertificateRevocationList = true;

            httpClient = new HttpClient(new RetryHandler(new HttpClientHandler()));
            // default timeout for each request is 16 seconds
            //httpClient.Timeout = TimeSpan.FromSeconds(16);
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

                var childCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                while (true)
                {
                    try
                    {   
                        response = await base.SendAsync(requestMessage, childCts.Token);
                    }
                    catch(OperationCanceledException e)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            logger.Debug("SF rest request timeout.");
                            cancellationToken.ThrowIfCancellationRequested(); 
                        }
                    }

                    if (response.IsSuccessStatusCode)
                    {
                        logger.Info("Retried request succeed.");
                        logger.TraceFormat("Success Response {0}", response.ToString());
                        return response;
                    }

                    logger.TraceFormat("Failed Response: {0}", response.ToString());
                    logger.DebugFormat("Sleep {0} seconds and then retry the request", backOffInSec);
                    Thread.Sleep(backOffInSec * 1000);
                    backOffInSec = backOffInSec >= 16 ? 16 : backOffInSec * 2;
                }
            }
        }
    }
}
