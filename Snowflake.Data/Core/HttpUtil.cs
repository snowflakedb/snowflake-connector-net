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

            httpClient = new HttpClient(new RetryHandler());
            // default timeout for each request is 16 seconds
            //httpClient.Timeout = TimeSpan.FromSeconds(16);
        }

        class RetryHandler : DelegatingHandler
        {
            static private ILog logger = LogManager.GetLogger<RetryHandler>();
            
            // each request timeout in 16 seconds if hanging
            const int singleRequestTimeout = 16 * 1000;

            internal RetryHandler()
            {
                InnerHandler = new HttpClientHandler();
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage requestMessage,
                CancellationToken cancellationToken)
            {
                HttpResponseMessage response = null;
                int backOffInSec = 1;

                var currentCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                currentCts.CancelAfter(singleRequestTimeout);

                while (true)
                {
                    try
                    {   
                        // for each http request, we retry up to 16 seconds if hanging. Mark it timeout if 
                        // parent method pass in cancel signal
                        response = await base.SendAsync(requestMessage, currentCts.Token);
                    }
                    catch(OperationCanceledException e)
                    {
                        logger.Debug("http request timeout reached. Cacnel the request.");
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
