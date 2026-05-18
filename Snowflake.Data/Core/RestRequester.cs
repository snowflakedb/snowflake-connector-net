using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    /// <summary>
    /// The RestRequester is responsible to send out a rest request and receive response.
    /// HTTP-level retry is handled by HttpClient.RetryHandler (HttpUtil.cs).
    /// JSON deserialization retry (1 attempt) is handled here for transient responses.
    /// </summary>
    internal interface IRestRequester
    {
        Task<T> PostAsync<T>(IRestRequest postRequest, CancellationToken cancellationToken);

        T Post<T>(IRestRequest postRequest);

        Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken);

        T Get<T>(IRestRequest request);

        Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken);

        HttpResponseMessage Get(IRestRequest request);

    }

    internal interface IMockRestRequester : IRestRequester
    {
        void setHttpClient(HttpClient httpClient);
    }

    internal class RestRequester : IRestRequester
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<RestRequester>();

        internal const string HttpStatusCodeDataKey = "HttpStatusCode";

        protected HttpClient HttpClient;

        public RestRequester(HttpClient httpClient)
        {
            HttpClient = httpClient;
        }

        public T Post<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (PostAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> PostAsync<T>(IRestRequest request, CancellationToken cancellationToken) =>
            SendAsync<T>(HttpMethod.Post, request, cancellationToken);

        public T Get<T>(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync<T>(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        public Task<T> GetAsync<T>(IRestRequest request, CancellationToken cancellationToken) =>
            SendAsync<T>(HttpMethod.Get, request, cancellationToken);

        public Task<HttpResponseMessage> GetAsync(IRestRequest request, CancellationToken cancellationToken) =>
            SendAsync(HttpMethod.Get, request, cancellationToken);

        public HttpResponseMessage Get(IRestRequest request)
        {
            //Run synchronous in a new thread-pool task.
            return Task.Run(async () => await (GetAsync(request, CancellationToken.None)).ConfigureAwait(false)).Result;
        }

        private async Task<T> SendAsync<T>(HttpMethod method, IRestRequest request, CancellationToken cancellationToken)
        {
            using var response = await SendAsync(method, request, cancellationToken).ConfigureAwait(false);
            try
            {
                return await DeserializeResponseAsync<T>(response, cancellationToken).ConfigureAwait(false);
            }
            catch (JsonException ex)
            {
                s_logger.Warn($"JSON deserialization failed for {method}, retrying request. Error: {ex.Message}.", ex);
            }

            // in rare instances server may return invalid response body (e.g. truncated one), whilst returning 2XX response. Those are mostly transient. See SNOW-3422038 for further details.
            using var retryResponse = await SendAsync(method, request, cancellationToken).ConfigureAwait(false);
            return await DeserializeResponseAsync<T>(retryResponse, cancellationToken).ConfigureAwait(false);
        }

        private Task<HttpResponseMessage> SendAsync(HttpMethod method,
                                                          IRestRequest request,
                                                          CancellationToken externalCancellationToken)
        {
            var message = request.ToRequestMessage(method);
            return SendAsync(message, request.GetRestTimeout(), externalCancellationToken, request.getSid());
        }

        protected virtual async Task<HttpResponseMessage> SendAsync(HttpRequestMessage message,
                                                              TimeSpan restTimeout,
                                                              CancellationToken externalCancellationToken,
                                                              string sid = "")
        {
            // merge multiple cancellation token
            using var restRequestTimeout = new CancellationTokenSource(restTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken,
                restRequestTimeout.Token);
            HttpResponseMessage response = null;
            s_logger.Debug($"Executing: {sid} {message.Method} {FormatUri(message.RequestUri)} HTTP/{message.Version}");
            var watch = new Stopwatch();
            int? failedHttpStatusCode = null;
            try
            {
                watch.Start();
                response = await HttpClient
                    .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token)
                    .ConfigureAwait(false);
                watch.Stop();
                if (!response.IsSuccessStatusCode)
                {
                    failedHttpStatusCode = (int)response.StatusCode;
                    s_logger.Error($"Failed response after {watch.ElapsedMilliseconds} ms: {sid} {message.Method} {FormatUri(message.RequestUri)} StatusCode: {(int)response.StatusCode}, ReasonPhrase: '{response.ReasonPhrase}'");
                }
                else
                {
                    s_logger.Debug($"Succeeded response after {watch.ElapsedMilliseconds} ms: {sid} {message.Method} {FormatUri(message.RequestUri)}");
                }
                response.EnsureSuccessStatusCode();

                message?.Dispose();
                return response;
            }
            catch (Exception e)
            {
                if (watch.IsRunning)
                {
                    watch.Stop();
                    s_logger.Error($"Response receiving interrupted by exception after {watch.ElapsedMilliseconds} ms. {sid} {message.Method} {FormatUri(message.RequestUri)}");
                }
                if (failedHttpStatusCode.HasValue)
                {
                    e.Data[HttpStatusCodeDataKey] = failedHttpStatusCode.Value;
                }
                // Disposing of the response if not null now that we don't need it anymore
                response?.Dispose();
                if (restRequestTimeout.IsCancellationRequested)
                {
                    throw new SnowflakeDbException(e, SFError.REQUEST_TIMEOUT);
                }
                throw;
            }
        }

        internal static bool HasUnauthorizedStatusCode(Exception ex)
        {
            var code = FindHttpStatusCode(ex);
            return code == (int)HttpStatusCode.Unauthorized;
        }

        private static string FormatUri(Uri uri)
        {
#if SF_PUBLIC_ENVIRONMENT
            return uri.AbsolutePath;
#else
            return uri.ToString();
#endif
        }

        private static async Task<T> DeserializeResponseAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            #if NET6_0_OR_GREATER
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            using var streamReader = new StreamReader(stream);
            await using var jsonReader = new JsonTextReader(streamReader);
            #else
            using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var streamReader = new StreamReader(stream);
            using var jsonReader = new JsonTextReader(streamReader);
            #endif
            return JsonUtils.Serializer.Deserialize<T>(jsonReader);
        }

        private static int? FindHttpStatusCode(Exception ex)
        {
            if (ex == null)
                return null;
            if (ex.Data.Contains(HttpStatusCodeDataKey) && ex.Data[HttpStatusCodeDataKey] is int statusCode)
                return statusCode;
            if (ex is AggregateException aggEx)
            {
                foreach (var inner in aggEx.Flatten().InnerExceptions)
                {
                    var code = FindHttpStatusCode(inner);
                    if (code.HasValue)
                        return code;
                }
            }
            return FindHttpStatusCode(ex.InnerException);
        }
    }
}
