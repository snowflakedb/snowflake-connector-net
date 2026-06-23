using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class RedirectUnitTest : IDisposable
    {
        private readonly WiremockRunner _runner;

        public RedirectUnitTest()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        public void Dispose()
        {
            _runner.Dispose();
        }

        [SFFact(SkipCondition.SkipOnJenkins, RetriesCount = RetriesCount.Thrice)]
        public async Task TestHttp307Retry()
        {
            // arrange
            _runner.AddMappings("wiremock/HttpUtil/http_307_retry.json");
            var expectedQueryId = "http_307_retry_queryId";
            var httpClient = CreateHttpClientForRetry();
            var request = CreateQueryRequest();

            //act
            var response = await httpClient.SendAsync(request).ConfigureAwait(false);

            // assert
            await AssertResponseId(response, expectedQueryId).ConfigureAwait(false);
        }

        [SFFact(SkipCondition.SkipOnJenkins, RetriesCount = RetriesCount.Thrice)]
        public async Task TestHttp308Retry()
        {
            // arrange
            _runner.AddMappings("wiremock/HttpUtil/http_308_retry.json");
            var expectedQueryId = "http_308_retry_queryId";
            var httpClient = CreateHttpClientForRetry();
            var request = CreateQueryRequest();

            //act
            var response = await httpClient.SendAsync(request).ConfigureAwait(false);

            // assert
            await AssertResponseId(response, expectedQueryId).ConfigureAwait(false);
        }

        private HttpClient CreateHttpClientForRetry()
        {
            var config = new HttpClientConfig(null, null, null, null, null, false, false, 7, 20);
            var handler = new CustomDelegatingHandler(new HttpClientHandler
            {
                ClientCertificateOptions = ClientCertificateOption.Manual,
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true,
                AllowAutoRedirect = false,
            });
            return HttpUtil.Instance.CreateNewHttpClient(config, handler);
        }

        private HttpRequestMessage CreateQueryRequest()
        {
            var request = new HttpRequestMessage(HttpMethod.Post, _runner.Url + "/queries/v1/query-request?requestId=abc");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(BaseRestRequest.s_defaultHttpSecondsTimeout));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(BaseRestRequest.s_defaultRestRetrySecondsTimeout));
            return request;
        }

        private async Task AssertResponseId(HttpResponseMessage response, string expectedQueryId)
        {
            var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            var result = JsonConvert.DeserializeObject<QueryExecResponse>(json, JsonUtils.JsonSettings);

            // assert
            Assert.True(response.IsSuccessStatusCode);
            Assert.Equal(expectedQueryId, result.data.queryId);
        }
    }

    public class CustomDelegatingHandler : DelegatingHandler
    {
        public CustomDelegatingHandler(HttpMessageHandler httpMessageHandler) : base(httpMessageHandler) { }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            return response;
        }
    }
}
