using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;
using System;
using System.Threading;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    public class HttpUtilWiremockRunnerTest
    {
        private WiremockRunner _runner;

        [OneTimeSetUp]
        public void BeforeAll()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        [SetUp]
        public void BeforeEach()
        {
            _runner.ResetMapping();
        }

        [OneTimeTearDown]
        public void AfterAll()
        {
            _runner.Stop();
        }

        [Test]
        public async Task TestHttp307Retry()
        {
            // arrange
            _runner.AddMappings("wiremock/HttpUtil/http_307_retry.json");
            var expectedQueryId = "http_307_retry_queryId";
            var httpClient = CreateHttpClientForRetry();
            var request = CreateQueryRequest();

            //act
            var response = await httpClient.SendAsync(request);

            // assert
            await AssertResponseId(response, expectedQueryId);
        }

        [Test]
        public async Task TestHttp308Retry()
        {
            // arrange
            _runner.AddMappings("wiremock/HttpUtil/http_308_retry.json");
            var expectedQueryId = "http_308_retry_queryId";
            var httpClient = CreateHttpClientForRetry();
            var request = CreateQueryRequest();

            //act
            var response = await httpClient.SendAsync(request);

            // assert
            await AssertResponseId(response, expectedQueryId);
        }

        private HttpClient CreateHttpClientForRetry()
        {
            var config = new HttpClientConfig(null, null, null, null, null, false, false, 7, 20);
            var handler = new CustomDelegatingHandler(new HttpClientHandler
            {
                ClientCertificateOptions = ClientCertificateOption.Manual,
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true,
                AllowAutoRedirect = true,
            });
            return HttpUtil.Instance.CreateNewHttpClient(config, handler);
        }

        private HttpRequestMessage CreateQueryRequest()
        {
            var request = new HttpRequestMessage(HttpMethod.Post, _runner.WiremockBaseHttpsUrl + "/queries/v1/query-request?requestId=abc");
#pragma warning disable CS0618 // Type or member is obsolete
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(BaseRestRequest.s_defaultHttpSecondsTimeout));
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(BaseRestRequest.s_defaultRestRetrySecondsTimeout));
#pragma warning restore CS0618 // Type or member is obsolete
            return request;
        }

        private async Task AssertResponseId(HttpResponseMessage response, string expectedQueryId)
        {
            var json = await response.Content.ReadAsStringAsync();
            var result = JsonConvert.DeserializeObject<QueryExecResponse>(json, JsonUtils.JsonSettings);

            // assert
            Assert.True(response.IsSuccessStatusCode);
            Assert.AreEqual(expectedQueryId, result.data.queryId);
        }
    }

    public class CustomDelegatingHandler : DelegatingHandler
    {
        public CustomDelegatingHandler(HttpMessageHandler httpMessageHandler) : base(httpMessageHandler) { }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken);
            return response;
        }
    }
}
