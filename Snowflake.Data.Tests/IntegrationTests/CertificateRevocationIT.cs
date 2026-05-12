using System;
using System.Net;
using System.Net.Http;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [IgnoreOnJenkins]
    public class CertificateRevocationIT : SFBaseTest
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public CertificateRevocationIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture) { _fixture = fixture; }

        [Fact(Skip = "Temporarily ignored")]
        public void TestCertificate()
        {
            // arrange
            var config = new HttpClientConfig(
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                3,
                20,
                true,
                CertRevocationCheckMode.Enabled.ToString(),
                true,
                true,
                false);
            var httpClient = HttpUtil.Instance.CreateNewHttpClient(config);
            var restRequester = new RestRequester(httpClient);
            var request = CreateRequest();

            // act
            var thrown = Assert.Throws<AggregateException>(() => restRequester.Get(new RestRequestWrapper(request)));

            // assert
            Assert.IsType<HttpRequestException>(thrown.InnerException);
            var innerException = (HttpRequestException)thrown.InnerException;
#if NETFRAMEWORK
            Assert.Contains("Unauthorized", innerException.Message);
#else
            Assert.Equal(HttpStatusCode.Unauthorized, innerException.StatusCode);
#endif
            // In case of failed revocation check the StatusCode would be null
            // and HttpRequestException would contain an inner exception of type AuthenticationException
            // with message: "The remote certificate was rejected by the provided RemoteCertificateValidationCallback.".
            // In case of unexpected exception thrown from the callback we would get TaskCanceledException instead of AggregateException.
        }

        private HttpRequestMessage CreateRequest()
        {
            var host = string.IsNullOrEmpty(_fixture.testConfig.host) ? $"{_fixture.testConfig.account}.snowflakecomputing.com" : _fixture.testConfig.host;
            var request = new HttpRequestMessage(HttpMethod.Post, $"https://{host}/queries/v1/abort-request");
            var timeout = TimeSpan.FromSeconds(30);
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, timeout);
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, timeout);
            return request;
        }
    }
}
