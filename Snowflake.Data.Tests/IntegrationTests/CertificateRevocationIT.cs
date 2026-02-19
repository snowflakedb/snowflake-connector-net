using System;
using System.Net;
using System.Net.Http;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Revocation;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    [IgnoreOnJenkins]
    public class CertificateRevocationIT : SFBaseTest
    {
        [Test]
        [Ignore("Temporarily ignored")]
        public void TestCertificate()
        {
            // arrange
            var config = new HttpClientConfig(
                false,
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
            Assert.That(thrown.InnerException, Is.TypeOf<HttpRequestException>());
            var innerException = (HttpRequestException)thrown.InnerException;
#if NETFRAMEWORK
            Assert.That(innerException.Message, Does.Contain("Unauthorized"));
#else
            Assert.AreEqual(HttpStatusCode.Unauthorized, innerException.StatusCode);
#endif
            // In case of failed revocation check the StatusCode would be null
            // and HttpRequestException would contain an inner exception of type AuthenticationException
            // with message: "The remote certificate was rejected by the provided RemoteCertificateValidationCallback.".
            // In case of unexpected exception thrown from the callback we would get TaskCanceledException instead of AggregateException.
        }

        private HttpRequestMessage CreateRequest()
        {
            var host = string.IsNullOrEmpty(testConfig.host) ? $"{testConfig.account}.snowflakecomputing.com" : testConfig.host;
            var request = new HttpRequestMessage(HttpMethod.Post, $"https://{host}/queries/v1/abort-request");
            var timeout = TimeSpan.FromSeconds(30);
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, timeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, timeout);
            return request;
        }
    }
}
