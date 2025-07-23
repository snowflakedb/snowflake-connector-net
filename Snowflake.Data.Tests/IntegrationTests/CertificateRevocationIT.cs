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
    public class CertificateRevocationIT : SFBaseTest
    {
        [Test]
        public void TestCertificate()
        {
            // arrange
            var config = new HttpClientConfig(
                true,
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                3,
                true,
                false,
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
            Assert.AreEqual(HttpStatusCode.Unauthorized, innerException.StatusCode);
            // In case of failed revocation check the StatusCode would be null
            // and HttpRequestException would contain an inner exception of type AuthenticationException
            // with message: "The remote certificate was rejected by the provided RemoteCertificateValidationCallback.".
            // In case of unexpected exception thrown from the callback we would get TaskCanceledException instead of AggregateException.
        }

        private HttpRequestMessage CreateRequest()
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"https://{testConfig.host}/queries/v1/abort-request");
            var timeout = TimeSpan.FromSeconds(30);
            request.Properties.Add(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, timeout);
            request.Properties.Add(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, timeout);
            return request;
        }
    }
}
