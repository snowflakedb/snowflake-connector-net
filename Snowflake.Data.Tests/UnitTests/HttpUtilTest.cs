using System.Net.Http;
using NUnit.Framework;
using Snowflake.Data.Core;
using RichardSzalay.MockHttp;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System;
using System.Security.Authentication;
using Moq;
using Moq.Protected;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class HttpUtilTest
    {
        [Test]
        public async Task TestNonRetryableHttpExceptionThrowsError()
        {
            var request = new HttpRequestMessage(HttpMethod.Post, new Uri("https://authenticationexceptiontest.com/"));
            // Disable warning as this is the way to be compliant with netstandard2.0
            // API reference: https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httprequestmessage?view=netstandard-2.0
#pragma warning disable CS0618 // Type or member is obsolete
            request.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = Timeout.InfiniteTimeSpan;
            request.Properties[BaseRestRequest.REST_REQUEST_TIMEOUT_KEY] = Timeout.InfiniteTimeSpan;
#pragma warning restore CS0618 // Type or member is obsolete

            var handler = new Mock<DelegatingHandler>();
            handler.Protected()
              .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => req.RequestUri.ToString().Contains("https://authenticationexceptiontest.com/")),
                ItExpr.IsAny<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("", new AuthenticationException()));

            var httpClient = HttpUtil.Instance.GetHttpClient(
                new HttpClientConfig(true, "fakeHost", "fakePort", "user", "password", "fakeProxyList", false, false, 7, 20, certRevocationCheckMode: "ENABLED"),
                handler.Object);

            try
            {
                await httpClient.SendAsync(request, CancellationToken.None).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (HttpRequestException e)
            {
                Assert.IsInstanceOf<AuthenticationException>(e.InnerException);
            }
            catch (Exception unexpected)
            {
                Assert.Fail($"Unexpected {unexpected.GetType()} exception occurred");
            }
        }

        [Test]
        // Parameters: status code, force retry on 404, expected retryable value
        [TestCase(HttpStatusCode.OK, false, false)]
        [TestCase(HttpStatusCode.BadRequest, false, false)]
        [TestCase(HttpStatusCode.Forbidden, false, true)]
        [TestCase(HttpStatusCode.NotFound, false, false)]
        [TestCase(HttpStatusCode.NotFound, true, true)] // force retry on 404
        [TestCase(HttpStatusCode.RequestTimeout, false, true)]
        [TestCase((HttpStatusCode)429, false, true)] // HttpStatusCode.TooManyRequests is not available on .NET Framework
        [TestCase(HttpStatusCode.InternalServerError, false, true)]
        [TestCase(HttpStatusCode.ServiceUnavailable, false, true)]
        [TestCase(HttpStatusCode.TemporaryRedirect, false, true)]
        [TestCase((HttpStatusCode)308, false, true)]  // HttpStatusCode.PermanentRedirect is not available on .NET Framework
        public async Task TestIsRetryableHTTPCode(HttpStatusCode statusCode, bool forceRetryOn404, bool expectedIsRetryable)
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("https://test.snowflakecomputing.com")
            .Respond(statusCode);
            var client = mockHttp.ToHttpClient();
            var response = await client.GetAsync("https://test.snowflakecomputing.com");

            bool actualIsRetryable = HttpUtil.isRetryableHTTPCode((int)response.StatusCode, forceRetryOn404);

            Assert.AreEqual(expectedIsRetryable, actualIsRetryable);
        }

        // Parameters: request url, expected value
        [TestCase("https://test.snowflakecomputing.com/session/v1/login-request", true)]
        [TestCase("https://test.snowflakecomputing.com/session/authenticator-request", true)]
        [TestCase("https://test.snowflakecomputing.com/session/token-request", true)]
        [TestCase("https://test.snowflakecomputing.com/queries/v1/query-request", false)]
        [Test]
        public void TestIsLoginUrl(string requestUrl, bool expectedIsLoginEndpoint)
        {
            // given
            var uri = new Uri(requestUrl);

            // when
            bool isLoginEndpoint = HttpUtil.IsLoginEndpoint(uri.AbsolutePath);

            // then
            Assert.AreEqual(expectedIsLoginEndpoint, isLoginEndpoint);
        }

        // Parameters: request url, expected value
        [TestCase("https://dev.okta.com/sso/saml", true)]
        [TestCase("https://test.snowflakecomputing.com/session/v1/login-request", false)]
        [TestCase("https://test.snowflakecomputing.com/session/authenticator-request", false)]
        [TestCase("https://test.snowflakecomputing.com/session/token-request", false)]
        [Test]
        public void TestIsOktaSSORequest(string requestUrl, bool expectedIsOktaSSORequest)
        {
            // given
            var uri = new Uri(requestUrl);

            // when
            bool isOktaSSORequest = HttpUtil.IsOktaSSORequest(uri.Host, uri.AbsolutePath);

            // then
            Assert.AreEqual(expectedIsOktaSSORequest, isOktaSSORequest);
        }

        // Parameters: time in seconds
        [TestCase(4)]
        [TestCase(8)]
        [TestCase(16)]
        [TestCase(32)]
        [TestCase(64)]
        [TestCase(128)]
        [Test]
        public void TestGetJitter(int seconds)
        {
            // given
            var lowerBound = -(seconds / 2);
            var upperBound = seconds / 2;

            double jitter;
            for (var i = 0; i < 10; i++)
            {
                // when
                jitter = HttpUtil.GetJitter(seconds);

                // then
                Assert.IsTrue(jitter >= lowerBound && jitter <= upperBound);
            }
        }

        [Test]
        public void TestCreateHttpClientHandlerWithExplicitProxy()
        {
            // arrange
            var config = new HttpClientConfig(
                true,
                "snowflake.com",
                "123",
                "testUser",
                "proxyPassword",
                "localhost",
                false,
                false,
                7,
                20
            );

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.IsTrue(handler.UseProxy);
            Assert.IsNotNull(handler.Proxy);
        }

        [Test]
        public void TestCreateHttpClientHandlerWithDefaultProxy()
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
                7,
                0
            );

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.IsTrue(handler.UseProxy);
            Assert.IsNull(handler.Proxy);
        }

        [Test]
        public void TestCreateHttpClientHandlerWithoutProxy()
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
                20,
                0
            );

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.IsFalse(handler.UseProxy);
            Assert.IsNull(handler.Proxy);
        }

        [Test]
        public void TestIgnoreProxyDetailsIfProxyDisabled()
        {
            // arrange
            var config = new HttpClientConfig(
                false,
                "snowflake.com",
                "123",
                "testUser",
                "proxyPassword",
                "localhost",
                false,
                false,
                7,
                20
            );

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.IsFalse(handler.UseProxy);
            Assert.IsNull(handler.Proxy);
        }

        [NonParallelizable]
        [Test]
        public void TestDefaultConnectionLimitIsNotChangedWhenOver50()
        {
            // arrange
            var expectedLimit = 51;
            var originalLimit = ServicePointManager.DefaultConnectionLimit;
            ServicePointManager.DefaultConnectionLimit = expectedLimit;

            try
            {
                // act
                HttpUtil.Instance.IncreaseLowDefaultConnectionLimitOfServicePointManager();

                // assert
                Assert.AreEqual(expectedLimit, ServicePointManager.DefaultConnectionLimit);
            }
            finally
            {
                ServicePointManager.DefaultConnectionLimit = originalLimit;
            }
        }

        [NonParallelizable]
        [Test]
        public void TestDefaultConnectionLimitIsChangedToDefaultWhenUnder50()
        {
            // arrange
            var originalLimit = ServicePointManager.DefaultConnectionLimit;
            ServicePointManager.DefaultConnectionLimit = 49;

            try
            {
                // act
                HttpUtil.Instance.IncreaseLowDefaultConnectionLimitOfServicePointManager();

                // assert
                Assert.AreEqual(HttpUtil.DefaultConnectionLimit, ServicePointManager.DefaultConnectionLimit);
            }
            finally
            {
                ServicePointManager.DefaultConnectionLimit = originalLimit;
            }
        }
    }
}
