using System.Net.Http;
using Xunit;
using Snowflake.Data.Core;
using RichardSzalay.MockHttp;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System;
using System.Security.Authentication;
using Moq;
using Moq.Protected;
using Snowflake.Data.Core.Extensions;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class HttpUtilTest
    {
        [SFFact]
        public async Task TestNonRetryableHttpExceptionThrowsError()
        {
            var request = new HttpRequestMessage(HttpMethod.Post, new Uri("https://authenticationexceptiontest.com/"));
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, Timeout.InfiniteTimeSpan);
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, Timeout.InfiniteTimeSpan);

            var handler = new Mock<DelegatingHandler>();
            handler.Protected()
              .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => req.RequestUri.ToString().Contains("https://authenticationexceptiontest.com/")),
                ItExpr.IsAny<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("", new AuthenticationException()));

            var httpClient = HttpUtil.Instance.GetHttpClient(
                new HttpClientConfig("fakeHost", "fakePort", "user", "password", "fakeProxyList", false, false, 7, 20, certRevocationCheckMode: "ENABLED"),
                handler.Object);

            try
            {
                await httpClient.SendAsync(request, CancellationToken.None).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (HttpRequestException e)
            {
                Assert.IsType<AuthenticationException>(e.InnerException);
            }
            catch (Exception unexpected)
            {
                Assert.Fail($"Unexpected {unexpected.GetType()} exception occurred");
            }
        }

        [SFTheory]
        // Parameters: status code, force retry on 404, expected retryable value
        [InlineData(HttpStatusCode.OK, false, false)]
        [InlineData(HttpStatusCode.BadRequest, false, false)]
        [InlineData(HttpStatusCode.Forbidden, false, true)]
        [InlineData(HttpStatusCode.NotFound, false, false)]
        [InlineData(HttpStatusCode.NotFound, true, true)] // force retry on 404
        [InlineData(HttpStatusCode.RequestTimeout, false, true)]
        [InlineData((HttpStatusCode)429, false, true)] // HttpStatusCode.TooManyRequests is not available on .NET Framework
        [InlineData(HttpStatusCode.InternalServerError, false, true)]
        [InlineData(HttpStatusCode.ServiceUnavailable, false, true)]
        [InlineData(HttpStatusCode.TemporaryRedirect, false, true)]
        [InlineData((HttpStatusCode)308, false, true)]  // HttpStatusCode.PermanentRedirect is not available on .NET Framework
        public async Task TestIsRetryableHTTPCode(HttpStatusCode statusCode, bool forceRetryOn404, bool expectedIsRetryable)
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("https://test.snowflakecomputing.com")
            .Respond(statusCode);
            var client = mockHttp.ToHttpClient();
            var response = await client.GetAsync("https://test.snowflakecomputing.com");

            bool actualIsRetryable = HttpUtil.isRetryableHTTPCode((int)response.StatusCode, forceRetryOn404);

            Assert.Equal(expectedIsRetryable, actualIsRetryable);
        }

        // Parameters: request url, expected value
        [InlineData("https://test.snowflakecomputing.com/session/v1/login-request", true)]
        [InlineData("https://test.snowflakecomputing.com/session/authenticator-request", true)]
        [InlineData("https://test.snowflakecomputing.com/session/token-request", true)]
        [InlineData("https://test.snowflakecomputing.com/queries/v1/query-request", false)]
        [SFTheory]
        public void TestIsLoginUrl(string requestUrl, bool expectedIsLoginEndpoint)
        {
            // given
            var uri = new Uri(requestUrl);

            // when
            bool isLoginEndpoint = HttpUtil.IsLoginEndpoint(uri.AbsolutePath);

            // then
            Assert.Equal(expectedIsLoginEndpoint, isLoginEndpoint);
        }

        // Parameters: request url, expected value
        [InlineData("https://dev.okta.com/sso/saml", true)]
        [InlineData("https://test.snowflakecomputing.com/session/v1/login-request", false)]
        [InlineData("https://test.snowflakecomputing.com/session/authenticator-request", false)]
        [InlineData("https://test.snowflakecomputing.com/session/token-request", false)]
        [SFTheory]
        public void TestIsOktaSSORequest(string requestUrl, bool expectedIsOktaSSORequest)
        {
            // given
            var uri = new Uri(requestUrl);

            // when
            bool isOktaSSORequest = HttpUtil.IsOktaSSORequest(uri.Host, uri.AbsolutePath);

            // then
            Assert.Equal(expectedIsOktaSSORequest, isOktaSSORequest);
        }

        // Parameters: time in seconds
        [InlineData(4)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(128)]
        [SFTheory]
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
                Assert.True(jitter >= lowerBound && jitter <= upperBound);
            }
        }

        [SFFact]
        public void TestCreateHttpClientHandlerWithProxy()
        {
            // arrange
            var config = new HttpClientConfig(
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
            Assert.True(handler.UseProxy);
            Assert.NotNull(handler.Proxy);
        }

        [SFFact]
        public void TestCreateHttpClientHandlerWithoutProxy()
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
                20,
                0
            );

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.False(handler.UseProxy);
            Assert.Null(handler.Proxy);
        }
        [SFFact]
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
                Assert.Equal(expectedLimit, ServicePointManager.DefaultConnectionLimit);
            }
            finally
            {
                ServicePointManager.DefaultConnectionLimit = originalLimit;
            }
        }
        [SFFact]
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
                Assert.Equal(HttpUtil.DefaultConnectionLimit, ServicePointManager.DefaultConnectionLimit);
            }
            finally
            {
                ServicePointManager.DefaultConnectionLimit = originalLimit;
            }
        }
    }
}
