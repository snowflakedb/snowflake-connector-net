/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

using System.Net.Http;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using RichardSzalay.MockHttp;
    using System.Threading.Tasks;
    using System.Net;
    using System;

    [TestFixture]
    class HttpUtilTest
    {
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
        public void ShouldCreateHttpClientHandlerWithProxy()
        {
            // given
            var config = new HttpClientConfig(
                true,
                "snowflake.com",
                "123",
                "testUser",
                "proxyPassword",
                "localhost", 
                false,
                false,
                7
            );
            
            // when
            var handler = (HttpClientHandler) HttpUtil.Instance.SetupCustomHttpHandler(config);
            
            // then
            Assert.IsTrue(handler.UseProxy);
            Assert.IsNotNull(handler.Proxy);
        }

        [Test]
        public void ShouldCreateHttpClientHandlerWithoutProxy()
        {
            // given
            var config = new HttpClientConfig(
                true,
                null,
                null,
                null,
                null,
                null, 
                false,
                false,
                0
            );
            
            // when
            var handler = (HttpClientHandler) HttpUtil.Instance.SetupCustomHttpHandler(config);
            
            // then
            Assert.IsFalse(handler.UseProxy);
            Assert.IsNull(handler.Proxy);
        }
    }
}
