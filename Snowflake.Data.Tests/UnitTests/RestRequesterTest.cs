using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class RestRequesterTest
    {
        [SFFact]
        public void TestSendAsyncTags401OnException()
        {
            // arrange — real RestRequester with an HttpClient that returns 401
            var mockHandler = new Mock<HttpMessageHandler>();
            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.Unauthorized));

            var httpClient = new HttpClient(mockHandler.Object);
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest();

            // act
            Exception caught = null;
            try
            {
                restRequester.Get<string>(request);
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            // assert — the production SendAsync must tag the exception with 401
            Assert.NotNull(caught);
            Assert.True(RestRequester.HasUnauthorizedStatusCode(caught));
        }

        [SFFact]
        public void TestSendAsyncTags403OnException()
        {
            // arrange
            var mockHandler = new Mock<HttpMessageHandler>();
            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.Forbidden));

            var httpClient = new HttpClient(mockHandler.Object);
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest();

            // act
            Exception caught = null;
            try
            {
                restRequester.Get<string>(request);
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            // assert — 403 should NOT be detected as unauthorized
            Assert.NotNull(caught);
            Assert.False(RestRequester.HasUnauthorizedStatusCode(caught));
        }

        [SFFact]
        public void TestSendAsyncDoesNotTagOnSuccess()
        {
            // arrange
            var mockHandler = new Mock<HttpMessageHandler>();
            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("\"ok\"")
                });

            var httpClient = new HttpClient(mockHandler.Object);
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest();

            // act & assert — no exception, no tagging
            restRequester.Get<string>(request);
        }

        [SFFact]
        public void TestHasUnauthorizedStatusCodeReturnsFalseForNull()
        {
            Assert.False(RestRequester.HasUnauthorizedStatusCode(null));
        }

        [SFFact]
        public void TestHasUnauthorizedStatusCodeReturnsFalseForPlainException()
        {
            Assert.False(RestRequester.HasUnauthorizedStatusCode(new Exception("no http info")));
        }

        private static IRestRequest CreateMockRestRequest()
        {
            var message = new HttpRequestMessage(HttpMethod.Get, "https://test.snowflakecomputing.com/session");
#pragma warning disable CS0618
            message.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = TimeSpan.FromSeconds(16);
            message.Properties[BaseRestRequest.REST_REQUEST_TIMEOUT_KEY] = TimeSpan.FromSeconds(120);
#pragma warning restore CS0618

            var mock = new Mock<IRestRequest>();
            mock.Setup(r => r.ToRequestMessage(It.IsAny<HttpMethod>())).Returns(message);
            mock.Setup(r => r.GetRestTimeout()).Returns(TimeSpan.FromSeconds(120));
            mock.Setup(r => r.getSid()).Returns("test-sid");
            return mock.Object;
        }
    }
}
