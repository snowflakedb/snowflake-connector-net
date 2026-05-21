using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using Xunit;
using Snowflake.Data.Core;

namespace Snowflake.Data.Tests.UnitTests
{

    public class RestRequesterTest
    {
        [Test]
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

        [Test]
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

        [Test]
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
            Assert.DoesNotThrow(() => restRequester.Get<string>(request));
        }

        [Test]
        public void TestHasUnauthorizedStatusCodeReturnsFalseForNull()
        {
            Assert.False(RestRequester.HasUnauthorizedStatusCode(null));
        }

        [Test]
        public void TestHasUnauthorizedStatusCodeReturnsFalseForPlainException()
        {
            Assert.False(RestRequester.HasUnauthorizedStatusCode(new Exception("no http info")));
        }

        [Test]
        public async Task TestPostAsyncDeserializesValidJsonWithoutRetry()
        {
            // arrange
            var validJson = "{\"message\":\"Some message!\",\"code\":0,\"success\":true}";
            var httpClient = CreateHttpClientWithSequentialResponses(
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(validJson) });
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest(HttpMethod.Post);

            // act
            var result = await restRequester.PostAsync<NullDataResponse>(request, CancellationToken.None);

            // assert
            Assert.True(result.success);
            Assert.Equal("Some message!", result.message);
        }

        [Test]
        public async Task TestPostAsyncRetriesOnTruncatedJson()
        {
            // arrange
            var truncatedJson = "{\"success\":tr";
            var validJson = "{\"message\":\"Some message!\",\"code\":0,\"success\":true}";
            var httpClient = CreateHttpClientWithSequentialResponses(
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(truncatedJson) },
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(validJson) });
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest(HttpMethod.Post);

            // act
            var result = await restRequester.PostAsync<NullDataResponse>(request, CancellationToken.None);

            // assert
            Assert.True(result.success);
            Assert.Equal("Some message!", result.message);
        }

        [Test]
        public void TestPostAsyncThrowsWhenBothAttemptsReturnTruncatedJson()
        {
            // arrange
            var truncatedJson = "{\"success\":tr";
            var httpClient = CreateHttpClientWithSequentialResponses(
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(truncatedJson) },
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(truncatedJson) });
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest(HttpMethod.Post);

            // act & assert
            Assert.ThrowsAsync<JsonReaderException>(
                () => restRequester.PostAsync<NullDataResponse>(request, CancellationToken.None));
        }

        [Test]
        public async Task TestGetAsyncRetriesOnTruncatedJson()
        {
            // arrange
            var truncatedJson = "{\"success\":tr";
            var validJson = "{\"message\":\"Some message!\",\"code\":0,\"success\":true}";
            var httpClient = CreateHttpClientWithSequentialResponses(
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(truncatedJson) },
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(validJson) });
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest(HttpMethod.Get);

            // act
            var result = await restRequester.GetAsync<NullDataResponse>(request, CancellationToken.None);

            // assert
            Assert.True(result.success);
            Assert.Equal("Some message!", result.message);
        }

        [Test]
        public void TestGetAsyncThrowsWhenBothAttemptsReturnTruncatedJson()
        {
            // arrange
            var truncatedJson = "{\"success\":tr";
            var httpClient = CreateHttpClientWithSequentialResponses(
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(truncatedJson) },
                new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(truncatedJson) });
            var restRequester = new RestRequester(httpClient);
            var request = CreateMockRestRequest(HttpMethod.Get);

            // act & assert
            Assert.ThrowsAsync<JsonReaderException>(
                () => restRequester.GetAsync<NullDataResponse>(request, CancellationToken.None));
        }

        private static HttpClient CreateHttpClientWithSequentialResponses(params HttpResponseMessage[] responses)
        {
            var callCount = 0;
            var mockHandler = new Mock<HttpMessageHandler>();
            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync( () => responses[callCount++]);
            return new HttpClient(mockHandler.Object);
        }

        private static IRestRequest CreateMockRestRequest(HttpMethod method = null)
        {
            method ??= HttpMethod.Get;
            var mock = new Mock<IRestRequest>();
            mock.Setup(r => r.ToRequestMessage(It.IsAny<HttpMethod>())).Returns(() =>
            {
                var message = new HttpRequestMessage(method, "https://test.snowflakecomputing.com/session");
#pragma warning disable CS0618
                message.Properties[BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY] = TimeSpan.FromSeconds(16);
                message.Properties[BaseRestRequest.REST_REQUEST_TIMEOUT_KEY] = TimeSpan.FromSeconds(120);
#pragma warning restore CS0618
                return message;
            });
            mock.Setup(r => r.GetRestTimeout()).Returns(TimeSpan.FromSeconds(120));
            mock.Setup(r => r.getSid()).Returns("test-sid");
            return mock.Object;
        }
    }
}
