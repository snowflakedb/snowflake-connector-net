using System.Net.Http;
using Xunit;
using Snowflake.Data.Core;
using RichardSzalay.MockHttp;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System;
using System.Security;
using System.Security.Authentication;
using Moq;
using Moq.Protected;
using Snowflake.Data.Client;
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
        [InlineData(HttpStatusCode.Ambiguous, false, true)]
        [InlineData(HttpStatusCode.Found, false, true)]
        [InlineData(HttpStatusCode.Moved, false, true)]
        [InlineData(HttpStatusCode.SeeOther, false, true)]
        public async Task TestIsRetryableHTTPCode(HttpStatusCode statusCode, bool forceRetryOn404, bool expectedIsRetryable)
        {
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When("https://test.snowflakecomputing.com")
            .Respond(statusCode);
            var client = mockHttp.ToHttpClient();
            var response = await client.GetAsync("https://test.snowflakecomputing.com").ConfigureAwait(false);

            bool actualIsRetryable = HttpUtil.IsRetryableHTTPCode(response.StatusCode, forceRetryOn404);

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
        public void TestCreateHttpClientHandlerDisablesAutoRedirect()
        {
            // arrange
            var config = new HttpClientConfig(null, null, null, null, null, false, false, 7, 20);

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.False(handler.AllowAutoRedirect);
        }

        [SFFact]
        public void TestFallbackHandlerDisablesAutoRedirect()
        {
            // arrange
            var config = CreateConfigWithTlsProtocols(null, null);

            // act
            var handler = HttpUtil.Instance.CreateFallbackHttpClientHandler(
                config, new PlatformNotSupportedException(), canApplyTlsProtocols: false);

            // assert
            Assert.False(handler.AllowAutoRedirect);
        }

        [SFTheory]
        // Same-origin HTTPS -> true
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "/temp-redirect-1", true)]
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "https://account.snowflakecomputing.com/temp-redirect-1", true)]
        [InlineData("https://account.snowflakecomputing.com:443/queries/v1/query-request", "https://account.snowflakecomputing.com:443/temp-redirect-1", true)]
        // Same-origin HTTP -> true
        [InlineData("http://localhost:8080/queries/v1/query-request", "/temp-redirect-1", true)]
        [InlineData("http://localhost:8080/queries/v1/query-request", "http://localhost:8080/temp-redirect-1", true)]
        // Cross-origin host mismatch -> false
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "https://attacker.com/steal", false)]
        [InlineData("https://account.okta.com/api/v1/authn", "https://attacker.okta.com/api/v1/authn", false)]
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "//attacker.com/steal", false)]
        // HTTPS downgrade to HTTP -> false
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "http://account.snowflakecomputing.com/temp-redirect-1", false)]
        [InlineData("https://account.okta.com/api/v1/authn", "http://account.okta.com/api/v1/authn", false)]
        // Port mismatch -> false
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "https://account.snowflakecomputing.com:8443/temp-redirect-1", false)]
        [InlineData("http://localhost:8080/queries/v1/query-request", "http://localhost:9090/temp-redirect-1", false)]
        // Non-http schemes -> false
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "ftp://account.snowflakecomputing.com/temp-redirect-1", false)]
        [InlineData("https://account.snowflakecomputing.com/queries/v1/query-request", "javascript:alert(1)", false)]
        public void TestIsSafeRedirect(string requestUrl, string locationUrl, bool expectedIsSafe)
        {
            // arrange
            var requestUri = new Uri(requestUrl);
            var locationUri = new Uri(locationUrl, UriKind.RelativeOrAbsolute);

            // act
            bool actualIsSafe = HttpUtil.IsSafeRedirect(requestUri, locationUri);

            // assert
            Assert.Equal(expectedIsSafe, actualIsSafe);
        }

        [SFFact]
        public void TestIsSafeRedirectWithNullOrRelativeRequest()
        {
            var requestUri = new Uri("https://account.snowflakecomputing.com/queries/v1/query-request");
            Assert.False(HttpUtil.IsSafeRedirect(null, new Uri("/temp", UriKind.Relative)));
            Assert.False(HttpUtil.IsSafeRedirect(requestUri, null));
            Assert.False(HttpUtil.IsSafeRedirect(null, null));
            Assert.False(HttpUtil.IsSafeRedirect(new Uri("/relative/path", UriKind.Relative), new Uri("/temp", UriKind.Relative)));
        }

        [SFTheory]
        [InlineData("https://attacker.com/steal")]
        [InlineData("http://test.snowflakecomputing.com/temp-redirect-1")]
        [InlineData("https://test.snowflakecomputing.com:8443/temp-redirect-1")]
        public async Task TestRetryHandlerDoesNotFollowUnsafeRedirect(string redirectTarget)
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri(redirectTarget, UriKind.RelativeOrAbsolute);
                    return response;
                });
            var unsafeTargetRequest = mockHttp.When(redirectTarget)
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act & assert: unsafe redirect throws SecurityException, target never contacted
            await Assert.ThrowsAsync<SecurityException>(() => client.SendAsync(request)).ConfigureAwait(false);
            Assert.Equal(0, mockHttp.GetMatchCount(unsafeTargetRequest));
        }

        [SFFact]
        public async Task TestRetryHandlerDoesNotFollowProtocolRelativeRedirect()
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri("//attacker.com/steal", UriKind.RelativeOrAbsolute);
                    return response;
                });
            var attackerRequest = mockHttp.When("https://attacker.com/*")
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act & assert: protocol-relative redirect to different origin throws SecurityException
            await Assert.ThrowsAsync<SecurityException>(() => client.SendAsync(request)).ConfigureAwait(false);
            Assert.Equal(0, mockHttp.GetMatchCount(attackerRequest));
        }

        [SFFact]
        public async Task TestRetryHandlerFollowsSafeSameOriginRedirect()
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri("/temp-redirect-1", UriKind.Relative);
                    return response;
                });
            var redirectedRequest = mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/temp-redirect-1")
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act
            var response = await client.SendAsync(request).ConfigureAwait(false);

            // assert: safe relative redirect is followed to same origin
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(1, mockHttp.GetMatchCount(redirectedRequest));
        }

        [SFFact]
        public async Task TestRetryHandlerFollows300WithSameOriginLocation()
        {
            // arrange
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.Ambiguous); // 300
                    response.Headers.Location = new Uri("/alternate", UriKind.Relative);
                    return response;
                });
            var redirectedRequest = mockHttp.When(HttpMethod.Get, "https://test.snowflakecomputing.com/alternate")
                .Respond(HttpStatusCode.OK);

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act
            var response = await client.SendAsync(request).ConfigureAwait(false);

            // assert: 300 with a same-origin Location is followed and method is downgraded to GET
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(1, mockHttp.GetMatchCount(redirectedRequest));
        }

        [SFFact]
        public async Task TestRetryHandlerReturns300WithoutLocation()
        {
            // arrange - 300 without a Location header cannot be followed as a redirect
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request")
                .Respond(_ => new HttpResponseMessage(HttpStatusCode.Ambiguous)); // 300, no Location

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/queries/v1/query-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act & assert: 300 without Location — IsSafeRedirect(_, null) is false, throws SecurityException
            await Assert.ThrowsAsync<SecurityException>(() => client.SendAsync(request)).ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestRetryHandlerFollowsSamePathDifferentQueryRedirect()
        {
            // arrange - a 307 redirect to the same path with different query parameters is
            // followed, matching .NET's native HttpClientHandler auto-redirect behavior
            var callCount = 0;
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/session/v1/login-request*")
                .Respond(_ =>
                {
                    if (++callCount != 1)
                        return new HttpResponseMessage(HttpStatusCode.OK);

                    var redirect = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    redirect.Headers.Location = new Uri(
                        "https://test.snowflakecomputing.com/session/v1/login-request?token=abc",
                        UriKind.Absolute);
                    return redirect;
                });

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/session/v1/login-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act
            var response = await client.SendAsync(request).ConfigureAwait(false);

            // assert: same-path redirect with different query is followed
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(2, callCount); // first call -> 307, second call -> 200
        }

        [SFFact]
        public async Task TestRetryHandlerThrowsOnRedirectHopLimitExceeded()
        {
            // arrange - server keeps redirecting to a new same-origin path on every hop
            var hopCount = 0;
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/*")
                .Respond(_ =>
                {
                    hopCount++;
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri($"/hop-{hopCount}", UriKind.Relative);
                    return response;
                });

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 0, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/session/v1/login-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(60));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(300));

            // act & assert: redirect loop is bounded by hop counter, not just by timeout
            var ex = await Assert.ThrowsAsync<SecurityException>(
                () => client.SendAsync(request)).ConfigureAwait(false);
            Assert.Contains("Redirect loop detected", ex.Message);
        }

        [SFFact]
        public async Task TestRetryHandlerResetsRedirectHopCounterAfterRetryableError()
        {
            // arrange - the consecutive redirect hop counter resets when a non-redirect retryable
            // response (e.g. 503) interrupts the redirect chain; without the reset a long-lived
            // request that alternates between redirects and retryable errors would falsely trip
            // the hop limit even though no single chain was longer than MaxRedirectsCount
            var callCount = 0;
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/*")
                .Respond(_ =>
                {
                    if (++callCount == 5)
                        return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);

                    if (callCount >= 10)
                        return new HttpResponseMessage(HttpStatusCode.OK);

                    var r = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    r.Headers.Location = new Uri($"/chain1-hop{callCount}", UriKind.Relative);
                    return r;
                });

            var config = new HttpClientConfig(null, null, null, null, null, false, false, 1, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/session/v1/login-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(30));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act
            var response = await client.SendAsync(request).ConfigureAwait(false);

            // assert: 8 total redirects across two chains succeed because neither chain exceeded
            // the hop limit on its own — the 503 in between reset the counter
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        [SFFact]
        public async Task TestRetryHandlerFollowsRedirectEvenWhenRetryIsDisabled()
        {
            // arrange - disableRetry=true must not prevent following safe redirects;
            // the old condition (isRetrying && !isRetryable || disableRetry) had a precedence bug
            // that treated disableRetry=true as "stop on any non-success", including redirects
            var mockHttp = new MockHttpMessageHandler();
            mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/session/v1/login-request")
                .Respond(_ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.TemporaryRedirect);
                    response.Headers.Location = new Uri("/redirected-login", UriKind.Relative);
                    return response;
                });
            var redirectedRequest = mockHttp.When(HttpMethod.Post, "https://test.snowflakecomputing.com/redirected-login")
                .Respond(HttpStatusCode.OK);

            //                                                         disableRetry=true ↓
            var config = new HttpClientConfig(null, null, null, null, null, true, false, 3, 20);
            var client = HttpUtil.Instance.CreateNewHttpClient(config, new CustomDelegatingHandler(mockHttp));
            var request = new HttpRequestMessage(HttpMethod.Post, "https://test.snowflakecomputing.com/session/v1/login-request");
            request.SetOption(BaseRestRequest.HTTP_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(16));
            request.SetOption(BaseRestRequest.REST_REQUEST_TIMEOUT_KEY, TimeSpan.FromSeconds(120));

            // act
            var response = await client.SendAsync(request).ConfigureAwait(false);

            // assert: redirect is followed despite retry being disabled
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(1, mockHttp.GetMatchCount(redirectedRequest));
        }

        [SFTheory]
        [InlineData("tls12", "tls13", SslProtocols.Tls12 | SslProtocolsExtensions.Tls13)]
        [InlineData("tls12", "tls12", SslProtocols.Tls12)]
        [InlineData("tls13", "tls13", SslProtocolsExtensions.Tls13)]
        public void TestRequestedTlsProtocolsAreAppliedOnHandler(string minTls, string maxTls, SslProtocols expectedProtocols)
        {
            // arrange
            Skip.When(!CanRuntimeApplyTlsProtocols(), TlsProtocolsUnsupportedRationale);
            var config = CreateConfigWithTlsProtocols(minTls, maxTls);

            // act
            var handler = (HttpClientHandler)HttpUtil.Instance.SetupCustomHttpHandler(config);

            // assert
            Assert.Equal(expectedProtocols, handler.SslProtocols);
        }

        [SFFact]
        public void TestFallbackHandlerKeepsRequestedTlsProtocols()
        {
            // arrange
            Skip.When(!CanRuntimeApplyTlsProtocols(), TlsProtocolsUnsupportedRationale);
            var config = CreateConfigWithTlsProtocols("tls13", "tls13");

            // act - a runtime rejecting only CheckCertificateRevocationList still applies TLS protocols
            var handler = HttpUtil.Instance.CreateFallbackHttpClientHandler(
                config, new PlatformNotSupportedException(), canApplyTlsProtocols: true);

            // assert
            Assert.Equal(SslProtocolsExtensions.Tls13, handler.SslProtocols);
        }

        [SFTheory]
        [InlineData("tls13", "tls13", "MINTLS=TLS13, MAXTLS=TLS13")]
        [InlineData("tls12", "tls13", "MINTLS=TLS12, MAXTLS=TLS13")]
        public void TestFallbackHandlerFailsWhenTlsProtocolsCannotBeApplied(string minTls, string maxTls, string expectedProtocolsInMessage)
        {
            // arrange
            var config = CreateConfigWithTlsProtocols(minTls, maxTls);
            var cause = new PlatformNotSupportedException();

            // act
            var exception = Assert.Throws<SnowflakeDbException>(() => HttpUtil.Instance.CreateFallbackHttpClientHandler(
                config, cause, canApplyTlsProtocols: false));

            // assert - no silent fallback to the protocols chosen by the OS
            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.TLS_CONFIGURATION_NOT_SUPPORTED);
            Assert.Contains(expectedProtocolsInMessage, exception.Message);
            Assert.Same(cause, exception.InnerException);
        }

        [SFFact]
        public void TestFallbackHandlerAcceptsDefaultedTlsProtocolsThatCannotBeApplied()
        {
            // arrange - MINTLS/MAXTLS always carry defaults, so a connection that never asked for a
            // TLS restriction must keep working on runtimes that cannot apply one
            var config = CreateConfigWithTlsProtocols(null, null);

            // act
            var handler = HttpUtil.Instance.CreateFallbackHttpClientHandler(
                config, new PlatformNotSupportedException(), canApplyTlsProtocols: false);

            // assert - protocol selection is left to the OS instead of failing
            Assert.NotNull(handler);
        }

        [SFFact]
        public void TestTlsConfigurationErrorIsRecognizedSoItIsNeverSwallowed()
        {
            // arrange - callers that fall back on failure (e.g. the bind stage upload) must let this
            // one through, otherwise the requested TLS restriction is silently dropped
            var config = CreateConfigWithTlsProtocols("tls13", "tls13");
            var tlsException = Assert.Throws<SnowflakeDbException>(() => HttpUtil.Instance.CreateFallbackHttpClientHandler(
                config, new PlatformNotSupportedException(), canApplyTlsProtocols: false));

            // act, assert
            Assert.True(tlsException.IsTlsConfigurationNotSupported());
            Assert.False(new PlatformNotSupportedException().IsTlsConfigurationNotSupported());
            Assert.False(new SnowflakeDbException(SFError.SESSION_GONE).IsTlsConfigurationNotSupported());
        }

        [SFFact]
        public void TestMinAndMaxTlsProtocolsAreParsedIndependently()
        {
            // arrange, act
            var config = CreateConfigWithTlsProtocols(null, "tls13");

            // assert - a missing minimum falls back to its default without discarding the requested
            // maximum, and asking for either counts as an explicit request
            Assert.Equal(SslProtocols.Tls12, config.MinTlsProtocol);
            Assert.Equal(SslProtocolsExtensions.Tls13, config.MaxTlsProtocol);
            Assert.True(config.TlsProtocolsExplicitlyRequested);
        }

        private const string TlsProtocolsUnsupportedRationale =
            "This runtime does not support setting TLS protocols on HttpClientHandler (.NET Framework 4.6.2 and 4.7.1).";

        private static bool CanRuntimeApplyTlsProtocols()
        {
            try
            {
                using var handler = new HttpClientHandler();
                handler.SslProtocols = SslProtocols.Tls12 | SslProtocolsExtensions.Tls13;
                return true;
            }
            catch (PlatformNotSupportedException)
            {
                return false;
            }
        }

        // Null min/max mean the connection string carried neither, i.e. only the defaults apply.
        private static HttpClientConfig CreateConfigWithTlsProtocols(string minTls, string maxTls) =>
            new HttpClientConfig(
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                7,
                20,
                minTlsProtocol: minTls,
                maxTlsProtocol: maxTls,
                tlsProtocolsExplicitlyRequested: minTls != null || maxTls != null
            );

#pragma warning disable SYSLIB0014
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
#if !NET9_0_OR_GREATER

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
#endif
        [SFFact]
        public void TestRejectsCustomHandlerWithAutoRedirect()
        {
            var innerHandler = new HttpClientHandler { AllowAutoRedirect = true };
            var customHandler = new CustomDelegatingHandler(innerHandler);
            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);

            var ex = Assert.Throws<SecurityException>(() =>
                HttpUtil.Instance.CreateNewHttpClient(config, customHandler));

            Assert.Contains("AllowAutoRedirect", ex.Message);
        }

        [SFFact]
        public void TestAcceptsCustomHandlerWithAutoRedirectDisabled()
        {
            var innerHandler = new HttpClientHandler { AllowAutoRedirect = false };
            var customHandler = new CustomDelegatingHandler(innerHandler);
            var config = new HttpClientConfig(null, null, null, null, null, false, false, 3, 20);

            using var client = HttpUtil.Instance.CreateNewHttpClient(config, customHandler);

            Assert.NotNull(client);
        }
    }
#pragma warning restore SYSLIB0014
}
