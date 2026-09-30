using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [CollectionDefinition(nameof(SFExternalBrowserTestCollection), DisableParallelization = true)]
    public sealed class SFExternalBrowserTestCollection : ICollectionFixture<SFExternalBrowserTest>
    {

    }

    [Collection(nameof(SFExternalBrowserTestCollection))]
    public sealed class SFExternalBrowserTest
    {

        private readonly Mock<IWebBrowserRunner> t_browserRunner;

        private static readonly HttpClient s_httpClient = new();

        public SFExternalBrowserTest()
        {
            t_browserRunner = new Mock<IWebBrowserRunner>();
        }

        [SFFact]
        public void TestDefaultAuthentication()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });
            var localhostRegex = new Regex("http:\\/\\/localhost:(.*)\\/?token=mockToken");
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var user = $"test{Guid.NewGuid():N}";
            var sfSession = new SFSession($"account=test;user={user};password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            sfSession.Open();

            Assert.True(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => localhostRegex.IsMatch(s.ToString()))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestConsoleLogin()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    var port = HttpUtility.ParseQueryString(uri.Query).Get("browser_mode_redirect_port");
                    var browserUrl = $"http://localhost:{port}/?token=mockToken";
                    s_httpClient.GetAsync(browserUrl);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var user = $"test{Guid.NewGuid():N}";
            var sfSession = new SFSession($"disable_console_login=false;account=test;user={user};password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            sfSession.Open();
            Assert.False(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => s.ToString().Contains("https://test.snowflakecomputing.com/console/login?"))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [SFFact]
        public void TestSSOToken()
        {
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, "mockIdToken");
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "https://www.mockSSOUrl.com"
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            sfSession.Open();

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Never);
        }

        [SFFact]
        public void TestThatTokenIsStoredWhenCacheIsEnabled()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });

            var expectedIdToken = "mockIdToken";
            var user = "testUser";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            sfSession.Open();

            Assert.Equal(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public void TestThatTokenIsNotStoredWhenCacheIsDisabled()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });

            var expectedIdToken = "mockIdToken";
            var user = "testUser";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=false;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            sfSession.Open();

            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public void TestThatRetriesAuthenticationForInvalidIdToken()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });

            var invalidIdToken = "invalidIdToken";
            var expectedIdToken = "mockIdToken";
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, invalidIdToken);
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken,
                ThrowInvalidIdToken = true
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.Equal(invalidIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            sfSession.Open();

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Once);
            Assert.Equal(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public void TestThatDoesNotRetryAuthenticationForNonInvalidIdTokenException()
        {
            var expectedIdToken = "validIdToken";
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, expectedIdToken);
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ThrowNonInvalidIdToken = true
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());

            Assert.Equal(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Equal(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public void TestThatThrowsTimeoutErrorWhenNoBrowserResponse()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback(async (Uri uri) =>
                {
                    await Task.Delay(1000).ContinueWith(_ =>
                    {
                        s_httpClient.GetAsync(uri.ToString());
                    }).ConfigureAwait(false);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=false;browser_response_timeout=0;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.Equal(SFError.BROWSER_RESPONSE_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact]
        public void TestThatThrowsErrorWhenUrlDoesNotMatchRegex()
        {
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "non-matching-regex.com"
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.Equal(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact]
        public void TestThatThrowsErrorWhenUrlIsNotWellFormedUriString()
        {
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "http://localhost:123/?token=mockToken\\\\"
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.Equal(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [SFFact]
        public async Task TestThatMatchingOriginPostWithJsonTokenCompletesBrowserListener()
        {
            Task browserRequests = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequests = Task.Run(async () =>
                    {
                        using (var emptyMatchingPost = CreatePostRequest(uri, "https://test.snowflakecomputing.com"))
                        using (var emptyMatchingResponse = await s_httpClient.SendAsync(emptyMatchingPost).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, emptyMatchingResponse.StatusCode);
                        }

                        using (var foreignOriginPost = CreateJsonPostRequest(uri, "https://other.snowflakecomputing.com", "{\"token\":\"ignored\"}"))
                        using (var foreignOriginResponse = await s_httpClient.SendAsync(foreignOriginPost).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.Forbidden, foreignOriginResponse.StatusCode);
                        }

                        using (var matchingOriginPost = CreateJsonPostRequest(uri, "https://test.snowflakecomputing.com", "{\"token\":\"post-json-token\",\"consent\":true}"))
                        using (var matchingOriginResponse = await s_httpClient.SendAsync(matchingOriginPost).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, matchingOriginResponse.StatusCode);
                            Assert.Equal("https://test.snowflakecomputing.com", matchingOriginResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequests.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestThatForeignOriginDoesNotCompleteBrowserListener()
        {
            Task browserRequests = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequests = Task.Run(async () =>
                    {
                        using (var foreignRequest = new HttpRequestMessage(HttpMethod.Get, uri))
                        {
                            foreignRequest.Headers.TryAddWithoutValidation("Origin", "https://other.snowflakecomputing.com");
                            using (var foreignResponse = await s_httpClient.SendAsync(foreignRequest).ConfigureAwait(false))
                            {
                                Assert.Equal(System.Net.HttpStatusCode.Forbidden, foreignResponse.StatusCode);
                                Assert.False(foreignResponse.Headers.Contains("Access-Control-Allow-Origin"));
                            }
                        }

                        using (var validResponse = await s_httpClient.GetAsync(uri).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, validResponse.StatusCode);
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequests.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestThatNullOriginGetIsAcceptedWithoutCorsHeaders()
        {
            Task browserRequest = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequest = Task.Run(async () =>
                    {
                        using (var request = new HttpRequestMessage(HttpMethod.Get, uri))
                        {
                            request.Headers.TryAddWithoutValidation("Origin", "null");
                            using (var response = await s_httpClient.SendAsync(request).ConfigureAwait(false))
                            {
                                Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
                                Assert.False(response.Headers.Contains("Access-Control-Allow-Origin"));
                            }
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequest.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestThatMatchingOriginGetIsAcceptedWithCorsHeader()
        {
            Task browserRequest = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequest = Task.Run(async () =>
                    {
                        using (var request = new HttpRequestMessage(HttpMethod.Get, uri))
                        {
                            request.Headers.TryAddWithoutValidation("Origin", "https://test.snowflakecomputing.com");
                            using (var response = await s_httpClient.SendAsync(request).ConfigureAwait(false))
                            {
                                Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);
                                Assert.Equal("https://test.snowflakecomputing.com", response.Headers.GetValues("Access-Control-Allow-Origin").Single());
                            }
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequest.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestThatValidPostPreflightUsesRequestedHeaders()
        {
            Task browserRequests = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequests = Task.Run(async () =>
                    {
                        using (var foreignPreflight = CreatePreflightRequest(uri, "https://other.snowflakecomputing.com", "POST", "Content-Type"))
                        using (var foreignResponse = await s_httpClient.SendAsync(foreignPreflight).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.BadRequest, foreignResponse.StatusCode);
                            Assert.False(foreignResponse.Headers.Contains("Access-Control-Allow-Origin"));
                        }

                        using (var wrongMethodPreflight = CreatePreflightRequest(uri, "https://test.snowflakecomputing.com", "GET", "Content-Type"))
                        using (var wrongMethodResponse = await s_httpClient.SendAsync(wrongMethodPreflight).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.BadRequest, wrongMethodResponse.StatusCode);
                        }

                        using (var wrongHeadersPreflight = CreatePreflightRequest(uri, "https://test.snowflakecomputing.com", "POST", "X-Test"))
                        using (var wrongHeadersResponse = await s_httpClient.SendAsync(wrongHeadersPreflight).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.BadRequest, wrongHeadersResponse.StatusCode);
                        }

                        using (var validPreflight = CreatePreflightRequest(uri, "https://test.snowflakecomputing.com", "post", "content-type"))
                        using (var validPreflightResponse = await s_httpClient.SendAsync(validPreflight).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.NoContent, validPreflightResponse.StatusCode);
                            Assert.Equal("https://test.snowflakecomputing.com", validPreflightResponse.Headers.GetValues("Access-Control-Allow-Origin").Single());
                            Assert.Equal("POST, GET, OPTIONS", validPreflightResponse.Headers.GetValues("Access-Control-Allow-Methods").Single());
                            Assert.Equal("content-type", validPreflightResponse.Headers.GetValues("Access-Control-Allow-Headers").Single());
                        }

                        using (var validResponse = await s_httpClient.GetAsync(uri).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, validResponse.StatusCode);
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequests.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestThatOriginlessPostDoesNotCompleteBrowserListener()
        {
            Task browserRequests = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequests = Task.Run(async () =>
                    {
                        using (var noOriginPost = CreateJsonPostRequest(uri, null, "{\"token\":\"should-be-ignored\"}"))
                        using (var noOriginResponse = await s_httpClient.SendAsync(noOriginPost).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.Forbidden, noOriginResponse.StatusCode);
                        }

                        using (var nullOriginPost = CreateJsonPostRequest(uri, "null", "{\"token\":\"also-ignored\"}"))
                        using (var nullOriginResponse = await s_httpClient.SendAsync(nullOriginPost).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.Forbidden, nullOriginResponse.StatusCode);
                        }

                        using (var validGetResponse = await s_httpClient.GetAsync(uri).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, validGetResponse.StatusCode);
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequests.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestThatTokenlessGetDoesNotCompleteBrowserListener()
        {
            Task browserRequests = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequests = Task.Run(async () =>
                    {
                        var callbackRoot = uri.GetLeftPart(UriPartial.Authority) + "/";
                        using (var originlessRoot = await s_httpClient.GetAsync(callbackRoot).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, originlessRoot.StatusCode);
                        }

                        using (var favicon = await s_httpClient.GetAsync(callbackRoot + "favicon.ico").ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, favicon.StatusCode);
                        }

                        using (var validResponse = await s_httpClient.GetAsync(uri).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, validResponse.StatusCode);
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequests.ConfigureAwait(false);
        }

        [SFFact]
        public async Task TestDefaultAuthenticationAsync()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });
            var localhostRegex = new Regex("http:\\/\\/localhost:(.*)\\/?token=mockToken");
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var user = $"test{Guid.NewGuid():N}";
            var sfSession = new SFSession($"account=test;user={user};password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.True(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => localhostRegex.IsMatch(s.ToString()))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [SFFact]
        public async Task TestConsoleLoginAsync()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    var port = HttpUtility.ParseQueryString(uri.Query).Get("browser_mode_redirect_port");
                    var browserUrl = $"http://localhost:{port}/?token=mockToken";
                    s_httpClient.GetAsync(browserUrl);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var user = $"test{Guid.NewGuid():N}";
            var sfSession = new SFSession($"disable_console_login=false;account=test;user={user};password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.False(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => s.ToString().Contains("https://test.snowflakecomputing.com/console/login?"))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [SFFact]
        public async Task TestSSOTokenAsync()
        {
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, "mockIdToken");
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "https://www.mockSSOUrl.com"
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Never());
        }

        [SFFact]
        public async Task TestThatTokenIsStoredWhenCacheIsEnabledAsync()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });

            var expectedIdToken = "mockIdToken";
            var user = "testUser";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public async Task TestThatTokenIsNotStoredWhenCacheIsDisabledAsync()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });

            var expectedIdToken = "mockIdToken";
            var user = "testUser";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=false;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public async Task TestThatRetriesAuthenticationForInvalidIdTokenAsync()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.GetAsync(uri.ToString());
                });

            var invalidIdToken = "invalidIdToken";
            var expectedIdToken = "mockIdToken";
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, invalidIdToken);
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken,
                ThrowInvalidIdToken = true
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SFExternalBrowserTest.SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.Equal(invalidIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            await sfSession.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Once);
            Assert.Equal(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public async Task TestThatDoesNotRetryAuthenticationForNonInvalidIdTokenExceptionAsync()
        {
            var expectedIdToken = "validIdToken";
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.BuildCacheKey(new CacheKeyInput(TokenType.IdToken, host, host, user, string.Empty));
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, expectedIdToken);
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ThrowNonInvalidIdToken = true
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => sfSession.OpenAsync(CancellationToken.None)).ConfigureAwait(false);

            Assert.Equal(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Equal(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [SFFact]
        public async Task TestThatUnsupportedMethodDoesNotCompleteBrowserListener()
        {
            Task browserRequests = null;
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    browserRequests = Task.Run(async () =>
                    {
                        using (var deleteRequest = new HttpRequestMessage(HttpMethod.Delete, uri))
                        using (var deleteResponse = await s_httpClient.SendAsync(deleteRequest).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.MethodNotAllowed, deleteResponse.StatusCode);
                        }

                        using (var validResponse = await s_httpClient.GetAsync(uri).ConfigureAwait(false))
                        {
                            Assert.Equal(System.Net.HttpStatusCode.OK, validResponse.StatusCode);
                        }
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            sfSession.Open();
            await browserRequests.ConfigureAwait(false);
        }

        private static void SetAuthenticatorWithMockBrowser(SFSession session, IWebBrowserRunner browserRunner)
        {
            var authenticator = new ExternalBrowserAuthenticator(session, browserRunner);
            session.authenticator = authenticator;
        }

        private static HttpRequestMessage CreatePostRequest(Uri uri, string origin)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, uri)
            {
                Content = new StringContent("")
            };
            if (origin != null)
            {
                request.Headers.TryAddWithoutValidation("Origin", origin);
            }
            return request;
        }

        private static HttpRequestMessage CreateJsonPostRequest(Uri uri, string origin, string json)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, uri.GetLeftPart(UriPartial.Authority) + "/")
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
            if (origin != null)
            {
                request.Headers.TryAddWithoutValidation("Origin", origin);
            }
            return request;
        }

        private static HttpRequestMessage CreatePreflightRequest(Uri uri, string origin, string method, string headers)
        {
            var request = new HttpRequestMessage(HttpMethod.Options, uri);
            request.Headers.TryAddWithoutValidation("Origin", origin);
            request.Headers.TryAddWithoutValidation("Access-Control-Request-Method", method);
            request.Headers.TryAddWithoutValidation("Access-Control-Request-Headers", headers);
            return request;
        }
    }

    [Collection(nameof(SFExternalBrowserTestCollection))]
    public sealed class WebBrowserListenerControlFlowTest
    {
        private static readonly HttpClient s_httpClient = new();

        [SFFact]
        public void TestThatRequestHandlerExceptionPropagatesFromBrowserListener()
        {
            var endpoints = new[] { $"http://{System.Net.IPAddress.Loopback}:{GetFreePort()}/" };
            var httpListener = new HttpListener();
            foreach (var ep in endpoints) httpListener.Prefixes.Add(ep);
            httpListener.Start();
            var listenerUri = new Uri(endpoints[0]);

            var boom = new InvalidOperationException("handler exploded");
            Func<HttpListenerContext, Result<string, IBrowserError>?> handler = _ => throw boom;

            using var listener = new WebBrowserListener<string>(
                httpListener, handler, "<ok/>", "<err/>");

            Task.Run(() => s_httpClient.GetAsync(listenerUri.ToString()));

            var thrown = Assert.Throws<InvalidOperationException>(
                () => listener.WaitAndGetResult(TimeSpan.FromSeconds(10)));
            Assert.Same(boom, thrown);
        }

        [SFFact]
        public void TestThatGracefulShutdownDuringContinueListeningDoesNotThrow()
        {
            var endpoints = new[] { $"http://{System.Net.IPAddress.Loopback}:{GetFreePort()}/" };
            var httpListener = new HttpListener();
            foreach (var ep in endpoints) httpListener.Prefixes.Add(ep);
            httpListener.Start();
            var listenerUri = new Uri(endpoints[0] + "?token=tok");

            int requestCount = 0;
            Func<HttpListenerContext, Result<string, IBrowserError>?> handler = ctx =>
            {
                if (System.Threading.Interlocked.Increment(ref requestCount) == 1)
                {
                    ctx.Response.StatusCode = 200;
                    ctx.Response.ContentLength64 = 0;
                    ctx.Response.Close();
                    return null;
                }
                return Result<string, IBrowserError>.CreateResult("tok");
            };

            using var listener = new WebBrowserListener<string>(
                httpListener, handler, "<ok/>", "<err/>");

            Task.Run(async () =>
            {
                await s_httpClient.GetAsync(listenerUri.ToString()).ConfigureAwait(false);
                await s_httpClient.GetAsync(listenerUri.ToString()).ConfigureAwait(false);
            });

            var result = listener.WaitAndGetResult(TimeSpan.FromSeconds(10));
            Assert.Equal("tok", result);
        }

        private static int GetFreePort()
        {
            var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            listener.Start();
            int port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
