using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using System;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFExternalBrowserTest
    {
        [ThreadStatic]
        private static Mock<IWebBrowserRunner> t_browserRunner;

        private static HttpClient s_httpClient = new HttpClient();

        [SetUp]
        public void BeforeEach()
        {
            t_browserRunner = new Mock<IWebBrowserRunner>();
        }

        [Test]
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
            var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            sfSession.Open();

            Assert.IsTrue(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => localhostRegex.IsMatch(s.ToString()))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [Test]
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
            var sfSession = new SFSession("disable_console_login=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            sfSession.Open();
            Assert.IsFalse(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => s.ToString().Contains("https://test.snowflakecomputing.com/console/login?"))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [Test]
        public void TestSSOToken()
        {
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
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

        [Test]
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
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.AreEqual(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            sfSession.Open();

            Assert.AreEqual(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
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
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=false;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.AreEqual(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            sfSession.Open();

            Assert.AreEqual(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
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
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
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

            Assert.AreEqual(invalidIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            sfSession.Open();

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Once);
            Assert.AreEqual(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
        public void TestThatDoesNotRetryAuthenticationForNonInvalidIdTokenException()
        {
            var expectedIdToken = "validIdToken";
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, expectedIdToken);
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ThrowNonInvalidIdToken = true
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());

            Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.AreEqual(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
        public void TestThatThrowsTimeoutErrorWhenNoBrowserResponse()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback(async (Uri uri) =>
                {
                    await Task.Delay(1000).ContinueWith(_ =>
                    {
                        s_httpClient.GetAsync(uri.ToString());
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=false;browser_response_timeout=0;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.BROWSER_RESPONSE_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestThatThrowsErrorWhenUrlDoesNotMatchRegex()
        {
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "non-matching-regex.com"
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestThatThrowsErrorWhenUrlIsNotWellFormedUriString()
        {
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "http://localhost:123/?token=mockToken\\\\"
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestThatThrowsErrorWhenBrowserRequestMethodIsNotGet()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    s_httpClient.PostAsync(uri.ToString(), new StringContent(""));
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.BROWSER_RESPONSE_WRONG_METHOD.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestThatThrowsErrorWhenBrowserRequestHasInvalidQuery()
        {
            t_browserRunner
                .Setup(b => b.Run(It.IsAny<Uri>()))
                .Callback((Uri uri) =>
                {
                    var urlWithoutQuery = uri.ToString().Substring(0, uri.ToString().IndexOf("?token="));
                    s_httpClient.GetAsync(urlWithoutQuery);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.BROWSER_RESPONSE_INVALID_PREFIX.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestDefaultAuthenticationAsync()
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
            var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            Assert.IsTrue(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => localhostRegex.IsMatch(s.ToString()))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [Test]
        public void TestConsoleLoginAsync()
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
            var sfSession = new SFSession("disable_console_login=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.snowflakecomputing.com", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);
            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            Assert.IsFalse(sfSession._disableConsoleLogin);
            t_browserRunner.Verify(b => b.Run(It.Is<Uri>(s => s.ToString().Contains("https://test.snowflakecomputing.com/console/login?"))), Times.Once());
            t_browserRunner.VerifyNoOtherCalls();
        }

        [Test]
        public void TestSSOTokenAsync()
        {
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, "mockIdToken");
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                SSOUrl = "https://www.mockSSOUrl.com"
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Never());
        }

        [Test]
        public void TestThatTokenIsStoredWhenCacheIsEnabledAsync()
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
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.AreEqual(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            Assert.AreEqual(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
        public void TestThatTokenIsNotStoredWhenCacheIsDisabledAsync()
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
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(key);
            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
                IdToken = expectedIdToken
            };
            var sfSession = new SFSession($"client_store_temporary_credential=false;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            SetAuthenticatorWithMockBrowser(sfSession, t_browserRunner.Object);

            Assert.AreEqual(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            Assert.AreEqual(string.Empty, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
        public void TestThatRetriesAuthenticationForInvalidIdTokenAsync()
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
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
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

            Assert.AreEqual(invalidIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));

            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            t_browserRunner.Verify(b => b.Run(It.IsAny<Uri>()), Times.Once);
            Assert.AreEqual(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        [Test]
        public void TestThatDoesNotRetryAuthenticationForNonInvalidIdTokenExceptionAsync()
        {
            var expectedIdToken = "validIdToken";
            var user = "test";
            var host = $"{user}.snowflakecomputing.com";
            var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
            var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
            credentialManager.SaveCredentials(key, expectedIdToken);
            SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ThrowNonInvalidIdToken = true
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", new SessionPropertiesContext(), restRequester);
            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            var thrown = Assert.Throws<AggregateException>(() => connectTask.Wait());

            Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode, ((SnowflakeDbException)thrown.InnerException).ErrorCode);
            Assert.AreEqual(expectedIdToken, SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(key));
        }

        private static void SetAuthenticatorWithMockBrowser(SFSession session, IWebBrowserRunner browserRunner)
        {
            var authenticator = new ExternalBrowserAuthenticator(session, browserRunner);
            session.authenticator = authenticator;
        }
    }
}
