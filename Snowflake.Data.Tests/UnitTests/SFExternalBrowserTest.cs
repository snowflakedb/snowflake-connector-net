using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Tools;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    class SFExternalBrowserTest
    {
        [ThreadStatic]
        private static Mock<BrowserOperations> t_browserOperations;

        private static HttpClient s_httpClient = new HttpClient();

        [SetUp]
        public void BeforeEach()
        {
            t_browserOperations = new Mock<BrowserOperations>();
        }

        [Test]
        public void TestDefaultAuthentication()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback((string url) => {
                    s_httpClient.GetAsync(url);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            sfSession.Open();

            Assert.IsTrue(sfSession._disableConsoleLogin);
            t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void TestConsoleLogin()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback((string url) => {
                    Uri uri = new Uri(url);
                    var port = HttpUtility.ParseQueryString(uri.Query).Get("browser_mode_redirect_port");
                    var browserUrl = $"http://localhost:{port}/?token=mockToken";
                    s_httpClient.GetAsync(browserUrl);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("disable_console_login=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            sfSession.Open();

            Assert.IsFalse(sfSession._disableConsoleLogin);
            t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void TestSSOToken()
        {
            try
            {
                var user = "test";
                var host = $"{user}.okta.com";
                var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
                var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
                credentialManager.SaveCredentials(key, "mockIdToken");
                SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                    SSOUrl = "https://www.mockSSOUrl.com"
                };
                var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", null, restRequester, t_browserOperations.Object);
                sfSession.Open();

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Never);
            }
            catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }

        [Test]
        public void TestThatThrowsTimeoutErrorWhenNoBrowserResponse()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback(async (string url) => {
                    await Task.Delay(1000).ContinueWith(_ =>
                    {
                        s_httpClient.GetAsync(url);
                    });
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=false;browser_response_timeout=0;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
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
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
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
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestThatThrowsErrorWhenBrowserRequestMethodIsNotGet()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback((string url) => {
                    s_httpClient.PostAsync(url, new StringContent(""));
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.BROWSER_RESPONSE_WRONG_METHOD.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestThatThrowsErrorWhenBrowserRequestHasInvalidQuery()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback((string url) => {
                    var urlWithoutQuery = url.Substring(0, url.IndexOf("?token="));
                    s_httpClient.GetAsync(urlWithoutQuery);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            var thrown = Assert.Throws<SnowflakeDbException>(() => sfSession.Open());
            Assert.AreEqual(SFError.BROWSER_RESPONSE_INVALID_PREFIX.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
        }

        [Test]
        public void TestDefaultAuthenticationAsync()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback((string url) => {
                    s_httpClient.GetAsync(url);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            Assert.IsTrue(sfSession._disableConsoleLogin);
            t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void TestConsoleLoginAsync()
        {
            t_browserOperations
                .Setup(b => b.OpenUrl(It.IsAny<string>()))
                .Callback((string url) => {
                    Uri uri = new Uri(url);
                    var port = HttpUtility.ParseQueryString(uri.Query).Get("browser_mode_redirect_port");
                    var browserUrl = $"http://localhost:{port}/?token=mockToken";
                    s_httpClient.GetAsync(browserUrl);
                });

            var restRequester = new Mock.MockExternalBrowserRestRequester()
            {
                ProofKey = "mockProofKey",
            };
            var sfSession = new SFSession("disable_console_login=false;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
            Task connectTask = sfSession.OpenAsync(CancellationToken.None);
            connectTask.Wait();

            Assert.IsFalse(sfSession._disableConsoleLogin);
            t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
        }

        [Test]
        public void TestSSOTokenAsync()
        {
            try
            {
                var user = "test";
                var host = $"{user}.okta.com";
                var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, user, TokenType.IdToken);
                var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
                credentialManager.SaveCredentials(key, "mockIdToken");
                SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                    SSOUrl = "https://www.mockSSOUrl.com"
                };
                var sfSession = new SFSession($"CLIENT_STORE_TEMPORARY_CREDENTIAL=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", null, restRequester, t_browserOperations.Object);
                Task connectTask = sfSession.OpenAsync(CancellationToken.None);
                connectTask.Wait();

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Never());
            }
            catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }
    }
}
