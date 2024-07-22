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
            try
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

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
            } catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }

        [Test]
        public void TestConsoleLogin()
        {
            try
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

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
            }
            catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }

        [Test]
        public void TestSSOToken()
        {
            try
            {
                var user = "test";
                var host = $"{user}.okta.com";
                var key = SFCredentialManagerFactory.BuildCredentialKey(host, user, TokenType.IdToken);
                var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
                credentialManager.SaveCredentials(key, "mockIdToken");
                SFCredentialManagerFactory.SetCredentialManager(credentialManager);

                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                    SSOUrl = "https://www.mockSSOUrl.com",
                };
                var sfSession = new SFSession($"allow_sso_token_caching=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", null, restRequester, t_browserOperations.Object);
                sfSession.Open();

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Never());
            }
            catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }

        [Test]
        [Ignore("Temporary only. Looking for fix when tests are ran parallel")]
        public void TestThatThrowsTimeoutErrorWhenNoBrowserResponse()
        {
            try
            {
                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                };
                var sfSession = new SFSession("browser_response_timeout=0;account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
                sfSession.Open();
                Assert.Fail("Should fail");
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.BROWSER_RESPONSE_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        public void TestThatThrowsErrorWhenUrlDoesNotMatchRegex()
        {
            try
            {
                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                    SSOUrl = "non-matching-regex.com"
                };
                var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
                sfSession.Open();
                Assert.Fail("Should fail");
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        public void TestThatThrowsErrorWhenUrlIsNotWellFormedUriString()
        {
            try
            {
                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                    SSOUrl = "http://localhost:123/?token=mockToken\\\\"
                };
                var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
                sfSession.Open();
                Assert.Fail("Should fail");
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.INVALID_BROWSER_URL.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Temporary only. Looking for fix when tests are ran parallel")]
        public void TestThatThrowsErrorWhenBrowserRequestMethodIsNotGet()
        {
            try
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
                var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
                sfSession.Open();
                Assert.Fail("Should fail");
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.BROWSER_RESPONSE_WRONG_METHOD.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Temporary only. Looking for fix when tests are ran parallel")]
        public void TestThatThrowsErrorWhenBrowserRequestHasInvalidQuery()
        {
            try
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
                var sfSession = new SFSession("account=test;user=test;password=test;authenticator=externalbrowser;host=test.okta.com", null, restRequester, t_browserOperations.Object);
                sfSession.Open();
                Assert.Fail("Should fail");
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(SFError.BROWSER_RESPONSE_INVALID_PREFIX.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
            }
        }

        [Test]
        public void TestDefaultAuthenticationAsync()
        {
            try
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

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
            }
            catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }

        [Test]
        public void TestConsoleLoginAsync()
        {
            try
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

                t_browserOperations.Verify(b => b.OpenUrl(It.IsAny<string>()), Times.Once());
            }
            catch (SnowflakeDbException e)
            {
                Assert.Fail("Should pass without exception", e);
            }
        }

        [Test]
        public void TestSSOTokenAsync()
        {
            try
            {
                var user = "test";
                var host = $"{user}.okta.com";
                var key = SFCredentialManagerFactory.BuildCredentialKey(host, user, TokenType.IdToken);
                var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
                credentialManager.SaveCredentials(key, "mockIdToken");
                SFCredentialManagerFactory.SetCredentialManager(credentialManager);

                var restRequester = new Mock.MockExternalBrowserRestRequester()
                {
                    ProofKey = "mockProofKey",
                    SSOUrl = "https://www.mockSSOUrl.com",
                };
                var sfSession = new SFSession($"allow_sso_token_caching=true;account=test;user={user};password=test;authenticator=externalbrowser;host={host}", null, restRequester, t_browserOperations.Object);
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
