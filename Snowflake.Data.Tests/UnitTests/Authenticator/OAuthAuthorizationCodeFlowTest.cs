using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture, NonParallelizable]
    public class OAuthAuthorizationCodeFlowTest
    {
        private static readonly string s_oauthMappingPath = Path.Combine("wiremock", "OAuth");
        private static readonly string s_oauthAuthorizationCodeMappingPath = Path.Combine(s_oauthMappingPath, "AuthorizationCode");
        private static readonly string s_authorizationCodeSuccessfulMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "successful_flow.json");
        private static readonly string s_invalidScopeErrorMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "invalid_scope_error.json");
        private static readonly string s_invalidStateErrorMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "invalid_state_error.json");
        private static readonly string s_badTokenRequestErrorMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "token_request_error.json");
        private static readonly string s_refreshTokenMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "refresh_token.json");
        private static readonly string s_oauthSnowflakeLoginSuccessMappingPath = Path.Combine(s_oauthMappingPath, "snowflake_successful_login.json");
        private static readonly string s_oauthSnowflakeLoginInvalidTokenMappingPath = Path.Combine(s_oauthMappingPath, "snowflake_invalid_token_login.json");
        private const string MasterToken = "masterToken123";
        private const string SessionToken = "sessionToken123";
        private const string AccessToken = "access-token-123";
        private const string RefreshToken = "refresh-token-123";
        private const string NewAccessToken = "new-access-token-123";
        private const string NewRefreshToken = "new-refresh-token-123";
        private const string SessionId = "1234567890";
        private const string User = "testUser";
        private const string AuthorizationScope = "session:role:ANALYST";
        private const string TokenHost = "localhost";
        private static readonly string s_externalAuthorizationUrl = $"http://localhost:{WiremockRunner.DefaultHttpPort}/oauth/authorize";
        private static readonly string s_externalTokenRequestUrl = $"http://localhost:{WiremockRunner.DefaultHttpPort}/oauth/token-request";

        private WiremockRunner _runner;

        [OneTimeSetUp]
        public void BeforeAll()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        [SetUp]
        public void BeforeEach()
        {
            SnowflakeCredentialManagerFactory.SetCredentialManager(new SFCredentialManagerInMemoryImpl());
            _runner.ResetMapping();
        }

        [OneTimeTearDown]
        public void AfterAll()
        {
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
            _runner.Stop();
        }

        [Test]
        public void TestSuccessfulAuthorizationCodeFlow()
        {
            // arrange
            _runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.AreEqual(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulAuthorizationCodeFlowAsync()
        {
            // arrange
            _runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.AreEqual(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        [Ignore("temporarily ignored")]
        public void TestSuccessfulAuthorizationCodeFlowWithDefaultCache()
        {
            // arrange
            try
            {
                SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
                RemoveTokenFromCache(TokenType.OAuthAccessToken);
                RemoveTokenFromCache(TokenType.OAuthRefreshToken);
                _runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
                _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
                var session = PrepareSession();
                var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

                // act
                session.Open();

                // assert
                Assert.NotNull(authenticator.AccessToken);
                Assert.AreEqual(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
                Assert.AreEqual(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
                Assert.AreEqual(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
                AssertSessionSuccessfullyCreated(session);
            }
            finally
            {
                RemoveTokenFromCache(TokenType.OAuthAccessToken);
                RemoveTokenFromCache(TokenType.OAuthRefreshToken);
            }
        }

        [Test]
        public void TestUseCachedAccessToken()
        {
            // arrange
            SaveTokenToCache(TokenType.OAuthAccessToken, AccessToken);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.AreEqual(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.IsEmpty(ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestUseCachedAccessTokenAsync()
        {
            // arrange
            SaveTokenToCache(TokenType.OAuthAccessToken, AccessToken);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.AreEqual(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.IsEmpty(ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestRefreshToken()
        {
            // arrange
            _runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginInvalidTokenMappingPath);
            _runner.AddMappings(s_refreshTokenMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath, new StringTransformation(AccessToken, NewAccessToken));
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(NewAccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.AreEqual(NewAccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(NewRefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestRefreshTokenAsync()
        {
            // arrange
            _runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginInvalidTokenMappingPath);
            _runner.AddMappings(s_refreshTokenMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath, new StringTransformation(AccessToken, NewAccessToken));
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator) session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(NewAccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.AreEqual(NewAccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(NewRefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestInvalidScope()
        {
            // arrange
            _runner.AddMappings(s_invalidScopeErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Invalid response from browser: Authorization code response has error 'invalid_scope' and description 'One or more scopes are not configured for the authorization server resource.'"));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [Test]
        public void TestInvalidScopeAsync()
        {
            // arrange
            _runner.AddMappings(s_invalidScopeErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Invalid response from browser: Authorization code response has error 'invalid_scope' and description 'One or more scopes are not configured for the authorization server resource.'"));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [Test]
        public void TestInvalidState()
        {
            // arrange
            _runner.AddMappings(s_invalidStateErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Invalid response from browser: State mismatch for authorization code request and response."));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [Test]
        public void TestInvalidStateAsync()
        {
            // arrange
            _runner.AddMappings(s_invalidStateErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Invalid response from browser: State mismatch for authorization code request and response."));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [Test]
        public void TestTokenRequestError()
        {
            // arrange
            _runner.AddMappings(s_badTokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)"));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [Test]
        public void TestTokenRequestErrorAsync()
        {
            // arrange
            _runner.AddMappings(s_badTokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)"));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.AreEqual(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        private void AssertSessionSuccessfullyCreated(SFSession session)
        {
            Assert.AreEqual(SessionId, session.sessionId);
            Assert.AreEqual(MasterToken, session.masterToken);
            Assert.AreEqual(SessionToken, session.sessionToken);
        }

        private SFSession PrepareSession()
        {
            var connectionString = GetAuthorizationCodeConnectionString();
            var sessionContext = new SessionPropertiesContext { AllowHttpForIdp = true };
            var session = new SFSession(connectionString, sessionContext);
            var challengeProvider = new Mock<ChallengeProvider>();
            challengeProvider.Setup(c => c.GenerateState())
                .Returns("abc123");
            var webBrowserMock = new WebBrowserStarter(new MockBrowser());
            var authenticator = new OAuthAuthorizationCodeAuthenticator(session, challengeProvider.Object, webBrowserMock, WebListenerStarter.Instance);
            session.ReplaceAuthenticator(authenticator);
            return session;
        }

        private string GetAuthorizationCodeConnectionString()
        {
            var authenticator = OAuthAuthorizationCodeAuthenticator.AuthName;
            var account = "testAccount";
            var db = "testDb";
            var role = "ANALYST";
            var warehouse = "testWarehouse";
            var host = WiremockRunner.Host;
            var port = WiremockRunner.DefaultHttpPort;
            var scheme = "http";
            var clientId = "123";
            var clientSecret = "123";
            var redirectUri = "http://localhost:8009/snowflake/oauth-redirect";
            return new StringBuilder()
                .Append($"authenticator={authenticator};user={User};account={account};")
                .Append($"db={db};role={role};warehouse={warehouse};host={host};port={port};scheme={scheme};")
                .Append($"oauthClientId={clientId};oauthClientSecret={clientSecret};oauthScope={AuthorizationScope};")
                .Append($"oauthRedirectUri={redirectUri};")
                .Append($"oauthAuthorizationUrl={s_externalAuthorizationUrl};oauthTokenRequestUrl={s_externalTokenRequestUrl};")
                .ToString();
        }

        private string ExtractTokenFromCache(TokenType tokenType)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(TokenHost, User, tokenType);
            return SnowflakeCredentialManagerFactory.GetCredentialManager().GetCredentials(cacheKey);
        }

        private void SaveTokenToCache(TokenType tokenType, string token)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(TokenHost, User, tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().SaveCredentials(cacheKey, token);
        }

        private void RemoveTokenFromCache(TokenType tokenType)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(TokenHost, User, tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(cacheKey);
        }
    }
}
