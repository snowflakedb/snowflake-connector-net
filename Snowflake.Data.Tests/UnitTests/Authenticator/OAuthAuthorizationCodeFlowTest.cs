using System.IO;
using System.Text;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.Browser;
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
        private static readonly string s_oauthSnowflakeLoginSuccessMappingPath = Path.Combine(s_oauthMappingPath, "snowflake_successful_login.json");

        private WiremockRunner _runner;

        [OneTimeSetUp]
        public void BeforeAll()
        {
            _runner = WiremockRunner.NewWiremock();
        }

        [SetUp]
        public void BeforeEach()
        {
            _runner.ResetMapping();
        }

        [OneTimeTearDown]
        public void AfterAll()
        {
            _runner.Stop();
        }

        [Test]
        public void TestSuccessfulAuthorizationCedeFlow()
        {
            // arrange
            _runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            Assert.NotNull(session._accessToken);
            Assert.AreEqual("access-token-123", SecureStringHelper.Decode(session._accessToken.Value));
            Assert.AreEqual("1234567890", session.sessionId);
            Assert.AreEqual("masterToken123", session.masterToken);
            Assert.AreEqual("sessionToken123", session.sessionToken);
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
            var user = "testUser";
            var account = "testAccount";
            var db = "testDb";
            var role = "ANALYST";
            var warehouse = "testWarehouse";
            var host = WiremockRunner.Host;
            var port = WiremockRunner.DefaultHttpPort;
            var scheme = "http";
            var clientId = "123";
            var clientSecret = "123";
            var authorizationScope = "session:role:ANALYST";
            var redirectUri = "http://localhost:8009/snowflake/oauth-redirect";
            var externalAuthorizationUrl = $"http://localhost:{port}/oauth/authorize";
            var externalTokenRequestUrl = $"http://localhost:{port}/oauth/token-request";
            return new StringBuilder()
                .Append($"authenticator={authenticator};user={user};account={account};")
                .Append($"db={db};role={role};warehouse={warehouse};host={host};port={port};scheme={scheme};")
                .Append($"oauthClientId={clientId};oauthClientSecret={clientSecret};oauthScope={authorizationScope};")
                .Append($"oauthRedirectUri={redirectUri};")
                .Append($"oauthAuthorizationUrl={externalAuthorizationUrl};oauthTokenRequestUrl={externalTokenRequestUrl};")
                .Append("poolingEnabled=false;minPoolSize=0;")
                .ToString();
        }
    }
}
