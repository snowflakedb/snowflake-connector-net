using System.Net;
using System.Text;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.Session;
using Moq;
using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class OAuthAuthorizationCodeAuthenticatorTest
    {
        private const string Account = "testaccount";
        private const string Host = Account + ".snowflakecomputing.com";
        private const int Port = 1080;
        private const string Scheme = "https";
        private const string Role = "ACCOUNTADMIN";
        private const string ClientId = "123";
        private const string ClientSecret = "1234";
        private const string AuthorizationScope = "session:role:ANALYST";
        private const string ExternalAuthorizationUrl = "https://localhost:8888/oauth/authorize";
        private const string ExternalTokenUrl = "https://localhost:8888/oauth/token";
        private const string State = "abc123";
        private const string CustomRedirectUri = "http://localhost:8001/oauth/redirect";
        private const string CustomRedirectUriWithSlash = CustomRedirectUri + "/";
        private const string DefaultScope = "session:role:" + Role;
        private static readonly string s_defaultAuthorizationEndpoint = $"https://{Host}:{Port}/oauth/authorize";
        private const int RandomPort = 1234;
        private static readonly string s_defaultRedirectUri = $"http://127.0.0.1:{RandomPort}";
        private static readonly string s_defaultRedirectUriWithSlash = s_defaultRedirectUri + "/";
        private const string CodeVerifier = "codeVerifierForAuthorizationCodeFlowMustBeALongString";

        [Test]
        public void TestUseDefaultValues()
        {
            // arrange
            var browserRunner = new Mock<IWebBrowserRunner>();
            var listenerStarter = new Mock<WebListenerStarter>();
            var authenticator = PrepareAuthenticator(browserRunner.Object, listenerStarter.Object, "");
            using var httpListener = new HttpListener();
            listenerStarter
                .Setup(s => s.GetRandomUnusedPort())
                .Returns(RandomPort);
            listenerStarter
                .Setup(s => s.StartHttpListener(s_defaultRedirectUriWithSlash))
                .Returns(httpListener);

            // act
            var authorizationData = authenticator.PrepareAuthorizationData();
            var listener = authenticator.StartListenerUpdatingRedirectUri(authorizationData.Request);

            // assert
            Assert.AreEqual(httpListener, listener);
            Assert.AreEqual(s_defaultAuthorizationEndpoint, authorizationData.Request.AuthorizationEndpoint);
            Assert.AreEqual(DefaultScope, authorizationData.Request.AuthorizationScope);
            Assert.AreEqual(ClientId, authorizationData.Request.ClientId);
            Assert.AreEqual(s_defaultRedirectUri, authorizationData.Request.RedirectUri);
            Assert.NotNull(authorizationData.Request.CodeChallenge);
            Assert.AreEqual(State, authorizationData.Request.State);
            listenerStarter.Verify(s => s.StartHttpListener(s_defaultRedirectUriWithSlash), Times.Once);
        }

        [Test]
        public void TestFailWhenCannotFindFreeRandomPortForDefaultRedirectUri()
        {
            // arrange
            var browserRunner = new Mock<IWebBrowserRunner>();
            var listenerStarter = new Mock<WebListenerStarter>();
            var authenticator = PrepareAuthenticator(browserRunner.Object, listenerStarter.Object, "");
            using var httpListener = new HttpListener();
            listenerStarter
                .Setup(s => s.GetRandomUnusedPort())
                .Returns(RandomPort);
            listenerStarter
                .Setup(s => s.StartHttpListener(s_defaultRedirectUriWithSlash))
                .Throws(() => new HttpListenerException(5, "Failed to listen on prefix because it conflicts with an existing registration on the machine."));

            // act/assert
            var authorizationData = authenticator.PrepareAuthorizationData();
            Assert.Throws<HttpListenerException>(() => authenticator.StartListenerUpdatingRedirectUri(authorizationData.Request));
        }

        [Test]
        public void TestUseCustomizedValues()
        {
            // arrange
            var customizedProperties = $"oauthScope={AuthorizationScope};oauthAuthorizationUrl={ExternalAuthorizationUrl};oauthTokenRequestUrl={ExternalTokenUrl};oauthRedirectUri={CustomRedirectUri};";
            var browserRunner = new Mock<IWebBrowserRunner>();
            var listenerStarter = new Mock<WebListenerStarter>();
            var authenticator = PrepareAuthenticator(browserRunner.Object, listenerStarter.Object, customizedProperties);
            using var httpListener = new HttpListener();
            listenerStarter
                .Setup(s => s.StartHttpListener(CustomRedirectUriWithSlash))
                .Returns(httpListener);

            // act
            var authorizationData = authenticator.PrepareAuthorizationData();
            var listener = authenticator.StartListenerUpdatingRedirectUri(authorizationData.Request);

            // assert
            Assert.AreEqual(httpListener, listener);
            Assert.AreEqual(ExternalAuthorizationUrl, authorizationData.Request.AuthorizationEndpoint);
            Assert.AreEqual(AuthorizationScope, authorizationData.Request.AuthorizationScope);
            Assert.AreEqual(ClientId, authorizationData.Request.ClientId);
            Assert.AreEqual(CustomRedirectUri, authorizationData.Request.RedirectUri);
            Assert.NotNull(authorizationData.Request.CodeChallenge);
            Assert.AreEqual(State, authorizationData.Request.State);
            listenerStarter.Verify(s => s.StartHttpListener(CustomRedirectUriWithSlash), Times.Once);
            listenerStarter.Verify(s => s.GetRandomUnusedPort(), Times.Never);
        }

        [Test]
        public void TestBrowserShouldFailWithExceptionWithMaskedUrl()
        {
            // arrange
            var customizedProperties = $"oauthScope={AuthorizationScope};oauthAuthorizationUrl={ExternalAuthorizationUrl};oauthTokenRequestUrl={ExternalTokenUrl};oauthRedirectUri={CustomRedirectUri};";
            var browserRunner = new Mock<IWebBrowserRunner>();
            var listenerStarter = new Mock<WebListenerStarter>();
            var authenticator = PrepareAuthenticator(browserRunner.Object, listenerStarter.Object, customizedProperties);
            var authorizationData = authenticator.PrepareAuthorizationData();
            var url = authorizationData.Request.GetUrl();
            var invalidUrl = new Url(url.Value.Replace("https://", "ftp://"), url.ValueWithoutSecrets.Replace("https://", "ftp://"));
            var browserStarter = new WebBrowserStarter(browserRunner.Object);
            var expectedUrlInException = "ftp://localhost:8888/oauth/authorize?client_id=****&response_type=code&redirect_uri=http%3a%2f%2flocalhost%3a8001%2foauth%2fredirect&scope=session%3arole%3aANALYST&code_challenge=xqXLsNRtro84G9aBLgX4c7djUqmrzbJCrW39HerS5Hk&code_challenge_method=S256&state=****";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => browserStarter.StartBrowser(invalidUrl));

            // assert
            Assert.That(thrown.Message, Does.Contain("Invalid browser url"));
            var urlInException = thrown.Message.Split('\"')[1];
            Assert.AreEqual(expectedUrlInException, urlInException);
        }

        private OAuthAuthorizationCodeAuthenticator PrepareAuthenticator(
            IWebBrowserRunner browserRunner,
            WebListenerStarter listenerStarter,
            string additionalParameters)
        {
            var connectionString = GetAuthorizationCodeConnectionString(additionalParameters);
            var sessionContext = new SessionPropertiesContext();
            var session = new SFSession(connectionString, sessionContext);
            var challengeProvider = new Mock<ChallengeProvider>();
            challengeProvider.Setup(c => c.GenerateState())
                .Returns(State);
            challengeProvider.Setup(c => c.GenerateCodeVerifier())
                .Returns(new CodeVerifier(CodeVerifier));
            var webBrowserMock = new WebBrowserStarter(browserRunner);
            var authenticator = new OAuthAuthorizationCodeAuthenticator(session, challengeProvider.Object, webBrowserMock, listenerStarter);
            session.ReplaceAuthenticator(authenticator);
            return authenticator;
        }

        private string GetAuthorizationCodeConnectionString(string additionalConnectionStringPart)
        {
            var authenticator = OAuthAuthorizationCodeAuthenticator.AuthName;
            var user = "testUser";
            var db = "testDb";
            var warehouse = "testWarehouse";
            var connectionString = new StringBuilder()
                .Append($"authenticator={authenticator};user={user};account={Account};")
                .Append($"db={db};role={Role};warehouse={warehouse};host={Host};port={Port};scheme={Scheme};")
                .Append($"oauthClientId={ClientId};oauthClientSecret={ClientSecret};");
            if (!string.IsNullOrEmpty(additionalConnectionStringPart))
                connectionString.Append(additionalConnectionStringPart);
            return connectionString.ToString();
        }
    }
}
