using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture, NonParallelizable]
    public class OAuthClientCredentialFlowTest : BaseOAuthFlowTest
    {
        private static readonly string s_oauthClientCredentialsMappingPath = Path.Combine(s_oauthMappingPath, "ClientCredentials");
        private static readonly string s_clientCredentialSuccessfulMappingPath = Path.Combine(s_oauthClientCredentialsMappingPath, "successful_flow.json");
        private static readonly string s_tokenRequestErrorMappingPath = Path.Combine(s_oauthClientCredentialsMappingPath, "token_request_error.json");
        private static readonly string s_tokenRequestNoRefreshTokenMappingPath = Path.Combine(s_oauthClientCredentialsMappingPath, "successful_without_refresh_token.json");

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
        public void TestSuccessfulClientCredentialsFlow()
        {
            // arrange
            _runner.AddMappings(s_clientCredentialSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulClientCredentialsFlowAsync()
        {
            // arrange
            _runner.AddMappings(s_clientCredentialSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestSuccessfulFlowWithoutRefreshToken()
        {
            // arrange
            _runner.AddMappings(s_tokenRequestNoRefreshTokenMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public async Task TestSuccessfulFlowWithoutRefreshTokenAsync()
        {
            // arrange
            _runner.AddMappings(s_tokenRequestNoRefreshTokenMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestSuccessfulAuthorizationCodeFlowWithClientSecretProvidedExternally()
        {
            // arrange
            _runner.AddMappings(s_clientCredentialSuccessfulMappingPath);
            _runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(clientSecretInConnectionString: false);

            // act
            session.Open();

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [Test]
        public void TestTokenRequestError()
        {
            // arrange
            _runner.AddMappings(s_tokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)"));
        }

        [Test]
        public void TestTokenRequestErrorAsync()
        {
            // arrange
            _runner.AddMappings(s_tokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.That(thrown.ErrorCode, Is.EqualTo(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode));
            Assert.That(thrown.Message, Does.Contain("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)"));
        }

        private void AssertAccessTokenSetInAuthenticator(SFSession session, string expectedAccessToken = AccessToken)
        {
            var authenticator = (OAuthClientCredentialsAuthenticator)session.GetAuthenticator();
            Assert.NotNull(authenticator.AccessToken);
            Assert.AreEqual(expectedAccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
        }

        private SFSession PrepareSession(bool clientSecretInConnectionString = true, bool userInConnectionString = true, string connectionStringSuffix = "client_store_temporary_credential=true;")
        {
            var connectionString = GetClientCredentialsConnectionString(clientSecretInConnectionString, userInConnectionString) + connectionStringSuffix;
            var sessionContext = new SessionPropertiesContext
            {
                OAuthClientSecret = clientSecretInConnectionString ? null : SecureStringHelper.Encode(ClientSecret)
            };
            return new SFSession(connectionString, sessionContext);
        }

        private string GetClientCredentialsConnectionString(bool addOAuthClientSecret, bool addUser)
        {
            var authenticator = OAuthClientCredentialsAuthenticator.AuthName;
            var account = "testAccount";
            var db = "testDb";
            var role = "ANALYST";
            var warehouse = "testWarehouse";
            var host = WiremockRunner.Host;
            var port = WiremockRunner.DefaultHttpPort;
            var scheme = "http";
            var clientId = "123";
            var connectionStringBuilder = new StringBuilder()
                .Append($"authenticator={authenticator};account={account};")
                .Append($"db={db};role={role};warehouse={warehouse};host={host};port={port};scheme={scheme};")
                .Append($"oauthClientId={clientId};oauthScope={AuthorizationScope};")
                .Append($"oauthTokenRequestUrl={s_externalTokenRequestUrl};");
            if (addOAuthClientSecret)
                connectionStringBuilder.Append($"oauthClientSecret={ClientSecret};");
            if (addUser)
                connectionStringBuilder.Append($"user={User};");
            return connectionStringBuilder.ToString();
        }
    }
}
