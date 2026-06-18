using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [CollectionDefinition(nameof(OAuthClientCredentialFlowTestFixture), DisableParallelization = true)]
    public sealed class OAuthClientCredentialFlowTestFixture : ICollectionFixture<OAuthClientCredentialFlowTestFixture.Fixture>
    {
        public sealed class Fixture : IDisposable
        {
            internal readonly IWiremockRunner Runner;

            public Fixture()
            {
                if (SkipConditionEvaluator.Evaluate(SkipCondition.SkipOnJenkins).ShouldSkip)
                {
                    Runner = new Mock<IWiremockRunner>().Object;
                    return;
                }

                Runner = WiremockRunner.NewWiremock();
            }

            public void Dispose()
            {
                Runner.Stop();
            }
        }
    }

    [Collection(nameof(OAuthClientCredentialFlowTestFixture))]
    public class OAuthClientCredentialFlowTest : BaseOAuthFlowTest
    {
        private readonly OAuthClientCredentialFlowTestFixture.Fixture _fixture;
        private static readonly string s_oauthClientCredentialsMappingPath = Path.Combine(s_oauthMappingPath, "ClientCredentials");
        private static readonly string s_clientCredentialSuccessfulMappingPath = Path.Combine(s_oauthClientCredentialsMappingPath, "successful_flow.json");
        private static readonly string s_tokenRequestErrorMappingPath = Path.Combine(s_oauthClientCredentialsMappingPath, "token_request_error.json");
        private static readonly string s_tokenRequestNoRefreshTokenMappingPath = Path.Combine(s_oauthClientCredentialsMappingPath, "successful_without_refresh_token.json");

        public OAuthClientCredentialFlowTest(OAuthClientCredentialFlowTestFixture.Fixture fixture)
        {
            _fixture = fixture;
            _fixture.Runner.ResetMapping();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulClientCredentialsFlow()
        {
            // arrange
            _fixture.Runner.AddMappings(s_clientCredentialSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulClientCredentialsFlowAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_clientCredentialSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulFlowWithoutRefreshToken()
        {
            // arrange
            _fixture.Runner.AddMappings(s_tokenRequestNoRefreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            session.Open();

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulFlowWithoutRefreshTokenAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_tokenRequestNoRefreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();

            // act
            await session.OpenAsync(CancellationToken.None);

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAuthorizationCodeFlowWithClientSecretProvidedExternally()
        {
            // arrange
            _fixture.Runner.AddMappings(s_clientCredentialSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(clientSecretInConnectionString: false);

            // act
            session.Open();

            // assert
            AssertAccessTokenSetInAuthenticator(session);
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestTokenRequestError()
        {
            // arrange
            _fixture.Runner.AddMappings(s_tokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.Equal(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)", thrown.Message);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestTokenRequestErrorAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_tokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None));

            // assert
            Assert.Equal(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)", thrown.Message);
        }

        private void AssertAccessTokenSetInAuthenticator(SFSession session, string expectedAccessToken = AccessToken)
        {
            var authenticator = (OAuthClientCredentialsAuthenticator)session.GetAuthenticator();
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(expectedAccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
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
