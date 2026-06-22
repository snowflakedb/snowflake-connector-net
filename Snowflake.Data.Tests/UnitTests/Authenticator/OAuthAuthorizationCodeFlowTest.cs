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
using Snowflake.Data.Core.Authenticator.Browser;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [CollectionDefinition(nameof(OAuthAuthorizationCodeFlowTestFixture), DisableParallelization = true)]
    public sealed class OAuthAuthorizationCodeFlowTestFixture : ICollectionFixture<OAuthAuthorizationCodeFlowTestFixture.Fixture>
    {
        public sealed class Fixture : IDisposable
        {
            internal readonly WiremockRunner Runner;

            public Fixture()
            {
                Runner = WiremockRunner.NewWiremock();
            }

            public void Dispose()
            {
                SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
                Runner.Stop();
            }
        }
    }

    [Collection(nameof(OAuthAuthorizationCodeFlowTestFixture))]
    public class OAuthAuthorizationCodeFlowTest : BaseOAuthFlowTest
    {
        private readonly OAuthAuthorizationCodeFlowTestFixture.Fixture _fixture;
        private static readonly string s_authorizationCodeSuccessfulMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "successful_flow.json");
        private static readonly string s_authorizationCodeSuccessfulWithSingleUseRefreshTokenMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "successful_flow_with_single_use_refresh_token.json");
        private static readonly string s_authorizationCodeSuccessfulWithoutRefreshTokenMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "successful_flow_without_refresh_token.json");
        private static readonly string s_invalidScopeErrorMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "invalid_scope_error.json");
        private static readonly string s_invalidStateErrorMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "invalid_state_error.json");
        private static readonly string s_badTokenRequestErrorMappingPath = Path.Combine(s_oauthAuthorizationCodeMappingPath, "token_request_error.json");
        private static readonly string s_externalAuthorizationUrl = $"http://localhost:{WiremockRunner.DefaultHttpPort}/oauth/authorize";

        public OAuthAuthorizationCodeFlowTest(OAuthAuthorizationCodeFlowTestFixture.Fixture fixture)
        {
            _fixture = fixture;
            SnowflakeCredentialManagerFactory.SetCredentialManager(new SFCredentialManagerInMemoryImpl());
            _fixture.Runner.ResetMapping();
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAuthorizationCodeFlow()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulAuthorizationCodeFlowAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAuthorizationCodeFlowWithSingleUseRefreshTokens()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulWithSingleUseRefreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(connectionStringSuffix: "client_store_temporary_credential=true;oauthEnableSingleUseRefreshTokens=true;");
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulAuthorizationCodeFlowWithSingleUseRefreshTokensAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulWithSingleUseRefreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(connectionStringSuffix: "client_store_temporary_credential=true;oauthEnableSingleUseRefreshTokens=true;");
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAuthorizationCodeFlowWithDefaultCache()
        {
            // arrange
            try
            {
                SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
                RemoveTokenFromCache(TokenType.OAuthAccessToken);
                RemoveTokenFromCache(TokenType.OAuthRefreshToken);
                _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
                _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
                var session = PrepareSession();
                var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

                // act
                session.Open();

                // assert
                Assert.NotNull(authenticator.AccessToken);
                Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
                Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
                Assert.Equal(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
                AssertSessionSuccessfullyCreated(session);
            }
            finally
            {
                RemoveTokenFromCache(TokenType.OAuthAccessToken);
                RemoveTokenFromCache(TokenType.OAuthRefreshToken);
            }
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAuthorizationFlowWithoutRefreshToken()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulWithoutRefreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestSuccessfulAuthorizationFlowWithoutRefreshTokenAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulWithoutRefreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestSuccessfulAuthorizationCodeFlowWithClientSecretProvidedExternally()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(false);
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(RefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestDontUseCacheWhenUserNotProvided()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(userInConnectionString: false);
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(0, InMemoryCacheCount());
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestDontUseCacheWhenUserNotProvidedAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(userInConnectionString: false);
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(0, InMemoryCacheCount());
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestDontUseCacheWhenClientStoreTemporaryCredentialsIsOff()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(connectionStringSuffix: "client_store_temporary_credential=false;");
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(0, InMemoryCacheCount());
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestDontUseCacheWhenClientStoreTemporaryCredentialsIsOffAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession(connectionStringSuffix: "client_store_temporary_credential=false;");
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(0, InMemoryCacheCount());
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestUseCachedAccessToken()
        {
            // arrange
            SaveTokenToCache(TokenType.OAuthAccessToken, AccessToken);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Empty(ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestUseCachedAccessTokenAsync()
        {
            // arrange
            SaveTokenToCache(TokenType.OAuthAccessToken, AccessToken);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath);
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(AccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(AccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Empty(ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestRefreshToken()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginInvalidTokenMappingPath);
            _fixture.Runner.AddMappings(s_refreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath, new StringTransformations().ThenTransform(AccessToken, NewAccessToken));
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            session.Open();

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(NewAccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(NewAccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(NewRefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestRefreshTokenAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_authorizationCodeSuccessfulMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginInvalidTokenMappingPath);
            _fixture.Runner.AddMappings(s_refreshTokenMappingPath);
            _fixture.Runner.AddMappings(s_oauthSnowflakeLoginSuccessMappingPath, new StringTransformations().ThenTransform(AccessToken, NewAccessToken));
            var session = PrepareSession();
            var authenticator = (OAuthAuthorizationCodeAuthenticator)session.GetAuthenticator();

            // act
            await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.NotNull(authenticator.AccessToken);
            Assert.Equal(NewAccessToken, SecureStringHelper.Decode(authenticator.AccessToken));
            Assert.Equal(NewAccessToken, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(NewRefreshToken, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
            AssertSessionSuccessfullyCreated(session);
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestInvalidScope()
        {
            // arrange
            _fixture.Runner.AddMappings(s_invalidScopeErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.Equal(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Invalid response from browser: Authorization code response has error 'invalid_scope' and description 'One or more scopes are not configured for the authorization server resource.'", thrown.Message);
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestInvalidScopeAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_invalidScopeErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None)).ConfigureAwait(false);

            // assert
            Assert.Equal(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Invalid response from browser: Authorization code response has error 'invalid_scope' and description 'One or more scopes are not configured for the authorization server resource.'", thrown.Message);
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestInvalidState()
        {
            // arrange
            _fixture.Runner.AddMappings(s_invalidStateErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.Equal(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Invalid response from browser: State mismatch for authorization code request and response.", thrown.Message);
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestInvalidStateAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_invalidStateErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None)).ConfigureAwait(false);

            // assert
            Assert.Equal(SFError.BROWSER_RESPONSE_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Invalid response from browser: State mismatch for authorization code request and response.", thrown.Message);
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public void TestTokenRequestError()
        {
            // arrange
            _fixture.Runner.AddMappings(s_badTokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => session.Open());

            // assert
            Assert.Equal(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)", thrown.Message);
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        [SFFact(SkipCondition.SkipOnJenkins)]
        public async Task TestTokenRequestErrorAsync()
        {
            // arrange
            _fixture.Runner.AddMappings(s_badTokenRequestErrorMappingPath);
            var session = PrepareSession();

            // act
            var thrown = await Assert.ThrowsAsync<SnowflakeDbException>(() => session.OpenAsync(CancellationToken.None)).ConfigureAwait(false);

            // assert
            Assert.Equal(SFError.OAUTH_TOKEN_REQUEST_ERROR.GetAttribute<SFErrorAttr>().errorCode, thrown.ErrorCode);
            Assert.Contains("Error on getting an OAuth token from IDP: Response status code does not indicate success: 400 (Bad Request)", thrown.Message);
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthAccessToken));
            Assert.Equal(string.Empty, ExtractTokenFromCache(TokenType.OAuthRefreshToken));
        }

        private SFSession PrepareSession(bool clientSecretInConnectionString = true, bool userInConnectionString = true, string connectionStringSuffix = "client_store_temporary_credential=true;")
        {
            var connectionString = GetAuthorizationCodeConnectionString(clientSecretInConnectionString, userInConnectionString) + connectionStringSuffix;
            var sessionContext = new SessionPropertiesContext
            {
                OAuthClientSecret = clientSecretInConnectionString ? null : SecureStringHelper.Encode(ClientSecret)
            };
            var session = new SFSession(connectionString, sessionContext);
            var challengeProvider = new Mock<ChallengeProvider>();
            challengeProvider.Setup(c => c.GenerateState())
                .Returns("abc123");
            challengeProvider.Setup(c => c.GenerateCodeVerifier())
                .CallBase();
            var webBrowserMock = new WebBrowserStarter(new MockBrowser());
            var authenticator = new OAuthAuthorizationCodeAuthenticator(session, challengeProvider.Object, webBrowserMock, WebListenerStarter.Instance);
            session.ReplaceAuthenticator(authenticator);
            return session;
        }

        private string GetAuthorizationCodeConnectionString(bool addOAuthClientSecret, bool addUser)
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
            var redirectUri = "http://localhost:8009/snowflake/oauth-redirect";
            var connectionStringBuilder = new StringBuilder()
                .Append($"authenticator={authenticator};account={account};")
                .Append($"db={db};role={role};warehouse={warehouse};host={host};port={port};scheme={scheme};")
                .Append($"oauthClientId={clientId};oauthScope={AuthorizationScope};")
                .Append($"oauthRedirectUri={redirectUri};")
                .Append($"oauthAuthorizationUrl={s_externalAuthorizationUrl};oauthTokenRequestUrl={s_externalTokenRequestUrl};");
            if (addOAuthClientSecret)
                connectionStringBuilder.Append($"oauthClientSecret={ClientSecret};");
            if (addUser)
                connectionStringBuilder.Append($"user={User};");
            return connectionStringBuilder.ToString();
        }
    }
}
