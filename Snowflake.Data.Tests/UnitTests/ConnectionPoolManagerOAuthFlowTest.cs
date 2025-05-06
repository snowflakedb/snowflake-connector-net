using System;
using System.Collections.Generic;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.Rest;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    public class ConnectionPoolManagerOAuthTest
    {
        private ConnectionPoolManager _connectionPoolManager;
        private static SessionPool _sessionPool;
        private const string ConnectionStringClientCredentials = "db=D1;warehouse=W1;account=A1;password=P1;role=R1;minPoolSize=2;authenticator=oauth_client_credentials;OAUTHCLIENTID=client_id;OAUTHCLIENTSECRET=client_secret;OAUTHTOKENREQUESTURL=https://test.snoflakecomputing.com;";
        private static Mock<IMockRestRequester> s_restRequester;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_restRequester = new Mock<IMockRestRequester>();
            SessionPool.SessionFactory = new MockSessionFactoryForRequester(s_restRequester.Object);
        }

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            SessionPool.SessionFactory = new SessionFactory();
            RemoveFromCache(TokenType.OAuthAccessToken);
            RemoveFromCache(TokenType.OAuthRefreshToken);
        }

        [SetUp]
        public void BeforeEach()
        {
            _connectionPoolManager = (ConnectionPoolManager) SnowflakeDbConnectionPool.ConnectionManager;
            s_restRequester.Reset();
            SetupRestRequester();
            RemoveFromCache(TokenType.OAuthAccessToken);
            RemoveFromCache(TokenType.OAuthRefreshToken);
        }

        [Test]
        public void TestBackgroundSessionOpenedAfterTheRequestedSessionEstablishedWhenCacheEnabled()
        {
            // arrange
            var connectionString = ConnectionStringClientCredentials + "CLIENT_STORE_TEMPORARY_CREDENTIAL=true;user=U1;";
            _sessionPool = _connectionPoolManager.GetPool(connectionString);

            // act
            _connectionPoolManager.GetSession(connectionString, new SessionPropertiesContext());

            // assert
            WaitUntilBothSessionsCreated();
            s_restRequester.Verify(r => r.Post<LoginResponse>(It.IsAny<IRestRequest>()), Times.Exactly(2));
            s_restRequester.Verify(r => r.Post<OAuthAccessTokenResponse>(It.IsAny<IRestRequest>()), Times.Exactly(1));
        }

        [Test]
        [TestCase("CLIENT_STORE_TEMPORARY_CREDENTIAL=false;user=U1;")]
        [TestCase("CLIENT_STORE_TEMPORARY_CREDENTIAL=true;")] // no user specified
        public void TestBackgroundSessionOpenedConcurrentlyWithTheRequestedSessionWhenCacheDisabled(string cacheRelatedProperties)
        {
            // arrange
            var connectionString = ConnectionStringClientCredentials + cacheRelatedProperties;
            _sessionPool = _connectionPoolManager.GetPool(connectionString);

            // act
            _connectionPoolManager.GetSession(connectionString, new SessionPropertiesContext());

            // assert
            WaitUntilBothSessionsCreated();
            s_restRequester.Verify(r => r.Post<LoginResponse>(It.IsAny<IRestRequest>()), Times.Exactly(2));
            s_restRequester.Verify(r => r.Post<OAuthAccessTokenResponse>(It.IsAny<IRestRequest>()), Times.Exactly(2));
        }

        private static void RemoveFromCache(TokenType tokenType)
        {
            var cacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey("test.snoflakecomputing.com", "U1", tokenType);
            SnowflakeCredentialManagerFactory.GetCredentialManager().RemoveCredentials(cacheKey);
        }

        private void WaitUntilBothSessionsCreated() =>
            Awaiter.WaitUntilConditionOrTimeout(() =>
            {
                var state = _sessionPool.GetCurrentState();
                return state.IdleSessionsCount == 1 && state.BusySessionsCount == 1 && state.Count() == 2;
            }, TimeSpan.FromSeconds(15));

        private static void SetupRestRequester()
        {
            s_restRequester
                .Setup(r => r.Post<OAuthAccessTokenResponse>(It.IsAny<IRestRequest>()))
                .Returns(new OAuthAccessTokenResponse
                {
                    AccessToken = "access-token-123"
                });
            s_restRequester
                .Setup(r => r.Post<LoginResponse>(It.IsAny<IRestRequest>()))
                .Returns(new LoginResponse
                {
                    code = 200,
                    success = true,
                    data = new LoginResponseData
                    {
                        sessionId = "12345",
                        token = "session-token-123",
                        masterToken = "master-token-123",
                        nameValueParameter = new List<NameValueParameter>(),
                        authResponseSessionInfo = new SessionInfo()
                    }
                });
            s_restRequester
                .Setup(r => r.Post<CloseResponse>(It.IsAny<IRestRequest>()))
                .Returns(new CloseResponse
                {
                    code = 200,
                    success = true
                });
        }
    }
}
