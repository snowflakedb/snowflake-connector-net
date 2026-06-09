using System.Security;
using System.Threading.Tasks;
using Snowflake.Data.Tests.IntegrationTests;

namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using Mock;
    using Xunit;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.Session;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core.Tools;
    using Snowflake.Data.Tests.Util;

    public class ConnectionPoolManagerMFATest : IDisposable
    {
        internal MockLoginMFATokenCacheRestRequester RestRequester { get; }

        private const string ConnectionStringMFACache = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;authenticator=username_password_mfa";

        private class Factory : IConnectionManagerFactory
        {
            private readonly IMockRestRequester _restRequester;

            public Factory(IMockRestRequester restRequester)
            {
                _restRequester = restRequester;
            }

            public IConnectionManager CreateConnectionManager(ConnectionPoolType requestedPoolType) =>
                new ConnectionPoolManager(SessionPoolFactory, this);

            private SessionPool SessionPoolFactory(string connectionString, SecureString password, SecureString oauthClientSecret, SecureString token)
            {
                var sessionFactory = new MockSessionFactoryMFA(_restRequester);
                return SessionPool.CreateSessionPool(connectionString, password, oauthClientSecret, token, sessionFactory);
            }
        }

        public ConnectionPoolManagerMFATest()
        {
            RestRequester = new MockLoginMFATokenCacheRestRequester();
            ConnectionManagerTestsFacade.Init();
            ConnectionManagerTestsFacade.RegisterDedicatedContext(nameof(ConnectionPoolManagerMFATest), ConnectionPoolType.MultipleConnectionPool, new Factory(RestRequester));
        }

        [SFFact(SkipCondition.SkipOnJenkins, RetriesCount = RetriesCount.Thrice)]
        public async Task TestPoolManagerReturnsSessionPoolForGivenConnectionStringUsingMFA()
        {
            // Arrange
            var testToken = "testToken1234";
            RestRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                mfaToken = testToken,
                authResponseSessionInfo = new SessionInfo()
            });
            RestRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                mfaToken = testToken,
                authResponseSessionInfo = new SessionInfo()
            });
            // Act
            var session = SnowflakeDbConnectionPool.ConnectionManager.GetSession(ConnectionStringMFACache, new SessionPropertiesContext());

            // Assert
            await Awaiter.WaitUntilConditionOrTimeout(() => RestRequester.LoginRequests.Count == 2, TimeSpan.FromSeconds(15));
            Assert.Equal(2, RestRequester.LoginRequests.Count);
            var loginRequest1 = RestRequester.LoginRequests.Dequeue();
            Assert.Equal(string.Empty, loginRequest1.data.Token);
            Assert.Equal(testToken, SecureStringHelper.Decode(session._mfaToken));
            Assert.True(loginRequest1.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value) && (bool)value);
            Assert.Equal("passcode", loginRequest1.data.extAuthnDuoMethod);
            var loginRequest2 = RestRequester.LoginRequests.Dequeue();
            Assert.Equal(testToken, loginRequest2.data.Token);
            Assert.True(loginRequest2.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value1) && (bool)value1);
            Assert.Equal("passcode", loginRequest2.data.extAuthnDuoMethod);
        }

        [SFFact]
        public void TestPoolManagerShouldThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.ConnectionManager.GetSession(connectionString, new SessionPropertiesContext()));
            Assert.Contains("Passcode with MinPoolSize feature of connection pool allowed only for username_password_mfa authentication", thrown.Message);
        }

        [SFFact]
        public void TestPoolManagerShouldThrowExceptionIfForcePoolingWithPasscodeAsSecureStringNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;POOLINGENABLED=true";
            var sessionContext = new SessionPropertiesContext { Passcode = SecureStringHelper.Encode("12345") };

            // Act and assert
            var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.ConnectionManager.GetSession(connectionString, sessionContext));
            Assert.Contains("Passcode with MinPoolSize feature of connection pool allowed only for username_password_mfa authentication", thrown.Message);
        }

        [SFFact]
        public void TestPoolManagerShouldNotThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=false";
            // Act and assert
            SnowflakeDbConnectionPool.ConnectionManager.GetSession(connectionString, new SessionPropertiesContext());
        }

        [SFFact]
        public void TestPoolManagerShouldNotThrowExceptionIfMinPoolSizeZeroNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=0;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            SnowflakeDbConnectionPool.ConnectionManager.GetSession(connectionString, new SessionPropertiesContext());
        }

        public void Dispose()
        {
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(nameof(ConnectionPoolManagerMFATest));
        }
    }

    class MockSessionFactoryMFA : ISessionFactory
    {
        private readonly IMockRestRequester restRequester;

        public MockSessionFactoryMFA(IMockRestRequester restRequester)
        {
            this.restRequester = restRequester;
        }

        public SFSession NewSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            return new SFSession(connectionString, sessionContext, EasyLoggingStarter.Instance, restRequester);
        }
    }
}
