namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using Mock;
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.Session;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core.Tools;
    using Snowflake.Data.Tests.Util;

    [TestFixture, NonParallelizable]
    class ConnectionPoolManagerMFATest
    {
        private readonly ConnectionPoolManager _connectionPoolManager = new ConnectionPoolManager();
        private const string ConnectionStringMFACache = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;authenticator=username_password_mfa";
        private static PoolConfig s_poolConfig;
        private static MockLoginMFATokenCacheRestRequester s_restRequester;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_poolConfig = new PoolConfig();
            s_restRequester = new MockLoginMFATokenCacheRestRequester();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SessionPool.SessionFactory = new MockSessionFactoryMFA(s_restRequester);
        }

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            s_poolConfig.Reset();
            SessionPool.SessionFactory = new SessionFactory();
        }

        [SetUp]
        public void BeforeEach()
        {
            _connectionPoolManager.ClearAllPools();
            s_restRequester.Reset();
        }

        [Test]
        public void TestPoolManagerReturnsSessionPoolForGivenConnectionStringUsingMFA()
        {
            // Arrange
            var testToken = "testToken1234";
            s_restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                mfaToken = testToken,
                authResponseSessionInfo = new SessionInfo()
            });
            s_restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                mfaToken = testToken,
                authResponseSessionInfo = new SessionInfo()
            });
            // Act
            var session = _connectionPoolManager.GetSession(ConnectionStringMFACache, new SessionPropertiesContext());

            // Assert
            Awaiter.WaitUntilConditionOrTimeout(() => s_restRequester.LoginRequests.Count == 2, TimeSpan.FromSeconds(15));
            Assert.AreEqual(2, s_restRequester.LoginRequests.Count);
            var loginRequest1 = s_restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(string.Empty, loginRequest1.data.Token);
            Assert.AreEqual(testToken, SecureStringHelper.Decode(session._mfaToken));
            Assert.IsTrue(loginRequest1.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value) && (bool)value);
            Assert.AreEqual("passcode", loginRequest1.data.extAuthnDuoMethod);
            var loginRequest2 = s_restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(testToken, loginRequest2.data.Token);
            Assert.IsTrue(loginRequest2.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value1) && (bool)value1);
            Assert.AreEqual("passcode", loginRequest2.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestPoolManagerShouldThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            var thrown = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetSession(connectionString, new SessionPropertiesContext()));
            Assert.That(thrown.Message, Does.Contain("Passcode with MinPoolSize feature of connection pool allowed only for username_password_mfa authentication"));
        }

        [Test]
        public void TestPoolManagerShouldThrowExceptionIfForcePoolingWithPasscodeAsSecureStringNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;POOLINGENABLED=true";
            var sessionContext = new SessionPropertiesContext { Passcode = SecureStringHelper.Encode("12345") };

            // Act and assert
            var thrown = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetSession(connectionString, sessionContext));
            Assert.That(thrown.Message, Does.Contain("Passcode with MinPoolSize feature of connection pool allowed only for username_password_mfa authentication"));
        }

        [Test]
        public void TestPoolManagerShouldNotThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=false";
            // Act and assert
            Assert.DoesNotThrow(() => _connectionPoolManager.GetSession(connectionString, new SessionPropertiesContext()));
        }

        [Test]
        public void TestPoolManagerShouldNotThrowExceptionIfMinPoolSizeZeroNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=0;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            Assert.DoesNotThrow(() => _connectionPoolManager.GetSession(connectionString, new SessionPropertiesContext()));
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
