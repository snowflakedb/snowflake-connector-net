/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */



namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using System.Linq;
    using System.Security;
    using System.Threading;
    using Mock;
    using NUnit.Framework;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.Session;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core.Tools;
    using Snowflake.Data.Tests.Util;

    [TestFixture]
    class ConnectionPoolManagerMFATest
    {
        private readonly ConnectionPoolManager _connectionPoolManager = new ConnectionPoolManager();
        private const string ConnectionStringMFACache = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;authenticator=username_password_mfa";
        private const string ConnectionStringMFABasicWithoutPasscode = "db=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;minPoolSize=3;";
        private static PoolConfig s_poolConfig;
        private static MockLoginMFATokenCacheRestRequester s_restRequester;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_poolConfig = new PoolConfig();
            s_restRequester = new MockLoginMFATokenCacheRestRequester();
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
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
            var session = _connectionPoolManager.GetSession(ConnectionStringMFACache, null);
            Thread.Sleep(3000);

            // Assert

            Assert.AreEqual(2, s_restRequester.LoginRequests.Count);
            var loginRequest1 = s_restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(loginRequest1.data.Token, string.Empty);
            Assert.AreEqual(SecureStringHelper.Decode(session._mfaToken), testToken);
            Assert.IsTrue(loginRequest1.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value) && (bool)value);
            Assert.AreEqual("passcode", loginRequest1.data.extAuthnDuoMethod);
            var loginRequest2 = s_restRequester.LoginRequests.Dequeue();
            Assert.AreEqual(loginRequest2.data.Token, testToken);
            Assert.IsTrue(loginRequest2.data.SessionParameters.TryGetValue(SFSessionParameter.CLIENT_REQUEST_MFA_TOKEN, out var value1) && (bool)value1);
            Assert.AreEqual("passcode", loginRequest1.data.extAuthnDuoMethod);
        }

        [Test]
        public void TestPoolManagerShouldOnlyUsePasscodeAsArgumentForFirstSessionWhenNotUsingMFAAuthenticator()
        {
            // Arrange
            const string TestPasscode = "123456";
            s_restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                authResponseSessionInfo = new SessionInfo()
            });
            s_restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                authResponseSessionInfo = new SessionInfo()
            });
            s_restRequester.LoginResponses.Enqueue(new LoginResponseData()
            {
                authResponseSessionInfo = new SessionInfo()
            });
            // Act
            var session = _connectionPoolManager.GetSession(ConnectionStringMFABasicWithoutPasscode, null, SecureStringHelper.Encode(TestPasscode));
            Thread.Sleep(3000);

            // Assert

            Assert.AreEqual(3, s_restRequester.LoginRequests.Count);
            var request = s_restRequester.LoginRequests.ToList();
            Assert.AreEqual(1, request.Count(r => r.data.extAuthnDuoMethod == "passcode" && r.data.passcode == TestPasscode));
            Assert.AreEqual(2, request.Count(r => r.data.extAuthnDuoMethod == "push" && r.data.passcode == null));
        }

        [Test]
        public void TestPoolManagerShouldThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            var thrown = Assert.Throws<Exception>(() =>_connectionPoolManager.GetSession(connectionString, null));
            Assert.That(thrown.Message, Does.Contain("Could not use connection pool because passcode was provided using a different authenticator than username_password_mfa"));
        }

        [Test]
        public void TestPoolManagerShouldNotThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=false";
            // Act and assert
            Assert.DoesNotThrow(() =>_connectionPoolManager.GetSession(connectionString, null));
        }

        [Test]
        public void TestPoolManagerShouldNotThrowExceptionIfMinPoolSizeZeroNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=0;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            Assert.DoesNotThrow(() =>_connectionPoolManager.GetSession(connectionString, null));
        }
    }

    class MockSessionFactoryMFA : ISessionFactory
    {
        private readonly IMockRestRequester restRequester;

        public MockSessionFactoryMFA(IMockRestRequester restRequester)
        {
            this.restRequester = restRequester;
        }

        public SFSession NewSession(string connectionString, SecureString password, SecureString passcode)
        {
            return new SFSession(connectionString, password, passcode, EasyLoggingStarter.Instance, restRequester);
        }
    }
}
