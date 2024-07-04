/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

using System.Security;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using Mock;

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
        public void TestPoolManagerShouldThrowExceptionIfForcePoolingWithPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;POOLINGENABLED=true";
            // Act and assert
            var thrown = Assert.Throws<Exception>(() =>_connectionPoolManager.GetSession(connectionString, null));
            Assert.That(thrown.Message, Does.Contain("Could not get a pool because passcode was provided using a different authenticator than username_password_mfa"));
        }

        [Test]
        public void TestPoolManagerShouldDisablePoolingWhenPassingPasscodeNotUsingMFATokenCacheAuthenticator()
        {
            // Arrange
            var connectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=2;passcode=12345;";
            var pool = _connectionPoolManager.GetPool(connectionString);
            // Act
            var session = _connectionPoolManager.GetSession(connectionString, null);

            // Asssert
            // TODO: Review pool config is not the same for session and session pool
            // Assert.IsFalse(session.GetPooling());
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
            Assert.IsFalse(pool.GetPooling());

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
            return new SFSession(connectionString, password, passcode, restRequester);
        }
    }
}
