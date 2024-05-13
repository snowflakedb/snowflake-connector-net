/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Net;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class ConnectionPoolManagerTest
    {
        private readonly ConnectionPoolManager _connectionPoolManager = new ConnectionPoolManager();
        private const string ConnectionString1 = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";
        private const string ConnectionString2 = "db=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;minPoolSize=1;";
        private const string ConnectionString3 = "db=D3;warehouse=W3;account=A3;user=U3;role=R3;minPoolSize=1;";
        private readonly SecureString _password1 = null;
        private readonly SecureString _password2 = null;
        private readonly SecureString _password3 = new NetworkCredential("", "P3").SecurePassword;
        private static PoolConfig s_poolConfig;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_poolConfig = new PoolConfig();
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
            SessionPool.SessionFactory = new MockSessionFactory();
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
        public void TestPoolManagerReturnsSessionPoolForGivenConnectionString()
        {
            // Act
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, _password1);

            // Assert
            Assert.AreEqual(ConnectionString1, sessionPool.ConnectionString);
            Assert.AreEqual(_password1, sessionPool.Password);
        }

        [Test]
        public void TestPoolManagerReturnsSessionPoolForGivenConnectionStringAndSecurelyProvidedPassword()
        {
            // Act
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString3, _password3);

            // Assert
            Assert.AreEqual(ConnectionString3, sessionPool.ConnectionString);
            Assert.AreEqual(_password3, sessionPool.Password);
        }

        [Test]
        public void TestPoolManagerThrowsWhenPasswordNotProvided()
        {
            // Act/Assert
            Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetPool(ConnectionString3, null));
        }

        [Test]
        public void TestPoolManagerReturnsSamePoolForGivenConnectionString()
        {
            // Arrange
            var anotherConnectionString = ConnectionString1;

            // Act
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(anotherConnectionString, _password1);

            // Assert
            Assert.AreEqual(sessionPool1, sessionPool2);
        }

        [Test]
        public void TestDifferentPoolsAreReturnedForDifferentConnectionStrings()
        {
            // Arrange
            Assert.AreNotSame(ConnectionString1, ConnectionString2);

            // Act
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password2);

            // Assert
            Assert.AreNotSame(sessionPool1, sessionPool2);
            Assert.AreEqual(ConnectionString1, sessionPool1.ConnectionString);
            Assert.AreEqual(ConnectionString2, sessionPool2.ConnectionString);
        }


        [Test]
        public void TestGetSessionWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = _connectionPoolManager.GetSession(ConnectionString1, _password1);

            // Assert
            Assert.AreEqual(ConnectionString1, sfSession.ConnectionString);
            Assert.AreEqual(_password1, sfSession.Password);
        }

        [Test]
        public async Task TestGetSessionAsyncWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = await _connectionPoolManager.GetSessionAsync(ConnectionString1, _password1, CancellationToken.None);

            // Assert
            Assert.AreEqual(ConnectionString1, sfSession.ConnectionString);
            Assert.AreEqual(_password1, sfSession.Password);
        }

        [Test]
        public void TestCountingOfSessionProvidedByPool()
        {
            // Act
            _connectionPoolManager.GetSession(ConnectionString1, _password1);

            // Assert
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            Assert.AreEqual(1, sessionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestCountingOfSessionReturnedBackToPool()
        {
            // Arrange
            var sfSession = _connectionPoolManager.GetSession(ConnectionString1, _password1);

            // Act
            _connectionPoolManager.AddSession(sfSession);

            // Assert
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            Assert.AreEqual(1, sessionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestSetMaxPoolSizeForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, _password1);

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetMaxPoolSize(3));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestSetTimeoutForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, _password1);

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetTimeout(3000));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestSetPoolingForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, _password1);

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetPooling(false));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestGetPoolingOnManagerLevelAlwaysTrue()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password2);
            sessionPool1.SetPooling(true);
            sessionPool2.SetPooling(false);

            // Act
            var pooling = _connectionPoolManager.GetPooling();

            // Assert
            Assert.IsTrue(pooling);
            Assert.IsTrue(sessionPool1.GetPooling());
            Assert.IsFalse(sessionPool2.GetPooling());
        }

        [Test]
        public void TestGetTimeoutOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password2);
            sessionPool1.SetTimeout(299);
            sessionPool2.SetTimeout(1313);

            // Act/Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetTimeout());
            Assert.IsNotNull(exception);
            Assert.AreEqual(SFError.INCONSISTENT_RESULT_ERROR.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.IsTrue(exception.Message.Contains("Multiple pools have different Timeout values"));
        }

        [Test]
        public void TestGetTimeoutOnManagerLevelWhenAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password2);
            sessionPool1.SetTimeout(3600);
            sessionPool2.SetTimeout(3600);

            // Act/Assert
            Assert.AreEqual(3600,_connectionPoolManager.GetTimeout());
        }

        [Test]
        public void TestGetMaxPoolSizeOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password2);
            sessionPool1.SetMaxPoolSize(1);
            sessionPool2.SetMaxPoolSize(17);

            // Act/Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetMaxPoolSize());
            Assert.IsNotNull(exception);
            Assert.AreEqual(SFError.INCONSISTENT_RESULT_ERROR.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.IsTrue(exception.Message.Contains("Multiple pools have different Max Pool Size values"));
        }

        [Test]
        public void TestGetMaxPoolSizeOnManagerLevelWhenAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password1);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password2);
            sessionPool1.SetMaxPoolSize(33);
            sessionPool2.SetMaxPoolSize(33);

            // Act/Assert
            Assert.AreEqual(33,_connectionPoolManager.GetMaxPoolSize());
        }

        [Test]
        public void TestGetCurrentPoolSizeReturnsSumOfPoolSizes()
        {
            // Arrange
            EnsurePoolSize(ConnectionString1, _password1, 2);
            EnsurePoolSize(ConnectionString2, _password2, 3);

            // act
            var poolSize = _connectionPoolManager.GetCurrentPoolSize();

            // assert
            Assert.AreEqual(5, poolSize);
        }

        private void EnsurePoolSize(string connectionString, SecureString password, int requiredCurrentSize)
        {
            var sessionPool = _connectionPoolManager.GetPool(connectionString, password);
            sessionPool.SetMaxPoolSize(requiredCurrentSize);
            for (var i = 0; i < requiredCurrentSize; i++)
            {
                _connectionPoolManager.GetSession(connectionString, password);
            }
            Assert.AreEqual(requiredCurrentSize, sessionPool.GetCurrentPoolSize());
        }
    }

    class MockSessionFactory : ISessionFactory
    {
        public SFSession NewSession(string connectionString, SecureString password)
        {
            var mockSfSession = new Mock<SFSession>(connectionString, password);
            mockSfSession.Setup(x => x.Open()).Verifiable();
            mockSfSession.Setup(x => x.OpenAsync(default)).Returns(Task.FromResult(this));
            mockSfSession.Setup(x => x.IsNotOpen()).Returns(false);
            mockSfSession.Setup(x => x.IsExpired(It.IsAny<TimeSpan>(), It.IsAny<long>())).Returns(false);
            return mockSfSession.Object;
        }
    }
}
