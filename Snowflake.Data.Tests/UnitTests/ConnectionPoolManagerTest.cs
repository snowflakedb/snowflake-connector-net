/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
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
        private const string ConnectionString1 = "database=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;";
        private const string ConnectionString2 = "database=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;";
        private readonly SecureString _password = new SecureString();
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
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, _password);
            
            // Assert
            Assert.AreEqual(ConnectionString1, sessionPool.ConnectionString);
            Assert.AreEqual(_password, sessionPool.Password);
        }
        
        [Test]
        public void TestPoolManagerReturnsSamePoolForGivenConnectionString()
        {
            // Arrange
            var anotherConnectionString = ConnectionString1;
            
            // Act
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(anotherConnectionString, _password);
            
            // Assert
            Assert.AreEqual(sessionPool1, sessionPool2);
        }
        
        [Test]
        public void TestDifferentPoolsAreReturnedForDifferentConnectionStrings()
        {
            // Arrange
            Assert.AreNotSame(ConnectionString1, ConnectionString2);
            
            // Act
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
            
            // Assert
            Assert.AreNotSame(sessionPool1, sessionPool2);
            Assert.AreEqual(ConnectionString1, sessionPool1.ConnectionString);
            Assert.AreEqual(ConnectionString2, sessionPool2.ConnectionString);
        }
        
                
        [Test]
        public void TestGetSessionWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = _connectionPoolManager.GetSession(ConnectionString1, _password);
            
            // Assert
            Assert.AreEqual(ConnectionString1, sfSession.ConnectionString);
            Assert.AreEqual(_password, sfSession.Password);
        }

        [Test]
        public async Task TestGetSessionAsyncWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = await _connectionPoolManager.GetSessionAsync(ConnectionString1, _password, CancellationToken.None);
            
            // Assert
            Assert.AreEqual(ConnectionString1, sfSession.ConnectionString);
            Assert.AreEqual(_password, sfSession.Password);
        }

        [Test]
        public void TestCountingOfSessionProvidedByPool()
        {
            // Act
            _connectionPoolManager.GetSession(ConnectionString1, _password);
            
            // Assert
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, _password);
            Assert.AreEqual(1, sessionPool.GetCurrentPoolSize());
        }
        
        [Test]
        public void TestCountingOfSessionReturnedBackToPool()
        {
            // Arrange
            var sfSession = _connectionPoolManager.GetSession(ConnectionString1, _password);
            
            // Act
            _connectionPoolManager.AddSession(sfSession);
            
            // Assert
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, _password);
            Assert.AreEqual(1, sessionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestSetMaxPoolSizeForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, _password);

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetMaxPoolSize(3));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot not change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }
         
        [Test]
        public void TestSetTimeoutForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, _password);

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetTimeout(3000));
            
            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot not change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }         
        
        [Test]
        public void TestSetPoolingForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, _password);

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetPooling(false));
            
            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot not change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestGetPoolingOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
            sessionPool1.SetPooling(true);
            sessionPool2.SetPooling(false);
            
            // Act/Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetPooling());
            Assert.IsNotNull(exception);
            Assert.AreEqual(SFError.INCONSISTENT_RESULT_ERROR.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.IsTrue(exception.Message.Contains("Multiple pools have different Pooling values"));
        }

        [Test]
        public void TestGetPoolingOnManagerLevelWorksWhenAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
            sessionPool1.SetPooling(true);
            sessionPool2.SetPooling(true);
            
            // Act/Assert
            Assert.AreEqual(true,_connectionPoolManager.GetPooling());
        }

        [Test]
        public void TestGetTimeoutOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
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
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
            sessionPool1.SetTimeout(3600);
            sessionPool2.SetTimeout(3600);
            
            // Act/Assert
            Assert.AreEqual(3600,_connectionPoolManager.GetTimeout());
        }

        [Test]
        public void TestGetMaxPoolSizeOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
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
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, _password);
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, _password);
            sessionPool1.SetMaxPoolSize(33);
            sessionPool2.SetMaxPoolSize(33);
            
            // Act/Assert
            Assert.AreEqual(33,_connectionPoolManager.GetMaxPoolSize());
        }

        [Test]
        public void TestGetCurrentPoolSizeThrowsExceptionWhenNotAllPoolsEqual()
        {
            // Arrange
            EnsurePoolSize(ConnectionString1, 2);
            EnsurePoolSize(ConnectionString2, 3);
            
            // Act/Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetCurrentPoolSize());
            Assert.IsNotNull(exception);
            Assert.AreEqual(SFError.INCONSISTENT_RESULT_ERROR.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.IsTrue(exception.Message.Contains("Multiple pools have different Current Pool Size values"));
        }
        
        private void EnsurePoolSize(string connectionString, int requiredCurrentSize)
        {
            var sessionPool = _connectionPoolManager.GetPool(connectionString, _password);
            sessionPool.SetMaxPoolSize(requiredCurrentSize);
            for (var i = 0; i < requiredCurrentSize; i++)
            {
                _connectionPoolManager.GetSession(connectionString, _password);
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
