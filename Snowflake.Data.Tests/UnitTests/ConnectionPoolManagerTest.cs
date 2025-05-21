using System;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    class ConnectionPoolManagerTest
    {
        private readonly ConnectionPoolManager _connectionPoolManager = new ConnectionPoolManager();
        private const string ConnectionString1 = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";
        private const string ConnectionString2 = "db=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;minPoolSize=1;";
        private const string ConnectionStringWithoutPassword = "db=D3;warehouse=W3;account=A3;user=U3;role=R3;minPoolSize=1;";
        private readonly SecureString _password3 = SecureStringHelper.Encode("P3");
        private static PoolConfig s_poolConfig;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_poolConfig = new PoolConfig();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.MultipleConnectionPool);
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
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Assert
            Assert.AreEqual(ConnectionString1, sessionPool.ConnectionString);
            Assert.AreEqual(null, sessionPool.Password);
        }

        [Test]
        public void TestPoolManagerReturnsSessionPoolForGivenConnectionStringAndSecurelyProvidedPassword()
        {
            // Arrange
            var sessionContext = new SessionPropertiesContext { Password = _password3 };

            // Act
            var sessionPool = _connectionPoolManager.GetPool(ConnectionStringWithoutPassword, sessionContext);

            // Assert
            Assert.AreEqual(ConnectionStringWithoutPassword, sessionPool.ConnectionString);
            Assert.AreEqual(_password3, sessionPool.Password);
        }

        [Test]
        public void TestPoolManagerThrowsWhenPasswordNotProvided()
        {
            // Act/Assert
            Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetPool(ConnectionStringWithoutPassword, new SessionPropertiesContext()));
        }

        [Test]
        public void TestPoolManagerReturnsSamePoolForGivenConnectionString()
        {
            // Arrange
            var anotherConnectionString = ConnectionString1;

            // Act
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(anotherConnectionString, new SessionPropertiesContext());

            // Assert
            Assert.AreEqual(sessionPool1, sessionPool2);
        }

        [Test]
        public void TestDifferentPoolsAreReturnedForDifferentConnectionStrings()
        {
            // Arrange
            Assert.AreNotSame(ConnectionString1, ConnectionString2);

            // Act
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());

            // Assert
            Assert.AreNotSame(sessionPool1, sessionPool2);
            Assert.AreEqual(ConnectionString1, sessionPool1.ConnectionString);
            Assert.AreEqual(ConnectionString2, sessionPool2.ConnectionString);
        }


        [Test]
        public void TestGetSessionWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = _connectionPoolManager.GetSession(ConnectionString1, new SessionPropertiesContext());

            // Assert
            Assert.AreEqual(ConnectionString1, sfSession.ConnectionString);
            Assert.AreEqual(null, sfSession.PropertiesContext.Password);
        }

        [Test]
        public async Task TestGetSessionAsyncWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = await _connectionPoolManager.GetSessionAsync(ConnectionString1, new SessionPropertiesContext(), CancellationToken.None);

            // Assert
            Assert.AreEqual(ConnectionString1, sfSession.ConnectionString);
            Assert.AreEqual(null, sfSession.PropertiesContext.Password);
        }

        [Test]
        public void TestCountingOfSessionProvidedByPool()
        {
            // Act
            _connectionPoolManager.GetSession(ConnectionString1, new SessionPropertiesContext());

            // Assert
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            Assert.AreEqual(1, sessionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestCountingOfSessionReturnedBackToPool()
        {
            // Arrange
            var sfSession = _connectionPoolManager.GetSession(ConnectionString1, new SessionPropertiesContext());

            // Act
            _connectionPoolManager.AddSession(sfSession);

            // Assert
            var sessionPool = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            Assert.AreEqual(1, sessionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestSetMaxPoolSizeForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetMaxPoolSize(3));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestSetTimeoutForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetTimeout(3000));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestSetPoolingForAllPoolsDisabled()
        {
            // Arrange
            _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Act
            var thrown = Assert.Throws<Exception>(() => _connectionPoolManager.SetPooling(false));

            // Assert
            Assert.That(thrown.Message, Does.Contain("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool"));
        }

        [Test]
        public void TestGetPoolingOnManagerLevelAlwaysTrue()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
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
        [TestCase("authenticator=externalbrowser;account=test;user=test;")]
        [TestCase("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key")]
        public void TestDisabledPoolingWhenSecretesProvidedExternally(string connectionString)
        {
            // act
            var pool = _connectionPoolManager.GetPool(connectionString, new SessionPropertiesContext());

            // assert
            Assert.IsFalse(pool.GetPooling());
        }

        [Test]
        public void TestGetTimeoutOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
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
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetTimeout(3600);
            sessionPool2.SetTimeout(3600);

            // Act/Assert
            Assert.AreEqual(3600, _connectionPoolManager.GetTimeout());
        }

        [Test]
        public void TestGetMaxPoolSizeOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
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
            var sessionPool1 = _connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = _connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetMaxPoolSize(33);
            sessionPool2.SetMaxPoolSize(33);

            // Act/Assert
            Assert.AreEqual(33, _connectionPoolManager.GetMaxPoolSize());
        }

        [Test]
        public void TestGetCurrentPoolSizeReturnsSumOfPoolSizes()
        {
            // Arrange
            EnsurePoolSize(ConnectionString1, new SessionPropertiesContext(), 2);
            EnsurePoolSize(ConnectionString2, new SessionPropertiesContext(), 3);

            // act
            var poolSize = _connectionPoolManager.GetCurrentPoolSize();

            // assert
            Assert.AreEqual(5, poolSize);
        }

        [Test]
        public void TestReturnPoolForSecurePassword()
        {
            // arrange
            const string AnotherPassword = "anotherPassword";
            var passwordContext = new SessionPropertiesContext { Password = _password3 };
            var anotherPasswordContext = new SessionPropertiesContext { Password = SecureStringHelper.Encode(AnotherPassword) };
            EnsurePoolSize(ConnectionStringWithoutPassword, passwordContext, 1);

            // act
            var pool = _connectionPoolManager.GetPool(ConnectionStringWithoutPassword, anotherPasswordContext); // a new pool has been created because the password is different

            // assert
            Assert.AreEqual(0, pool.GetCurrentPoolSize());
            Assert.AreEqual(AnotherPassword, SecureStringHelper.Decode(pool.Password));
        }

        [Test]
        public void TestReturnDifferentPoolWhenPasswordProvidedInDifferentWay()
        {
            // arrange
            var connectionStringWithPassword = $"{ConnectionStringWithoutPassword}password={SecureStringHelper.Decode(_password3)}";
            var sessionContext = new SessionPropertiesContext { Password = _password3 };
            EnsurePoolSize(ConnectionStringWithoutPassword, sessionContext, 2);
            EnsurePoolSize(connectionStringWithPassword, new SessionPropertiesContext(), 5);
            EnsurePoolSize(connectionStringWithPassword, sessionContext, 8);

            // act
            var pool1 = _connectionPoolManager.GetPool(ConnectionStringWithoutPassword, sessionContext);
            var pool2 = _connectionPoolManager.GetPool(connectionStringWithPassword, new SessionPropertiesContext());
            var pool3 = _connectionPoolManager.GetPool(connectionStringWithPassword, sessionContext);

            // assert
            Assert.AreEqual(2, pool1.GetCurrentPoolSize());
            Assert.AreEqual(5, pool2.GetCurrentPoolSize());
            Assert.AreEqual(8, pool3.GetCurrentPoolSize());
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        public void TestGetPoolFailsWhenNoPasswordProvided(string password)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var sessionContext = new SessionPropertiesContext { Password = securePassword };

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => _connectionPoolManager.GetPool(ConnectionStringWithoutPassword, sessionContext));

            // assert
            Assert.That(thrown.Message, Does.Contain("Required property PASSWORD is not provided"));
        }

        [Test]
        public void TestPoolDoesNotSerializePassword()
        {
            // arrange
            var password = SecureStringHelper.Decode(_password3);
            var connectionStringWithPassword = $"{ConnectionStringWithoutPassword}password={password}";
            var sessionContext = new SessionPropertiesContext { Password = _password3 };
            var pool = _connectionPoolManager.GetPool(connectionStringWithPassword, sessionContext);

            // act
            var serializedPool = pool.ToString();

            // assert
            Assert.IsFalse(serializedPool.Contains(password));
        }

        private void EnsurePoolSize(string connectionString, SessionPropertiesContext sessionContext, int requiredCurrentSize)
        {
            var sessionPool = _connectionPoolManager.GetPool(connectionString, sessionContext);
            sessionPool.SetMaxPoolSize(requiredCurrentSize);
            for (var i = 0; i < requiredCurrentSize; i++)
            {
                _connectionPoolManager.GetSession(connectionString, sessionContext);
            }
            Assert.AreEqual(requiredCurrentSize, sessionPool.GetCurrentPoolSize());
        }
    }

    class MockSessionFactory : ISessionFactory
    {
        public SFSession NewSession(string connectionString, SessionPropertiesContext sessionContext)
        {
            var mockSfSession = new Mock<SFSession>(connectionString, sessionContext, EasyLoggingStarter.Instance);
            mockSfSession.Setup(x => x.Open()).Verifiable();
            mockSfSession.Setup(x => x.OpenAsync(default)).Returns(Task.FromResult(this));
            mockSfSession.Setup(x => x.IsNotOpen()).Returns(false);
            mockSfSession.Setup(x => x.IsExpired(It.IsAny<TimeSpan>(), It.IsAny<long>())).Returns(false);
            return mockSfSession.Object;
        }
    }
}
