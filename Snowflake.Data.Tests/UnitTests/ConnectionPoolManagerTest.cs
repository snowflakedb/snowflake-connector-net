using System;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Moq;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.IntegrationTests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{

    public class ConnectionPoolManagerTest
    {
        private const string ConnectionString1 = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";
        private const string ConnectionString2 = "db=D2;warehouse=W2;account=A2;user=U2;password=P2;role=R2;minPoolSize=1;";
        private const string ConnectionStringWithoutPassword = "db=D3;warehouse=W3;account=A3;user=U3;role=R3;minPoolSize=1;";
        private readonly SecureString _password3 = SecureStringHelper.Encode("P3");

        public ConnectionPoolManagerTest()
        {
            ConnectionManagerTestsFacade.RegisterDedicatedContext(nameof(ConnectionPoolManagerTest), ConnectionPoolType.MultipleConnectionPool, new Factory());
        }

        private class Factory : IConnectionManagerFactory
        {
            public IConnectionManager CreateConnectionManager(ConnectionPoolType requestedPoolType) =>
                new ConnectionPoolManager(SessionPoolFactory, this);

            private static SessionPool SessionPoolFactory(string connectionString, SecureString password, SecureString oauthClientSecret, SecureString token)
            {
                var sessionFactory = new MockSessionFactory();
                return SessionPool.CreateSessionPool(connectionString, password, oauthClientSecret, token, sessionFactory);
            }
        }


        [SFFact]
        public void TestPoolManagerReturnsSessionPoolForGivenConnectionString()
        {
            // Act
            var sessionPool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Assert
            Assert.Equal(ConnectionString1, sessionPool.ConnectionString);
            Assert.Equal(null, sessionPool.Password);
        }

        [SFFact]
        public void TestPoolManagerReturnsSessionPoolForGivenConnectionStringAndSecurelyProvidedPassword()
        {
            // Arrange
            var sessionContext = new SessionPropertiesContext { Password = _password3 };

            // Act
            var sessionPool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(ConnectionStringWithoutPassword, sessionContext);

            // Assert
            Assert.Equal(ConnectionStringWithoutPassword, sessionPool.ConnectionString);
            Assert.Equal(_password3, sessionPool.Password);
        }

        [SFFact]
        public void TestPoolManagerThrowsWhenPasswordNotProvided()
        {
            // Act/Assert
            Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.ConnectionManager.GetPool(ConnectionStringWithoutPassword, new SessionPropertiesContext()));
        }

        [SFFact]
        public void TestPoolManagerReturnsSamePoolForGivenConnectionString()
        {
            // Arrange
            var anotherConnectionString = ConnectionString1;

            // Act
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(anotherConnectionString, new SessionPropertiesContext());

            // Assert
            Assert.Equal(sessionPool1, sessionPool2);
        }

        [SFFact]
        public void TestDifferentPoolsAreReturnedForDifferentConnectionStrings()
        {
            // Arrange
            Assert.NotSame(ConnectionString1, ConnectionString2);

            // Act
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());

            // Assert
            Assert.NotSame(sessionPool1, sessionPool2);
            Assert.Equal(ConnectionString1, sessionPool1.ConnectionString);
            Assert.Equal(ConnectionString2, sessionPool2.ConnectionString);
        }


        [SFFact]
        public void TestGetSessionWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = SnowflakeDbConnectionPool.ConnectionManager.GetSession(ConnectionString1, new SessionPropertiesContext());

            // Assert
            Assert.Equal(ConnectionString1, sfSession.ConnectionString);
            Assert.Equal(null, sfSession.PropertiesContext.Password);
        }

        [SFFact]
        public async Task TestGetSessionAsyncWorksForSpecifiedConnectionString()
        {
            // Act
            var sfSession = await SnowflakeDbConnectionPool.ConnectionManager.GetSessionAsync(ConnectionString1, new SessionPropertiesContext(), CancellationToken.None);

            // Assert
            Assert.Equal(ConnectionString1, sfSession.ConnectionString);
            Assert.Equal(null, sfSession.PropertiesContext.Password);
        }

        [SFFact]
        public void TestCountingOfSessionProvidedByPool()
        {
            // Act
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            connectionPoolManager.GetSession(ConnectionString1, new SessionPropertiesContext());

            // Assert
            var sessionPool = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            Assert.Equal(1, sessionPool.GetCurrentPoolSize());
        }

        [SFFact]
        public void TestCountingOfSessionReturnedBackToPool()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sfSession = connectionPoolManager.GetSession(ConnectionString1, new SessionPropertiesContext());

            // Act
            connectionPoolManager.AddSession(sfSession);

            // Assert
            var sessionPool = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            Assert.Equal(1, sessionPool.GetCurrentPoolSize());
        }

        [SFFact]
        public void TestSetMaxPoolSizeForAllPoolsDisabled()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Act
            var thrown = Assert.Throws<Exception>(() => connectionPoolManager.SetMaxPoolSize(3));

            // Assert
            Assert.Contains("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool", thrown.Message);
        }

        [SFFact]
        public void TestSetTimeoutForAllPoolsDisabled()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Act
            var thrown = Assert.Throws<Exception>(() => connectionPoolManager.SetTimeout(3000));

            // Assert
            Assert.Contains("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool", thrown.Message);
        }

        [SFFact]
        public void TestSetPoolingForAllPoolsDisabled()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());

            // Act
            var thrown = Assert.Throws<Exception>(() => connectionPoolManager.SetPooling(false));

            // Assert
            Assert.Contains("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool", thrown.Message);
        }

        [SFFact]
        public void TestGetPoolingOnManagerLevelAlwaysTrue()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetPooling(true);
            sessionPool2.SetPooling(false);

            // Act
            var pooling = connectionPoolManager.GetPooling();

            // Assert
            Assert.True(pooling);
            Assert.True(sessionPool1.GetPooling());
            Assert.False(sessionPool2.GetPooling());
        }

        [SFTheory]
        [InlineData("authenticator=externalbrowser;account=test;user=test;")]
        [InlineData("authenticator=snowflake_jwt;account=test;user=test;private_key_file=/some/file.key")]
        public void TestDisabledPoolingWhenSecretesProvidedExternally(string connectionString)
        {
            // act
            var pool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(connectionString, new SessionPropertiesContext());

            // assert
            Assert.False(pool.GetPooling());
        }

        [SFFact]
        public void TestGetTimeoutOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetTimeout(299);
            sessionPool2.SetTimeout(1313);

            // Act/Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => connectionPoolManager.GetTimeout());
            Assert.NotNull(exception);
            Assert.Equal(SFError.INCONSISTENT_RESULT_ERROR.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.True(exception.Message.Contains("Multiple pools have different Timeout values"));
        }

        [SFFact]
        public void TestGetTimeoutOnManagerLevelWhenAllPoolsEqual()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetTimeout(3600);
            sessionPool2.SetTimeout(3600);

            // Act/Assert
            Assert.Equal(3600, connectionPoolManager.GetTimeout());
        }

        [SFFact]
        public void TestGetMaxPoolSizeOnManagerLevelWhenNotAllPoolsEqual()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetMaxPoolSize(1);
            sessionPool2.SetMaxPoolSize(17);

            // Act/Assert
            var exception = Assert.Throws<SnowflakeDbException>(() => connectionPoolManager.GetMaxPoolSize());
            Assert.NotNull(exception);
            Assert.Equal(SFError.INCONSISTENT_RESULT_ERROR.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.True(exception.Message.Contains("Multiple pools have different Max Pool Size values"));
        }

        [SFFact]
        public void TestGetMaxPoolSizeOnManagerLevelWhenAllPoolsEqual()
        {
            // Arrange
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool1 = connectionPoolManager.GetPool(ConnectionString1, new SessionPropertiesContext());
            var sessionPool2 = connectionPoolManager.GetPool(ConnectionString2, new SessionPropertiesContext());
            sessionPool1.SetMaxPoolSize(33);
            sessionPool2.SetMaxPoolSize(33);

            // Act/Assert
            Assert.Equal(33, connectionPoolManager.GetMaxPoolSize());
        }

        [SFFact]
        public void TestGetCurrentPoolSizeReturnsSumOfPoolSizes()
        {
            // Arrange
            EnsurePoolSize(ConnectionString1, new SessionPropertiesContext(), 2);
            EnsurePoolSize(ConnectionString2, new SessionPropertiesContext(), 3);

            // act
            var poolSize = SnowflakeDbConnectionPool.ConnectionManager.GetCurrentPoolSize();

            // assert
            Assert.Equal(5, poolSize);
        }

        [SFFact]
        public void TestReturnPoolForSecurePassword()
        {
            // arrange
            const string AnotherPassword = "anotherPassword";
            var passwordContext = new SessionPropertiesContext { Password = _password3 };
            var anotherPasswordContext = new SessionPropertiesContext { Password = SecureStringHelper.Encode(AnotherPassword) };
            EnsurePoolSize(ConnectionStringWithoutPassword, passwordContext, 1);

            // act
            var pool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(ConnectionStringWithoutPassword, anotherPasswordContext); // a new pool has been created because the password is different

            // assert
            Assert.Equal(0, pool.GetCurrentPoolSize());
            Assert.Equal(AnotherPassword, SecureStringHelper.Decode(pool.Password));
        }

        [SFFact]
        public void TestReturnDifferentPoolWhenPasswordProvidedInDifferentWay()
        {
            // arrange
            var connectionStringWithPassword = $"{ConnectionStringWithoutPassword}password={SecureStringHelper.Decode(_password3)}";
            var sessionContext = new SessionPropertiesContext { Password = _password3 };
            EnsurePoolSize(ConnectionStringWithoutPassword, sessionContext, 2);
            EnsurePoolSize(connectionStringWithPassword, new SessionPropertiesContext(), 5);
            EnsurePoolSize(connectionStringWithPassword, sessionContext, 8);

            // act
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var pool1 = connectionPoolManager.GetPool(ConnectionStringWithoutPassword, sessionContext);
            var pool2 = connectionPoolManager.GetPool(connectionStringWithPassword, new SessionPropertiesContext());
            var pool3 = connectionPoolManager.GetPool(connectionStringWithPassword, sessionContext);

            // assert
            Assert.Equal(2, pool1.GetCurrentPoolSize());
            Assert.Equal(5, pool2.GetCurrentPoolSize());
            Assert.Equal(8, pool3.GetCurrentPoolSize());
        }

        [SFTheory]
        [InlineData(null)]
        [InlineData("")]
        public void TestGetPoolFailsWhenNoPasswordProvided(string password)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var sessionContext = new SessionPropertiesContext { Password = securePassword };

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SnowflakeDbConnectionPool.ConnectionManager.GetPool(ConnectionStringWithoutPassword, sessionContext));

            // assert
            Assert.Contains("Required property PASSWORD is not provided", thrown.Message);
        }

        [SFFact]
        public void TestPoolDoesNotSerializePassword()
        {
            // arrange
            var password = SecureStringHelper.Decode(_password3);
            var connectionStringWithPassword = $"{ConnectionStringWithoutPassword}password={password}";
            var sessionContext = new SessionPropertiesContext { Password = _password3 };
            var pool = SnowflakeDbConnectionPool.ConnectionManager.GetPool(connectionStringWithPassword, sessionContext);

            // act
            var serializedPool = pool.ToString();

            // assert
            Assert.False(serializedPool.Contains(password));
        }

        private void EnsurePoolSize(string connectionString, SessionPropertiesContext sessionContext, int requiredCurrentSize)
        {
            var connectionPoolManager = SnowflakeDbConnectionPool.ConnectionManager;
            var sessionPool = connectionPoolManager.GetPool(connectionString, sessionContext);
            sessionPool.SetMaxPoolSize(requiredCurrentSize);
            for (var i = 0; i < requiredCurrentSize; i++)
            {
                connectionPoolManager.GetSession(connectionString, sessionContext);
            }
            Assert.Equal(requiredCurrentSize, sessionPool.GetCurrentPoolSize());
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
