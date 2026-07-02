using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Mono.Unix;
using Moq;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Configuration;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [CollectionDefinition(nameof(SnowflakeDbConnectionTestFixture), DisableParallelization = true)]
    public sealed class SnowflakeDbConnectionTestFixture { }

    [Collection(nameof(SnowflakeDbConnectionTestFixture))]
    public class SnowflakeDbConnectionTest
    {
        [SFFact]
        public void TestFillConnectionStringFromTomlConfig()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockFacade = new Mock<IEnvironmentFacade>();
            mockFacade.Setup(x => x.GetString(EnvVars.DefaultConnectionName)).Returns(EnvVars.DefaultConnectionName.DefaultValue);
            mockFacade.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.IsAny<string>(), It.IsAny<Action<UnixStream>>()))
                .Returns("[default]\naccount=\"testaccount\"\nuser=\"testuser\"\npassword=\"testpassword\"\n");
            var tomlConnectionBuilder = new TomlConnectionBuilder(mockFileOperations.Object, mockFacade.Object);

            // Act
            using (var conn = new SnowflakeDbConnection(tomlConnectionBuilder))
            {
                conn.FillConnectionStringFromTomlConfigIfNotSet();
                // Assert
                Assert.Equal("account=testaccount;user=testuser;password=testpassword;", conn.ConnectionString);
            }
        }

        [SFFact]
        public void TestTomlConfigurationDoesNotOverrideExistingConnectionString()
        {
            // Arrange
            var connectionTest = "account=user1account;user=user1;password=user1password;";
            var mockFileOperations = new Mock<FileOperations>();
            var mockFacade = new Mock<IEnvironmentFacade>();
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.IsAny<string>()))
                .Returns("[default]\naccount=\"testaccount\"\nuser=\"testuser\"\npassword=\"testpassword\"\n");
            var tomlConnectionBuilder = new TomlConnectionBuilder(mockFileOperations.Object, mockFacade.Object);

            // Act
            using (var conn = new SnowflakeDbConnection(tomlConnectionBuilder))
            {
                conn.ConnectionString = connectionTest;
                conn.FillConnectionStringFromTomlConfigIfNotSet();
                // Assert
                Assert.Equal(connectionTest, conn.ConnectionString);
            }
        }

        [SFFact]
        public void TestUseConfigurationProvidedOutsideOfConnectionString()
        {
            // arrange
            var connectionManager = new Mock<IConnectionManager>();
            var oldConnectionManager = SnowflakeDbConnectionPool.ReplaceConnectionManager(connectionManager.Object);
            try
            {
                var connectionString = "account=user1account;user=user1;";
                var password = "testpassword";
                var passcode = "testpasscode";
                var oauthClientSecret = "testoauthclientsecret";
                var token = "testtoken";
                var sessionProperties = new SessionPropertiesContext
                {
                    Password = SecureStringHelper.Encode(password),
                    Passcode = SecureStringHelper.Encode(passcode),
                    OAuthClientSecret = SecureStringHelper.Encode(oauthClientSecret),
                    Token = SecureStringHelper.Encode(token)
                };
                connectionManager
                    .Setup(m => m.GetSession(connectionString, It.IsAny<SessionPropertiesContext>()))
                    .Returns(new SFSession(connectionString, sessionProperties));

                using (var connection = new SnowflakeDbConnection(connectionString))
                {
                    connection.Password = SecureStringHelper.Encode(password);
                    connection.Passcode = SecureStringHelper.Encode(passcode);
                    connection.OAuthClientSecret = SecureStringHelper.Encode(oauthClientSecret);
                    connection.Token = SecureStringHelper.Encode(token);

                    // act
                    connection.Open();

                    // assert
                    connectionManager.Verify(m => m.GetSession(connectionString,
                        It.Is<SessionPropertiesContext>(context =>
                            SecureStringHelper.Decode(context.Password) == password &&
                            SecureStringHelper.Decode(context.Passcode) == passcode &&
                            SecureStringHelper.Decode(context.OAuthClientSecret) == oauthClientSecret &&
                            SecureStringHelper.Decode(context.Token) == token)));
                }
            }
            finally
            {
                SnowflakeDbConnectionPool.ReplaceConnectionManager(oldConnectionManager);
            }
        }

        [SFFact]
        public void TestUseConfigurationProvidedOutsideOfConnectionStringAsync()
        {
            // arrange
            var connectionManager = new Mock<IConnectionManager>();
            var oldConnectionManager = SnowflakeDbConnectionPool.ReplaceConnectionManager(connectionManager.Object);
            try
            {
                var connectionString = "account=user1account;user=user1;";
                var password = "testpassword";
                var passcode = "testpasscode";
                var oauthClientSecret = "testoauthclientsecret";
                var token = "testtoken";
                var sessionProperties = new SessionPropertiesContext
                {
                    Password = SecureStringHelper.Encode(password),
                    Passcode = SecureStringHelper.Encode(passcode),
                    OAuthClientSecret = SecureStringHelper.Encode(oauthClientSecret),
                    Token = SecureStringHelper.Encode(token)
                };
                connectionManager
                    .Setup(m => m.GetSessionAsync(connectionString, It.IsAny<SessionPropertiesContext>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(new SFSession(connectionString, sessionProperties)));

                using (var connection = new SnowflakeDbConnection(connectionString))
                {
                    connection.Password = SecureStringHelper.Encode(password);
                    connection.Passcode = SecureStringHelper.Encode(passcode);
                    connection.OAuthClientSecret = SecureStringHelper.Encode(oauthClientSecret);
                    connection.Token = SecureStringHelper.Encode(token);

                    // act
                    connection.OpenAsync(CancellationToken.None).Wait();

                    // assert
                    connectionManager.Verify(m => m.GetSessionAsync(connectionString,
                        It.Is<SessionPropertiesContext>(context =>
                            SecureStringHelper.Decode(context.Password) == password &&
                            SecureStringHelper.Decode(context.Passcode) == passcode &&
                            SecureStringHelper.Decode(context.OAuthClientSecret) == oauthClientSecret &&
                            SecureStringHelper.Decode(context.Token) == token),
                        It.IsAny<CancellationToken>()));
                }
            }
            finally
            {
                SnowflakeDbConnectionPool.ReplaceConnectionManager(oldConnectionManager);
            }
        }

        [SFFact]
        public void TestCloseWhenStateIsBrokenWithSessionTransitionsToClosed()
        {
            // Arrange
            var connectionManager = new Mock<IConnectionManager>();
            connectionManager.Setup(m => m.GetPooling()).Returns(false);
            connectionManager.Setup(m => m.ReleaseBusySession(It.IsAny<SFSession>()));
            var oldConnectionManager = SnowflakeDbConnectionPool.ReplaceConnectionManager(connectionManager.Object);
            try
            {
                var conn = new SnowflakeDbConnection();
                conn.ConnectionString = "account=test;user=test;password=test;";
                conn.SfSession = new SFSession("account=test;user=test;password=test;", new SessionPropertiesContext());
                conn._connectionState = ConnectionState.Broken;

                // Act
                conn.Close();

                // Assert
                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Null(conn.SfSession);
            }
            finally
            {
                SnowflakeDbConnectionPool.ReplaceConnectionManager(oldConnectionManager);
            }
        }

        [SFFact]
        public async Task TestCloseAsyncWhenStateIsBrokenWithSessionTransitionsToClosed()
        {
            // Arrange
            var connectionManager = new Mock<IConnectionManager>();
            connectionManager.Setup(m => m.GetPooling()).Returns(false);
            connectionManager.Setup(m => m.ReleaseBusySession(It.IsAny<SFSession>()));
            var oldConnectionManager = SnowflakeDbConnectionPool.ReplaceConnectionManager(connectionManager.Object);
            try
            {
                var conn = new SnowflakeDbConnection();
                conn.ConnectionString = "account=test;user=test;password=test;";
                conn.SfSession = new SFSession("account=test;user=test;password=test;", new SessionPropertiesContext());
                conn._connectionState = ConnectionState.Broken;

                // Act
                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);

                // Assert
                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Null(conn.SfSession);
            }
            finally
            {
                SnowflakeDbConnectionPool.ReplaceConnectionManager(oldConnectionManager);
            }
        }

        [SFFact]
        public void TestCloseWhenStateIsConnectingDoesNotChangeState()
        {
            // Arrange
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = "account=test;user=test;password=test;";
            conn.SfSession = new SFSession("account=test;user=test;password=test;", new SessionPropertiesContext());
            conn._connectionState = ConnectionState.Connecting;

            // Act
            conn.Close();

            // Assert
            Assert.Equal(ConnectionState.Connecting, conn.State);
            Assert.NotNull(conn.SfSession);
        }

        [SFFact]
        public async Task TestCloseAsyncWhenStateIsConnectingDoesNotChangeState()
        {
            // Arrange
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = "account=test;user=test;password=test;";
            conn.SfSession = new SFSession("account=test;user=test;password=test;", new SessionPropertiesContext());
            conn._connectionState = ConnectionState.Connecting;

            // Act
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            // Assert
            Assert.Equal(ConnectionState.Connecting, conn.State);
            Assert.NotNull(conn.SfSession);
        }

        [SFFact]
        public void TestCloseWhenAlreadyClosedIsIdempotent()
        {
            // Arrange
            var conn = new SnowflakeDbConnection();
            conn._connectionState = ConnectionState.Closed;

            // Act & Assert - no exception thrown
            conn.Close();

            Assert.Equal(ConnectionState.Closed, conn.State);
            Assert.Null(conn.SfSession);
        }

        [SFFact]
        public async Task TestCloseAsyncWhenCancelledDoesNotChangeState()
        {
            // Arrange
            var mockRestRequester = new MockCloseHangingRestRequester();
            var conn = new MockSnowflakeDbConnection(mockRestRequester);
            conn.ConnectionString = "account=test;user=test;password=test;";
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);

            using var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromMilliseconds(100));

            // Act & Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.CloseAsync(cts.Token)).ConfigureAwait(false);

            Assert.Equal(ConnectionState.Open, conn.State);
            Assert.NotNull(conn.SfSession);
        }

        [SFFact]
        public void TestCloseWhenSessionCloseThrowsSetsStateToBroken()
        {
            // Arrange
            var mockRestRequester = new MockCloseSessionException();
            var conn = new MockSnowflakeDbConnection(mockRestRequester);
            conn.ConnectionString = "account=test;user=test;password=test;";
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);

            // Act & Assert
            var ex = Assert.ThrowsAny<Exception>(conn.Close);
            Assert.True( ex is SnowflakeDbException or AggregateException { InnerException: SnowflakeDbException },
                $"Expected SnowflakeDbException but got {ex.GetType().Name}");

            Assert.Equal(ConnectionState.Broken, conn.State);
        }
    }
}
