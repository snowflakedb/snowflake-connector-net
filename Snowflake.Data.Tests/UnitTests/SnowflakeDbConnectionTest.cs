using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Mono.Unix;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests
{

    [TestFixture, NonParallelizable]
    public class SnowflakeDbConnectionTest
    {
        [Test]
        public void TestFillConnectionStringFromTomlConfig()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.IsAny<string>(), It.IsAny<Action<UnixStream>>()))
                .Returns("[default]\naccount=\"testaccount\"\nuser=\"testuser\"\npassword=\"testpassword\"\n");
            var tomlConnectionBuilder = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            using (var conn = new SnowflakeDbConnection(tomlConnectionBuilder))
            {
                conn.FillConnectionStringFromTomlConfigIfNotSet();
                // Assert
                Assert.AreEqual("account=testaccount;user=testuser;password=testpassword;", conn.ConnectionString);
            }
        }

        [Test]
        public void TestTomlConfigurationDoesNotOverrideExistingConnectionString()
        {
            // Arrange
            var connectionTest = "account=user1account;user=user1;password=user1password;";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.IsAny<string>()))
                .Returns("[default]\naccount=\"testaccount\"\nuser=\"testuser\"\npassword=\"testpassword\"\n");
            var tomlConnectionBuilder = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            using (var conn = new SnowflakeDbConnection(tomlConnectionBuilder))
            {
                conn.ConnectionString = connectionTest;
                conn.FillConnectionStringFromTomlConfigIfNotSet();
                // Assert
                Assert.AreEqual(connectionTest, conn.ConnectionString);
            }
        }

        [Test]
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

        [Test]
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
    }
}
