

using System;
using System.IO;
using Mono.Unix;

namespace Snowflake.Data.Tests.UnitTests
{
    using Core;
    using Core.Tools;
    using Moq;
    using NUnit.Framework;
    using Snowflake.Data.Client;

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
    }
}
