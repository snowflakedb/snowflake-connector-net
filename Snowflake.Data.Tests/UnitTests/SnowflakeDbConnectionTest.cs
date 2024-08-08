

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
            mockEnvironmentOperations.Setup(e => e.GetEnvironmentVariable(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string v, string d) => d);
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.IsAny<string>()))
                .Returns("[default]\naccount=\"testaccount\"\nuser=\"testuser\"\npassword=\"testpassword\"\n");
            var tomlConnectionBuilder = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            using (var conn = new SnowflakeDbConnection(tomlConnectionBuilder))
            {
                conn.ConnectionString = "account=user1account;user=user1;password=user1password;";
                conn.FillConnectionStringFromTomlConfigIfNotSet();
                // Assert
                Assert.AreNotEqual("account=testaccount;user=testuser;password=testpassword;", conn.ConnectionString);
                Assert.AreNotEqual("account=testaccount;user=testuser;password=testpassword;", conn.ConnectionString);
            }
        }

        [Test]
        public void TestFillConnectionStringFromTomlConfigShouldNotBeExecutedIfAlreadySetConnectionString()
        {
            // Arrange
            var connectionTest = "account=user1account;user=user1;password=user1password;";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetEnvironmentVariable(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string v, string d) => d);
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.IsAny<string>()))
                .Returns("[default]\naccount=\"testaccount\"\nuser=\"testuser\"\npassword=\"testpassword\"\n");
            var tomlConnectionBuilder = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

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
