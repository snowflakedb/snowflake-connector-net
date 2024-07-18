/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using Core.Tools;
    using Moq;
    using NUnit.Framework;
    using Snowflake.Data.Core;

    [TestFixture]
    class SnowflakeTomlConnectionBuilderTest
    {
        private readonly string _basicTomlConfig = @"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[testconnection]
account = ""testaccountname""
user = ""testusername""
password = ""testpassword""
[otherconnection]
account = ""otheraccountname""
user = ""otherusername""
password = ""otherpassword""";

        [Test]
        public void TestConnectionWithReadFromDefaultValuesInEnvironmentVariables()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string e, string s) => s);

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(_basicTomlConfig);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual("account=defaultaccountname;user=defaultusername;password=defaultpassword;", connectionString);
        }

        [Test]
        public void TestConnectionWithUserConnectionNameFromEnvVariable()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("testconnection");

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText("/testpath/connections.toml"))
                .Returns(_basicTomlConfig);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual("account=testaccountname;user=testusername;password=testpassword;", connectionString);
        }

        [Test]
        public void TestConnectionWithUserConnectionNameFromEnvVariableWithMultipleConnections()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("otherconnection");

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText("/testpath/connections.toml"))
                .Returns(_basicTomlConfig);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual("account=otheraccountname;user=otherusername;password=otherpassword;", connectionString);
        }

        [Test]
        public void TestConnectionWithUserConnectionName()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("otherconnection");

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText("/testpath/connections.toml"))
                .Returns(_basicTomlConfig);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml("testconnection");

            // Assert
            Assert.AreEqual("account=testaccountname;user=testusername;password=testpassword;", connectionString);
        }


        [Test]
        [TestCase("database = \"mydb\"", "DB=mydb;")]
        public void TestConnectionMapPropertiesFromTomlKeyValues(string tomlKeyValue, string connectionStringValue)
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("default");

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText("/testpath/connections.toml"))
                .Returns($@"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
{tomlKeyValue}
");

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=defaultaccountname;user=defaultusername;password=defaultpassword;{connectionStringValue}", connectionString);
        }

        [Test]
        public void TestConnectionConfigurationFileDoesNotExistsShouldReturnEmpty()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(false);
            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual(string.Empty, connectionString);
        }

        [Test]
        public void TestConnectionWithInvalidConnectionName()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("wrongconnectionname");

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(_basicTomlConfig);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act and assert
            Assert.Throws<Exception>(() => reader.GetConnectionStringFromToml(), "Specified connection name does not exist in connections.toml");
        }

        [Test]
        public void TestConnectionWithNonExistingDefaultConnection()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string e, string s) => s);

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns("[qa]\naccount = \"qaaccountname\"\nuser = \"qausername\"\npassword = \"qapassword\"");

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual(string.Empty, connectionString);
        }


        [Test]
        public void TestConnectionWithSpecifiedConnectionEmpty()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns("/testpath");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("testconnection1");

            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText("/testpath/connections.toml"))
                .Returns(@"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[testconnection1]
[testconnection2]
account = ""testaccountname""
user = ""testusername""
password = ""testpassword""");

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual(string.Empty, connectionString);
        }
    }

}
