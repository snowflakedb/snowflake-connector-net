/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using System.IO;
    using Moq;
    using NUnit.Framework;
    using Core.Tools;
    using Snowflake.Data.Core;

    [TestFixture]
    class SnowflakeTomlConnectionBuilderTest
    {
        private const string BasicTomlConfig = @"
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
                .Returns((string _, string s) => s);
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(BasicTomlConfig);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual("account=defaultaccountname;user=defaultusername;password=defaultpassword;", connectionString);
        }

        [Test]
        public void TestConnectionFromCustomSnowflakeHome()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns($"{Path.DirectorySeparatorChar}customsnowhome");
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns((string _, string s) => s);
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("customsnowhome"))))
                .Returns(BasicTomlConfig);

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
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("testconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(BasicTomlConfig);

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
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("otherconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(BasicTomlConfig);

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
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("otherconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(BasicTomlConfig);

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
                .Setup(e => e.GetEnvironmentVariable(It.IsAny<string>(), It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
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
                .Returns($"{Path.DirectorySeparatorChar}notexistenttestpath");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
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
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("wrongconnectionname");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(BasicTomlConfig);

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
                .Returns((string _, string s) => s);
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
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
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("testconnection1");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
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

        [Test]
        public void TestConnectionWithOauthAuthenticatorTokenFromFile()
        {
            // Arrange
            var tokenFilePath = "/Users/testuser/token";
            var testToken = "token1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(tokenFilePath)).Returns(testToken);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""
token_file_path = ""{tokenFilePath}""");

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=testaccountname;authenticator=oauth;token={testToken};", connectionString);
        }

        [Test]
        public void TestConnectionWithOauthAuthenticatorFromDefaultIfTokenFilePathNotExists()
        {
            // Arrange
            var tokenFilePath = "/Users/testuser/token";
            var defaultToken = "defaultToken1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(tokenFilePath)).Returns(false);
            mockFileOperations.Setup(f => f.Exists(It.Is<string>(p => !p.Equals(tokenFilePath)))).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""
token_file_path = ""{tokenFilePath}""");
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("/snowflake/session/token")))).Returns(defaultToken);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=testaccountname;authenticator=oauth;token={defaultToken};", connectionString);
        }

        [Test]
        public void TestConnectionWithOauthAuthenticatorFromDefaultPathShouldBeLoadedIfTokenFilePathNotSpecified()
        {
            // Arrange
            var defaultToken = "defaultToken1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""");
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("/snowflake/session/token")))).Returns(defaultToken);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=testaccountname;authenticator=oauth;token={defaultToken};", connectionString);
        }

        [Test]
        public void TestConnectionWithOauthAuthenticatorShouldNotIncludeTokenIfNotStoredDefaultPath()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.Is<string>(p => p.Contains("/snowflake/session/token")))).Returns(false);
            mockFileOperations.Setup(f => f.Exists(It.Is<string>(p => !string.IsNullOrEmpty(p) && !p.Contains("/snowflake/session/token")))).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""");

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=testaccountname;authenticator=oauth;", connectionString);
        }


        [Test]
        public void TestConnectionWithOauthAuthenticatorShouldNotLoadFromFileIsSpecifiedInTokenProperty()
        {
            // Arrange
            var tokenFilePath = "/Users/testuser/token";
            var tokenFromToml = "tomlToken1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""
token = ""{tokenFromToml}""
token_file_path = ""{tokenFilePath}""");

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=testaccountname;authenticator=oauth;token={tokenFromToml};", connectionString);
        }

        [Test]
        public void TestConnectionWithOauthAuthenticatorShouldNotIncludeTokenIfNullOrEmpty()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeHome, It.IsAny<string>()))
                .Returns((string _, string d) => d);
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(EnvironmentVariables.SnowflakeDefaultConnectionName, It.IsAny<string>()))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake"))))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""");
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("/snowflake/session/token")))).Returns(string.Empty);

            var reader = new SnowflakeTomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.AreEqual($"account=testaccountname;authenticator=oauth;", connectionString);
        }
    }

}
