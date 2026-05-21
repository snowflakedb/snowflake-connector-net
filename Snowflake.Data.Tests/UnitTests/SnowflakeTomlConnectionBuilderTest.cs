using Mono.Unix;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    using System;
    using System.IO;
    using Moq;
    using Xunit;
    using Core.Tools;
    using Snowflake.Data.Core;
    public class TomlConnectionBuilderTest
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

        [SFFact]
        public void TestConnectionWithReadFromDefaultValuesInSnowflakeTomlConnectionBuilder()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(BasicTomlConfig);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal("account=defaultaccountname;user=defaultusername;password=defaultpassword;", connectionString);
        }

        [SFFact]
        public void TestConnectionFromCustomSnowflakeHome()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome))
                .Returns($"{Path.DirectorySeparatorChar}customsnowhome");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("customsnowhome")), It.IsAny<Action<UnixStream>>()))
                .Returns(BasicTomlConfig);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal("account=defaultaccountname;user=defaultusername;password=defaultpassword;", connectionString);
        }

        [SFFact]
        public void TestConnectionWithUserConnectionNameFromEnvVariable()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("testconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(BasicTomlConfig);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal("account=testaccountname;user=testusername;password=testpassword;", connectionString);
        }

        [SFFact]
        public void TestConnectionWithUserConnectionNameFromEnvVariableWithMultipleConnections()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("otherconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(BasicTomlConfig);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal("account=otheraccountname;user=otherusername;password=otherpassword;", connectionString);
        }

        [SFFact]
        public void TestConnectionWithUserConnectionName()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("otherconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(BasicTomlConfig);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml("testconnection");

            // Assert
            Assert.Equal("account=testaccountname;user=testusername;password=testpassword;", connectionString);
        }


        [SFTheory]
        [InlineData("database = \"mydb\"", "DB=mydb;")]
        public void TestConnectionMapPropertiesFromTomlKeyValues(string tomlKeyValue, string connectionStringValue)
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns($@"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
{tomlKeyValue}
");

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal($"account=defaultaccountname;user=defaultusername;password=defaultpassword;{connectionStringValue}", connectionString);
        }

        [SFFact]
        public void TestConnectionConfigurationFileDoesNotExistsShouldReturnEmpty()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeHome))
                .Returns($"{Path.DirectorySeparatorChar}notexistenttestpath");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(false);
            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal(string.Empty, connectionString);
        }

        [SFFact]
        public void TestConnectionWithInvalidConnectionName()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("wrongconnectionname");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(BasicTomlConfig);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act and assert
            Assert.Throws<Exception>(() => reader.GetConnectionStringFromToml());
        }

        [SFFact]
        public void TestConnectionWithNonExistingDefaultConnection()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns("[qa]\naccount = \"qaaccountname\"\nuser = \"qausername\"\npassword = \"qapassword\"");

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal(string.Empty, connectionString);
        }


        [SFFact]
        public void TestConnectionWithSpecifiedConnectionEmpty()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("testconnection1");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
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

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal(string.Empty, connectionString);
        }

        [SFFact]
        public void TestConnectionWithOauthAuthenticatorTokenFromFile()
        {
            // Arrange
            var tokenFilePath = "/Users/testuser/token";
            var testToken = "token1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(tokenFilePath, It.IsAny<Action<UnixStream>>())).Returns(testToken);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""
token_file_path = ""{tokenFilePath}""");

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal($"account=testaccountname;authenticator=oauth;token={testToken};", connectionString);
        }

        [SFFact]
        public void TestConnectionWithOauthAuthenticatorThrowsExceptionIfTokenFilePathNotExists()
        {
            // Arrange
            var tokenFilePath = "/Users/testuser/token";
            var defaultToken = "defaultToken1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(tokenFilePath)).Returns(false);
            mockFileOperations.Setup(f => f.Exists(It.Is<string>(p => !p.Equals(tokenFilePath)))).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""
token_file_path = ""{tokenFilePath}""");
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("/snowflake/session/token")), It.IsAny<Action<UnixStream>>())).Returns(defaultToken);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act and assert
            var exception = Assert.Throws<SnowflakeDbException>(() => reader.GetConnectionStringFromToml());
            Assert.True(exception.Message.StartsWith("Error: Invalid parameter value /Users/testuser/token for token_file_path"));
        }

        [SFFact]
        public void TestConnectionWithOauthAuthenticatorFromDefaultPathShouldBeLoadedIfTokenFilePathNotSpecified()
        {
            // Arrange
            var defaultToken = "defaultToken1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""");
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("/snowflake/session/token")), It.IsAny<Action<UnixStream>>())).Returns(defaultToken);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal($"account=testaccountname;authenticator=oauth;token={defaultToken};", connectionString);
        }

        [SFFact]
        public void TestConnectionWithOauthAuthenticatorShouldNotIncludeTokenIfNotStoredDefaultPath()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.Is<string>(p => p.Contains("/snowflake/session/token")))).Returns(false);
            mockFileOperations.Setup(f => f.Exists(It.Is<string>(p => !string.IsNullOrEmpty(p) && !p.Contains("/snowflake/session/token")))).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""");

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal($"account=testaccountname;authenticator=oauth;", connectionString);
        }


        [SFFact]
        public void TestConnectionWithOauthAuthenticatorShouldNotLoadFromFileIsSpecifiedInTokenProperty()
        {
            // Arrange
            var tokenFilePath = "/Users/testuser/token";
            var tokenFromToml = "tomlToken1234";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
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

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal($"account=testaccountname;authenticator=oauth;token={tokenFromToml};", connectionString);
        }

        [SFFact]
        public void TestConnectionWithOauthAuthenticatorShouldNotIncludeTokenIfNullOrEmpty()
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations
                .Setup(e => e.GetEnvironmentVariable(TomlConnectionBuilder.SnowflakeDefaultConnectionName))
                .Returns("oauthconnection");
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(@$"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""defaultpassword""
[oauthconnection]
account = ""testaccountname""
authenticator = ""oauth""");
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains("/snowflake/session/token")), It.IsAny<Action<UnixStream>>())).Returns(string.Empty);

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();

            // Assert
            Assert.Equal($"account=testaccountname;authenticator=oauth;", connectionString);
        }

        [SFTheory]
        [InlineData("\\\"password;default\\\"", "password;default")]
        [InlineData("\\\"\\\"\\\"password;default\\\"", "\"password;default")]
        [InlineData("p\\\"assworddefault", "p\"assworddefault")]
        [InlineData("password\\\"default", "password\"default")]
        [InlineData("password\'default", "password\'default")]
        [InlineData("password=default", "password=default")]
        [InlineData("\\\"pa=ss\\\"\\\"word;def\'ault\\\"", "pa=ss\"word;def\'ault")]
        public void TestConnectionMapPropertiesWithSpecialCharacters(string passwordValueWithSpecialCharacter, string expectedValue)
        {
            // Arrange
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns($@"
[default]
account = ""defaultaccountname""
user = ""defaultusername""
password = ""{passwordValueWithSpecialCharacter}""
");

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // Assert
            Assert.Equal(expectedValue, properties[SFSessionProperty.PASSWORD]);
        }

        [SFFact]
        public void TestConnectionWithCompleteSPCSConfiguration()
        {
            // Arrange
            var tokenFilePath = "/path/to/token";
            var testToken = "oauth_token_12345";
            var mockFileOperations = new Mock<FileOperations>();
            var mockEnvironmentOperations = new Mock<EnvironmentOperations>();
            mockEnvironmentOperations.Setup(e => e.GetFolderPath(Environment.SpecialFolder.UserProfile))
                .Returns($"{Path.DirectorySeparatorChar}home");
            mockFileOperations.Setup(f => f.Exists(It.IsAny<string>())).Returns(true);
            mockFileOperations.Setup(f => f.ReadAllText(tokenFilePath, It.IsAny<Action<UnixStream>>())).Returns(testToken);
            mockFileOperations.Setup(f => f.ReadAllText(It.Is<string>(p => p.Contains(".snowflake")), It.IsAny<Action<UnixStream>>()))
                .Returns(@$"
[default]
host = 'host.snowflake.com'
protocol = 'http'
port = 80
account = 'account123'
database = 'testdb'
schema = 'testschema'
warehouse = 'testwh'
authenticator = 'oauth'
token_file_path = '{tokenFilePath}'
client_session_keep_alive = true
ocsp_fail_open = true
disable_ocsp_check = true
");

            var reader = new TomlConnectionBuilder(mockFileOperations.Object, mockEnvironmentOperations.Object);

            // Act
            var connectionString = reader.GetConnectionStringFromToml();
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // Assert
            Assert.Multiple(() =>
            {
                Assert.Equal("host.snowflake.com", properties[SFSessionProperty.HOST]);
                Assert.Equal("http", properties[SFSessionProperty.SCHEME]);
                Assert.Equal("80", properties[SFSessionProperty.PORT]);
                Assert.Equal("account123", properties[SFSessionProperty.ACCOUNT]);
                Assert.Equal("testdb", properties[SFSessionProperty.DB]);
                Assert.Equal("testschema", properties[SFSessionProperty.SCHEMA]);
                Assert.Equal("testwh", properties[SFSessionProperty.WAREHOUSE]);
                Assert.Equal("oauth", properties[SFSessionProperty.AUTHENTICATOR]);
                Assert.Equal(testToken, properties[SFSessionProperty.TOKEN]);
                Assert.Equal("true", properties[SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE]);
            });
        }
    }

}
