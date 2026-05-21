using System.Net;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;


namespace Snowflake.Data.Tests.UnitTests
{

    public class AuthenticationPropertiesValidatorTest
    {
        private const string _necessaryNonAuthProperties = "account=a;";

        [InlineData("authenticator=snowflake;user=test;password=test", null)]
        [InlineData("authenticator=Snowflake;user=test", "test")]
        [InlineData("authenticator=ExternalBrowser", null)]
        [InlineData("authenticator=snowflake_jwt;user=test;private_key_file=key.file", null)]
        [InlineData("authenticator=SNOWFLAKE_JWT;user=test;private_key=key", null)]
        [InlineData("authenticator=Snowflake_jwt;user=test;private_key=key;private_key_pwd=test", null)]
        [InlineData("authenticator=oauth;token=value", null)]
        [InlineData("authenticator=https://something.oktapreview.com;user=test;password=test", null)]
        [InlineData("authenticator=https://vanity.url/snowflake/okta;USER=TEST;PASSWORD=TEST", null)]
        public void TestAuthPropertiesValid(string connectionString, string password)
        {
            // Arrange
            var securePassword = string.IsNullOrEmpty(password) ? null : new NetworkCredential(string.Empty, password).SecurePassword;
            var propertiesContext = new SessionPropertiesContext { Password = securePassword };

            // Act/Assert
            Assert.DoesNotThrow(() => SFSessionProperties.ParseConnectionString(_necessaryNonAuthProperties + connectionString, propertiesContext));
        }

        [InlineData("authenticator=snowflake;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided.")]
        [InlineData("authenticator=snowflake;", "test", SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [InlineData("authenticator=snowflake;user=;password=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided.")]
        [InlineData("authenticator=snowflake;user=;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [InlineData("authenticator=snowflake;user=;", "test", SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [InlineData("authenticator=snowflake_jwt;private_key_file=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [InlineData("authenticator=snowflake_jwt;private_key=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [InlineData("authenticator=snowflake_jwt;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [InlineData("authenticator=oauth;TOKen=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property TOKEN is not provided")]
        [InlineData("authenticator=oauth;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property TOKEN is not provided")]
        [InlineData("authenticator=https://okta.com;user=;password=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [InlineData("authenticator=https://okta.com;user=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [InlineData("authenticator=https://okta.com;password=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [InlineData("authenticator=https://okta.com;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [InlineData("authenticator=unknown;", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        [InlineData("authenticator=http://unknown.okta.com;", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        [InlineData("AUTHENTICATOR=HTTPS://SOMETHING.OKTA.COM;USER=TEST;PASSWORD=TEST", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        [InlineData("authenticator=https://unknown;", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        public void TestAuthPropertiesInvalid(string connectionString, string password, SFError expectedError, string expectedErrorMessage)
        {
            // Arrange
            var securePassword = string.IsNullOrEmpty(password) ? null : new NetworkCredential(string.Empty, password).SecurePassword;
            var propertiesContext = new SessionPropertiesContext { Password = securePassword };

            // Act
            var exception = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(_necessaryNonAuthProperties + connectionString, propertiesContext));

            // Assert
            SnowflakeDbExceptionAssert.HasErrorCode(exception, expectedError);
            Assert.That(exception.Message.Contains(expectedErrorMessage), $"Expecting:\n\t{exception.Message}\nto contain:\n\t{expectedErrorMessage}");
        }
    }
}
