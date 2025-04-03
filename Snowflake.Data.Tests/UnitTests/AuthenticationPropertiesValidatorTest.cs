using System.Net;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;


namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture]
    public class AuthenticationPropertiesValidatorTest
    {
        private const string _necessaryNonAuthProperties = "account=a;";

        [TestCase("authenticator=snowflake;user=test;password=test", null)]
        [TestCase("authenticator=Snowflake;user=test", "test")]
        [TestCase("authenticator=ExternalBrowser", null)]
        [TestCase("authenticator=snowflake_jwt;user=test;private_key_file=key.file", null)]
        [TestCase("authenticator=SNOWFLAKE_JWT;user=test;private_key=key", null)]
        [TestCase("authenticator=Snowflake_jwt;user=test;private_key=key;private_key_pwd=test", null)]
        [TestCase("authenticator=oauth;token=value", null)]
        [TestCase("authenticator=https://something.oktapreview.com;user=test;password=test", null)]
        [TestCase("authenticator=https://vanity.url/snowflake/okta;USER=TEST;PASSWORD=TEST", null)]
        public void TestAuthPropertiesValid(string connectionString, string password)
        {
            // Arrange
            var securePassword = string.IsNullOrEmpty(password) ? null : new NetworkCredential(string.Empty, password).SecurePassword;
            var propertiesContext = new SessionPropertiesContext { Password = securePassword };

            // Act/Assert
            Assert.DoesNotThrow(() => SFSessionProperties.ParseConnectionString(_necessaryNonAuthProperties + connectionString, propertiesContext));
        }

        [TestCase("authenticator=snowflake;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided.")]
        [TestCase("authenticator=snowflake;", "test", SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [TestCase("authenticator=snowflake;user=;password=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided.")]
        [TestCase("authenticator=snowflake;user=;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [TestCase("authenticator=snowflake;user=;", "test", SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [TestCase("authenticator=snowflake_jwt;private_key_file=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [TestCase("authenticator=snowflake_jwt;private_key=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [TestCase("authenticator=snowflake_jwt;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property USER is not provided")]
        [TestCase("authenticator=oauth;TOKen=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property TOKEN is not provided")]
        [TestCase("authenticator=oauth;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property TOKEN is not provided")]
        [TestCase("authenticator=https://okta.com;user=;password=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [TestCase("authenticator=https://okta.com;user=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [TestCase("authenticator=https://okta.com;password=", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [TestCase("authenticator=https://okta.com;", null, SFError.MISSING_CONNECTION_PROPERTY, "Error: Required property PASSWORD is not provided")]
        [TestCase("authenticator=unknown;", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        [TestCase("authenticator=http://unknown.okta.com;", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        [TestCase("AUTHENTICATOR=HTTPS://SOMETHING.OKTA.COM;USER=TEST;PASSWORD=TEST", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
        [TestCase("authenticator=https://unknown;", null, SFError.UNKNOWN_AUTHENTICATOR, "Unknown authenticator")]
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
