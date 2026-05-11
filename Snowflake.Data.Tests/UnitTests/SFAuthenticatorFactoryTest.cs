using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    using Xunit;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.Authenticator;
    class SFAuthenticatorFactoryTest
    {
        IAuthenticator _authenticator;

        private IAuthenticator GetAuthenticator(string authenticatorName, string extraParams = "")
        {
            string connectionString = $"account=test;user=test;password=test;authenticator={authenticatorName};{extraParams}";
            SFSession session = new SFSession(connectionString, new SessionPropertiesContext());

            return AuthenticatorFactory.GetAuthenticator(session);
        }

        [Fact]
        public void TestGetAuthenticatorBasic()
        {
            _authenticator = GetAuthenticator(BasicAuthenticator.AUTH_NAME);
            Assert.IsType<BasicAuthenticator>(_authenticator);
        }

        [Fact]
        public void TestGetAuthenticatorExternalBrowser()
        {
            _authenticator = GetAuthenticator(ExternalBrowserAuthenticator.AUTH_NAME);
            Assert.IsType<ExternalBrowserAuthenticator>(_authenticator);
        }

        [Fact]
        public void TestGetAuthenticatorKeyPairWithPrivateKey()
        {
            _authenticator = GetAuthenticator(KeyPairAuthenticator.AUTH_NAME, "private_key=xxxx");
            Assert.IsType<KeyPairAuthenticator>(_authenticator);
        }

        [Fact]
        public void TestGetAuthenticatorKeyPairWithPrivateKeyFile()
        {
            _authenticator = GetAuthenticator(KeyPairAuthenticator.AUTH_NAME, "private_key_file=xxxx");
            Assert.IsType<KeyPairAuthenticator>(_authenticator);
        }

        [Fact]
        public void TestGetAuthenticatorKeyPairWithMissingKey()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => GetAuthenticator(KeyPairAuthenticator.AUTH_NAME));
            Assert.Equal(SFError.INVALID_CONNECTION_STRING.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }

        [Fact]
        public void TestGetAuthenticatorOAuth()
        {
            _authenticator = GetAuthenticator(OAuthAuthenticator.AUTH_NAME);
            Assert.IsType<OAuthAuthenticator>(_authenticator);
        }

        [Fact]
        public void TestGetAuthenticatorOAuthWithMissingToken()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => GetAuthenticator(OAuthAuthenticator.AUTH_NAME));
            Assert.Equal(SFError.MISSING_CONNECTION_PROPERTY.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }

        [Fact]
        public void TestGetAuthenticatorOkta()
        {
            _authenticator = GetAuthenticator("https://xxxxxx.okta.com");
            Assert.IsType<OktaAuthenticator>(_authenticator);
        }

        [Fact]
        public void TestGetAuthenticatorUnknown()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => GetAuthenticator("Unknown"));
            Assert.Equal(SFError.UNKNOWN_AUTHENTICATOR.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }
    }
}
