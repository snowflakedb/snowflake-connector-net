using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using Snowflake.Data.Core;
    using Snowflake.Data.Core.Authenticator;

    [TestFixture]
    class SFAuthenticatorFactoryTest
    {
        IAuthenticator _authenticator;

        private IAuthenticator GetAuthenticator(string authenticatorName, string extraParams = "")
        {
            string connectionString = $"account=test;user=test;password=test;authenticator={authenticatorName};{extraParams}";
            SFSession session = new SFSession(connectionString, new SessionPropertiesContext());

            return AuthenticatorFactory.GetAuthenticator(session);
        }

        [Test]
        public void TestGetAuthenticatorBasic()
        {
            _authenticator = GetAuthenticator(BasicAuthenticator.AUTH_NAME);
            Assert.IsInstanceOf<BasicAuthenticator>(_authenticator);
        }

        [Test]
        public void TestGetAuthenticatorExternalBrowser()
        {
            _authenticator = GetAuthenticator(ExternalBrowserAuthenticator.AUTH_NAME);
            Assert.IsInstanceOf<ExternalBrowserAuthenticator>(_authenticator);
        }

        [Test]
        public void TestGetAuthenticatorKeyPairWithPrivateKey()
        {
            _authenticator = GetAuthenticator(KeyPairAuthenticator.AUTH_NAME, "private_key=xxxx");
            Assert.IsInstanceOf<KeyPairAuthenticator>(_authenticator);
        }

        [Test]
        public void TestGetAuthenticatorKeyPairWithPrivateKeyFile()
        {
            _authenticator = GetAuthenticator(KeyPairAuthenticator.AUTH_NAME, "private_key_file=xxxx");
            Assert.IsInstanceOf<KeyPairAuthenticator>(_authenticator);
        }

        [Test]
        public void TestGetAuthenticatorKeyPairWithMissingKey()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => GetAuthenticator(KeyPairAuthenticator.AUTH_NAME));
            Assert.AreEqual(SFError.INVALID_CONNECTION_STRING.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }

        [Test]
        public void TestGetAuthenticatorOAuth()
        {
            _authenticator = GetAuthenticator(OAuthAuthenticator.AUTH_NAME, "token=xxxx");
            Assert.IsInstanceOf<OAuthAuthenticator>(_authenticator);
        }

        [Test]
        public void TestGetAuthenticatorOAuthWithMissingToken()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => GetAuthenticator(OAuthAuthenticator.AUTH_NAME));
            Assert.AreEqual(SFError.MISSING_CONNECTION_PROPERTY.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }

        [Test]
        public void TestGetAuthenticatorOkta()
        {
            _authenticator = GetAuthenticator("https://xxxxxx.okta.com");
            Assert.IsInstanceOf<OktaAuthenticator>(_authenticator);
        }

        [Test]
        public void TestGetAuthenticatorUnknown()
        {
            SnowflakeDbException ex = Assert.Throws<SnowflakeDbException>(() => GetAuthenticator("Unknown"));
            Assert.AreEqual(SFError.UNKNOWN_AUTHENTICATOR.GetAttribute<SFErrorAttr>().errorCode, ex.ErrorCode);
        }
    }
}
