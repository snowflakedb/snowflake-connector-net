using System.Collections.Generic;
using Snowflake.Data.Core;
using System.Security;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests
{

    class SFSessionPropertyTest
    {

        [Test, TestCaseSource(nameof(ConnectionStringTestCases))]
        public void TestThatPropertiesAreParsed(TestCase testcase)
        {
            // arrange
            var propertiesContext = new SessionPropertiesContext { Password = testcase.SecurePassword };

            // act
            var properties = SFSessionProperties.ParseConnectionString(testcase.ConnectionString, propertiesContext);

            // assert
            CollectionAssert.IsSubsetOf(testcase.ExpectedProperties, properties);
        }

        [Test]
        [TestCase("a", "a", "a.snowflakecomputing.com")]
        [TestCase("ab", "ab", "ab.snowflakecomputing.com")]
        [TestCase("a.b", "a", "a.b.snowflakecomputing.com")]
        [TestCase("a-b", "a-b", "a-b.snowflakecomputing.com")]
        [TestCase("a_b", "a_b", "a-b.snowflakecomputing.com")]
        [TestCase("abc", "abc", "abc.snowflakecomputing.com")]
        [TestCase("xy12345.us-east-2.aws", "xy12345", "xy12345.us-east-2.aws.snowflakecomputing.com")]
        public void TestValidateCorrectAccountNames(string accountName, string expectedAccountName, string expectedHost)
        {
            // arrange
            var connectionString = $"ACCOUNT={accountName};USER=test;PASSWORD=test;";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(expectedAccountName, properties[SFSessionProperty.ACCOUNT]);
            Assert.AreEqual(expectedHost, properties[SFSessionProperty.HOST]);
        }

        [Test]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;FILE_TRANSFER_MEMORY_THRESHOLD=0;", "Error: Invalid parameter value 0 for FILE_TRANSFER_MEMORY_THRESHOLD")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;FILE_TRANSFER_MEMORY_THRESHOLD=xyz;", "Error: Invalid parameter value xyz for FILE_TRANSFER_MEMORY_THRESHOLD")]
        [TestCase("ACCOUNT=testaccount?;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value testaccount? for ACCOUNT")]
        [TestCase("ACCOUNT=complicated.long.testaccount?;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value complicated.long.testaccount? for ACCOUNT")]
        [TestCase("ACCOUNT=?testaccount;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value ?testaccount for ACCOUNT")]
        [TestCase("ACCOUNT=.testaccount;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value .testaccount for ACCOUNT")]
        [TestCase("ACCOUNT=testaccount.;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value testaccount. for ACCOUNT")]
        [TestCase("ACCOUNT=test%account;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value test%account for ACCOUNT")]
        public void TestThatItFailsForWrongConnectionParameter(string connectionString, string expectedErrorMessagePart)
        {
            // act
            var exception = Assert.Throws<SnowflakeDbException>(
                () => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext())
            );

            // assert
            Assert.AreEqual(SFError.INVALID_CONNECTION_PARAMETER_VALUE.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.IsTrue(exception.Message.Contains(expectedErrorMessagePart));
        }

        [Test]
        [TestCase("ACCOUNT=;USER=testuser;PASSWORD=testpassword")]
        [TestCase("USER=testuser;PASSWORD=testpassword")]
        public void TestThatItFailsIfNoAccountSpecified(string connectionString)
        {
            // act
            var exception = Assert.Throws<SnowflakeDbException>(
                () => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext())
            );

            // assert
            Assert.AreEqual(SFError.MISSING_CONNECTION_PROPERTY.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
        }

        [Test]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=", null)]
        [TestCase("ACCOUNT=testaccount;USER=testuser;", "")]
        [TestCase("authenticator=https://okta.com;ACCOUNT=testaccount;USER=testuser;PASSWORD=", null)]
        [TestCase("authenticator=https://okta.com;ACCOUNT=testaccount;USER=testuser;", "")]
        public void TestFailWhenNoPasswordProvided(string connectionString, string password)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var propertiesContext = new SessionPropertiesContext { Password = securePassword };

            // act
            var exception = Assert.Throws<SnowflakeDbException>(
                () => SFSessionProperties.ParseConnectionString(connectionString, propertiesContext)
            );

            // assert
            Assert.AreEqual(SFError.MISSING_CONNECTION_PROPERTY.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.That(exception.Message, Does.Contain("Required property PASSWORD is not provided"));
        }

        [Test]
        public void TestParsePasscode()
        {
            // arrange
            var expectedPasscode = "abc";
            var connectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;PASSCODE={expectedPasscode}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(expectedPasscode, properties[SFSessionProperty.PASSCODE]);
        }

        [Test]
        public void TestUsePasscodeFromSecureString()
        {
            // arrange
            var expectedPasscode = "abc";
            var connectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword";
            var securePasscode = SecureStringHelper.Encode(expectedPasscode);
            var propertiesContext = new SessionPropertiesContext { Passcode = securePasscode };

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, propertiesContext);

            // assert
            Assert.AreEqual(expectedPasscode, properties[SFSessionProperty.PASSCODE]);
        }

        [Test]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;PASSCODE=")]
        public void TestDoNotParsePasscodeWhenNotProvided(string connectionString)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.False(properties.TryGetValue(SFSessionProperty.PASSCODE, out _));
        }

        [Test]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;", "false")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=", "false")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=true", "true")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=TRUE", "TRUE")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=false", "false")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=FALSE", "FALSE")]
        public void TestParsePasscodeInPassword(string connectionString, string expectedPasscodeInPassword)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.IsTrue(properties.TryGetValue(SFSessionProperty.PASSCODEINPASSWORD, out var passcodeInPassword));
            Assert.AreEqual(expectedPasscodeInPassword, passcodeInPassword);
        }

        [Test]
        public void TestFailWhenInvalidPasscodeInPassword()
        {
            // arrange
            var invalidConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=abc";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(invalidConnectionString, new SessionPropertiesContext()));

            Assert.That(thrown.Message, Does.Contain("Invalid parameter value  for PASSCODEINPASSWORD"));
        }

        [Test]
        [TestCase("DB", SFSessionProperty.DB, "\"testdb\"")]
        [TestCase("SCHEMA", SFSessionProperty.SCHEMA, "\"quotedSchema\"")]
        [TestCase("ROLE", SFSessionProperty.ROLE, "\"userrole\"")]
        [TestCase("WAREHOUSE", SFSessionProperty.WAREHOUSE, "\"warehouse  test\"")]
        public void TestValidateSupportEscapedQuotesValuesForObjectProperties(string propertyName, SFSessionProperty sessionProperty, string value)
        {
            // arrange
            var connectionString = $"ACCOUNT=test;{propertyName}={value};USER=test;PASSWORD=test;";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(value, properties[sessionProperty]);
        }

        [Test]
        [TestCase("DB", SFSessionProperty.DB, "testdb", "testdb")]
        [TestCase("DB", SFSessionProperty.DB, "\"testdb\"", "\"testdb\"")]
        [TestCase("DB", SFSessionProperty.DB, "\"\"\"testDB\"\"\"", "\"\"testDB\"\"")]
        [TestCase("DB", SFSessionProperty.DB, "\"\"\"test\"\"DB\"\"\"", "\"\"test\"DB\"\"")]
        [TestCase("SCHEMA", SFSessionProperty.SCHEMA, "\"quoted\"\"Schema\"", "\"quoted\"Schema\"")]
        public void TestValidateSupportEscapedQuotesInsideValuesForObjectProperties(string propertyName, SFSessionProperty sessionProperty, string value, string expectedValue)
        {
            // arrange
            var connectionString = $"ACCOUNT=test;{propertyName}={value};USER=test;PASSWORD=test;";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(expectedValue, properties[sessionProperty]);
        }

        [Test]
        [TestCase("true")]
        [TestCase("false")]
        public void TestValidateDisableSamlUrlCheckProperty(string expectedDisableSamlUrlCheck)
        {
            // arrange
            var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;DISABLE_SAML_URL_CHECK={expectedDisableSamlUrlCheck}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(expectedDisableSamlUrlCheck, properties[SFSessionProperty.DISABLE_SAML_URL_CHECK]);
        }

        [Test]
        [TestCase("account.snowflakecomputing.cn", "Connecting to CHINA Snowflake domain")]
        [TestCase("account.snowflakecomputing.com", "Connecting to GLOBAL Snowflake domain")]
        public void TestResolveConnectionArea(string host, string expectedMessage)
        {
            // act
            var message = SFSessionProperties.ResolveConnectionAreaMessage(host);

            // assert
            Assert.AreEqual(expectedMessage, message);
        }

        [Test]
        public void TestParseClientId()
        {
            // arrange
            var clientId = "abc";
            var connectionString = $"ACCOUNT=test;USER=test;PASSWORD=test;CLIENT_ID={clientId}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.CLIENT_ID]);
        }

        [Test]
        public void TestParseClientSecret()
        {
            // arrange
            var clientSecret = "abc";
            var connectionString = $"ACCOUNT=test;USER=test;PASSWORD=test;CLIENT_SECRET={clientSecret}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.CLIENT_SECRET]);
        }

        [Test]
        public void TestParseAuthorizationScope()
        {
            // arrange
            var authorizationScope = "abc";
            var connectionString = $"ACCOUNT=test;USER=test;PASSWORD=test;AUTHORIZATION_SCOPE={authorizationScope}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(authorizationScope, properties[SFSessionProperty.AUTHORIZATION_SCOPE]);
        }

        [Test]
        public void TestParseRedirectUri()
        {
            // arrange
            var redirectUri = "http://localhost:8080";
            var connectionString = $"ACCOUNT=test;USER=test;PASSWORD=test;REDIRECT_URI={redirectUri}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(redirectUri, properties[SFSessionProperty.REDIRECT_URI]);
        }

        [Test]
        public void TestParseExternalAuthorizationUrl()
        {
            // arrange
            var externalAuthorizationUrl = "https://okta.com/authorize";
            var connectionString = $"ACCOUNT=test;USER=test;PASSWORD=test;EXTERNAL_AUTHORIZATION_URL={externalAuthorizationUrl}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(externalAuthorizationUrl, properties[SFSessionProperty.EXTERNAL_AUTHORIZATION_URL]);
        }

        [Test]
        public void TestParseExternalTokenRequestUrl()
        {
            // arrange
            var externalTokenRequestUrl = "https://okta.com/token";
            var connectionString = $"ACCOUNT=test;USER=test;PASSWORD=test;EXTERNAL_TOKEN_REQUEST_URL={externalTokenRequestUrl}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(externalTokenRequestUrl, properties[SFSessionProperty.EXTERNAL_TOKEN_REQUEST_URL]);
        }

        [Test]
        public void TestNoOAuthPropertiesFound()
        {
            // arrange
            var connectionString = "ACCOUNT=test;USER=test;PASSWORD=test";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.CLIENT_ID, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.CLIENT_SECRET, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.AUTHORIZATION_SCOPE, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.REDIRECT_URI, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.EXTERNAL_AUTHORIZATION_URL, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.EXTERNAL_TOKEN_REQUEST_URL, out var _));
        }

        [Test]
        public void TestOAuthAuthorizationCodeAllParameters()
        {
            // arrange
            var clientId = "abc";
            var clientSecret = "def";
            var scope = "ghi";
            var redirectUri = "http://localhost";
            var authorizationUrl = "https://okta.com/authorize";
            var tokenUrl = "https://okta.com/token-request";
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;CLIENT_ID={clientId};CLIENT_SECRET={clientSecret};AUTHORIZATION_SCOPE={scope};REDIRECT_URI={redirectUri};EXTERNAL_AUTHORIZATION_URL={authorizationUrl};EXTERNAL_TOKEN_REQUEST_URL={tokenUrl};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.CLIENT_ID]);
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.CLIENT_SECRET]);
            Assert.AreEqual(scope, properties[SFSessionProperty.AUTHORIZATION_SCOPE]);
            Assert.AreEqual(redirectUri, properties[SFSessionProperty.REDIRECT_URI]);
            Assert.AreEqual(authorizationUrl, properties[SFSessionProperty.EXTERNAL_AUTHORIZATION_URL]);
            Assert.AreEqual(tokenUrl, properties[SFSessionProperty.EXTERNAL_TOKEN_REQUEST_URL]);
        }

        [Test]
        public void TestOAuthAuthorizationCodeMinimalParameters()
        {
            // arrange
            var clientId = "abc";
            var clientSecret = "def";
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;CLIENT_ID={clientId};CLIENT_SECRET={clientSecret};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.CLIENT_ID]);
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.CLIENT_SECRET]);
        }

        [Test]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;CLIENT_SECRET=def;", "Required property CLIENT_ID is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;CLIENT_ID=abc;", "Required property CLIENT_SECRET is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;CLIENT_ID=abc;CLIENT_SECRET=def;", "Required property AUTHORIZATION_SCOPE or ROLE is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;CLIENT_ID=abc;CLIENT_SECRET=def;AUTHORIZATION_SCOPE=ghi;EXTERNAL_AUTHORIZATION_URL=https://okta.com/authorize", "Required property EXTERNAL_TOKEN_REQUEST_URL is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;CLIENT_ID=abc;CLIENT_SECRET=def;AUTHORIZATION_SCOPE=ghi;EXTERNAL_TOKEN_REQUEST_URL=https://okta.com/token-request", "Required property EXTERNAL_AUTHORIZATION_URL is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;CLIENT_ID=abc;CLIENT_SECRET=def;AUTHORIZATION_SCOPE=ghi;EXTERNAL_AUTHORIZATION_URL=http://okta.com/authorize;EXTERNAL_TOKEN_REQUEST_URL=https://okta.com/token-request", "Invalid parameter value  for EXTERNAL_AUTHORIZATION_URL")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;CLIENT_ID=abc;CLIENT_SECRET=def;AUTHORIZATION_SCOPE=ghi;EXTERNAL_AUTHORIZATION_URL=https://okta.com/authorize;EXTERNAL_TOKEN_REQUEST_URL=http://okta.com/token-request", "Invalid parameter value  for EXTERNAL_TOKEN_REQUEST_URL")]
        public void TestOAuthAuthorizationCodeMissingOrInvalidParameters(string connectionString, string errorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(errorMessage));
        }

        public static IEnumerable<TestCase> ConnectionStringTestCases()
        {
            string defAccount = "testaccount";
            string defUser = "testuser";
            string defHost = "testaccount.snowflakecomputing.com";
            string defAuthenticator = "snowflake";
            string defScheme = "https";
            string defConnectionTimeout = "300";
            string defBrowserResponseTime = "120";
            string defPassword = "123";
            string defPort = "443";

            string defProxyHost = "proxy.com";
            string defProxyPort = "1234";
            string defNonProxyHosts = "localhost";

            string defRetryTimeout = "300";
            string defMaxHttpRetries = "7";
            string defIncludeRetryReason = "true";
            string defDisableQueryContextCache = "false";
            string defDisableConsoleLogin = "true";
            string defAllowUnderscoresInHost = "false";

            var simpleTestCase = new TestCase()
            {
                ConnectionString = $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };

            var testCaseWithBrowserResponseTimeout = new TestCase()
            {
                ConnectionString = $"ACCOUNT={defAccount};BROWSER_RESPONSE_TIMEOUT=180;authenticator=externalbrowser",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, "" },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, ExternalBrowserAuthenticator.AUTH_NAME },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "180" },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };
            var testCaseWithProxySettings = new TestCase()
            {
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.USEPROXY, "true" },
                    { SFSessionProperty.PROXYHOST, defProxyHost },
                    { SFSessionProperty.PROXYPORT, defProxyPort },
                    { SFSessionProperty.NONPROXYHOSTS, defNonProxyHosts },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                },
                ConnectionString =
                    $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};useProxy=true;proxyHost=proxy.com;proxyPort=1234;nonProxyHosts=localhost"
            };
            var testCaseThatDefaultForUseProxyIsFalse = new TestCase()
            {
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.PROXYHOST, defProxyHost },
                    { SFSessionProperty.PROXYPORT, defProxyPort },
                    { SFSessionProperty.NONPROXYHOSTS, defNonProxyHosts },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                },
                ConnectionString =
                    $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};proxyHost=proxy.com;proxyPort=1234;nonProxyHosts=localhost"
            };
            var testCaseWithFileTransferMaxBytesInMemory = new TestCase()
            {
                ConnectionString = $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};FILE_TRANSFER_MEMORY_THRESHOLD=25;",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.FILE_TRANSFER_MEMORY_THRESHOLD, "25" },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };
            var testCaseWithIncludeRetryReason = new TestCase()
            {
                ConnectionString = $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};IncludeRetryReason=false",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, "false" },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };
            var testCaseWithDisableQueryContextCache = new TestCase()
            {
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, "true" },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                },
                ConnectionString =
                    $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};DISABLEQUERYCONTEXTCACHE=true"
            };
            var testCaseWithDisableConsoleLogin = new TestCase()
            {
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, defHost },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, "false" },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                },
                ConnectionString =
                    $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};DISABLE_CONSOLE_LOGIN=false"
            };
            var complicatedAccount = $"{defAccount}.region-name.host-name";
            var testCaseComplicatedAccountName = new TestCase()
            {
                ConnectionString = $"ACCOUNT={complicatedAccount};USER={defUser};PASSWORD={defPassword};",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, defAccount },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, $"{complicatedAccount}.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };
            var testCaseUnderscoredAccountName = new TestCase()
            {
                ConnectionString = $"ACCOUNT=prefix_{defAccount};USER={defUser};PASSWORD={defPassword};",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, $"prefix_{defAccount}" },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, $"prefix-{defAccount}.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, defAllowUnderscoresInHost },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };
            var testCaseUnderscoredAccountNameWithEnabledAllowUnderscores = new TestCase()
            {
                ConnectionString = $"ACCOUNT=prefix_{defAccount};USER={defUser};PASSWORD={defPassword};allowUnderscoresInHost=true;",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, $"prefix_{defAccount}" },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, $"prefix_{defAccount}.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, "true" },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };

            var testQueryTag = "Test QUERY_TAG 12345";
            var testCaseQueryTag = new TestCase()
            {
                ConnectionString = $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};QUERY_TAG={testQueryTag}",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, $"{defAccount}" },
                    { SFSessionProperty.USER, defUser },
                    { SFSessionProperty.HOST, $"{defAccount}.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, defAuthenticator },
                    { SFSessionProperty.SCHEME, defScheme },
                    { SFSessionProperty.CONNECTION_TIMEOUT, defConnectionTimeout },
                    { SFSessionProperty.PASSWORD, defPassword },
                    { SFSessionProperty.PORT, defPort },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime },
                    { SFSessionProperty.RETRY_TIMEOUT, defRetryTimeout },
                    { SFSessionProperty.MAXHTTPRETRIES, defMaxHttpRetries },
                    { SFSessionProperty.INCLUDERETRYREASON, defIncludeRetryReason },
                    { SFSessionProperty.DISABLEQUERYCONTEXTCACHE, defDisableQueryContextCache },
                    { SFSessionProperty.DISABLE_CONSOLE_LOGIN, defDisableConsoleLogin },
                    { SFSessionProperty.ALLOWUNDERSCORESINHOST, "false" },
                    { SFSessionProperty.QUERY_TAG, testQueryTag },
                    { SFSessionProperty.MAXPOOLSIZE, DefaultValue(SFSessionProperty.MAXPOOLSIZE) },
                    { SFSessionProperty.MINPOOLSIZE, DefaultValue(SFSessionProperty.MINPOOLSIZE) },
                    { SFSessionProperty.CHANGEDSESSION, DefaultValue(SFSessionProperty.CHANGEDSESSION) },
                    { SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT, DefaultValue(SFSessionProperty.WAITINGFORIDLESESSIONTIMEOUT) },
                    { SFSessionProperty.EXPIRATIONTIMEOUT, DefaultValue(SFSessionProperty.EXPIRATIONTIMEOUT) },
                    { SFSessionProperty.POOLINGENABLED, DefaultValue(SFSessionProperty.POOLINGENABLED) },
                    { SFSessionProperty.DISABLE_SAML_URL_CHECK, DefaultValue(SFSessionProperty.DISABLE_SAML_URL_CHECK) },
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) }
                }
            };

            return new TestCase[]
            {
                simpleTestCase,
                testCaseWithBrowserResponseTimeout,
                testCaseWithProxySettings,
                testCaseThatDefaultForUseProxyIsFalse,
                testCaseWithFileTransferMaxBytesInMemory,
                testCaseWithIncludeRetryReason,
                testCaseWithDisableQueryContextCache,
                testCaseWithDisableConsoleLogin,
                testCaseComplicatedAccountName,
                testCaseUnderscoredAccountName,
                testCaseUnderscoredAccountNameWithEnabledAllowUnderscores,
                testCaseQueryTag
            };
        }

        private static string DefaultValue(SFSessionProperty property) =>
            property.GetAttribute<SFSessionPropertyAttr>().defaultValue;

        internal class TestCase
        {
            public string ConnectionString { get; set; }
            public SecureString SecurePassword { get; set; }
            public SFSessionProperties ExpectedProperties { get; set; }
        }
    }
}
