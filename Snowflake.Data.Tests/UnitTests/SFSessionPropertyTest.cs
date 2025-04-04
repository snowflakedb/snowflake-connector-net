using System.Collections.Generic;
using Snowflake.Data.Core;
using System.Security;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Tools;
using System.Runtime.InteropServices;

namespace Snowflake.Data.Tests.UnitTests
{

    class SFSessionPropertyTest
    {

        [Test, TestCaseSource(nameof(ConnectionStringTestCases))]
        public void TestThatPropertiesAreParsed(TestCase testcase)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(
                testcase.ConnectionString,
                testcase.SecurePassword);

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
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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
                () => SFSessionProperties.ParseConnectionString(connectionString, null)
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
                () => SFSessionProperties.ParseConnectionString(connectionString, null)
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

            // act
            var exception = Assert.Throws<SnowflakeDbException>(
                () => SFSessionProperties.ParseConnectionString(connectionString, securePassword)
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
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null, securePasscode);

            // assert
            Assert.AreEqual(expectedPasscode, properties[SFSessionProperty.PASSCODE]);
        }

        [Test]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;")]
        [TestCase("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;PASSCODE=")]
        public void TestDoNotParsePasscodeWhenNotProvided(string connectionString)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(invalidConnectionString, null));

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
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

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
        [TestCase("true")]
        [TestCase("false")]
        public void TestValidateClientStoreTemporaryCredentialProperty(string expectedClientStoreTemporaryCredential)
        {
            // arrange
            var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;CLIENT_STORE_TEMPORARY_CREDENTIAL={expectedClientStoreTemporaryCredential}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, null);

            // assert
            Assert.AreEqual(expectedClientStoreTemporaryCredential, properties[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL]);
        }

        [Test]
        public void TestFailWhenClientStoreTemporaryCredentialContainsInvalidValue()
        {
            // arrange
            var invalidValue = "invalidValue";
            var invalidConnectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;CLIENT_STORE_TEMPORARY_CREDENTIAL={invalidValue}";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(invalidConnectionString, null));

            Assert.That(thrown.Message, Does.Contain($"Invalid parameter value  for CLIENT_STORE_TEMPORARY_CREDENTIAL"));
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) },
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

        private static string DefaultValue(SFSessionProperty property)
        {
            var defaultValue = property.GetAttribute<SFSessionPropertyAttr>().defaultValue;
            var defaultNonWindowsValue = property.GetAttribute<SFSessionPropertyAttr>().defaultNonWindowsValue;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return defaultValue;
            return defaultNonWindowsValue ?? defaultValue;
        }

        internal class TestCase
        {
            public string ConnectionString { get; set; }
            public SecureString SecurePassword { get; set; }
            public SFSessionProperties ExpectedProperties { get; set; }
        }
    }
}
