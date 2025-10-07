using System.Collections.Generic;
using System.Runtime.InteropServices;
using Snowflake.Data.Core;
using System.Security;
using Moq;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Tests.UnitTests
{

    class SFSessionPropertyTest
    {
        private const string DifferentHostsWarning = "Properties OAUTHAUTHORIZATIONURL and OAUTHTOKENREQUESTURL are configured for a different host";

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
        [TestCase("true")]
        [TestCase("false")]
        public void TestValidateClientStoreTemporaryCredentialProperty(string expectedClientStoreTemporaryCredential)
        {
            // arrange
            var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;CLIENT_STORE_TEMPORARY_CREDENTIAL={expectedClientStoreTemporaryCredential}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

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
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(invalidConnectionString, new SessionPropertiesContext()));

            Assert.That(thrown.Message, Does.Contain($"Invalid parameter value  for CLIENT_STORE_TEMPORARY_CREDENTIAL"));
        }

        [Test]
        [TestCase("ACCOUNT=test;USER=test;PASSWORD=test;")]
        [TestCase("ACCOUNT=test;USER=test;PASSWORD=test;OAUTHCLIENTSECRET=ignored_value;")]
        public void TestParseOAuthClientSecretProvidedExternally(string connectionString)
        {
            // arrange
            var oauthClientSecret = "abc";
            var secureOAuthClientSecret = SecureStringHelper.Encode(oauthClientSecret);

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext { OAuthClientSecret = secureOAuthClientSecret });

            // assert
            Assert.AreEqual(oauthClientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
        }

        [Test]
        public void TestNoOAuthPropertiesFound()
        {
            // arrange
            var connectionString = "ACCOUNT=test;USER=test;PASSWORD=test";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.OAUTHCLIENTID, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.OAUTHCLIENTSECRET, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.OAUTHSCOPE, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.OAUTHREDIRECTURI, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.OAUTHAUTHORIZATIONURL, out var _));
            Assert.IsFalse(properties.TryGetValue(SFSessionProperty.OAUTHTOKENREQUESTURL, out var _));
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
            var enableSingleUseRefreshTokens = "true";
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId={clientId};oauthClientSecret={clientSecret};oauthScope={scope};oauthRedirectUri={redirectUri};oauthAuthorizationUrl={authorizationUrl};oauthTokenRequestUrl={tokenUrl};oauthEnableSingleUseRefreshTokens={enableSingleUseRefreshTokens}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
            Assert.AreEqual(scope, properties[SFSessionProperty.OAUTHSCOPE]);
            Assert.AreEqual(redirectUri, properties[SFSessionProperty.OAUTHREDIRECTURI]);
            Assert.AreEqual(authorizationUrl, properties[SFSessionProperty.OAUTHAUTHORIZATIONURL]);
            Assert.AreEqual(tokenUrl, properties[SFSessionProperty.OAUTHTOKENREQUESTURL]);
            Assert.AreEqual(enableSingleUseRefreshTokens, properties[SFSessionProperty.OAUTHENABLESINGLEUSEREFRESHTOKENS]);
        }

        [Test]
        public void TestOAuthClientCredentialsAllParameters()
        {
            // arrange
            var clientId = "abc";
            var clientSecret = "def";
            var scope = "ghi";
            var tokenUrl = "https://okta.com/token-request";
            var connectionString = $"AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId={clientId};oauthClientSecret={clientSecret};oauthScope={scope};oauthTokenRequestUrl={tokenUrl};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
            Assert.AreEqual(scope, properties[SFSessionProperty.OAUTHSCOPE]);
            Assert.AreEqual(tokenUrl, properties[SFSessionProperty.OAUTHTOKENREQUESTURL]);
        }

        [Test]
        public void TestOAuthAuthorizationCodeFlowWithMinimalParameters()
        {
            // arrange
            var clientId = "abc";
            var clientSecret = "def";
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId={clientId};oauthClientSecret={clientSecret};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
        }

        [Test]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthAuthorizationUrl=https://test.snowflakecomputing.com/authorize;oauthTokenRequestUrl=https://test.snowflakecomputing.com/token-request;")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthAuthorizationUrl=https://test.snowflakecomputing.cn/authorize;oauthTokenRequestUrl=https://test.snowflakecomputing.cn/token-request;")]
        public void TestOAuthAuthorizationCodeFlowDefaultClientIdAndSecret(string connectionString)
        {
            // arrange
            const string ExpectedClientIdAndSecret = "LOCAL_APPLICATION";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(ExpectedClientIdAndSecret, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.AreEqual(ExpectedClientIdAndSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
        }

        [Test]
        public void TestOAuthClientCredentialsWithMinimalParameters()
        {
            // arrange
            var clientId = "abc";
            var clientSecret = "def";
            var tokenUrl = "https://okta.com/token_request";
            var connectionString = $"AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;ROLE=ANALYST;oauthClientId={clientId};oauthClientSecret={clientSecret};oauthTokenRequestUrl={tokenUrl}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.AreEqual(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
            Assert.AreEqual(tokenUrl, properties[SFSessionProperty.OAUTHTOKENREQUESTURL]);
        }


        [Test]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientSecret=def;", "Required property OAUTHCLIENTID is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;", "Required property OAUTHCLIENTSECRET is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;", "Required property OAUTHSCOPE or ROLE is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize", "Required property OAUTHTOKENREQUESTURL is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=https://okta.com/token-request", "Required property OAUTHAUTHORIZATIONURL is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=okta.com/authorize;oauthTokenRequestUrl=https://okta.com/token-request", "Missing or invalid protocol in the OAUTHAUTHORIZATIONURL url")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize;oauthTokenRequestUrl=okta.com/token-request", "Missing or invalid protocol in the OAUTHTOKENREQUESTURL url")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize;oauthTokenRequestUrl=https://okta.com/token-request", "Required property OAUTHCLIENTID is not provided")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;oauthClientSecret=def;poolingEnabled=true;", "You cannot enable pooling for oauth authorization code authentication without specifying a user in the connection string.")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;oauthClientSecret=def;oauthEnableSingleUseRefreshTokens=xyz;", "Parameter OAUTHENABLESINGLEUSEREFRESHTOKENS value should be parsable as boolean.")]
        public void TestOAuthAuthorizationCodeMissingOrInvalidParameters(string connectionString, string errorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(errorMessage));
        }

        [Test]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;ROLE=ANALYST;oauthClientSecret=def;oauthTokenRequestUrl=http://okta.com/token-request;", "Required property OAUTHCLIENTID is not provided")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;oauthTokenRequestUrl=http://okta.com/token-request;", "Required property OAUTHCLIENTSECRET is not provided")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthTokenRequestUrl=http://okta.com/token-request;", "Required property OAUTHSCOPE or ROLE is not provided")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;", "Required property OAUTHTOKENREQUESTURL is not provided")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=okta.com/token-request;", "Missing or invalid protocol in the OAUTHTOKENREQUESTURL url")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthScope=ghi;oauthTokenRequestUrl=https://test.snowflakecomputing.com;", "Required property OAUTHCLIENTID is not provided")]
        public void TestOAuthClientCredentialsMissingOrInvalidParameters(string connectionString, string errorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(errorMessage));
        }


        [Test, NonParallelizable]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=http://okta.com/authorize;oauthTokenRequestUrl=https://okta.com/token-request", "Insecure OAUTHAUTHORIZATIONURL property value. It does not start with 'https://'")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize;oauthTokenRequestUrl=http://okta.com/token-request", "Insecure OAUTHTOKENREQUESTURL property value. It does not start with 'https://'")]
        [TestCase("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;scheme=http", "Insecure SCHEME property value. Http protocol is not secure.")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=https://okta.com/token-request;scheme=http", "Insecure SCHEME property value. Http protocol is not secure.")]
        [TestCase("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=http://okta.com/token-request;", "Insecure OAUTHTOKENREQUESTURL property value. It does not start with 'https://'")]
        public void TestWarningOnHttpCommunicationWithIdentityProviderAndSnowflakeServer(string connectionString, string expectedWarning)
        {
            // arrange
            var logger = new Mock<SFLogger>();
            var oldLogger = SFSessionProperties.ReplaceLogger(logger.Object);
            try
            {
                // act
                SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

                // assert
                logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains(expectedWarning)), null), Times.Once);
            }
            finally
            {
                SFSessionProperties.ReplaceLogger(oldLogger);
            }
        }

        [Test, NonParallelizable]
        [TestCase("https://okta.com/authorize", "https://other.okta.com/token-request")]
        public void TestWarningOnDifferentOAuthHosts(string authorizationUrl, string tokenUrl)
        {
            // arrange
            var logger = new Mock<SFLogger>();
            var oldLogger = SFSessionProperties.ReplaceLogger(logger.Object);
            try
            {
                var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl={authorizationUrl};oauthTokenRequestUrl={tokenUrl};";

                // act
                SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

                // assert
                logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains(DifferentHostsWarning)), null), Times.Once);
            }
            finally
            {
                SFSessionProperties.ReplaceLogger(oldLogger);
            }
        }

        [Test, NonParallelizable]
        [TestCase("https://okta.com/authorize", "https://okta.com/token-request")]
        public void TestNoWarningOnTheSameOAuthHosts(string authorizationUrl, string tokenUrl)
        {
            // arrange
            var logger = new Mock<SFLogger>();
            var oldLogger = SFSessionProperties.ReplaceLogger(logger.Object);
            try
            {
                var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl={authorizationUrl};oauthTokenRequestUrl={tokenUrl};";

                // act
                SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

                // assert
                logger.Verify(l => l.Warn(It.Is<string>(s => s.Contains(DifferentHostsWarning)), null), Times.Never);
            }
            finally
            {
                SFSessionProperties.ReplaceLogger(oldLogger);
            }
        }

        [Test]
        public void TestProgrammaticAccessTokenParameters()
        {
            // arrange
            var token = "testToken";
            var connectionString = $"AUTHENTICATOR=programmatic_access_token;ACCOUNT=test;TOKEN={token};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(token, properties[SFSessionProperty.TOKEN]);
        }

        [Test]
        public void TestProgrammaticAccessTokenProvidedExternally()
        {
            // arrange
            var token = "testToken";
            var connectionString = $"AUTHENTICATOR=programmatic_access_token;ACCOUNT=test;";
            var secureToken = SecureStringHelper.Encode(token);

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext { Token = secureToken });

            // assert
            Assert.AreEqual(token, properties[SFSessionProperty.TOKEN]);
        }

        [Test]
        [TestCase("AUTHENTICATOR=programmatic_access_token;ACCOUNT=test;USER=testUser;", "Required property TOKEN is not provided.")]
        public void TestInvalidProgrammaticAccessTokenParameters(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(expectedErrorMessage));
        }

        [Test]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;PORT=abc;", "Invalid parameter value PORT for a non integer value")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;HOST=http://test.snowflakecomputing.com;", "Connection string is invalid: scheme/host/port properties do not combine into a valid uri")]
        public void TestFailOnInvalidSchemeHostPortConfiguration(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(expectedErrorMessage));
        }

        [Test]
        [TestCase("authenticator=workload_identity;account=test;workload_identity_provider=abc;", "Connection string is invalid: Unknown value of workload_identity_provider parameter.")]
        [TestCase("authenticator=workload_identity;account=test;workload_identity_provider=OIDC;", "Required property TOKEN is not provided.")]
        public void TestFailOnWrongWifConfiguration(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(expectedErrorMessage));
        }

        [Test]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;", "disabled", "true", "true", "false")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;certRevocationCheckMode=enabled;enableCrlDiskCaching=false;enableCrlInMemoryCaching=false;allowCertificatesWithoutCrlUrl=true;", "enabled", "false", "false", "true")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;certRevocationCheckMode=advisory;enableCrlDiskCaching=false;enableCrlInMemoryCaching=true;allowCertificatesWithoutCrlUrl=true;", "advisory", "false", "true", "true")]
        public void TestParseCrlCheckParameters(
            string connectionString,
            string expectedCertRevocationCheckMode,
            string expectedEnableCrlDiskCaching,
            string expectedEnableCrlInMemoryCaching,
            string expectedAllowCertificatesWithoutCrlUrl)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(expectedCertRevocationCheckMode, properties[SFSessionProperty.CERTREVOCATIONCHECKMODE]);
            Assert.AreEqual(expectedEnableCrlDiskCaching, properties[SFSessionProperty.ENABLECRLDISKCACHING]);
            Assert.AreEqual(expectedEnableCrlInMemoryCaching, properties[SFSessionProperty.ENABLECRLINMEMORYCACHING]);
            Assert.AreEqual(expectedAllowCertificatesWithoutCrlUrl, properties[SFSessionProperty.ALLOWCERTIFICATESWITHOUTCRLURL]);
        }

        [Test]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;certRevocationCheckMode=unknown;", "Parameter CERTREVOCATIONCHECKMODE should have one of following values: ENABLED, ADVISORY, DISABLED, NATIVE.")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;enableCrlDiskCaching=unknown;", "Parameter ENABLECRLDISKCACHING should have a boolean value.")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;enableCrlInMemoryCaching=unknown;", "Parameter ENABLECRLINMEMORYCACHING should have a boolean value.")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;allowCertificatesWithoutCrlUrl=unknown;", "Parameter ALLOWCERTIFICATESWITHOUTCRLURL should have a boolean value.")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=abc;", "Parameter CRLDOWNLOADTIMEOUT should have an integer value.")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=0;", "Parameter CRLDOWNLOADTIMEOUT should be greater than 0.")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=-5;", "Parameter CRLDOWNLOADTIMEOUT should be greater than 0.")]
        public void TestFailOnInvalidCrlParameters(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.That(thrown.Message, Does.Contain(expectedErrorMessage));
        }

        [Test]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;", "10")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=30;", "30")]
        [TestCase("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=120;", "120")]
        public void TestParseCrlDownloadTimeout(string connectionString, string expectedTimeout)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.AreEqual(expectedTimeout, properties[SFSessionProperty.CRLDOWNLOADTIMEOUT]);
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
                    { SFSessionProperty.PASSCODEINPASSWORD, DefaultValue(SFSessionProperty.PASSCODEINPASSWORD) },
                    { SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL, DefaultValue(SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL) }
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
