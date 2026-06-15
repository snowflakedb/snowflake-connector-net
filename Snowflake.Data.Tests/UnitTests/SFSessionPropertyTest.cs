using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Snowflake.Data.Core;
using System.Security;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class SFSessionPropertyTest
    {
        [SFTheory, MemberData(nameof(ConnectionStringTestCases))]
        public void TestThatPropertiesAreParsed(TestCase testcase)
        {
            // arrange
            var propertiesContext = new SessionPropertiesContext { Password = testcase.SecurePassword };

            // act
            var properties = SFSessionProperties.ParseConnectionString(testcase.ConnectionString, propertiesContext);

            // assert
            foreach (var kvp in testcase.ExpectedProperties)
            {
                Assert.True(properties.ContainsKey(kvp.Key), $"Missing key: {kvp.Key}");
                Assert.Equal(kvp.Value, properties[kvp.Key]);
            }
        }

        [SFTheory]
        [InlineData("a", "a", "a.snowflakecomputing.com")]
        [InlineData("ab", "ab", "ab.snowflakecomputing.com")]
        [InlineData("a.b", "a", "a.b.snowflakecomputing.com")]
        [InlineData("a-b", "a-b", "a-b.snowflakecomputing.com")]
        [InlineData("a_b", "a_b", "a-b.snowflakecomputing.com")]
        [InlineData("abc", "abc", "abc.snowflakecomputing.com")]
        [InlineData("xy12345.us-east-2.aws", "xy12345", "xy12345.us-east-2.aws.snowflakecomputing.com")]
        public void TestValidateCorrectAccountNames(string accountName, string expectedAccountName, string expectedHost)
        {
            // arrange
            var connectionString = $"ACCOUNT={accountName};USER=test;PASSWORD=test;";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedAccountName, properties[SFSessionProperty.ACCOUNT]);
            Assert.Equal(expectedHost, properties[SFSessionProperty.HOST]);
        }

        [SFTheory]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;FILE_TRANSFER_MEMORY_THRESHOLD=0;", "Error: Invalid parameter value 0 for FILE_TRANSFER_MEMORY_THRESHOLD")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;FILE_TRANSFER_MEMORY_THRESHOLD=xyz;", "Error: Invalid parameter value xyz for FILE_TRANSFER_MEMORY_THRESHOLD")]
        [InlineData("ACCOUNT=testaccount?;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value testaccount? for ACCOUNT")]
        [InlineData("ACCOUNT=complicated.long.testaccount?;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value complicated.long.testaccount? for ACCOUNT")]
        [InlineData("ACCOUNT=?testaccount;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value ?testaccount for ACCOUNT")]
        [InlineData("ACCOUNT=.testaccount;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value .testaccount for ACCOUNT")]
        [InlineData("ACCOUNT=testaccount.;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value testaccount. for ACCOUNT")]
        [InlineData("ACCOUNT=test%account;USER=testuser;PASSWORD=testpassword", "Error: Invalid parameter value test%account for ACCOUNT")]
        public void TestThatItFailsForWrongConnectionParameter(string connectionString, string expectedErrorMessagePart)
        {
            // act
            var exception = Assert.Throws<SnowflakeDbException>(
                () => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext())
            );

            // assert
            Assert.Equal(SFError.INVALID_CONNECTION_PARAMETER_VALUE.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.True(exception.Message.Contains(expectedErrorMessagePart));
        }

        [SFTheory]
        [InlineData("ACCOUNT=;USER=testuser;PASSWORD=testpassword")]
        [InlineData("USER=testuser;PASSWORD=testpassword")]
        public void TestThatItFailsIfNoAccountSpecified(string connectionString)
        {
            // act
            var exception = Assert.Throws<SnowflakeDbException>(
                () => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext())
            );

            // assert
            Assert.Equal(SFError.MISSING_CONNECTION_PROPERTY.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
        }

        [SFTheory]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=", null)]
        [InlineData("ACCOUNT=testaccount;USER=testuser;", "")]
        [InlineData("authenticator=https://okta.com;ACCOUNT=testaccount;USER=testuser;PASSWORD=", null)]
        [InlineData("authenticator=https://okta.com;ACCOUNT=testaccount;USER=testuser;", "")]
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
            Assert.Equal(SFError.MISSING_CONNECTION_PROPERTY.GetAttribute<SFErrorAttr>().errorCode, exception.ErrorCode);
            Assert.Contains("Required property PASSWORD is not provided", exception.Message);
        }

        [SFFact]
        public void TestParsePasscode()
        {
            // arrange
            var expectedPasscode = "abc";
            var connectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;PASSCODE={expectedPasscode}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedPasscode, properties[SFSessionProperty.PASSCODE]);
        }

        [SFFact]
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
            Assert.Equal(expectedPasscode, properties[SFSessionProperty.PASSCODE]);
        }

        [SFTheory]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;PASSCODE=")]
        public void TestDoNotParsePasscodeWhenNotProvided(string connectionString)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.False(properties.TryGetValue(SFSessionProperty.PASSCODE, out _));
        }

        [SFTheory]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;", "false")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=", "false")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=true", "true")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=TRUE", "TRUE")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=false", "false")]
        [InlineData("ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=FALSE", "FALSE")]
        public void TestParsePasscodeInPassword(string connectionString, string expectedPasscodeInPassword)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.True(properties.TryGetValue(SFSessionProperty.PASSCODEINPASSWORD, out var passcodeInPassword));
            Assert.Equal(expectedPasscodeInPassword, passcodeInPassword);
        }

        [SFFact]
        public void TestFailWhenInvalidPasscodeInPassword()
        {
            // arrange
            var invalidConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;passcodeInPassword=abc";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(invalidConnectionString, new SessionPropertiesContext()));

            Assert.Contains("Invalid parameter value  for PASSCODEINPASSWORD", thrown.Message);
        }

        [SFFact]
        public void TestTurkishCultureInvariantPropertyParsing()
        {
            // arrange
            var currentCulture = System.Threading.Thread.CurrentThread.CurrentCulture;
            var currentUICulture = System.Threading.Thread.CurrentThread.CurrentUICulture;
            var turkishCulture = new System.Globalization.CultureInfo("tr-TR");

            try
            {
                // Set Turkish culture where 'i'.ToUpper() becomes 'İ' instead of 'I'
                System.Threading.Thread.CurrentThread.CurrentCulture = turkishCulture;
                System.Threading.Thread.CurrentThread.CurrentUICulture = turkishCulture;

                var connectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;authenticator=snowflake;private_key=dummykey";

                // act - this should not throw an exception even with Turkish culture
                var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

                // assert
                Assert.Equal("snowflake", properties[SFSessionProperty.AUTHENTICATOR]);
                Assert.Equal("dummykey", properties[SFSessionProperty.PRIVATE_KEY]);
            }
            finally
            {
                // restore original culture
                System.Threading.Thread.CurrentThread.CurrentCulture = currentCulture;
                System.Threading.Thread.CurrentThread.CurrentUICulture = currentUICulture;
            }
        }

        [SFFact]
        public void TestTurkishCultureDemonstratesProblemAndInvalidPropertiesStillFail()
        {
            // arrange
            var currentCulture = System.Threading.Thread.CurrentThread.CurrentCulture;
            var currentUICulture = System.Threading.Thread.CurrentThread.CurrentUICulture;
            var turkishCulture = new System.Globalization.CultureInfo("tr-TR");

            try
            {
                // Set Turkish culture
                System.Threading.Thread.CurrentThread.CurrentCulture = turkishCulture;
                System.Threading.Thread.CurrentThread.CurrentUICulture = turkishCulture;

                // Demonstrate the Turkish culture problem with string casing
                // In Turkish culture, 'i'.ToUpper() becomes 'İ' (U+0130) instead of 'I' (U+0049)
                var testString = "authenticator";
                var cultureDependent = testString.ToUpper(); // Would be "AUTHENTİCATOR" in Turkish
                var cultureInvariant = testString.ToUpperInvariant(); // Always "AUTHENTICATOR"

                // Verify the Turkish culture issue exists
                Assert.NotEqual(cultureDependent, cultureInvariant);
                Assert.True(cultureDependent.Contains("İ"), "Turkish ToUpper() should contain Turkish dotted capital I");
                Assert.True(cultureInvariant.Contains("I"), "Invariant ToUpper() should contain ASCII capital I");

                // Verify that invalid properties still throw exceptions even with Turkish culture
                var invalidConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;invalidproperty=somevalue";

                // act & assert - invalid properties should be ignored (logged as debug), not throw exceptions
                var properties = SFSessionProperties.ParseConnectionString(invalidConnectionString, new SessionPropertiesContext());

                // Verify valid properties are still parsed correctly
                Assert.Equal("testaccount", properties[SFSessionProperty.ACCOUNT]);
                Assert.Equal("testuser", properties[SFSessionProperty.USER]);
                Assert.Equal("testpassword", properties[SFSessionProperty.PASSWORD]);

                // Verify invalid property is not present (can't use Enum.Parse here as it would throw)
                Assert.False(properties.Any(p => p.Key.ToString().Equals("INVALIDPROPERTY", StringComparison.OrdinalIgnoreCase)),
                    "Invalid property should not be present in parsed properties");
            }
            finally
            {
                // restore original culture
                System.Threading.Thread.CurrentThread.CurrentCulture = currentCulture;
                System.Threading.Thread.CurrentThread.CurrentUICulture = currentUICulture;
            }
        }

        [SFTheory]
        [InlineData("true")]
        [InlineData("false")]
        public void TestClientTelemetryEnabledPropertyIsReadFromConnectionString(string value)
        {
            // arrange
            var connectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;CLIENT_TELEMETRY_ENABLED={value}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(value, properties[SFSessionProperty.CLIENT_TELEMETRY_ENABLED]);
        }

        [SFTheory]
        [InlineData("DB", 1, "\"testdb\"")]
        [InlineData("SCHEMA", 6, "\"quotedSchema\"")]
        [InlineData("ROLE", 5, "\"userrole\"")]
        [InlineData("WAREHOUSE", 9, "\"warehouse  test\"")]
        public void TestValidateSupportEscapedQuotesValuesForObjectProperties(string propertyName, int sessionProperty, string value)
        {
            // arrange
            var connectionString = $"ACCOUNT=test;{propertyName}={value};USER=test;PASSWORD=test;";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(value, properties[(SFSessionProperty)sessionProperty]);
        }

        [SFTheory]
        [InlineData("DB", 1, "testdb", "testdb")]
        [InlineData("DB", 1, "\"testdb\"", "\"testdb\"")]
        [InlineData("DB", 1, "\"\"\"testDB\"\"\"", "\"\"testDB\"\"")]
        [InlineData("DB", 1, "\"\"\"test\"\"DB\"\"\"", "\"\"test\"DB\"\"")]
        [InlineData("SCHEMA", 6, "\"quoted\"\"Schema\"", "\"quoted\"Schema\"")]
        public void TestValidateSupportEscapedQuotesInsideValuesForObjectProperties(string propertyName, int sessionProperty, string value, string expectedValue)
        {
            // arrange
            var connectionString = $"ACCOUNT=test;{propertyName}={value};USER=test;PASSWORD=test;";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedValue, properties[(SFSessionProperty)sessionProperty]);
        }

        [SFTheory]
        [InlineData("true")]
        [InlineData("false")]
        public void TestValidateDisableSamlUrlCheckProperty(string expectedDisableSamlUrlCheck)
        {
            // arrange
            var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;DISABLE_SAML_URL_CHECK={expectedDisableSamlUrlCheck}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedDisableSamlUrlCheck, properties[SFSessionProperty.DISABLE_SAML_URL_CHECK]);
        }

        [SFTheory]
        [InlineData("account.snowflakecomputing.cn", "Connecting to CHINA Snowflake domain")]
        [InlineData("account.snowflakecomputing.com", "Connecting to GLOBAL Snowflake domain")]
        public void TestResolveConnectionArea(string host, string expectedMessage)
        {
            // act
            var message = SFSessionProperties.ResolveConnectionAreaMessage(host);

            // assert
            Assert.Equal(expectedMessage, message);
        }

        [SFTheory]
        [InlineData("true")]
        [InlineData("false")]
        public void TestValidateClientStoreTemporaryCredentialProperty(string expectedClientStoreTemporaryCredential)
        {
            // arrange
            var connectionString = $"ACCOUNT=account;USER=test;PASSWORD=test;CLIENT_STORE_TEMPORARY_CREDENTIAL={expectedClientStoreTemporaryCredential}";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedClientStoreTemporaryCredential, properties[SFSessionProperty.CLIENT_STORE_TEMPORARY_CREDENTIAL]);
        }

        [SFFact]
        public void TestFailWhenClientStoreTemporaryCredentialContainsInvalidValue()
        {
            // arrange
            var invalidValue = "invalidValue";
            var invalidConnectionString = $"ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;CLIENT_STORE_TEMPORARY_CREDENTIAL={invalidValue}";

            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(invalidConnectionString, new SessionPropertiesContext()));

            Assert.Contains($"Invalid parameter value  for CLIENT_STORE_TEMPORARY_CREDENTIAL", thrown.Message);
        }

        [SFTheory]
        [InlineData("ACCOUNT=test;USER=test;PASSWORD=test;")]
        [InlineData("ACCOUNT=test;USER=test;PASSWORD=test;OAUTHCLIENTSECRET=ignored_value;")]
        public void TestParseOAuthClientSecretProvidedExternally(string connectionString)
        {
            // arrange
            var oauthClientSecret = "abc";
            var secureOAuthClientSecret = SecureStringHelper.Encode(oauthClientSecret);

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext { OAuthClientSecret = secureOAuthClientSecret });

            // assert
            Assert.Equal(oauthClientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
        }

        [SFFact]
        public void TestNoOAuthPropertiesFound()
        {
            // arrange
            var connectionString = "ACCOUNT=test;USER=test;PASSWORD=test";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.False(properties.TryGetValue(SFSessionProperty.OAUTHCLIENTID, out var _));
            Assert.False(properties.TryGetValue(SFSessionProperty.OAUTHCLIENTSECRET, out var _));
            Assert.False(properties.TryGetValue(SFSessionProperty.OAUTHSCOPE, out var _));
            Assert.False(properties.TryGetValue(SFSessionProperty.OAUTHREDIRECTURI, out var _));
            Assert.False(properties.TryGetValue(SFSessionProperty.OAUTHAUTHORIZATIONURL, out var _));
            Assert.False(properties.TryGetValue(SFSessionProperty.OAUTHTOKENREQUESTURL, out var _));
        }

        [SFFact]
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
            Assert.Equal(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.Equal(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
            Assert.Equal(scope, properties[SFSessionProperty.OAUTHSCOPE]);
            Assert.Equal(redirectUri, properties[SFSessionProperty.OAUTHREDIRECTURI]);
            Assert.Equal(authorizationUrl, properties[SFSessionProperty.OAUTHAUTHORIZATIONURL]);
            Assert.Equal(tokenUrl, properties[SFSessionProperty.OAUTHTOKENREQUESTURL]);
            Assert.Equal(enableSingleUseRefreshTokens, properties[SFSessionProperty.OAUTHENABLESINGLEUSEREFRESHTOKENS]);
        }

        [SFFact]
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
            Assert.Equal(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.Equal(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
            Assert.Equal(scope, properties[SFSessionProperty.OAUTHSCOPE]);
            Assert.Equal(tokenUrl, properties[SFSessionProperty.OAUTHTOKENREQUESTURL]);
        }

        [SFFact]
        public void TestOAuthAuthorizationCodeFlowWithMinimalParameters()
        {
            // arrange
            var clientId = "abc";
            var clientSecret = "def";
            var connectionString = $"AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId={clientId};oauthClientSecret={clientSecret};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.Equal(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
        }

        [SFTheory]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthAuthorizationUrl=https://test.snowflakecomputing.com/authorize;oauthTokenRequestUrl=https://test.snowflakecomputing.com/token-request;")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthAuthorizationUrl=https://test.snowflakecomputing.cn/authorize;oauthTokenRequestUrl=https://test.snowflakecomputing.cn/token-request;")]
        public void TestOAuthAuthorizationCodeFlowDefaultClientIdAndSecret(string connectionString)
        {
            // arrange
            const string ExpectedClientIdAndSecret = "LOCAL_APPLICATION";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(ExpectedClientIdAndSecret, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.Equal(ExpectedClientIdAndSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
        }

        [SFFact]
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
            Assert.Equal(clientId, properties[SFSessionProperty.OAUTHCLIENTID]);
            Assert.Equal(clientSecret, properties[SFSessionProperty.OAUTHCLIENTSECRET]);
            Assert.Equal(tokenUrl, properties[SFSessionProperty.OAUTHTOKENREQUESTURL]);
        }


        [SFTheory]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientSecret=def;", "Required property OAUTHCLIENTID is not provided")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;", "Required property OAUTHCLIENTSECRET is not provided")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;", "Required property OAUTHSCOPE or ROLE is not provided")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize", "Required property OAUTHTOKENREQUESTURL is not provided")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=https://okta.com/token-request", "Required property OAUTHAUTHORIZATIONURL is not provided")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=okta.com/authorize;oauthTokenRequestUrl=https://okta.com/token-request", "Missing or invalid protocol in the OAUTHAUTHORIZATIONURL url")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize;oauthTokenRequestUrl=okta.com/token-request", "Missing or invalid protocol in the OAUTHTOKENREQUESTURL url")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;oauthScope=ghi;oauthAuthorizationUrl=https://okta.com/authorize;oauthTokenRequestUrl=https://okta.com/token-request", "Required property OAUTHCLIENTID is not provided")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;oauthClientSecret=def;poolingEnabled=true;", "You cannot enable pooling for oauth authorization code authentication without specifying a user in the connection string.")]
        [InlineData("AUTHENTICATOR=oauth_authorization_code;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;oauthClientSecret=def;oauthEnableSingleUseRefreshTokens=xyz;", "Parameter OAUTHENABLESINGLEUSEREFRESHTOKENS value should be parsable as boolean.")]
        public void TestOAuthAuthorizationCodeMissingOrInvalidParameters(string connectionString, string errorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.Contains(errorMessage, thrown.Message);
        }

        [SFTheory]
        [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;ROLE=ANALYST;oauthClientSecret=def;oauthTokenRequestUrl=http://okta.com/token-request;", "Required property OAUTHCLIENTID is not provided")]
        [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;ROLE=ANALYST;oauthClientId=abc;oauthTokenRequestUrl=http://okta.com/token-request;", "Required property OAUTHCLIENTSECRET is not provided")]
        [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthTokenRequestUrl=http://okta.com/token-request;", "Required property OAUTHSCOPE or ROLE is not provided")]
        [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;", "Required property OAUTHTOKENREQUESTURL is not provided")]
        [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthClientId=abc;oauthClientSecret=def;oauthScope=ghi;oauthTokenRequestUrl=okta.com/token-request;", "Missing or invalid protocol in the OAUTHTOKENREQUESTURL url")]
        [InlineData("AUTHENTICATOR=oauth_client_credentials;ACCOUNT=test;oauthScope=ghi;oauthTokenRequestUrl=https://test.snowflakecomputing.com;", "Required property OAUTHCLIENTID is not provided")]
        public void TestOAuthClientCredentialsMissingOrInvalidParameters(string connectionString, string errorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.Contains(errorMessage, thrown.Message);
        }

        [SFFact]
        public void TestProgrammaticAccessTokenParameters()
        {
            // arrange
            var token = "testToken";
            var connectionString = $"AUTHENTICATOR=programmatic_access_token;ACCOUNT=test;TOKEN={token};";

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(token, properties[SFSessionProperty.TOKEN]);
        }

        [SFFact]
        public void TestProgrammaticAccessTokenProvidedExternally()
        {
            // arrange
            var token = "testToken";
            var connectionString = $"AUTHENTICATOR=programmatic_access_token;ACCOUNT=test;";
            var secureToken = SecureStringHelper.Encode(token);

            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext { Token = secureToken });

            // assert
            Assert.Equal(token, properties[SFSessionProperty.TOKEN]);
        }

        [SFTheory]
        [InlineData("AUTHENTICATOR=programmatic_access_token;ACCOUNT=test;USER=testUser;", "Required property TOKEN is not provided.")]
        public void TestInvalidProgrammaticAccessTokenParameters(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.Contains(expectedErrorMessage, thrown.Message);
        }

        [SFTheory]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;PORT=abc;", "Invalid parameter value PORT for a non integer value")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;HOST=http://test.snowflakecomputing.com;", "Connection string is invalid: scheme/host/port properties do not combine into a valid uri")]
        public void TestFailOnInvalidSchemeHostPortConfiguration(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.Contains(expectedErrorMessage, thrown.Message);
        }

        [SFTheory]
        [InlineData("authenticator=workload_identity;account=test;workload_identity_provider=abc;", "Connection string is invalid: Unknown value of workload_identity_provider parameter.")]
        [InlineData("authenticator=workload_identity;account=test;workload_identity_provider=OIDC;", "Required property TOKEN is not provided.")]
        public void TestFailOnWrongWifConfiguration(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.Contains(expectedErrorMessage, thrown.Message);
        }

        [SFTheory]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;", "disabled", "true", "true", "false")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;certRevocationCheckMode=enabled;enableCrlDiskCaching=false;enableCrlInMemoryCaching=false;allowCertificatesWithoutCrlUrl=true;", "enabled", "false", "false", "true")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;certRevocationCheckMode=advisory;enableCrlDiskCaching=false;enableCrlInMemoryCaching=true;allowCertificatesWithoutCrlUrl=true;", "advisory", "false", "true", "true")]
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
            Assert.Equal(expectedCertRevocationCheckMode, properties[SFSessionProperty.CERTREVOCATIONCHECKMODE]);
            Assert.Equal(expectedEnableCrlDiskCaching, properties[SFSessionProperty.ENABLECRLDISKCACHING]);
            Assert.Equal(expectedEnableCrlInMemoryCaching, properties[SFSessionProperty.ENABLECRLINMEMORYCACHING]);
            Assert.Equal(expectedAllowCertificatesWithoutCrlUrl, properties[SFSessionProperty.ALLOWCERTIFICATESWITHOUTCRLURL]);
        }

        [SFTheory]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;certRevocationCheckMode=unknown;", "")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;enableCrlDiskCaching=unknown;", "Parameter ENABLECRLDISKCACHING should have a boolean value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;enableCrlInMemoryCaching=unknown;", "Parameter ENABLECRLINMEMORYCACHING should have a boolean value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;allowCertificatesWithoutCrlUrl=unknown;", "Parameter ALLOWCERTIFICATESWITHOUTCRLURL should have a boolean value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=abc;", "Parameter CRLDOWNLOADTIMEOUT should have an integer value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=0;", "Parameter CRLDOWNLOADTIMEOUT should be greater than 0.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=-5;", "Parameter CRLDOWNLOADTIMEOUT should be greater than 0.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadMaxSize=abc;", "Parameter CRLDOWNLOADMAXSIZE should have a long value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadMaxSize=1.5;", "Parameter CRLDOWNLOADMAXSIZE should have a long value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadMaxSize=9223372036854775808;", "Parameter CRLDOWNLOADMAXSIZE should have a long value.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadMaxSize=0;", "Parameter CRLDOWNLOADMAXSIZE should be greater than 0.")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadMaxSize=-100;", "Parameter CRLDOWNLOADMAXSIZE should be greater than 0.")]
        public void TestFailOnInvalidCrlParameters(string connectionString, string expectedErrorMessage)
        {
            // act
            var thrown = Assert.Throws<SnowflakeDbException>(() => SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext()));

            // assert
            Assert.Contains(expectedErrorMessage, thrown.Message);
        }

        [SFTheory]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;", "10")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=30;", "30")]
        [InlineData("ACCOUNT=test;USER=testUser;password=testPassword;crlDownloadTimeout=120;", "120")]
        public void TestParseCrlDownloadTimeout(string connectionString, string expectedTimeout)
        {
            // act
            var properties = SFSessionProperties.ParseConnectionString(connectionString, new SessionPropertiesContext());

            // assert
            Assert.Equal(expectedTimeout, properties[SFSessionProperty.CRLDOWNLOADTIMEOUT]);
        }

        public static IEnumerable<object[]> ConnectionStringTestCases()
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
            }.Select(tc => new object[] { tc });
        }

        private static string DefaultValue(SFSessionProperty property)
        {
            var defaultValue = property.GetAttribute<SFSessionPropertyAttr>().defaultValue;
            var defaultNonWindowsValue = property.GetAttribute<SFSessionPropertyAttr>().defaultNonWindowsValue;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return defaultValue;
            return defaultNonWindowsValue ?? defaultValue;
        }

        public class TestCase
        {
            public string ConnectionString { get; set; }
            public SecureString SecurePassword { get; set; }
            internal SFSessionProperties ExpectedProperties { get; set; }
        }
    }
}
