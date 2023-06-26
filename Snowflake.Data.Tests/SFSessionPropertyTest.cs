/*
 * Copyright (c) 2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Collections.Generic;
using Snowflake.Data.Core;
using System.Security;
using NUnit.Framework;

namespace Snowflake.Data.Tests
{
    class SFSessionPropertyTest
    {

        [Test, TestCaseSource("ConnectionStringTestCases")]
        public void ShouldParseProperties(TestCase testcase)
        {
            // when
            var properties = SFSessionProperties.parseConnectionString(
                testcase.ConnectionString,
                testcase.SecurePassword);

            // then
            CollectionAssert.AreEquivalent(testcase.ExpectedProperties, properties);
        }
        
        public static IEnumerable<TestCase> ConnectionStringTestCases()
        {
            string defAccount = "testaccount";
            string defUser = "testuser";
            string defHost = "testaccount.snowflakecomputing.com";
            string defAuthenticator = "snowflake";
            string defScheme = "https";
            string defConnectionTimeout = "120";
            string defBrowserResponseTime = "120";
            string defPassword = "123";
            string defPort = "443";

            string defProxyHost = "proxy.com";
            string defProxyPort = "1234";
            string defNonProxyHosts = "localhost";
            
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
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime }
                }
            };
            var testCaseWithBrowserResponseTimeout = new TestCase()
            {
                ConnectionString = $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};BROWSER_RESPONSE_TIMEOUT=180",
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
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "180" }
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
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime }
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
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, defBrowserResponseTime }
                },
                ConnectionString =
                    $"ACCOUNT={defAccount};USER={defUser};PASSWORD={defPassword};proxyHost=proxy.com;proxyPort=1234;nonProxyHosts=localhost"
            };            
            return new TestCase[]
            {
                simpleTestCase,
                testCaseWithBrowserResponseTimeout,
                testCaseWithProxySettings,
                testCaseThatDefaultForUseProxyIsFalse
            };
        }
        
        internal class TestCase
        {
            public string ConnectionString { get; set; }
            public SecureString SecurePassword { get; set; }
            public SFSessionProperties ExpectedProperties { get; set; }
        }
    }
}
