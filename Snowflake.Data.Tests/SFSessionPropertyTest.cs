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
            var simpleTestCase = new TestCase()
            {
                ConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=123;",
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, "testaccount" },
                    { SFSessionProperty.USER, "testuser" },
                    { SFSessionProperty.HOST, "testaccount.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, "snowflake" },
                    { SFSessionProperty.SCHEME, "https" },
                    { SFSessionProperty.CONNECTION_TIMEOUT, "120" },
                    { SFSessionProperty.PASSWORD, "123" },
                    { SFSessionProperty.PORT, "443" },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "60" }
                }
            };
            var testCaseWithProxySettings = new TestCase()
            {
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, "testaccount" },
                    { SFSessionProperty.USER, "testuser" },
                    { SFSessionProperty.HOST, "testaccount.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, "snowflake" },
                    { SFSessionProperty.SCHEME, "https" },
                    { SFSessionProperty.CONNECTION_TIMEOUT, "120" },
                    { SFSessionProperty.PASSWORD, "123" },
                    { SFSessionProperty.PORT, "443" },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.USEPROXY, "true" },
                    { SFSessionProperty.PROXYHOST, "proxy.com" },
                    { SFSessionProperty.PROXYPORT, "1234" },
                    { SFSessionProperty.NONPROXYHOSTS, "localhost" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "60" }
                },
                ConnectionString =
                    "ACCOUNT=testaccount;USER=testuser;PASSWORD=123;useProxy=true;proxyHost=proxy.com;proxyPort=1234;nonProxyHosts=localhost"
            };
            var testCaseThatDefaultForUseProxyIsFalse = new TestCase()
            {
                ExpectedProperties = new SFSessionProperties()
                {
                    { SFSessionProperty.ACCOUNT, "testaccount" },
                    { SFSessionProperty.USER, "testuser" },
                    { SFSessionProperty.HOST, "testaccount.snowflakecomputing.com" },
                    { SFSessionProperty.AUTHENTICATOR, "snowflake" },
                    { SFSessionProperty.SCHEME, "https" },
                    { SFSessionProperty.CONNECTION_TIMEOUT, "120" },
                    { SFSessionProperty.PASSWORD, "123" },
                    { SFSessionProperty.PORT, "443" },
                    { SFSessionProperty.VALIDATE_DEFAULT_PARAMETERS, "true" },
                    { SFSessionProperty.INSECUREMODE, "false" },
                    { SFSessionProperty.DISABLERETRY, "false" },
                    { SFSessionProperty.FORCERETRYON404, "false" },
                    { SFSessionProperty.CLIENT_SESSION_KEEP_ALIVE, "false" },
                    { SFSessionProperty.FORCEPARSEERROR, "false" },
                    { SFSessionProperty.USEPROXY, "false" },
                    { SFSessionProperty.PROXYHOST, "proxy.com" },
                    { SFSessionProperty.PROXYPORT, "1234" },
                    { SFSessionProperty.NONPROXYHOSTS, "localhost" },
                    { SFSessionProperty.BROWSER_RESPONSE_TIMEOUT, "60" }
                },
                ConnectionString =
                    "ACCOUNT=testaccount;USER=testuser;PASSWORD=123;proxyHost=proxy.com;proxyPort=1234;nonProxyHosts=localhost"
            };            
            return new TestCase[]
            {
                simpleTestCase,
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
