/*
 * Copyright (c) 2019 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Core;
using System.Security;
using NUnit.Framework;

namespace Snowflake.Data.Tests
{
    /// <summary>
    /// The purpose of these testcases is to test if the connections string
    /// can be parsed correctly into properties for a session.
    /// </summary>
    class SFSessionPropertyTest
    {
        private class Testcase
        {
            public string ConnectionString { get; set; }
            public SecureString SecurePassword { get; set; }
            public SFSessionProperties ExpectedProperties { get; set; }

            public void TestValidCase()
            {
                SFSessionProperties actualProperties = SFSessionProperties.parseConnectionString(ConnectionString, SecurePassword);
                Assert.AreEqual(actualProperties, ExpectedProperties);
            }
        }

        [Test]
        [Ignore("SessionPropertyTest")]
        public void SessionPropertyTestDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestValidConnectionString()
        {
            Testcase[] testcases = new Testcase[]
            {
                new Testcase()
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
                    },
                },
            };

            foreach (Testcase testcase in testcases)
            {
                testcase.TestValidCase();
            }
        }
    }
}
