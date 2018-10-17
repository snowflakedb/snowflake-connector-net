/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using System;

    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        [Test]
        public void testBasicConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                Assert.AreEqual(0, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.AreEqual("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                string[] versionElements = serverVersion.Split('.');
                Assert.AreEqual(3, versionElements.Length);

                conn.Close();
            }
        }

        [Test]
        public void TestConnectViaSecureString()
        {
            String[] connEntries = connectionString.Split(';');
            String connectionStringWithoutPassword = "";
            using (var conn = new SnowflakeDbConnection())
            {
                var password = new System.Security.SecureString();
                foreach (String entry in connEntries)
                {
                    if (!entry.StartsWith("password="))
                    {
                        connectionStringWithoutPassword += entry;
                        connectionStringWithoutPassword += ';';
                    }
                    else
                    {
                        var pass = entry.Substring(9);
                        foreach (char c in pass)
                        {
                            password.AppendChar(c);
                        }
                    }
                }
                conn.ConnectionString = connectionStringWithoutPassword;
                conn.Password = password;
                conn.Open();
                Assert.AreEqual(testConfig.database, conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);
                conn.Close();
            }
        }

        [Test]
        public void testLoginTimeout()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                string invalidConnectionString = "host=invalidaccount.snowflakecomputing.com;connection_timeout=5;"
                    + "account=invalidaccount;user=snowman;password=test;";

                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch(AggregateException e)
                {
                    Assert.AreEqual(270007, ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                Assert.AreEqual(5, conn.ConnectionTimeout);
            }

        }

        [Test]
        public void testInvalidConnectioinString()
        {
            string[] invalidStrings = {
                // missing required connection property password
                "ACCOUNT=testaccount;user=testuser",
                // invalid account value
                "ACCOUNT=A=C;USER=testuser;password=123",
                "complete_invalid_string",
            };

            int[] expectedErrorCode = { 270006, 270008, 270008 };

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                for (int i=0; i<invalidStrings.Length; i++)
                {
                    try
                    {
                        conn.ConnectionString = invalidStrings[i];
                        conn.Open();
                        Assert.Fail();
                    }
                    catch (SnowflakeDbException e)
                    {
                        Assert.AreEqual(expectedErrorCode[i], e.ErrorCode);
                    }
                }
            }
        }

        [Test]
        public void testUnknownConnectionProperty()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // invalid propety will be ignored.
                conn.ConnectionString = connectionString += ";invalidProperty=invalidvalue;";

                conn.Open();
                Assert.AreEqual(conn.State, ConnectionState.Open);
                conn.Close();
            }
        }

        [Test]
        public void testSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                conn.Open();
                Assert.AreEqual(testConfig.database, conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);

                conn.Close();
            }

        }

        [Test]
        public void TestConnectWithoutHost()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                string connStrFmt = "account={0};user={1};password={2}";
                conn.ConnectionString = string.Format(connStrFmt, testConfig.account,
                    testConfig.user, testConfig.password);
                // Check that connection succeeds if host is not specified in test configs, i.e. default should work.
                if (string.IsNullOrEmpty(testConfig.host))
                {
                    conn.Open();
                    Assert.AreEqual(conn.State, ConnectionState.Open);
                    conn.Close();
                }
                // If host is specified in the configs the reason probably is that the different AWS region is used and host must be specified, otherwise the connection will fail.
                else
                {
                    Assert.Throws<AggregateException>(() => conn.Open(), "Connection attempt should fail if the account uses non-default region.");
                }
            }
        }

        [Test]
        public void TestConnectWithDifferentRole()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                var host = testConfig.host;
                if (string.IsNullOrEmpty(host))
                {
                    host = $"{testConfig.account}.snowflakecomputing.com";
                }

                string connStrFmt = "host={0};port=443;account={1};user={2};password={3};role=public;db=snowflake_sample_data;schema=information_schema;warehouse=shige_wh";
                conn.ConnectionString = string.Format(connStrFmt, host, testConfig.account, testConfig.user, testConfig.password);
                conn.Open();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select current_role()";
                    Assert.AreEqual(command.ExecuteScalar().ToString(), "PUBLIC");

                    command.CommandText = "select current_database()";
                    Assert.AreEqual(command.ExecuteScalar().ToString(), "SNOWFLAKE_SAMPLE_DATA");

                    command.CommandText = "select current_schema()";
                    Assert.AreEqual(command.ExecuteScalar().ToString(), "INFORMATION_SCHEMA");

                    command.CommandText = "select current_warehouse()";
                    // Command will return empty string if the hardcoded warehouse does not exist.
                    CollectionAssert.Contains(new [] { "SHIGE_WH", "" }, command.ExecuteScalar().ToString());
                }
                conn.Close();
            }
        }
    }
}
