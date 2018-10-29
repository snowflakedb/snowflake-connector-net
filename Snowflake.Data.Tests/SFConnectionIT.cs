/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

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
        public void TestBasicConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(0, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.AreEqual("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                string[] versionElements = serverVersion.Split('.');
                Assert.AreEqual(3, versionElements.Length);

                conn.Close();
                Assert.AreEqual(ConnectionState.Closed, conn.State);
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
                Assert.AreEqual("TESTDB_DOTNET", conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);
                conn.Close();
            }
        }

        [Test]
        public void TestLoginTimeout()
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
        public void TestInvalidConnectioinString()
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
        public void TestUnknownConnectionProperty()
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
        public void TestSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                conn.Open();
                Assert.AreEqual("TESTDB_DOTNET", conn.Database);
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
                String connStrFmt = "account={0};user={1};password={2}";
                conn.ConnectionString = String.Format(connStrFmt, testConfig.account,
                    testConfig.user, testConfig.password);
                conn.Open();
                Assert.AreEqual(conn.State, ConnectionState.Open);
                conn.Close();
            }
        }

        [Test]
        public void TestConnectWithDifferentRole()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                String connStrFmt = "account={0};user={1};password={2};role=public;db=snowflake_sample_data;schema=information_schema;warehouse=shige_wh";
                conn.ConnectionString = String.Format(connStrFmt, testConfig.account,
                    testConfig.user, testConfig.password);
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
                    Assert.AreEqual(command.ExecuteScalar().ToString(), "SHIGE_WH");
                }
                conn.Close();
            }
        }
    }
}
