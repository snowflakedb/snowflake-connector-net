﻿/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

using System.Text;
using System.Threading.Tasks;

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using System;
    using Snowflake.Data.Core;

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

                Assert.AreEqual(testConfig.database.ToUpper(), conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

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
                catch(SnowflakeDbException e)
                {
                    Assert.AreEqual(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode,e.ErrorCode);
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
        [IgnoreOnEnvIs("snowflake_cloud_env", "AZURE")]
        public void TestSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                conn.Open();

                Assert.AreEqual(testConfig.database.ToUpper(), conn.Database);
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

                string connStrFmt = "host={0};port=443;account={1};user={2};password={3};role=public;db=snowflake_sample_data;schema=information_schema;warehouse=WH_NOT_EXISTED";
                conn.ConnectionString = string.Format(connStrFmt, host, testConfig.account, testConfig.user, testConfig.password);
                conn.Open();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select current_role()";
                    Assert.AreEqual(command.ExecuteScalar().ToString(), "PUBLIC");

                    command.CommandText = "select current_database()";
                    CollectionAssert.Contains(new [] { "SNOWFLAKE_SAMPLE_DATA", "" }, command.ExecuteScalar().ToString());

                    command.CommandText = "select current_schema()";
                    CollectionAssert.Contains(new [] { "INFORMATION_SCHEMA", "" }, command.ExecuteScalar().ToString());

                    command.CommandText = "select current_warehouse()";
                    // Command will return empty string if the hardcoded warehouse does not exist.
                    Assert.AreEqual("", command.ExecuteScalar().ToString());
                }
                conn.Close();
            }
        }

        // Test that when a connection is disposed, a close would send out and unfinished transaction would be roll back.
        [Test]
        public void TestConnectionDispose()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Setup
                conn.ConnectionString = connectionString;
                conn.Open();
                IDbCommand command = conn.CreateCommand();
                command.CommandText = "create or replace table testConnDispose(c int)";
                command.ExecuteNonQuery();

                IDbTransaction t1 = conn.BeginTransaction();
                IDbCommand t1c1 = conn.CreateCommand();
                t1c1.Transaction = t1;
                t1c1.CommandText = "insert into testConnDispose values (1)";
                t1c1.ExecuteNonQuery();
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Previous connection would be disposed and 
                // uncommitted txn would rollback at this point
                conn.ConnectionString = connectionString;
                conn.Open();
                IDbCommand command = conn.CreateCommand();
                command.CommandText = "SELECT * FROM testConnDispose";
                IDataReader reader = command.ExecuteReader();
                Assert.IsFalse(reader.Read());

                // Cleanup
                command.CommandText = "DROP TABLE IF EXISTS testConnDispose";
                command.ExecuteNonQuery();
            }
        }

        [Test]
        public void TestUnknownAuthenticator()
        {
            string[] wrongAuthenticators = new string[]
            {
                "http://snowflakecomputing.okta.com",
                "https://snowflake.com",
                "unknown",
            };

            foreach (string wrongAuthenticator in wrongAuthenticators)
            {
                try
                {
                    IDbConnection conn = new SnowflakeDbConnection();
                    conn.ConnectionString = "scheme=http;host=test;port=8080;user=test;password=test;account=test;authenticator=" + wrongAuthenticator;
                    conn.Open();
                    Assert.Fail("Authentication of {0} should fail", wrongAuthenticator);
                } catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(SFError.UNKNOWN_AUTHENTICATOR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
                }

            }
        }

        [Test]
        [Ignore("This test requires manual setup and therefore cannot be run in CI")]
        public void TestOktaConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = "scheme=http;host=10.211.55.3;port=8080;user=qa@snowflakecomputing.com;password=Test123!;" +
                    "account=testaccount;role=sysadmin;db=testdb;schema=public;warehouse=regress;authenticator=https://snowflakecomputing.okta.com";
                conn.Open();
            }
        }
    }
}
