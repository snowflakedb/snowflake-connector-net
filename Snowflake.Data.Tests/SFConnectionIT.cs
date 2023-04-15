/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

namespace Snowflake.Data.Tests
{
    using NUnit.Framework;
    using Snowflake.Data.Client;
    using System.Data;
    using System;
    using Snowflake.Data.Core;
    using System.Threading.Tasks;
    using System.Threading;
    using Snowflake.Data.Log;
    using System.Diagnostics;
    using Snowflake.Data.Tests.Mock;
    using System.Runtime.InteropServices;

    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [Test]
        [Ignore("ConnectionIT")]
        public void ConnectionITDone()
        {
            // Do nothing;
        }

        [Test]
        public void TestBasicConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                SnowflakeDbConnectionPool.SetPooling(false);
                conn.ConnectionString = ConnectionString;
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(120, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.AreEqual("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                if (!string.Equals(serverVersion, "Dev"))
                {
                    string[] versionElements = serverVersion.Split('.');
                    Assert.AreEqual(3, versionElements.Length);
                }

                conn.Close();
                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        public void TestApplicationName()
        {
            string[] validApplicationNames = { "test1234", "test_1234", "test-1234", "test.1234"};
            string[] invalidApplicationNames = { "1234test", "test$A", "test<script>" };

            // Valid names
            foreach (string appName in validApplicationNames)
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    conn.ConnectionString += $"application={appName}";
                    conn.Open();
                    Assert.AreEqual(ConnectionState.Open, conn.State);

                    conn.Close();
                    Assert.AreEqual(ConnectionState.Closed, conn.State);
                }
            }

            // Invalid names
            foreach (string appName in invalidApplicationNames)
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    conn.ConnectionString += $"application={appName}";
                    try
                    {
                        conn.Open();
                        logger.Debug("{appName}");
                        Assert.Fail();

                    }
                    catch (SnowflakeDbException e)
                    {
                        // Expected
                        logger.Debug("Failed opening connection ", e);
                        Assert.AreEqual("08006", e.SqlState); // Connection failure
                    }

                    Assert.AreEqual(ConnectionState.Closed, conn.State);
                }
            }
        }

        [Test]
        public void TestIncorrectUserOrPasswordBasicConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = String.Format("scheme={0};host={1};port={2};" +
            "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                    testConfig.protocol,
                    testConfig.host,
                    testConfig.port,
                    testConfig.account,
                    testConfig.role,
                    testConfig.database,
                    testConfig.schema,
                    testConfig.warehouse,
                    "unknown",
                    testConfig.password);

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                    Assert.Fail();
                
                }
                catch (SnowflakeDbException e)
                {
                    // Expected
                    logger.Debug("Failed opening connection ", e);
                    Assert.AreEqual("08006", e.SqlState); // Connection failure
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
			}
        }

        public void TestCrlCheckSwitchConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + ";INSECUREMODE=true";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + ";INSECUREMODE=false";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + ";INSECUREMODE=false";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + ";INSECUREMODE=true";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        public void TestConnectString()
        {
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString;
            conn.Open();
            using (IDbCommand cmd = conn.CreateCommand())
            {
                //cmd.CommandText = "create database \"dlTest\"";
                //cmd.ExecuteNonQuery();
                //cmd.CommandText = "use database \"dlTest\"";
                //cmd.ExecuteNonQuery();
                cmd.CommandText = "create schema \"dlSchema\"";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "use schema \"dlSchema\"";
                cmd.ExecuteNonQuery();
                //cmd.CommandText = "create table \"dlTest\".\"dlSchema\".test1 (col1 string, col2 int)";
                cmd.CommandText = "create table test1 (col1 string, col2 int)";
                cmd.ExecuteNonQuery();
                //cmd.CommandText = "insert into \"dlTest\".\"dlSchema\".test1 Values ('test 1', 1);";
                cmd.CommandText = "insert into test1 Values ('test 1', 1);";
                cmd.ExecuteNonQuery();
            }
           
            using (var conn1 = new SnowflakeDbConnection())
            {
                conn1.ConnectionString = String.Format("scheme={0};host={1};port={2};" +
                    "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                        testConfig.protocol,
                        testConfig.host,
                        testConfig.port,
                        testConfig.account,
                        testConfig.role,
                        //"\"dlTest\"",
                        testConfig.database,
                        "\"dlSchema\"",
                        //testConfig.schema,
                        testConfig.warehouse,
                        testConfig.user,
                        testConfig.password);
                Assert.AreEqual(conn1.State, ConnectionState.Closed);

                conn1.Open();
                using (IDbCommand cmd = conn1.CreateCommand())
                {
                    cmd.CommandText = "SELECT count(*) FROM test1";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.IsTrue(reader.Read());
                    Assert.AreEqual(1, reader.GetInt32(0));
                }
                conn1.Close();

                Assert.AreEqual(ConnectionState.Closed, conn1.State); 
            }
            
            using (IDbCommand cmd = conn.CreateCommand())
            {
                //cmd.CommandText = "drop database \"dlTest\"";
                cmd.CommandText = "drop schema \"dlSchema\"";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "use database "+ testConfig.database;
                cmd.ExecuteNonQuery();
            }
            conn.Close();
        }

        [Test]
        [Ignore("TestConnectStringWithUserPwd, this will popup an internet browser for external login.")]
        public void TestConnectStringWithUserPwd()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = String.Format("scheme={0};host={1};port={2};" +
            "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};authenticator={10};",
                    testConfig.protocol,
                    testConfig.host,
                    testConfig.port,
                    testConfig.account,
                    testConfig.role,
                    testConfig.database,
                    testConfig.schema,
                    testConfig.warehouse,
                    "",
                    "",
                    "externalbrowser");

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                conn.Open();
                conn.Close();
                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        public void TestConnectViaSecureString()
        {
            String[] connEntries = ConnectionString.Split(';');
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
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (IDbConnection conn = new MockSnowflakeDbConnection())
                {
                    int timeoutSec = 5;
                    string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0}",
                        timeoutSec);

                    conn.ConnectionString = loginTimeOut5sec;

                    Assert.AreEqual(conn.State, ConnectionState.Closed);
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    try
                    {
                        conn.Open();
                        Assert.Fail();

                    }
                    catch (AggregateException e)
                    {
                        Assert.AreEqual(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode,
                            ((SnowflakeDbException)e.InnerException).ErrorCode);
                    }
                    stopwatch.Stop();
                    int detla = 10; //in case server time slower.

                    //Should timeout before the default timeout (120 sec) * 1000
                    Assert.Less(stopwatch.ElapsedMilliseconds, 120 * 1000);
                    // Should timeout after the defined connection timeout
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeoutSec * 1000 - detla);
                    Assert.AreEqual(5, conn.ConnectionTimeout);
                }
            }
        }

        [Test]
        [Ignore("Disable unstable test cases for now")]
        public void TestDefaultLoginTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                // Default timeout is 120 sec
                Assert.AreEqual(120, conn.ConnectionTimeout);

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {                    
                    conn.Open();                    
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    if (e.InnerException is SnowflakeDbException)
                    {
                        Assert.AreEqual(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode,
                        ((SnowflakeDbException)e.InnerException).ErrorCode);

                        stopwatch.Stop();
                        // Should timeout after the default timeout (120 sec)
                        Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 120 * 1000);
                        // But never more than 16 sec (max backoff) after the default timeout
                        Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (120 + 16) * 1000);
                    }
                }
            }
        }

        [Test]
        public void TestConnectionFailFast()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Just a way to get a 404 on the login request and make sure there are no retry
                string invalidConnectionString = "host=docs.microsoft.com;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;";

                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode,
                        e.ErrorCode);
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        public void TestEnableRetry()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string invalidConnectionString = "host=docs.microsoft.com;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;disableretry=true;forceretryon404=true";
                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode,
                        e.ErrorCode);
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        public void TestValidateDefaultParameters()
        {
            string connectionString = String.Format("scheme={0};host={1};port={2};" +
            "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                    testConfig.protocol,
                    testConfig.host,
                    testConfig.port,
                    testConfig.account,
                    testConfig.role,
                    testConfig.database,
                    testConfig.schema,
                    "WAREHOUSE_NEVER_EXISTS",
                    testConfig.user,
                    testConfig.password);

            // By default should validate parameters
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                try
                {
                    conn.ConnectionString = connectionString;
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(390201, e.ErrorCode);
                }
            }

            // This should succeed
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString + ";VALIDATE_DEFAULT_PARAMETERS=false";
                conn.Open();
            }
        }

        [Test]
        public void TestInvalidConnectionString()
        {
            string[] invalidStrings = {
                // missing required connection property password
                "ACCOUNT=testaccount;user=testuser",
                // invalid account value
                "ACCOUNT=A=C;USER=testuser;password=123;key",
                "complete_invalid_string",
            };

            int[] expectedErrorCode = { 270006, 270008, 270008 };

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                for (int i = 0; i < invalidStrings.Length; i++)
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
                conn.ConnectionString = ConnectionString + ";invalidProperty=invalidvalue;";

                conn.Open();
                Assert.AreEqual(conn.State, ConnectionState.Open);
                conn.Close();
            }
        }

        [Test]
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] { "AZURE", "GCP" })]
        public void TestSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                conn.Open();

                Assert.AreEqual(testConfig.database.ToUpper(), conn.Database);
                Assert.AreEqual(conn.State, ConnectionState.Open);

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                    Assert.AreEqual("SNOWFLAKE_SAMPLE_DATA", conn.Database);
                }

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

                string connStrFmt = "scheme={0};host={1};port={2};" +
                    "user={3};password={4};account={5};role=public;db=snowflake_sample_data;schema=information_schema;warehouse=WH_NOT_EXISTED;validate_default_parameters=false";

                conn.ConnectionString = string.Format(
                    connStrFmt,
                    testConfig.protocol,
                    testConfig.host,
                    testConfig.port,
                    testConfig.user,
                    testConfig.password,
                    testConfig.account
                    );
                conn.Open();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select current_role()";
                    Assert.AreEqual(command.ExecuteScalar().ToString(), "PUBLIC");

                    command.CommandText = "select current_database()";
                    CollectionAssert.Contains(new[] { "SNOWFLAKE_SAMPLE_DATA", "" }, command.ExecuteScalar().ToString());

                    command.CommandText = "select current_schema()";
                    CollectionAssert.Contains(new[] { "INFORMATION_SCHEMA", "" }, command.ExecuteScalar().ToString());

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
                conn.ConnectionString = ConnectionString;
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
                conn.ConnectionString = ConnectionString;
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
                }
                catch (SnowflakeDbException e)
                {
                    Assert.AreEqual(SFError.UNKNOWN_AUTHENTICATOR.GetAttribute<SFErrorAttr>().errorCode, e.ErrorCode);
                }

            }
        }

        [Test]
        [Ignore("This test requires manual setup and therefore cannot be run in CI")]
        public void TestOktaConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        testConfig.OktaURL,
                        testConfig.OktaUser,
                        testConfig.OktaPassword);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("This test requires manual setup and therefore cannot be run in CI")]
        public void TestOkta2ConnectionsFollowingEachOther()
        {
            // This test is here because Cookies were messing up with sequential Okta connections
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        testConfig.OktaURL,
                        testConfig.OktaUser,
                        testConfig.OktaPassword);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }


            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        testConfig.OktaURL,
                        testConfig.OktaUser,
                        testConfig.OktaPassword);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUser()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.AreEqual("QA", command.ExecuteScalar().ToString());
                }
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithWrongUser()
        {
            try
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + ";authenticator=externalbrowser;user=wrong@snowflakecomputing.com";
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                Assert.AreEqual(390191, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtUnencryptedPemFileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                        testConfig.jwtAuthUser,
                        testConfig.pemFilePath);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtUnencryptedP8FileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                        testConfig.jwtAuthUser,
                        testConfig.p8FilePath);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkFileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1};private_key_pwd={2}",
                        testConfig.jwtAuthUser,
                        testConfig.pwdProtectedPrivateKeyFilePath,
                        testConfig.privateKeyFilePwd);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtUnencryptedPkConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key={1}",
                        testConfig.jwtAuthUser,
                        testConfig.privateKey);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key={1};private_key_pwd={2}",
                        testConfig.jwtAuthUser,
                        testConfig.pwdProtectedPrivateKey,
                        testConfig.privateKeyFilePwd);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtMissingConnectionSettingConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_pwd={1}",
                            testConfig.jwtAuthUser,
                            testConfig.privateKeyFilePwd);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Missing PRIVATE_KEY_FILE connection setting required for 
                // authenticator =snowflake_jwt
                Assert.AreEqual(270008, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkFileInvalidPwdConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1};private_key_pwd=Invalid",
                            testConfig.jwtAuthUser,
                            testConfig.pwdProtectedPrivateKeyFilePath);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Invalid password for decrypting the private key
                Assert.AreEqual(270052, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkFileNoPwdConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                            testConfig.jwtAuthUser,
                            testConfig.pwdProtectedPrivateKeyFilePath);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Invalid password (none provided) for decrypting the private key
                Assert.AreEqual(270052, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtConnectionWithWrongUser()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                            "WrongUser",
                            testConfig.pemFilePath);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Jwt token is invalid
                Assert.AreEqual(390144, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkConnectionWithWrongUser()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1};private_key_pwd={2}",
                            "WrongUser",
                            testConfig.pwdProtectedPrivateKeyFilePath,
                            testConfig.privateKeyFilePwd);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Jwt token is invalid
                Assert.AreEqual(390144, e.ErrorCode);
            }
        }


        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestValidOAuthConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=oauth;token={0}",
                        testConfig.oauthToken);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        public void TestInValidOAuthTokenConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + ";authenticator=oauth;token=notAValidOAuthToken";
                    conn.Open();
                    Assert.AreEqual(ConnectionState.Open, conn.State);
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Invalid OAuth access token
                Assert.AreEqual(390303, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestValidOAuthExpiredTokenConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                   = ConnectionStringWithoutAuth
                   + String.Format(
                       ";authenticator=oauth;token={0}",
                       testConfig.expOauthToken);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                Console.Write(e);
                // Token is expired
                Assert.AreEqual(390318, e.ErrorCode);
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestCorrectProxySettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1}",
                    testConfig.proxyHost,
                    testConfig.proxyPort);

                conn.Open();
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestCorrectProxyWithCredsSettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3}",
                    testConfig.authProxyHost,
                    testConfig.authProxyPort,
                    testConfig.authProxyUser,
                    testConfig.authProxyPwd);

                conn.Open();
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestCorrectProxySettingWithByPassListFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4}",
                    testConfig.authProxyHost,
                    testConfig.authProxyPort,
                    testConfig.authProxyUser,
                    testConfig.authProxyPwd,
                    "*.foo.com %7C" + testConfig.host + "|localhost");

                conn.Open();
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestMultipleConnectionWithDifferentHttpHandlerSettings()
        {
            // Authenticated proxy
            using (var conn1 = new SnowflakeDbConnection())
            {
                conn1.ConnectionString = ConnectionString
                    + String.Format(
                        ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3}",
                        testConfig.authProxyHost,
                        testConfig.authProxyPort,
                        testConfig.authProxyUser,
                        testConfig.authProxyPwd);
                conn1.Open();
            }

            // No proxy
            using (var conn2 = new SnowflakeDbConnection())
            {
                conn2.ConnectionString = ConnectionString;
                conn2.Open();
            }

            // Non authenticated proxy
            using (var conn3 = new SnowflakeDbConnection())
            {
                conn3.ConnectionString = ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1}",
                    testConfig.proxyHost,
                    testConfig.proxyPort);
                conn3.Open();
            }

            // Invalid proxy
            using (var conn4 = new SnowflakeDbConnection())
            {
                conn4.ConnectionString =
                    ConnectionString + "connection_timeout=20;useProxy=true;proxyHost=Invalid;proxyPort=8080;INSECUREMODE=true";
                try
                {
                    conn4.Open();
                    Assert.Fail();
                }
                catch
                {
                    // Expected
                }
            }

            // Another authenticated proxy connection, same proxy but insecure mode is true
			// Will use a different httpclient
            using (var conn5 = new SnowflakeDbConnection())
            {
                conn5.ConnectionString = ConnectionString
                    + String.Format(
                        ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};INSECUREMODE=true",
                        testConfig.authProxyHost,
                        testConfig.authProxyPort,
                        testConfig.authProxyUser,
                        testConfig.authProxyPwd);
                conn5.Open();
            }

            // No proxy again, but insecure mode is true
			// Will use a different httpclient
            using (var conn6 = new SnowflakeDbConnection())
            {

                conn6.ConnectionString = ConnectionString + ";INSECUREMODE=true";
                conn6.Open();
            }

            // Another authenticated proxy, but this will create a new httpclient because there is
            // a bypass list
            using (var conn7 = new SnowflakeDbConnection())
            {
                conn7.ConnectionString
              = ConnectionString
              + String.Format(
                  ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4}",
                  testConfig.authProxyHost,
                  testConfig.authProxyPort,
                  testConfig.authProxyUser,
                  testConfig.authProxyPwd,
                  "*.foo.com %7C" + testConfig.host + "|localhost");

                conn7.Open();
            }

            // No proxy again, insecure mode is false
            // Should use same httpclient than conn2
            using (var conn8 = new SnowflakeDbConnection())
            {
                conn8.ConnectionString = ConnectionString + ";INSECUREMODE=false";
                conn8.Open();
            }

            // Another authenticated proxy with bypasslist, but this will create a new httpclient because 
            // InsecureMode=true
            using (var conn9 = new SnowflakeDbConnection())
            {
                conn9.ConnectionString
              = ConnectionString
              + String.Format(
                  ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4};INSECUREMODE=true",
                  testConfig.authProxyHost,
                  testConfig.authProxyPort,
                  testConfig.authProxyUser,
                  testConfig.authProxyPwd,
                  "*.foo.com %7C" + testConfig.host + "|localhost");

                conn9.Open();
            }

            // Another authenticated proxy with bypasslist
            // Should use same httpclient than conn7
            using (var conn10 = new SnowflakeDbConnection())
            {
                conn10.ConnectionString
              = ConnectionString
              + String.Format(
                  ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4}",
                  testConfig.authProxyHost,
                  testConfig.authProxyPort,
                  testConfig.authProxyUser,
                  testConfig.authProxyPwd,
                  "*.foo.com %7C" + testConfig.host + "|localhost");

                conn10.Open();
            }

            // No proxy, but insecuremode=true
            // Should use same httpclient than conn6
            using (var conn11 = new SnowflakeDbConnection())
            {
                conn11.ConnectionString = ConnectionString+";INSECUREMODE=true";
                conn11.Open();
            }
        }

        [Test]
        public void TestInvalidProxySettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {

                conn.ConnectionString =
                    ConnectionString + "connection_timeout=5;useProxy=true;proxyHost=Invalid;proxyPort=8080";
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    // Expected
                    logger.Debug("Failed opening connection ", e);
                    Assert.AreEqual(270001, e.ErrorCode); //Internal error
                    Assert.AreEqual("08006", e.SqlState); // Connection failure
                }
            }
        }

        [Test]
        public void TestUseProxyFalseWithInvalidProxyConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString =
                    ConnectionString + ";useProxy=false;proxyHost=Invalid;proxyPort=8080";
                conn.Open();
                // Because useProxy=false, the proxy settings are ignored
            }
        }

        [Test]
        public void TestInvalidProxySettingWithByPassListFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost=Invalid;proxyPort=8080;nonProxyHosts={0}",
                    "*.foo.com %7C" + testConfig.account + ".snowflakecomputing.com|localhost");
                conn.Open();
                // Because testConfig.host is in the bypass list, the proxy should not be used
            }
        }

        [Test]
        [Ignore("Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void testMulitpleConnectionInParallel()
        {
            string baseConnectionString = ConnectionString + $";CONNECTION_TIMEOUT=30;";
            string authenticatedProxy = String.Format("useProxy =true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};",
                  testConfig.authProxyHost,
                  testConfig.authProxyPort,
                  testConfig.authProxyUser,
                  testConfig.authProxyPwd);
            string byPassList = "nonProxyHosts=*.foo.com %7C" + testConfig.host + "|localhost;";
            string insecureModeTrue = "INSECUREMODE=true;";
            string insecureModeFalse = "INSECUREMODE=false;";

            string[] connectionStrings = {
                baseConnectionString,
                baseConnectionString + insecureModeFalse ,
                baseConnectionString + insecureModeTrue,
                baseConnectionString + authenticatedProxy,
                baseConnectionString + authenticatedProxy + insecureModeFalse,
                baseConnectionString + authenticatedProxy + insecureModeTrue,
                baseConnectionString + authenticatedProxy + byPassList,
                baseConnectionString + authenticatedProxy + byPassList + insecureModeFalse,
                baseConnectionString + authenticatedProxy + byPassList + insecureModeTrue};

            bool failed = false;

           Task[] tasks = new Task[450];
            for (int i = 0; i < 450; i++)
            {
                string connString = connectionStrings[i % (connectionStrings.Length)];
                tasks[i] = Task.Run(() =>
                {
                    using (IDbConnection conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = connString;
                        Console.WriteLine($"{conn.ConnectionString}");
                        try
                        {
                            conn.Open();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            Console.WriteLine("--------------------------");
                            Console.WriteLine(e.InnerException);
                            failed = true;
                        }

                        using (IDbCommand command = conn.CreateCommand())
                        {
                            try
                            {
                                command.CommandText = "SELECT 1";
                                command.ExecuteScalar();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("ExecuteScalar error");
                                Console.WriteLine(e);
                                failed = true;
                            }
                        }
                    }
                });
            }
            try
            {
                Task.WaitAll(tasks);
            }
            catch (AggregateException ae)
            {
                Console.WriteLine("One or more exceptions occurred: ");
                foreach (var ex in ae.Flatten().InnerExceptions)
                    Console.WriteLine("   {0}", ex.Message);
                failed = true;
            }

            if (failed)
            {
                Assert.Fail();
            }
        }

        [Test]
        [Ignore("Ignore this test, please test this manual with breakpoint at SFSessionProperty::parseConnectionString() to verify")]
        public void TestEscapeChar()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                SnowflakeDbConnectionPool.SetPooling(false);
                conn.ConnectionString = ConnectionString + "key1=test\'password;key2=test\"password;key3=test==password";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(120, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.AreEqual("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                if (!string.Equals(serverVersion, "Dev"))
                {
                    string[] versionElements = serverVersion.Split('.');
                    Assert.AreEqual(3, versionElements.Length);
                }

                conn.Close();
                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        [Ignore("Ignore this test, please test this manual with breakpoint at SFSessionProperty::parseConnectionString() to verify")]
        public void TestEscapeChar1()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                SnowflakeDbConnectionPool.SetPooling(false);
                conn.ConnectionString = ConnectionString + "key==word=value; key1=\"test;password\"; key2=\"test=password\"";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(120, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.AreEqual("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                if (!string.Equals(serverVersion, "Dev"))
                {
                    string[] versionElements = serverVersion.Split('.');
                    Assert.AreEqual(3, versionElements.Length);
                }

                conn.Close();
                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }
        
        [Test]
        [Ignore("Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public void TestHeartBeat()
        {
            SnowflakeDbConnectionPool.SetPooling(false);
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
            conn.Open();

            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs
            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.AreEqual(command.ExecuteScalar(), 46 );
            }

            conn.Close();
            Assert.AreEqual(ConnectionState.Closed, conn.State);
        }

        [Test]
        [Ignore("Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public void TestHeartBeatWithConnectionPool()
        {
            SnowflakeDbConnectionPool.ClearAllPools();
            SnowflakeDbConnectionPool.SetMaxPoolSize(2);
            SnowflakeDbConnectionPool.SetTimeout(14800);
            SnowflakeDbConnectionPool.SetPooling(true);

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
            conn.Open();
            conn.Close();

            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
            
            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
            conn1.Open();
            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.AreEqual(command.ExecuteScalar(), 46);
            }

            conn1.Close();
            Assert.AreEqual(ConnectionState.Closed, conn1.State);
            Assert.AreEqual(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Test]
        public void TestKeepAlive()
        {
            // create 100 connections, one per second
            var connCount = 100;
            // pooled connectin expire in 5 seconds so after 5 seconds,
            // one connection per second will be closed
            SnowflakeDbConnectionPool.SetTimeout(5);
            SnowflakeDbConnectionPool.SetMaxPoolSize(20);
            // heart beat interval is validity/4 so send out per 5 seconds
            HeartBeatBackground.setValidity(20);
            try
            {
                for (int i = 0; i < connCount; i++)
                {
                    using (var conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
                        conn.Open();
                    }
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    // roughly should only have 5 sessions in pool stay alive
                    // check for 10 in case of bad timing, also much less than the
                    // pool max size to ensure it's unpooled because of expire
                    Assert.Less(HeartBeatBackground.getQueueLength(), 10);
                }
            }
            catch
            {
                // fail the test case if any exception is thrown
                Assert.Fail();
            }
        }
    }

    [TestFixture]
    class SFConnectionITAsync : SFBaseTestAsync
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionITAsync>();


        [Test]
        public void TestCancelLoginBeforeTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                // No timeout
                int timeoutSec = 0;
                string infiniteLoginTimeOut = String.Format(ConnectionString + ";connection_timeout={0}",
                    timeoutSec);

                conn.ConnectionString = infiniteLoginTimeOut;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                // At this point the connection string has not been parsed, it will return the 
                // default value
                //Assert.AreEqual(120, conn.ConnectionTimeout);

                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                Task connectTask = conn.OpenAsync(connectionCancelToken.Token);                

                // Sleep for 130 sec (more than the default connection timeout and the httpclient 
                // timeout to make sure there are no false positive )
                Thread.Sleep(130*1000);
           
                Assert.AreEqual(ConnectionState.Connecting, conn.State);

                // Cancel the connection because it will never succeed since there is no 
                // connection_timeout defined
                logger.Debug("connectionCancelToken.Cancel ");
                connectionCancelToken.Cancel();

                try
                {
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(
                        "System.Threading.Tasks.TaskCanceledException",
                        e.InnerException.GetType().ToString());
                    
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(0, conn.ConnectionTimeout);
            }
        }

        [Test]
        public void TestAsyncLoginTimeout()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var conn = new MockSnowflakeDbConnection())
                {
                    int timeoutSec = 5;
                    string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0}",
                        timeoutSec);
                    conn.ConnectionString = loginTimeOut5sec;

                    Assert.AreEqual(conn.State, ConnectionState.Closed);

                    CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    try
                    {
                        Task connectTask = conn.OpenAsync(connectionCancelToken.Token);
                        connectTask.Wait();
                    }
                    catch (AggregateException e)
                    {
                        Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode,
                            ((SnowflakeDbException)e.InnerException).ErrorCode);

                    }
                    stopwatch.Stop();
                    int detla = 10; //in case server time slower.

                    // Should timeout after 5sec + 3 retry 20 sec
                    Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 35 * 1000 - detla);
                    // But never more than 1 sec (max backoff) after the default timeout
                    Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (66) * 1000);

                    Assert.AreEqual(ConnectionState.Closed, conn.State);
                    Assert.AreEqual(5, conn.ConnectionTimeout);
                }
            }
        }

        [Test]
        public void TestAsyncDefaultLoginTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    Task connectTask = conn.OpenAsync(connectionCancelToken.Token);
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode,
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();
                int detla = 10; //in case server time slower.

                // Should timeout after the default timeout (120 sec + 6 retry 840 sec)
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 840 * 1000 - detla);
                // But never more than 16 sec (max backoff) after the default timeout
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (840 + 16) * 1000);

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(120, conn.ConnectionTimeout);
            }
        }

        [Test]
        public void TestAsyncConnectionFailFast()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Just a way to get a 404 on the login request and make sure there are no retry
                string invalidConnectionString = "host=docs.microsoft.com;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;";

                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                Task connectTask = null;
                try
                {
                    connectTask = conn.OpenAsync(connectionCancelToken.Token);
                    connectTask.Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode,
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.IsTrue(connectTask.IsFaulted);
            }
        }

        [Test]
        public void TestCloseAsync()
        {
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection.close
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection.closeasync
            // An application can call Close or CloseAsync more than one time. 
            // No exception is generated.
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                Assert.AreEqual(conn.State, ConnectionState.Closed);
                Task task = null;

                // Close the connection. It's not opened yet, but it should not have any issue
                task = conn.CloseAsync(new CancellationTokenSource().Token);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);

                // Open the connection
                task = conn.OpenAsync(new CancellationTokenSource().Token);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync(new CancellationTokenSource().Token);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);

                // Close the connection again.
                task = conn.CloseAsync(new CancellationTokenSource().Token);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);
            }
        }

        [Test]
        public void TestCloseAsyncFailure()
        {
            using (var conn = new MockSnowflakeDbConnection(new MockCloseSessionException()))
            {
                SnowflakeDbConnectionPool.SetPooling(false);
                conn.ConnectionString = ConnectionString;
                Assert.AreEqual(conn.State, ConnectionState.Closed);
                Task task = null;

                // Open the connection
                task = conn.OpenAsync(new CancellationTokenSource().Token);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                // Close the opened connection
                task =  conn.CloseAsync(new CancellationTokenSource().Token);
                try
                {
                    task.Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(MockCloseSessionException.SESSION_CLOSE_ERROR,
                        ((SnowflakeDbException)(e.InnerException).InnerException).ErrorCode);
                }
                Assert.AreEqual(conn.State, ConnectionState.Open);
            }
        }
    }
}


