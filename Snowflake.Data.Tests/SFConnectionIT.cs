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

    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [Test]
        public void TestBasicConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
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

                //Should timeout before the default timeout (120 sec) * 1000
                Assert.Less(stopwatch.ElapsedMilliseconds, 120 * 1000);
                // Should timeout after the defined connection timeout
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeoutSec * 1000);
                Assert.AreEqual(5, conn.ConnectionTimeout);
            }
        }

        [Test]
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
                    Assert.AreEqual(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode,
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();
                // Should timeout after the default timeout (120 sec)
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 120 * 1000);
                // But never more than 16 sec (max backoff) after the default timeout
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (120 + 16) * 1000);
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
                "ACCOUNT=A=C;USER=testuser;password=123",
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
                    Task connectTask =  conn.OpenAsync(connectionCancelToken.Token);
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(SFError.INTERNAL_ERROR.GetAttribute<SFErrorAttr>().errorCode,
                        ((SnowflakeDbException)e.InnerException).ErrorCode);

                }
                stopwatch.Stop();

                // Should timeout after 5sec
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 5 * 1000);
                // But never more than 1 sec (max backoff) after the default timeout
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (6) * 1000);

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(5, conn.ConnectionTimeout);
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

                // Should timeout after the default timeout (120 sec)
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 120 * 1000);
                // But never more than 16 sec (max backoff) after the default timeout
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (120 + 16) * 1000);

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
    }
}


