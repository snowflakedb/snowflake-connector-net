using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    [TestFixture]
    class SFConnectionIT : SFBaseTest
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [Test]
        public void TestBasicConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
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
            string[] validApplicationNames = { "test1234", "test_1234", "test-1234", "test.1234" };
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
                        s_logger.Debug("{appName}");
                        Assert.Fail();

                    }
                    catch (SnowflakeDbException e)
                    {
                        // Expected
                        s_logger.Debug("Failed opening connection ", e);
                        AssertIsConnectionFailure(e);
                    }

                    Assert.AreEqual(ConnectionState.Closed, conn.State);
                }
            }
        }

        [Test]
        public void TestApplicationPathIsSentDuringAuthentication()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                var authenticator = (BaseAuthenticator)conn.SfSession.authenticator;
                var clientEnv = authenticator.BuildLoginRequestData().clientEnv;
                var lowerPath = clientEnv.applicationPath.ToLower();
                Assert.IsTrue(
                    lowerPath.Contains("snowflake.data.tests") &&
                    lowerPath.Contains("bin") &&
                    lowerPath.Contains("testhost") &&
                    (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")),
                    $"APPLICATION_PATH should contain 'snowflake.data.tests', 'bin', 'testhost' and end with .dll or .exe. Got: {clientEnv.applicationPath}");
            }
        }

        [Test]
        public void TestIncorrectUserOrPasswordBasicConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
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
                    s_logger.Debug("Failed opening connection ", e);
                    AssertIsConnectionFailure(e);
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestConnectionIsNotMarkedAsOpenWhenWasNotCorrectlyOpenedBefore(bool explicitClose)
        {
            for (int i = 0; i < 2; ++i)
            {
                s_logger.Debug($"Running try #{i}");
                SnowflakeDbConnection snowflakeConnection = null;
                try
                {
                    snowflakeConnection = new SnowflakeDbConnection(ConnectionStringWithInvalidUserName);
                    snowflakeConnection.Open();
                    Assert.Fail("Connection open should fail");
                }
                catch (SnowflakeDbException e)
                {
                    AssertIsConnectionFailure(e);
                    AssertConnectionIsNotOpen(snowflakeConnection);
                    if (explicitClose)
                    {
                        snowflakeConnection.Close();
                        AssertConnectionIsNotOpen(snowflakeConnection);
                    }
                }
            }
        }

        [Test]
        public void TestConnectionIsNotMarkedAsOpenWhenWasNotCorrectlyOpenedWithUsingClause()
        {
            for (int i = 0; i < 2; ++i)
            {
                s_logger.Debug($"Running try #{i}");
                SnowflakeDbConnection snowflakeConnection = null;
                try
                {
                    using (snowflakeConnection = new SnowflakeDbConnection(ConnectionStringWithInvalidUserName))
                    {
                        snowflakeConnection.Open();
                    }
                }
                catch (SnowflakeDbException e)
                {
                    AssertIsConnectionFailure(e);
                    AssertConnectionIsNotOpen(snowflakeConnection);
                }
            }
        }

        private static void AssertConnectionIsNotOpen(SnowflakeDbConnection snowflakeDbConnection)
        {
            Assert.NotNull(snowflakeDbConnection);
            Assert.IsFalse(snowflakeDbConnection.IsOpen()); // check via public method
            Assert.AreEqual(ConnectionState.Closed, snowflakeDbConnection.State); // ensure internal state is expected
        }

        private static void AssertIsConnectionFailure(SnowflakeDbException e)
        {
            Assert.AreEqual(SnowflakeDbException.CONNECTION_FAILURE_SSTATE, e.SqlState);
        }

        [Test]
        public void TestConnectString()
        {
            var schemaName = "dlSchema_" + Guid.NewGuid().ToString().Replace("-", "_");
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString;
            conn.Open();
            using (IDbCommand cmd = conn.CreateCommand())
            {
                //cmd.CommandText = "create database \"dlTest\"";
                //cmd.ExecuteNonQuery();
                //cmd.CommandText = "use database \"dlTest\"";
                //cmd.ExecuteNonQuery();
                cmd.CommandText = $"create schema \"{schemaName}\"";
                cmd.ExecuteNonQuery();
                cmd.CommandText = $"use schema \"{schemaName}\"";
                cmd.ExecuteNonQuery();
                //cmd.CommandText = "create table \"dlTest\".\"dlSchema\".test1 (col1 string, col2 int)";
                cmd.CommandText = $"create table {TableName} (col1 string, col2 int)";
                cmd.ExecuteNonQuery();
                //cmd.CommandText = "insert into \"dlTest\".\"dlSchema\".test1 Values ('test 1', 1);";
                cmd.CommandText = $"insert into {TableName} Values ('test 1', 1);";
                cmd.ExecuteNonQuery();
            }

            using (var conn1 = new SnowflakeDbConnection())
            {
                conn1.ConnectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
                    "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                        testConfig.protocol,
                        testConfig.host,
                        testConfig.port,
                        testConfig.account,
                        testConfig.role,
                        //"\"dlTest\"",
                        testConfig.database,
                        $"\"{schemaName}\"",
                        //testConfig.schema,
                        testConfig.warehouse,
                        testConfig.user,
                        testConfig.password);
                Assert.AreEqual(conn1.State, ConnectionState.Closed);

                conn1.Open();
                using (IDbCommand cmd = conn1.CreateCommand())
                {
                    cmd.CommandText = $"SELECT count(*) FROM {TableName}";
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
                cmd.CommandText = $"drop schema \"{schemaName}\"";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "use database " + testConfig.database;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "use schema " + testConfig.schema;
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
                conn.ConnectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
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
        [Retry(2)]
        public void TestLoginTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                int timeoutSec = 5;
                string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0};maxHttpRetries=0",
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
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    Assert.IsTrue(e.InnerException is TaskCanceledException ||
                        SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode ==
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();
                int delta = 15; // in case server time slower.

                // Should timeout before the defined timeout plus 1 (buffer time)
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (timeoutSec + 1) * 1000);
                // Should timeout after the defined timeout since retry count is infinite
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeoutSec * 1000 - delta);

                Assert.AreEqual(timeoutSec, conn.ConnectionTimeout);
            }
        }

        [Test]
        public void TestLoginWithMaxRetryReached()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                string maxRetryConnStr = ConnectionString + "maxHttpRetries=7";

                conn.ConnectionString = maxRetryConnStr;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (Exception e)
                {
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    Assert.IsTrue(e.InnerException is TaskCanceledException ||
                        SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode ==
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();

                // retry 7 times with starting backoff of 1 second
                // backoff is chosen randomly it can drop to 0. So the minimal backoff time could be 1 + 0 + 0 + 0 + 0 + 0 + 0 = 1
                // The maximal backoff time could be 1 + 2 + 5 + 10 + 21 + 42 + 85 = 166
                Assert.Less(stopwatch.ElapsedMilliseconds, 166 * 1000);
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 1 * 1000);
            }
        }

        [Test]
        [Retry(2)]
        public void TestLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                int connectionTimeout = 600;
                int retryTimeout = 350;
                string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
                    connectionTimeout, retryTimeout);

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
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    Assert.IsTrue(e.InnerException is TaskCanceledException ||
                        SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode ==
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();
                int delta = 10; // in case server time slower.

                // Should timeout before the defined timeout plus 1 (buffer time)
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (retryTimeout + 1) * 1000);
                // Should timeout after the defined timeout since retry count is infinite
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, retryTimeout * 1000 - delta);

                Assert.AreEqual(retryTimeout, conn.ConnectionTimeout);
            }
        }

        [Test]
        [Ignore("Disable unstable test cases for now")]
        public void TestDefaultLoginTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                // Default timeout is 300 sec
                Assert.AreEqual(SFSessionHttpClientProperties.DefaultRetryTimeout, conn.ConnectionTimeout);

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
                        SnowflakeDbExceptionAssert.HasErrorCode(e.InnerException, SFError.REQUEST_TIMEOUT);

                        stopwatch.Stop();
                        int delta = 10; // in case server time slower.

                        // Should timeout after the default timeout (300 sec)
                        Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, conn.ConnectionTimeout * 1000 - delta);
                        // But never more because there's no connection timeout remaining (with 2 seconds margin)
                        Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (conn.ConnectionTimeout + 2) * 1000);
                    }
                }
            }
        }

        [Test]
        public void TestConnectionFailFastForNonRetried404OnLogin()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Just a way to get a 404 on the login request and make sure there are no retry
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;certRevocationCheckMode=enabled;";

                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    SnowflakeDbExceptionAssert.HasHttpErrorCodeInExceptionChain(e, HttpStatusCode.NotFound);
                    SnowflakeDbExceptionAssert.HasMessageInExceptionChain(e, "404 (Not Found)");
                }
                catch (Exception unexpected)
                {
                    Assert.Fail($"Unexpected {unexpected.GetType()} exception occurred");
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        public void TestEnableLoginRetryOn404()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;disableretry=true;forceretryon404=true;certRevocationCheckMode=enabled;";
                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.INTERNAL_ERROR);
                    SnowflakeDbExceptionAssert.HasHttpErrorCodeInExceptionChain(e, HttpStatusCode.NotFound);
                }
                catch (Exception unexpected)
                {
                    Assert.Fail($"Unexpected {unexpected.GetType()} exception occurred");
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
            }
        }

        [Test]
        public void TestValidateDefaultParameters()
        {
            string connectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
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

                conn.ChangeDatabase(testConfig.database);
                conn.Close();
            }

        }

        [Test]
        public void TestConnectWithoutHost()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                string connStrFmt = "account={0};user={1};password={2};certRevocationCheckMode=enabled;";
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

                string connStrFmt = "scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
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
            using (IDbConnection conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                CreateOrReplaceTable(conn, TableName, new[] { "c INT" });
                var t1 = conn.BeginTransaction();
                var t1c1 = conn.CreateCommand();
                t1c1.Transaction = t1;
                t1c1.CommandText = $"insert into {TableName} values (1)";
                t1c1.ExecuteNonQuery();
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Previous connection would be disposed and
                // uncommitted txn would rollback at this point
                conn.ConnectionString = ConnectionString;
                conn.Open();
                IDbCommand command = conn.CreateCommand();
                command.CommandText = $"SELECT * FROM {TableName}";
                IDataReader reader = command.ExecuteReader();
                Assert.IsFalse(reader.Read());
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
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.UNKNOWN_AUTHENTICATOR);
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
                        testConfig.oktaUrl,
                        testConfig.oktaUser,
                        testConfig.oktaPassword);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        public void TestOktaConnectionUntilMaxTimeout()
        {
            var expectedMaxRetryCount = 15;
            var expectedMaxConnectionTimeout = 450;
            var oktaUrl = "https://test.okta.com";
            var mockRestRequester = new MockOktaRestRequester()
            {
                TokenUrl = $"{oktaUrl}/api/v1/sessions?additionalFields=cookieToken",
                SSOUrl = $"{oktaUrl}/app/testaccount/sso/saml",
                ResponseContent = "<form=error}",
                MaxRetryCount = expectedMaxRetryCount,
                MaxRetryTimeout = expectedMaxConnectionTimeout
            };
            using (DbConnection conn = new MockSnowflakeDbConnection(mockRestRequester))
            {
                try
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator={0};user=test;password=test;MAXHTTPRETRIES={1};RETRY_TIMEOUT={2};",
                            oktaUrl,
                            expectedMaxRetryCount,
                            expectedMaxConnectionTimeout);
                    conn.Open();
                    Assert.Fail();
                }
                catch (Exception e)
                {
                    Assert.IsInstanceOf<SnowflakeDbException>(e);
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.INTERNAL_ERROR);
                    Assert.IsTrue(e.Message.Contains(
                        $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                        $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                        "while trying to authenticate through Okta"));
                }
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
                        testConfig.oktaUrl,
                        testConfig.oktaUser,
                        testConfig.oktaPassword);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }


            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        testConfig.oktaUrl,
                        testConfig.oktaUser,
                        testConfig.oktaPassword);
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUser()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                // connection pooling is disabled for external browser by default
                Assert.AreEqual(false, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.AreEqual("QA", command.ExecuteScalar().ToString());
                }
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithPoolingEnabled()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;POOLINGENABLED=TRUE";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
                Assert.AreEqual(true, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.AreEqual("QA", command.ExecuteScalar().ToString());
                }
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUserAsync()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);
                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Task<object> task = command.ExecuteScalarAsync(CancellationToken.None);
                    task.Wait(CancellationToken.None);
                    Assert.AreEqual("QA", task.Result);
                }
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUserAndDisableConsoleLogin()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;disable_console_login=false;";
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
        public void TestSSOConnectionWithUserAsyncAndDisableConsoleLogin()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;disable_console_login=false;";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);
                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Task<object> task = command.ExecuteScalarAsync(CancellationToken.None);
                    task.Wait(CancellationToken.None);
                    Assert.AreEqual("QA", task.Result);
                }
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionTimeoutAfter10s()
        {
            // Do not log in by external browser - timeout after 10s should happen
            int waitSeconds = 10;
            Stopwatch stopwatch = Stopwatch.StartNew();
            Assert.Throws<SnowflakeDbException>(() =>
                {
                    using (IDbConnection conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString
                            = ConnectionStringWithoutAuth
                              + $";authenticator=externalbrowser;user=qa@snowflakecomputing.com;BROWSER_RESPONSE_TIMEOUT={waitSeconds}";
                        conn.Open();
                        Assert.AreEqual(ConnectionState.Open, conn.State);
                        using (IDbCommand command = conn.CreateCommand())
                        {
                            command.CommandText = "SELECT CURRENT_USER()";
                            Assert.AreEqual("QA", command.ExecuteScalar().ToString());
                        }
                    }
                }
            );
            stopwatch.Stop();

            // timeout after specified number of seconds
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, waitSeconds * 1000);
            // and not later than 5s after expected time
            Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (waitSeconds + 5) * 1000);
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithTokenCaching()
        {
            /*
             * This test checks that the connector successfully stores an SSO token and uses it for authentication if it exists
             * 1. Login normally using external browser with CLIENT_STORE_TEMPORARY_CREDENTIAL enabled
             * 2. Login again, this time without a browser, as the connector should be using the SSO token retrieved from step 1
            */

            // Set the CLIENT_STORE_TEMPORARY_CREDENTIAL property to true to enable token caching
            // The specified user should be configured for SSO
            var externalBrowserConnectionString
                = ConnectionStringWithoutAuth
                    + $";authenticator=externalbrowser;user={testConfig.user};CLIENT_STORE_TEMPORARY_CREDENTIAL=true;poolingEnabled=false";

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate to retrieve and store the token if doesn't exist or invalid
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithInvalidCachedToken()
        {
            /*
             * This test checks that the connector will attempt to re-authenticate using external browser if the token retrieved from the cache is invalid
             * 1. Create a credential manager and save credentials for the user with a wrong token
             * 2. Open a connection which initially should try to use the token and then switch to external browser when the token fails
            */

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Set the CLIENT_STORE_TEMPORARY_CREDENTIAL property to true to enable token caching
                conn.ConnectionString
                    = ConnectionStringWithoutAuth
                        + $";authenticator=externalbrowser;user={testConfig.user};CLIENT_STORE_TEMPORARY_CREDENTIAL=true;";

                // Create a credential manager and save a wrong token for the test user
                var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(testConfig.host, testConfig.user, TokenType.IdToken);
                var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
                credentialManager.SaveCredentials(key, "wrongToken");

                // Use the credential manager with the wrong token
                SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

                // Open a connection which should switch to external browser after trying to connect using the wrong token
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                // Switch back to the default credential manager
                SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
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
                    ConnectionString + "connection_timeout=20;useProxy=true;proxyHost=Invalid;proxyPort=8080;";
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

            // Another authenticated proxy connection, same proxy but crl check is disabled
            // Will use a different httpclient
            using (var conn5 = new SnowflakeDbConnection())
            {
                conn5.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(ConnectionString
                    + String.Format(
                        ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};",
                        testConfig.authProxyHost,
                        testConfig.authProxyPort,
                        testConfig.authProxyUser,
                        testConfig.authProxyPwd));
                conn5.Open();
            }

            // No proxy again, but crl check is disabled
            // Will use a different httpclient
            using (var conn6 = new SnowflakeDbConnection())
            {

                conn6.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(ConnectionString);
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

            // No proxy again, crl check is enabled in the default connection string for tests
            // Should use same httpclient than conn2
            using (var conn8 = new SnowflakeDbConnection())
            {
                conn8.ConnectionString = ConnectionString;
                conn8.Open();
            }

            // Another authenticated proxy with bypasslist, but this will create a new httpclient because of
            // disabled certificate revocation check
            using (var conn9 = new SnowflakeDbConnection())
            {
                conn9.ConnectionString
              = ConnectionStringModifier.DisableCrlRevocationCheck(ConnectionString)
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4};",
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

            // No proxy, but crl check disabled
            // Should use same httpclient than conn6
            using (var conn11 = new SnowflakeDbConnection())
            {
                conn11.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(ConnectionString);
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
                    s_logger.Debug("Failed opening connection ", e);
                    Assert.AreEqual(270001, e.ErrorCode); //Internal error
                    AssertIsConnectionFailure(e);
                }
            }
        }

        [Test]
        [TestCase("*")]
        [TestCase("*{0}*")]
        [TestCase("^*{0}*")]
        [TestCase("*{0}*$")]
        [TestCase("^*{0}*$")]
        [TestCase("^nonmatch*{0}$|*")]
        [TestCase("*a*", "a")]
        [TestCase("*la*", "la")]
        public void TestNonProxyHostShouldBypassProxyServer(string regexHost, string proxyHost = "proxyserverhost")
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                var host = ResolveHost();
                var nonProxyHosts = string.Format(regexHost, $"{host}");
                conn.ConnectionString =
                    $"{ConnectionString}USEPROXY=true;PROXYHOST={proxyHost};NONPROXYHOSTS={nonProxyHosts};PROXYPORT=3128;";

                // Act
                conn.Open();

                // Assert
                // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [TestCase("invalid{0}")]
        [TestCase("*invalid{0}*")]
        [TestCase("^invalid{0}$")]
        [TestCase("*a.b")]
        [TestCase("a", "a")]
        [TestCase("la", "la")]
        public void TestNonProxyHostShouldNotBypassProxyServer(string regexHost, string proxyHost = "proxyserverhost")
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                var nonProxyHosts = string.Format(regexHost, $"{testConfig.host}");
                conn.ConnectionString =
                    $"{ConnectionString}connection_timeout=5;USEPROXY=true;PROXYHOST={proxyHost};NONPROXYHOSTS={nonProxyHosts};PROXYPORT=3128;";

                // Act/Assert
                // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
                var exception = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // Assert
                Assert.AreEqual(270001, exception.ErrorCode);
                AssertIsConnectionFailure(exception);
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
                    $"*.foo.com %7C{testConfig.account}.snowflakecomputing.com|*{testConfig.host}*");
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

            string[] connectionStrings = {
                baseConnectionString,
                ConnectionStringModifier.DisableCrlRevocationCheck(baseConnectionString),
                baseConnectionString + authenticatedProxy,
                ConnectionStringModifier.DisableCrlRevocationCheck(baseConnectionString + authenticatedProxy),
                baseConnectionString + authenticatedProxy + byPassList,
                ConnectionStringModifier.DisableCrlRevocationCheck(baseConnectionString + authenticatedProxy + byPassList)
            };

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
        [Ignore("Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
        public void TestEscapeChar()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false;key1=test\'password;key2=test\"password;key3=test==password";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
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
        [Ignore("Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
        public void TestEscapeChar1()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString + "poolingEnabled=false;key==word=value; key1=\"test;password\"; key2=\"test=password\"";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);

                Assert.AreEqual(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
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
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString + "poolingEnabled=false;CLIENT_SESSION_KEEP_ALIVE=true";
            conn.Open();

            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs
            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.AreEqual(command.ExecuteScalar(), 46);
            }

            conn.Close();
            Assert.AreEqual(ConnectionState.Closed, conn.State);
        }

        [Test]
        [Ignore("Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public void TestHeartBeatWithConnectionPool()
        {
            SnowflakeDbConnectionPool.ClearAllPools();

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = ConnectionString + "maxPoolSize=2;minPoolSize=0;expirationTimeout=14800;CLIENT_SESSION_KEEP_ALIVE=true";
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
            // pooled connection expires in 5 seconds so after 5 seconds,
            // one connection per second will be closed
            var connectionString = ConnectionString + "maxPoolSize=20;ExpirationTimeout=5;CLIENT_SESSION_KEEP_ALIVE=true";
            // heart beat interval is validity/4 so send out per 5 seconds
            HeartBeatBackground.setValidity(20);
            try
            {
                for (int i = 0; i < connCount; i++)
                {
                    using (var conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = connectionString;
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
                string infiniteLoginTimeOut = String.Format(ConnectionString + ";connection_timeout={0};maxHttpRetries=0",
                    timeoutSec);

                conn.ConnectionString = infiniteLoginTimeOut;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                Task connectTask = conn.OpenAsync(connectionCancelToken.Token);

                Assert.AreEqual(ConnectionState.Connecting, conn.State);

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
                Assert.AreEqual(timeoutSec, conn.ConnectionTimeout);
            }
        }

        [Test]
        public void TestAsyncLoginTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                int timeoutSec = 5;
                string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0};maxHttpRetries=0",
                    timeoutSec);
                conn.ConnectionString = loginTimeOut5sec;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    Task connectTask = conn.OpenAsync(CancellationToken.None);
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCode(e.InnerException, SFError.INTERNAL_ERROR);
                }
                stopwatch.Stop();
                int delta = 10; // in case server time slower.

                // Should timeout after the defined timeout since retry count is infinite
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeoutSec * 1000 - delta);
                // But never more than 3 sec (buffer time) after the defined timeout
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (timeoutSec + 3) * 1000);

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(timeoutSec, conn.ConnectionTimeout);
            }
        }

        [Test]
        [Retry(2)]
        public void TestAsyncLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                int connectionTimeout = 600;
                int retryTimeout = 350;
                string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
                    connectionTimeout, retryTimeout);
                conn.ConnectionString = loginTimeOut5sec;

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    Task connectTask = conn.OpenAsync(CancellationToken.None);
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCode(e.InnerException, SFError.INTERNAL_ERROR);
                }
                stopwatch.Stop();
                int delta = 10; // in case server time slower.

                // Should timeout after the defined timeout since retry count is infinite
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, retryTimeout * 1000 - delta);
                // But never more than 2 sec (buffer time) after the defined timeout
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (retryTimeout + 2) * 1000);

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(retryTimeout, conn.ConnectionTimeout);
            }
        }

        [Test]
        public void TestAsyncDefaultLoginTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                // unlimited retry count to trigger the timeout
                conn.ConnectionString = ConnectionString + "maxHttpRetries=0";

                Assert.AreEqual(conn.State, ConnectionState.Closed);

                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    Task connectTask = conn.OpenAsync(CancellationToken.None);
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCode(e.InnerException, SFError.INTERNAL_ERROR);
                }
                stopwatch.Stop();
                int delta = 10; // in case server time slower.

                // Should timeout after the default timeout (300 sec)
                Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, conn.ConnectionTimeout * 1000 - delta);
                // But never more because there's no connection timeout remaining (with 2 seconds margin)
                Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (conn.ConnectionTimeout + 2) * 1000);

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.AreEqual(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
            }
        }

        [Test]
        public void TestAsyncConnectionFailFastForNonRetried404OnLogin()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Just a way to get a 404 on the login request and make sure there are no retry
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;certRevocationCheckMode=enabled;";

                conn.ConnectionString = invalidConnectionString;

                Assert.AreEqual(conn.State, ConnectionState.Closed);
                Task connectTask = null;
                try
                {
                    connectTask = conn.OpenAsync(CancellationToken.None);
                    connectTask.Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    SnowflakeDbExceptionAssert.HasHttpErrorCodeInExceptionChain(e, HttpStatusCode.NotFound);
                    SnowflakeDbExceptionAssert.HasMessageInExceptionChain(e, "404 (Not Found)");
                }
                catch (Exception unexpected)
                {
                    Assert.Fail($"Unexpected {unexpected.GetType()} exception occurred");
                }

                Assert.AreEqual(ConnectionState.Closed, conn.State);
                Assert.IsTrue(connectTask.IsFaulted);
            }
        }

        [Test]
        public void TestCloseAsyncWithCancellation()
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
                task = conn.CloseAsync(CancellationToken.None);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);

                // Open the connection
                task = conn.OpenAsync(CancellationToken.None);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync(CancellationToken.None);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);

                // Close the connection again.
                task = conn.CloseAsync(CancellationToken.None);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);
            }
        }

#if NETCOREAPP3_0_OR_GREATER
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
                task = conn.CloseAsync();
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);

                // Open the connection
                task = conn.OpenAsync();
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync();
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);

                // Close the connection again.
                task = conn.CloseAsync();
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Closed);
            }
        }
#endif

        [Test]
        public void TestCloseAsyncFailure()
        {
            using (var conn = new MockSnowflakeDbConnection(new MockCloseSessionException()))
            {
                conn.ConnectionString = ConnectionString;
                Assert.AreEqual(conn.State, ConnectionState.Closed);
                Task task = null;

                // Open the connection
                task = conn.OpenAsync(CancellationToken.None);
                task.Wait();
                Assert.AreEqual(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync(CancellationToken.None);
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

        [Test]
        public void TestExplicitTransactionOperationsTracked()
        {
            using (var conn = new SnowflakeDbConnection(ConnectionString))
            {
                conn.Open();
                Assert.AreEqual(false, conn.HasActiveExplicitTransaction());

                var trans = conn.BeginTransaction();
                Assert.AreEqual(true, conn.HasActiveExplicitTransaction());
                trans.Rollback();
                Assert.AreEqual(false, conn.HasActiveExplicitTransaction());

                conn.BeginTransaction().Rollback();
                Assert.AreEqual(false, conn.HasActiveExplicitTransaction());

                conn.BeginTransaction().Commit();
                Assert.AreEqual(false, conn.HasActiveExplicitTransaction());
            }
        }


        [Test]
        public void TestAsyncOktaConnectionUntilMaxTimeout()
        {
            var expectedMaxRetryCount = 15;
            var expectedMaxConnectionTimeout = 450;
            var oktaUrl = "https://test.okta.com";
            var mockRestRequester = new MockOktaRestRequester()
            {
                TokenUrl = $"{oktaUrl}/api/v1/sessions?additionalFields=cookieToken",
                SSOUrl = $"{oktaUrl}/app/testaccount/sso/saml",
                ResponseContent = "<form=error}",
                MaxRetryCount = expectedMaxRetryCount,
                MaxRetryTimeout = expectedMaxConnectionTimeout
            };
            using (DbConnection conn = new MockSnowflakeDbConnection(mockRestRequester))
            {
                try
                {
                    conn.ConnectionString
                        = ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator={0};user=test;password=test;MAXHTTPRETRIES={1};RETRY_TIMEOUT={2};",
                            oktaUrl,
                            expectedMaxRetryCount,
                            expectedMaxConnectionTimeout);
                    Task connectTask = conn.OpenAsync(CancellationToken.None);
                    connectTask.Wait();
                    Assert.Fail();
                }
                catch (Exception e)
                {
                    Assert.IsInstanceOf<SnowflakeDbException>(e.InnerException);
                    SnowflakeDbExceptionAssert.HasErrorCode(e.InnerException, SFError.INTERNAL_ERROR);
                    Exception oktaException;
#if NETFRAMEWORK
                    oktaException = e.InnerException.InnerException.InnerException;
#else
                    oktaException = e.InnerException.InnerException;
#endif
                    Assert.IsTrue(oktaException.Message.Contains(
                        $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                        $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                        "while trying to authenticate through Okta"));
                }
            }
        }

        [Test]
        [Ignore("This test requires established dev Okta SSO and credentials matching Snowflake user")]
        public void TestNativeOktaSuccess()
        {
            var oktaUrl = "https://***.okta.com/";
            var oktaUser = "***";
            var oktaPassword = "***";
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionStringWithoutAuth +
                                        $";authenticator={oktaUrl};user={oktaUser};password={oktaPassword};";
                conn.Open();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        public void TestConnectStringWithQueryTag()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string expectedQueryTag = "Test QUERY_TAG 12345";
                conn.ConnectionString = ConnectionString + $";query_tag={expectedQueryTag}";

                conn.Open();
                var command = conn.CreateCommand();
                // This query itself will be part of the history and will have the query tag
                command.CommandText = "SELECT QUERY_TAG FROM table(information_schema.query_history_by_session())";
                var queryTag = command.ExecuteScalar();

                Assert.AreEqual(expectedQueryTag, queryTag);
            }
        }

        [Test]
        public void TestUseMultiplePoolsConnectionPoolByDefault()
        {
            // act
            var poolVersion = SnowflakeDbConnectionPool.GetConnectionPoolVersion();

            // assert
            Assert.AreEqual(ConnectionPoolType.MultipleConnectionPool, poolVersion);
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")] // to enroll to mfa authentication edit your user profile
        public void TestMFATokenCachingWithPasscodeFromConnectionString()
        {
            // Use a connection with MFA enabled and set passcode property for mfa authentication. e.g. ConnectionString + ";authenticator=username_password_mfa;passcode=(set proper passcode)"
            // ACCOUNT PARAMETER ALLOW_CLIENT_MFA_CACHING should be set to true in the account.
            // On Mac/Linux OS the default credential manager is a file based one. Uncomment the following line to test in memory implementation.
            // SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = ConnectionString
                      + ";authenticator=username_password_mfa;application=DuoTest;minPoolSize=0;passcode=(set proper passcode)";


                // Authenticate to retrieve and store the token if doesn't exist or invalid
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [Ignore("Requires manual steps and environment with mfa authentication enrolled")] // to enroll to mfa authentication edit your user profile
        public void TestMfaWithPasswordConnectionUsingPasscodeWithSecureString()
        {
            // Use a connection with MFA enabled and Passcode property on connection instance.
            // ACCOUNT PARAMETER ALLOW_CLIENT_MFA_CACHING should be set to true in the account.
            // On Mac/Linux OS the default credential manager is a file based one. Uncomment the following line to test in memory implementation.
            // SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();
            // arrange
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.Passcode = SecureStringHelper.Encode("$(set proper passcode)");
                // manual action: stop here in breakpoint to provide proper passcode by: conn.Passcode = SecureStringHelper.Encode("...");
                conn.ConnectionString = ConnectionString + "minPoolSize=2;application=DuoTest;";

                // act
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();

                // assert
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }
        }

        [Test]
        [TestCase("connection_timeout=5;")]
        [TestCase("")]
        public void TestOpenAsyncThrowExceptionWhenConnectToUnreachableHost(string extraParameters)
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;useProxy=true;proxyHost=no.such.pro.xy;proxyPort=8080;certRevocationCheckMode=enabled;" +
                                   extraParameters;
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                // act
                var thrown = Assert.Throws<AggregateException>(() => connection.OpenAsync().Wait());

                // assert
                Assert.IsTrue(thrown.InnerException is TaskCanceledException || thrown.InnerException is SnowflakeDbException);
                if (thrown.InnerException is SnowflakeDbException)
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown.InnerException, SFError.INTERNAL_ERROR);
                Assert.AreEqual(ConnectionState.Closed, connection.State);
            }
        }

        [Test]
        public void TestOpenAsyncThrowExceptionWhenOperationIsCancelled()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;useProxy=true;proxyHost=no.such.pro.xy;proxyPort=8080;certRevocationCheckMode=enabled;";
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                var shortCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                // act
                var thrown = Assert.Throws<AggregateException>(() => connection.OpenAsync(shortCancellation.Token).Wait());

                // assert
                Assert.IsInstanceOf<TaskCanceledException>(thrown.InnerException);
                Assert.AreEqual(ConnectionState.Closed, connection.State);
            }
        }

        [Test]
        [Ignore("This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithTokenCachingAsync()
        {
            /*
             * This test checks that the connector successfully stores an SSO token and uses it for authentication if it exists
             * 1. Login normally using external browser with CLIENT_STORE_TEMPORARY_CREDENTIAL enabled
             * 2. Login again, this time without a browser, as the connector should be using the SSO token retrieved from step 1
            */

            // Set the CLIENT_STORE_TEMPORARY_CREDENTIAL property to true to enable token caching
            // The specified user should be configured for SSO
            var externalBrowserConnectionString
                = ConnectionStringWithoutAuth
                    + $";authenticator=externalbrowser;user={testConfig.user};CLIENT_STORE_TEMPORARY_CREDENTIAL=true;poolingEnabled=false";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate to retrieve and store the token if doesn't exist or invalid
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.AreEqual(ConnectionState.Open, conn.State);
            }

        }

        [Test]
        public void TestCloseSessionWhenGarbageCollectorFinalizesConnection()
        {
            // arrange
            var session = GetSessionFromForgottenConnection();
            Assert.NotNull(session);
            Assert.NotNull(session.sessionId);
            Assert.NotNull(session.sessionToken);

            // act
            GC.Collect();
            Awaiter.WaitUntilConditionOrTimeout(() => session.sessionToken == null, TimeSpan.FromSeconds(15));

            // assert
            Assert.IsNull(session.sessionToken);
        }

        private SFSession GetSessionFromForgottenConnection()
        {
            var connection = new SnowflakeDbConnection(ConnectionString + ";poolingEnabled=false;application=TestGarbageCollectorCloseSession");
            connection.Open();
            return connection.SfSession;
        }

        [Test]
        public void TestHangingCloseIsNotBlocking()
        {
            // arrange
            var restRequester = new MockCloseHangingRestRequester();
            var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            session.Open();
            var watchClose = new Stopwatch();
            var watchClosedFinished = new Stopwatch();

            // act
            watchClose.Start();
            watchClosedFinished.Start();
            session.CloseNonBlocking();
            watchClose.Stop();
            Awaiter.WaitUntilConditionOrTimeout(() => restRequester.CloseRequests.Count > 0, TimeSpan.FromSeconds(15));
            watchClosedFinished.Stop();

            // assert
            Assert.AreEqual(1, restRequester.CloseRequests.Count);
            Assert.Less(watchClose.Elapsed.Duration(), TimeSpan.FromSeconds(5)); // close executed immediately
            Assert.GreaterOrEqual(watchClosedFinished.Elapsed.Duration(), TimeSpan.FromSeconds(10)); // while background task took more time
        }

        [Test]
        [Ignore("Manual test only")]
        public void TestOAuthFlow()
        {
            // arrange
            var driverRootPath = Path.Combine("..", "..", "..", "..");
            var configFilePath = Path.Combine(driverRootPath, "..", ".parameters_oauth_authorization_code_okta.json"); // Adjust to a proper config for your manual testing
            var authenticator = OAuthAuthorizationCodeAuthenticator.AuthName; // Set either OAuthAuthorizationCodeAuthenticator.AuthName or OAuthClientCredentialsAuthenticator.AuthName
            var testConfig = TestEnvironment.ReadTestConfigFile(configFilePath);
            RemoveOAuthCache(testConfig);
            try
            {
                using (var connection = new SnowflakeDbConnection(ConnectionStringForOAuthFlows(testConfig, authenticator)))
                {
                    // act
                    connection.Open();
                }
            }
            finally
            {
                RemoveOAuthCache(testConfig);
            }
        }

        [Test]
        [Ignore("Manual test only")]
        public void TestProgrammaticAccessTokenAuthentication()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionStringForPat(testConfig)))
            {
                // act
                connection.Open();
            }
        }

        private void RemoveOAuthCache(TestConfig testConfig)
        {
            var host = new Uri(testConfig.oauthTokenRequestUrl).Host;
            var accessCacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, testConfig.user, TokenType.OAuthAccessToken);
            var refreshCacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, testConfig.user, TokenType.OAuthRefreshToken);
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();
            credentialManager.RemoveCredentials(accessCacheKey);
            credentialManager.RemoveCredentials(refreshCacheKey);
        }

        private string ConnectionStringForOAuthFlows(TestConfig testConfig, string authenticator)
        {
            var builder = new StringBuilder()
                .Append($"authenticator={authenticator};user={testConfig.user};password={testConfig.password};account={testConfig.account};certRevocationCheckMode=enabled;")
                .Append($"db={testConfig.database};role={testConfig.role};warehouse={testConfig.warehouse};host={testConfig.host};port={testConfig.port};")
                .Append($"oauthClientId={testConfig.oauthClientId};oauthClientSecret={testConfig.oauthClientSecret};oauthScope={testConfig.oauthScope};")
                .Append($"oauthTokenRequestUrl={testConfig.oauthTokenRequestUrl};")
                .Append("poolingEnabled=false;");
            switch (authenticator)
            {
                case OAuthAuthorizationCodeAuthenticator.AuthName:
                    return builder
                        .Append($"oauthRedirectUri={testConfig.oauthRedirectUri};")
                        .Append($"oauthAuthorizationUrl={testConfig.oauthAuthorizationUrl}")
                        .ToString();
                case OAuthClientCredentialsAuthenticator.AuthName:
                    return builder.ToString();
                default:
                    throw new Exception("Unknown authenticator");
            }
        }

        private string ConnectionStringForPat(TestConfig testConfig)
        {
            var role = "ANALYST";
            return new StringBuilder()
                .Append($"authenticator=programmatic_access_token;user={testConfig.user};account={testConfig.account};certRevocationCheckMode=enabled;")
                .Append($"db={testConfig.database};role={role};warehouse={testConfig.warehouse};host={testConfig.host};port={testConfig.port};")
                .Append($"token={testConfig.programmaticAccessToken};")
                .Append("poolingEnabled=false;")
                .ToString();
        }
    }
}


