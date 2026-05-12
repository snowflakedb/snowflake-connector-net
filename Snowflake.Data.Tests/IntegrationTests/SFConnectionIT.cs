using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
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
    class SFConnectionIT : SFBaseTest
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFConnectionIT(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture) { _fixture = fixture; }

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [Fact]
        public void TestBasicConnection()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);

                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.Equal("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                if (!string.IsNullOrEmpty(serverVersion))
                {
                    string[] versionElements = serverVersion.Split('.');
                    Assert.Equal(3, versionElements.Length);
                }

                conn.Close();
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact]
        public void TestApplicationName()
        {
            string[] validApplicationNames = { "test1234", "test_1234", "test-1234", "test.1234" };
            string[] invalidApplicationNames = { "1234test", "test$A", "test<script>" };

            // Valid names
            foreach (string appName in validApplicationNames)
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString;
                    conn.ConnectionString += $"application={appName}";
                    conn.Open();
                    Assert.Equal(ConnectionState.Open, conn.State);

                    conn.Close();
                    Assert.Equal(ConnectionState.Closed, conn.State);
                }
            }

            // Invalid names
            foreach (string appName in invalidApplicationNames)
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString;
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

                    Assert.Equal(ConnectionState.Closed, conn.State);
                }
            }
        }

        [Fact]
        [RunOnlyOnCI]
        public void TestApplicationPathIsSentDuringAuthentication()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                conn.Open();

                var authenticator = (BaseAuthenticator)conn.SfSession.authenticator;
                var clientEnv = authenticator.BuildLoginRequestData().clientEnv;
                var lowerPath = clientEnv.applicationPath.ToLower();
#if NETFRAMEWORK
                Assert.True(
                    lowerPath.Contains("testhost") &&
                    (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")));
#else
                Assert.True(
                    lowerPath.Contains("snowflake.data.tests") &&
                    lowerPath.Contains("bin") &&
                    lowerPath.Contains("testhost") &&
                    (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")),
                    $"APPLICATION_PATH should contain 'snowflake.data.tests', 'bin', 'testhost' and end with .dll or .exe. Got: {clientEnv.applicationPath}");
#endif
            }
        }

        [Fact]
        public void TestIncorrectUserOrPasswordBasicConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
            "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                    _fixture.testConfig.protocol,
                    _fixture.testConfig.host,
                    _fixture.testConfig.port,
                    _fixture.testConfig.account,
                    _fixture.testConfig.role,
                    _fixture.testConfig.database,
                    _fixture.testConfig.schema,
                    _fixture.testConfig.warehouse,
                    "unknown",
                    _fixture.testConfig.password);

                Assert.Equal(conn.State, ConnectionState.Closed);
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

                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestConnectionIsNotMarkedAsOpenWhenWasNotCorrectlyOpenedBefore(bool explicitClose)
        {
            for (int i = 0; i < 2; ++i)
            {
                s_logger.Debug($"Running try #{i}");
                SnowflakeDbConnection snowflakeConnection = null;
                try
                {
                    snowflakeConnection = new SnowflakeDbConnection(_fixture.ConnectionStringWithInvalidUserName);
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

        [Fact]
        public void TestConnectionIsNotMarkedAsOpenWhenWasNotCorrectlyOpenedWithUsingClause()
        {
            for (int i = 0; i < 2; ++i)
            {
                s_logger.Debug($"Running try #{i}");
                SnowflakeDbConnection snowflakeConnection = null;
                try
                {
                    using (snowflakeConnection = new SnowflakeDbConnection(_fixture.ConnectionStringWithInvalidUserName))
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
            Assert.False(snowflakeDbConnection.IsOpen()); // check via public method
            Assert.Equal(ConnectionState.Closed, snowflakeDbConnection.State); // ensure internal state is expected
        }

        private static void AssertIsConnectionFailure(SnowflakeDbException e)
        {
            Assert.Equal(SnowflakeDbException.CONNECTION_FAILURE_SSTATE, e.SqlState);
        }

        [Fact]
        public void TestConnectString()
        {
            var schemaName = "dlSchema_" + Guid.NewGuid().ToString().Replace("-", "_");
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString;
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
                cmd.CommandText = $"create table {_fixture.TableName} (col1 string, col2 int)";
                cmd.ExecuteNonQuery();
                //cmd.CommandText = "insert into \"dlTest\".\"dlSchema\".test1 Values ('test 1', 1);";
                cmd.CommandText = $"insert into {_fixture.TableName} Values ('test 1', 1);";
                cmd.ExecuteNonQuery();
            }

            using (var conn1 = new SnowflakeDbConnection())
            {
                conn1.ConnectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
                    "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                        _fixture.testConfig.protocol,
                        _fixture.testConfig.host,
                        _fixture.testConfig.port,
                        _fixture.testConfig.account,
                        _fixture.testConfig.role,
                        //"\"dlTest\"",
                        _fixture.testConfig.database,
                        $"\"{schemaName}\"",
                        //_fixture.testConfig.schema,
                        _fixture.testConfig.warehouse,
                        _fixture.testConfig.user,
                        _fixture.testConfig.password);
                Assert.Equal(conn1.State, ConnectionState.Closed);

                conn1.Open();
                using (IDbCommand cmd = conn1.CreateCommand())
                {
                    cmd.CommandText = $"SELECT count(*) FROM {_fixture.TableName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.True(reader.Read());
                    Assert.Equal(1, reader.GetInt32(0));
                }
                conn1.Close();

                Assert.Equal(ConnectionState.Closed, conn1.State);
            }

            using (IDbCommand cmd = conn.CreateCommand())
            {
                //cmd.CommandText = "drop database \"dlTest\"";
                cmd.CommandText = $"drop schema \"{schemaName}\"";
                cmd.ExecuteNonQuery();
                cmd.CommandText = "use database " + _fixture.testConfig.database;
                cmd.ExecuteNonQuery();
                cmd.CommandText = "use schema " + _fixture.testConfig.schema;
                cmd.ExecuteNonQuery();
            }
            conn.Close();
        }

        [Fact(Skip = "TestConnectStringWithUserPwd, this will popup an internet browser for external login.")]
        public void TestConnectStringWithUserPwd()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
            "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};authenticator={10};",
                    _fixture.testConfig.protocol,
                    _fixture.testConfig.host,
                    _fixture.testConfig.port,
                    _fixture.testConfig.account,
                    _fixture.testConfig.role,
                    _fixture.testConfig.database,
                    _fixture.testConfig.schema,
                    _fixture.testConfig.warehouse,
                    "",
                    "",
                    "externalbrowser");

                Assert.Equal(conn.State, ConnectionState.Closed);
                conn.Open();
                conn.Close();
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact]
        public void TestConnectViaSecureString()
        {
            String[] connEntries = _fixture.ConnectionString.Split(';');
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

                Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
                Assert.Equal(conn.State, ConnectionState.Open);

                conn.Close();
            }
        }

        [Fact]
        public void TestLoginTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                int timeoutSec = 5;
                string loginTimeOut5sec = String.Format(_fixture.ConnectionString + "connection_timeout={0};maxHttpRetries=0",
                    timeoutSec);

                conn.ConnectionString = loginTimeOut5sec;

                Assert.Equal(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    Assert.True(e.InnerException is TaskCanceledException ||
                        SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode ==
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();
                int delta = 15; // in case server time slower.

                // Should timeout before the defined timeout plus 1 (buffer time)
                Assert.True(stopwatch.ElapsedMilliseconds <= (timeoutSec + 1) * 1000);
                // Should timeout after the defined timeout since retry count is infinite
                Assert.True(stopwatch.ElapsedMilliseconds >= timeoutSec * 1000 - delta);

                Assert.Equal(timeoutSec, conn.ConnectionTimeout);
            }
        }

        [Fact]
        public void TestLoginWithMaxRetryReached()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                string maxRetryConnStr = _fixture.ConnectionString + "maxHttpRetries=7";

                conn.ConnectionString = maxRetryConnStr;

                Assert.Equal(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (Exception e)
                {
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    Assert.True(e.InnerException is TaskCanceledException ||
                        SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode ==
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();

                // retry 7 times with starting backoff of 1 second
                // backoff is chosen randomly it can drop to 0. So the minimal backoff time could be 1 + 0 + 0 + 0 + 0 + 0 + 0 = 1
                // The maximal backoff time could be 1 + 2 + 5 + 10 + 21 + 42 + 85 = 166
                Assert.True(stopwatch.ElapsedMilliseconds < 166 * 1000);
                Assert.True(stopwatch.ElapsedMilliseconds >= 1 * 1000);
            }
        }

        [Fact]
        public void TestLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                int connectionTimeout = 600;
                int retryTimeout = 350;
                string loginTimeOut5sec = String.Format(_fixture.ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
                    connectionTimeout, retryTimeout);

                conn.ConnectionString = loginTimeOut5sec;

                Assert.Equal(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    Assert.True(e.InnerException is TaskCanceledException ||
                        SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode ==
                        ((SnowflakeDbException)e.InnerException).ErrorCode);
                }
                stopwatch.Stop();
                int delta = 10; // in case server time slower.

                // Should timeout before the defined timeout plus 1 (buffer time)
                Assert.True(stopwatch.ElapsedMilliseconds <= (retryTimeout + 1) * 1000);
                // Should timeout after the defined timeout since retry count is infinite
                Assert.True(stopwatch.ElapsedMilliseconds >= retryTimeout * 1000 - delta);

                Assert.Equal(retryTimeout, conn.ConnectionTimeout);
            }
        }

        [Fact]
        public void TestDefaultLoginTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;

                // Default timeout is 300 sec
                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);

                Assert.Equal(conn.State, ConnectionState.Closed);
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
                        Assert.True(stopwatch.ElapsedMilliseconds >= conn.ConnectionTimeout * 1000 - delta);
                        // But never more because there's no connection timeout remaining (with 2 seconds margin)
                        Assert.True(stopwatch.ElapsedMilliseconds <= (conn.ConnectionTimeout + 2) * 1000);
                    }
                }
            }
        }

        [Fact]
        public void TestConnectionFailFastForNonRetried404OnLogin()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Just a way to get a 404 on the login request and make sure there are no retry
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;certRevocationCheckMode=enabled;";

                conn.ConnectionString = invalidConnectionString;

                Assert.Equal(conn.State, ConnectionState.Closed);
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

                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact]
        public void TestEnableLoginRetryOn404()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;disableretry=true;forceretryon404=true;certRevocationCheckMode=enabled;";
                conn.ConnectionString = invalidConnectionString;

                Assert.Equal(conn.State, ConnectionState.Closed);
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

                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact]
        public void TestValidateDefaultParameters()
        {
            string connectionString = String.Format("scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
            "account={3};role={4};db={5};schema={6};warehouse={7};user={8};password={9};",
                    _fixture.testConfig.protocol,
                    _fixture.testConfig.host,
                    _fixture.testConfig.port,
                    _fixture.testConfig.account,
                    _fixture.testConfig.role,
                    _fixture.testConfig.database,
                    _fixture.testConfig.schema,
                    "WAREHOUSE_NEVER_EXISTS",
                    _fixture.testConfig.user,
                    _fixture.testConfig.password);

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
                    Assert.Equal(390201, e.ErrorCode);
                }
            }

            // This should succeed
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString + ";VALIDATE_DEFAULT_PARAMETERS=false";
                conn.Open();
            }
        }

        [Fact]
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
                        Assert.Equal(expectedErrorCode[i], e.ErrorCode);
                    }
                }
            }
        }

        [Fact]
        public void TestUnknownConnectionProperty()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // invalid propety will be ignored.
                conn.ConnectionString = _fixture.ConnectionString + ";invalidProperty=invalidvalue;";

                conn.Open();
                Assert.Equal(conn.State, ConnectionState.Open);
                conn.Close();
            }
        }

        [Fact]
        [IgnoreOnEnvIs("snowflake_cloud_env",
                       new string[] { "AZURE", "GCP" })]
        public void TestSwitchDb()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;

                Assert.Equal(conn.State, ConnectionState.Closed);

                conn.Open();

                Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
                Assert.Equal(conn.State, ConnectionState.Open);

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                    Assert.Equal("SNOWFLAKE_SAMPLE_DATA", conn.Database);
                }

                conn.ChangeDatabase(_fixture.testConfig.database);
                conn.Close();
            }

        }

        [Fact]
        public void TestConnectWithoutHost()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                string connStrFmt = "account={0};user={1};password={2};certRevocationCheckMode=enabled;";
                conn.ConnectionString = string.Format(connStrFmt, _fixture.testConfig.account,
                    _fixture.testConfig.user, _fixture.testConfig.password);
                // Check that connection succeeds if host is not specified in test configs, i.e. default should work.
                if (string.IsNullOrEmpty(_fixture.testConfig.host))
                {
                    conn.Open();
                    Assert.Equal(conn.State, ConnectionState.Open);
                    conn.Close();
                }
            }
        }

        [Fact]
        public void TestConnectWithDifferentRole()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                var host = _fixture.testConfig.host;
                if (string.IsNullOrEmpty(host))
                {
                    host = $"{_fixture.testConfig.account}.snowflakecomputing.com";
                }

                string connStrFmt = "scheme={0};host={1};port={2};certRevocationCheckMode=enabled;" +
                    "user={3};password={4};account={5};role=public;db=snowflake_sample_data;schema=information_schema;warehouse=WH_NOT_EXISTED;validate_default_parameters=false";

                conn.ConnectionString = string.Format(
                    connStrFmt,
                    _fixture.testConfig.protocol,
                    _fixture.testConfig.host,
                    _fixture.testConfig.port,
                    _fixture.testConfig.user,
                    _fixture.testConfig.password,
                    _fixture.testConfig.account
                    );
                conn.Open();
                Assert.Equal(conn.State, ConnectionState.Open);

                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "select current_role()";
                    Assert.NotEmpty(command.ExecuteScalar().ToString());

                    command.CommandText = "select current_database()";
                    Assert.Contains(command.ExecuteScalar().ToString(), new[] { "SNOWFLAKE_SAMPLE_DATA", "" });

                    command.CommandText = "select current_schema()";
                    Assert.Contains(command.ExecuteScalar().ToString(), new[] { "INFORMATION_SCHEMA", "" });

                    command.CommandText = "select current_warehouse()";
                    // Command will return empty string if the hardcoded warehouse does not exist.
                    Assert.Equal("", command.ExecuteScalar().ToString());
                }
                conn.Close();
            }
        }

        // Test that when a connection is disposed, a close would send out and unfinished transaction would be roll back.
        [Fact]
        public void TestConnectionDispose()
        {
            using (IDbConnection conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                _fixture.CreateOrReplaceTable(conn, _fixture.TableName, new[] { "c INT" });
                var t1 = conn.BeginTransaction();
                var t1c1 = conn.CreateCommand();
                t1c1.Transaction = t1;
                t1c1.CommandText = $"insert into {_fixture.TableName} values (1)";
                t1c1.ExecuteNonQuery();
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Previous connection would be disposed and
                // uncommitted txn would rollback at this point
                conn.ConnectionString = _fixture.ConnectionString;
                conn.Open();
                IDbCommand command = conn.CreateCommand();
                command.CommandText = $"SELECT * FROM {_fixture.TableName}";
                IDataReader reader = command.ExecuteReader();
                Assert.False(reader.Read());
            }
        }

        [Fact]
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
                    Assert.Fail("Authentication of {0} should fail");
                }
                catch (SnowflakeDbException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.UNKNOWN_AUTHENTICATOR);
                }

            }
        }

        [Fact(Skip = "This test requires manual setup and therefore cannot be run in CI")]
        public void TestOktaConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        _fixture.testConfig.oktaUrl,
                        _fixture.testConfig.oktaUser,
                        _fixture.testConfig.oktaPassword);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact]
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
                        = _fixture.ConnectionStringWithoutAuth
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
                    Assert.IsType<SnowflakeDbException>(e);
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.INTERNAL_ERROR);
                    Assert.True(e.Message.Contains(
                        $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                        $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                        "while trying to authenticate through Okta"));
                }
            }
        }

        [Fact(Skip = "This test requires manual setup and therefore cannot be run in CI")]
        public void TestOkta2ConnectionsFollowingEachOther()
        {
            // This test is here because Cookies were messing up with sequential Okta connections
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        _fixture.testConfig.oktaUrl,
                        _fixture.testConfig.oktaUser,
                        _fixture.testConfig.oktaPassword);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }


            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator={0};user={1};password={2};",
                        _fixture.testConfig.oktaUrl,
                        _fixture.testConfig.oktaUser,
                        _fixture.testConfig.oktaPassword);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUser()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);

                // connection pooling is disabled for external browser by default
                Assert.Equal(false, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.Equal("QA", command.ExecuteScalar().ToString());
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithPoolingEnabled()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;POOLINGENABLED=TRUE";
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
                Assert.Equal(true, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.Equal("QA", command.ExecuteScalar().ToString());
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUserAsync()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.Equal(ConnectionState.Open, conn.State);
                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Task<object> task = command.ExecuteScalarAsync(CancellationToken.None);
                    task.Wait(CancellationToken.None);
                    Assert.Equal("QA", task.Result);
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUserAndDisableConsoleLogin()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;disable_console_login=false;";
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
                using (IDbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.Equal("QA", command.ExecuteScalar().ToString());
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithUserAsyncAndDisableConsoleLogin()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;disable_console_login=false;";

                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.Equal(ConnectionState.Open, conn.State);
                using (DbCommand command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Task<object> task = command.ExecuteScalarAsync(CancellationToken.None);
                    task.Wait(CancellationToken.None);
                    Assert.Equal("QA", task.Result);
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
                            = _fixture.ConnectionStringWithoutAuth
                              + $";authenticator=externalbrowser;user=qa@snowflakecomputing.com;BROWSER_RESPONSE_TIMEOUT={waitSeconds}";
                        conn.Open();
                        Assert.Equal(ConnectionState.Open, conn.State);
                        using (IDbCommand command = conn.CreateCommand())
                        {
                            command.CommandText = "SELECT CURRENT_USER()";
                            Assert.Equal("QA", command.ExecuteScalar().ToString());
                        }
                    }
                }
            );
            stopwatch.Stop();

            // timeout after specified number of seconds
            Assert.True(stopwatch.ElapsedMilliseconds >= waitSeconds * 1000);
            // and not later than 5s after expected time
            Assert.True(stopwatch.ElapsedMilliseconds <= (waitSeconds + 5) * 1000);
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
                = _fixture.ConnectionStringWithoutAuth
                    + $";authenticator=externalbrowser;user={_fixture.testConfig.user};CLIENT_STORE_TEMPORARY_CREDENTIAL=true;poolingEnabled=false";

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate to retrieve and store the token if doesn't exist or invalid
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
                    = _fixture.ConnectionStringWithoutAuth
                        + $";authenticator=externalbrowser;user={_fixture.testConfig.user};CLIENT_STORE_TEMPORARY_CREDENTIAL=true;";

                // Create a credential manager and save a wrong token for the test user
                var key = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(_fixture.testConfig.host, _fixture.testConfig.user, TokenType.IdToken);
                var credentialManager = SFCredentialManagerInMemoryImpl.Instance;
                credentialManager.SaveCredentials(key, "wrongToken");

                // Use the credential manager with the wrong token
                SnowflakeCredentialManagerFactory.SetCredentialManager(credentialManager);

                // Open a connection which should switch to external browser after trying to connect using the wrong token
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);

                // Switch back to the default credential manager
                SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public void TestSSOConnectionWithWrongUser()
        {
            try
            {
                using (IDbConnection conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + ";authenticator=externalbrowser;user=wrong@snowflakecomputing.com";
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                Assert.Equal(390191, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtUnencryptedPemFileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.pemFilePath);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtUnencryptedP8FileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.p8FilePath);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkFileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1};private_key_pwd={2}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.pwdProtectedPrivateKeyFilePath,
                        _fixture.testConfig.privateKeyFilePwd);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtUnencryptedPkConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key={1}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.privateKey);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key={1};private_key_pwd={2}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.pwdProtectedPrivateKey,
                        _fixture.testConfig.privateKeyFilePwd);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtMissingConnectionSettingConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_pwd={1}",
                            _fixture.testConfig.jwtAuthUser,
                            _fixture.testConfig.privateKeyFilePwd);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Missing PRIVATE_KEY_FILE connection setting required for
                // authenticator =snowflake_jwt
                Assert.Equal(270008, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkFileInvalidPwdConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1};private_key_pwd=Invalid",
                            _fixture.testConfig.jwtAuthUser,
                            _fixture.testConfig.pwdProtectedPrivateKeyFilePath);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Invalid password for decrypting the private key
                Assert.Equal(270052, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkFileNoPwdConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                            _fixture.testConfig.jwtAuthUser,
                            _fixture.testConfig.pwdProtectedPrivateKeyFilePath);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Invalid password (none provided) for decrypting the private key
                Assert.Equal(270052, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtConnectionWithWrongUser()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                            "WrongUser",
                            _fixture.testConfig.pemFilePath);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Jwt token is invalid
                Assert.Equal(390144, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestJwtEncryptedPkConnectionWithWrongUser()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + String.Format(
                            ";authenticator=snowflake_jwt;user={0};private_key_file={1};private_key_pwd={2}",
                            "WrongUser",
                            _fixture.testConfig.pwdProtectedPrivateKeyFilePath,
                            _fixture.testConfig.privateKeyFilePwd);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Jwt token is invalid
                Assert.Equal(390144, e.ErrorCode);
            }
        }


        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestValidOAuthConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=oauth;token={0}",
                        _fixture.testConfig.oauthToken);
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact]
        public void TestInValidOAuthTokenConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + ";authenticator=oauth;token=notAValidOAuthToken";
                    conn.Open();
                    Assert.Equal(ConnectionState.Open, conn.State);
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                // Invalid OAuth access token
                Assert.Equal(390303, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestValidOAuthExpiredTokenConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                   = _fixture.ConnectionStringWithoutAuth
                   + String.Format(
                       ";authenticator=oauth;token={0}",
                       _fixture.testConfig.expOauthToken);
                    conn.Open();
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                Console.Write(e);
                // Token is expired
                Assert.Equal(390318, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestCorrectProxySettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1}",
                    _fixture.testConfig.proxyHost,
                    _fixture.testConfig.proxyPort);

                conn.Open();
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestCorrectProxyWithCredsSettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3}",
                    _fixture.testConfig.authProxyHost,
                    _fixture.testConfig.authProxyPort,
                    _fixture.testConfig.authProxyUser,
                    _fixture.testConfig.authProxyPwd);

                conn.Open();
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestCorrectProxySettingWithByPassListFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4}",
                    _fixture.testConfig.authProxyHost,
                    _fixture.testConfig.authProxyPort,
                    _fixture.testConfig.authProxyUser,
                    _fixture.testConfig.authProxyPwd,
                    "*.foo.com %7C" + _fixture.testConfig.host + "|localhost");

                conn.Open();
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void TestMultipleConnectionWithDifferentHttpHandlerSettings()
        {
            // Authenticated proxy
            using (var conn1 = new SnowflakeDbConnection())
            {
                conn1.ConnectionString = _fixture.ConnectionString
                    + String.Format(
                        ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3}",
                        _fixture.testConfig.authProxyHost,
                        _fixture.testConfig.authProxyPort,
                        _fixture.testConfig.authProxyUser,
                        _fixture.testConfig.authProxyPwd);
                conn1.Open();
            }

            // No proxy
            using (var conn2 = new SnowflakeDbConnection())
            {
                conn2.ConnectionString = _fixture.ConnectionString;
                conn2.Open();
            }

            // Non authenticated proxy
            using (var conn3 = new SnowflakeDbConnection())
            {
                conn3.ConnectionString = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1}",
                    _fixture.testConfig.proxyHost,
                    _fixture.testConfig.proxyPort);
                conn3.Open();
            }

            // Invalid proxy
            using (var conn4 = new SnowflakeDbConnection())
            {
                conn4.ConnectionString =
                    _fixture.ConnectionString + "connection_timeout=20;useProxy=true;proxyHost=Invalid;proxyPort=8080;";
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
                conn5.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString
                    + String.Format(
                        ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};",
                        _fixture.testConfig.authProxyHost,
                        _fixture.testConfig.authProxyPort,
                        _fixture.testConfig.authProxyUser,
                        _fixture.testConfig.authProxyPwd));
                conn5.Open();
            }

            // No proxy again, but crl check is disabled
            // Will use a different httpclient
            using (var conn6 = new SnowflakeDbConnection())
            {

                conn6.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString);
                conn6.Open();
            }

            // Another authenticated proxy, but this will create a new httpclient because there is
            // a bypass list
            using (var conn7 = new SnowflakeDbConnection())
            {
                conn7.ConnectionString
              = _fixture.ConnectionString
              + String.Format(
                  ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4}",
                  _fixture.testConfig.authProxyHost,
                  _fixture.testConfig.authProxyPort,
                  _fixture.testConfig.authProxyUser,
                  _fixture.testConfig.authProxyPwd,
                  "*.foo.com %7C" + _fixture.testConfig.host + "|localhost");

                conn7.Open();
            }

            // No proxy again, crl check is enabled in the default connection string for tests
            // Should use same httpclient than conn2
            using (var conn8 = new SnowflakeDbConnection())
            {
                conn8.ConnectionString = _fixture.ConnectionString;
                conn8.Open();
            }

            // Another authenticated proxy with bypasslist, but this will create a new httpclient because of
            // disabled certificate revocation check
            using (var conn9 = new SnowflakeDbConnection())
            {
                conn9.ConnectionString
              = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString)
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4};",
                    _fixture.testConfig.authProxyHost,
                    _fixture.testConfig.authProxyPort,
                    _fixture.testConfig.authProxyUser,
                    _fixture.testConfig.authProxyPwd,
                    "*.foo.com %7C" + _fixture.testConfig.host + "|localhost");

                conn9.Open();
            }

            // Another authenticated proxy with bypasslist
            // Should use same httpclient than conn7
            using (var conn10 = new SnowflakeDbConnection())
            {
                conn10.ConnectionString
              = _fixture.ConnectionString
              + String.Format(
                  ";useProxy=true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};nonProxyHosts={4}",
                  _fixture.testConfig.authProxyHost,
                  _fixture.testConfig.authProxyPort,
                  _fixture.testConfig.authProxyUser,
                  _fixture.testConfig.authProxyPwd,
                  "*.foo.com %7C" + _fixture.testConfig.host + "|localhost");

                conn10.Open();
            }

            // No proxy, but crl check disabled
            // Should use same httpclient than conn6
            using (var conn11 = new SnowflakeDbConnection())
            {
                conn11.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString);
                conn11.Open();
            }
        }

        [Fact]
        public void TestInvalidProxySettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {

                conn.ConnectionString =
                    _fixture.ConnectionString + "connection_timeout=5;useProxy=true;proxyHost=Invalid;proxyPort=8080";
                try
                {
                    conn.Open();
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    // Expected
                    s_logger.Debug("Failed opening connection ", e);
                    Assert.Equal(270001, e.ErrorCode); //Internal error
                    AssertIsConnectionFailure(e);
                }
            }
        }

        [Theory]
        [InlineData("*")]
        [InlineData("*{0}*")]
        [InlineData("^*{0}*")]
        [InlineData("*{0}*$")]
        [InlineData("^*{0}*$")]
        [InlineData("^nonmatch*{0}$|*")]
        [InlineData("*a*")]
        [InlineData("*la*", "la")]
        public void TestNonProxyHostShouldBypassProxyServer(string regexHost, string proxyHost = "proxyserverhost")
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                var host = _fixture.ResolveHost();
                var nonProxyHosts = string.Format(regexHost, $"{host}");
                conn.ConnectionString =
                    $"{_fixture.ConnectionString}USEPROXY=true;PROXYHOST={proxyHost};NONPROXYHOSTS={nonProxyHosts};PROXYPORT=3128;";

                // Act
                conn.Open();

                // Assert
                // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Theory]
        [InlineData("invalid{0}")]
        [InlineData("*invalid{0}*")]
        [InlineData("^invalid{0}$")]
        [InlineData("*a.b")]
        [InlineData("a")]
        [InlineData("la", "la")]
        public void TestNonProxyHostShouldNotBypassProxyServer(string regexHost, string proxyHost = "proxyserverhost")
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                var nonProxyHosts = string.Format(regexHost, $"{_fixture.testConfig.host}");
                conn.ConnectionString =
                    $"{_fixture.ConnectionString}connection_timeout=5;USEPROXY=true;PROXYHOST={proxyHost};NONPROXYHOSTS={nonProxyHosts};PROXYPORT=3128;";

                // Act/Assert
                // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
                var exception = Assert.Throws<SnowflakeDbException>(() => conn.Open());

                // Assert
                Assert.Equal(270001, exception.ErrorCode);
                AssertIsConnectionFailure(exception);
            }
        }

        [Fact]
        public void TestUseProxyFalseWithInvalidProxyConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString =
                    _fixture.ConnectionString + ";useProxy=false;proxyHost=Invalid;proxyPort=8080";
                conn.Open();
                // Because useProxy=false, the proxy settings are ignored
            }
        }

        [Fact]
        public void TestInvalidProxySettingWithByPassListFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost=Invalid;proxyPort=8080;nonProxyHosts={0}",
                    $"*.foo.com %7C{_fixture.testConfig.account}.snowflakecomputing.com|*{_fixture.testConfig.host}*");
                conn.Open();
                // Because _fixture.testConfig.host is in the bypass list, the proxy should not be used
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public void testMulitpleConnectionInParallel()
        {
            string baseConnectionString = _fixture.ConnectionString + $";CONNECTION_TIMEOUT=30;";
            string authenticatedProxy = String.Format("useProxy =true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};",
                  _fixture.testConfig.authProxyHost,
                  _fixture.testConfig.authProxyPort,
                  _fixture.testConfig.authProxyUser,
                  _fixture.testConfig.authProxyPwd);
            string byPassList = "nonProxyHosts=*.foo.com %7C" + _fixture.testConfig.host + "|localhost;";

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

        [Fact(Skip = "Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
        public void TestEscapeChar()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;key1=test\'password;key2=test\"password;key3=test==password";
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);

                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.Equal("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                if (!string.IsNullOrEmpty(serverVersion))
                {
                    string[] versionElements = serverVersion.Split('.');
                    Assert.Equal(3, versionElements.Length);
                }

                conn.Close();
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
        public void TestEscapeChar1()
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;key==word=value; key1=\"test;password\"; key2=\"test=password\"";
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);

                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
                // Data source is empty string for now
                Assert.Equal("", ((SnowflakeDbConnection)conn).DataSource);

                string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
                if (!string.IsNullOrEmpty(serverVersion))
                {
                    string[] versionElements = serverVersion.Split('.');
                    Assert.Equal(3, versionElements.Length);
                }

                conn.Close();
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public void TestHeartBeat()
        {
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;CLIENT_SESSION_KEEP_ALIVE=true";
            conn.Open();

            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs
            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.Equal(command.ExecuteScalar(), 46);
            }

            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);
        }

        [Fact(Skip = "Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public void TestHeartBeatWithConnectionPool()
        {
            SnowflakeDbConnectionPool.ClearAllPools();

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=0;expirationTimeout=14800;CLIENT_SESSION_KEEP_ALIVE=true";
            conn.Open();
            conn.Close();

            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
            conn1.Open();
            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.Equal(command.ExecuteScalar(), 46);
            }

            conn1.Close();
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [Fact]
        public async Task TestKeepAlive()
        {
            // create 100 connections, one per second
            var connCount = 100;
            // pooled connection expires in 5 seconds so after 5 seconds,
            // one connection per second will be closed
            var connectionString = _fixture.ConnectionString + "maxPoolSize=20;ExpirationTimeout=5;CLIENT_SESSION_KEEP_ALIVE=true";
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
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    // roughly should only have 5 sessions in pool stay alive
                    // check for 10 in case of bad timing, also much less than the
                    // pool max size to ensure it's unpooled because of expire
                    Assert.True(HeartBeatBackground.getQueueLength() < 10);
                }
            }
            catch
            {
                // fail the test case if any exception is thrown
                Assert.Fail();
            }
        }
    }
    class SFConnectionITAsync : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFConnectionITAsync(SFBaseTestAsyncFixture fixture, IntegrationTestFixture envFixture) : base(fixture, envFixture) { _fixture = fixture; }

        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionITAsync>();


        [Fact]
        public void TestCancelLoginBeforeTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                // No timeout
                int timeoutSec = 0;
                string infiniteLoginTimeOut = String.Format(_fixture.ConnectionString + ";connection_timeout={0};maxHttpRetries=0",
                    timeoutSec);

                conn.ConnectionString = infiniteLoginTimeOut;

                Assert.Equal(conn.State, ConnectionState.Closed);

                CancellationTokenSource connectionCancelToken = new CancellationTokenSource();
                Task connectTask = conn.OpenAsync(connectionCancelToken.Token);

                Assert.Equal(ConnectionState.Connecting, conn.State);

                logger.Debug("connectionCancelToken.Cancel ");
                connectionCancelToken.Cancel();

                try
                {
                    connectTask.Wait();
                }
                catch (AggregateException e)
                {
                    Assert.Equal(
                        "System.Threading.Tasks.TaskCanceledException",
                        e.InnerException.GetType().ToString());

                }

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(timeoutSec, conn.ConnectionTimeout);
            }
        }

        [Fact]
        public void TestAsyncLoginTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                int timeoutSec = 5;
                string loginTimeOut5sec = String.Format(_fixture.ConnectionString + "connection_timeout={0};maxHttpRetries=0",
                    timeoutSec);
                conn.ConnectionString = loginTimeOut5sec;

                Assert.Equal(conn.State, ConnectionState.Closed);

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
                Assert.True(stopwatch.ElapsedMilliseconds >= timeoutSec * 1000 - delta);
                // But never more than 3 sec (buffer time) after the defined timeout
                Assert.True(stopwatch.ElapsedMilliseconds <= (timeoutSec + 3) * 1000);

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(timeoutSec, conn.ConnectionTimeout);
            }
        }

        [Fact]
        public void TestAsyncLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                int connectionTimeout = 600;
                int retryTimeout = 350;
                string loginTimeOut5sec = String.Format(_fixture.ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
                    connectionTimeout, retryTimeout);
                conn.ConnectionString = loginTimeOut5sec;

                Assert.Equal(conn.State, ConnectionState.Closed);

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
                Assert.True(stopwatch.ElapsedMilliseconds >= retryTimeout * 1000 - delta);
                // But never more than 2 sec (buffer time) after the defined timeout
                Assert.True(stopwatch.ElapsedMilliseconds <= (retryTimeout + 2) * 1000);

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(retryTimeout, conn.ConnectionTimeout);
            }
        }

        [Fact]
        public void TestAsyncDefaultLoginTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                // unlimited retry count to trigger the timeout
                conn.ConnectionString = _fixture.ConnectionString + "maxHttpRetries=0";

                Assert.Equal(conn.State, ConnectionState.Closed);

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
                Assert.True(stopwatch.ElapsedMilliseconds >= conn.ConnectionTimeout * 1000 - delta);
                // But never more because there's no connection timeout remaining (with 2 seconds margin)
                Assert.True(stopwatch.ElapsedMilliseconds <= (conn.ConnectionTimeout + 2) * 1000);

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
            }
        }

        [Fact]
        public void TestAsyncConnectionFailFastForNonRetried404OnLogin()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Just a way to get a 404 on the login request and make sure there are no retry
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;certRevocationCheckMode=enabled;";

                conn.ConnectionString = invalidConnectionString;

                Assert.Equal(conn.State, ConnectionState.Closed);
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

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.True(connectTask.IsFaulted);
            }
        }

        [Fact]
        public void TestCloseAsyncWithCancellation()
        {
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection.close
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection.closeasync
            // An application can call Close or CloseAsync more than one time.
            // No exception is generated.
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                Assert.Equal(conn.State, ConnectionState.Closed);
                Task task = null;

                // Close the connection. It's not opened yet, but it should not have any issue
                task = conn.CloseAsync(CancellationToken.None);
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Closed);

                // Open the connection
                task = conn.OpenAsync(CancellationToken.None);
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync(CancellationToken.None);
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Closed);

                // Close the connection again.
                task = conn.CloseAsync(CancellationToken.None);
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Closed);
            }
        }

#if NETCOREAPP3_0_OR_GREATER
        [Fact]
        public void TestCloseAsync()
        {
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection.close
            // https://docs.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection.closeasync
            // An application can call Close or CloseAsync more than one time.
            // No exception is generated.
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                Assert.Equal(conn.State, ConnectionState.Closed);
                Task task = null;

                // Close the connection. It's not opened yet, but it should not have any issue
                task = conn.CloseAsync();
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Closed);

                // Open the connection
                task = conn.OpenAsync();
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync();
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Closed);

                // Close the connection again.
                task = conn.CloseAsync();
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Closed);
            }
        }
#endif

        [Fact]
        public void TestCloseAsyncFailure()
        {
            using (var conn = new MockSnowflakeDbConnection(new MockCloseSessionException()))
            {
                conn.ConnectionString = _fixture.ConnectionString;
                Assert.Equal(conn.State, ConnectionState.Closed);
                Task task = null;

                // Open the connection
                task = conn.OpenAsync(CancellationToken.None);
                task.Wait();
                Assert.Equal(conn.State, ConnectionState.Open);

                // Close the opened connection
                task = conn.CloseAsync(CancellationToken.None);
                try
                {
                    task.Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    Assert.Equal(MockCloseSessionException.SESSION_CLOSE_ERROR,
                        ((SnowflakeDbException)(e.InnerException).InnerException).ErrorCode);
                }
                Assert.Equal(conn.State, ConnectionState.Open);
            }
        }

        [Fact]
        public void TestExplicitTransactionOperationsTracked()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                conn.Open();
                Assert.Equal(false, conn.HasActiveExplicitTransaction());

                var trans = conn.BeginTransaction();
                Assert.Equal(true, conn.HasActiveExplicitTransaction());
                trans.Rollback();
                Assert.Equal(false, conn.HasActiveExplicitTransaction());

                conn.BeginTransaction().Rollback();
                Assert.Equal(false, conn.HasActiveExplicitTransaction());

                conn.BeginTransaction().Commit();
                Assert.Equal(false, conn.HasActiveExplicitTransaction());
            }
        }


        [Fact]
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
                        = _fixture.ConnectionStringWithoutAuth
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
                    Assert.IsType<SnowflakeDbException>(e.InnerException);
                    SnowflakeDbExceptionAssert.HasErrorCode(e.InnerException, SFError.INTERNAL_ERROR);
                    Exception oktaException;
#if NETFRAMEWORK
                    oktaException = e.InnerException.InnerException.InnerException;
#else
                    oktaException = e.InnerException.InnerException;
#endif
                    Assert.True(oktaException.Message.Contains(
                        $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                        $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                        "while trying to authenticate through Okta"));
                }
            }
        }

        [Fact(Skip = "This test requires established dev Okta SSO and credentials matching Snowflake user")]
        public void TestNativeOktaSuccess()
        {
            var oktaUrl = "https://***.okta.com/";
            var oktaUser = "***";
            var oktaPassword = "***";
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionStringWithoutAuth +
                                        $";authenticator={oktaUrl};user={oktaUser};password={oktaPassword};";
                conn.Open();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact]
        public void TestConnectStringWithQueryTag()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string expectedQueryTag = "Test QUERY_TAG 12345";
                conn.ConnectionString = _fixture.ConnectionString + $";query_tag={expectedQueryTag}";

                conn.Open();
                var command = conn.CreateCommand();
                // This query itself will be part of the history and will have the query tag
                command.CommandText = "SELECT QUERY_TAG FROM table(information_schema.query_history_by_session())";
                var queryTag = command.ExecuteScalar();

                Assert.Equal(expectedQueryTag, queryTag);
            }
        }

        [Fact]
        public void TestUseMultiplePoolsConnectionPoolByDefault()
        {
            // act
            var poolVersion = SnowflakeDbConnectionPool.GetConnectionPoolVersion();

            // assert
            Assert.Equal(ConnectionPoolType.MultipleConnectionPool, poolVersion);
        }

        [Fact]
        // to enroll to mfa authentication edit your user profile
        public void TestMFATokenCachingWithPasscodeFromConnectionString()
        {
            // Use a connection with MFA enabled and set passcode property for mfa authentication. e.g. _fixture.ConnectionString + ";authenticator=username_password_mfa;passcode=(set proper passcode)"
            // ACCOUNT PARAMETER ALLOW_CLIENT_MFA_CACHING should be set to true in the account.
            // On Mac/Linux OS the default credential manager is a file based one. Uncomment the following line to test in memory implementation.
            // SnowflakeCredentialManagerFactory.UseInMemoryCredentialManager();
            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionString
                      + ";authenticator=username_password_mfa;application=DuoTest;minPoolSize=0;passcode=(set proper passcode)";


                // Authenticate to retrieve and store the token if doesn't exist or invalid
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact]
        // to enroll to mfa authentication edit your user profile
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
                conn.ConnectionString = _fixture.ConnectionString + "minPoolSize=2;application=DuoTest;";

                // act
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();

                // assert
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Theory]
        [InlineData("connection_timeout=5;")]
        [InlineData("")]
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
                Assert.True(thrown.InnerException is TaskCanceledException || thrown.InnerException is SnowflakeDbException);
                if (thrown.InnerException is SnowflakeDbException)
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown.InnerException, SFError.INTERNAL_ERROR);
                Assert.Equal(ConnectionState.Closed, connection.State);
            }
        }

        [Fact]
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
                Assert.IsType<TaskCanceledException>(thrown.InnerException);
                Assert.Equal(ConnectionState.Closed, connection.State);
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
                = _fixture.ConnectionStringWithoutAuth
                    + $";authenticator=externalbrowser;user={_fixture.testConfig.user};CLIENT_STORE_TEMPORARY_CREDENTIAL=true;poolingEnabled=false";

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate to retrieve and store the token if doesn't exist or invalid
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.Equal(ConnectionState.Open, conn.State);
            }

            using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
                Task connectTask = conn.OpenAsync(CancellationToken.None);
                connectTask.Wait();
                Assert.Equal(ConnectionState.Open, conn.State);
            }

        }

        [Fact]
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
            Assert.Null(session.sessionToken);
        }

        private SFSession GetSessionFromForgottenConnection()
        {
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString + ";poolingEnabled=false;application=TestGarbageCollectorCloseSession");
            connection.Open();
            return connection.SfSession;
        }

        [Fact]
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
            Assert.Equal(1, restRequester.CloseRequests.Count);
            Assert.True(watchClose.Elapsed.Duration() < TimeSpan.FromSeconds(5)); // close executed immediately
            Assert.True(watchClosedFinished.Elapsed.Duration() >= TimeSpan.FromSeconds(10)); // while background task took more time
        }

        [Fact(Skip = "Manual test only")]
        public void TestOAuthFlow()
        {
            // arrange
            var driverRootPath = Path.Combine("..", "..");
            var configFilePath = Path.Combine(driverRootPath, "..", ".parameters_oauth_authorization_code_okta.json"); // Adjust to a proper config for your manual testing
            var authenticator = OAuthAuthorizationCodeAuthenticator.AuthName; // Set either OAuthAuthorizationCodeAuthenticator.AuthName or OAuthClientCredentialsAuthenticator.AuthName
            RemoveOAuthCache(_fixture.testConfig);
            try
            {
                using (var connection = new SnowflakeDbConnection(ConnectionStringForOAuthFlows(_fixture.testConfig, authenticator)))
                {
                    // act
                    connection.Open();
                }
            }
            finally
            {
                RemoveOAuthCache(_fixture.testConfig);
            }
        }

        [Fact(Skip = "Manual test only")]
        public void TestProgrammaticAccessTokenAuthentication()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionStringForPat(_fixture.testConfig)))
            {
                // act
                connection.Open();
            }
        }

        private void RemoveOAuthCache(TestConfig testConfig)
        {
            var host = new Uri(_fixture.testConfig.oauthTokenRequestUrl).Host;
            var accessCacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, _fixture.testConfig.user, TokenType.OAuthAccessToken);
            var refreshCacheKey = SnowflakeCredentialManagerFactory.GetSecureCredentialKey(host, _fixture.testConfig.user, TokenType.OAuthRefreshToken);
            var credentialManager = SnowflakeCredentialManagerFactory.GetCredentialManager();
            credentialManager.RemoveCredentials(accessCacheKey);
            credentialManager.RemoveCredentials(refreshCacheKey);
        }

        private string ConnectionStringForOAuthFlows(TestConfig testConfig, string authenticator)
        {
            var builder = new StringBuilder()
                .Append($"authenticator={authenticator};user={_fixture.testConfig.user};password={_fixture.testConfig.password};account={_fixture.testConfig.account};certRevocationCheckMode=enabled;")
                .Append($"db={_fixture.testConfig.database};role={_fixture.testConfig.role};warehouse={_fixture.testConfig.warehouse};host={_fixture.testConfig.host};port={_fixture.testConfig.port};")
                .Append($"oauthClientId={_fixture.testConfig.oauthClientId};oauthClientSecret={_fixture.testConfig.oauthClientSecret};oauthScope={_fixture.testConfig.oauthScope};")
                .Append($"oauthTokenRequestUrl={_fixture.testConfig.oauthTokenRequestUrl};")
                .Append("poolingEnabled=false;");
            switch (authenticator)
            {
                case OAuthAuthorizationCodeAuthenticator.AuthName:
                    return builder
                        .Append($"oauthRedirectUri={_fixture.testConfig.oauthRedirectUri};")
                        .Append($"oauthAuthorizationUrl={_fixture.testConfig.oauthAuthorizationUrl}")
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
                .Append($"authenticator=programmatic_access_token;user={_fixture.testConfig.user};account={_fixture.testConfig.account};certRevocationCheckMode=enabled;")
                .Append($"db={_fixture.testConfig.database};role={role};warehouse={_fixture.testConfig.warehouse};host={_fixture.testConfig.host};port={_fixture.testConfig.port};")
                .Append($"token={_fixture.testConfig.programmaticAccessToken};")
                .Append("poolingEnabled=false;")
                .ToString();
        }
    }
}


