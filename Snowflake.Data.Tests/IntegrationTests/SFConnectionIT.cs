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
    public class SFConnectionIT : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFConnectionIT(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFConnectionIT>();

        [SFFact]
        public async Task TestBasicConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
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

                await conn.CloseAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [SFFact]
        public async Task TestApplicationName()
        {
            string[] validApplicationNames = { "test1234", "test_1234", "test-1234", "test.1234" };
            string[] invalidApplicationNames = { "1234test", "test$A", "test<script>" };

            // Valid names
            foreach (string appName in validApplicationNames)
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString;
                    conn.ConnectionString += $"application={appName}";
                    await conn.OpenAsync(CancellationToken.None);
                    Assert.Equal(ConnectionState.Open, conn.State);

                    await conn.CloseAsync(CancellationToken.None);
                    Assert.Equal(ConnectionState.Closed, conn.State);
                }
            }

            // Invalid names
            foreach (string appName in invalidApplicationNames)
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString = _fixture.ConnectionString;
                    conn.ConnectionString += $"application={appName}";
                    try
                    {
                        await conn.OpenAsync(CancellationToken.None);
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

        [SFFact(SkipCondition.RunOnlyOnCI)]
        public async Task TestApplicationPathIsSentDuringAuthentication()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);

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

        [SFFact]
        public async Task TestIncorrectUserOrPasswordBasicConnection()
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
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestConnectionIsNotMarkedAsOpenWhenWasNotCorrectlyOpenedBefore(bool explicitClose)
        {
            for (int i = 0; i < 2; ++i)
            {
                s_logger.Debug($"Running try #{i}");
                SnowflakeDbConnection snowflakeConnection = null;
                try
                {
                    snowflakeConnection = new SnowflakeDbConnection(_fixture.ConnectionStringWithInvalidUserName);
                    await snowflakeConnection.OpenAsync(CancellationToken.None);
                    Assert.Fail("Connection open should fail");
                }
                catch (SnowflakeDbException e)
                {
                    AssertIsConnectionFailure(e);
                    AssertConnectionIsNotOpen(snowflakeConnection);
                    if (explicitClose)
                    {
                        await snowflakeConnection.CloseAsync(CancellationToken.None);
                        AssertConnectionIsNotOpen(snowflakeConnection);
                    }
                }
            }
        }

        [SFFact]
        public async Task TestConnectionIsNotMarkedAsOpenWhenWasNotCorrectlyOpenedWithUsingClause()
        {
            for (int i = 0; i < 2; ++i)
            {
                s_logger.Debug($"Running try #{i}");
                SnowflakeDbConnection snowflakeConnection = null;
                try
                {
                    using (snowflakeConnection = new SnowflakeDbConnection(_fixture.ConnectionStringWithInvalidUserName))
                    {
                        await snowflakeConnection.OpenAsync(CancellationToken.None);
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

        [SFFact]
        public async Task TestConnectString()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            var schemaName = "dlSchema_" + Guid.NewGuid().ToString().Replace("-", "_");
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString;
            await conn.OpenAsync(CancellationToken.None);
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
                cmd.CommandText = $"create table {tableName} (col1 string, col2 int)";
                cmd.ExecuteNonQuery();
                //cmd.CommandText = "insert into \"dlTest\".\"dlSchema\".test1 Values ('test 1', 1);";
                cmd.CommandText = $"insert into {tableName} Values ('test 1', 1);";
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

                await conn1.OpenAsync(CancellationToken.None);
                using (IDbCommand cmd = conn1.CreateCommand())
                {
                    cmd.CommandText = $"SELECT count(*) FROM {tableName}";
                    IDataReader reader = cmd.ExecuteReader();
                    Assert.True(reader.Read());
                    Assert.Equal(1, reader.GetInt32(0));
                }
                await conn1.CloseAsync(CancellationToken.None);

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
            await conn.CloseAsync(CancellationToken.None);
        }

        [Fact(Skip = "TestConnectStringWithUserPwd, this will popup an internet browser for external login.")]
        public async Task TestConnectStringWithUserPwd()
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
                await conn.OpenAsync(CancellationToken.None);
                await conn.CloseAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [SFFact]
        public async Task TestConnectViaSecureString()
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
                await conn.OpenAsync(CancellationToken.None);

                Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
                Assert.Equal(conn.State, ConnectionState.Open);

                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestLoginTimeout()
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
                    await conn.OpenAsync(CancellationToken.None);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    Assert.True(SFError.REQUEST_TIMEOUT.GetAttribute<SFErrorAttr>().errorCode == e.ErrorCode);
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

        [SFFact]
        public async Task TestLoginWithMaxRetryReached()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                string maxRetryConnStr = _fixture.ConnectionString + "maxHttpRetries=7";

                conn.ConnectionString = maxRetryConnStr;

                Assert.Equal(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact]
        public async Task TestLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
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
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact]
        public async Task TestDefaultLoginTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;

                // Default timeout is 300 sec
                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);

                Assert.Equal(conn.State, ConnectionState.Closed);
                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact]
        public async Task TestConnectionFailFastForNonRetried404OnLogin()
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
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact]
        public async Task TestEnableLoginRetryOn404()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string invalidConnectionString = "host=google.com/404;"
                    + "connection_timeout=0;account=testFailFast;user=testFailFast;password=testFailFast;disableretry=true;forceretryon404=true;certRevocationCheckMode=enabled;";
                conn.ConnectionString = invalidConnectionString;

                Assert.Equal(conn.State, ConnectionState.Closed);
                try
                {
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFFact]
        public async Task TestValidateDefaultParameters()
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
            using (var conn = new SnowflakeDbConnection())
            {
                try
                {
                    conn.ConnectionString = connectionString;
                    await conn.OpenAsync(CancellationToken.None);
                    Assert.Fail();
                }
                catch (SnowflakeDbException e)
                {
                    var aggregateEx = (AggregateException)((AggregateException)e.InnerException).InnerExceptions[0];
                    Assert.Equal(390201, ((SnowflakeDbException)aggregateEx.InnerExceptions[0]).ErrorCode);
                }
            }

            // This should succeed
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = connectionString + ";VALIDATE_DEFAULT_PARAMETERS=false";
                await conn.OpenAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestInvalidConnectionString()
        {
            string[] invalidStrings = {
                // missing required connection property password
                "ACCOUNT=testaccount;user=testuser",
                // invalid account value
                "ACCOUNT=A=C;USER=testuser;password=123;key",
                "complete_invalid_string",
            };

            int[] expectedErrorCode = { 270006, 270008, 270008 };

            using (var conn = new SnowflakeDbConnection())
            {
                for (int i = 0; i < invalidStrings.Length; i++)
                {
                    try
                    {
                        conn.ConnectionString = invalidStrings[i];
                        await conn.OpenAsync(CancellationToken.None);
                        Assert.Fail();
                    }
                    catch (SnowflakeDbException e)
                    {
                        Assert.Equal(expectedErrorCode[i], e.ErrorCode);
                    }
                }
            }
        }

        [SFFact]
        public async Task TestUnknownConnectionProperty()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // invalid propety will be ignored.
                conn.ConnectionString = _fixture.ConnectionString + ";invalidProperty=invalidvalue;";

                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(conn.State, ConnectionState.Open);
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        [SFFact(SkipCondition.SkipOnCloudAzure | SkipCondition.SkipOnCloudGCP)]
        public async Task TestSwitchDb()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString;

                Assert.Equal(conn.State, ConnectionState.Closed);

                await conn.OpenAsync(CancellationToken.None);

                Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
                Assert.Equal(conn.State, ConnectionState.Open);

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                    Assert.Equal("SNOWFLAKE_SAMPLE_DATA", conn.Database);
                }

                conn.ChangeDatabase(_fixture.testConfig.database);
                await conn.CloseAsync(CancellationToken.None);
            }

        }

        [SFFact]
        public async Task TestConnectWithoutHost()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string connStrFmt = "account={0};user={1};password={2};certRevocationCheckMode=enabled;";
                conn.ConnectionString = string.Format(connStrFmt, _fixture.testConfig.account,
                    _fixture.testConfig.user, _fixture.testConfig.password);
                // Check that connection succeeds if host is not specified in test configs, i.e. default should work.
                if (string.IsNullOrEmpty(_fixture.testConfig.host))
                {
                    await conn.OpenAsync(CancellationToken.None);
                    Assert.Equal(conn.State, ConnectionState.Open);
                    await conn.CloseAsync(CancellationToken.None);
                }
            }
        }

        [SFFact]
        public async Task TestConnectWithDifferentRole()
        {
            using (var conn = new SnowflakeDbConnection())
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
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(conn.State, ConnectionState.Open);

                using (var command = conn.CreateCommand())
                {
                    command.CommandText = "select current_role()";
                    Assert.NotEmpty((await command.ExecuteScalarAsync()).ToString());

                    command.CommandText = "select current_database()";
                    Assert.Contains((await command.ExecuteScalarAsync()).ToString(), new[] { "SNOWFLAKE_SAMPLE_DATA", "" });

                    command.CommandText = "select current_schema()";
                    Assert.Contains((await command.ExecuteScalarAsync()).ToString(), new[] { "INFORMATION_SCHEMA", "" });

                    command.CommandText = "select current_warehouse()";
                    // Command will return empty string if the hardcoded warehouse does not exist.
                    Assert.Equal("", (await command.ExecuteScalarAsync()).ToString());
                }
                await conn.CloseAsync(CancellationToken.None);
            }
        }

        // Test that when a connection is disposed, a close would send out and unfinished transaction would be roll back.
        [SFFact]
        public async Task TestConnectionDispose()
        {
            var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync(CancellationToken.None);
                _fixture.CreateOrReplaceTable(conn, tableName, new[] { "c INT" });
                var t1 = await conn.BeginTransactionAsync();
                var t1c1 = conn.CreateCommand();
                t1c1.Transaction = t1;
                t1c1.CommandText = $"insert into {tableName} values (1)";
                await t1c1.ExecuteNonQueryAsync();
            }

            using (var conn = new SnowflakeDbConnection())
            {
                // Previous connection would be disposed and
                // uncommitted txn would rollback at this point
                conn.ConnectionString = _fixture.ConnectionString;
                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                command.CommandText = $"SELECT * FROM {tableName}";
                IDataReader reader = await command.ExecuteReaderAsync();
                Assert.False(reader.Read());
            }
        }

        [SFFact]
        public async Task TestUnknownAuthenticator()
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
                    var conn = new SnowflakeDbConnection();
                    conn.ConnectionString = "scheme=http;host=test;port=8080;user=test;password=test;account=test;authenticator=" + wrongAuthenticator;
                    await conn.OpenAsync(CancellationToken.None);
                    Assert.Fail("Authentication of {0} should fail");
                }
                catch (SnowflakeDbException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.UNKNOWN_AUTHENTICATOR);
                }

            }
        }

        [Fact(Skip = "This test requires manual setup and therefore cannot be run in CI")]
        public async Task TestOktaConnection()
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
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [SFFact]
        public async Task TestOktaConnectionUntilMaxTimeout()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestOkta2ConnectionsFollowingEachOther()
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
                await conn.OpenAsync(CancellationToken.None);
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
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public async Task TestSSOConnectionWithUser()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);

                // connection pooling is disabled for external browser by default
                Assert.Equal(false, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
                using (var command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.Equal("QA", (await command.ExecuteScalarAsync()).ToString());
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public async Task TestSSOConnectionWithPoolingEnabled()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;POOLINGENABLED=TRUE";
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
                Assert.Equal(true, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
                using (var command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.Equal("QA", (await command.ExecuteScalarAsync()).ToString());
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public async Task TestSSOConnectionWithUserAsync()
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
        public async Task TestSSOConnectionWithUserAndDisableConsoleLogin()
        {
            // Use external browser to log in using proper password for qa@snowflakecomputing.com
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;disable_console_login=false;";
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
                using (var command = conn.CreateCommand())
                {
                    command.CommandText = "SELECT CURRENT_USER()";
                    Assert.Equal("QA", (await command.ExecuteScalarAsync()).ToString());
                }
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public async Task TestSSOConnectionWithUserAsyncAndDisableConsoleLogin()
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
        public async Task TestSSOConnectionTimeoutAfter10s()
        {
            // Do not log in by external browser - timeout after 10s should happen
            int waitSeconds = 10;
            Stopwatch stopwatch = Stopwatch.StartNew();
            await Assert.ThrowsAsync<SnowflakeDbException>(async () =>
                {
                    using (var conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString
                            = _fixture.ConnectionStringWithoutAuth
                              + $";authenticator=externalbrowser;user=qa@snowflakecomputing.com;BROWSER_RESPONSE_TIMEOUT={waitSeconds}";
                        await conn.OpenAsync(CancellationToken.None);
                        Assert.Equal(ConnectionState.Open, conn.State);
                        using (var command = conn.CreateCommand())
                        {
                            command.CommandText = "SELECT CURRENT_USER()";
                            Assert.Equal("QA", (await command.ExecuteScalarAsync()).ToString());
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
        public async Task TestSSOConnectionWithTokenCaching()
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

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate to retrieve and store the token if doesn't exist or invalid
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }

            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = externalBrowserConnectionString;

                // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public async Task TestSSOConnectionWithInvalidCachedToken()
        {
            /*
             * This test checks that the connector will attempt to re-authenticate using external browser if the token retrieved from the cache is invalid
             * 1. Create a credential manager and save credentials for the user with a wrong token
             * 2. Open a connection which initially should try to use the token and then switch to external browser when the token fails
            */

            using (var conn = new SnowflakeDbConnection())
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
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);

                // Switch back to the default credential manager
                SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
            }
        }

        [Fact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
        public async Task TestSSOConnectionWithWrongUser()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + ";authenticator=externalbrowser;user=wrong@snowflakecomputing.com";
                    await conn.OpenAsync(CancellationToken.None);
                    Assert.Fail();
                }
            }
            catch (SnowflakeDbException e)
            {
                Assert.Equal(390191, e.ErrorCode);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestJwtUnencryptedPemFileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.pemFilePath);
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestJwtUnencryptedP8FileConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key_file={1}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.p8FilePath);
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestJwtEncryptedPkFileConnection()
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
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestJwtUnencryptedPkConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=snowflake_jwt;user={0};private_key={1}",
                        _fixture.testConfig.jwtAuthUser,
                        _fixture.testConfig.privateKey);
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestJwtEncryptedPkConnection()
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
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestJwtMissingConnectionSettingConnection()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestJwtEncryptedPkFileInvalidPwdConnection()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestJwtEncryptedPkFileNoPwdConnection()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestJwtConnectionWithWrongUser()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestJwtEncryptedPkConnectionWithWrongUser()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestValidOAuthConnection()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                    + String.Format(
                        ";authenticator=oauth;token={0}",
                        _fixture.testConfig.oauthToken);
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [SFFact]
        public async Task TestInValidOAuthTokenConnection()
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    conn.ConnectionString
                        = _fixture.ConnectionStringWithoutAuth
                        + ";authenticator=oauth;token=notAValidOAuthToken";
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestValidOAuthExpiredTokenConnection()
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
                    await conn.OpenAsync(CancellationToken.None);
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
        public async Task TestCorrectProxySettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1}",
                    _fixture.testConfig.proxyHost,
                    _fixture.testConfig.proxyPort);

                await conn.OpenAsync(CancellationToken.None);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestCorrectProxyWithCredsSettingFromConnectionString()
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

                await conn.OpenAsync(CancellationToken.None);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestCorrectProxySettingWithByPassListFromConnectionString()
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

                await conn.OpenAsync(CancellationToken.None);
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task TestMultipleConnectionWithDifferentHttpHandlerSettings()
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
                await conn1.OpenAsync(CancellationToken.None);
            }

            // No proxy
            using (var conn2 = new SnowflakeDbConnection())
            {
                conn2.ConnectionString = _fixture.ConnectionString;
                await conn2.OpenAsync(CancellationToken.None);
            }

            // Non authenticated proxy
            using (var conn3 = new SnowflakeDbConnection())
            {
                conn3.ConnectionString = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost={0};proxyPort={1}",
                    _fixture.testConfig.proxyHost,
                    _fixture.testConfig.proxyPort);
                await conn3.OpenAsync(CancellationToken.None);
            }

            // Invalid proxy
            using (var conn4 = new SnowflakeDbConnection())
            {
                conn4.ConnectionString =
                    _fixture.ConnectionString + "connection_timeout=20;useProxy=true;proxyHost=Invalid;proxyPort=8080;";
                try
                {
                    await conn4.OpenAsync(CancellationToken.None);
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
                await conn5.OpenAsync(CancellationToken.None);
            }

            // No proxy again, but crl check is disabled
            // Will use a different httpclient
            using (var conn6 = new SnowflakeDbConnection())
            {

                conn6.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString);
                await conn6.OpenAsync(CancellationToken.None);
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

                await conn7.OpenAsync(CancellationToken.None);
            }

            // No proxy again, crl check is enabled in the default connection string for tests
            // Should use same httpclient than conn2
            using (var conn8 = new SnowflakeDbConnection())
            {
                conn8.ConnectionString = _fixture.ConnectionString;
                await conn8.OpenAsync(CancellationToken.None);
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

                await conn9.OpenAsync(CancellationToken.None);
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

                await conn10.OpenAsync(CancellationToken.None);
            }

            // No proxy, but crl check disabled
            // Should use same httpclient than conn6
            using (var conn11 = new SnowflakeDbConnection())
            {
                conn11.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString);
                await conn11.OpenAsync(CancellationToken.None);
            }
        }

        [SFFact]
        public async Task TestInvalidProxySettingFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {

                conn.ConnectionString =
                    _fixture.ConnectionString + "connection_timeout=5;useProxy=true;proxyHost=Invalid;proxyPort=8080";
                try
                {
                    await conn.OpenAsync(CancellationToken.None);
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

        [SFTheory]
        [InlineData("*")]
        [InlineData("*{0}*")]
        [InlineData("^*{0}*")]
        [InlineData("*{0}*$")]
        [InlineData("^*{0}*$")]
        [InlineData("^nonmatch*{0}$|*")]
        [InlineData("*a*")]
        [InlineData("*la*", "la")]
        public async Task TestNonProxyHostShouldBypassProxyServer(string regexHost, string proxyHost = "proxyserverhost")
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                var host = _fixture.ResolveHost();
                var nonProxyHosts = string.Format(regexHost, $"{host}");
                conn.ConnectionString =
                    $"{_fixture.ConnectionString}USEPROXY=true;PROXYHOST={proxyHost};NONPROXYHOSTS={nonProxyHosts};PROXYPORT=3128;";

                // Act
                await conn.OpenAsync(CancellationToken.None);

                // Assert
                // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [SFTheory]
        [InlineData("invalid{0}")]
        [InlineData("*invalid{0}*")]
        [InlineData("^invalid{0}$")]
        [InlineData("*a.b")]
        [InlineData("a")]
        [InlineData("la", "la")]
        public async Task TestNonProxyHostShouldNotBypassProxyServer(string regexHost, string proxyHost = "proxyserverhost")
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Arrange
                var nonProxyHosts = string.Format(regexHost, $"{_fixture.testConfig.host}");
                conn.ConnectionString =
                    $"{_fixture.ConnectionString}connection_timeout=5;USEPROXY=true;PROXYHOST={proxyHost};NONPROXYHOSTS={nonProxyHosts};PROXYPORT=3128;";

                // Act/Assert
                // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
                var exception = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync(CancellationToken.None));

                // Assert
                Assert.Equal(270001, exception.ErrorCode);
                AssertIsConnectionFailure(exception);
            }
        }

        [SFFact]
        public async Task TestUseProxyFalseWithInvalidProxyConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString =
                    _fixture.ConnectionString + ";useProxy=false;proxyHost=Invalid;proxyPort=8080";
                await conn.OpenAsync(CancellationToken.None);
                // Because useProxy=false, the proxy settings are ignored
            }
        }

        [SFFact]
        public async Task TestInvalidProxySettingWithByPassListFromConnectionString()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                = _fixture.ConnectionString
                + String.Format(
                    ";useProxy=true;proxyHost=Invalid;proxyPort=8080;nonProxyHosts={0}",
                    $"*.foo.com %7C{_fixture.testConfig.account}.snowflakecomputing.com|*{_fixture.testConfig.host}*");
                await conn.OpenAsync(CancellationToken.None);
                // Because _fixture.testConfig.host is in the bypass list, the proxy should not be used
            }
        }

        [Fact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
        public async Task testMulitpleConnectionInParallel()
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
                tasks[i] = Task.Run(async () =>
                {
                    using (var conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = connString;
                        Console.WriteLine($"{conn.ConnectionString}");
                        try
                        {
                            await conn.OpenAsync(CancellationToken.None);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            Console.WriteLine("--------------------------");
                            Console.WriteLine(e.InnerException);
                            failed = true;
                        }

                        using (var command = conn.CreateCommand())
                        {
                            try
                            {
                                command.CommandText = "SELECT 1";
                                await command.ExecuteScalarAsync();
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
        public async Task TestEscapeChar()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;key1=test\'password;key2=test\"password;key3=test==password";
                await conn.OpenAsync(CancellationToken.None);
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

                await conn.CloseAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
        public async Task TestEscapeChar1()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;key==word=value; key1=\"test;password\"; key2=\"test=password\"";
                await conn.OpenAsync(CancellationToken.None);
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

                await conn.CloseAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }

        [Fact(Skip = "Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public async Task TestHeartBeat()
        {
            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;CLIENT_SESSION_KEEP_ALIVE=true";
            await conn.OpenAsync(CancellationToken.None);

            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs
            using (var command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.Equal(await command.ExecuteScalarAsync(), 46);
            }

            await conn.CloseAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Closed, conn.State);
        }

        [Fact(Skip = "Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
        public async Task TestHeartBeatWithConnectionPool()
        {
            SnowflakeDbConnectionPool.ClearAllPools();

            var conn = new SnowflakeDbConnection();
            conn.ConnectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=0;expirationTimeout=14800;CLIENT_SESSION_KEEP_ALIVE=true";
            await conn.OpenAsync(CancellationToken.None);
            await conn.CloseAsync(CancellationToken.None);

            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

            var conn1 = new SnowflakeDbConnection();
            conn1.ConnectionString = _fixture.ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
            await conn1.OpenAsync(CancellationToken.None);
            Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs

            using (var command = conn.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
                Assert.Equal(await command.ExecuteScalarAsync(), 46);
            }

            await conn1.CloseAsync(CancellationToken.None);
            Assert.Equal(ConnectionState.Closed, conn1.State);
            Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
        }

        [SFFact]
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
                        await conn.OpenAsync(CancellationToken.None);
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
    public class SFConnectionITAsync : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFConnectionITAsync(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionITAsync>();


        [SFFact]
        public async Task TestCancelLoginBeforeTimeout()
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

        [SFFact]
        public async Task TestAsyncLoginTimeout()
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

        [SFFact]
        public async Task TestAsyncLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
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

        [SFFact]
        public async Task TestAsyncDefaultLoginTimeout()
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

        [SFFact]
        public async Task TestAsyncConnectionFailFastForNonRetried404OnLogin()
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

        [SFFact]
        public async Task TestCloseAsyncWithCancellation()
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
        [SFFact]
        public async Task TestCloseAsync()
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
#endif

        [SFFact]
        public async Task TestCloseAsyncFailure()
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

        [SFFact]
        public async Task TestExplicitTransactionOperationsTracked()
        {
            using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync(CancellationToken.None);
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


        [SFFact]
        public async Task TestAsyncOktaConnectionUntilMaxTimeout()
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
        public async Task TestNativeOktaSuccess()
        {
            var oktaUrl = "https://***.okta.com/";
            var oktaUser = "***";
            var oktaPassword = "***";
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = _fixture.ConnectionStringWithoutAuth +
                                        $";authenticator={oktaUrl};user={oktaUser};password={oktaPassword};";
                await conn.OpenAsync(CancellationToken.None);
                Assert.Equal(ConnectionState.Open, conn.State);
            }
        }

        [SFFact]
        public async Task TestConnectStringWithQueryTag()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                string expectedQueryTag = "Test QUERY_TAG 12345";
                conn.ConnectionString = _fixture.ConnectionString + $";query_tag={expectedQueryTag}";

                await conn.OpenAsync(CancellationToken.None);
                var command = conn.CreateCommand();
                // This query itself will be part of the history and will have the query tag
                command.CommandText = "SELECT QUERY_TAG FROM table(information_schema.query_history_by_session())";
                var queryTag = await command.ExecuteScalarAsync();

                Assert.Equal(expectedQueryTag, queryTag);
            }
        }

        [SFFact]
        public async Task TestUseMultiplePoolsConnectionPoolByDefault()
        {
            // act
            var poolVersion = SnowflakeDbConnectionPool.GetConnectionPoolVersion();

            // assert
            Assert.Equal(ConnectionPoolType.MultipleConnectionPool, poolVersion);
        }

        [SFFact]
        // to enroll to mfa authentication edit your user profile
        public async Task TestMFATokenCachingWithPasscodeFromConnectionString()
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

        [SFFact]
        // to enroll to mfa authentication edit your user profile
        public async Task TestMfaWithPasswordConnectionUsingPasscodeWithSecureString()
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

        [SFTheory]
        [InlineData("connection_timeout=5;")]
        [InlineData("")]
        public async Task TestOpenAsyncThrowExceptionWhenConnectToUnreachableHost(string extraParameters)
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;useProxy=true;proxyHost=no.such.pro.xy;proxyPort=8080;certRevocationCheckMode=enabled;" +
                                   extraParameters;
            using (var connection = new SnowflakeDbConnection(connectionString))
            {
                // act
                var thrown = Assert.Throws<AggregateException>(() => connection.OpenAsync(CancellationToken.None).Wait());

                // assert
                Assert.True(thrown.InnerException is TaskCanceledException || thrown.InnerException is SnowflakeDbException);
                if (thrown.InnerException is SnowflakeDbException)
                    SnowflakeDbExceptionAssert.HasErrorCode(thrown.InnerException, SFError.INTERNAL_ERROR);
                Assert.Equal(ConnectionState.Closed, connection.State);
            }
        }

        [SFFact]
        public async Task TestOpenAsyncThrowExceptionWhenOperationIsCancelled()
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
        public async Task TestSSOConnectionWithTokenCachingAsync()
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

        [SFFact]
        public async Task TestCloseSessionWhenGarbageCollectorFinalizesConnection()
        {
            // arrange
            var session = await GetSessionFromForgottenConnection();
            Assert.NotNull(session);
            Assert.NotNull(session.sessionId);
            Assert.NotNull(session.sessionToken);

            // act
            GC.Collect();
            await Awaiter.WaitUntilConditionOrTimeout(() => session.sessionToken == null, TimeSpan.FromSeconds(15));

            // assert
            Assert.Null(session.sessionToken);
        }

        private async Task<SFSession> GetSessionFromForgottenConnection()
        {
            var connection = new SnowflakeDbConnection(_fixture.ConnectionString + ";poolingEnabled=false;application=TestGarbageCollectorCloseSession");
            await connection.OpenAsync(CancellationToken.None);
            return connection.SfSession;
        }

        [SFFact]
        public async Task TestHangingCloseIsNotBlocking()
        {
            // arrange
            var restRequester = new MockCloseHangingRestRequester();
            var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
            await session.OpenAsync(CancellationToken.None);
            var watchClose = new Stopwatch();
            var watchClosedFinished = new Stopwatch();

            // act
            watchClose.Start();
            watchClosedFinished.Start();
            session.CloseNonBlocking();
            watchClose.Stop();
            await Awaiter.WaitUntilConditionOrTimeout(() => restRequester.CloseRequests.Count > 0, TimeSpan.FromSeconds(15));
            watchClosedFinished.Stop();

            // assert
            Assert.Equal(1, restRequester.CloseRequests.Count);
            Assert.True(watchClose.Elapsed.Duration() < TimeSpan.FromSeconds(5)); // close executed immediately
            Assert.True(watchClosedFinished.Elapsed.Duration() >= TimeSpan.FromSeconds(10)); // while background task took more time
        }

        [Fact(Skip = "Manual test only")]
        public async Task TestOAuthFlow()
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
                    await connection.OpenAsync(CancellationToken.None);
                }
            }
            finally
            {
                RemoveOAuthCache(_fixture.testConfig);
            }
        }

        [Fact(Skip = "Manual test only")]
        public async Task TestProgrammaticAccessTokenAuthentication()
        {
            // arrange
            using (var connection = new SnowflakeDbConnection(ConnectionStringForPat(_fixture.testConfig)))
            {
                // act
                await connection.OpenAsync(CancellationToken.None);
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


