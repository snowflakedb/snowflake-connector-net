using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class SFConnectionITAsync : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;
    public SFConnectionITAsync(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

    private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SFConnectionITAsync>();

    [SFFact]
    public async Task TestBasicConnection()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = _fixture.ConnectionString;
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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

            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(ConnectionState.Open, conn.State);

                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
                    await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            var authenticator = (BaseAuthenticator)conn.SfSession.authenticator;
            var clientEnv = authenticator.BuildLoginRequestData().clientEnv;
            var lowerPath = clientEnv.applicationPath.ToLower();
#if NETFRAMEWORK
                Assert.True(
                    (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")));
#else
            Assert.True(
                lowerPath.Contains("snowflake.data.tests") &&
                lowerPath.Contains("bin") &&
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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
                await snowflakeConnection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail("Connection open should fail");
            }
            catch (SnowflakeDbException e)
            {
                AssertIsConnectionFailure(e);
                AssertConnectionIsNotOpen(snowflakeConnection);
                if (explicitClose)
                {
                    await snowflakeConnection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
                    await snowflakeConnection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (SnowflakeDbException e)
            {
                AssertIsConnectionFailure(e);
                AssertConnectionIsNotOpen(snowflakeConnection);
            }
        }
    }

    [SFFact]
    public async Task TestConnectString()
    {
        var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
        var schemaName = "dlSchema_" + Guid.NewGuid().ToString().Replace("-", "_");
        var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString;
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        using (var cmd = conn.CreateCommand())
        {
            //cmd.CommandText = "create database \"dlTest\"";
            //cmd.ExecuteNonQuery();
            //cmd.CommandText = "use database \"dlTest\"";
            //cmd.ExecuteNonQuery();
            cmd.CommandText = $"create schema \"{schemaName}\"";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            cmd.CommandText = $"use schema \"{schemaName}\"";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            //cmd.CommandText = "create table \"dlTest\".\"dlSchema\".test1 (col1 string, col2 int)";
            cmd.CommandText = $"create table {tableName} (col1 string, col2 int)";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            //cmd.CommandText = "insert into \"dlTest\".\"dlSchema\".test1 Values ('test 1', 1);";
            cmd.CommandText = $"insert into {tableName} Values ('test 1', 1);";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
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

            await conn1.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            using (IDbCommand cmd = conn1.CreateCommand())
            {
                cmd.CommandText = $"SELECT count(*) FROM {tableName}";
                IDataReader reader = cmd.ExecuteReader();
                Assert.True(reader.Read());
                Assert.Equal(1, reader.GetInt32(0));
            }
            await conn1.CloseAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(ConnectionState.Closed, conn1.State);
        }

        using (var cmd = conn.CreateCommand())
        {
            //cmd.CommandText = "drop database \"dlTest\"";
            cmd.CommandText = $"drop schema \"{schemaName}\"";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            cmd.CommandText = "use database " + _fixture.testConfig.database;
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            cmd.CommandText = "use schema " + _fixture.testConfig.schema;
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
            Assert.Equal(conn.State, ConnectionState.Open);

            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsType<SnowflakeDbException>(e);
            }
            stopwatch.Stop();

            // retry 7 times with starting backoff of 1 second
            // backoff is chosen randomly it can drop to 0. So the minimal backoff time could be 1 + 0 + 0 + 0 + 0 + 0 + 0 = 1
            // The maximal backoff time could be 1 + 2 + 5 + 10 + 21 + 42 + 85 = 166
            Assert.InRange(stopwatch.ElapsedMilliseconds, 1 * 1000, 166 * 1000);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    [SFFact]
    public async Task TestInvalidConnectionString()
    {
        Skip.When(true, "TODO Bug: SNOW-3552882");
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
                    await conn.OpenAsync();
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

            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Open);
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    [SFFact(SkipCondition.SkipOnCloudAzure | SkipCondition.SkipOnCloudGCP)]
    public async Task TestSwitchDb()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = _fixture.ConnectionString;

            Assert.Equal(conn.State, ConnectionState.Closed);

            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
            Assert.Equal(conn.State, ConnectionState.Open);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.Equal("SNOWFLAKE_SAMPLE_DATA", conn.Database);
            }

            conn.ChangeDatabase(_fixture.testConfig.database);
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    [SFTheory(SkipCondition.RunOnlyOnCloudAWS)]
    [InlineData("SNOWFLAKE_SAMPLE_DAT")]
    [InlineData("SNOWFLAKE_$$$123SAMPLE_DATA")]
    [InlineData("\"1SNOWFLAKE_SAMPLE_DATA\"")]
    [InlineData("_SNOWFLAKE$$1_$AMPLEDATA")]
    [InlineData("$SNOWFLAKEAMP_LEDATA")]
    [InlineData("$SNOWFLAKEAMP_12LEDATA")]
    [InlineData("\"SNOWFLAKEAMPLEDATA\"")]
    [InlineData("\"SNOWFLAKE\"\"AMPLEDATA\"")]
    public async Task TestSwitchDbVariousIdentifiers(string databaseName)
    {
        using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString;

        Assert.Equal(conn.State, ConnectionState.Closed);

        await conn.OpenAsync(CancellationToken);

        Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
        Assert.Equal(conn.State, ConnectionState.Open);

        var ex = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.ChangeDatabaseAsync(databaseName));
        Assert.Equal(2043, ex.ErrorCode); // object does not exist
        await conn.CloseAsync(CancellationToken);
    }

    [SFTheory(SkipCondition.RunOnlyOnCloudAWS)]
    [InlineData("@SNOWFLAKE_SAMPLE_DATA")]
    [InlineData("!SNOWFLAKE_SAMPLE_DATA")]
    [InlineData("1SNOWFLAKE_SAMPLE_DATA")]
    [InlineData("\"SNOW\"FLAKE_SAMPLE_DATA\"")]
    public async Task TestSwitchDbWhenInvalidIdentifier(string invalidDatabaseName)
    {
        using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString;

        Assert.Equal(conn.State, ConnectionState.Closed);

        await conn.OpenAsync(CancellationToken);

        Assert.Equal(_fixture.testConfig.database.ToUpper(), conn.Database);
        Assert.Equal(conn.State, ConnectionState.Open);

        var ex = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.ChangeDatabaseAsync(invalidDatabaseName, CancellationToken));
        Assert.Equal(1003, ex.ErrorCode); // unable to parse sql
        await conn.CloseAsync(CancellationToken);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(conn.State, ConnectionState.Open);
                await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Open);

            using (var command = conn.CreateCommand())
            {
                command.CommandText = "select current_role()";
                Assert.Equal("PUBLIC", (await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString());

                command.CommandText = "select current_database()";
                Assert.Contains((await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString(), new[] { "SNOWFLAKE_SAMPLE_DATA", "" });

                command.CommandText = "select current_schema()";
                Assert.Contains((await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString(), new[] { "INFORMATION_SCHEMA", "" });

                command.CommandText = "select current_warehouse()";
                // Command will return empty string if the hardcoded warehouse does not exist.
                Assert.Equal("", (await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString());
            }
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    // Test that when a connection is disposed, a close would send out and unfinished transaction would be roll back.
    [SFFact]
    public async Task TestConnectionDispose()
    {
        var tableName = _fixture.TableNameBaseName + Guid.NewGuid().ToString("N");
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            await _fixture.CreateOrReplaceTable(conn, tableName, new[] { "c INT" });
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail("Authentication of {0} should fail");
            }
            catch (SnowflakeDbException e)
            {
                SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.UNKNOWN_AUTHENTICATOR);
            }

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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsType<SnowflakeDbException>(e);
                SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.INTERNAL_ERROR);
                var message = string.Join("\n", ((AggregateException)e.InnerException).InnerExceptions.Select(x => x.Message));
                Assert.Contains(
                    $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                    $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                    "while trying to authenticate through Okta", message);
            }
        }
    }


    private static void AssertIsConnectionFailure(SnowflakeDbException e)
    {
        Assert.Equal(SnowflakeDbException.CONNECTION_FAILURE_SSTATE, e.SqlState);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Equal(ConnectionState.Open, conn.State);
                Assert.Fail();
            }
        }
        catch (SnowflakeDbException e)
        {
            // Invalid OAuth access token
            var aggregateEx = ((AggregateException)e.InnerException);
            var aggregateEx2 = ((AggregateException)aggregateEx.InnerException);
            var innerEx = aggregateEx2.InnerExceptions.First() as SnowflakeDbException;
            Assert.Equal(390303, innerEx.ErrorCode);
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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

    [SFTheory(RetriesCount = RetriesCount.Thrice)]
    [InlineData("*")]
    [InlineData("*{0}*")]
    [InlineData("^*{0}*")]
    [InlineData("*{0}*$")]
    [InlineData("^*{0}*$")]
    [InlineData("^nonmatch*{0}$|*")]
    [InlineData("*a*", "a")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // Assert
            // The connection would fail to open if the web proxy would be used because the proxy is configured to a non-existent host.
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFTheory(RetriesCount = RetriesCount.Thrice)]
    [InlineData("invalid{0}")]
    [InlineData("*invalid{0}*")]
    [InlineData("^invalid{0}$")]
    [InlineData("*a.b")]
    [InlineData("a", "a")]
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
            var exception = await Assert.ThrowsAsync<SnowflakeDbException>(() => conn.OpenAsync(CancellationToken.None)).ConfigureAwait(false);

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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            // Because _fixture.testConfig.host is in the bypass list, the proxy should not be used
        }
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
                    await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                }
                await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
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
            var openTask = conn.OpenAsync(connectionCancelToken.Token);

            Assert.Equal(ConnectionState.Connecting, conn.State);

            s_logger.Debug("connectionCancelToken.Cancel ");

            try
            {
                connectionCancelToken.Cancel();
                await openTask.ConfigureAwait(false);
                Assert.Fail();
            }
            catch (Exception e)
            {
                AssertExtensions.AnySucceeds(
                    () => Assert.IsAssignableFrom<TaskCanceledException>(e),
                    () => Assert.IsAssignableFrom<TaskCanceledException>(e.InnerException)
                );
            }

            Assert.Equal(ConnectionState.Closed, conn.State);
            Assert.Equal(timeoutSec, conn.ConnectionTimeout);
        }
    }

    [SFFact(RetriesCount = RetriesCount.Thrice)]
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (SnowflakeDbException e)
            {
                SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.INTERNAL_ERROR);
            }
            catch (AggregateException e)
            {
                SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.INTERNAL_ERROR);
            }
            stopwatch.Stop();
            int delta = 50; // in case server time slower.

            // Should timeout after the defined timeout since retry count is infinite
            Assert.InRange(stopwatch.ElapsedMilliseconds, timeoutSec * 1000 - delta, long.MaxValue);

            Assert.Equal(ConnectionState.Closed, conn.State);
            Assert.Equal(timeoutSec, conn.ConnectionTimeout);
        }
    }


    [Collection(nameof(FixtureAsync))]
    public sealed class Isolated : SFBaseTestAsync, IDisposable
    {
        private readonly SFBaseTestAsyncFixture _fixtureHere;
        private readonly TimeSpan _oldTimeout;

        public Isolated(SFBaseTestAsyncFixture fixture) : base(fixture)
        {
            _fixtureHere = fixture;
            _oldTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout;
            SFSessionHttpClientProperties.DefaultRetryTimeout = TimeSpan.FromSeconds(1);
        }

        [CollectionDefinition(nameof(FixtureAsync), DisableParallelization = true)]
        public sealed class FixtureAsync : ICollectionFixture<FixtureAsync>
        {
        }

        public void Dispose()
        {
            SFSessionHttpClientProperties.DefaultRetryTimeout = _oldTimeout;
        }

        [SFFact]
        public async Task TestAsyncLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
        {
            using (var conn = new MockSnowflakeDbConnection())
            {
                int connectionTimeout = 5;
                int retryTimeout = 1;
                conn.ConnectionString = String.Format(_fixtureHere.ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
                    connectionTimeout, retryTimeout);

                Assert.Equal(conn.State, ConnectionState.Closed);

                Stopwatch stopwatch = Stopwatch.StartNew();
                try
                {
                    await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (AggregateException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.REQUEST_TIMEOUT);
                }
                catch (SnowflakeDbException e)
                {
                    SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.REQUEST_TIMEOUT);
                }

                stopwatch.Stop();
                int delta = 10; // in case server time slower.

                // Should timeout after the defined timeout since retry count is infinite
                Assert.InRange(stopwatch.ElapsedMilliseconds, retryTimeout * 1000 - delta, long.MaxValue);

                Assert.Equal(ConnectionState.Closed, conn.State);
                Assert.Equal(retryTimeout, conn.ConnectionTimeout);
            }
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
            try
            {
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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

            // Close the connection. It's not opened yet, but it should not have any issue
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Closed);

            // Open the connection
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Open);

            // Close the opened connection
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Closed);

            // Close the connection again.
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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

            // Close the connection. It's not opened yet, but it should not have any issue
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Closed);

            // Open the connection
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Open);

            // Close the opened connection
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Closed);

            // Close the connection again.
            await conn.CloseAsync(CancellationToken.None).ConfigureAwait(false);
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

            // Open the connection
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(conn.State, ConnectionState.Open);

            // Close the opened connection
            var closeTask = conn.CloseAsync(CancellationToken.None);
            try
            {
                await closeTask;
                Assert.Fail();
            }
            catch (AggregateException e)
            {
                Assert.Equal(MockCloseSessionException.SESSION_CLOSE_ERROR,
                    ((SnowflakeDbException)e.InnerException).ErrorCode);
            }
            Assert.Equal(conn.State, ConnectionState.Open);
        }
    }

    [SFFact]
    public async Task TestExplicitTransactionOperationsTracked()
    {
        using (var conn = new SnowflakeDbConnection(_fixture.ConnectionString))
        {
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.False(conn.HasActiveExplicitTransaction());

            var trans = await conn.BeginTransactionAsync();
            Assert.True(conn.HasActiveExplicitTransaction());
            await trans.RollbackAsync();
            Assert.False(conn.HasActiveExplicitTransaction());

            await (await conn.BeginTransactionAsync()).RollbackAsync();
            Assert.False(conn.HasActiveExplicitTransaction());

            await (await conn.BeginTransactionAsync()).CommitAsync();
            Assert.False(conn.HasActiveExplicitTransaction());
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
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail();
            }
            catch (Exception e)
            {
                SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.INTERNAL_ERROR);
                var expectedMessage =
                    $"The retry count has reached its limit of {expectedMaxRetryCount} and the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} while trying to authenticate through Okta";
                AssertExtensions.AnySucceeds(
                    () => Assert.Contains(expectedMessage, e.Message),
                    () => Assert.Contains(expectedMessage, e.InnerException?.Message),
                    () => Assert.Contains(expectedMessage, e.InnerException?.InnerException?.Message),
                    () => Assert.Contains(expectedMessage, e.InnerException?.InnerException?.InnerException?.Message)
                );
            }
        }
    }

    [SFFact]
    public async Task TestConnectStringWithQueryTag()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            string expectedQueryTag = "Test QUERY_TAG 12345";
            conn.ConnectionString = _fixture.ConnectionString + $";query_tag={expectedQueryTag}";

            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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

    [SFFact(SkipCondition.SkipOnJenkins)]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

            // assert
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFTheory(RetriesCount = RetriesCount.Thrice)]
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
            var thrown = await Assert.ThrowsAnyAsync<Exception>(() => connection.OpenAsync(CancellationToken.None)).ConfigureAwait(false);

            // assert
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
        using var connection = new SnowflakeDbConnection(connectionString);
        var shortCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // act
        await Assert.ThrowsAsync<TaskCanceledException>(() => connection.OpenAsync(shortCancellation.Token)).ConfigureAwait(false);

        // assert
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [SFFact(RetriesCount = RetriesCount.Thrice)]
    public async Task TestCloseSessionWhenGarbageCollectorFinalizesConnection()
    {
        // arrange
        var session = GetSessionFromForgottenConnection();
        Assert.NotNull(session);
        Assert.NotNull(session.sessionId);
        Assert.NotNull(session.sessionToken);

        // act
        GC.Collect();
        await Awaiter.WaitUntilConditionOrTimeout(() => session.sessionToken == null, TimeSpan.FromSeconds(30)).ConfigureAwait(false);

        // assert
        Assert.Null(session.sessionToken);
    }

    private SFSession GetSessionFromForgottenConnection()
    {
        var connection = new SnowflakeDbConnection(_fixture.ConnectionString + ";poolingEnabled=false;application=TestGarbageCollectorCloseSession");
        connection.Open();
        return connection.SfSession;
    }

    [SFFact]
    public async Task TestHangingCloseIsNotBlocking()
    {
        // arrange
        var restRequester = new MockCloseHangingRestRequester();
        var session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext(), restRequester);
        await session.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        var watchClose = new Stopwatch();
        var watchClosedFinished = new Stopwatch();

        // act
        watchClose.Start();
        watchClosedFinished.Start();
        session.CloseNonBlocking();
        watchClose.Stop();
        await Awaiter.WaitUntilConditionOrTimeout(() => restRequester.CloseRequests.Count > 0, TimeSpan.FromSeconds(15)).ConfigureAwait(false);
        watchClosedFinished.Stop();

        // assert
        Assert.Single(restRequester.CloseRequests);
        Assert.True(watchClose.Elapsed.Duration() < TimeSpan.FromSeconds(5)); // close executed immediately
        Assert.True(watchClosedFinished.Elapsed.Duration() >= TimeSpan.FromSeconds(10)); // while background task took more time
    }

    private static void AssertConnectionIsNotOpen(SnowflakeDbConnection snowflakeDbConnection)
    {
        Assert.NotNull(snowflakeDbConnection);
        Assert.False(snowflakeDbConnection.IsOpen()); // check via public method
        Assert.Equal(ConnectionState.Closed, snowflakeDbConnection.State); // ensure internal state is expected
    }
}
