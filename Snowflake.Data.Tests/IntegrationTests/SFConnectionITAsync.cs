using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests;


class SFConnectionITAsync : SFBaseTestAsync
{
    private static SFLogger logger = SFLoggerFactory.GetLogger<SFConnectionITAsync>();

    [Test]
    public void TestBasicConnection()
    {
        using (IDbConnection conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = ConnectionString;
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);

            Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
            // Data source is empty string for now
            Assert.Equal("", ((SnowflakeDbConnection)conn).DataSource);

            string serverVersion = ((SnowflakeDbConnection)conn).ServerVersion;
            if (!string.Equals(serverVersion, "Dev"))
            {
                string[] versionElements = serverVersion.Split('.');
                Assert.Equal(3, versionElements.Length);
            }

            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);
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

                Assert.Equal(ConnectionState.Closed, conn.State);
            }
        }
    }

    [Test]
    [RunOnlyOnCI]
    public void TestApplicationPathIsSentDuringAuthentication()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var authenticator = (BaseAuthenticator)conn.SfSession.authenticator;
            var clientEnv = authenticator.BuildLoginRequestData().clientEnv;
            var lowerPath = clientEnv.applicationPath.ToLower();
#if NETFRAMEWORK
                Assert.True(
                    lowerPath.Contains("testhost") &&
                    (lowerPath.EndsWith(".dll") || lowerPath.EndsWith(".exe")),
                    $"APPLICATION_PATH should contain 'testhost' and end with .dll or .exe. Got: {clientEnv.applicationPath}");
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
        Assert.False(snowflakeDbConnection.IsOpen()); // check via public method
        Assert.Equal(ConnectionState.Closed, snowflakeDbConnection.State); // ensure internal state is expected
    }

    private static void AssertIsConnectionFailure(SnowflakeDbException e)
    {
        Assert.Equal(SnowflakeDbException.CONNECTION_FAILURE_SSTATE, e.SqlState);
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
            Assert.Equal(conn1.State, ConnectionState.Closed);

            conn1.Open();
            using (IDbCommand cmd = conn1.CreateCommand())
            {
                cmd.CommandText = $"SELECT count(*) FROM {TableName}";
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
            cmd.CommandText = "use database " + testConfig.database;
            cmd.ExecuteNonQuery();
            cmd.CommandText = "use schema " + testConfig.schema;
            cmd.ExecuteNonQuery();
        }

        conn.Close();
    }

    [Test]
    [TimeSensitive]
    public void TestLoginWithMaxRetryReached()
    {
        using (IDbConnection conn = new MockSnowflakeDbConnection())
        {
            string maxRetryConnStr = ConnectionString + "maxHttpRetries=7";

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
            Assert.Less(stopwatch.ElapsedMilliseconds, 166 * 1000);
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, 1 * 1000);
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

    [Test]
    public void TestInvalidConnectionString()
    {
        string[] invalidStrings =
        {
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

    [Test]
    public void TestUnknownConnectionProperty()
    {
        using (IDbConnection conn = new SnowflakeDbConnection())
        {
            // invalid propety will be ignored.
            conn.ConnectionString = ConnectionString + ";invalidProperty=invalidvalue;";

            conn.Open();
            Assert.Equal(conn.State, ConnectionState.Open);
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

            Assert.Equal(conn.State, ConnectionState.Closed);

            conn.Open();

            Assert.Equal(testConfig.database.ToUpper(), conn.Database);
            Assert.Equal(conn.State, ConnectionState.Open);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                conn.ChangeDatabase("SNOWFLAKE_SAMPLE_DATA");
                Assert.Equal("SNOWFLAKE_SAMPLE_DATA", conn.Database);
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
                Assert.Equal(conn.State, ConnectionState.Open);
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
            Assert.Equal(conn.State, ConnectionState.Open);

            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = "select current_role()";
                Assert.Equal(command.ExecuteScalar().ToString(), "PUBLIC");

                command.CommandText = "select current_database()";
                CollectionAssert.Contains(new[] { "SNOWFLAKE_SAMPLE_DATA", "" }, command.ExecuteScalar().ToString());

                command.CommandText = "select current_schema()";
                CollectionAssert.Contains(new[] { "INFORMATION_SCHEMA", "" }, command.ExecuteScalar().ToString());

                command.CommandText = "select current_warehouse()";
                // Command will return empty string if the hardcoded warehouse does not exist.
                Assert.Equal("", command.ExecuteScalar().ToString());
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
            Assert.False(reader.Read());
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
                conn.ConnectionString = "scheme=http;host=test;port=8080;user=test;password=test;account=test;authenticator=" +
                                        wrongAuthenticator;
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
                Assert.InstanceOf<SnowflakeDbException>(e);
                SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.INTERNAL_ERROR);
                Assert.True(e.Message.Contains(
                    $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                    $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                    "while trying to authenticate through Okta"));
            }
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
                Assert.Equal(270001, e.ErrorCode); //Internal error
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
    [Retry(3)]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [Test]
    [TestCase("invalid{0}")]
    [TestCase("*invalid{0}*")]
    [TestCase("^invalid{0}$")]
    [TestCase("*a.b")]
    [TestCase("a", "a")]
    [TestCase("la", "la")]
    [Retry(3)]
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
            Assert.Equal(270001, exception.ErrorCode);
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
    [TimeSensitive]
    public async Task TestKeepAlive()
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
                await Task.Delay(TimeSpan.FromSeconds(1));
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

    [Test]
    [TimeSensitive]
    public void TestAsyncLoginTimeout()
    {
        using (var conn = new MockSnowflakeDbConnection())
        {
            int timeoutSec = 5;
            string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0};maxHttpRetries=0",
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
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, timeoutSec * 1000 - delta);
            // But never more than 3 sec (buffer time) after the defined timeout
            Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (timeoutSec + 3) * 1000);

            Assert.Equal(ConnectionState.Closed, conn.State);
            Assert.Equal(timeoutSec, conn.ConnectionTimeout);
        }
    }

    [Test]
    [Retry(2)]
    [TimeSensitive]
    public void TestAsyncLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
    {
        using (var conn = new MockSnowflakeDbConnection())
        {
            int connectionTimeout = 600;
            int retryTimeout = 350;
            string loginTimeOut5sec = String.Format(ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
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
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, retryTimeout * 1000 - delta);
            // But never more than 2 sec (buffer time) after the defined timeout
            Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (retryTimeout + 2) * 1000);

            Assert.Equal(ConnectionState.Closed, conn.State);
            Assert.Equal(retryTimeout, conn.ConnectionTimeout);
        }
    }

    [Test]
    [TimeSensitive]
    public void TestAsyncDefaultLoginTimeout()
    {
        using (var conn = new MockSnowflakeDbConnection())
        {
            // unlimited retry count to trigger the timeout
            conn.ConnectionString = ConnectionString + "maxHttpRetries=0";

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
            Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, conn.ConnectionTimeout * 1000 - delta);
            // But never more because there's no connection timeout remaining (with 2 seconds margin)
            Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (conn.ConnectionTimeout + 2) * 1000);

            Assert.Equal(ConnectionState.Closed, conn.State);
            Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
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

    [Test]
    public void TestCloseAsyncFailure()
    {
        using (var conn = new MockSnowflakeDbConnection(new MockCloseSessionException()))
        {
            conn.ConnectionString = ConnectionString;
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

    [Test]
    public void TestExplicitTransactionOperationsTracked()
    {
        using (var conn = new SnowflakeDbConnection(ConnectionString))
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
                Assert.InstanceOf<SnowflakeDbException>(e.InnerException);
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
            Assert.Equal(ConnectionState.Open, conn.State);
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

            Assert.Equal(expectedQueryTag, queryTag);
        }
    }

    [Test]
    public void TestUseMultiplePoolsConnectionPoolByDefault()
    {
        // act
        var poolVersion = SnowflakeDbConnectionPool.GetConnectionPoolVersion();

        // assert
        Assert.Equal(ConnectionPoolType.MultipleConnectionPool, poolVersion);
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
            Assert.Equal(ConnectionState.Open, conn.State);
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
            Assert.Equal(ConnectionState.Open, conn.State);
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
            Assert.True(thrown.InnerException is TaskCanceledException || thrown.InnerException is SnowflakeDbException);
            if (thrown.InnerException is SnowflakeDbException)
                SnowflakeDbExceptionAssert.HasErrorCode(thrown.InnerException, SFError.INTERNAL_ERROR);
            Assert.Equal(ConnectionState.Closed, connection.State);
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
            Assert.InstanceOf<TaskCanceledException>(thrown.InnerException);
            Assert.Equal(ConnectionState.Closed, connection.State);
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
        Assert.Null(session.sessionToken);
    }

    private SFSession GetSessionFromForgottenConnection()
    {
        var connection = new SnowflakeDbConnection(ConnectionString + ";poolingEnabled=false;application=TestGarbageCollectorCloseSession");
        connection.Open();
        return connection.SfSession;
    }

    [Test]
    [TimeSensitive]
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
