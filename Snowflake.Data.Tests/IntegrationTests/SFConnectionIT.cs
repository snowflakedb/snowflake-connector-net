using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Core.Authenticator;
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
        [TimeSensitive]
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
        [TimeSensitive]
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
        [TimeSensitive]
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
        [TimeSensitive]
        public void TestDefaultLoginTimeout()
        {
            using (IDbConnection conn = new MockSnowflakeDbConnection())
            {
                conn.ConnectionString = ConnectionString;

                // Default timeout is 300 sec
                Assert.AreEqual(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);

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

    }
}


