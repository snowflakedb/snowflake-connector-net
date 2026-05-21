using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    public sealed class SFConnectionIT : SFBaseTestAsync
    {
        private readonly SFBaseTestAsyncFixture _fixture;
        public SFConnectionIT(SFBaseTestAsyncFixture fixture) : base(fixture) { _fixture = fixture; }

        [SFFact(RetriesCount = RetriesCount.Thrice)]
        public void TestLoginTimeout()
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
                    conn.Open();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    // Jitter can cause the request to reach max number of retries before reaching the timeout
                    AssertExtensions.AnySucceeds(
                        () => SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.REQUEST_TIMEOUT),
                                    () => SnowflakeDbExceptionAssert.HasErrorCodeInExceptionChain(e, SFError.INTERNAL_ERROR)
                        );
                }

                stopwatch.Stop();
                int delta = 50; // in case server time slower.

                // Should timeout before the defined timeout plus 1 (buffer time)
                // Should timeout after the defined timeout since retry count is infinite
                Assert.InRange(stopwatch.ElapsedMilliseconds, timeoutSec * 1000 - delta, (timeoutSec + 1) * 1000);
                Assert.Equal(timeoutSec, conn.ConnectionTimeout);
            }
        }


        [Collection(nameof(IsolatedFixture))]
        public sealed class Isolated : SFBaseTestAsync, IDisposable
        {
            private readonly SFBaseTestAsyncFixture _fixtureHere;
            private readonly TimeSpan _oldTimeout;

            [CollectionDefinition(nameof(IsolatedFixture), DisableParallelization = true)]
            public sealed class IsolatedFixture : ICollectionFixture<IsolatedFixture>
            {
            }

            public Isolated(SFBaseTestAsyncFixture fixture) : base(fixture)
            {
                _fixtureHere = fixture;
                _oldTimeout = SFSessionHttpClientProperties.DefaultRetryTimeout;
                SFSessionHttpClientProperties.DefaultRetryTimeout = TimeSpan.FromSeconds(1);
            }

            public void Dispose()
            {
                SFSessionHttpClientProperties.DefaultRetryTimeout = _oldTimeout;
            }

            [SFFact]
            public void TestLoginTimeoutWithRetryTimeoutLesserThanConnectionTimeout()
            {
                using (IDbConnection conn = new MockSnowflakeDbConnection())
                {
                    int connectionTimeout = 5;
                    int retryTimeout = 1;
                    string loginTimeOut5sec = String.Format(_fixtureHere.ConnectionString + "connection_timeout={0};retry_timeout={1};maxHttpRetries=0",
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
                    int delta = 50; // in case server time slower.

                    // Should timeout after the defined timeout since retry count is infinite
                    Assert.InRange(stopwatch.ElapsedMilliseconds, retryTimeout * 1000 - delta, long.MaxValue);
                    Assert.Equal(retryTimeout, conn.ConnectionTimeout);
                }
            }
        }

        [SFFact]
        public void TestDefaultLoginTimeout()
        {
            using (var conn = new SnowflakeDbConnection())
            {
                // Default timeout is 300 sec
                Assert.Equal(SFSessionHttpClientProperties.DefaultRetryTimeout.TotalSeconds, conn.ConnectionTimeout);
                Assert.Equal(conn.State, ConnectionState.Closed);
            }
        }

        [SFFact]
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

        [SFFact]
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

        [SFFact]
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
                    SnowflakeDbExceptionAssert.HasErrorCode(e, SFError.INTERNAL_ERROR);
                    Assert.True(e.Message.Contains(
                        $"The retry count has reached its limit of {expectedMaxRetryCount} and " +
                        $"the timeout elapsed has reached its limit of {expectedMaxConnectionTimeout} " +
                        "while trying to authenticate through Okta"));
                }
            }
        }

        [SFFact]
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
    }
}


