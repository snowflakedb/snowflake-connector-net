using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
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
    [TimeSensitive]
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
    [TimeSensitive]
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
