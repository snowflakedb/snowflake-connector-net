using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Core.CredentialManager;
using Snowflake.Data.Core.CredentialManager.Infrastructure;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;
using Xunit;

namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class SFConnectionManualIT : SFBaseTestAsync
{
    private readonly SFBaseTestAsyncFixture _fixture;

    public SFConnectionManualIT(SFBaseTestAsyncFixture fixture) : base(fixture)
    {
        _fixture = fixture;
    }

    [SFFact(Skip = "This test requires established dev Okta SSO and credentials matching Snowflake user")]
    public async Task TestNativeOktaSuccess()
    {
        var oktaUrl = "https://***.okta.com/";
        var oktaUser = "***";
        var oktaPassword = "***";
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = _fixture.ConnectionStringWithoutAuth +
                                    $";authenticator={oktaUrl};user={oktaUser};password={oktaPassword};";
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "This test requires manual setup and therefore cannot be run in CI")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "This test requires manual setup and therefore cannot be run in CI")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
    public async Task TestSSOConnectionWithUser()
    {
        // Use external browser to log in using proper password for qa@snowflakecomputing.com
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString
                = _fixture.ConnectionStringWithoutAuth
                  + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com";
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);

            // connection pooling is disabled for external browser by default
            Assert.False(SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
            using (var command = conn.CreateCommand())
            {
                command.CommandText = "SELECT CURRENT_USER()";
                Assert.Equal("QA", (await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString());
            }
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
    public async Task TestSSOConnectionWithPoolingEnabled()
    {
        // Use external browser to log in using proper password for qa@snowflakecomputing.com
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString
                = _fixture.ConnectionStringWithoutAuth
                  + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;POOLINGENABLED=TRUE";
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
            Assert.True(SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
            using (var command = conn.CreateCommand())
            {
                command.CommandText = "SELECT CURRENT_USER()";
                Assert.Equal("QA", (await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString());
            }
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
    public async Task TestSSOConnectionWithUserAndDisableConsoleLogin()
    {
        // Use external browser to log in using proper password for qa@snowflakecomputing.com
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString
                = _fixture.ConnectionStringWithoutAuth
                  + ";authenticator=externalbrowser;user=qa@snowflakecomputing.com;disable_console_login=false;";
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
            using (var command = conn.CreateCommand())
            {
                command.CommandText = "SELECT CURRENT_USER()";
                Assert.Equal("QA", (await command.ExecuteScalarAsync().ConfigureAwait(false)).ToString());
            }
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
        ).ConfigureAwait(false);
        stopwatch.Stop();

        // timeout after specified number of seconds
        Assert.True(stopwatch.ElapsedMilliseconds >= waitSeconds * 1000);
        // and not later than 5s after expected time
        Assert.True(stopwatch.ElapsedMilliseconds <= (waitSeconds + 5) * 1000);
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }

        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = externalBrowserConnectionString;

            // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }


    [SFFact(Skip = "Manual test only")]
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
                await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        finally
        {
            RemoveOAuthCache(_fixture.testConfig);
        }
    }

    [SFFact(Skip = "Manual test only")]
    public async Task TestProgrammaticAccessTokenAuthentication()
    {
        // arrange
        using (var connection = new SnowflakeDbConnection(ConnectionStringForPat(_fixture.testConfig)))
        {
            // act
            await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }

        using (SnowflakeDbConnection conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = externalBrowserConnectionString;

            // Authenticate using the SSO token (the connector will automatically use the token and a browser should not pop-up in this step)
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
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

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
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
            await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);

            // Switch back to the default credential manager
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
        }
    }

    [SFFact(Skip = "This test requires manual interaction and therefore cannot be run in CI")]
    public async Task TestSSOConnectionWithWrongUser()
    {
        try
        {
            using (var conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString
                    = _fixture.ConnectionStringWithoutAuth
                      + ";authenticator=externalbrowser;user=wrong@snowflakecomputing.com";
                await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                Assert.Fail();
            }
        }
        catch (SnowflakeDbException e)
        {
            Assert.Equal(390191, e.ErrorCode);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
                await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
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

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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

            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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

            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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

            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
            await conn1.OpenAsync(CancellationToken).ConfigureAwait(false);
        }

        // No proxy
        using (var conn2 = new SnowflakeDbConnection())
        {
            conn2.ConnectionString = _fixture.ConnectionString;
            await conn2.OpenAsync(CancellationToken).ConfigureAwait(false);
        }

        // Non authenticated proxy
        using (var conn3 = new SnowflakeDbConnection())
        {
            conn3.ConnectionString = _fixture.ConnectionString
                                     + String.Format(
                                         ";useProxy=true;proxyHost={0};proxyPort={1}",
                                         _fixture.testConfig.proxyHost,
                                         _fixture.testConfig.proxyPort);
            await conn3.OpenAsync(CancellationToken).ConfigureAwait(false);
        }

        // Invalid proxy
        using (var conn4 = new SnowflakeDbConnection())
        {
            conn4.ConnectionString =
                _fixture.ConnectionString + "connection_timeout=20;useProxy=true;proxyHost=Invalid;proxyPort=8080;";
            try
            {
                await conn4.OpenAsync(CancellationToken).ConfigureAwait(false);
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
            await conn5.OpenAsync(CancellationToken).ConfigureAwait(false);
        }

        // No proxy again, but crl check is disabled
        // Will use a different httpclient
        using (var conn6 = new SnowflakeDbConnection())
        {
            conn6.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString);
            await conn6.OpenAsync(CancellationToken).ConfigureAwait(false);
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

            await conn7.OpenAsync(CancellationToken).ConfigureAwait(false);
        }

        // No proxy again, crl check is enabled in the default connection string for tests
        // Should use same httpclient than conn2
        using (var conn8 = new SnowflakeDbConnection())
        {
            conn8.ConnectionString = _fixture.ConnectionString;
            await conn8.OpenAsync(CancellationToken).ConfigureAwait(false);
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

            await conn9.OpenAsync(CancellationToken).ConfigureAwait(false);
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

            await conn10.OpenAsync(CancellationToken).ConfigureAwait(false);
        }

        // No proxy, but crl check disabled
        // Should use same httpclient than conn6
        using (var conn11 = new SnowflakeDbConnection())
        {
            conn11.ConnectionString = ConnectionStringModifier.DisableCrlRevocationCheck(_fixture.ConnectionString);
            await conn11.OpenAsync(CancellationToken).ConfigureAwait(false);
        }
    }

    [SFFact(Skip = "Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
    public async Task TestEscapeChar()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;key1=test\'password;key2=test\"password;key3=test==password";
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
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

            await conn.CloseAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
    public async Task TestEscapeChar1()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString =
                _fixture.ConnectionString + "poolingEnabled=false;key==word=value; key1=\"test;password\"; key2=\"test=password\"";
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
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

            await conn.CloseAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
    public async Task TestHeartBeat()
    {
        var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString + "poolingEnabled=false;CLIENT_SESSION_KEEP_ALIVE=true";
        await conn.OpenAsync(CancellationToken).ConfigureAwait(false);

        Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs
        using (var command = conn.CreateCommand())
        {
            command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
            Assert.Equal(46, await command.ExecuteScalarAsync().ConfigureAwait(false));
        }

        await conn.CloseAsync(CancellationToken).ConfigureAwait(false);
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [SFFact(Skip = "Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
    public async Task TestHeartBeatWithConnectionPool()
    {
        SnowflakeDbConnectionPool.ClearAllPools();

        var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _fixture.ConnectionString + "maxPoolSize=2;minPoolSize=0;expirationTimeout=14800;CLIENT_SESSION_KEEP_ALIVE=true";
        await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
        await conn.CloseAsync(CancellationToken).ConfigureAwait(false);

        Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

        var conn1 = new SnowflakeDbConnection();
        conn1.ConnectionString = _fixture.ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
        await conn1.OpenAsync(CancellationToken).ConfigureAwait(false);
        Thread.Sleep(TimeSpan.FromSeconds(14430)); // more than 4 hrs

        using (var command = conn.CreateCommand())
        {
            command.CommandText = $"SELECT COUNT(*) FROM DOUBLE_TABLE";
            Assert.Equal(46, await command.ExecuteScalarAsync().ConfigureAwait(false));
        }

        await conn1.CloseAsync(CancellationToken).ConfigureAwait(false);
        Assert.Equal(ConnectionState.Closed, conn1.State);
        Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());
    }


    [SFFact(Skip = "TestConnectStringWithUserPwd, this will popup an internet browser for external login.")]
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

            Assert.Equal(ConnectionState.Closed, conn.State);
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            await conn.CloseAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
                await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
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

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
                await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
                Assert.Fail();
            }
        }
        catch (SnowflakeDbException e)
        {
            // Invalid password for decrypting the private key
            Assert.Equal(270052, e.ErrorCode);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
                await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
                Assert.Fail();
            }
        }
        catch (SnowflakeDbException e)
        {
            // Invalid password (none provided) for decrypting the private key
            Assert.Equal(270052, e.ErrorCode);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
                await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
                Assert.Fail();
            }
        }
        catch (SnowflakeDbException e)
        {
            // Jwt token is invalid
            Assert.Equal(390144, e.ErrorCode);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
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
                await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
                Assert.Fail();
            }
        }
        catch (SnowflakeDbException e)
        {
            // Jwt token is invalid
            Assert.Equal(390144, e.ErrorCode);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
    public async Task TestValidOAuthConnection()
    {
        using (var conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString
                = _fixture.ConnectionStringWithoutAuth
                  + String.Format(
                      ";authenticator=oauth;token={0}",
                      _fixture.testConfig.oauthToken);
            await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact(Skip = "Ignore this test until configuration is setup for CI integration. Can be run manually.")]
    public async Task testMulitpleConnectionInParallel()
    {
        string baseConnectionString = _fixture.ConnectionString + $";CONNECTION_TIMEOUT=30;";
        string authenticatedProxy = String.Format("useProxy =true;proxyHost={0};proxyPort={1};proxyUser={2};proxyPassword={3};",
            _fixture.testConfig.authProxyHost,
            _fixture.testConfig.authProxyPort,
            _fixture.testConfig.authProxyUser,
            _fixture.testConfig.authProxyPwd);
        string byPassList = "nonProxyHosts=*.foo.com %7C" + _fixture.testConfig.host + "|localhost;";

        string[] connectionStrings =
        {
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
                        await conn.OpenAsync(CancellationToken).ConfigureAwait(false);
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
                            await command.ExecuteScalarAsync().ConfigureAwait(false);
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
