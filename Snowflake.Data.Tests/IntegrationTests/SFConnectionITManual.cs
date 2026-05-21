namespace Snowflake.Data.Tests.IntegrationTests;

public sealed class SFConnectionITManual
{
    [SFFact]
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

            Assert.Equal(conn.State, ConnectionState.Closed);
            conn.Open();
            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
            Assert.Equal(true, SnowflakeDbConnectionPool.GetPool(conn.ConnectionString).GetPooling());
            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = "SELECT CURRENT_USER()";
                Assert.Equal("QA", command.ExecuteScalar().ToString());
            }
        }
    }

    [SFFact]
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

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
            using (IDbCommand command = conn.CreateCommand())
            {
                command.CommandText = "SELECT CURRENT_USER()";
                Assert.Equal("QA", command.ExecuteScalar().ToString());
            }
        }
    }

    [SFFact]
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

    [SFFact]
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
        Assert.GreaterOrEqual(stopwatch.ElapsedMilliseconds, waitSeconds * 1000);
        // and not later than 5s after expected time
        Assert.LessOrEqual(stopwatch.ElapsedMilliseconds, (waitSeconds + 5) * 1000);
    }

    [SFFact]
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

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);

            // Switch back to the default credential manager
            SnowflakeCredentialManagerFactory.UseDefaultCredentialManager();
        }
    }

    [SFFact]
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
            Assert.Equal(390191, e.ErrorCode);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(270008, e.ErrorCode);
        }
    }

    [SFFact]
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
            Assert.Equal(270052, e.ErrorCode);
        }
    }

    [SFFact]
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
            Assert.Equal(270052, e.ErrorCode);
        }
    }

    [SFFact]
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
            Assert.Equal(390144, e.ErrorCode);
        }
    }

    [SFFact]
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
            Assert.Equal(390144, e.ErrorCode);
        }
    }


    [SFFact]
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
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [SFFact]
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
            Assert.Equal(390318, e.ErrorCode);
        }
    }

    [SFFact]
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

    [SFFact]
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

    [SFFact]
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

    [SFFact]
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

    [SFFact]
    [Ignore("Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
    public void TestEscapeChar()
    {
        using (IDbConnection conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = ConnectionString + "poolingEnabled=false;key1=test\'password;key2=test\"password;key3=test==password";
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

    [SFFact]
    [Ignore("Ignore this test, please test this manual with breakpoint at SFSessionProperty::ParseConnectionString() to verify")]
    public void TestEscapeChar1()
    {
        using (IDbConnection conn = new SnowflakeDbConnection())
        {
            conn.ConnectionString = ConnectionString + "poolingEnabled=false;key==word=value; key1=\"test;password\"; key2=\"test=password\"";
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

    [SFFact]
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
            Assert.Equal(command.ExecuteScalar(), 46);
        }

        conn.Close();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [SFFact]
    [Ignore("Ignore this test. Please run this manually, since it takes 4 hrs to finish.")]
    public void TestHeartBeatWithConnectionPool()
    {
        SnowflakeDbConnectionPool.ClearAllPools();

        var conn = new SnowflakeDbConnection();
        conn.ConnectionString = ConnectionString + "maxPoolSize=2;minPoolSize=0;expirationTimeout=14800;CLIENT_SESSION_KEEP_ALIVE=true";
        conn.Open();
        conn.Close();

        Assert.Equal(1, SnowflakeDbConnectionPool.GetCurrentPoolSize());

        var conn1 = new SnowflakeDbConnection();
        conn1.ConnectionString = ConnectionString + ";CLIENT_SESSION_KEEP_ALIVE=true";
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
}
