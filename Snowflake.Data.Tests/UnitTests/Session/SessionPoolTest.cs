using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    public sealed class SessionPoolTest
    {
        private const string ConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;";

        [SFFact]
        public void TestPoolParametersAreNotOverriden()
        {
            // act
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);

            // assert
            Assert.False(pool.IsConfigOverridden());
        }

        [SFFact]
        public void TestOverrideMaxPoolSize()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);
            var newMaxPoolSize = 15;

            // act
            pool.SetMaxPoolSize(newMaxPoolSize);

            // assert
            Assert.Equal(newMaxPoolSize, pool.GetMaxPoolSize());
            Assert.True(pool.IsConfigOverridden());
        }

        [SFFact]
        public void TestOverrideExpirationTimeout()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);
            var newExpirationTimeoutSeconds = 15;

            // act
            pool.SetTimeout(newExpirationTimeoutSeconds);

            // assert
            Assert.Equal(newExpirationTimeoutSeconds, pool.GetTimeout());
            Assert.True(pool.IsConfigOverridden());
        }

        [SFFact]
        public void TestOverrideSetPooling()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);

            // act
            pool.SetPooling(false);

            // assert
            Assert.False(pool.GetPooling());
            Assert.True(pool.IsConfigOverridden());
        }

        [SFTheory]
        [InlineData("account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443", "somePassword", "someSecret", "someToken", " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;private_key=SomePrivateKey;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;token=someToken;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;private_key_pwd=somePrivateKeyPwd;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;proxyPassword=someProxyPassword;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("ACCOUNT=someAccount;DB=someDb;HOST=someHost;PASSWORD=somePassword;passcode=123;USER=SomeUser;PORT=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [InlineData("ACCOUNT=\"someAccount\";DB=\"someDb\";HOST=\"someHost\";PASSWORD=\"somePassword\";PASSCODE=\"123\";USER=\"SomeUser\";PORT=\"443\"", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        public void TestPoolIdentificationBasedOnConnectionString(string connectionString, string password, string clientSecret, string token, string expectedPoolIdentification)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var secureClientSecret = clientSecret == null ? null : SecureStringHelper.Encode(clientSecret);
            var secureToken = token == null ? null : SecureStringHelper.Encode(token);
            var pool = SessionPool.CreateSessionPool(connectionString, securePassword, secureClientSecret, secureToken);

            // act
            var poolIdentification = pool.PoolIdentificationBasedOnConnectionString;

            // assert
            Assert.Equal(expectedPoolIdentification, poolIdentification);
        }

        [SFFact]
        public void TestRetrievePoolFailureForInvalidConnectionString()
        {
            // arrange
            var invalidConnectionString = "account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443"; // invalid because password is not provided

            // act
            var exception = Assert.Throws<SnowflakeDbException>(() => SessionPool.CreateSessionPool(invalidConnectionString, null, null, null));

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.MISSING_CONNECTION_PROPERTY);
            Assert.True(exception.Message.Contains("Required property PASSWORD is not provided"));
        }

        [SFFact]
        public void TestPoolIdentificationBasedOnInternalId()
        {
            // arrange
            var connectionString = "account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;port=443";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            var poolIdRegex = new Regex(@"^ \[pool: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\]$");

            // act
            var poolIdentification = pool.PoolIdentificationBasedOnInternalId;

            // assert
            Assert.True(poolIdRegex.IsMatch(poolIdentification));
        }

        [SFFact]
        public void TestPoolIdentificationForOldPool()
        {
            // arrange
            var pool = SessionPool.CreateSessionCache();

            // act
            var poolIdentification = pool.PoolIdentification();

            // assert
            Assert.Equal("", poolIdentification);
        }

        [SFTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("anyPassword")]
        public void TestValidateValidSecurePassword(string password)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var pool = SessionPool.CreateSessionPool(ConnectionString, securePassword, null, null);

            // act
            pool.ValidateSecurePassword(securePassword);
        }

        [SFTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("anySecret")]
        public void TestValidateValidSecureClientCredentials(string oauthClientSecret)
        {
            // arrange
            var secureOAuthClientSecret = oauthClientSecret == null ? null : SecureStringHelper.Encode(oauthClientSecret);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, secureOAuthClientSecret, null);

            // act
            pool.ValidateSecureOAuthClientSecret(secureOAuthClientSecret);
        }

        [SFTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("anySecret")]
        public void TestValidateValidSecureToken(string token)
        {
            // arrange
            var secureToken = token == null ? null : SecureStringHelper.Encode(token);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, secureToken);

            // act
            pool.ValidateSecureToken(secureToken);
        }

        [SFTheory]
        [InlineData("somePassword", null)]
        [InlineData("somePassword", "")]
        [InlineData("somePassword", "anotherPassword")]
        [InlineData("", "anotherPassword")]
        [InlineData(null, "anotherPassword")]
        public void TestFailToValidateNotMatchingSecurePassword(string poolPassword, string notMatchingPassword)
        {
            // arrange
            var poolSecurePassword = poolPassword == null ? null : SecureStringHelper.Encode(poolPassword);
            var notMatchingSecurePassword = notMatchingPassword == null ? null : SecureStringHelper.Encode(notMatchingPassword);
            var pool = SessionPool.CreateSessionPool(ConnectionString, poolSecurePassword, null, null);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecurePassword(notMatchingSecurePassword));

            // assert
            Assert.Contains("Could not get a pool because of password mismatch", thrown.Message);
        }

        [SFTheory]
        [InlineData("someSecret", null)]
        [InlineData("someSecret", "")]
        [InlineData("someSecret", "anotherSecret")]
        [InlineData("", "anotherSecret")]
        [InlineData(null, "anotherSecret")]
        public void TestFailToValidateNotMatchingSecureClientCredentials(string poolOAuthClientSecret, string notMatchingOAuthClientSecret)
        {
            // arrange
            var poolSecureOAuthClientSecret = poolOAuthClientSecret == null ? null : SecureStringHelper.Encode(poolOAuthClientSecret);
            var notMatchingSecureOAuthClientSecret = notMatchingOAuthClientSecret == null ? null : SecureStringHelper.Encode(notMatchingOAuthClientSecret);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, poolSecureOAuthClientSecret, null);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecureOAuthClientSecret(notMatchingSecureOAuthClientSecret));

            // assert
            Assert.Contains("Could not get a pool because of oauth client secret mismatch", thrown.Message);
        }

        [SFTheory]
        [InlineData("someToken", null)]
        [InlineData("someToken", "")]
        [InlineData("someToken", "anotherToken")]
        [InlineData("", "anotherToken")]
        [InlineData(null, "anotherToken")]
        public void TestFailToValidateNotMatchingSecureToken(string poolToken, string notMatchingToken)
        {
            // arrange
            var poolSecureToken = poolToken == null ? null : SecureStringHelper.Encode(poolToken);
            var notMatchingSecureToken = notMatchingToken == null ? null : SecureStringHelper.Encode(notMatchingToken);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, poolSecureToken);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecureToken(notMatchingSecureToken));

            // assert
            Assert.Contains("Could not get a pool because of token mismatch", thrown.Message);
        }

        [SFTheory]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=true;", true)]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=false;", false)]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;", false)]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;", false)]
        public void TestConnectionPoolPoolingForOAuthAuthorizationCode(string connectionString, bool expectedPoolingEnabled)
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            var session = CreateSessionWithCurrentStartTime(connectionString);

            // assert
            Assert.Equal(expectedPoolingEnabled, pool.GetPooling());

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.Equal(expectedPoolingEnabled, isSessionReturnedToPool);
        }

        [SFTheory]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=true;")]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=false;")]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;")]
        [InlineData("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;")]
        [InlineData("authenticator=oauth_client_credentials;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;oauthTokenRequestUrl=https://okta.com/token-request;")]
        [InlineData("authenticator=programmatic_access_token;account=test;token=patToken")]
        public void TestConnectionCachePoolingDisabledForNewAuthenticators(string connectionString)
        {
            // arrange
            var session = CreateSessionWithCurrentStartTime(connectionString);
            var pool = SessionPool.CreateSessionCache();

            // assert
            Assert.Equal(true, pool.GetPooling()); // for the old connection cache pooling is always enabled

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.False(isSessionReturnedToPool);
        }

        [SFFact]
        public void TestShouldClearQueryContextCacheOnReturningToConnectionCache()
        {
            // arrange
            var session = CreateSessionWithCurrentStartTime("account=testAccount;user=testUser;password=testPassword");
            var pool = SessionPool.CreateSessionCache();
            var contextElement = new QueryContextElement(123, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 1, "context");
            var context = new ResponseQueryContext { Entries = new List<ResponseQueryContextElement> { new(contextElement) } };
            session.UpdateQueryContextCache(context);
            Assert.Equal(1, session.GetQueryContextRequest().Entries.Count);

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.True(isSessionReturnedToPool);
            Assert.Equal(0, session.GetQueryContextRequest().Entries.Count);
        }

        [SFFact]
        public void TestShouldClearQueryContextCacheOnReturningToConnectionPool()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword";
            var session = CreateSessionWithCurrentStartTime(connectionString);
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            var contextElement = new QueryContextElement(123, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 1, "context");
            var context = new ResponseQueryContext { Entries = new List<ResponseQueryContextElement> { new(contextElement) } };
            session.UpdateQueryContextCache(context);
            Assert.Equal(1, session.GetQueryContextRequest().Entries.Count);

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.True(isSessionReturnedToPool);
            Assert.Equal(0, session.GetQueryContextRequest().Entries.Count);
        }

        [SFFact]
        public void TestShouldRenewSessionIfKeepAliveIsEnabled()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var session = CreateSessionWithCurrentStartTime(connectionString, new MockRestSessionExpired());
            session.startHeartBeatForThisSession();
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool.SetTimeout(0);
            pool._idleSessions.Add(session);

            // act
            pool.ExtractIdleSession(connectionString);

            // assert
            Assert.Equal(MockRestSessionExpired.NEW_SESSION_TOKEN, session.sessionToken);
        }

        [SFFact]
        public void TestShouldContinueExecutionIfRenewingFails()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var session = CreateSessionWithCurrentStartTime(connectionString, new MockRestSessionExpired());
            session.startHeartBeatForThisSession();
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool.SetTimeout(0);
            pool._idleSessions.Add(session);
            session.sessionToken = MockRestSessionExpired.THROW_ERROR_TOKEN;

            // act
            try
            {
                pool.ExtractIdleSession(connectionString);
            }
            catch
            {
                Assert.Fail("Should not throw exception even if session renewal fails");
            }

            // assert
            Assert.NotEqual(MockRestSessionExpired.NEW_SESSION_TOKEN, session.sessionToken);
        }

        [SFFact]
        public void TestShouldNotRenewSessionIfKeepAliveIsDisabled()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var session = CreateSessionWithCurrentStartTime(connectionString, new MockRestSessionExpired());
            session.stopHeartBeatForThisSession();
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool.SetTimeout(0);
            pool._idleSessions.Add(session);

            // act
            pool.ExtractIdleSession(connectionString);

            // assert
            Assert.Null(session.sessionToken);
        }

        [SFFact]
        public void TestClearIdleSessionsShouldEvictAllSessionsEvenWhenCloseThrows()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var throwingRequester = new MockCloseSessionException();
            var session1 = CreateSessionWithCurrentStartTime(connectionString, throwingRequester);
            session1.sessionToken = "token1";
            var session2 = CreateSessionWithCurrentStartTime(connectionString, throwingRequester);
            session2.sessionToken = "token2";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool._idleSessions.Add(session1);
            pool._idleSessions.Add(session2);
            Assert.Equal(2, pool._idleSessions.Count);

            // act
            pool.ClearIdleSessions();

            // assert - pool must be empty even though close() threw for each session
            Assert.Equal(0, pool._idleSessions.Count);
        }

        [SFFact]
        public void TestClearIdleSessionsShouldCloseRemainingSessionsAfterOneCloseThrows()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var throwingRequester = new MockCloseSessionException();
            var normalRequester = new MockRestSessionExpired();
            var throwingSession = CreateSessionWithCurrentStartTime(connectionString, throwingRequester);
            throwingSession.sessionToken = "token_will_throw";
            var normalSession = CreateSessionWithCurrentStartTime(connectionString, normalRequester);
            normalSession.sessionToken = "token_normal";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool._idleSessions.Add(throwingSession);
            pool._idleSessions.Add(normalSession);

            // act
            pool.ClearIdleSessions();

            // assert - pool must be empty and second session's close should have been attempted
            Assert.Equal(0, pool._idleSessions.Count);
            Assert.Null(normalSession.sessionToken);
        }

        [SFFact]
        public void TestReturnSessionToPoolShouldRejectSessionAfter401QueryFailure()
        {
            // arrange — session with a mock that throws 401-tagged exception on query
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var mock401Requester = new MockRestRequesterUnauthorizedOnQuery();
            var session = CreateSessionWithCurrentStartTime(connectionString, mock401Requester);
            session.sessionToken = "valid_token";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);

            // act — execute a query through SFStatement (the real production code path)
            var statement = new SFStatement(session);
            Assert.ThrowsAny<Exception>(() =>
                statement.Execute(0, "SELECT 1", null, false, false));

            // the session should now be invalidated by SFStatement's catch block
            Assert.True(session.IsInvalidatedForPooling());

            // act — try to return the session to the pool
            var wasAdded = pool.AddSession(session, false);

            // assert — invalidated session must be rejected
            Assert.False(wasAdded);
            Assert.Equal(0, pool._idleSessions.Count);
        }

        [SFFact]
        public void TestReturnSessionToPoolShouldAcceptValidSession()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var session = CreateSessionWithCurrentStartTime(connectionString);
            session.sessionToken = "valid_token";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);

            // act
            var wasAdded = pool.AddSession(session, false);

            // assert - valid session should be returned to pool
            Assert.True(wasAdded);
            Assert.Equal(1, pool._idleSessions.Count);
        }

        [SFFact]
        public void TestDestroyPoolShouldSucceedEvenWhenSessionCloseThrows()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var throwingRequester = new MockCloseSessionException();
            var session = CreateSessionWithCurrentStartTime(connectionString, throwingRequester);
            session.sessionToken = "token_will_throw";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool._idleSessions.Add(session);

            // act & assert - DestroyPool should not throw even if session.close() fails
            pool.DestroyPool();
            Assert.Equal(0, pool._idleSessions.Count);
        }

        [SFFact]
        public void TestClearSessionsShouldSucceedEvenWhenSessionCloseThrows()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword;";
            var throwingRequester = new MockCloseSessionException();
            var session = CreateSessionWithCurrentStartTime(connectionString, throwingRequester);
            session.sessionToken = "token_will_throw";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            pool._idleSessions.Add(session);

            // act & assert - ClearSessions should not throw even if session.close() fails
            pool.ClearSessions();
            Assert.Equal(0, pool._idleSessions.Count);
        }

        [SFFact]
        public async Task TestCancelledGetSessionAsyncLeaksCreationTokens()
        {
            // arrange
            var cancelledToken = new CancellationToken(canceled: true);
            // minPoolSize=0 avoids background session creation that would interfere with the count
            var connectionString = new StringBuilder("ACCOUNT=testaccount;USER=testuser;password").Append("=testpwd;minPoolSize=0;").ToString();
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            const int Iterations = 5;
            for (var i = 0; i < Iterations; i++)
            {
                try
                {
                    await pool.GetSessionAsync(connectionString, new SessionPropertiesContext(), cancelledToken);
                }
                catch (OperationCanceledException)
                {
                }
            }

            AssertExtensions.Equal(0, pool.OngoingSessionCreationsCount(),
                $"Expected 0 leaked tokens after {Iterations} cancelled operations, " +
                $"but {pool.OngoingSessionCreationsCount()} token(s) remain. ");
        }

        private static SFSession CreateSessionWithCurrentStartTime(string connectionString, IMockRestRequester restRequester = null)
        {
            var session = restRequester == null ? new SFSession(connectionString, new SessionPropertiesContext()) :
                new SFSession(connectionString, new SessionPropertiesContext(), restRequester);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            session.SetStartTime(now);
            return session;
        }
    }
}
