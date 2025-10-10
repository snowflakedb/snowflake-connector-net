using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Mock;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SessionPoolTest
    {
        private const string ConnectionString = "ACCOUNT=testaccount;USER=testuser;PASSWORD=testpassword;";

        [Test]
        public void TestPoolParametersAreNotOverriden()
        {
            // act
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);

            // assert
            Assert.IsFalse(pool.IsConfigOverridden());
        }

        [Test]
        public void TestOverrideMaxPoolSize()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);
            var newMaxPoolSize = 15;

            // act
            pool.SetMaxPoolSize(newMaxPoolSize);

            // assert
            Assert.AreEqual(newMaxPoolSize, pool.GetMaxPoolSize());
            Assert.IsTrue(pool.IsConfigOverridden());
        }

        [Test]
        public void TestOverrideExpirationTimeout()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);
            var newExpirationTimeoutSeconds = 15;

            // act
            pool.SetTimeout(newExpirationTimeoutSeconds);

            // assert
            Assert.AreEqual(newExpirationTimeoutSeconds, pool.GetTimeout());
            Assert.IsTrue(pool.IsConfigOverridden());
        }

        [Test]
        public void TestOverrideSetPooling()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, null);

            // act
            pool.SetPooling(false);

            // assert
            Assert.IsFalse(pool.GetPooling());
            Assert.IsTrue(pool.IsConfigOverridden());
        }

        [Test]
        [TestCase("account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443", "somePassword", "someSecret", "someToken", " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;private_key=SomePrivateKey;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;token=someToken;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;private_key_pwd=somePrivateKeyPwd;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;proxyPassword=someProxyPassword;port=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("ACCOUNT=someAccount;DB=someDb;HOST=someHost;PASSWORD=somePassword;passcode=123;USER=SomeUser;PORT=443", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("ACCOUNT=\"someAccount\";DB=\"someDb\";HOST=\"someHost\";PASSWORD=\"somePassword\";PASSCODE=\"123\";USER=\"SomeUser\";PORT=\"443\"", null, null, null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
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
            Assert.AreEqual(expectedPoolIdentification, poolIdentification);
        }

        [Test]
        public void TestRetrievePoolFailureForInvalidConnectionString()
        {
            // arrange
            var invalidConnectionString = "account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443"; // invalid because password is not provided

            // act
            var exception = Assert.Throws<SnowflakeDbException>(() => SessionPool.CreateSessionPool(invalidConnectionString, null, null, null));

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.MISSING_CONNECTION_PROPERTY);
            Assert.IsTrue(exception.Message.Contains("Required property PASSWORD is not provided"));
        }

        [Test]
        public void TestPoolIdentificationBasedOnInternalId()
        {
            // arrange
            var connectionString = "account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;port=443";
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            var poolIdRegex = new Regex(@"^ \[pool: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\]$");

            // act
            var poolIdentification = pool.PoolIdentificationBasedOnInternalId;

            // assert
            Assert.IsTrue(poolIdRegex.IsMatch(poolIdentification));
        }

        [Test]
        public void TestPoolIdentificationForOldPool()
        {
            // arrange
            var pool = SessionPool.CreateSessionCache();

            // act
            var poolIdentification = pool.PoolIdentification();

            // assert
            Assert.AreEqual("", poolIdentification);
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("anyPassword")]
        public void TestValidateValidSecurePassword(string password)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var pool = SessionPool.CreateSessionPool(ConnectionString, securePassword, null, null);

            // act
            Assert.DoesNotThrow(() => pool.ValidateSecurePassword(securePassword));
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("anySecret")]
        public void TestValidateValidSecureClientCredentials(string oauthClientSecret)
        {
            // arrange
            var secureOAuthClientSecret = oauthClientSecret == null ? null : SecureStringHelper.Encode(oauthClientSecret);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, secureOAuthClientSecret, null);

            // act
            Assert.DoesNotThrow(() => pool.ValidateSecureOAuthClientSecret(secureOAuthClientSecret));
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("anySecret")]
        public void TestValidateValidSecureToken(string token)
        {
            // arrange
            var secureToken = token == null ? null : SecureStringHelper.Encode(token);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, secureToken);

            // act
            Assert.DoesNotThrow(() => pool.ValidateSecureToken(secureToken));
        }

        [Test]
        [TestCase("somePassword", null)]
        [TestCase("somePassword", "")]
        [TestCase("somePassword", "anotherPassword")]
        [TestCase("", "anotherPassword")]
        [TestCase(null, "anotherPassword")]
        public void TestFailToValidateNotMatchingSecurePassword(string poolPassword, string notMatchingPassword)
        {
            // arrange
            var poolSecurePassword = poolPassword == null ? null : SecureStringHelper.Encode(poolPassword);
            var notMatchingSecurePassword = notMatchingPassword == null ? null : SecureStringHelper.Encode(notMatchingPassword);
            var pool = SessionPool.CreateSessionPool(ConnectionString, poolSecurePassword, null, null);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecurePassword(notMatchingSecurePassword));

            // assert
            Assert.That(thrown.Message, Does.Contain("Could not get a pool because of password mismatch"));
        }

        [Test]
        [TestCase("someSecret", null)]
        [TestCase("someSecret", "")]
        [TestCase("someSecret", "anotherSecret")]
        [TestCase("", "anotherSecret")]
        [TestCase(null, "anotherSecret")]
        public void TestFailToValidateNotMatchingSecureClientCredentials(string poolOAuthClientSecret, string notMatchingOAuthClientSecret)
        {
            // arrange
            var poolSecureOAuthClientSecret = poolOAuthClientSecret == null ? null : SecureStringHelper.Encode(poolOAuthClientSecret);
            var notMatchingSecureOAuthClientSecret = notMatchingOAuthClientSecret == null ? null : SecureStringHelper.Encode(notMatchingOAuthClientSecret);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, poolSecureOAuthClientSecret, null);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecureOAuthClientSecret(notMatchingSecureOAuthClientSecret));

            // assert
            Assert.That(thrown.Message, Does.Contain("Could not get a pool because of oauth client secret mismatch"));
        }

        [Test]
        [TestCase("someToken", null)]
        [TestCase("someToken", "")]
        [TestCase("someToken", "anotherToken")]
        [TestCase("", "anotherToken")]
        [TestCase(null, "anotherToken")]
        public void TestFailToValidateNotMatchingSecureToken(string poolToken, string notMatchingToken)
        {
            // arrange
            var poolSecureToken = poolToken == null ? null : SecureStringHelper.Encode(poolToken);
            var notMatchingSecureToken = notMatchingToken == null ? null : SecureStringHelper.Encode(notMatchingToken);
            var pool = SessionPool.CreateSessionPool(ConnectionString, null, null, poolSecureToken);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecureToken(notMatchingSecureToken));

            // assert
            Assert.That(thrown.Message, Does.Contain("Could not get a pool because of token mismatch"));
        }

        [Test]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=true;", true)]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=false;", false)]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;", false)]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;", false)]
        public void TestConnectionPoolPoolingForOAuthAuthorizationCode(string connectionString, bool expectedPoolingEnabled)
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            var session = CreateSessionWithCurrentStartTime(connectionString);

            // assert
            Assert.AreEqual(expectedPoolingEnabled, pool.GetPooling());

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.AreEqual(expectedPoolingEnabled, isSessionReturnedToPool);
        }

        [Test]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=true;")]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;poolingEnabled=false;")]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;user=testUser;")]
        [TestCase("authenticator=oauth_authorization_code;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;")]
        [TestCase("authenticator=oauth_client_credentials;account=test;role=ANALYST;oauthClientId=abc;oauthClientSecret=def;oauthTokenRequestUrl=https://okta.com/token-request;")]
        [TestCase("authenticator=programmatic_access_token;account=test;token=patToken")]
        public void TestConnectionCachePoolingDisabledForNewAuthenticators(string connectionString)
        {
            // arrange
            var session = CreateSessionWithCurrentStartTime(connectionString);
            var pool = SessionPool.CreateSessionCache();

            // assert
            Assert.AreEqual(true, pool.GetPooling()); // for the old connection cache pooling is always enabled

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.IsFalse(isSessionReturnedToPool);
        }

        [Test]
        public void TestShouldClearQueryContextCacheOnReturningToConnectionCache()
        {
            // arrange
            var session = CreateSessionWithCurrentStartTime("account=testAccount;user=testUser;password=testPassword");
            var pool = SessionPool.CreateSessionCache();
            var contextElement = new QueryContextElement(123, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 1, "context");
            var context = new ResponseQueryContext { Entries = new List<ResponseQueryContextElement> { new(contextElement) } };
            session.UpdateQueryContextCache(context);
            Assert.AreEqual(1, session.GetQueryContextRequest().Entries.Count);

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.IsTrue(isSessionReturnedToPool);
            Assert.AreEqual(0, session.GetQueryContextRequest().Entries.Count);
        }

        [Test]
        public void TestShouldClearQueryContextCacheOnReturningToConnectionPool()
        {
            // arrange
            var connectionString = "account=testAccount;user=testUser;password=testPassword";
            var session = CreateSessionWithCurrentStartTime(connectionString);
            var pool = SessionPool.CreateSessionPool(connectionString, null, null, null);
            var contextElement = new QueryContextElement(123, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 1, "context");
            var context = new ResponseQueryContext { Entries = new List<ResponseQueryContextElement> { new(contextElement) } };
            session.UpdateQueryContextCache(context);
            Assert.AreEqual(1, session.GetQueryContextRequest().Entries.Count);

            // act
            var isSessionReturnedToPool = pool.AddSession(session, false);

            // assert
            Assert.IsTrue(isSessionReturnedToPool);
            Assert.AreEqual(0, session.GetQueryContextRequest().Entries.Count);
        }

        [Test]
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
            Assert.AreEqual(MockRestSessionExpired.NEW_SESSION_TOKEN, session.sessionToken);
        }

        [Test]
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
            Assert.IsNull(session.sessionToken);
        }

        private SFSession CreateSessionWithCurrentStartTime(string connectionString, IMockRestRequester restRequester = null)
        {
            var session = restRequester == null ? new SFSession(connectionString, new SessionPropertiesContext()) :
                new SFSession(connectionString, new SessionPropertiesContext(), restRequester);
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            session.SetStartTime(now);
            return session;
        }
    }
}
