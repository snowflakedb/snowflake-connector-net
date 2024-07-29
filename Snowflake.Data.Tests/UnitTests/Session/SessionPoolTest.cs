using System;
using System.Net;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
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
            var pool = SessionPool.CreateSessionPool(ConnectionString, null);

            // assert
            Assert.IsFalse(pool.IsConfigOverridden());
        }

        [Test]
        public void TestOverrideMaxPoolSize()
        {
            // arrange
            var pool = SessionPool.CreateSessionPool(ConnectionString, null);
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
            var pool = SessionPool.CreateSessionPool(ConnectionString, null);
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
            var pool = SessionPool.CreateSessionPool(ConnectionString, null);

            // act
            pool.SetPooling(false);

            // assert
            Assert.IsFalse(pool.GetPooling());
            Assert.IsTrue(pool.IsConfigOverridden());
        }

        [Test]
        [TestCase("account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443", "somePassword", " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;private_key=SomePrivateKey;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;token=someToken;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;private_key_pwd=somePrivateKeyPwd;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;passcode=123;user=SomeUser;proxyPassword=someProxyPassword;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("ACCOUNT=someAccount;DB=someDb;HOST=someHost;PASSWORD=somePassword;passcode=123;USER=SomeUser;PORT=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("ACCOUNT=\"someAccount\";DB=\"someDb\";HOST=\"someHost\";PASSWORD=\"somePassword\";PASSCODE=\"123\";USER=\"SomeUser\";PORT=\"443\"", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        public void TestPoolIdentificationBasedOnConnectionString(string connectionString, string password, string expectedPoolIdentification)
        {
            // arrange
            var securePassword = password == null ? null : SecureStringHelper.Encode(password);
            var pool = SessionPool.CreateSessionPool(connectionString, securePassword);

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
            var exception = Assert.Throws<SnowflakeDbException>(() => SessionPool.CreateSessionPool(invalidConnectionString, null));

            // assert
            SnowflakeDbExceptionAssert.HasErrorCode(exception, SFError.MISSING_CONNECTION_PROPERTY);
            Assert.IsTrue(exception.Message.Contains("Required property PASSWORD is not provided"));
        }

        [Test]
        public void TestPoolIdentificationBasedOnInternalId()
        {
            // arrange
            var connectionString = "account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;port=443";
            var pool = SessionPool.CreateSessionPool(connectionString, null);
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
            var pool = SessionPool.CreateSessionPool(ConnectionString, securePassword);

            // act
            Assert.DoesNotThrow(() => pool.ValidateSecurePassword(securePassword));
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
            var pool = SessionPool.CreateSessionPool(ConnectionString, poolSecurePassword);

            // act
            var thrown = Assert.Throws<Exception>(() => pool.ValidateSecurePassword(notMatchingSecurePassword));

            // assert
            Assert.That(thrown.Message, Does.Contain("Could not get a pool because of password mismatch"));
        }
    }
}
