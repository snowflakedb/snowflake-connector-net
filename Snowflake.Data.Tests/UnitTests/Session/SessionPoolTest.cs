using System.Net;
using System.Security;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

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
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;private_key=SomePrivateKey;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;token=someToken;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;private_key_pwd=somePrivateKeyPwd;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("account=someAccount;db=someDb;host=someHost;password=somePassword;user=SomeUser;proxyPassword=someProxyPassword;port=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("ACCOUNT=someAccount;DB=someDb;HOST=someHost;PASSWORD=somePassword;USER=SomeUser;PORT=443", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        [TestCase("ACCOUNT=\"someAccount\";DB=\"someDb\";HOST=\"someHost\";PASSWORD=\"somePassword\";USER=\"SomeUser\";PORT=\"443\"", null, " [pool: account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443;]")]
        public void TestPoolIdentification(string connectionString, string password, string expectedPoolIdentification)
        {
            // arrange
            var securePassword = password == null ? null : new NetworkCredential("", password).SecurePassword;
            var pool = SessionPool.CreateSessionPool(connectionString, securePassword);

            // act
            var poolIdentification = pool.PoolIdentification();

            // assert
            Assert.AreEqual(expectedPoolIdentification, poolIdentification);
        }

        [Test]
        public void TestPoolIdentificationForInvalidConnectionString()
        {
            // arrange
            var invalidConnectionString = "account=someAccount;db=someDb;host=someHost;user=SomeUser;port=443";
            var pool = SessionPool.CreateSessionPool(invalidConnectionString, null);

            // act
            var poolIdentification = pool.PoolIdentification();

            // assert
            Assert.AreEqual(" [pool: could not parse connection string]", poolIdentification);
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
    }
}
