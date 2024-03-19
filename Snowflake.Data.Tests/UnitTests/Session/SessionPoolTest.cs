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
    }
}
