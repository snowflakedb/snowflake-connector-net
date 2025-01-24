using NUnit.Framework;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    [TestFixture, NonParallelizable]
    public class ConnectionCacheManagerTest
    {
        private readonly ConnectionCacheManager _connectionCacheManager = new ConnectionCacheManager();
        private const string ConnectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";
        private static PoolConfig s_poolConfig;

        [OneTimeSetUp]
        public static void BeforeAllTests()
        {
            s_poolConfig = new PoolConfig();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SessionPool.SessionFactory = new MockSessionFactory();
        }

        [OneTimeTearDown]
        public static void AfterAllTests()
        {
            s_poolConfig.Reset();
            SessionPool.SessionFactory = new SessionFactory();
        }

        [SetUp]
        public void BeforeEach()
        {
            _connectionCacheManager.ClearAllPools();
        }

        [Test]
        public void TestEnablePoolingRegardlessOfConnectionStringProperty()
        {
            // act
            var pool = _connectionCacheManager.GetPool(ConnectionString + "poolingEnabled=false");

            // assert
            Assert.IsTrue(pool.GetPooling());
        }
    }
}
