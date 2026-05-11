using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class ConnectionCacheManagerTest
    {
        private readonly ConnectionCacheManager _connectionCacheManager = new ConnectionCacheManager();
        private const string ConnectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";
        private static PoolConfig s_poolConfig;
        public static void BeforeAllTests()
        {
            s_poolConfig = new PoolConfig();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SessionPool.SessionFactory = new MockSessionFactory();
        }
        public static void AfterAllTests()
        {
            s_poolConfig.Reset();
            SessionPool.SessionFactory = new SessionFactory();
        }
        public void BeforeEach()
        {
            _connectionCacheManager.ClearAllPools();
        }

        [Fact]
        public void TestEnablePoolingRegardlessOfConnectionStringProperty()
        {
            // act
            var pool = _connectionCacheManager.GetPool(ConnectionString + "poolingEnabled=false");

            // assert
            Assert.True(pool.GetPooling());
        }
    }
}
