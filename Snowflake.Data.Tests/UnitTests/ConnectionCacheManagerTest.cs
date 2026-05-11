using System;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public sealed class ConnectionCacheManagerTestFixture : IDisposable
    {
        private readonly PoolConfig _poolConfig;

        public ConnectionCacheManagerTestFixture()
        {
            _poolConfig = new PoolConfig();
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
            SessionPool.SessionFactory = new MockSessionFactory();
        }

        public void Dispose()
        {
            _poolConfig.Reset();
            SessionPool.SessionFactory = new SessionFactory();
        }
    }

    public class ConnectionCacheManagerTest : IClassFixture<ConnectionCacheManagerTestFixture>
    {
        private readonly ConnectionCacheManager _connectionCacheManager = new ConnectionCacheManager();
        private const string ConnectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";

        public ConnectionCacheManagerTest(ConnectionCacheManagerTestFixture fixture)
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
