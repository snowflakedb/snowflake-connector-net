using System;
using Xunit;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.IntegrationTests;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests
{
    public class ConnectionCacheManagerTest : IDisposable
    {
        private readonly ConnectionCacheManager _connectionCacheManager = new ConnectionCacheManager();
        private const string ConnectionString = "db=D1;warehouse=W1;account=A1;user=U1;password=P1;role=R1;minPoolSize=1;";

        public ConnectionCacheManagerTest()
        {
            ConnectionManagerTestsFacade.Init();
            ConnectionManagerTestsFacade.RegisterDedicatedContext(nameof(ConnectionCacheManagerTest), ConnectionPoolType.SingleConnectionCache);
        }

        [SFFact]
        public void TestEnablePoolingRegardlessOfConnectionStringProperty()
        {
            // act
            var pool = _connectionCacheManager.GetPool(ConnectionString + "poolingEnabled=false");

            // assert
            Assert.True(pool.GetPooling());
        }

        public void Dispose()
        {
            ConnectionManagerTestsFacade.UnregisterDedicatedContext(nameof(ConnectionCacheManagerTest));
        }
    }
}
