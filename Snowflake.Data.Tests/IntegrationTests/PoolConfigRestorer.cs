using Snowflake.Data.Client;

namespace Snowflake.Data.Tests.IntegrationTests
{
    class PoolConfigRestorer {
        private readonly bool _pooling;
        private readonly long _timeout;
        private readonly int _maxPoolSize;

        public PoolConfigRestorer()
        {
            _maxPoolSize = SnowflakeDbConnectionPool.GetMaxPoolSize();
            _timeout = SnowflakeDbConnectionPool.GetTimeout();
            _pooling = SnowflakeDbConnectionPool.GetPooling();
        }

        public void Reset()
        {
            SnowflakeDbConnectionPool.SetMaxPoolSize(_maxPoolSize);
            SnowflakeDbConnectionPool.SetTimeout(_timeout);
            SnowflakeDbConnectionPool.SetPooling(_pooling);
        }
    }


}