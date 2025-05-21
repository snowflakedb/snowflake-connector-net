using Snowflake.Data.Client;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.Util
{
    class PoolConfig
    {
        private readonly bool _pooling;
        private readonly long _timeout;
        private readonly int _maxPoolSize;
        private readonly ConnectionPoolType _connectionPoolType;

        public PoolConfig()
        {
            _maxPoolSize = SFSessionHttpClientProperties.DefaultMaxPoolSize;
            _timeout = (long)SFSessionHttpClientProperties.DefaultExpirationTimeout.TotalSeconds;
            _pooling = SFSessionHttpClientProperties.DefaultPoolingEnabled;
            _connectionPoolType = SnowflakeDbConnectionPool.DefaultConnectionPoolType;
        }

        public void Reset()
        {
            SnowflakeDbConnectionPool.ForceConnectionPoolVersion(_connectionPoolType);
            if (_connectionPoolType == ConnectionPoolType.MultipleConnectionPool)
                return; // for multiple connection pool setting parameters for all the pools doesn't work by design
            SnowflakeDbConnectionPool.SetMaxPoolSize(_maxPoolSize);
            SnowflakeDbConnectionPool.SetTimeout(_timeout);
            SnowflakeDbConnectionPool.SetPooling(_pooling);
        }
    }
}
