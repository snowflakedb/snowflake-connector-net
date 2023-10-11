/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

using Snowflake.Data.Client;
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
            _maxPoolSize = SnowflakeDbConnectionPool.GetMaxPoolSize();
            _timeout = SnowflakeDbConnectionPool.GetTimeout();
            _pooling = SnowflakeDbConnectionPool.GetPooling();
            _connectionPoolType = SnowflakeDbConnectionPool.GetConnectionPoolVersion();
        }

        public void Reset()
        {
            SnowflakeDbConnectionPool.SetConnectionPoolVersion(_connectionPoolType);
            SnowflakeDbConnectionPool.SetMaxPoolSize(_maxPoolSize);
            SnowflakeDbConnectionPool.SetTimeout(_timeout);
            SnowflakeDbConnectionPool.SetPooling(_pooling);
        }
    }
}
