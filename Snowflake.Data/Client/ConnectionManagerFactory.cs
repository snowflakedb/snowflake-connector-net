using System;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client;

internal interface IConnectionManagerFactory
{
    IConnectionManager CreateConnectionManager(ConnectionPoolType requestedPoolType);
}

internal class ConnectionManagerFactory : IConnectionManagerFactory
{
    public static ConnectionManagerFactory Singleton { get; } = new();

    private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionManagerFactory>();

    public IConnectionManager CreateConnectionManager(ConnectionPoolType requestedPoolType)
    {
        IConnectionManager result;
        switch (requestedPoolType)
        {
            case ConnectionPoolType.MultipleConnectionPool:
                result = new ConnectionPoolManager();
                s_logger.Info("SnowflakeDbConnectionPool - multiple connection pools enabled");
                break;
            case ConnectionPoolType.SingleConnectionCache:
                result = new ConnectionCacheManager();
                s_logger.Warn("SnowflakeDbConnectionPool - connection cache enabled");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(requestedPoolType), requestedPoolType, null);
        }

        return result;
    }
}
