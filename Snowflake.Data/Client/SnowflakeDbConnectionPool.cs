using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Security;
using Snowflake.Data.Log;
using System.Linq;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbConnectionPool
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        private static List<SnowflakeDbConnection> connectionPool;
        private static readonly object _connectionPoolLock = new object();
        private static int maxPoolSize;
        private static long timeout;
        private const int MAX_POOL_SIZE = 10;
        private const long TIMEOUT = 3600;
        private static bool pooling = true;

        private static void initConnectionPool()
        {
            logger.Debug("SnowflakeDbConnectionPool::initConnectionPool");
            lock (_connectionPoolLock)
            {
                connectionPool = new List<SnowflakeDbConnection>();
                maxPoolSize = MAX_POOL_SIZE;
                timeout = TIMEOUT;
            }
        }

        private static void cleanExpiredConnections()
        {
            logger.Debug("SnowflakeDbConnectionPool::cleanExpiredConnections");
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            lock (_connectionPoolLock)
            {
                long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();

                foreach (var item in connectionPool.ToList())
                {
                    if(item._poolTimeout <= timeNow)
                    {
                        connectionPool.Remove(item);
                        if(item.SfSession != null)
                        {
                            item.SfSession.close();
                        }
                    }
                }
            }
        }

        internal static SnowflakeDbConnection getConnection(string connStr)
        {
            logger.Debug("SnowflakeDbConnectionPool::getConnection");
            if (!pooling)
                return null;
            if (connectionPool == null)
            {
                initConnectionPool();
            }
            lock (_connectionPoolLock)
            {
                for (int i = 0; i < connectionPool.Count; i++)
                {
                    if (connectionPool[i].ConnectionString.Equals(connStr))
                    {
                        SnowflakeDbConnection conn = connectionPool[i];
                        connectionPool.RemoveAt(i);
                        return conn;
                    }
                }
            }

            return null;
        }

        internal static bool addConnection(SnowflakeDbConnection conn)
        {
            logger.Debug("SnowflakeDbConnectionPool::addConnection");
            if (!pooling)
                return false;
            lock (_connectionPoolLock)
            {
                if (connectionPool == null)
                {
                    initConnectionPool();
                }
                if (connectionPool.Count < maxPoolSize)
                {
                    long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();
                    conn._poolTimeout = timeNow + timeout;
                    connectionPool.Add(conn);
                    return true;
                }
                else
                {
                    cleanExpiredConnections();
                    if (connectionPool.Count < maxPoolSize)
                    {
                        long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();
                        conn._poolTimeout = timeNow + timeout;
                        connectionPool.Add(conn);
                        return true;
                    }
                }
            }
            return false;
        }

        public static void ClearAllPools()
        {
            logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            lock (_connectionPoolLock)
            {
                connectionPool.Clear();
            }
        }

        public static void SetMaxPoolSize(int size)
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }
            maxPoolSize = size;
        }

        public static void SetTimeout(long time)
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }
            timeout = time;
        }

        public static int GetCurrentPoolSize()
        {
            return connectionPool.Count;
        }

        public static void SetPooling(bool isEnable)
        {
            pooling = isEnable;
        }

        public static bool GetPooling()
        {
            return pooling;
        }
    }
}
