using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Security;

namespace Snowflake.Data.Client
{
    class SnowflakeDbConnectionPool
    {
        private static ConcurrentDictionary<string, SnowflakeDbConnection> connectionPool;
        private static readonly object _connectionPoolLock = new object();
        private static int minPoolSize;
        private static int maxPoolSize;
        private static int cleanupCounter;
        private static int currentCounter;
        private const int MIN_POOL_SIZE = 0;
        private const int MAX_POOL_SIZE = 10;
        private const int CLEANUP_COUNTER = 0;

        private static void initConnectionPool()
        {
            lock (_connectionPoolLock)
            {
                connectionPool = new ConcurrentDictionary<string, SnowflakeDbConnection>();
                minPoolSize = MIN_POOL_SIZE;
                maxPoolSize = MAX_POOL_SIZE;
                cleanupCounter = CLEANUP_COUNTER;
                currentCounter = 0;
            }
        }

        private static void cleanNonActiveConnections()
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            lock (_connectionPoolLock)
            {
                List<string> keys = new List<string>(connectionPool.Keys);
                int curSize = keys.Count;
                foreach (var item in keys)
                {
                    SnowflakeDbConnection conn;
                    connectionPool.TryGetValue(item, out conn);
                    if (conn._refCount <= 0 && curSize > minPoolSize)
                    {
                        connectionPool.TryRemove(item, out conn);
                        conn.CloseConnection();
                        conn.DisposeConnection(false);
                        curSize--;
                    }
                }
            }
        }

        internal static SnowflakeDbConnection getConnection(string connStr, SecureString pw)
        {
            if (connectionPool == null)
            {
                initConnectionPool();
            }
            if (connectionPool.ContainsKey(connStr))
            {
                SnowflakeDbConnection conn;
                connectionPool.TryGetValue(connStr, out conn);
                return conn;
            }
            return null;
        }

        internal static bool addConnection(SnowflakeDbConnection conn)
        {
            lock (_connectionPoolLock)
            {
                SnowflakeDbConnection poolConn;
                connectionPool.TryGetValue(conn.ConnectionString, out poolConn);

                if (poolConn != null)
                {
                    poolConn._refCount++;
                    return false;
                }

                if (connectionPool.Count >= maxPoolSize)
                {
                    if (currentCounter > 0)
                    {
                        currentCounter--;
                        return false;
                    }
                    else
                    {
                        currentCounter = cleanupCounter;
                        cleanNonActiveConnections();
                        if (connectionPool.Count >= maxPoolSize)
                        {
                            return false;
                        }
                    }
                }

                connectionPool.TryAdd(conn.ConnectionString, conn);
                return true;
            }
        }

        public static void ClearAllPools()
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            lock (_connectionPoolLock)
            {
                List<string> keys = new List<string>(connectionPool.Keys);
                foreach (var item in keys)
                {
                    SnowflakeDbConnection conn;
                    connectionPool.TryGetValue(item, out conn);
                    connectionPool.TryRemove(item, out conn);
                    conn._refCount = 0;
                    conn.CloseConnection();
                    conn.DisposeConnection(false);
                }
            }
        }

        public static bool ClearPool(SnowflakeDbConnection conn)
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            lock (_connectionPoolLock)
            {
                if (connectionPool.ContainsKey(conn.ConnectionString))
                {
                    connectionPool.TryRemove(conn.ConnectionString, out conn);
                    conn._refCount = 0;
                    conn.CloseConnection();
                    conn.DisposeConnection(false);
                    return true;
                }
            }
            return false;
        }

        public static void SetMinPoolSize(int size)
        {
            minPoolSize = size;
        }

        public static void SetMaxPoolSize(int size)
        {
            maxPoolSize = size;
        }

        public static void SetCleanupCounter(int size)
        {
            cleanupCounter = size;
        }
    }
}
