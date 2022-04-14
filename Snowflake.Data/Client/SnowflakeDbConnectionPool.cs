using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Security;

namespace Snowflake.Data.Client
{
    class SnowflakeDbConnectionPool
    {
        private static ConcurrentDictionary<Tuple<string, SecureString>, SnowflakeDbConnection> connectionPool;
        private static int minPoolSize;
        private static int maxPoolSize;
        private static int cleanupCounter;
        private static int currentCounter;
        private const int MIN_POOL_SIZE = 0;
        private const int MAX_POOL_SIZE = 10;
        private const int CLEANUP_COUNTER = 0;

        private static void initConnectionPool()
        {
            connectionPool = new ConcurrentDictionary<Tuple<string, SecureString>, SnowflakeDbConnection>();
            minPoolSize = MIN_POOL_SIZE;
            maxPoolSize = MAX_POOL_SIZE;
            cleanupCounter = CLEANUP_COUNTER;
            currentCounter = 0;
        }

        private static void cleanNonActiveConnections()
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            List<Tuple<string, SecureString>> keys = new List<Tuple<string, SecureString>>(connectionPool.Keys);
            int curSize = keys.Count;
            foreach (var item in keys)
            {
                SnowflakeDbConnection conn;
                connectionPool.TryGetValue(item, out conn);
                if (!conn.isActive && curSize > minPoolSize)
                {
                    connectionPool.TryRemove(item, out conn);
                    conn.CloseConnection();
                    conn.DisposeConnection(false);
                    curSize--;
                }
            }
        }

        internal static SnowflakeDbConnection getConnection(string connStr, SecureString pw)
        {
            var connKey = Tuple.Create(connStr, pw);
            if (connectionPool == null)
            {
                initConnectionPool();
            }
            if (connectionPool.ContainsKey(connKey))
            {
                SnowflakeDbConnection conn;
                connectionPool.TryGetValue(connKey, out conn);
                return conn;
            }
            return null;
        }

        internal static bool addConnection(SnowflakeDbConnection conn)
        {
            var connKey = Tuple.Create(conn.ConnectionString, conn.Password);
            if (connectionPool.ContainsKey(connKey))
            {
                conn.isActive = true;
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

            conn.isActive = true;
            connectionPool.TryAdd(connKey, conn);
            return true;
        }

        public static void ClearAllPools()
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            List<Tuple<string, SecureString>> keys = new List<Tuple<string, SecureString>>(connectionPool.Keys);
            foreach (var item in keys)
            {
                SnowflakeDbConnection conn;
                connectionPool.TryGetValue(item, out conn);
                connectionPool.TryRemove(item, out conn);
                conn.isActive = false;
                conn.CloseConnection();
                conn.DisposeConnection(false);
            }
        }

        public static bool ClearPool(SnowflakeDbConnection conn)
        {
            if (null == connectionPool)
            {
                initConnectionPool();
            }

            var connKey = Tuple.Create(conn.ConnectionString, conn.Password);
            if (connectionPool.ContainsKey(connKey))
            {
                connectionPool.TryRemove(connKey, out conn);
                conn.isActive = false;
                conn.CloseConnection();
                conn.DisposeConnection(false);
                return true;
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
