using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Concurrent;
using System.Security;
using Snowflake.Data.Log;
using System.Linq;

namespace Snowflake.Data.Client
{
    sealed class ConnectionPoolSingleton : IDisposable
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();
        private static ConnectionPoolSingleton instance = null;
        private static readonly object _connectionPoolLock = new object();

        private List<SnowflakeDbConnection> connectionPool;
        private int maxPoolSize;
        private long timeout;
        private int MAX_POOL_SIZE = 10;
        private const long TIMEOUT = 3600;
        private bool pooling = false;// temporarily desiable connection pooling

        ConnectionPoolSingleton()
        {
            lock (_connectionPoolLock)
            {
                connectionPool = new List<SnowflakeDbConnection>();
                maxPoolSize = MAX_POOL_SIZE;
                timeout = TIMEOUT;
            }
        }
        ~ConnectionPoolSingleton()
        {
            ClearAllPools();
        }

        public void Dispose()
        {
            ClearAllPools();
        }

        public static ConnectionPoolSingleton Instance
        {
            get
            {
                lock (_connectionPoolLock)
                {
                    if(instance == null)
                    {
                        instance = new ConnectionPoolSingleton();
                    }
                    return instance;
                }
            }
        }

        private void cleanExpiredConnections()
        {
            logger.Debug("ConnectionPool::cleanExpiredConnections");
            lock (_connectionPoolLock)
            {
                long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();

                foreach (var item in connectionPool.ToList())
                {
                    if (item._poolTimeout <= timeNow)
                    {
                        connectionPool.Remove(item);
                        if (item.SfSession != null)
                        {
                            item.SfSession.close();
                        }
                    }
                }
            }
        }

        internal SnowflakeDbConnection getConnection(string connStr)
        {
            logger.Debug("ConnectionPool::getConnection");
            if (!pooling)
                return null;
            lock (_connectionPoolLock)
            {
                for (int i = 0; i < connectionPool.Count; i++)
                {
                    if (connectionPool[i].ConnectionString.Equals(connStr))
                    {
                        SnowflakeDbConnection conn = connectionPool[i];
                        connectionPool.RemoveAt(i);
                        long timeNow = DateTimeOffset.Now.ToUnixTimeSeconds();
                        if (conn._poolTimeout <= timeNow)
                        {
                            if (conn.SfSession != null)
                            {
                                conn.SfSession.close();
                            }
                            i--;
                        }
                        else
                        {
                            return conn;
                        }
                    }
                }
            }
            return null;
        }
        internal bool addConnection(SnowflakeDbConnection conn)
        {
            logger.Debug("ConnectionPool::addConnection");
            if (!pooling)
                return false;
            lock (_connectionPoolLock)
            {
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

        internal void ClearAllPools()
        {
            logger.Debug("ConnectionPool::ClearAllPools");
            lock (_connectionPoolLock)
            {
                foreach (SnowflakeDbConnection conn in connectionPool)
                {
                    conn.PostClose();
                }
                connectionPool.Clear();
            }
        }

        public void SetMaxPoolSize(int size)
        {
            maxPoolSize = size;
        }

        public void SetTimeout(long time)
        {
            timeout = time;
        }

        public int GetCurrentPoolSize()
        {
            return connectionPool.Count;
        }

        public bool SetPooling(bool isEnable)
        {
            // temporarily disable connection pooling
            // as it's buggy and needs refactoring.
            return false;
            /*
            if (pooling == isEnable)
                return false;
            pooling = isEnable;
            if (!pooling)
            {
                ClearAllPools();
            }
            return true;
            */
        }

        public bool GetPooling()
        {
            return pooling;
        }
    }
    public class SnowflakeDbConnectionPool
    {
        private static SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeDbConnection>();

        internal static SnowflakeDbConnection getConnection(string connStr)
        {
            logger.Debug("SnowflakeDbConnectionPool::getConnection");
            return ConnectionPoolSingleton.Instance.getConnection(connStr);
        }

        internal static bool addConnection(SnowflakeDbConnection conn)
        {
            logger.Debug("SnowflakeDbConnectionPool::addConnection");
            return ConnectionPoolSingleton.Instance.addConnection(conn);
        }

        public static void ClearAllPools()
        {
            logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            ConnectionPoolSingleton.Instance.ClearAllPools();
        }

        public static void SetMaxPoolSize(int size)
        {
            ConnectionPoolSingleton.Instance.SetMaxPoolSize(size);
        }

        public static void SetTimeout(long time)
        {
            ConnectionPoolSingleton.Instance.SetTimeout(time);
        }

        public static int GetCurrentPoolSize()
        {
            return ConnectionPoolSingleton.Instance.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            return ConnectionPoolSingleton.Instance.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            return ConnectionPoolSingleton.Instance.GetPooling();
        }
    }
}
