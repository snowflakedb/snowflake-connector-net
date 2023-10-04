using System;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Core.ConnectionPool;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbConnectionPool
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeDbConnectionPool>();
        private static readonly Object s_instanceLock = new Object();
        private static ConnectionPoolManagerBase s_connectionPoolManager;
        private static PoolManagerVersion s_poolVersion = PoolManagerVersion.Version2;
        
        public static ConnectionPoolManagerBase Instance
        {
            get
            {
                if (s_connectionPoolManager != null)
                    return s_connectionPoolManager;
                lock (s_instanceLock)
                {
                    s_connectionPoolManager = ProvideConnectionPoolManager();
                }
                return s_connectionPoolManager;
            }
        }
        
        public static SessionPool GetPool(string connectionString, SecureString password)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetSession");
            return Instance.GetPool(connectionString, password);
        }

        public static void ClearAllPools()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            Instance.ClearAllPools();
        }

        public static void SetMaxPoolSize(int size)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetMaxPoolSize");
            Instance.SetMaxPoolSize(size);
        }

        public static int GetMaxPoolSize()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetMaxPoolSize");
            return Instance.GetMaxPoolSize();
        }

        public static void SetTimeout(long time)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetTimeout");
            Instance.SetTimeout(time);
        }
        
        public static long GetTimeout()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetTimeout");
            return Instance.GetTimeout();
        }

        public static int GetCurrentPoolSize()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetCurrentPoolSize");
            return Instance.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetPooling");
            return Instance.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetPooling");
            return Instance.GetPooling();
        }

        private static ConnectionPoolManagerBase ProvideConnectionPoolManager()
        {
            switch (s_poolVersion)
            {
                case PoolManagerVersion.Version1: return new ConnectionPoolManagerV1();
                case PoolManagerVersion.Version2: return new ConnectionPoolManagerV2();
                default: throw new NotSupportedException("Pool version not supported");
            }
        }

        internal static SFSession GetSession(string connectionString, SecureString password)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetSession");
            return Instance.GetSession(connectionString, password);
        }
        
        internal static Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetSessionAsync");
            return Instance.GetSessionAsync(connectionString, password, cancellationToken);
        }
        
        internal static bool AddSession(string connectionString, SecureString password, SFSession session)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::AddSession");
            return Instance.AddSession(connectionString, password, session);
        }

        public static void InternalTogglePreviousPool()
        {
            s_logger.Debug("ClearAllPools");
            if (Instance.GetCurrentPoolSize() > 0)
                throw new SnowflakeDbException(SFError.INTERNAL_ERROR, "Switch pool version before connections are established!");
            ClearAllPools();
            lock (s_instanceLock)
            {
                s_poolVersion = PoolManagerVersion.Version1;
                s_connectionPoolManager = ProvideConnectionPoolManager();
            }
        }
    }
}
