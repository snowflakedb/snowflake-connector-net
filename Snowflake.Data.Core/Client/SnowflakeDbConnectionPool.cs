/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbConnectionPool
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeDbConnectionPool>();
        private static readonly IConnectionManager s_connectionManager = new ConnectionCacheManager();
        
        internal static SFSession GetSession(string connectionString, SecureString password)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetSession");
            return s_connectionManager.GetSession(connectionString, password);
        }
        
        internal static Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetSessionAsync");
            return s_connectionManager.GetSessionAsync(connectionString, password, cancellationToken);
        }
        
        internal static bool AddSession(SFSession session)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::AddSession");
            return s_connectionManager.AddSession(session);
        }

        public static void ClearAllPools()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            s_connectionManager.ClearAllPools();
        }

        public static void SetMaxPoolSize(int maxPoolSize)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetMaxPoolSize");
            s_connectionManager.SetMaxPoolSize(maxPoolSize);
        }

        public static int GetMaxPoolSize()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetMaxPoolSize");
            return s_connectionManager.GetMaxPoolSize();
        }

        public static void SetTimeout(long connectionTimeout)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetTimeout");
            s_connectionManager.SetTimeout(connectionTimeout);
        }
        
        public static long GetTimeout()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetTimeout");
            return s_connectionManager.GetTimeout();
        }

        public static int GetCurrentPoolSize()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetCurrentPoolSize");
            return s_connectionManager.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetPooling");
            return s_connectionManager.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetPooling");
            return s_connectionManager.GetPooling();
        }
    }
}
