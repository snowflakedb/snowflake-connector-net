/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbConnectionPool
    {
        private static readonly SFLoggerPair s_loggerPair = SFLoggerPair.GetLoggerPair<SnowflakeDbConnectionPool>();
        private static readonly Object s_connectionManagerInstanceLock = new Object();
        private static IConnectionManager s_connectionManager;
        internal const ConnectionPoolType DefaultConnectionPoolType = ConnectionPoolType.MultipleConnectionPool;

        private static IConnectionManager ConnectionManager
        {
            get
            {
                if (s_connectionManager != null)
                    return s_connectionManager;
                SetConnectionPoolVersion(DefaultConnectionPoolType, false);
                return s_connectionManager;
            }
        }

        internal static SFSession GetSession(string connectionString, SecureString password)
        {
            s_loggerPair.LogDebug($"SnowflakeDbConnectionPool::GetSession");
            return ConnectionManager.GetSession(connectionString, password);
        }

        internal static Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            s_loggerPair.LogDebug($"SnowflakeDbConnectionPool::GetSessionAsync");
            return ConnectionManager.GetSessionAsync(connectionString, password, cancellationToken);
        }

        public static SnowflakeDbSessionPool GetPool(string connectionString, SecureString password)
        {
            s_loggerPair.LogDebug($"SnowflakeDbConnectionPool::GetPool");
            return new SnowflakeDbSessionPool(ConnectionManager.GetPool(connectionString, password));
        }

        public static SnowflakeDbSessionPool GetPool(string connectionString)
        {
            s_loggerPair.LogDebug($"SnowflakeDbConnectionPool::GetPool");
            return new SnowflakeDbSessionPool(ConnectionManager.GetPool(connectionString));
        }

        internal static SessionPool GetPoolInternal(string connectionString)
        {
            s_loggerPair.LogDebug($"SnowflakeDbConnectionPool::GetPoolInternal");
            return ConnectionManager.GetPool(connectionString);
        }

        internal static bool AddSession(SFSession session)
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::AddSession");
            return ConnectionManager.AddSession(session);
        }

        internal static void ReleaseBusySession(SFSession session)
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::ReleaseBusySession");
            ConnectionManager.ReleaseBusySession(session);
        }

        public static void ClearAllPools()
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::ClearAllPools");
            ConnectionManager.ClearAllPools();
        }

        public static void SetMaxPoolSize(int maxPoolSize)
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::SetMaxPoolSize");
            ConnectionManager.SetMaxPoolSize(maxPoolSize);
        }

        public static int GetMaxPoolSize()
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::GetMaxPoolSize");
            return ConnectionManager.GetMaxPoolSize();
        }

        public static void SetTimeout(long connectionTimeout)
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::SetTimeout");
            ConnectionManager.SetTimeout(connectionTimeout);
        }

        public static long GetTimeout()
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::GetTimeout");
            return ConnectionManager.GetTimeout();
        }

        public static int GetCurrentPoolSize()
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::GetCurrentPoolSize");
            return ConnectionManager.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::SetPooling");
            return ConnectionManager.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            s_loggerPair.LogDebug("SnowflakeDbConnectionPool::GetPooling");
            return ConnectionManager.GetPooling();
        }

        public static void SetOldConnectionPoolVersion()
        {
            ForceConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
        }

        private static void SetConnectionPoolVersion(ConnectionPoolType requestedPoolType, bool force)
        {
            lock (s_connectionManagerInstanceLock)
            {
                if (s_connectionManager != null && !force)
                    return;
                Diagnostics.LogDiagnostics();
                s_connectionManager?.ClearAllPools();
                if (requestedPoolType == ConnectionPoolType.MultipleConnectionPool)
                {
                    s_connectionManager = new ConnectionPoolManager();
                    s_loggerPair.LogInformation("SnowflakeDbConnectionPool - multiple connection pools enabled");
                }
                if (requestedPoolType == ConnectionPoolType.SingleConnectionCache)
                {
                    s_connectionManager = new ConnectionCacheManager();
                    s_loggerPair.LogWarning("SnowflakeDbConnectionPool - connection cache enabled");
                }
            }
        }

        internal static void ForceConnectionPoolVersion(ConnectionPoolType requestedPoolType)
        {
            SetConnectionPoolVersion(requestedPoolType, true);
        }

        internal static ConnectionPoolType GetConnectionPoolVersion()
        {
            if (ConnectionManager != null)
            {
                switch (ConnectionManager)
                {
                    case ConnectionCacheManager _: return ConnectionPoolType.SingleConnectionCache;
                    case ConnectionPoolManager _: return ConnectionPoolType.MultipleConnectionPool;
                }
            }
            return DefaultConnectionPoolType;
        }
    }
}
