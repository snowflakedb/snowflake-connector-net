﻿/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

using System;
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
        private static readonly Object s_connectionManagerInstanceLock = new Object();
        private static IConnectionManager s_connectionManager;
        internal const ConnectionPoolType DefaultConnectionPoolType = ConnectionPoolType.MultipleConnectionPool;

        private static IConnectionManager ConnectionManager
        {
            get
            {
                if (s_connectionManager != null)
                    return s_connectionManager;
                SetConnectionPoolVersion(DefaultConnectionPoolType);
                return s_connectionManager;
            }
        }

        internal static SFSession GetSession(string connectionString, SecureString password)
        {
            s_logger.Debug($"SnowflakeDbConnectionPool::GetSession");
            return ConnectionManager.GetSession(connectionString, password);
        }

        internal static Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug($"SnowflakeDbConnectionPool::GetSessionAsync");
            return ConnectionManager.GetSessionAsync(connectionString, password, cancellationToken);
        }

        public static SnowflakeDbSessionPool GetPool(string connectionString, SecureString password)
        {
            s_logger.Debug($"SnowflakeDbConnectionPool::GetPool");
            return new SnowflakeDbSessionPool(ConnectionManager.GetPool(connectionString, password));
        }

        public static SnowflakeDbSessionPool GetPool(string connectionString)
        {
            s_logger.Debug($"SnowflakeDbConnectionPool::GetPool");
            return new SnowflakeDbSessionPool(ConnectionManager.GetPool(connectionString));
        }

        internal static SessionPool GetPoolInternal(string connectionString)
        {
            s_logger.Debug($"SnowflakeDbConnectionPool::GetPoolInternal");
            return ConnectionManager.GetPool(connectionString);
        }

        internal static bool AddSession(SFSession session)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::AddSession");
            return ConnectionManager.AddSession(session);
        }

        internal static void ReleaseBusySession(SFSession session)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::ReleaseBusySession");
            ConnectionManager.ReleaseBusySession(session);
        }

        public static void ClearAllPools()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            ConnectionManager.ClearAllPools();
        }

        public static void SetMaxPoolSize(int maxPoolSize)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetMaxPoolSize");
            ConnectionManager.SetMaxPoolSize(maxPoolSize);
        }

        public static int GetMaxPoolSize()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetMaxPoolSize");
            return ConnectionManager.GetMaxPoolSize();
        }

        public static void SetTimeout(long connectionTimeout)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetTimeout");
            ConnectionManager.SetTimeout(connectionTimeout);
        }

        public static long GetTimeout()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetTimeout");
            return ConnectionManager.GetTimeout();
        }

        public static int GetCurrentPoolSize()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetCurrentPoolSize");
            return ConnectionManager.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::SetPooling");
            return ConnectionManager.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetPooling");
            return ConnectionManager.GetPooling();
        }

        public static void SetOldConnectionPoolVersion()
        {
            SetConnectionPoolVersion(ConnectionPoolType.SingleConnectionCache);
        }

        internal static void SetConnectionPoolVersion(ConnectionPoolType requestedPoolType)
        {
            lock (s_connectionManagerInstanceLock)
            {
                s_connectionManager?.ClearAllPools();
                if (requestedPoolType == ConnectionPoolType.MultipleConnectionPool)
                {
                    s_connectionManager = new ConnectionPoolManager();
                    s_logger.Info("SnowflakeDbConnectionPool - multiple connection pools enabled");
                }
                if (requestedPoolType == ConnectionPoolType.SingleConnectionCache)
                {
                    s_connectionManager = new ConnectionCacheManager();
                    s_logger.Warn("SnowflakeDbConnectionPool - connection cache enabled");
                }
            }
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
