/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{
    internal sealed class ConnectionPoolManager : IConnectionManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionPoolManager>();
        private static readonly Object s_poolsLock = new Object();
        private readonly Dictionary<string, SessionPool> _pools;

        internal ConnectionPoolManager()
        {
            lock (s_poolsLock)
            {
                _pools = new Dictionary<string, SessionPool>();
            }
        }
        
        public SFSession GetSession(string connectionString, SecureString password)
        {
            s_logger.Debug($"ConnectionPoolManager::GetSession");
            return GetPool(connectionString, password).GetSession();
        }

        public Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            s_logger.Debug($"ConnectionPoolManager::GetSessionAsync");
            return GetPool(connectionString, password).GetSessionAsync(cancellationToken);
        }

        public bool AddSession(SFSession session)
        {
            s_logger.Debug($"ConnectionPoolManager::AddSession for {session.ConnectionString}");
            return GetPool(session.ConnectionString, session.Password).AddSession(session);
        }

        public void ClearAllPools()
        {
            s_logger.Debug("ConnectionPoolManager::ClearAllPools");
            foreach (var sessionPool in _pools.Values)
            {
                sessionPool.ClearSessions();       
            }
            _pools.Clear();
        }
        
        public void SetMaxPoolSize(int maxPoolSize)
        {
            s_logger.Debug("ConnectionPoolManager::SetMaxPoolSize for all pools");
            foreach (var pool in _pools.Values)
            {
                pool.SetMaxPoolSize(maxPoolSize);
            }
        }

        public int GetMaxPoolSize()
        {
            s_logger.Debug("ConnectionPoolManager::GetMaxPoolSize");
            var values = _pools.Values.Select(it => it.GetMaxPoolSize()).Distinct().ToList();
            return values.Count == 1
                ? values.First()
                : throw new SnowflakeDbException(SFError.INCONSISTENT_RESULT_ERROR, "Multiple pools have different Max Pool Size values");
        }

        public void SetTimeout(long connectionTimeout)
        {
            s_logger.Debug("ConnectionPoolManager::SetTimeout for all pools");
            foreach (var pool in _pools.Values)
            {
                pool.SetTimeout(connectionTimeout);
            }
        }

        public long GetTimeout()
        {
            s_logger.Debug("ConnectionPoolManager::GetTimeout");
            var values = _pools.Values.Select(it => it.GetTimeout()).Distinct().ToList();
            return values.Count == 1
                ? values.First()
                : throw new SnowflakeDbException(SFError.INCONSISTENT_RESULT_ERROR, "Multiple pools have different Timeout values");
        }

        public int GetCurrentPoolSize()
        {
            s_logger.Debug("ConnectionPoolManager::GetCurrentPoolSize");
            var values = _pools.Values.Select(it => it.GetCurrentPoolSize()).Distinct().ToList();
            return values.Count == 1
                ? values.First()
                : throw new SnowflakeDbException(SFError.INCONSISTENT_RESULT_ERROR, "Multiple pools have different Current Pool Size values");
        }

        public bool SetPooling(bool poolingEnabled)
        {
            if (!poolingEnabled)
                throw new Exception(
                    "Could not disable pooling for all connections. You could disable pooling by given connection string instead.");
            s_logger.Debug("ConnectionPoolManager::SetPooling for all pools");
            return _pools.Values
                .Select(pool => pool.SetPooling(poolingEnabled))
                .All(setPoolingResult => setPoolingResult);
        }

        public bool GetPooling() 
        {
            s_logger.Debug("ConnectionPoolManager::GetPooling");
            var values = _pools.Values.Select(it => it.GetPooling()).Distinct().ToList();
            return values.Count == 1
                ? values.First()
                : throw new SnowflakeDbException(SFError.INCONSISTENT_RESULT_ERROR, "Multiple pools have different Pooling values");
        }

        internal SessionPool GetPool(string connectionString, SecureString password)
        {
            s_logger.Debug($"ConnectionPoolManager::GetPool");
            var poolKey = GetPoolKey(connectionString);
            
            if (_pools.TryGetValue(poolKey, out var item))
                return item;
            lock (s_poolsLock)
            {
                if (_pools.TryGetValue(poolKey, out var poolCreatedWhileWaitingOnLock))
                    return poolCreatedWhileWaitingOnLock;
                s_logger.Info($"Creating new pool");
                var pool = SessionPool.CreateSessionPool(connectionString, password);
                _pools.Add(poolKey, pool);
                return pool;
            }
        }

        public SessionPool GetPool(string connectionString)
        {
            s_logger.Debug($"ConnectionPoolManager::GetPool");
            return GetPool(connectionString, null);
        }

        // TODO: SNOW-937188
        private string GetPoolKey(string connectionString)
        {
            return connectionString;
        }
    }
}
