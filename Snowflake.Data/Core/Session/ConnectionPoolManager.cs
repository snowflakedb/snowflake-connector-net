using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Session
{
    internal sealed class ConnectionPoolManager : IConnectionManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<ConnectionPoolManager>();
        private static readonly Object s_poolsLock = new Object();
        private static readonly Exception s_operationNotAvailable = new Exception("You cannot change connection pool parameters for all the pools. Instead you can change it on a particular pool");
        private readonly Dictionary<string, SessionPool> _pools;

        internal ConnectionPoolManager()
        {
            lock (s_poolsLock)
            {
                _pools = new Dictionary<string, SessionPool>();
            }
        }

        public SFSession GetSession(string connectionString, SecureString password, SecureString passcode)
        {
            s_logger.Debug($"ConnectionPoolManager::GetSession");
            return GetPool(connectionString, password).GetSession(passcode);
        }

        public Task<SFSession> GetSessionAsync(string connectionString, SecureString password, SecureString passcode, CancellationToken cancellationToken)
        {
            s_logger.Debug($"ConnectionPoolManager::GetSessionAsync");
            return GetPool(connectionString, password).GetSessionAsync(passcode, cancellationToken);
        }

        public bool AddSession(SFSession session)
        {
            s_logger.Debug("ConnectionPoolManager::AddSession");
            return GetPool(session.ConnectionString, session.Password).AddSession(session, true);
        }

        public void ReleaseBusySession(SFSession session)
        {
            s_logger.Debug("ConnectionPoolManager::ReleaseBusySession");
            GetPool(session.ConnectionString, session.Password).ReleaseBusySession(session);
        }

        public void ClearAllPools()
        {
            s_logger.Debug("ConnectionPoolManager::ClearAllPools");
            foreach (var sessionPool in _pools.Values)
            {
                sessionPool.DestroyPool();
            }
            _pools.Clear();
        }

        public void SetMaxPoolSize(int maxPoolSize)
        {
            throw s_operationNotAvailable;
        }

        public int GetMaxPoolSize()
        {
            s_logger.Debug("ConnectionPoolManager::GetMaxPoolSize");
            var values = _pools.Values.Select(it => it.GetMaxPoolSize()).Distinct().ToList();
            switch (values.Count)
            {
                case 0:
                    return SFSessionHttpClientProperties.DefaultMaxPoolSize;
                case 1:
                    return values.First();
                default:
                    throw new SnowflakeDbException(SFError.INCONSISTENT_RESULT_ERROR, "Multiple pools have different Max Pool Size values");
            }
        }

        public void SetTimeout(long connectionTimeout)
        {
            throw s_operationNotAvailable;
        }

        public long GetTimeout()
        {
            s_logger.Debug("ConnectionPoolManager::GetTimeout");
            var values = _pools.Values.Select(it => it.GetTimeout()).Distinct().ToList();
            switch (values.Count)
            {
                case 0:
                    return (long) SFSessionHttpClientProperties.DefaultExpirationTimeout.TotalSeconds;
                case 1:
                    return values.First();
                default:
                    throw new SnowflakeDbException(SFError.INCONSISTENT_RESULT_ERROR, "Multiple pools have different Timeout values");
            }
        }

        public int GetCurrentPoolSize()
        {
            s_logger.Debug("ConnectionPoolManager::GetCurrentPoolSize");
            return _pools.Values.Select(it => it.GetCurrentPoolSize()).Sum();
        }

        public bool SetPooling(bool poolingEnabled)
        {
            throw s_operationNotAvailable;
        }

        public bool GetPooling()
        {
            s_logger.Debug("ConnectionPoolManager::GetPooling");
            return true; // in new pool pooling is always enabled by default, disabling only by connection string parameter
        }

        public SessionPool GetPool(string connectionString, SecureString password)
        {
            s_logger.Debug("ConnectionPoolManager::GetPool with connection string and secure password");
            var poolKey = GetPoolKey(connectionString, password);

            if (_pools.TryGetValue(poolKey, out var item))
            {
                item.ValidateSecurePassword(password);
                return item;
            }

            lock (s_poolsLock)
            {
                if (_pools.TryGetValue(poolKey, out var poolCreatedWhileWaitingOnLock))
                {
                    poolCreatedWhileWaitingOnLock.ValidateSecurePassword(password);
                    return poolCreatedWhileWaitingOnLock;
                }
                s_logger.Info($"Creating new pool");
                var pool = SessionPool.CreateSessionPool(connectionString, password);
                _pools.Add(poolKey, pool);
                return pool;
            }
        }

        public SessionPool GetPool(string connectionString)
        {
            s_logger.Debug("ConnectionPoolManager::GetPool with connection string");
            return GetPool(connectionString, null);
        }

        private string GetPoolKey(string connectionString, SecureString password) =>
            password != null && password.Length > 0
                ? connectionString + ";password=" + SecureStringHelper.Decode(password) + ";"
                : connectionString + ";password=;";
    }
}
