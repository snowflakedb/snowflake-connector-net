using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Session
{
    public enum PoolManagerVersion
    {
        Version1,
        Version2
    }

    public abstract class ConnectionPoolManagerBase
    {
        protected static readonly Object PoolsLock = new Object();
        protected readonly Dictionary<string, SessionPool> Pools;
        protected ConnectionPoolManagerBase()
        {
            lock (PoolsLock)
            {
                Pools = new Dictionary<string, SessionPool>();
            }
        }

        protected abstract PoolManagerVersion GetVersion();

        protected abstract void ApplyPoolDefaults(SessionPool pool);

        protected virtual string GetPoolKey(string connectionString) => connectionString;

        public void ClearAllPools()
        {
            Pools.Values.ToList().ForEach(pool => pool.ClearAllPools());
        }
        
        public virtual SessionPool GetPool(string connectionString, SecureString password)
        {
            string poolKey = GetPoolKey(connectionString);
            if (Pools.TryGetValue(poolKey, out var pool))
                return pool;
            
            pool = CreateSessionPool(connectionString, password);
            ApplyPoolDefaults(pool); 
            Pools.Add(poolKey, pool);
            return pool;
        }
        
        public virtual void SetMaxPoolSize(int size) => throw FeatureNotAvailableForPoolVersion();
        public virtual int GetMaxPoolSize() => throw FeatureNotAvailableForPoolVersion();
        public virtual void SetTimeout(long time) => throw FeatureNotAvailableForPoolVersion();
        public virtual long GetTimeout() => throw FeatureNotAvailableForPoolVersion();
        public virtual int GetCurrentPoolSize() => throw FeatureNotAvailableForPoolVersion();
        public virtual bool SetPooling(bool isEnable) => throw FeatureNotAvailableForPoolVersion();
        public virtual bool GetPooling() => throw FeatureNotAvailableForPoolVersion();

        internal SFSession GetSession(string connectionString, SecureString password)
        {
            // TODO:
            // pool ver.1 is the same for any connection string so there's still need to pass params to GetSession
            // pool ver.2 is different for each connection strings so in theory no need to pass params to GetSession
            var sessionPool = GetPool(connectionString, password);
            return sessionPool.GetSession(connectionString, password); 
        }

        internal Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
        {
            return GetPool(connectionString, password).GetSessionAsync(connectionString, password, cancellationToken); 
        }

        internal bool AddSession(string connectionString, SecureString password, SFSession session)
        {
            return GetPool(connectionString, password).AddSession(session);
        }

        private SessionPool CreateSessionPool(string connectionString, SecureString password) => new SessionPool(connectionString, password);

        private NotSupportedException FeatureNotAvailableForPoolVersion()
        {
            return new NotSupportedException("API not available for selected connection pool version selected: " +
                                             GetVersion());
        }
    }
}