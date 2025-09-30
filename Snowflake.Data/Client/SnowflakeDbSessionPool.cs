using System;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbSessionPool
    {
        private readonly SessionPool _sessionPool;

        internal SnowflakeDbSessionPool(SessionPool sessionPool)
            => _sessionPool = sessionPool ?? throw new NullReferenceException("SessionPool not provided!");

        public bool GetPooling() => _sessionPool.GetPooling();

        public int GetMinPoolSize() => _sessionPool.GetMinPoolSize();

        public int GetMaxPoolSize() => _sessionPool.GetMaxPoolSize();

        public int GetCurrentPoolSize() => _sessionPool.GetCurrentPoolSize();

        public long GetExpirationTimeout() => _sessionPool.GetTimeout();

        public long GetConnectionTimeout() => _sessionPool.GetConnectionTimeout();

        public long GetWaitForIdleSessionTimeout() => _sessionPool.GetWaitForIdleSessionTimeout();

        public void ClearPool() => _sessionPool.ClearSessions();

        public ChangedSessionBehavior GetChangedSession() => _sessionPool.GetChangedSession();
    }
}
