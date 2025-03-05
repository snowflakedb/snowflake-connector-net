using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Session
{
    internal sealed class ConnectionCacheManager : IConnectionManager
    {
        private readonly SessionPool _sessionPool = SessionPool.CreateSessionCache();
        public SFSession GetSession(string connectionString, SessionPropertiesContext sessionContext) => _sessionPool.GetSession(connectionString, sessionContext);
        public Task<SFSession> GetSessionAsync(string connectionString, SessionPropertiesContext sessionContext, CancellationToken cancellationToken)
            => _sessionPool.GetSessionAsync(connectionString, sessionContext, cancellationToken);
        public bool AddSession(SFSession session) => _sessionPool.AddSession(session, false);
        public void ReleaseBusySession(SFSession session) => _sessionPool.ReleaseBusySession(session);
        public void ClearAllPools() => _sessionPool.ClearSessions();
        public void SetMaxPoolSize(int maxPoolSize) => _sessionPool.SetMaxPoolSize(maxPoolSize);
        public int GetMaxPoolSize() => _sessionPool.GetMaxPoolSize();
        public void SetTimeout(long connectionTimeout) => _sessionPool.SetTimeout(connectionTimeout);
        public long GetTimeout() => _sessionPool.GetTimeout();
        public int GetCurrentPoolSize() => _sessionPool.GetCurrentPoolSize();
        public bool SetPooling(bool poolingEnabled) => _sessionPool.SetPooling(poolingEnabled);
        public bool GetPooling() => _sessionPool.GetPooling();
        public SessionPool GetPool(string connectionString) => _sessionPool;
        public SessionPool GetPool(string connectionString, SessionPropertiesContext sessionContext) => _sessionPool;
    }
}
