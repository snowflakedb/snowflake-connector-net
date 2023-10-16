using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Session
{
    internal sealed class ConnectionCacheManager : IConnectionManager
    {
        private readonly SessionPool _sessionPool = new SessionPool();
        public SFSession GetSession(string connectionString, SecureString password) => _sessionPool.GetSession(connectionString, password);
        public Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken)
            => _sessionPool.GetSessionAsync(connectionString, password, cancellationToken);
        public bool AddSession(SFSession session) => _sessionPool.AddSession(session);
        public void ClearAllPools() => _sessionPool.ClearAllPools();
        public void SetMaxPoolSize(int maxPoolSize) => _sessionPool.SetMaxPoolSize(maxPoolSize);
        public int GetMaxPoolSize() => _sessionPool.GetMaxPoolSize();
        public void SetTimeout(long connectionTimeout) => _sessionPool.SetTimeout(connectionTimeout);
        public long GetTimeout() => _sessionPool.GetTimeout();
        public int GetCurrentPoolSize() => _sessionPool.GetCurrentPoolSize();
        public bool SetPooling(bool poolingEnabled) => _sessionPool.SetPooling(poolingEnabled);
        public bool GetPooling() => _sessionPool.GetPooling();
    }
}
