using System;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Client;

public class SnowflakeDbSessionPool
{
    // Pools that stay unused are removed and recreated on demand, so the pool is looked up on every call
    // to keep a handle from pointing at a pool that is no longer in use.
    private readonly Func<SessionPool> _sessionPoolProvider;

    internal SnowflakeDbSessionPool(Func<SessionPool> sessionPoolProvider)
    {
        _sessionPoolProvider = sessionPoolProvider ?? throw new NullReferenceException("SessionPool provider not provided!");
        _ = SessionPool;
    }

    private SessionPool SessionPool => _sessionPoolProvider() ?? throw new NullReferenceException("SessionPool not provided!");

    public bool GetPooling() => SessionPool.GetPooling();

    public int GetMinPoolSize() => SessionPool.GetMinPoolSize();

    public int GetMaxPoolSize() => SessionPool.GetMaxPoolSize();

    public int GetCurrentPoolSize() => SessionPool.GetCurrentPoolSize();

    public long GetExpirationTimeout() => SessionPool.GetTimeout();

    public long GetConnectionTimeout() => SessionPool.GetConnectionTimeout();

    public long GetWaitForIdleSessionTimeout() => SessionPool.GetWaitForIdleSessionTimeout();

    public void ClearPool() => SessionPool.ClearSessions();

    public ChangedSessionBehavior GetChangedSession() => SessionPool.GetChangedSession();
}
