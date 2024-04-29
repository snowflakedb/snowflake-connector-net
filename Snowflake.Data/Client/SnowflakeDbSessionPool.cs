/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
using System;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Client
{

    public class SnowflakeDbSessionPool : IDisposable
    {
        private SessionPool _sessionPool;

        internal SnowflakeDbSessionPool(SessionPool sessionPool)
            => _sessionPool = sessionPool ?? throw new NullReferenceException("SessionPool not provided!");
        public void Dispose() => _sessionPool = null;

        public void SetMaxPoolSize(int size) => _sessionPool.SetMaxPoolSize(size);
        public int GetMaxPoolSize() => _sessionPool.GetMaxPoolSize();

        public void SetMinPoolSize(int size) => _sessionPool.SetMinPoolSize(size);
        public int GetMinPoolSize() => _sessionPool.GetMinPoolSize();

        public void SetExpirationTimeout(long seconds) => _sessionPool.SetTimeout(seconds);
        public long GetExpirationTimeout() => _sessionPool.GetTimeout();

        public void SetConnectionTimeout(long seconds) => _sessionPool.SetTimeout(seconds);
        public long GetConnectionTimeout() => _sessionPool.GetTimeout();

        public int GetCurrentPoolSize() => _sessionPool.GetCurrentPoolSize();

        public bool SetPooling(bool isEnable) => _sessionPool.SetPooling(isEnable);
        public bool GetPooling() => _sessionPool.GetPooling();

        public void SetChangedSession(ChangedSessionBehavior newChangedSession) => _sessionPool.SetChangedSession(newChangedSession);
        public ChangedSessionBehavior GetChangedSession() => _sessionPool.GetChangedSession();

        public void SetWaitForIdleSessionTimeout(double seconds) => _sessionPool.SetWaitForIdleSessionTimeout(seconds);
        public double GetWaitForIdleSessionTimeout() => _sessionPool.GetWaitForIdleSessionTimeout();
    }
}
