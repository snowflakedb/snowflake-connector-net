/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Session
{
    internal interface IConnectionManager
    {
        SFSession GetSession(string connectionString, SecureString password);
        Task<SFSession> GetSessionAsync(string connectionString, SecureString password, CancellationToken cancellationToken);
        bool AddSession(SFSession session);
        void ClearAllPools();
        void SetMaxPoolSize(int maxPoolSize);
        int GetMaxPoolSize();
        void SetTimeout(long connectionTimeout);
        long GetTimeout();
        int GetCurrentPoolSize();
        bool SetPooling(bool poolingEnabled);
        bool GetPooling();
    }
}
