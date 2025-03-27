using System.Security;
using System.Threading;
using System.Threading.Tasks;

namespace Snowflake.Data.Core.Session
{
    internal interface IConnectionManager
    {
        SFSession GetSession(string connectionString, SecureString password, SecureString passcode = null);
        Task<SFSession> GetSessionAsync(string connectionString, SecureString password, SecureString passcode, CancellationToken cancellationToken);
        bool AddSession(SFSession session);
        void ReleaseBusySession(SFSession session);
        void ClearAllPools();
        void SetMaxPoolSize(int maxPoolSize);
        int GetMaxPoolSize();
        void SetTimeout(long connectionTimeout);
        long GetTimeout();
        int GetCurrentPoolSize();
        bool SetPooling(bool poolingEnabled);
        bool GetPooling();
        SessionPool GetPool(string connectionString);
        SessionPool GetPool(string connectionString, SecureString password);
    }
}
