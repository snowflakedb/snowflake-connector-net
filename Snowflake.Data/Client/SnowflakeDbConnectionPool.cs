using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Log;

namespace Snowflake.Data.Client
{
    public class SnowflakeDbConnectionPool
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<SnowflakeDbConnectionPool>();

        internal static SFSession GetSession(string connStr)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::GetSession");
            return SessionPoolSingleton.Instance.GetSession(connStr);
        }

        internal static bool AddSession(SFSession session)
        {
            s_logger.Debug("SnowflakeDbConnectionPool::AddSession");
            return SessionPoolSingleton.Instance.AddSession(session);
        }

        public static void ClearAllPools()
        {
            s_logger.Debug("SnowflakeDbConnectionPool::ClearAllPools");
            SessionPoolSingleton.Instance.ClearAllPools();
        }

        public static void SetMaxPoolSize(int size)
        {
            SessionPoolSingleton.Instance.SetMaxPoolSize(size);
        }

        public static int GetMaxPoolSize()
        {
            return SessionPoolSingleton.Instance.GetMaxPoolSize();
        }

        public static void SetTimeout(long time)
        {
            SessionPoolSingleton.Instance.SetTimeout(time);
        }
        
        public static long GetTimeout()
        {
            return SessionPoolSingleton.Instance.GetTimeout();
        }

        public static int GetCurrentPoolSize()
        {
            return SessionPoolSingleton.Instance.GetCurrentPoolSize();
        }

        public static bool SetPooling(bool isEnable)
        {
            return SessionPoolSingleton.Instance.SetPooling(isEnable);
        }

        public static bool GetPooling()
        {
            return SessionPoolSingleton.Instance.GetPooling();
        }
    }
}
