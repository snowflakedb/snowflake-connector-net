using System.Linq;
using System.Security;

namespace Snowflake.Data.Core.Session
{
    sealed class ConnectionPoolManagerV1 : ConnectionPoolManagerBase
    {
        private const bool AllowExceedMaxPoolSizeDefault = true;
        // private const int MinPoolSizeDefault = 0; // TODO: SNOW-902610
        private const int MaxPoolSizeDefault = 10;
        private const string SinglePoolKeyForAllDataSources = "CONNECTION_CACHE";
        private const SessionPickAlgorithm SessionPicking = SessionPickAlgorithm.MatchConnectionString;
        
        protected override void ApplyPoolDefaults(SessionPool pool)
        {
            pool.SetAllowExceedMaxPoolSize(AllowExceedMaxPoolSizeDefault);
            pool.SetMaxPoolSize(MaxPoolSizeDefault);
            pool.SetSessionPickAlgorithm(SessionPicking);
        }
        protected override PoolManagerVersion GetVersion() => PoolManagerVersion.Version1;
        // Same pool for any connection string (backward compatible solution)
        protected override string GetPoolKey(string connectionString) => SinglePoolKeyForAllDataSources;
        public override void SetMaxPoolSize(int size) => GetPool().SetMaxPoolSize(size);
        public override int GetMaxPoolSize() => GetPool().GetMaxPoolSize();
        public override void SetTimeout(long time) => Pools.Values.First().SetTimeout(time);
        public override long GetTimeout() => Pools.Values.First().GetTimeout();
        public override int GetCurrentPoolSize() => Pools.Values.First().GetCurrentPoolSize();
        public override bool GetPooling() => Pools.Values.First().GetPooling();
        public override bool SetPooling(bool isEnable)
        {
            if (GetPooling() == isEnable)
                return false;
            Pools.Values.First().SetPooling(isEnable);
            return isEnable;
        }
        private SessionPool GetPool() => GetPool(null, null);
    }
}