using System.Linq;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Core.ConnectionPool
{
    class ConnectionPoolManagerV2 : ConnectionPoolManagerBase
    {
        private const bool AllowExceedMaxPoolSizeDefault = false;
        // private const int MinPoolSizeDefault = 0; // TODO: SNOW-902610
        private const int MaxPoolSizeDefault = 10;
        private const SessionPickAlgorithm SessionPicking = SessionPickAlgorithm.PickOldest;

        protected override PoolManagerVersion GetVersion() => PoolManagerVersion.Version2;

        protected override void ApplyPoolDefaults(SessionPool pool)
        {
            pool.SetAllowExceedMaxPoolSize(AllowExceedMaxPoolSizeDefault);
            // pool.SetMinPoolSize(MinPoolSizeDefault); // TODO: SNOW-902610
            pool.SetMaxPoolSize(MaxPoolSizeDefault);
            pool.SetSessionPickAlgorithm(SessionPicking);
        }

        public new int GetCurrentPoolSize()
        {
            return Pools.Values.Sum(sessionPool => sessionPool.GetCurrentPoolSize());
        }
    }
}