using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Core.ConnectionPool
{
    class ConnectionPoolManagerV2 : ConnectionPoolManagerBase
    {
        private const bool AllowExceedMaxPoolSizeDefault = false;
        // private const int MinPoolSizeDefault = 0; // TODO: SNOW-902610
        private const int MaxPoolSizeDefault = 10;

        protected override PoolManagerVersion GetVersion() => PoolManagerVersion.Version2;

        protected override void ApplyPoolDefaults(SessionPool pool)
        {
            pool.SetAllowExceedMaxPoolSize(AllowExceedMaxPoolSizeDefault);
            // pool.SetMinPoolSize(MinPoolSizeDefault); // TODO: SNOW-902610
            pool.SetMaxPoolSize(MaxPoolSizeDefault);
        }
    }
}