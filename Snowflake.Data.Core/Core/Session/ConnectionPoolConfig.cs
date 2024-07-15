using System;

namespace Snowflake.Data.Core.Session
{
    internal class ConnectionPoolConfig
    {
        public int MaxPoolSize { get; set; } = SFSessionHttpClientProperties.DefaultMaxPoolSize;
        public int MinPoolSize { get; set; } = SFSessionHttpClientProperties.DefaultMinPoolSize;
        public ChangedSessionBehavior ChangedSession { get; set; } = SFSessionHttpClientProperties.DefaultChangedSession;
        public TimeSpan WaitingForIdleSessionTimeout { get; set; } = SFSessionHttpClientProperties.DefaultWaitingForIdleSessionTimeout;
        public TimeSpan ExpirationTimeout { get; set; } = SFSessionHttpClientProperties.DefaultExpirationTimeout;
        public bool PoolingEnabled { get; set; } = SFSessionHttpClientProperties.DefaultPoolingEnabled;
        public TimeSpan ConnectionTimeout { get; set; } = SFSessionHttpClientProperties.DefaultConnectionTimeout;
    }
}
