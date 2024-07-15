using System;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Core.Session
{
    internal class SessionCreationToken
    {
        public Guid Id { get; }
        private readonly long _grantedAtAsEpochMillis;
        private readonly TimeSpan _timeout;

        public SessionCreationToken(TimeSpan timeout)
        {
            Id = Guid.NewGuid();
            _grantedAtAsEpochMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _timeout = timeout;
        }

        public bool IsExpired(long nowMillis) =>
            TimeoutHelper.IsExpired(_grantedAtAsEpochMillis, nowMillis, _timeout);
    }
}
