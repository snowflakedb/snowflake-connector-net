using System;

namespace Snowflake.Data.Core.Session
{
    internal class SessionCreationToken
    {
        public Guid Id { get; }
        private readonly long _grantedAtAsEpochMillis;
        private readonly long _timeoutMillis;

        public SessionCreationToken(long timeoutMillis)
        {
            Id = Guid.NewGuid();
            _grantedAtAsEpochMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _timeoutMillis = timeoutMillis;
        }

        public bool IsExpired(long nowMillis) => nowMillis > _grantedAtAsEpochMillis + _timeoutMillis;
    }
}
