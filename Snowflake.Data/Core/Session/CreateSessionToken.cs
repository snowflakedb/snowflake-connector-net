using System;

namespace Snowflake.Data.Core.Session
{
    internal class CreateSessionToken
    {
        public Guid Id { get; }
        private readonly long _grantedAtAsEpocMillis;
        private readonly long _timeoutMillis;

        public CreateSessionToken(long timeoutMillis)
        {
            Id = Guid.NewGuid();
            _grantedAtAsEpocMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _timeoutMillis = timeoutMillis;
        }

        public bool IsExpired(long nowMillis) => nowMillis > _grantedAtAsEpocMillis + _timeoutMillis;
    }
}
