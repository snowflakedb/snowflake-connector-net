using System;

namespace Snowflake.Data.Core.Session
{
    internal class CreateSessionToken
    {
        public Guid Id { get; }
        private readonly long _grantedAtAsEpocMillis;
        private readonly long _timeout;

        public CreateSessionToken(long timeout)
        {
            Id = Guid.NewGuid();
            _grantedAtAsEpocMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _timeout = timeout;
        }

        public bool IsExpired(long nowMillis) => nowMillis > _grantedAtAsEpocMillis + _timeout;
    }
}
