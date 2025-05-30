using System;

namespace Snowflake.Data.Core.Session
{
    internal class NonCountingSessionCreationTokenCounter : ISessionCreationTokenCounter
    {
        private static readonly TimeSpan s_irrelevantCreateSessionTimeout = SFSessionHttpClientProperties.DefaultConnectionTimeout; // in case of old caching pool or pooling disabled we do not remove expired ones nor even store them

        public SessionCreationToken NewToken() => new SessionCreationToken(s_irrelevantCreateSessionTimeout);

        public void RemoveToken(SessionCreationToken creationToken)
        {
        }

        public int Count() => 0;

        public void Reset()
        {
        }
    }
}
