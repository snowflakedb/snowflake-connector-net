namespace Snowflake.Data.Core.Session
{
    internal class SessionPoolState
    {
        private readonly int _idleSessionsCount;
        private readonly int _busySessionsCount;
        private readonly int _sessionCreationsCount;
        private readonly int _waitingCount;
        private readonly bool _extensiveFormat;

        public SessionPoolState(int idleSessionsCount, int busySessionsCount, int sessionCreationsCount, int waitingCount, bool extensiveFormat)
        {
            _idleSessionsCount = idleSessionsCount;
            _busySessionsCount = busySessionsCount;
            _sessionCreationsCount = sessionCreationsCount;
            _waitingCount = waitingCount;
            _extensiveFormat = extensiveFormat;
        }

        public int Count() => _idleSessionsCount + _busySessionsCount + _sessionCreationsCount;

        public int IdleSessionsCount { get => _idleSessionsCount; }

        public int BusySessionsCount { get => _busySessionsCount; }

        public override string ToString()
        {
            return _extensiveFormat
                ? $"[pool size: {Count()} (idle sessions: {_idleSessionsCount}, busy sessions: {_busySessionsCount}, sessions under creation: {_sessionCreationsCount}), waiting sessions: {_waitingCount}]"
                : $"[pool size: {Count()}]";
        }
    }
}
