using System;

namespace Snowflake.Data.Tests.Util
{
    internal class StopWatch
    {
        private long _startMillis;
        private long _stopMillis;

        public long ElapsedMilliseconds
        {
            get => _stopMillis - _startMillis;
        }
        
        public void Start()
        {
            _startMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        public void Stop()
        {
            _stopMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }
    }
}
