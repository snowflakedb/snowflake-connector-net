using System;

namespace Snowflake.Data.Tests.Util
{
    /**
     * StopWatch class is a measure time based on DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().
     * The class System.Diagnostics.Stopwatch uses ticks of processor and calculates the time based on tick frequency.
     * If the code which we are testing uses DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() it is better to use this StopWatch class
     * because the tests are less flaky in GH builds.
     */
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
