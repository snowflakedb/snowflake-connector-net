using System;
using System.Threading;

namespace Snowflake.Data.Core.Tools
{
    internal class TimeoutHelper
    {
        public static bool IsExpired(long startedAtMillis, long nowMillis, TimeSpan timeout)
        {
            if (IsInfinite(timeout))
                return false;
            var timeoutInMillis = (long)timeout.TotalMilliseconds;
            return startedAtMillis + timeoutInMillis <= nowMillis;
        }

        public static bool IsExpired(long elapsedMillis, TimeSpan timeout)
        {
            if (IsInfinite(timeout))
                return false;
            return elapsedMillis >= timeout.TotalMilliseconds;
        }

        public static bool IsInfinite(TimeSpan timeout) => timeout == Timeout.InfiniteTimeSpan;

        public static bool IsZeroLength(TimeSpan timeout)
        {
            if (IsInfinite(timeout))
                return false;
            return TimeSpan.Zero == timeout;
        }

        public static TimeSpan Infinity() => Timeout.InfiniteTimeSpan;

        public static long FiniteTimeoutLeftMillis(long startedAtMillis, long nowMillis, TimeSpan timeout)
        {
            if (IsInfinite(timeout))
            {
                throw new Exception("Infinite timeout cannot be used to determine milliseconds left");
            }
            var passedMillis = nowMillis - startedAtMillis;
            return Math.Max((long)timeout.TotalMilliseconds - passedMillis, 0);
        }
    }
}
