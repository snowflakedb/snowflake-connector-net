using System;
using System.Threading;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.Util
{
    public static class Awaiter
    {
        private static readonly TimeSpan s_defaultDelay = TimeSpan.FromMilliseconds(200);

        public static void WaitUntilConditionOrTimeout(Func<bool> condition, TimeSpan timeout)
        {
            WaitUntilConditionOrTimeout(condition, timeout, s_defaultDelay);
        }

        public static void WaitUntilConditionOrTimeout(Func<bool> condition, TimeSpan timeout, TimeSpan delay)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var breakTime = TimeoutHelper.IsInfinite(timeout) ? long.MaxValue : startTime + timeout.TotalMilliseconds;
            while (!condition() && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < breakTime)
            {
                Thread.Sleep(delay);
            }
        }
    }
}
