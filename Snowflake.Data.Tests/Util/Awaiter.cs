using System;
using System.Threading.Tasks;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.Util
{
    public static class Awaiter
    {
        private static readonly TimeSpan s_defaultDelay = TimeSpan.FromMilliseconds(200);

        public static Task WaitUntilConditionOrTimeout(Func<bool> condition, TimeSpan timeout)
        {
            return WaitUntilConditionOrTimeout(condition, timeout, s_defaultDelay);
        }

        public static async Task WaitUntilConditionOrTimeout(Func<bool> condition, TimeSpan timeout, TimeSpan delay)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var breakTime = TimeoutHelper.IsInfinite(timeout) ? long.MaxValue : startTime + timeout.TotalMilliseconds;
            while (!condition() && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < breakTime)
            {
                await Task.Delay(delay);
            }
        }
    }
}
