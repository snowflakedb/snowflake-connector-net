using System;
using System.Threading.Tasks;
using Xunit;

namespace Snowflake.Data.Tests.Util
{
    public class AwaiterTest
    {
        private readonly TimeSpan _maxDurationRegardedAsImmediately = TimeSpan.FromSeconds(1);

        [SFFact]
        public async Task TestReturnsImmediatelyWhenConditionIsMet()
        {
            // act
            var millis = await MillisecondsOfWaiting(() => true, TimeSpan.FromHours(1));

            // assert
            Assert.True(millis <= _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        [SFFact]
        public async Task TestReturnsImmediatelyOnZeroTimeout()
        {
            // act
            var millis = await MillisecondsOfWaiting(() => false, TimeSpan.FromMilliseconds(0));

            // assert
            Assert.True(millis <= _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        [SFFact]
        public async Task TestReturnsOnTimeout()
        {
            // arrange
            var timeout = TimeSpan.FromSeconds(2);

            // act
            var millis = await MillisecondsOfWaiting(() => false, TimeSpan.FromSeconds(2));

            // assert
            Assert.InRange(millis, _maxDurationRegardedAsImmediately.TotalMilliseconds, timeout.TotalMilliseconds + _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        private async Task<long> MillisecondsOfWaiting(Func<bool> condition, TimeSpan timeout)
        {
            var watch = new StopWatch();
            watch.Start();
            await Awaiter.WaitUntilConditionOrTimeout(condition, timeout);
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }
    }
}
