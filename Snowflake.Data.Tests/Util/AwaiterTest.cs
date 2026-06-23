using System;
using System.Threading.Tasks;
using Xunit;

namespace Snowflake.Data.Tests.Util
{
    public class AwaiterTest
    {
        private readonly TimeSpan _maxDurationRegardedAsImmediately = TimeSpan.FromMilliseconds(100);

        [SFFact]
        public async Task TestReturnsImmediatelyWhenConditionIsMet()
        {
            // act
            var millis = await MillisecondsOfWaiting(() => true, TimeSpan.FromHours(1)).ConfigureAwait(false);

            // assert
            Assert.InRange(millis, long.MinValue, _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        [SFFact]
        public async Task TestReturnsImmediatelyOnZeroTimeout()
        {
            // act
            var millis = await MillisecondsOfWaiting(() => false, TimeSpan.FromMilliseconds(0)).ConfigureAwait(false);

            // assert
            Assert.True(millis <= _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        [SFFact(RetriesCount = RetriesCount.Twice)]
        public async Task TestReturnsOnTimeout()
        {
            // arrange
            var timeout = TimeSpan.FromSeconds(2);

            // act
            var millis = await MillisecondsOfWaiting(() => false, TimeSpan.FromSeconds(2)).ConfigureAwait(false);

            // assert
            Assert.True(millis >= timeout.TotalMilliseconds);
        }

        private async Task<long> MillisecondsOfWaiting(Func<bool> condition, TimeSpan timeout)
        {
            var watch = new StopWatch();
            watch.Start();
            await Awaiter.WaitUntilConditionOrTimeout(condition, timeout).ConfigureAwait(false);
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }
    }
}
