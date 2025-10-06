using System;
using NUnit.Framework;

namespace Snowflake.Data.Tests.Util
{
    [TestFixture]
    public class AwaiterTest
    {
        private readonly TimeSpan _maxDurationRegardedAsImmediately = TimeSpan.FromSeconds(1);

        [Test]
        public void TestReturnsImmediatelyWhenConditionIsMet()
        {
            // act
            var millis = MillisecondsOfWaiting(() => true, TimeSpan.FromHours(1));

            // assert
            Assert.LessOrEqual(millis, _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        [Test]
        public void TestReturnsImmediatelyOnZeroTimeout()
        {
            // act
            var millis = MillisecondsOfWaiting(() => false, TimeSpan.FromMilliseconds(0));

            // assert
            Assert.LessOrEqual(millis, _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        [Test]
        public void TestReturnsOnTimeout()
        {
            // arrange
            var timeout = TimeSpan.FromSeconds(2);

            // act
            var millis = MillisecondsOfWaiting(() => false, TimeSpan.FromSeconds(2));

            // assert
            Assert.GreaterOrEqual(millis, _maxDurationRegardedAsImmediately.TotalMilliseconds);
            Assert.LessOrEqual(millis, timeout.TotalMilliseconds + _maxDurationRegardedAsImmediately.TotalMilliseconds);
        }

        private long MillisecondsOfWaiting(Func<bool> condition, TimeSpan timeout)
        {
            var watch = new StopWatch();
            watch.Start();
            Awaiter.WaitUntilConditionOrTimeout(condition, timeout);
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }
    }
}
