using System.Diagnostics;
using System.Threading;
using Xunit;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    public class NonWaitingQueueTest
    {
        [SFFact]
        public void TestWaitDoesNotHangAndReturnsFalse()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();
            var watch = new Stopwatch();

            // act
            watch.Start();
            var result = nonWaitingQueue.Wait(10000, CancellationToken.None);
            watch.Stop();

            // assert
            Assert.False(result);
            Assert.InRange(watch.ElapsedMilliseconds, long.MinValue, 50);
        }

        [SFFact]
        public void TestNoOneIsWaiting()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();
            nonWaitingQueue.Wait(10000, CancellationToken.None);

            // act
            var isAnyoneWaiting = nonWaitingQueue.IsAnyoneWaiting();

            // assert
            Assert.False(isAnyoneWaiting);
        }

        [SFFact]
        public void TestWaitingDisabled()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();

            // act
            var isWaitingEnabled = nonWaitingQueue.IsWaitingEnabled();

            // assert
            Assert.False(isWaitingEnabled);
        }

        [SFFact]
        public void TestReset()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();

            // act/assert
            nonWaitingQueue.Reset();
        }
    }
}
