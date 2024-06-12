using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class NonWaitingQueueTest
    {
        [Test]
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
            Assert.IsFalse(result);
            Assert.LessOrEqual(watch.ElapsedMilliseconds, 50);
        }

        [Test]
        public void TestNoOneIsWaiting()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();
            nonWaitingQueue.Wait(10000, CancellationToken.None);

            // act
            var isAnyoneWaiting = nonWaitingQueue.IsAnyoneWaiting();

            // assert
            Assert.IsFalse(isAnyoneWaiting);
        }

        [Test]
        public void TestWaitingDisabled()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();

            // act
            var isWaitingEnabled = nonWaitingQueue.IsWaitingEnabled();

            // assert
            Assert.IsFalse(isWaitingEnabled);
        }

        [Test]
        public void TestReset()
        {
            // arrange
            var nonWaitingQueue = new NonWaitingQueue();

            // act/assert
            Assert.DoesNotThrow(() => nonWaitingQueue.Reset());
        }
    }
}
