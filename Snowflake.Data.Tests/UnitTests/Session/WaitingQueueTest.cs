using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class WaitingQueueTest
    {
        [Test]
        public void TestWaitForTheResourceUntilTimeout()
        {
            // arrange
            var queue = new WaitingQueue();
            var watch = new Stopwatch();

            // act
            watch.Start();
            var result = queue.Wait(50, CancellationToken.None);
            watch.Stop();

            // assert
            Assert.IsFalse(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(45, 1500)); // sometimes Wait takes a bit smaller amount of time than it should. Thus we expect it to be greater than 45, not just 50.
        }

        [Test]
        public void TestWaitForTheResourceUntilCancellation()
        {
            // arrange
            var queue = new WaitingQueue();
            var cancellationSource = new CancellationTokenSource(50);
            var watch = new Stopwatch();

            // act
            watch.Start();
            var result = queue.Wait(30000, cancellationSource.Token);
            watch.Stop();

            // assert
            Assert.IsFalse(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(45, 1500)); // sometimes Wait takes a bit smaller amount of time than it should. Thus we expect it to be greater than 45, not just 50.
        }

        [Test]
        [Retry(2)]
        public void TestWaitUntilResourceAvailable()
        {
            // arrange
            var queue = new WaitingQueue();
            var watch = new Stopwatch();
            Task.Run(() =>
            {
                Thread.Sleep(50);
                queue.OnResourceIncrease();
            });

            // act
            watch.Start();
            var result = queue.Wait(30000, CancellationToken.None);
            watch.Stop();

            // assert
            Assert.IsTrue(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(50, 1500));
        }

        [Test]
        public void TestWaitingEnabled()
        {
            // arrange
            var queue = new WaitingQueue();

            // act
            var isWaitingEnabled = queue.IsWaitingEnabled();

            // assert
            Assert.IsTrue(isWaitingEnabled);
        }

        [Test]
        public void TestNoOneIsWaiting()
        {
            // arrange
            var queue = new WaitingQueue();

            // act
            var isAnyoneWaiting = queue.IsAnyoneWaiting();

            // assert
            Assert.IsFalse(isAnyoneWaiting);
        }

        [Test]
        public void TestSomeoneIsWaiting()
        {
            // arrange
            var queue = new WaitingQueue();
            var syncThreadsSemaphore = new SemaphoreSlim(0, 1);
            Task.Run(() =>
            {
                syncThreadsSemaphore.Release();
                return queue.Wait(1000, CancellationToken.None);
            });
            syncThreadsSemaphore.Wait(10000); // make sure scheduled thread execution has started
            Thread.Sleep(50);

            // act
            var isAnyoneWaiting = queue.IsAnyoneWaiting();

            // assert
            Assert.IsTrue(isAnyoneWaiting);
        }

        [Test]
        [Retry(2)]
        public void TestReturnUnsuccessfulOnResetWhileWaiting()
        {
            // arrange
            var queue = new WaitingQueue();
            var watch = new Stopwatch();
            Task.Run(() =>
            {
                Thread.Sleep(50);
                queue.Reset();
            });

            // act
            watch.Start();
            var result = queue.Wait(30000, CancellationToken.None);
            watch.Stop();

            // assert
            Assert.IsFalse(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(50, 1500));
        }
    }
}
