using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    [Parallelizable(ParallelScope.Self)]
    public class WaitingQueueTest
    {
        private static readonly int s_timeMeasurementLeftToleranceInMs = Stopwatch.IsHighResolution ? 1 : 20; // DateTime precision is ~10ms, safety coefficient = x2

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
            Assert.False(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(50 - s_timeMeasurementLeftToleranceInMs, 1500));
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
            Assert.False(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(50 - s_timeMeasurementLeftToleranceInMs, 1500));
        }

        [Test]
        public void TestWaitUntilResourceAvailable()
        {
            // arrange
            var queue = new WaitingQueue();
            var watch = new Stopwatch();
            Task.Run(async () =>
            {
                await Task.Delay(50).ConfigureAwait(false);
                queue.OnResourceIncrease();
            });

            // act
            watch.Start();
            var result = queue.Wait(30000, CancellationToken.None);
            watch.Stop();

            // assert
            Assert.True(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(50 - s_timeMeasurementLeftToleranceInMs, 1500));
        }

        [Test]
        public void TestWaitingEnabled()
        {
            // arrange
            var queue = new WaitingQueue();

            // act
            var isWaitingEnabled = queue.IsWaitingEnabled();

            // assert
            Assert.True(isWaitingEnabled);
        }

        [Test]
        public void TestNoOneIsWaiting()
        {
            // arrange
            var queue = new WaitingQueue();

            // act
            var isAnyoneWaiting = queue.IsAnyoneWaiting();

            // assert
            Assert.False(isAnyoneWaiting);
        }

        [Test]
        public async Task TestSomeoneIsWaiting()
        {
            // arrange
            var queue = new WaitingQueue();
            var syncThreadsSemaphore = new SemaphoreSlim(0, 1);
            _ = Task.Run(() =>
            {
                syncThreadsSemaphore.Release();
                return queue.Wait(1000, CancellationToken.None);
            });
            await syncThreadsSemaphore.WaitAsync(10000); // make sure scheduled thread execution has started
            await Task.Delay(50).ConfigureAwait(false);

            // act
            var isAnyoneWaiting = queue.IsAnyoneWaiting();

            // assert
            Assert.True(isAnyoneWaiting);
        }

        [Test]
        public void TestReturnUnsuccessfulOnResetWhileWaiting()
        {
            // arrange
            var queue = new WaitingQueue();
            var watch = new Stopwatch();
            Task.Run(async () =>
            {
                await Task.Delay(50);
                queue.Reset();
            });

            // act
            watch.Start();
            var result = queue.Wait(30000, CancellationToken.None);
            watch.Stop();

            // assert
            Assert.False(result);
            Assert.That(watch.ElapsedMilliseconds, Is.InRange(50 - s_timeMeasurementLeftToleranceInMs, 1500));
        }
    }
}
