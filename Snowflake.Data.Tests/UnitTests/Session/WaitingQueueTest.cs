using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

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
        public void TestWaitUntilResourceAvailable()
        {
            // arrange
            var tests = TestRepeater<Tuple<bool, long>>.Test(3, () =>
            {
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
                return Tuple.Create(result, watch.ElapsedMilliseconds);
            });

            // assert
            tests.ForEach(t => Assert.IsTrue(t.Item1));
            tests.ForEach(t => Assert.GreaterOrEqual(t.Item2, 50));
            tests.SkipLargest(t => t.Item2) // some execution can be randomly delayed so we skip the largest value
                .ForEach(t => Assert.That(t.Item2, Is.InRange(50, 1500)));
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
    }
}
