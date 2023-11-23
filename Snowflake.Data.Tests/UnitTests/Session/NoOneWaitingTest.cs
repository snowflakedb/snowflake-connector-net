using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class NoOneWaitingTest
    {
        [Test]
        public void TestWaitDoesNotHangAndReturnsFalse()
        {
            // arrange
            var noOneWaiting = new NoOneWaiting();
            var watch = new Stopwatch();
            
            // act
            watch.Start();
            var result = noOneWaiting.Wait(10000, CancellationToken.None);
            watch.Stop();
            
            // assert
            Assert.IsFalse(result);
            Assert.LessOrEqual(watch.ElapsedMilliseconds, 50);
        }

        [Test]
        public void TestNoOneIsWaiting()
        {
            // arrange
            var noOneWaiting = new NoOneWaiting();
            noOneWaiting.Wait(10000, CancellationToken.None);
            
            // act
            var isAnyoneWaiting = noOneWaiting.IsAnyoneWaiting();
            
            // assert
            Assert.IsFalse(isAnyoneWaiting);
        }

        [Test]
        public void TestWaitingDisabled()
        {
            // arrange
            var noOneWaiting = new NoOneWaiting();

            // act
            var isWaitingEnabled = noOneWaiting.IsWaitingEnabled();
            
            // assert
            Assert.IsFalse(isWaitingEnabled);
        }
    }
}