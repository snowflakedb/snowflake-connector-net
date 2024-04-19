using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Data.Core.Tools;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    [TestFixture]
    public class TimeoutHelperTest
    {
        [Test]
        [TestCaseSource(nameof(InfiniteTimeouts))]
        public void TestInfinity(TimeSpan infiniteTimeout)
        {
            // act
            var isInfinite = TimeoutHelper.IsInfinite(infiniteTimeout);

            // assert
            Assert.IsTrue(isInfinite);
        }

        [Test]
        [TestCaseSource(nameof(FiniteTimeouts))]
        public void TestFiniteValue(TimeSpan finiteTimeout)
        {
            // act
            var isInfinite = TimeoutHelper.IsInfinite(finiteTimeout);

            // assert
            Assert.IsFalse(isInfinite);
        }

        [Test]
        [TestCaseSource(nameof(ZeroLengthTimeouts))]
        public void TestZeroLength(TimeSpan zeroTimeout)
        {
            // act
            var isZeroLength = TimeoutHelper.IsZeroLength(zeroTimeout);

            // assert
            Assert.IsTrue(isZeroLength);
        }

        [Test]
        [TestCaseSource(nameof(NonZeroLengthTimeouts))]
        public void TestNonZeroLength(TimeSpan nonZeroTimeout)
        {
            // act
            var isZeroLength = TimeoutHelper.IsZeroLength(nonZeroTimeout);

            // assert
            Assert.IsFalse(isZeroLength);
        }

        [Test]
        [TestCase(1000, 1000)]
        [TestCase(1000, 2000)]
        public void TestInfiniteTimeoutDoesNotExpire(long startedAtMillis, long nowMillis)
        {
            // act
            var isExpired = TimeoutHelper.IsExpired(startedAtMillis, nowMillis, TimeoutHelper.Infinity());

            // assert
            Assert.IsFalse(isExpired);
        }

        [Test]
        [TestCase(1000, 1000, 0, true)]
        [TestCase(1000, 2000, 0, true)]
        [TestCase(1000, 1100, 100, true)]
        [TestCase(1000, 1099, 100, false)]
        [TestCase(1000, 2000, 100, true)]
        public void TestExpiredTimeout(long startedAtMillis, long nowMillis, long timeoutMillis, bool expectedIsExpired)
        {
            // arrange
            var timeout = TimeSpan.FromMilliseconds(timeoutMillis);

            // act
            var isExpired = TimeoutHelper.IsExpired(startedAtMillis, nowMillis, timeout);

            // assert
            Assert.AreEqual(expectedIsExpired, isExpired);
        }

        [Test]
        public void TestInfiniteTimeoutNeverExpires()
        {
            // act
            var isExpired = TimeoutHelper.IsExpired(1000, TimeoutHelper.Infinity());

            // assert
            Assert.IsFalse(isExpired);
        }

        [Test]
        [TestCase(0, 0, true)]
        [TestCase(1000, 0, true)]
        [TestCase(100, 100, true)]
        [TestCase(99, 100, false)]
        [TestCase(1000, 100, true)]
        public void TestExpiredTimeoutByDuration(long durationMillis, long timeoutMillis, bool expectedIsExpired)
        {
            // arrange
            var timeout = TimeSpan.FromMilliseconds(timeoutMillis);

            // act
            var isExpired = TimeoutHelper.IsExpired(durationMillis, timeout);

            // assert
            Assert.AreEqual(expectedIsExpired, isExpired);
        }

        [Test]
        public void TestFiniteTimeoutLeftFailsForInfiniteTimeout()
        {
            // act
            var thrown = Assert.Throws<Exception>(() =>
                TimeoutHelper.FiniteTimeoutLeftMillis(1000, 2000, TimeoutHelper.Infinity()));

            // assert
            Assert.That(thrown.Message, Does.Contain("Infinite timeout cannot be used to determine milliseconds left"));
        }


        [Test]
        [TestCase(1000, 1000, 0, 0)]
        [TestCase(1000, 2000, 0, 0)]
        [TestCase(1000, 1100, 100, 0)]
        [TestCase(1000, 1095, 100, 5)]
        public void TestFiniteTimeoutLeft(long startedAtMillis, long nowMillis, long timeoutMillis, long expectedMillisLeft)
        {
            // arrange
            var timeout = TimeSpan.FromMilliseconds(timeoutMillis);

            // act
            var millisLeft = TimeoutHelper.FiniteTimeoutLeftMillis(startedAtMillis, nowMillis, timeout);

            // assert
            Assert.AreEqual(expectedMillisLeft, millisLeft);
        }

        public static IEnumerable<TimeSpan> InfiniteTimeouts()
        {
            yield return TimeoutHelper.Infinity();
            yield return TimeSpan.FromMilliseconds(-1);
        }

        public static IEnumerable<TimeSpan> FiniteTimeouts()
        {
            yield return TimeSpan.Zero;
            yield return TimeSpan.FromMilliseconds(1);
            yield return TimeSpan.FromSeconds(2);
        }

        public static IEnumerable<TimeSpan> ZeroLengthTimeouts()
        {
            yield return TimeSpan.Zero;
            yield return TimeSpan.FromMilliseconds(0);
            yield return TimeSpan.FromSeconds(0);
        }

        public static IEnumerable<TimeSpan> NonZeroLengthTimeouts()
        {
            yield return TimeoutHelper.Infinity();
            yield return TimeSpan.FromMilliseconds(3);
            yield return TimeSpan.FromSeconds(5);
        }
    }
}
