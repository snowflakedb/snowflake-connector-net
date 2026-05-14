using System;
using System.Collections.Generic;
using Xunit;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Tools
{
    public class TimeoutHelperTest
    {
        [SFTheory]
        [MemberData(nameof(InfiniteTimeouts))]
        public void TestInfinity(TimeSpan infiniteTimeout)
        {
            // act
            var isInfinite = TimeoutHelper.IsInfinite(infiniteTimeout);

            // assert
            Assert.True(isInfinite);
        }

        [SFTheory]
        [MemberData(nameof(FiniteTimeouts))]
        public void TestFiniteValue(TimeSpan finiteTimeout)
        {
            // act
            var isInfinite = TimeoutHelper.IsInfinite(finiteTimeout);

            // assert
            Assert.False(isInfinite);
        }

        [SFTheory]
        [MemberData(nameof(ZeroLengthTimeouts))]
        public void TestZeroLength(TimeSpan zeroTimeout)
        {
            // act
            var isZeroLength = TimeoutHelper.IsZeroLength(zeroTimeout);

            // assert
            Assert.True(isZeroLength);
        }

        [SFTheory]
        [MemberData(nameof(NonZeroLengthTimeouts))]
        public void TestNonZeroLength(TimeSpan nonZeroTimeout)
        {
            // act
            var isZeroLength = TimeoutHelper.IsZeroLength(nonZeroTimeout);

            // assert
            Assert.False(isZeroLength);
        }

        [SFTheory]
        [InlineData(1000, 1000)]
        [InlineData(1000, 2000)]
        public void TestInfiniteTimeoutDoesNotExpire(long startedAtMillis, long nowMillis)
        {
            // act
            var isExpired = TimeoutHelper.IsExpired(startedAtMillis, nowMillis, TimeoutHelper.Infinity());

            // assert
            Assert.False(isExpired);
        }

        [SFTheory]
        [InlineData(1000, 1000, 0, true)]
        [InlineData(1000, 2000, 0, true)]
        [InlineData(1000, 1100, 100, true)]
        [InlineData(1000, 1099, 100, false)]
        [InlineData(1000, 2000, 100, true)]
        public void TestExpiredTimeout(long startedAtMillis, long nowMillis, long timeoutMillis, bool expectedIsExpired)
        {
            // arrange
            var timeout = TimeSpan.FromMilliseconds(timeoutMillis);

            // act
            var isExpired = TimeoutHelper.IsExpired(startedAtMillis, nowMillis, timeout);

            // assert
            Assert.Equal(expectedIsExpired, isExpired);
        }

        [SFFact]
        public void TestInfiniteTimeoutNeverExpires()
        {
            // act
            var isExpired = TimeoutHelper.IsExpired(1000, TimeoutHelper.Infinity());

            // assert
            Assert.False(isExpired);
        }

        [SFTheory]
        [InlineData(0, 0, true)]
        [InlineData(1000, 0, true)]
        [InlineData(100, 100, true)]
        [InlineData(99, 100, false)]
        [InlineData(1000, 100, true)]
        public void TestExpiredTimeoutByDuration(long durationMillis, long timeoutMillis, bool expectedIsExpired)
        {
            // arrange
            var timeout = TimeSpan.FromMilliseconds(timeoutMillis);

            // act
            var isExpired = TimeoutHelper.IsExpired(durationMillis, timeout);

            // assert
            Assert.Equal(expectedIsExpired, isExpired);
        }

        [SFFact]
        public void TestFiniteTimeoutLeftFailsForInfiniteTimeout()
        {
            // act
            var thrown = Assert.Throws<Exception>(() =>
                TimeoutHelper.FiniteTimeoutLeftMillis(1000, 2000, TimeoutHelper.Infinity()));

            // assert
            Assert.Contains("Infinite timeout cannot be used to determine milliseconds left", thrown.Message);
        }


        [SFTheory]
        [InlineData(1000, 1000, 0, 0)]
        [InlineData(1000, 2000, 0, 0)]
        [InlineData(1000, 1100, 100, 0)]
        [InlineData(1000, 1095, 100, 5)]
        public void TestFiniteTimeoutLeft(long startedAtMillis, long nowMillis, long timeoutMillis, long expectedMillisLeft)
        {
            // arrange
            var timeout = TimeSpan.FromMilliseconds(timeoutMillis);

            // act
            var millisLeft = TimeoutHelper.FiniteTimeoutLeftMillis(startedAtMillis, nowMillis, timeout);

            // assert
            Assert.Equal(expectedMillisLeft, millisLeft);
        }

        public static IEnumerable<object[]> InfiniteTimeouts()
        {
            yield return new object[] { TimeoutHelper.Infinity() };
            yield return new object[] { TimeSpan.FromMilliseconds(-1) };
        }

        public static IEnumerable<object[]> FiniteTimeouts()
        {
            yield return new object[] { TimeSpan.Zero };
            yield return new object[] { TimeSpan.FromMilliseconds(1) };
            yield return new object[] { TimeSpan.FromSeconds(2) };
        }

        public static IEnumerable<object[]> ZeroLengthTimeouts()
        {
            yield return new object[] { TimeSpan.Zero };
            yield return new object[] { TimeSpan.FromMilliseconds(0) };
            yield return new object[] { TimeSpan.FromSeconds(0) };
        }

        public static IEnumerable<object[]> NonZeroLengthTimeouts()
        {
            yield return new object[] { TimeoutHelper.Infinity() };
            yield return new object[] { TimeSpan.FromMilliseconds(3) };
            yield return new object[] { TimeSpan.FromSeconds(5) };
        }
    }
}
