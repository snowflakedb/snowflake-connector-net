using System;
using Xunit;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    public class SessionCreationTokenTest
    {
        private static readonly TimeSpan s_timeout30Seconds = TimeSpan.FromSeconds(30);

        [Fact]
        public void TestTokenIsNotExpired()
        {
            // arrange
            var token = new SessionCreationToken(s_timeout30Seconds);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            // assert
            Assert.False(isExpired);
        }

        [Fact]
        public void TestTokenIsExpired()
        {
            // arrange
            var token = new SessionCreationToken(s_timeout30Seconds);
            var timeout30SecondsAsMillis = (long)s_timeout30Seconds.TotalMilliseconds;

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeout30SecondsAsMillis + 1);

            // assert
            Assert.True(isExpired);
        }
    }
}
