using System;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SessionCreationTokenTest
    {
        private const long Timeout30SecondsAsMillis = 30000;
        
        [Test]
        public void TestTokenIsNotExpired()
        {
            // arrange
            var token = new SessionCreationToken(Timeout30SecondsAsMillis);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            
            // assert
            Assert.IsFalse(isExpired);
        }
        
        [Test]
        public void TestTokenIsExpired()
        {
            // arrange
            var token = new SessionCreationToken(Timeout30SecondsAsMillis);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Timeout30SecondsAsMillis + 1);

            // assert
            Assert.IsTrue(isExpired);
        }
    }
}
