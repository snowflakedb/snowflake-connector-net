using System;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class CreateSessionTokenTest
    {
        private const long Timeout = 30000; // 30 seconds in millis
        
        [Test]
        public void TestTokenIsNotExpired()
        {
            // arrange
            var token = new CreateSessionToken(Timeout);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            
            // assert
            Assert.IsFalse(isExpired);
        }
        
        [Test]
        public void TestTokenIsExpired()
        {
            // arrange
            var token = new CreateSessionToken(Timeout);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + Timeout + 1);

            // assert
            Assert.IsTrue(isExpired);
        }
    }
}
