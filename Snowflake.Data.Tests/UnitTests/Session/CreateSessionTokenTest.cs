using System;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class CreateSessionTokenTest
    {
        private readonly long _timeout = 30000; // 30 seconds in millis
        
        [Test]
        public void TestTokenIsNotExpired()
        {
            // arrange
            var token = new CreateSessionToken(_timeout);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            
            // assert
            Assert.IsFalse(isExpired);
        }
        
        [Test]
        public void TestTokenIsExpired()
        {
            // arrange
            var token = new CreateSessionToken(_timeout);

            // act
            var isExpired = token.IsExpired(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + _timeout + 1);

            // assert
            Assert.IsTrue(isExpired);
        }
    }
}
