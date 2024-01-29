using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SessionCreationTokenCounterTest
    {
        private const long LongTimeAsMillis = 30000;
        private const long ShortTimeAsMillis = 50;
        
        [Test]
        public void TestGrantSessionCreation()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(LongTimeAsMillis);
            
            // act
            tokens.NewToken();
            
            // assert
            Assert.AreEqual(1, tokens.Count());
            
            // act
            tokens.NewToken();
            
            // assert
            Assert.AreEqual(2, tokens.Count());
        }

        [Test]
        public void TestCompleteSessionCreation()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(LongTimeAsMillis);
            var token1 = tokens.NewToken();
            var token2 = tokens.NewToken();
            
            // act
            tokens.RemoveToken(token1);
            
            // assert
            Assert.AreEqual(1, tokens.Count());
            
            // act
            tokens.RemoveToken(token2);
            
            // assert
            Assert.AreEqual(0, tokens.Count());
        }

        [Test]
        public void TestCompleteUnknownTokenDoesNotThrowExceptions()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(LongTimeAsMillis);
            tokens.NewToken();
            var unknownToken = new SessionCreationToken(0);
            
            // act
            tokens.RemoveToken(unknownToken);

            // assert
            Assert.AreEqual(1, tokens.Count());
        }

        [Test]
        public void TestCompleteCleansExpiredTokens()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(ShortTimeAsMillis);
            var token = tokens.NewToken();
            tokens.NewToken(); // this token will be cleaned because of expiration
            Assert.AreEqual(2, tokens.Count());
            Thread.Sleep((int) ShortTimeAsMillis + 5);

            // act
            tokens.RemoveToken(token);
            
            // assert
            Assert.AreEqual(0, tokens.Count());
        }
    }
}
