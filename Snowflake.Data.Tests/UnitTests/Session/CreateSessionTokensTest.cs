using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class CreateSessionTokensTest
    {
        [Test]
        public void TestGrantSessionCreation()
        {
            // arrange
            var tokens = new CreateSessionTokens();
            
            // act
            tokens.BeginCreate();
            
            // assert
            Assert.AreEqual(1, tokens.Count());
            
            // act
            tokens.BeginCreate();
            
            // assert
            Assert.AreEqual(2, tokens.Count());
        }

        [Test]
        public void TestCompleteSessionCreation()
        {
            // arrange
            var tokens = new CreateSessionTokens();
            var token1 = tokens.BeginCreate();
            var token2 = tokens.BeginCreate();
            
            // act
            tokens.EndCreate(token1);
            
            // assert
            Assert.AreEqual(1, tokens.Count());
            
            // act
            tokens.EndCreate(token2);
            
            // assert
            Assert.AreEqual(0, tokens.Count());
        }

        [Test]
        public void TestCompleteUnknownTokenDoesNotThrowExceptions()
        {
            // arrange
            var tokens = new CreateSessionTokens();
            tokens.BeginCreate();
            var unknownToken = new CreateSessionToken(0);
            
            // act
            tokens.EndCreate(unknownToken);

            // assert
            Assert.AreEqual(1, tokens.Count());
        }

        [Test]
        public void TestCompleteCleansExpiredTokens()
        {
            // arrange
            var tokens = new CreateSessionTokens();
            tokens._timeout = 50;
            var token = tokens.BeginCreate();
            tokens.BeginCreate(); // this token will be cleaned because of expiration
            Assert.AreEqual(2, tokens.Count());
            Thread.Sleep((int) tokens._timeout + 5);

            // act
            tokens.EndCreate(token);
            
            // assert
            Assert.AreEqual(0, tokens.Count());
        }
    }
}
