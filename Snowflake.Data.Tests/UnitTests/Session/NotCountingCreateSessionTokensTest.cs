using NUnit.Framework;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class NotCountingCreateSessionTokensTest
    {
        [Test]
        public void TestGrantSessionCreation()
        {
            // arrange
            var tokens = new NotCountingCreateSessionTokens();
            
            // act
            tokens.BeginCreate();
            
            // assert
            Assert.AreEqual(0, tokens.Count());
        }
        
        [Test]
        public void TestCompleteSessionCreation()
        {
            // arrange
            var tokens = new NotCountingCreateSessionTokens();
            var token = tokens.BeginCreate();
            
            // act
            tokens.EndCreate(token);
            
            // assert
            Assert.AreEqual(0, tokens.Count());
        }
        
        [Test]
        public void TestCompleteUnknownTokenDoesNotThrowExceptions()
        {
            // arrange
            var tokens = new NotCountingCreateSessionTokens();
            tokens.BeginCreate();
            var unknownToken = new CreateSessionToken(0);
            
            // act
            tokens.EndCreate(unknownToken);

            // assert
            Assert.AreEqual(0, tokens.Count());
        }
    }
}
