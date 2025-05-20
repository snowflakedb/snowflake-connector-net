using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class NonCountingSessionCreationTokenCounterTest
    {
        [Test]
        public void TestGrantSessionCreation()
        {
            // arrange
            var tokens = new NonCountingSessionCreationTokenCounter();

            // act
            tokens.NewToken();

            // assert
            Assert.AreEqual(0, tokens.Count());
        }

        [Test]
        public void TestCompleteSessionCreation()
        {
            // arrange
            var tokens = new NonCountingSessionCreationTokenCounter();
            var token = tokens.NewToken();

            // act
            tokens.RemoveToken(token);

            // assert
            Assert.AreEqual(0, tokens.Count());
        }

        [Test]
        public void TestCompleteUnknownTokenDoesNotThrowExceptions()
        {
            // arrange
            var tokens = new NonCountingSessionCreationTokenCounter();
            tokens.NewToken();
            var unknownToken = new SessionCreationToken(SFSessionHttpClientProperties.DefaultConnectionTimeout);

            // act
            tokens.RemoveToken(unknownToken);

            // assert
            Assert.AreEqual(0, tokens.Count());
        }
    }
}
