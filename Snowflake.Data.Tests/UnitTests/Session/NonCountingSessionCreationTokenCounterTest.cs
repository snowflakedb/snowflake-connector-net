using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    public class NonCountingSessionCreationTokenCounterTest
    {
        [SFFact]
        public void TestGrantSessionCreation()
        {
            // arrange
            var tokens = new NonCountingSessionCreationTokenCounter();

            // act
            tokens.NewToken();

            // assert
            Assert.Equal(0, tokens.Count());
        }

        [SFFact]
        public void TestCompleteSessionCreation()
        {
            // arrange
            var tokens = new NonCountingSessionCreationTokenCounter();
            var token = tokens.NewToken();

            // act
            tokens.RemoveToken(token);

            // assert
            Assert.Equal(0, tokens.Count());
        }

        [SFFact]
        public void TestCompleteUnknownTokenDoesNotThrowExceptions()
        {
            // arrange
            var tokens = new NonCountingSessionCreationTokenCounter();
            tokens.NewToken();
            var unknownToken = new SessionCreationToken(SFSessionHttpClientProperties.DefaultConnectionTimeout);

            // act
            tokens.RemoveToken(unknownToken);

            // assert
            Assert.Equal(0, tokens.Count());
        }
    }
}
