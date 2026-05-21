using System;
using System.Linq;
using Xunit;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{

    public class SessionOrCreationTokensTest
    {
        private SFSession _session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext());

        [SFFact]
        public void TestNoBackgroundSessionsToCreateWhenInitialisedWithSession()
        {
            // arrange
            var sessionOrTokens = new SessionOrCreationTokens(_session);

            // act
            var backgroundCreationTokens = sessionOrTokens.BackgroundSessionCreationTokens();

            Assert.Equal(0, backgroundCreationTokens.Count);
        }

        [SFFact]
        public void TestReturnFirstCreationToken()
        {
            // arrange
            var sessionCreationTokenCounter = new SessionCreationTokenCounter(TimeSpan.FromSeconds(10));
            var tokens = Enumerable.Range(1, 3)
                .Select(_ => sessionCreationTokenCounter.NewToken())
                .ToList();
            var sessionOrTokens = new SessionOrCreationTokens(tokens);

            // act
            var token = sessionOrTokens.SessionCreationToken();

            // assert
            Assert.Same(tokens[0], token);
        }

        [SFFact]
        public void TestReturnCreationTokensFromTheSecondOneForBackgroundExecution()
        {
            // arrange
            var sessionCreationTokenCounter = new SessionCreationTokenCounter(TimeSpan.FromSeconds(10));
            var tokens = Enumerable.Range(1, 3)
                .Select(_ => sessionCreationTokenCounter.NewToken())
                .ToList();
            var sessionOrTokens = new SessionOrCreationTokens(tokens);

            // act
            var backgroundTokens = sessionOrTokens.BackgroundSessionCreationTokens();

            // assert
            Assert.Equal(2, backgroundTokens.Count);
            Assert.Same(tokens[1], backgroundTokens[0]);
            Assert.Same(tokens[2], backgroundTokens[1]);
        }
    }
}
