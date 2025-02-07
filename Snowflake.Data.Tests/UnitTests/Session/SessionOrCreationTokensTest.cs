using System;
using System.Linq;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SessionOrCreationTokensTest
    {
        private SFSession _session = new SFSession("account=test;user=test;password=test", new SessionPropertiesContext());

        [Test]
        public void TestNoBackgroundSessionsToCreateWhenInitialisedWithSession()
        {
            // arrange
            var sessionOrTokens = new SessionOrCreationTokens(_session);

            // act
            var backgroundCreationTokens = sessionOrTokens.BackgroundSessionCreationTokens();

            Assert.AreEqual(0, backgroundCreationTokens.Count);
        }

        [Test]
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
            Assert.AreSame(tokens[0], token);
        }

        [Test]
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
            Assert.AreEqual(2, backgroundTokens.Count);
            Assert.AreSame(tokens[1], backgroundTokens[0]);
            Assert.AreSame(tokens[2], backgroundTokens[1]);
        }
    }
}
