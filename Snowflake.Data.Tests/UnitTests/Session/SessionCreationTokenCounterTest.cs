using System;
using System.Threading;
using NUnit.Framework;
using Snowflake.Data.Core;
using Snowflake.Data.Core.Session;

namespace Snowflake.Data.Tests.UnitTests.Session
{
    [TestFixture]
    public class SessionCreationTokenCounterTest
    {
        private static readonly TimeSpan s_longTime = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan s_shortTime = TimeSpan.FromMilliseconds(50);

        [Test]
        public void TestGrantSessionCreation()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(s_longTime);

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
            var tokens = new SessionCreationTokenCounter(s_longTime);
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
            var tokens = new SessionCreationTokenCounter(s_longTime);
            tokens.NewToken();
            var unknownToken = new SessionCreationToken(SFSessionHttpClientProperties.DefaultConnectionTimeout);

            // act
            tokens.RemoveToken(unknownToken);

            // assert
            Assert.AreEqual(1, tokens.Count());
        }

        [Test]
        public void TestCompleteCleansExpiredTokens()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(s_shortTime);
            var token = tokens.NewToken();
            tokens.NewToken(); // this token will be cleaned because of expiration
            Assert.AreEqual(2, tokens.Count());
            const int EpsilonMillis = 5;
            Thread.Sleep((int)s_shortTime.TotalMilliseconds + EpsilonMillis);

            // act
            tokens.RemoveToken(token);

            // assert
            Assert.AreEqual(0, tokens.Count());
        }

        [Test]
        public void TestResetTokens()
        {
            // arrange
            var tokens = new SessionCreationTokenCounter(s_longTime);
            tokens.NewToken();
            tokens.NewToken();

            // act
            tokens.Reset();

            // assert
            Assert.AreEqual(0, tokens.Count());
        }
    }
}
