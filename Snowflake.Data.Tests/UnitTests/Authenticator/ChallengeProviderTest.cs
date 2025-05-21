using System.Text.RegularExpressions;
using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class ChallengeProviderTest
    {
        private readonly ChallengeProvider _challengeProvider = new();
        private readonly Regex _onlyDigitsOrNumbers = new(@"^[0-9a-zA-Z]+$");

        [Test]
        public void TestGenerateState()
        {
            // act
            var state = _challengeProvider.GenerateState();

            // assert
            Assert.AreEqual(32, state.Length);
            Assert.True(_onlyDigitsOrNumbers.IsMatch(state));
        }

        [Test]
        public void TestGenerateCodeVerifier()
        {
            // act
            var codeVerifier = _challengeProvider.GenerateCodeVerifier().Value;

            // assert
            Assert.That(codeVerifier.Length, Is.GreaterThanOrEqualTo(43).And.LessThanOrEqualTo(128));
            Assert.IsTrue(_onlyDigitsOrNumbers.IsMatch(codeVerifier));
        }
    }
}
