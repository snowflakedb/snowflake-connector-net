using System.Text.RegularExpressions;
using Xunit;
using Snowflake.Data.Core.Authenticator;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    public class ChallengeProviderTest
    {
        private readonly ChallengeProvider _challengeProvider = new();
        private readonly Regex _onlyDigitsOrNumbers = new(@"^[0-9a-zA-Z]+$");

        [SFFact]
        public void TestGenerateState()
        {
            // act
            var state = _challengeProvider.GenerateState();

            // assert
            Assert.Equal(32, state.Length);
            Assert.Matches(_onlyDigitsOrNumbers, state);
        }

        [SFFact]
        public void TestGenerateCodeVerifier()
        {
            // act
            var codeVerifier = _challengeProvider.GenerateCodeVerifier().Value;

            // assert
            Assert.InRange(codeVerifier.Length, 43, 128);
            Assert.Matches(_onlyDigitsOrNumbers, codeVerifier);
        }
    }
}
