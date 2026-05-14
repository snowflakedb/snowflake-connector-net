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
            Assert.True(_onlyDigitsOrNumbers.IsMatch(state));
        }

        [SFFact]
        public void TestGenerateCodeVerifier()
        {
            // act
            var codeVerifier = _challengeProvider.GenerateCodeVerifier().Value;

            // assert
            Assert.True(codeVerifier.Length >= 43 && codeVerifier.Length <= 128);
            Assert.True(_onlyDigitsOrNumbers.IsMatch(codeVerifier));
        }
    }
}
