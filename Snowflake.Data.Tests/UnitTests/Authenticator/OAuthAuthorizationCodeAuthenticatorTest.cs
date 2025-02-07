
using NUnit.Framework;
using Snowflake.Data.Core.Authenticator;

namespace Snowflake.Data.Tests.UnitTests.Authenticator
{
    [TestFixture]
    public class OAuthAuthorizationCodeAuthenticatorTest
    {
        [Test]
        public void TestGenerateCodeVerifier()
        {
            var challengeProvider = new ChallengeProvider();
            var verifierCode = challengeProvider.GenerateCodeVerifier();
        }
    }
}
