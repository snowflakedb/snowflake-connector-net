using System;

namespace Snowflake.Data.Core.Authenticator
{
    internal class ChallengeProvider
    {
        public virtual string GenerateState()
        {
            return Guid.NewGuid().ToString().Replace("-", "");
        }

        public virtual CodeVerifier GenerateCodeVerifier()
        {
            Random random = new Random();
            byte[] randomness = new byte[48];
            random.NextBytes(randomness);
            var codeVerifierInput = Convert.ToBase64String(randomness);
            return new CodeVerifier(codeVerifierInput);
        }
    }
}
