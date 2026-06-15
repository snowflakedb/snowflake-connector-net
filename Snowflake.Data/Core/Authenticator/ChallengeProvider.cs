using System;
using System.Security.Cryptography;

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
            byte[] randomness = new byte[48];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(randomness);
            }
            var codeVerifierInput = Convert.ToBase64String(randomness);
            return new CodeVerifier(codeVerifierInput);
        }
    }
}
