using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Snowflake.Data.Core.Authenticator
{
    internal class CodeVerifier
    {
        public string Value { get; }

        public CodeVerifier(string value)
        {
            var valueWithValidChars = SkipIllegalCharacters(value);
            ValidateLength(valueWithValidChars);
            Value = valueWithValidChars;
        }

        public string ComputeCodeChallenge()
        {
            var codeVerifierBytes = Encoding.UTF8.GetBytes(Value);
            using (SHA256 sha256Encoder = SHA256.Create())
            {
                byte[] sha256Hash = sha256Encoder.ComputeHash(codeVerifierBytes);
                return Base64UrlEncoder.Encode(sha256Hash);
            }
        }

        private void ValidateLength(string value)
        {
            if ((value?.Length ?? 0) < 43)
                throw new ArgumentException("The code verifier must be at least 43 characters");
            if (value.Length > 128)
                throw new ArgumentException("The code verifier must not be longer than 128 characters");
        }

        private string SkipIllegalCharacters(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;
            var validChars = value.ToCharArray()
                .Where(IsLegalCharacter)
                .ToArray();
            return new StringBuilder().Append(validChars).ToString();
        }

        private static bool IsLegalCharacter(char value)
        {
            if (value > 127)
                return false;
            return (value >= 'A' && value <= 'Z') ||
                   (value >= 'a' && value <= 'z') ||
                   (value >= '0' && value <= '9') ||
                   value == '-' ||
                   value == '.' ||
                   value == '_' ||
                   value == '~';
        }
    }
}
