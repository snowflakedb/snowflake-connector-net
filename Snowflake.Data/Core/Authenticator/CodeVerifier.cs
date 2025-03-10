using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Snowflake.Data.Core.Authenticator
{
    public class CodeVerifier
    {
        public string Value { get; }

        public CodeVerifier(string value)
        {
            var valueWithValidChars = SkipIllegalCharacters(value);
            Validate(valueWithValidChars);
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

        private void Validate(string value)
        {
            if (value?.Length < 43)
                throw new ArgumentException("The code verifier must be at least 43 characters");
            if (value.Length > 128)
                throw new ArgumentException("The code verifier must not be longer than 128 characters");
            if (!HasOnlyLegalCharacters(value))
                throw new ArgumentException("Illegal char(s) in code verifier, see RFC 7636, section 4.1");
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

        private static bool HasOnlyLegalCharacters(string code)
        {
            if (string.IsNullOrEmpty(code))
                return true;
            return code.ToCharArray().All(IsLegalCharacter);
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
