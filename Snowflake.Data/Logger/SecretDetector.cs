using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Snowflake.Data.Log
{
    class SecretDetector
    {
        public struct Mask
        {
            public Mask(bool isMasked = false, string maskedText = null, string errStr = null)
            {
                this.isMasked = isMasked;
                this.maskedText = maskedText;
                this.errStr = errStr;
            }

            public bool isMasked { get; set; }
            public string maskedText { get; set; }
            public string errStr { get; set; }
        }

        private static List<string> CUSTOM_PATTERNS_REGEX = new List<string>();
        private static List<string> CUSTOM_PATTERNS_MASK = new List<string>();
        private static int CUSTOM_PATTERNS_LENGTH;

        public static void SetCustomPatterns(string[] customRegex, string[] customMask)
        {
            if (CUSTOM_PATTERNS_LENGTH != 0)
            {
                ClearCustomPatterns();
            }

            if (customRegex.Length == customMask.Length)
            {
                CUSTOM_PATTERNS_LENGTH = customRegex.Length;
                for (int index = 0; index < CUSTOM_PATTERNS_LENGTH; index++)
                {
                    CUSTOM_PATTERNS_REGEX.Add(customRegex[index]);
                    CUSTOM_PATTERNS_MASK.Add(customMask[index]);
                }
            }
            else
            {
                throw new ArgumentException("Regex count and mask count must be equal.");
            }
        }

        public static void ClearCustomPatterns()
        {
            CUSTOM_PATTERNS_REGEX.Clear();
            CUSTOM_PATTERNS_MASK.Clear();
            CUSTOM_PATTERNS_LENGTH = 0;
        }

        private static string MaskCustomPatterns(string text)
        {
            string result;
            for (int index = 0; index < CUSTOM_PATTERNS_LENGTH; index++)
            {
                result = Regex.Replace(text, CUSTOM_PATTERNS_REGEX[index], CUSTOM_PATTERNS_MASK[index],
                                         RegexOptions.IgnoreCase);

                if (result != text)
                {
                    return result;
                }
            }
            return text;
        }

        /*
         * https://docs.microsoft.com/en-us/dotnet/standard/base-types/character-escapes-in-regular-expressions
         * . $ ^ { [ ( | ) * + ? \
         * The characters are special regular expression language elements.
         * To match them in a regular expression, they must be escaped or included in a positive character group.
         * [ ] \ - ^
         * The characters are special character group element.
         * To match them in a character group, they must be escaped.
         */
        private const string AwsKeyPattern = @"(aws_key_id|aws_secret_key|access_key_id|secret_access_key)('|"")?(\s*[:=]\s*)'([^']+)'";
        private const string AwsTokenPattern = @"(accessToken|tempToken|keySecret)\""\s*:\s*\""([a-z0-9/+]{32,}={0,2})\""";
        private const string AwsServerSidePattern = @"((x-amz-server-side-encryption)([a-z0-9\-])*)\s*(:|=)\s*([a-z0-9/_\-+:=])+";
        private const string SasTokenPattern = @"(sig|signature|AWSAccessKeyId|password|passcode)=([a-z0-9%/+]{16,})";
        private const string PrivateKeyPattern = @"-----BEGIN PRIVATE KEY-----\n([a-z0-9/+=\n]{32,})\n-----END PRIVATE KEY-----"; // pragma: allowlist secret
        private const string PrivateKeyDataPattern = @"""privateKeyData"": ""([a-z0-9/+=\n]{10,})""";
        private const string PrivateKeyPropertyPrefixPattern = @"(private_key\s*=)";
        private const string ConnectionTokenPattern = @"(token|assertion content)(['""\s:=]+)([a-z0-9=/_\-+:]{8,})";
        private const string TokenPropertyPattern = @"(token)(\s*=)(.*)";
        private const string PasswordPattern = @"(password|passcode|client_?secret|pwd|proxypassword|private_key_pwd)(['""\s:=]+)([a-z0-9!""#$%&'\()*+,-./:;<=>?@\[\]\^_`{|}~]{6,})";
        private const string PasswordPropertyPattern = @"(password|passcode|oauthclientsecret|proxypassword|private_key_pwd)(\s*=)(.*)";

        private static readonly Func<string, string>[] s_maskFunctions = {
            MaskAWSServerSide,
            MaskAWSKeys,
            MaskSASTokens,
            MaskAWSTokens,
            MaskPrivateKey,
            MaskPrivateKeyData,
            MaskPrivateKeyProperty,
            MaskPassword,
            MaskPasswordProperty,
            MaskConnectionTokens,
            MaskTokenProperty
        };

        private static string MaskAWSKeys(string text)
        {
            return Regex.Replace(text, AwsKeyPattern, @"$1$2$3'****'",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskAWSTokens(string text)
        {
            return Regex.Replace(text, AwsTokenPattern, @"$1"":""XXXX""",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskAWSServerSide(string text)
        {
            return Regex.Replace(text, AwsServerSidePattern, @"$1:....",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskSASTokens(string text)
        {
            return Regex.Replace(text, SasTokenPattern, @"$1=****",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskPrivateKey(string text)
        {
            return Regex.Replace(text, PrivateKeyPattern, "-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----", // pragma: allowlist secret
                                         RegexOptions.IgnoreCase | RegexOptions.Multiline);
        }

        private static string MaskPrivateKeyProperty(string text)
        {
            var match = Regex.Match(text, PrivateKeyPropertyPrefixPattern, RegexOptions.IgnoreCase);
            if (match.Success)
            {
                int length = match.Index + match.Value.Length;
                return text.Substring(0, length) + "****";
            }
            return text;
        }

        private static string MaskPrivateKeyData(string text)
        {
            return Regex.Replace(text, PrivateKeyDataPattern, @"""privateKeyData"": ""XXXX""",
                                         RegexOptions.IgnoreCase | RegexOptions.Multiline);
        }

        private static string MaskConnectionTokens(string text)
        {
            return Regex.Replace(text, ConnectionTokenPattern, @"$1$2****",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskPassword(string text)
        {
            return Regex.Replace(text, PasswordPattern, @"$1$2****",
                                         RegexOptions.IgnoreCase);
        }

        private static string MaskPasswordProperty(string text)
        {
            return Regex.Replace(text, PasswordPropertyPattern, @"$1$2****", RegexOptions.IgnoreCase);
        }

        private static string MaskTokenProperty(string text)
        {
            return Regex.Replace(text, TokenPropertyPattern, @"$1$2****", RegexOptions.IgnoreCase);
        }

        public static Mask MaskSecrets(string text)
        {
            Mask result = new Mask(maskedText: text);

            if (String.IsNullOrEmpty(text))
            {
                return result;
            }

            try
            {
                result.maskedText = MaskAllPatterns(text);
                if (result.maskedText != text)
                {
                    result.isMasked = true;
                }
            }
            catch (Exception ex)
            {
                //We'll assume that the exception was raised during masking
                //to be safe consider that the log has sensitive information
                //and do not raise an exception.
                result.isMasked = true;
                result.maskedText = ex.Message;
                result.errStr = ex.Message;
            }
            return result;
        }

        private static string MaskAllPatterns(string text)
        {
            string result = text;
            foreach (var maskFunction in s_maskFunctions)
            {
                result = maskFunction.Invoke(result);
            }
            if (CUSTOM_PATTERNS_LENGTH > 0)
            {
                result = MaskCustomPatterns(result);
            }
            return result;
        }
    }
}
